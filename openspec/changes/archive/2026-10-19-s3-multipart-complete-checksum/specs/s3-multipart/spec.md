## MODIFIED Requirements

### Requirement: UploadPart writes a part as an independent blob
The S3 gateway SHALL accept `PUT /<bucket>/<key>?partNumber=N&uploadId=ID`, write the part body as a separate blob via `put_auto_with_ttl`, compute its MD5 ETag, and upsert a row in `multipart_parts` **inside a single metastore transaction that takes the session's `multipart_uploads` row lock (`SELECT … FOR UPDATE`) first and locks the part row before reading it**. On re-upload of the same part number the upsert SHALL return the replaced part's `blob_id`, and the handler SHALL tombstone it (`router.delete`) within the same request (idempotent), resolving its reclaim WAL row (queued in the same transaction) on success. Under concurrent re-uploads of the same part number, the ownership of every replaced blob SHALL be handed over exactly once (no blob left without a reclaimer; lock-reads return the latest committed row, so a late uploader never receives a stale old value). A crash between commit and tombstone SHALL leave the WAL row for GC reclamation.

Flexible checksums on a part body SHALL be verified with the same semantics as PutObject (request-header and trailer carriers, `BadDigest`/`InvalidArgument`/`NotImplemented` faces, verification before the blob write), and the UploadPart response SHALL echo the gateway-computed checksum (`x-amz-checksum-{algo}` + `x-amz-checksum-type: FULL_OBJECT`). The **verified** part checksum (strongest claim when several algorithms are declared, mirroring PutObject persistence) SHALL be persisted on the `multipart_parts` row (`part_checksum_algo` / `part_checksum_b64`, migration 007) as raw material for CompleteMultipartUpload's complete-level rulings.

#### Scenario: Successful part upload
- **WHEN** client sends `PUT /mybucket/bigfile.bin?partNumber=1&uploadId=abc` with 5 MiB body
- **THEN** gateway returns 200 with `ETag` for that part

#### Scenario: Part checksum verified, echoed, and persisted
- **WHEN** a part upload carries the correct `x-amz-checksum-crc32`
- **THEN** the response echoes the value with `x-amz-checksum-type: FULL_OBJECT`, and the part row persists the algorithm and value for the complete-level ruling

## ADDED Requirements

### Requirement: CompleteMultipartUpload enforces complete-level checksums

CompleteMultipartUpload SHALL evaluate the object's final checksum from the manifest-listed parts, accept client-supplied complete-level checksum headers, and persist the winning value on the object row with an explicit kind (`checksum_type` column: `FULL_OBJECT` | `COMPOSITE`; NULL reads as `FULL_OBJECT` for pre-migration rows).

**COMPOSITE path** (`x-amz-checksum-type: COMPOSITE`): only the CRC32/CRC32C family is legal (SHA-family or both CRCs together → `InvalidRequest` 400). The composite value SHALL be `base64(concat(raw-part-checksum-bytes in manifest order))`, derived **inside the complete transaction's session lock** where the part snapshot is final; the object row stores `{algo, composite, COMPOSITE}` and the response echoes it. A client-supplied composite value SHALL be compared against the derivation — mismatch → `BadDigest` 400 with zero side effects (session and parts rows untouched, retry-safe). Derivation requires every manifest-listed part to carry a same-family CRC part checksum; mixed algorithms or partially-covered parts (or no part values at all) under the composite claim → `InvalidRequest` 400.

**FULL_OBJECT path** (type absent defaults here, or explicit): every four-family checksum value header present SHALL be verified against the streamed concatenation of the manifest-listed part blobs (bounded memory, at most one part in flight), before the metastore commit; mismatch on any → `BadDigest` 400 zero side effects. Passing values are persisted under the existing strongest-one-wins rule with kind `FULL_OBJECT`. A declared type with no value headers → `InvalidRequest` 400. `x-amz-checksum-crc64nvme` → 501 (family policy, consistent with PUT/Part).

When parts carry a same-family CRC and the request supplies no complete-level headers, the gateway SHALL still derive, persist, and echo the composite (integrity-by-default). Bare parts (no values anywhere) and no headers → object row keeps NULL checksums (existing behavior).

#### Scenario: Composite auto-derivation
- **WHEN** parts 1..3 upload with correct `x-amz-checksum-crc32` values and complete is sent plain
- **THEN** the complete response carries `x-amz-checksum-crc32` = base64(concat of raw CRCs) + `x-amz-checksum-type: COMPOSITE`, the object row persists it, and a later mode-enabled GET echoes the same pair

#### Scenario: SDK composite value mismatch is rejected with no side effects
- **WHEN** complete supplies a wrong `x-amz-checksum-crc32` composite value with type COMPOSITE
- **THEN** gateway returns `BadDigest` 400 and re-sending the same complete with the correct value succeeds on the untouched session

#### Scenario: Full-object verification
- **WHEN** complete supplies `x-amz-checksum-sha256` with type FULL_OBJECT (or absent type) over a two-part object
- **THEN** the streamed concatenation is hashed and compared before commit; correct values persist with kind FULL_OBJECT and echo on the complete response, wrong values fail `BadDigest` with the session intact

#### Scenario: Family violations are loudly rejected
- **WHEN** complete supplies `x-amz-checksum-sha256` with type COMPOSITE, or parts carry crc32 on some parts and crc32c on others under a composite claim, or some listed parts lack checksum values
- **THEN** gateway returns `InvalidRequest` 400 naming the violation
