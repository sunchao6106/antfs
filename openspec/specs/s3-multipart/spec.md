## Purpose

Multipart upload sessions on the S3 gateway: create, upload part, complete, abort, list.
## Requirements
### Requirement: CreateMultipartUpload initiates a multipart session
The S3 gateway SHALL accept `POST /<bucket>/<key>?uploads`, generate a unique `upload_id`, insert a row into `multipart_uploads` (carrying the request's `Content-Type` — defaulting to `binary/octet-stream` when absent — and `x-amz-meta-*` user metadata), and return the upload_id in XML response. Metadata validation is the same as PutObject: total user-metadata bytes over 2048 → `MetadataTooLarge` (400); `Content-Type` longer than 256 bytes → `InvalidArgument` (400).

#### Scenario: Successful initiation
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads`
- **THEN** gateway returns 200 XML with `<UploadId>` and the upload appears in ListMultipartUploads

#### Scenario: Initiation with metadata
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads` with `Content-Type: video/mp4` and `x-amz-meta-project: demo`
- **THEN** the multipart session stores both; after CompleteMultipartUpload, GetObject returns `Content-Type: video/mp4` and `x-amz-meta-project: demo`

#### Scenario: Initiation with oversized metadata
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads` with `x-amz-meta-*` headers totaling more than 2048 bytes
- **THEN** gateway returns `MetadataTooLarge` (400), no session is created

### Requirement: UploadPart writes a part as an independent blob
The S3 gateway SHALL accept `PUT /<bucket>/<key>?partNumber=N&uploadId=ID`, write the part body as a separate blob via `put_auto_with_ttl`, compute its MD5 ETag, and upsert a row in `multipart_parts` **inside a single metastore transaction that takes the session's `multipart_uploads` row lock (`SELECT … FOR UPDATE`) first and locks the part row before reading it**. On re-upload of the same part number the upsert SHALL return the replaced part's `blob_id`, and the handler SHALL tombstone it (`router.delete`) within the same request (idempotent), resolving its reclaim WAL row (queued in the same transaction) on success. Under concurrent re-uploads of the same part number, the ownership of every replaced blob SHALL be handed over exactly once (no blob left without a reclaimer; lock-reads return the latest committed row, so a late uploader never receives a stale old value). A crash between commit and tombstone SHALL leave the WAL row for GC reclamation.

Flexible checksums on a part body SHALL be verified with the same semantics as PutObject (request-header and trailer carriers, `BadDigest`/`InvalidArgument`/`NotImplemented` faces, verification before the blob write), and the UploadPart response SHALL echo the gateway-computed checksum (`x-amz-checksum-{algo}` + `x-amz-checksum-type: FULL_OBJECT`). The **verified** part checksum (strongest claim when several algorithms are declared, mirroring PutObject persistence) SHALL be persisted on the `multipart_parts` row (`part_checksum_algo` / `part_checksum_b64`, migration 007) as raw material for CompleteMultipartUpload's complete-level rulings.

#### Scenario: Successful part upload
- **WHEN** client sends `PUT /mybucket/bigfile.bin?partNumber=1&uploadId=abc` with 5 MiB body
- **THEN** gateway returns 200 with `ETag` for that part

#### Scenario: Part checksum verified, echoed, and persisted
- **WHEN** a part upload carries the correct `x-amz-checksum-crc32`
- **THEN** the response echoes the value with `x-amz-checksum-type: FULL_OBJECT`, and the part row persists the algorithm and value for the complete-level ruling

### Requirement: CompleteMultipartUpload assembles parts into a named object
The S3 gateway SHALL accept `POST /<bucket>/<key>?uploadId=ID` with XML listing parts, validate ETags match, store a manifest (ordered part blob_ids + sizes) in `named_objects` — inheriting the `content_type`/`user_meta` captured at CreateMultipartUpload — delete the multipart session, and return the final object's ETag. **The whole session read + session deletion SHALL run in one metastore transaction holding the `multipart_uploads` row lock (so no part upload can interleave between the manifest snapshot and the session cleanup).** Part blobs that were uploaded but NOT listed in the CompleteMultipartUpload XML SHALL be reported back to the handler via `CompleteMultipartResult.discarded_part_blobs` and tombstoned (`router.delete`), per S3's "complete discards unlisted parts" semantics.

#### Scenario: Successful completion
- **WHEN** client sends CompleteMultipartUpload with 3 parts whose ETags match uploaded parts
- **THEN** gateway inserts a READY row in `named_objects`, returns 200 with composite `ETag`, GetObject streams all 3 parts concatenated

#### Scenario: ETag mismatch
- **WHEN** client sends CompleteMultipartUpload with an ETag that doesn't match an uploaded part
- **THEN** gateway returns `InvalidPart` error (400)

#### Scenario: Missing part number
- **WHEN** client lists part 1 and 3 but part 2 was never uploaded
- **THEN** gateway returns `InvalidPart` error (400)

#### Scenario: Unlisted parts are discarded and claimed
- **WHEN** 3 parts were uploaded and CompleteMultipartUpload lists only parts 1 and 2
- **THEN** the completion succeeds for the 2-part manifest and `discarded_part_blobs` contains exactly the part-3 blob, which the handler tombstones; no unlisted part blob is left unreclaimed

### Requirement: AbortMultipartUpload cancels a session
The S3 gateway SHALL accept `DELETE /<bucket>/<key>?uploadId=ID` and, **in one metastore transaction holding the `multipart_uploads` row lock**, delete all `multipart_parts` rows, delete the `multipart_uploads` row, and return the part blob_ids for the handler to tombstone (idempotent re-delete).

#### Scenario: Successful abort
- **WHEN** client sends AbortMultipartUpload for an active upload
- **THEN** gateway returns 204, upload no longer appears in ListMultipartUploads, and every part blob known at abort time is returned exactly once for reclamation

### Requirement: ListParts returns uploaded parts for a session
The S3 gateway SHALL accept `GET /<bucket>/<key>?uploadId=ID` and return XML listing each part's number, size, ETag, with pagination via `part-number-marker` and `max-parts`.

#### Scenario: List parts with pagination
- **WHEN** upload has 20 parts and client sends `max-parts=10`
- **THEN** response lists first 10 parts with `IsTruncated=true` and `NextPartNumberMarker=10`

### Requirement: ListMultipartUploads returns active uploads in a bucket
The S3 gateway SHALL accept `GET /<bucket>?uploads` and return XML listing active (non-completed, non-aborted) multipart uploads, with pagination via `key-marker`/`upload-id-marker` and `max-uploads`, and prefix filtering.

#### Scenario: List uploads with prefix
- **WHEN** bucket has uploads for `logs/a.log`, `logs/b.log`, `data/c.bin` and client sends `prefix=logs/`
- **THEN** response lists only the two `logs/` uploads

### Requirement: Assembled reads keep memory bounded and support ranged reads
GetObject on a multipart object SHALL assemble the manifest parts **sequentially through a streaming response body** — the gateway SHALL NOT materialize the whole object (or any concatenation of all parts) in memory; at most ONE part's bytes (each part is bounded by `max_body_bytes`) SHALL be buffered at a time. The response's `Content-Length` SHALL be the named-object row's `size`; before responding, the gateway SHALL verify that the sum of manifest part sizes equals that row size and SHALL fail loudly with `InternalError` (500) on any mismatch (never serve a short/long body against a declared Content-Length).

When the request carries a satisfiable `Range`, the gateway SHALL translate it via manifest part sizes into a covering window: only parts intersecting the window SHALL be fetched (non-covering parts induce zero network reads), with the head/tail parts byte-sliced; the response SHALL be 206 per the `s3-core` ranged-read semantics.

#### Scenario: Full multipart read streams without whole-object buffering
- **WHEN** a 60-part object (64 KiB each) is read with a plain GET
- **THEN** gateway returns 200 with exactly the concatenated part bytes and `Content-Length` = Σ parts; the read path buffers at most one part in memory (implementation-level: sequential part stream, no `Vec` accumulation)

#### Scenario: Ranged read across parts fetches only covering parts
- **WHEN** an object has 3 parts of 1 MiB each and client sends `Range: bytes=1048576-2097151` (exactly part 2)
- **THEN** gateway returns 206 with `Content-Range: bytes 1048576-2097151/3145728` and part 2's bytes verbatim; only part 2's blob is fetched from the cluster

#### Scenario: Ranged read spanning a part boundary slices head and tail
- **WHEN** 3 parts of 1 MiB each and client sends `Range: bytes=1048500-1048700` (spans the part 1/2 boundary)
- **THEN** gateway returns 206 with body = last 76 bytes of part 1 followed by first 124 bytes of part 2

#### Scenario: Manifest-row size mismatch fails loud
- **WHEN** a row's `size` disagrees with the sum of its manifest part sizes (corrupt metadata)
- **THEN** gateway returns `InternalError` (500) instead of serving a body that contradicts its `Content-Length`

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

