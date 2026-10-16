# s3-core 变更(s3-checksums)

## MODIFIED Requirements

### Requirement: PutObject writes a named object
The S3 gateway SHALL accept `PUT /<bucket>/<key>` with a request body, allocate a blobId via Router SDK (`put_auto_with_ttl` for small bodies, `put_file_with_ttl` for large bodies), write the blob to the storage cluster, then insert a mapping row `(bucket, key, version) → blob_id` into MetaStore. The response SHALL include `ETag` (MD5 hex of body). The gateway SHALL capture `Content-Type` and `x-amz-meta-*` request headers into the MetaStore row: when `Content-Type` is absent the gateway SHALL store the S3 default `binary/octet-stream`; user metadata (Σ of UTF-8 bytes of keys and values) exceeding 2048 bytes SHALL be rejected with `MetadataTooLarge` (400); a `Content-Type` header longer than 256 bytes SHALL be rejected with `InvalidArgument` (400). The blob itself carries no metadata (engine-layer isolation). When a READY row already exists for the key, the old rows SHALL be shadow-marked (`state = 0`, `deleted_at` = action time) in the same insert transaction and their blob_ids returned to the handler, which tombstones them (`router.delete`) immediately.

The gateway SHALL verify flexible checksums **before writing any blob**: request headers `x-amz-checksum-{crc32|crc32c|sha1|sha256}` (base64 of the algorithm digest over the plaintext body) and — for `aws-chunked` requests — the trailer-declared checksum value surfaced by the streaming decoder (see the `s3-auth` capability) SHALL each be compared against the gateway's own digest of the decoded body. Mismatch SHALL return **400 `BadDigest`** (naming the checksum form); an unparsable Base64 value or a digest length that does not match the algorithm SHALL return **400 `InvalidArgument`**; a checksum header of an unimplemented family (e.g. `x-amz-checksum-crc64nvme`) SHALL return **501 `NotImplemented`**; a checksum supplied simultaneously as request header and as trailer SHALL return **400 `InvalidArgument`**; `x-amz-checksum-type`, when present, SHALL be `FULL_OBJECT` (anything else → 400). Failed verification SHALL leave no blob, row, or tombstone side-effect.

When verification passes, the gateway SHALL persist exactly one checksum into the MetaStore row — the strongest algorithm present (`SHA256 > SHA1 > CRC32C > CRC32`; all provided values are still verified) — alongside its base64 value; SSE objects persist the **plaintext** checksum. The PutObject response SHALL echo the persisted checksum as `x-amz-checksum-{algo}` plus `x-amz-checksum-type: FULL_OBJECT`. `x-amz-sdk-checksum-algorithm` is informational and ignored.

#### Scenario: Successful small object upload
- **WHEN** client sends `PUT /mybucket/hello.txt` with 100-byte body and valid SigV4
- **THEN** gateway returns 200 with `ETag` header, object is readable via GetObject

#### Scenario: Successful large object upload (streaming)
- **WHEN** client sends `PUT /mybucket/big.bin` with 10 MiB body and Content-Length
- **THEN** gateway stages to temp file, writes via `put_file_with_ttl`, returns 200 with `ETag`

#### Scenario: Overwrite existing key
- **WHEN** client sends `PUT /mybucket/hello.txt` and a READY row already exists for that key
- **THEN** gateway writes a new blob + inserts a new version row; GET returns the latest version; the old rows are shadow-marked and the old blob is tombstoned (`router.delete`) within the same request

#### Scenario: Partition full retry
- **WHEN** all replicas of the initially chosen partition return 507
- **THEN** gateway (via `put_auto_with_ttl`) evicts that partition and retries on a different one, transparently to the S3 client

#### Scenario: Metadata roundtrip
- **WHEN** client sends `PUT /mybucket/hello.txt` with `Content-Type: text/plain` and `x-amz-meta-owner: alice`
- **THEN** GET and HEAD on that key return `Content-Type: text/plain` and `x-amz-meta-owner: alice`

#### Scenario: Default content type when absent
- **WHEN** client sends `PUT /mybucket/data.bin` without a `Content-Type` header
- **THEN** GET returns `Content-Type: binary/octet-stream`

#### Scenario: User metadata over the size limit
- **WHEN** client sends `PUT /mybucket/hello.txt` with `x-amz-meta-*` headers totaling more than 2048 bytes (UTF-8)
- **THEN** gateway returns `MetadataTooLarge` (400), no object is written

#### Scenario: Content-Type over the length limit
- **WHEN** client sends `PUT /mybucket/hello.txt` with a `Content-Type` header longer than 256 bytes
- **THEN** gateway returns `InvalidArgument` (400), no object is written

#### Scenario: Valid checksum accepted, persisted, and echoed
- **WHEN** client PUTs 100 bytes with the correct `x-amz-checksum-crc32` (base64 of the body CRC32)
- **THEN** gateway returns 200 with `x-amz-checksum-crc32` and `x-amz-checksum-type: FULL_OBJECT` headers; a later GET with `x-amz-checksum-mode: ENABLED` returns the same value

#### Scenario: Checksum mismatch rejected with no side-effect
- **WHEN** client PUTs with a wrong `x-amz-checksum-sha256` value
- **THEN** gateway returns 400 `BadDigest` naming the form; no blob, no row, and a following GET returns `NoSuchKey`

#### Scenario: Malformed checksum value rejected
- **WHEN** the checksum header is not valid Base64, or its decoded length does not match the algorithm
- **THEN** gateway returns 400 `InvalidArgument`

#### Scenario: Unimplemented checksum family is explicitly out
- **WHEN** the request carries `x-amz-checksum-crc64nvme`
- **THEN** gateway returns 501 `NotImplemented`

#### Scenario: Header and trailer for the same algorithm conflict
- **WHEN** `x-amz-checksum-crc32` is present both as a request header and as a streamed trailer value
- **THEN** gateway returns 400 `InvalidArgument` (a checksum form has one carrier)

#### Scenario: Multiple algorithms all verified, strongest persisted
- **WHEN** the request carries valid `x-amz-checksum-crc32` and `x-amz-checksum-sha256`
- **THEN** both are verified; the stored/echoed checksum is the SHA256 one

#### Scenario: SSE objects verify, persist, and echo the plaintext checksum
- **WHEN** an SSE PUT carries a correct `x-amz-checksum-crc32c` over the plaintext body
- **THEN** verification passes, the row persists the plaintext checksum, and GET (mode enabled) echoes it

### Requirement: object attributes retrieval
The S3 gateway SHALL support `GET /<bucket>/<key>?attributes` with the `x-amz-object-attributes` request header restricting the returned attribute set to members of {`ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`}. Unknown attribute names or a missing header SHALL return `InvalidArgument` (400). `ObjectParts` SHALL return `NotImplemented` (501) explicitly. `Checksum` SHALL be supported for objects whose row carries a persisted checksum: the `GetObjectAttributesOutput` XML SHALL then include a `Checksum` element with the per-algorithm child (`ChecksumCRC32` / `ChecksumCRC32C` / `ChecksumSHA1` / `ChecksumSHA256`) and `ChecksumType` fixed to `FULL_OBJECT`; when no checksum is persisted (multipart-assembled objects, objects written before this change, copy products) the element SHALL be omitted rather than emitted empty. The response SHALL carry `ETag` (quoted) and `Last-Modified` headers plus a `GetObjectAttributesOutput` XML body with the requested supported attributes (`ETag` bare, `ObjectSize`, `StorageClass` fixed to `STANDARD`).

#### Scenario: Supported attributes returned
- **WHEN** the attributes header lists `ETag,ObjectSize,StorageClass`
- **THEN** the body contains those three elements and the headers carry `ETag` and `Last-Modified`

#### Scenario: Checksum attribute returned when persisted
- **WHEN** an object was uploaded with `x-amz-checksum-sha1` and the attributes header lists `Checksum`
- **THEN** the body contains `<Checksum><ChecksumSHA1>…</ChecksumSHA1><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`

#### Scenario: Checksum attribute omitted when not persisted
- **WHEN** an object was assembled via multipart (no persisted checksum) and the attributes header lists `Checksum`
- **THEN** 200 with the remaining requested attributes; no empty `Checksum` element

#### Scenario: ObjectParts remains explicitly out
- **WHEN** the attributes header lists `ObjectParts`
- **THEN** the gateway returns 501 `NotImplemented`

#### Scenario: Missing object
- **WHEN** the key does not exist
- **THEN** the response is `NoSuchKey` (404)

## ADDED Requirement: checksum response headers on reads
GetObject (200/206) and HeadObject SHALL return `x-amz-checksum-{algo}` (algos among crc32/crc32c/sha1/sha256) plus `x-amz-checksum-type: FULL_OBJECT` **only when** the request carries `x-amz-checksum-mode: ENABLED` (any other mode value is `InvalidArgument` 400) and the object row carries a persisted checksum (plaintext semantics for SSE objects). Responses with status 304/412 SHALL NOT carry checksum headers, and objects without a persisted checksum (multipart-assembled, pre-existing rows, copy products) SHALL omit them silently.

#### Scenario: Read echoes the persisted checksum
- **WHEN** an object uploaded with `x-amz-checksum-crc32` is fetched with `x-amz-checksum-mode: ENABLED`
- **THEN** GET and HEAD return the same `x-amz-checksum-crc32` header and `x-amz-checksum-type: FULL_OBJECT`

#### Scenario: Mode absent means no checksum headers
- **WHEN** the same object is fetched without the mode header
- **THEN** the response carries no `x-amz-checksum-*` headers

#### Scenario: Invalid mode value rejected
- **WHEN** the request carries `x-amz-checksum-mode: SOMETHING`
- **THEN** gateway returns 400 `InvalidArgument`
