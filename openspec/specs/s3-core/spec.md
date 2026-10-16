## Purpose

Core object operations of the S3 gateway: Put/Get/Head/Delete and batch delete, with metadata stored in MetaStore and bytes through the Router SDK.
## Requirements
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

### Requirement: GetObject reads a named object
The S3 gateway SHALL accept `GET /<bucket>/<key>`, look up the latest READY version from MetaStore, then read the blob body from the storage cluster via `router.get(&blob_id)` (single-blob objects are bounded by `max_body_bytes`; multipart objects SHALL be assembled per the `s3-multipart` spec's bounded-memory read requirement). Response SHALL include `Content-Length`, `Content-Type`, `ETag`, `Last-Modified` (RFC 1123 formatting of the object's `created_at`), `x-amz-meta-*` user metadata, and `Accept-Ranges: bytes` (metadata is served from the MetaStore row; rows with NULL `content_type` — written before this change — simply omit the header).

Conditional requests SHALL be evaluated in RFC 7232 §6 order, strictly from MetaStore metadata and before any blob byte is read: (1) `If-Match` — a mismatch SHALL produce **412 `PreconditionFailed`**; success short-circuits the remaining conditional headers; (2) `If-Unmodified-Since` (evaluated only when `If-Match` is absent) — an object modified after the given date SHALL produce **412**; (3) `If-None-Match` — a match (quote-stripped equality, or `*`) SHALL produce **304 Not Modified** with an empty body, `ETag`, and `Last-Modified`, and no blob read; a non-match short-circuits `If-Modified-Since`; (4) `If-Modified-Since` (evaluated only when `If-None-Match` is absent) — an object not modified since the given date SHALL produce **304**. A syntactically malformed date in any conditional header SHALL produce **400 `InvalidArgument`** (same behavior as the copy path).

When the request carries a `Range` header and no failing `If-Range` (see below), the gateway SHALL parse exactly one byte-range spec (`bytes=start-end` / `bytes=start-` / `bytes=-suffix`): a satisfiable range SHALL produce **206 Partial Content** with `Content-Range: bytes start-end/total`, `Content-Length` equal to the range width, and the object's regular metadata headers; a range whose start is beyond the last byte (or an empty object) SHALL produce **416 `InvalidRange`**; a syntactically invalid or multi-range header SHALL be ignored and served as a full **200** response. `end` exceeding the object size SHALL be clamped to `size-1`; a suffix longer than the object SHALL return the whole object as 206. Range semantics SHALL be identical for plaintext, SSE-encrypted (decrypt-then-slice), and multipart objects.

When a `Range` (or `partNumber`, below) window is requested together with `If-Range`, the validator SHALL gate the partial response: an ETag form (quoted, strong — `W/` weak tags are treated as a mismatch) passes only on exact equality with the object ETag; an HTTP-date form passes only when the date is at least as recent as the object's `Last-Modified`. A failing (mismatch, stale, or unparseable) `If-Range` SHALL cause the `Range`/`partNumber` window to be ignored and the **entire object** to be returned as **200**.

When the request carries `partNumber` (GET only; HEAD SHALL ignore it), the gateway SHALL resolve the part window from metadata: for a multipart object, `partNumber` SHALL address one manifest part by cumulative byte offset — a valid part SHALL produce **206** with `Content-Range: bytes start-end/total` in whole-object coordinates and a body equal to exactly that part's bytes; for a single-part object (plaintext or SSE), `partNumber=1` SHALL produce **206** covering the whole object (`bytes 0-(size-1)/size`). `partNumber` values below 1 or beyond the part count SHALL produce **416 `InvalidRange`** naming the actual part count. When both `Range` and `partNumber` are present and valid, the served window SHALL be their intersection (still 206, `Content-Range` in whole-object coordinates); an empty intersection SHALL produce **416**.

When conditional headers produce 304 or 412, the gateway SHALL NOT read the blob body; HEAD requests SHALL NOT process `Range` (S3 HeadObject semantics) but SHALL return `Accept-Ranges: bytes`.

#### Scenario: Successful read
- **WHEN** client sends `GET /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with body bytes matching what was uploaded, including `Accept-Ranges: bytes` and an RFC 1123 `Last-Modified`

#### Scenario: Object not found
- **WHEN** client sends `GET /mybucket/nonexistent`
- **THEN** gateway returns S3 `NoSuchKey` error (404 XML)

#### Scenario: Deleted object not found
- **WHEN** an object was deleted (soft-delete in MetaStore) and client sends GET
- **THEN** gateway returns `NoSuchKey` (404)

#### Scenario: Metadata headers returned
- **WHEN** client sends GET for an object uploaded with `Content-Type: text/plain` and `x-amz-meta-owner: alice`
- **THEN** response includes headers `Content-Type: text/plain` and `x-amz-meta-owner: alice`

#### Scenario: Ranged read returns 206
- **WHEN** client sends `GET /mybucket/hello.txt` with `Range: bytes=10-19` on a 100-byte object
- **THEN** gateway returns 206 with `Content-Range: bytes 10-19/100`, `Content-Length: 10`, and body bytes 10..=19 of the object

#### Scenario: Ranged read clamps overlong end
- **WHEN** client sends `Range: bytes=90-1000` on a 100-byte object
- **THEN** gateway returns 206 with `Content-Range: bytes 90-99/100` and 10 body bytes

#### Scenario: Open-ended and suffix ranges
- **WHEN** client sends `Range: bytes=90-` then `Range: bytes=-7` on a 100-byte object
- **THEN** gateway returns 206 `bytes 90-99/100` (10 bytes), then 206 `bytes 93-99/100` (7 bytes)

#### Scenario: Unsatisfiable range returns 416
- **WHEN** client sends `Range: bytes=100-` or `bytes=0-0` on a 0-byte object (or `bytes=-0`)
- **THEN** gateway returns 416 with XML error code `InvalidRange`; no body is read

#### Scenario: Invalid or multi-range is ignored
- **WHEN** client sends `Range: bytes=5-1` or `Range: bytes=0-1,3-4` or a non-`bytes` unit
- **THEN** gateway returns 200 with the full object (an ignored, not failed, Range)

#### Scenario: HEAD ignores Range and partNumber
- **WHEN** client sends `HEAD /mybucket/hello.txt` with `Range: bytes=0-9`
- **THEN** gateway returns 200, full `Content-Length`, empty body, and `Accept-Ranges: bytes`

#### Scenario: If-Match mismatch returns 412
- **WHEN** client sends GET with `If-Match: "deadbeef"` for an object whose ETag differs
- **THEN** gateway returns 412 `PreconditionFailed` XML and reads no blob bytes

#### Scenario: If-Match success short-circuits
- **WHEN** client sends GET with a matching `If-Match` and an otherwise-failing `If-Modified-Since`
- **THEN** gateway returns 200 with the full object (post-If-Match conditionals are ignored)

#### Scenario: If-None-Match hit returns 304
- **WHEN** the object was not modified and client sends GET with `If-None-Match: "<etag>"` (or `*`)
- **THEN** gateway returns 304 with empty body, including `ETag` and `Last-Modified`, and reads no blob bytes

#### Scenario: If-Modified-Since not modified returns 304
- **WHEN** the object's Last-Modified is older than a GET's `If-Modified-Since` date
- **THEN** gateway returns 304 with `ETag` and `Last-Modified` and an empty body

#### Scenario: If-Unmodified-Since modified returns 412
- **WHEN** the object was modified after the GET's `If-Unmodified-Since` date and no `If-Match` is present
- **THEN** gateway returns 412 `PreconditionFailed`

#### Scenario: HEAD conditional requests
- **WHEN** client sends HEAD with a matching `If-None-Match` (or a failing `If-Unmodified-Since`)
- **THEN** gateway returns 304 (or 412, respectively) with empty body

#### Scenario: Malformed conditional date returns 400
- **WHEN** a conditional header carries a non-HTTP-date string
- **THEN** gateway returns 400 `InvalidArgument`

#### Scenario: If-Range pass serves the range
- **WHEN** client sends `Range: bytes=0-9` with `If-Range: "<current etag>"` (or an HTTP-date at least as recent as Last-Modified)
- **THEN** gateway returns 206 with the ranged bytes

#### Scenario: If-Range failure serves the whole object
- **WHEN** client sends `Range: bytes=0-9` with a stale or mismatching (or unparseable) `If-Range`
- **THEN** gateway returns 200 with the entire object and no `Content-Range`

#### Scenario: partNumber serves a single part of a multipart object
- **WHEN** a multipart object has parts sized [10, 20, 30] and client sends GET with `?partNumber=2`
- **THEN** gateway returns 206 with `Content-Range: bytes 10-29/60` and exactly the 20 bytes of part 2

#### Scenario: partNumber out of range returns 416
- **WHEN** the same object is requested with `?partNumber=0` or `?partNumber=4`
- **THEN** gateway returns 416 `InvalidRange` naming the actual part count

#### Scenario: partNumber on a single-part object
- **WHEN** a plain (or SSE) single-blob object is requested with `?partNumber=1`
- **THEN** gateway returns 206 with `Content-Range: bytes 0-(size-1)/size` and the whole body; `?partNumber=2` returns 416

#### Scenario: partNumber combined with Range
- **WHEN** `?partNumber=2` is combined with `Range: bytes=15-19` on the [10, 20, 30] object (part 2 spans object bytes 10..30)
- **THEN** gateway returns 206 with `Content-Range: bytes 15-19/60` (the intersection, whole-object coordinates)

#### Scenario: partNumber combined with an out-of-part Range
- **WHEN** `?partNumber=2` is combined with `Range: bytes=0-9` (which lies entirely before part 2)
- **THEN** gateway returns 416 `InvalidRange` (empty intersection)

#### Scenario: If-Range failure with partNumber serves the whole object
- **WHEN** `?partNumber=2` is combined with a failing `If-Range` validator
- **THEN** gateway returns 200 with the entire assembled object

### Requirement: HeadObject returns object metadata
The S3 gateway SHALL accept `HEAD /<bucket>/<key>`, look up the latest READY version from MetaStore, and return metadata headers without reading the blob body, including `Accept-Ranges: bytes` and `Last-Modified` (RFC 1123 formatting of the object's `created_at`). A `Range` or `partNumber` parameter on HEAD SHALL be ignored (no 206 responses on HEAD). Conditional headers SHALL be evaluated with the same order and outcomes as GetObject (412 for `If-Match`/`If-Unmodified-Since` failures, 304 with empty body for `If-None-Match`/`If-Modified-Since` hits), without reading any body.

#### Scenario: Successful head
- **WHEN** client sends `HEAD /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, `x-amz-meta-*`, and `Accept-Ranges: bytes` headers, empty body

### Requirement: DeleteObject soft-deletes a named object
The S3 gateway SHALL accept `DELETE /<bucket>/<key>`, set `deleted_at` on the latest READY MetaStore row, then delete the blob via `router.delete(&blob_id)`.

#### Scenario: Successful delete
- **WHEN** client sends `DELETE /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 204, subsequent GET returns NoSuchKey (404)

#### Scenario: Delete nonexistent is idempotent
- **WHEN** client sends `DELETE /mybucket/nonexistent`
- **THEN** gateway returns 204 (S3 spec: delete of nonexistent is success)

### Requirement: DeleteObjects batch-deletes multiple objects
The S3 gateway SHALL accept `POST /<bucket>?delete` with XML body listing keys, soft-delete each, and return per-key success/error in XML response.

#### Scenario: Batch delete mixed results
- **WHEN** client sends DeleteObjects with 3 keys: one exists, one already deleted, one never existed
- **THEN** gateway returns XML with all 3 marked as successfully deleted (S3 idempotent delete)

### Requirement: Bucket storage quotas gate writes
The S3 gateway SHALL enforce a per-bucket storage quota (`buckets.quota_bytes`, 0 = unlimited): PutObject and CompleteMultipartUpload that would push the bucket's live logical bytes (`used_bytes`) beyond the quota SHALL be rejected with HTTP 507, XML error code `QuotaExceeded`, carrying bucket/quota/used. The rejected request's already-written blob SHALL be tombstoned (`router.delete`) within the same request (s3-gc death-point model). Quota changes take effect for subsequent writes only (no retroactive deletion).

#### Scenario: Put rejected when quota exceeded
- **WHEN** bucket quota is 100 KiB, live bytes are 96 KiB, client PUTs a 8 KiB object
- **THEN** gateway returns 507 with code QuotaExceeded; the new blob is tombstoned; used_bytes stays 96 KiB

#### Scenario: Quota raise restores writes
- **WHEN** the operator raises the bucket quota to 200 KiB
- **THEN** the same 8 KiB PUT succeeds (204-less 200) and used_bytes becomes 104 KiB

#### Scenario: Delete frees room
- **WHEN** an object is deleted (state=0), its size is subtracted from used_bytes
- **THEN** a subsequent PUT that previously hit the quota now succeeds

#### Scenario: Overwrite decrements the shadowed version
- **WHEN** a 100 KiB object is overwritten by a 20 KiB object
- **THEN** used_bytes reflects the 20 KiB live row (shadow-marked 100 KiB subtracted in the same transaction)

#### Scenario: Multipart complete counts logical size
- **WHEN** a multipart upload completes with parts totalling 300 KiB in a quota-limited bucket
- **THEN** 300 KiB (manifest logical size) is added atomically at complete time; parts themselves were never counted

#### Scenario: Unlimited by default
- **WHEN** no quota was ever set (quota_bytes = 0)
- **THEN** writes behave exactly as before this change

### Requirement: Private bucket access control
All bucket-scoped operations SHALL require the authenticated caller's credential owner to equal the bucket owner, with one carve-out: the read surface (GetObject / HeadObject / ListObjectsV2) of a `public-read` bucket, which SHALL be granted to any caller — anonymous (unsigned) requests, signed non-owner tenants, and the owner alike. Anonymous means strictly "no credential attached at all" (no `Authorization` header and no presigned query); requests carrying credentials that fail SigV4 verification SHALL be rejected (403) by the auth middleware and never downgraded to anonymous. Cross-tenant access on private surface returns 403 `AccessDenied`; anonymous access to any auth-required surface returns 403 `AccessDenied`; unknown buckets return 404 `NoSuchBucket` (existence is decided before ACL). CreateBucket / ListBuckets require a signed request (CreateBucket claims the name under the caller's owner; an already-taken name keeps the existing conflict semantics; ListBuckets still lists only the caller's own buckets).

The canned ACL is per-bucket: `private` (default — exactly the prior semantics) or `public-read`. It is set via the `x-amz-acl` header on CreateBucket or on `PUT /{bucket}?acl` (owner-only); ACL changes take effect for subsequent requests immediately. `GET /{bucket}?acl` (owner-only) returns the AccessControlPolicy XML, carrying an AllUsers READ grant iff the bucket is public-read. PutBucketAcl in the AccessControlPolicy-XML-body form (no canned header) SHALL return 501 `NotImplemented`; an unsupported `x-amz-acl` value SHALL return 400 `InvalidArgument`.

#### Scenario: Cross-tenant object write denied
- **WHEN** owner `bob` PUTs an object into a bucket owned by `default`
- **THEN** the gateway returns 403 AccessDenied and no object row or blob is written

#### Scenario: Cross-tenant read / list / delete denied on a private bucket
- **WHEN** owner `bob` issues GET / HEAD / DELETE on objects, ListObjectsV2, or batch delete in a private bucket owned by `default`
- **THEN** each returns 403 AccessDenied

#### Scenario: Cross-tenant multipart denied
- **WHEN** owner `bob` initiates a multipart upload (or any multipart op) in a bucket owned by `default`
- **THEN** the gateway returns 403 and no session is created

#### Scenario: Unknown bucket
- **WHEN** any authenticated caller addresses a bucket that does not exist
- **THEN** the gateway returns 404 NoSuchBucket

#### Scenario: Owner unaffected
- **WHEN** the bucket owner accesses their own bucket
- **THEN** all operations behave exactly as before (200/204 semantics unchanged)

#### Scenario: ListBuckets isolation
- **WHEN** owner `bob` lists buckets while owner `default` owns one
- **THEN** `default`'s bucket is absent from bob's listing

#### Scenario: Anonymous request on a private bucket is denied
- **WHEN** an unsigned GET fetches an object in a private (default) bucket
- **THEN** the gateway returns 403 AccessDenied; no body is disclosed

#### Scenario: Public-read grants anonymous reads
- **WHEN** the owner sets the bucket to `public-read` (via `PUT ?acl` with `x-amz-acl`, or creates it with the header) and an unsigned caller performs GET / HEAD on an object, or ListObjectsV2
- **THEN** each returns 2xx with the real content / listing

#### Scenario: Anonymous write still denied on a public bucket
- **WHEN** an unsigned caller PUTs, DELETEs, or initiates multipart in a `public-read` bucket
- **THEN** the gateway returns 403 AccessDenied and nothing is written

#### Scenario: Cross-tenant signed read granted only on public-read
- **WHEN** signed owner `bob` GETs an object in `default`'s bucket
- **THEN** the gateway returns 200 if the bucket is public-read, 403 AccessDenied if private

#### Scenario: Failed signature is never anonymous
- **WHEN** a request carries an `Authorization` header that fails SigV4 verification, against a public-read bucket
- **THEN** the gateway returns 403 at the auth middleware; the request is not treated as anonymous

#### Scenario: ACL switch is immediate
- **WHEN** the owner flips a bucket private → public-read (or back) via `PUT ?acl`
- **THEN** the very next request observes the new ACL (public GET 200 / private GET 403)

#### Scenario: GetBucketAcl is owner-only and reflects the ACL
- **WHEN** the owner GETs `?acl` on a public-read bucket (then flips it back to private)
- **THEN** the first response contains an AllUsers READ grant, the second does not; a non-owner GET `?acl` returns 403 in both cases

#### Scenario: Invalid canned ACL value
- **WHEN** CreateBucket or PUT `?acl` carries `x-amz-acl: public-read-write`
- **THEN** the gateway returns 400 InvalidArgument

#### Scenario: XML-body PutBucketAcl is explicitly out of scope
- **WHEN** PUT `?acl` arrives without the `x-amz-acl` header (the AccessControlPolicy XML body form)
- **THEN** the gateway returns 501 NotImplemented

### Requirement: server-side object copy
The S3 gateway SHALL support `x-amz-copy-source` on PutObject: read the source object server-side and write the destination without a client body round trip. The source copy gate SHALL be the public-read gate (owner or `public-read`), the destination gate SHALL remain the bucket-owner write gate. The four conditional headers (`x-amz-copy-source-if-match` / `-if-none-match` / `-if-unmodified-since` / `-if-modified-since`) SHALL be evaluated against the source in that precedence order, with any unsatisfied condition rejecting the copy as `PreconditionFailed` (412). The metadata directive SHALL be `COPY` (default: replicate source `Content-Type` and `x-amz-meta-*`) or `REPLACE` (capture from the copy request); invalid values SHALL return `InvalidArgument` (400).

#### Scenario: Single-part copy round trip
- **WHEN** the owner PUTs an object carrying a valid `x-amz-copy-source` of an existing object
- **THEN** the response is 200 with `CopyObjectResult` XML, and GET of the destination returns bytes identical to the source

#### Scenario: Copy with metadata directives
- **WHEN** a copy sets `x-amz-metadata-directive: REPLACE` with new metadata headers
- **THEN** the destination stores the new `Content-Type` and user metadata, while `COPY` (or absent directive) replicates the source's

#### Scenario: Copy-source preconditions
- **WHEN** `x-amz-copy-source-if-match` carries an ETag different from the source's
- **THEN** the gateway returns `PreconditionFailed` (412) and nothing is written to the destination

#### Scenario: Self copy without replacement
- **WHEN** source equals destination (same bucket and key) and the directive is `COPY`
- **THEN** the gateway returns `InvalidRequest` (400); with `REPLACE` it rewrites the object's metadata

#### Scenario: Copy of a multipart object stays memory-bounded
- **WHEN** the source is a multipart object
- **THEN** the gateway re-creates it through an internal multipart session part by part (one part materialized at a time) and the destination reads back equal to the source

#### Scenario: Quota gates the copy
- **WHEN** the destination bucket's quota would be exceeded by the copy
- **THEN** the gateway returns `QuotaExceeded` (507) and the destination object is not created

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

### Requirement: server-side encryption at rest
The S3 gateway SHALL support server-side encryption of objects opt-in via the `x-amz-server-side-encryption: AES256` request header on PutObject (and CopyObject for the destination): object bytes SHALL be encrypted with a per-object random data key wrapped by the configured master key before reaching the blob engine, and GET SHALL decrypt transparently. The response of the writing call, GET, and HEAD SHALL echo the `x-amz-server-side-encryption: AES256` header. With no master key configured the header SHALL yield `NotImplemented` (501); any other value SHALL yield `InvalidArgument` (400). Multipart uploads carrying the header (create or part) SHALL yield `NotImplemented` (501) explicitly this round.

#### Scenario: encrypted round trip
- **WHEN** an object is PUT with the SSE header and then GET
- **THEN** the body is byte-identical to the plaintext and both responses carry the header

#### Scenario: ciphertext differs from plaintext
- **WHEN** the stored blob bytes are inspected directly (engine level)
- **THEN** they carry the SSE frame magic and do not equal the plaintext

#### Scenario: corruption is loud
- **WHEN** any frame region is tampered with
- **THEN** the read fails with an internal error (500); it never returns substituted or partial plaintext silently

#### Scenario: unconfigured gateway
- **WHEN** the PUT carries the SSE header but no master key is configured
- **THEN** the gateway returns 501 `NotImplemented`

#### Scenario: legacy rows stay plaintext
- **WHEN** an object written before this feature (NULL sse column) is read
- **THEN** it is returned as stored, byte-identical, with no SSE header

### Requirement: server-side encryption master key rotation

The gateway binary SHALL provide an offline rotation mode invoked as `antfs-s3 --rotate-sse-key <new-key-file>`: it SHALL build the same state as normal startup (same config file, same metastore and router connections, old master key from `s3.sse_master_key_file`), run the rotation to completion, print a report (`scanned`, `rotated`, `skipped-other-key`) to stdout, and exit — without starting the GC loop or the HTTP listener. Rotation SHALL be run only while the gateway is stopped: the tool prints an offline-mode warning on start and takes no other concurrency precaution (a concurrent PutObject during rotation may lose that write).

For every `state=1` row with a non-NULL sse descriptor, the tool SHALL: abort loudly on an unparseable descriptor, an algorithm other than `AES256`, an sse descriptor combined with `parts_manifest` (unreachable state), or any blob read / decrypt / re-encrypt / row-write failure — reporting the progress at that point (rows rotated so far plus the offending bucket/key). A row whose descriptor `key_id` differs from the old master key's id SHALL be left byte-for-byte untouched (skipped and counted). Rotated rows SHALL preserve every ObjectMeta field verbatim (etag, size, content_type, user metadata, checksum) and swap exactly `blob_id` (new blob carrying the re-encrypted frame) and the sse descriptor (`key_id` of the new master key). Shadowed old blobs SHALL be tombstoned and their reclaim-WAL rows resolved via the existing overwrite arc, and a failed row-write after a successful new-blob write SHALL tombstone the new blob in the same invocation (quota-rejection arc).

Rotation SHALL be resumable by construction (no tool-side progress state): re-running it with the same old/new pair skips already-rotated rows (their descriptors now carry the new key id) and converges without double-writing.

#### Scenario: full rotation swaps the master and reads back

- **WHEN** SSE objects are written under master key A, the tool runs with new key B, the key file is swapped to B, and the gateway restarts
- **THEN** GET/HEAD of every rotated object return identical bytes, etag, size, and metadata as before rotation, still echoing `x-amz-server-side-encryption: AES256`, and the old blobs are tombstoned with no pending reclaim rows left

#### Scenario: re-run after an abort resumes without double work

- **WHEN** a previous rotation pass aborted (or completed) and the tool runs again with the same old/new pair
- **THEN** rows already carrying the new key id are reported as `skipped-other-key` and are not re-encrypted

#### Scenario: descriptor pointing at a foreign key id stays untouched

- **WHEN** a row's descriptor bears a key id that is neither the old nor the new key
- **THEN** the row's version, blob id, and descriptor are unchanged

#### Scenario: corrupt row aborts with progress

- **WHEN** a row's descriptor is unparseable, or declares an unknown algorithm, or combines sse with a parts manifest, or its blob/decrypt/re-encrypt/row-write fails mid-run
- **THEN** the tool stops at the first such row, reports rows rotated so far and the offending bucket/key, and exits non-zero

