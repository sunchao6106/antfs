# s3-core 变更(s3-range-streaming)

## MODIFIED Requirements

### Requirement: GetObject reads a named object
The S3 gateway SHALL accept `GET /<bucket>/<key>`, look up the latest READY version from MetaStore, then read the blob body from the storage cluster via `router.get(&blob_id)` (single-blob objects are bounded by `max_body_bytes`; multipart objects SHALL be assembled per the `s3-multipart` spec's bounded-memory read requirement). Response SHALL include `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, `x-amz-meta-*` user metadata, and `Accept-Ranges: bytes` (metadata is served from the MetaStore row; rows with NULL `content_type` — written before this change — simply omit the header).

When the request carries a `Range` header, the gateway SHALL parse exactly one byte-range spec (`bytes=start-end` / `bytes=start-` / `bytes=-suffix`): a satisfiable range SHALL produce **206 Partial Content** with `Content-Range: bytes start-end/total`, `Content-Length` equal to the range width, and the object's regular metadata headers; a range whose start is beyond the last byte (or an empty object) SHALL produce **416 `InvalidRange`**; a syntactically invalid or multi-range header SHALL be ignored and served as a full **200** response. `end` exceeding the object size SHALL be clamped to `size-1`; a suffix longer than the object SHALL return the whole object as 206. Range semantics SHALL be identical for plaintext, SSE-encrypted (decrypt-then-slice), and multipart objects. HEAD requests SHALL NOT process `Range` (S3 HeadObject semantics) but SHALL return `Accept-Ranges: bytes`.

#### Scenario: Successful read
- **WHEN** client sends `GET /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with body bytes matching what was uploaded, including `Accept-Ranges: bytes`

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

#### Scenario: HEAD ignores Range
- **WHEN** client sends `HEAD /mybucket/hello.txt` with `Range: bytes=0-9`
- **THEN** gateway returns 200, full `Content-Length`, empty body, and `Accept-Ranges: bytes`

### Requirement: HeadObject returns object metadata
The S3 gateway SHALL accept `HEAD /<bucket>/<key>`, look up the latest READY version from MetaStore, and return metadata headers without reading the blob body, including `Accept-Ranges: bytes`. A `Range` header on HEAD SHALL be ignored (no 206 responses on HEAD).

#### Scenario: Successful head
- **WHEN** client sends `HEAD /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, `x-amz-meta-*`, and `Accept-Ranges: bytes` headers, empty body
