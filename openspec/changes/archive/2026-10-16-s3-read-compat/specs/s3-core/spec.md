# s3-core 变更(s3-read-compat)

## MODIFIED Requirements

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
