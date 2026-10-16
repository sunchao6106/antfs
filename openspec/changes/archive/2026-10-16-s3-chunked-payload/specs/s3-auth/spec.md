## ADDED Requirements

### Requirement: aws-chunked streaming payload decoding
The S3 gateway SHALL detect `aws-chunked` framed request bodies (the three `STREAMING-*` payload-hash literals together with `Content-Encoding: aws-chunked` and `x-amz-decoded-content-length`), decode the framing into the plaintext object body before the handler reads it, and reject inconsistent declarations with `InvalidArgument` (400) before any database lookup. Presigned URLs SHALL NOT be combinable with streaming framing. A declared decoded length exceeding `s3.max_body_bytes` SHALL be rejected with `EntityTooLarge` (413) before reading the body stream.

#### Scenario: Consistent signed streaming declaration
- **WHEN** a PUT/POST carries a valid SigV4 header whose `x-amz-content-sha256` is one of the three streaming literals and the matching framing headers
- **THEN** the gateway authenticates the header signature, decodes the framing, and hands the handler the plaintext body

#### Scenario: Torn declaration is rejected
- **WHEN** `Content-Encoding: aws-chunked` or `x-amz-decoded-content-length` appears without one of the three streaming literals (or any literal appears without its companions, or the method is not PUT/POST, or the request is presigned)
- **THEN** the gateway returns `InvalidArgument` (400) without consulting the credential store

#### Scenario: Oversized declared decoded length is rejected early
- **WHEN** the declared decoded length exceeds `s3.max_body_bytes`
- **THEN** the gateway returns `EntityTooLarge` (413) without reading the body stream

### Requirement: chunk signature chain verification
For the two chunk-signed flavors (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `...-PAYLOAD-TRAILER`), the gateway SHALL verify each chunk's `;chunk-signature=` value against the SigV4 signing chain (seed = request signature, each chunk's string-to-sign covers the previous signature and the chunk-data SHA-256), comparing in constant time.

#### Scenario: Valid chunk chain accepted
- **WHEN** all chunk signatures follow the chain
- **THEN** the object is stored from the decoded plaintext

#### Scenario: Tampered chunk signature rejected and nothing stored
- **WHEN** any chunk signature hex is altered
- **THEN** the gateway returns `SignatureDoesNotMatch` (403) and a subsequent GET of the key returns `NoSuchKey` (404)

### Requirement: decoded-length accounting
For framed bodies the gateway SHALL account the length against `x-amz-decoded-content-length` (not the wire `Content-Length`), reject truncation and mismatch with `IncompleteBody` (400), and capture unknown framing garbage as `InvalidArgument` (400). Trailer-section checksum values (e.g. `x-amz-checksum-crc32c`) SHALL be parsed and tolerated without verification (trust level equal to `UNSIGNED-PAYLOAD`).

#### Scenario: Declared length does not match decoded bytes
- **WHEN** the decoded plaintext length differs from the declared decoded length
- **THEN** the gateway returns `IncompleteBody` (400) and nothing is stored

#### Scenario: Unsigned-trailer flavor accepted
- **WHEN** a PUT uses `STREAMING-UNSIGNED-PAYLOAD-TRAILER` framing with a trailing checksum header
- **THEN** the object is stored from the decoded plaintext and the checksum value is not verified (reported as ignored)

#### Scenario: Chunked upload part
- **WHEN** `UploadPart` submits its part body in `aws-chunked` framing
- **THEN** the part is decoded and verified by the same rules and the upload proceeds
