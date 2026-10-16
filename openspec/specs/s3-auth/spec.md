## Purpose

SigV4 authentication (header and presigned URL) and credential management for the S3 gateway.
## Requirements
### Requirement: SigV4 header authentication
The S3 gateway SHALL parse the `Authorization` header with algorithm `AWS4-HMAC-SHA256`, extract access key / signed headers / signature, look up the secret key from MetaStore `s3_credentials`, compute the expected signature (canonical request → string-to-sign → signing key → HMAC-SHA256), and compare in constant time. Requests with invalid or missing credentials SHALL receive `AccessDenied` (403) or `InvalidAccessKeyId` (403).

#### Scenario: Valid signature accepted
- **WHEN** client sends a request with correct SigV4 Authorization header
- **THEN** request is processed normally

#### Scenario: Invalid signature rejected
- **WHEN** client sends a request with incorrect signature (wrong secret key)
- **THEN** gateway returns `SignatureDoesNotMatch` (403)

#### Scenario: Unknown access key
- **WHEN** client sends a request with an access_key not in `s3_credentials`
- **THEN** gateway returns `InvalidAccessKeyId` (403)

#### Scenario: Clock skew beyond 15 minutes
- **WHEN** request's `X-Amz-Date` is more than 15 minutes from server time
- **THEN** gateway returns `RequestTimeTooSkewed` (403)

### Requirement: SigV4 presigned URL authentication
The S3 gateway SHALL accept presigned URLs with `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature` query parameters, verify the signature, and reject expired URLs.

#### Scenario: Valid presigned URL
- **WHEN** client accesses a presigned URL within its expiry
- **THEN** request is processed normally

#### Scenario: Expired presigned URL
- **WHEN** client accesses a presigned URL after `X-Amz-Expires` has elapsed
- **THEN** gateway returns `AccessDenied` (403)

### Requirement: Unsigned payload support
The S3 gateway SHALL accept `x-amz-content-sha256: UNSIGNED-PAYLOAD` for PutObject requests, skipping payload hash verification while still verifying the header signature.

#### Scenario: Unsigned payload PutObject
- **WHEN** client sends PutObject with `UNSIGNED-PAYLOAD` content SHA256 and valid header signature
- **THEN** object is stored normally

### Requirement: Credential CRUD via admin endpoint
The S3 gateway SHALL expose `POST /admin/credentials` (create) and `DELETE /admin/credentials/{access_key}` (revoke), protected by an operator token (same pattern as admin crate).

#### Scenario: Create credential
- **WHEN** operator sends `POST /admin/credentials` with owner name
- **THEN** gateway returns JSON `{access_key, secret_key}`, credential is usable for S3 requests

#### Scenario: Revoke credential
- **WHEN** operator sends `DELETE /admin/credentials/AKIA...`
- **THEN** credential is deleted, subsequent S3 requests with that key return 403

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

