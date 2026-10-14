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
