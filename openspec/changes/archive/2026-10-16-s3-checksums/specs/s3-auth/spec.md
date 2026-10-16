# s3-auth 变更(s3-checksums)

## MODIFIED Requirements

### Requirement: decoded-length accounting
For framed bodies the gateway SHALL account the length against `x-amz-decoded-content-length` (not the wire `Content-Length`), reject truncation and mismatch with `IncompleteBody` (400), and capture unknown framing garbage as `InvalidArgument` (400). Trailer-section header lines SHALL be parsed and collected into a per-request trailer collector surfaced with the request (request extensions) so writer handlers can **verify** declared checksum values after the body is consumed (previously tolerated without verification); trailer lines themselves remain unsigned in the SIGNED-TRAILER flavor (the chunk-signature chain covers data chunks only, unchanged). A request whose `x-amz-trailer` header declares a checksum form that never arrives SHALL fail with `InvalidArgument` (400) at the writer.

#### Scenario: Trailer checksum value is surfaced for verification
- **WHEN** an `aws-chunked` UNSIGNED-TRAILER PUT declares `x-amz-trailer: x-amz-checksum-crc32` and streams the correct trailer value
- **THEN** the writer verifies it against the decoded body and stores the object (200)

#### Scenario: Declared trailer checksum never arrives
- **WHEN** `x-amz-trailer` declares a checksum form and the frame ends without that trailer line
- **THEN** the write fails with `InvalidArgument` (400) and nothing is stored

#### Scenario: Wrong trailer checksum rejected
- **WHEN** the streamed trailer carries a checksum value that does not match the decoded body
- **THEN** the write fails with `BadDigest` (400) and nothing is stored
