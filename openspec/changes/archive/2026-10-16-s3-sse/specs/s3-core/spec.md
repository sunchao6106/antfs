## ADDED Requirements

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
