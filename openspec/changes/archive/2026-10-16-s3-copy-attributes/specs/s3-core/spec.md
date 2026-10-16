## ADDED Requirements

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
The S3 gateway SHALL support `GET /{bucket}/{key}?attributes` with the `x-amz-object-attributes` request header restricting the returned attribute set to members of {`ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`}. Unknown attribute names or a missing header SHALL return `InvalidArgument` (400). `Checksum` and `ObjectParts` SHALL return `NotImplemented` (501) explicitly. The response SHALL carry `ETag` (quoted) and `Last-Modified` headers plus a `GetObjectAttributesOutput` XML body with the requested supported attributes (`ETag` bare, `ObjectSize`, `StorageClass` fixed to `STANDARD`).

#### Scenario: Supported attributes returned
- **WHEN** the attributes header lists `ETag,ObjectSize,StorageClass`
- **THEN** the body contains those three elements and the headers carry `ETag` and `Last-Modified`

#### Scenario: Unsupported attribute families are explicitly out
- **WHEN** the attributes header lists `Checksum` or `ObjectParts`
- **THEN** the gateway returns 501 `NotImplemented` rather than omitting silently

#### Scenario: Missing object
- **WHEN** the key does not exist
- **THEN** the response is `NoSuchKey` (404)
