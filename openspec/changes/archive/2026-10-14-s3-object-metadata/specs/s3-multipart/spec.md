## MODIFIED Requirements

### Requirement: CreateMultipartUpload initiates a multipart session
The S3 gateway SHALL accept `POST /<bucket>/<key>?uploads`, generate a unique `upload_id`, insert a row into `multipart_uploads` (carrying the request's `Content-Type` — defaulting to `binary/octet-stream` when absent — and `x-amz-meta-*` user metadata), and return the upload_id in XML response. Metadata validation is the same as PutObject: total user-metadata bytes over 2048 → `MetadataTooLarge` (400); `Content-Type` longer than 256 bytes → `InvalidArgument` (400).

#### Scenario: Successful initiation
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads`
- **THEN** gateway returns 200 XML with `<UploadId>` and the upload appears in ListMultipartUploads

#### Scenario: Initiation with metadata
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads` with `Content-Type: video/mp4` and `x-amz-meta-project: demo`
- **THEN** the multipart session stores both; after CompleteMultipartUpload, GetObject returns `Content-Type: video/mp4` and `x-amz-meta-project: demo`

#### Scenario: Initiation with oversized metadata
- **WHEN** client sends `POST /mybucket/bigfile.bin?uploads` with `x-amz-meta-*` headers totaling more than 2048 bytes
- **THEN** gateway returns `MetadataTooLarge` (400), no session is created

### Requirement: CompleteMultipartUpload assembles parts into a named object
The S3 gateway SHALL accept `POST /<bucket>/<key>?uploadId=ID` with XML listing parts, validate ETags match, store a manifest (ordered part blob_ids + sizes) in `named_objects` — inheriting the `content_type`/`user_meta` captured at CreateMultipartUpload — delete the multipart session, and return the final object's ETag.

#### Scenario: Successful completion
- **WHEN** client sends CompleteMultipartUpload with 3 parts whose ETags match uploaded parts
- **THEN** gateway inserts a READY row in `named_objects`, returns 200 with composite `ETag`, GetObject streams all 3 parts concatenated

#### Scenario: ETag mismatch
- **WHEN** client sends CompleteMultipartUpload with an ETag that doesn't match an uploaded part
- **THEN** gateway returns `InvalidPart` error (400)

#### Scenario: Missing part number
- **WHEN** client lists part 1 and 3 but part 2 was never uploaded
- **THEN** gateway returns `InvalidPart` error (400)
