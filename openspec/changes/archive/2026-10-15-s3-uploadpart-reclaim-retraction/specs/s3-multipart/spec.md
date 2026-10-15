# s3-multipart 变更(s3-uploadpart-reclaim-retraction)

## MODIFIED Requirements

### Requirement: UploadPart writes a part as an independent blob
The S3 gateway SHALL accept `PUT /<bucket>/<key>?partNumber=N&uploadId=ID`, write the part body as a separate blob via `put_auto_with_ttl`, compute its MD5 ETag, and upsert a row in `multipart_parts`. On re-upload of the same part number the metastore upsert SHALL return the replaced part's `blob_id`, and the handler SHALL tombstone it (`router.delete`) within the same request (idempotent).

#### Scenario: Successful part upload
- **WHEN** client sends `PUT /mybucket/bigfile.bin?partNumber=1&uploadId=abc` with 5 MiB body
- **THEN** gateway returns 200 with `ETag` for that part

#### Scenario: Re-upload overwrites previous part
- **WHEN** client uploads partNumber=1 twice with different data
- **THEN** the second upload's blob_id and etag replace the first in `multipart_parts`; the old part blob is tombstoned (`router.delete`) within the same request
