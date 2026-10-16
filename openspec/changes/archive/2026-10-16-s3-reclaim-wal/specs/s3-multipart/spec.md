# s3-multipart 变更(s3-reclaim-wal)

## MODIFIED Requirements

### Requirement: UploadPart writes a part as an independent blob
The S3 gateway SHALL accept `PUT /<bucket>/<key>?partNumber=N&uploadId=ID`, write the part body as a separate blob via `put_auto_with_ttl`, compute its MD5 ETag, and upsert a row in `multipart_parts` **inside a single metastore transaction that takes the session's `multipart_uploads` row lock (`SELECT … FOR UPDATE`) first and locks the part row before reading it**. On re-upload of the same part number the upsert SHALL return the replaced part's `blob_id`, and the handler SHALL tombstone it (`router.delete`) within the same request (idempotent), resolving its reclaim-WAL row (queued in the same transaction) on success. Under concurrent re-uploads of the same part number, the ownership of every replaced blob SHALL be handed over exactly once (no blob left without a reclaimer; lock-reads return the latest committed row, so a late uploader never receives a stale old value). A crash between commit and tombstone SHALL leave the WAL row for GC reclamation.

Flexible checksums on a part body SHALL be verified with the same semantics as PutObject (request-header and trailer carriers, `BadDigest`/`InvalidArgument`/`NotImplemented` faces, verification before the blob write), and the UploadPart response SHALL echo the gateway-computed checksum (`x-amz-checksum-{algo}` + `x-amz-checksum-type: FULL_OBJECT`). Part checksums SHALL NOT be persisted (complete-level composite/full-object checksums are out of this change's scope).

#### Scenario: Successful part upload
- **WHEN** client sends `PUT /mybucket/bigfile.bin?partNumber=1&uploadId=abc` with 5 MiB body
- **THEN** gateway returns 200 with `ETag` for that part

#### Scenario: Part checksum verified and echoed
- **WHEN** a part upload carries the correct `x-amz-checksum-crc32`
- **THEN** the response includes `x-amz-checksum-crc32` and `x-amz-checksum-type: FULL_OBJECT`

#### Scenario: Part checksum mismatch rejected
- **WHEN** a part upload carries a wrong checksum value
- **THEN** gateway returns 400 `BadDigest`; no part blob is written and the session is unchanged

#### Scenario: Re-upload overwrites previous part
- **WHEN** client uploads partNumber=1 twice with different data
- **THEN** the second upload's blob_id and etag replace the first in `multipart_parts`; the old part blob is tombstoned (`router.delete`) within the same request

#### Scenario: Re-upload crash window is WAL-backed
- **WHEN** the gateway crashes after the re-upload transaction commits but before the old part's tombstone lands
- **THEN** the old part's blob id remains in the reclaim WAL and GC reclaims it

#### Scenario: Concurrent re-upload of the same part number
- **WHEN** 8 clients concurrently upload partNumber=1 for the same session with different bodies
- **THEN** all requests succeed (200); each uploaded blob is accounted for exactly once — it is either the final row's blob or handed back as the old value to exactly one request; no intermediate blob is left without a tombstone

#### Scenario: Upload racing with abort sees an ordered outcome
- WHEN one client uploads a part WHILE another aborts the session
- THEN either the upload is rejected with `NoSuchUpload` (abort committed first, no part row survives), or the abort's returned blob list includes the part the uploading client stored (upload committed first); in neither order does an ownerless `multipart_parts` row or part blob remain afterwards
