## MODIFIED Requirements

### Requirement: UploadPart writes a part as an independent blob
The S3 gateway SHALL accept `PUT /<bucket>/<key>?partNumber=N&uploadId=ID`, write the part body as a separate blob via `put_auto_with_ttl`, compute its MD5 ETag, and upsert a row in `multipart_parts` **inside a single metastore transaction that takes the session's `multipart_uploads` row lock (`SELECT … FOR UPDATE`) first and locks the part row before reading it**. On re-upload of the same part number the upsert SHALL return the replaced part's `blob_id`, and the handler SHALL tombstone it (`router.delete`) within the same request (idempotent). Under concurrent re-uploads of the same part number, the ownership of every replaced blob SHALL be handed over exactly once (no blob left without a reclaimer; lock-reads return the latest committed row, so a late uploader never receives a stale old value).

#### Scenario: Successful part upload
- **WHEN** client sends `PUT /mybucket/bigfile.bin?partNumber=1&uploadId=abc` with 5 MiB body
- **THEN** gateway returns 200 with `ETag` for that part

#### Scenario: Re-upload overwrites previous part
- **WHEN** client uploads partNumber=1 twice with different data
- **THEN** the second upload's blob_id and etag replace the first in `multipart_parts`; the old part blob is tombstoned (`router.delete`) within the same request

#### Scenario: Concurrent re-upload of the same part number
- **WHEN** 8 clients concurrently upload partNumber=1 for the same session with different bodies
- **THEN** all requests succeed (200); each uploaded blob is accounted for exactly once — it is either the final row's blob or handed back as the old value to exactly one request; no intermediate blob is left without a tombstone

#### Scenario: Upload racing with abort sees an ordered outcome
- WHEN one client uploads a part WHILE another aborts the session
- THEN either the upload is rejected with `NoSuchUpload` (abort committed first, no part row survives), or the abort's returned blob list includes the part the uploading client stored (upload committed first); in neither order does an ownerless `multipart_parts` row or part blob remain afterwards

### Requirement: CompleteMultipartUpload assembles parts into a named object
The S3 gateway SHALL accept `POST /<bucket>/<key>?uploadId=ID` with XML listing parts, validate ETags match, store a manifest (ordered part blob_ids + sizes) in `named_objects` — inheriting the `content_type`/`user_meta` captured at CreateMultipartUpload — delete the multipart session, and return the final object's ETag. **The whole session read + session deletion SHALL run in one metastore transaction holding the `multipart_uploads` row lock (so no part upload can interleave between the manifest snapshot and the session cleanup).** Part blobs that were uploaded but NOT listed in the CompleteMultipartUpload XML SHALL be reported back to the handler via `CompleteMultipartResult.discarded_part_blobs` and tombstoned (`router.delete`), per S3's "complete discards unlisted parts" semantics.

#### Scenario: Successful completion
- **WHEN** client sends CompleteMultipartUpload with 3 parts whose ETags match uploaded parts
- **THEN** gateway inserts a READY row in `named_objects`, returns 200 with composite `ETag`, GetObject streams all 3 parts concatenated

#### Scenario: ETag mismatch
- **WHEN** client sends CompleteMultipartUpload with an ETag that doesn't match an uploaded part
- **THEN** gateway returns `InvalidPart` error (400)

#### Scenario: Missing part number
- **WHEN** client lists part 1 and 3 but part 2 was never uploaded
- **THEN** gateway returns `InvalidPart` error (400)

#### Scenario: Unlisted parts are discarded and claimed
- **WHEN** 3 parts were uploaded and CompleteMultipartUpload lists only parts 1 and 2
- **THEN** the completion succeeds for the 2-part manifest and `discarded_part_blobs` contains exactly the part-3 blob, which the handler tombstones; no unlisted part blob is left unreclaimed

### Requirement: AbortMultipartUpload cancels a session
The S3 gateway SHALL accept `DELETE /<bucket>/<key>?uploadId=ID` and, **in one metastore transaction holding the `multipart_uploads` row lock**, delete all `multipart_parts` rows, delete the `multipart_uploads` row, and return the part blob_ids for the handler to tombstone (idempotent re-delete).

#### Scenario: Successful abort
- **WHEN** client sends AbortMultipartUpload for an active upload
- **THEN** gateway returns 204, upload no longer appears in ListMultipartUploads, and every part blob known at abort time is returned exactly once for reclamation
