## ADDED Requirements

### Requirement: PutObject writes a named object
The S3 gateway SHALL accept `PUT /<bucket>/<key>` with a request body, allocate a blobId via Router SDK (`put_auto_with_ttl` for small bodies, `put_file_with_ttl` for large bodies), write the blob to the storage cluster, then insert a mapping row `(bucket, key, version) → blob_id` into MetaStore. The response SHALL include `ETag` (MD5 hex of body).

#### Scenario: Successful small object upload
- **WHEN** client sends `PUT /mybucket/hello.txt` with 100-byte body and valid SigV4
- **THEN** gateway returns 200 with `ETag` header, object is readable via GetObject

#### Scenario: Successful large object upload (streaming)
- **WHEN** client sends `PUT /mybucket/big.bin` with 10 MiB body and Content-Length
- **THEN** gateway stages to temp file, writes via `put_file_with_ttl`, returns 200 with `ETag`

#### Scenario: Overwrite existing key
- **WHEN** client sends `PUT /mybucket/hello.txt` and a READY row already exists for that key
- **THEN** gateway writes a new blob + inserts a new version row; GET returns the latest version; old blob becomes reclaimable by GC

#### Scenario: Partition full retry
- **WHEN** all replicas of the initially chosen partition return 507
- **THEN** gateway (via `put_auto_with_ttl`) evicts that partition and retries on a different one, transparently to the S3 client

### Requirement: GetObject reads a named object
The S3 gateway SHALL accept `GET /<bucket>/<key>`, look up the latest READY version from MetaStore, then stream the blob body from the storage cluster via `router.get(&blob_id)`. Response SHALL include `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, and `x-amz-meta-*` user metadata.

#### Scenario: Successful read
- **WHEN** client sends `GET /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with body bytes matching what was uploaded

#### Scenario: Object not found
- **WHEN** client sends `GET /mybucket/nonexistent`
- **THEN** gateway returns S3 `NoSuchKey` error (404 XML)

#### Scenario: Deleted object not found
- **WHEN** an object was deleted (soft-delete in MetaStore) and client sends GET
- **THEN** gateway returns `NoSuchKey` (404)

### Requirement: HeadObject returns object metadata
The S3 gateway SHALL accept `HEAD /<bucket>/<key>`, look up the latest READY version from MetaStore, and return metadata headers without reading the blob body.

#### Scenario: Successful head
- **WHEN** client sends `HEAD /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified` headers, empty body

### Requirement: DeleteObject soft-deletes a named object
The S3 gateway SHALL accept `DELETE /<bucket>/<key>`, set `deleted_at` on the latest READY MetaStore row, then delete the blob via `router.delete(&blob_id)`.

#### Scenario: Successful delete
- **WHEN** client sends `DELETE /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 204, subsequent GET returns NoSuchKey (404)

#### Scenario: Delete nonexistent is idempotent
- **WHEN** client sends `DELETE /mybucket/nonexistent`
- **THEN** gateway returns 204 (S3 spec: delete of nonexistent is success)

### Requirement: DeleteObjects batch-deletes multiple objects
The S3 gateway SHALL accept `POST /<bucket>?delete` with XML body listing keys, soft-delete each, and return per-key success/error in XML response.

#### Scenario: Batch delete mixed results
- **WHEN** client sends DeleteObjects with 3 keys: one exists, one already deleted, one never existed
- **THEN** gateway returns XML with all 3 marked as successfully deleted (S3 idempotent delete)
