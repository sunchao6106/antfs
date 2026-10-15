## Purpose

Core object operations of the S3 gateway: Put/Get/Head/Delete and batch delete, with metadata stored in MetaStore and bytes through the Router SDK.
## Requirements
### Requirement: PutObject writes a named object
The S3 gateway SHALL accept `PUT /<bucket>/<key>` with a request body, allocate a blobId via Router SDK (`put_auto_with_ttl` for small bodies, `put_file_with_ttl` for large bodies), write the blob to the storage cluster, then insert a mapping row `(bucket, key, version) → blob_id` into MetaStore. The response SHALL include `ETag` (MD5 hex of body). The gateway SHALL capture `Content-Type` and `x-amz-meta-*` request headers into the MetaStore row: when `Content-Type` is absent the gateway SHALL store the S3 default `binary/octet-stream`; user metadata (Σ of UTF-8 bytes of keys and values) exceeding 2048 bytes SHALL be rejected with `MetadataTooLarge` (400); a `Content-Type` header longer than 256 bytes SHALL be rejected with `InvalidArgument` (400). The blob itself carries no metadata (engine-layer isolation). When a READY row already exists for the key, the old rows SHALL be shadow-marked (`state = 0`, `deleted_at` = action time) in the same insert transaction and their blob_ids returned to the handler, which tombstones them (`router.delete`) immediately.

#### Scenario: Successful small object upload
- **WHEN** client sends `PUT /mybucket/hello.txt` with 100-byte body and valid SigV4
- **THEN** gateway returns 200 with `ETag` header, object is readable via GetObject

#### Scenario: Successful large object upload (streaming)
- **WHEN** client sends `PUT /mybucket/big.bin` with 10 MiB body and Content-Length
- **THEN** gateway stages to temp file, writes via `put_file_with_ttl`, returns 200 with `ETag`

#### Scenario: Overwrite existing key
- **WHEN** client sends `PUT /mybucket/hello.txt` and a READY row already exists for that key
- **THEN** gateway writes a new blob + inserts a new version row; GET returns the latest version; the old rows are shadow-marked and the old blob is tombstoned (`router.delete`) within the same request

#### Scenario: Partition full retry
- **WHEN** all replicas of the initially chosen partition return 507
- **THEN** gateway (via `put_auto_with_ttl`) evicts that partition and retries on a different one, transparently to the S3 client

#### Scenario: Metadata roundtrip
- **WHEN** client sends `PUT /mybucket/hello.txt` with `Content-Type: text/plain` and `x-amz-meta-owner: alice`
- **THEN** GET and HEAD on that key return `Content-Type: text/plain` and `x-amz-meta-owner: alice`

#### Scenario: Default content type when absent
- **WHEN** client sends `PUT /mybucket/data.bin` without a `Content-Type` header
- **THEN** GET returns `Content-Type: binary/octet-stream`

#### Scenario: User metadata over the size limit
- **WHEN** client sends `PUT /mybucket/hello.txt` with `x-amz-meta-*` headers totaling more than 2048 bytes (UTF-8)
- **THEN** gateway returns `MetadataTooLarge` (400), no object is written

#### Scenario: Content-Type over the length limit
- **WHEN** client sends `PUT /mybucket/hello.txt` with a `Content-Type` header longer than 256 bytes
- **THEN** gateway returns `InvalidArgument` (400), no object is written

### Requirement: GetObject reads a named object
The S3 gateway SHALL accept `GET /<bucket>/<key>`, look up the latest READY version from MetaStore, then stream the blob body from the storage cluster via `router.get(&blob_id)`. Response SHALL include `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, and `x-amz-meta-*` user metadata (metadata is served from the MetaStore row; rows with NULL `content_type` — written before this change — simply omit the header).

#### Scenario: Successful read
- **WHEN** client sends `GET /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with body bytes matching what was uploaded

#### Scenario: Object not found
- **WHEN** client sends `GET /mybucket/nonexistent`
- **THEN** gateway returns S3 `NoSuchKey` error (404 XML)

#### Scenario: Deleted object not found
- **WHEN** an object was deleted (soft-delete in MetaStore) and client sends GET
- **THEN** gateway returns `NoSuchKey` (404)

#### Scenario: Metadata headers returned
- **WHEN** client sends GET for an object uploaded with `Content-Type: text/plain` and `x-amz-meta-owner: alice`
- **THEN** response includes headers `Content-Type: text/plain` and `x-amz-meta-owner: alice`

### Requirement: HeadObject returns object metadata
The S3 gateway SHALL accept `HEAD /<bucket>/<key>`, look up the latest READY version from MetaStore, and return metadata headers without reading the blob body.

#### Scenario: Successful head
- **WHEN** client sends `HEAD /mybucket/hello.txt` and the object exists
- **THEN** gateway returns 200 with `Content-Length`, `Content-Type`, `ETag`, `Last-Modified`, and `x-amz-meta-*` headers, empty body

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

### Requirement: Bucket storage quotas gate writes
The S3 gateway SHALL enforce a per-bucket storage quota (`buckets.quota_bytes`, 0 = unlimited): PutObject and CompleteMultipartUpload that would push the bucket's live logical bytes (`used_bytes`) beyond the quota SHALL be rejected with HTTP 507, XML error code `QuotaExceeded`, carrying bucket/quota/used. The rejected request's already-written blob SHALL be tombstoned (`router.delete`) within the same request (s3-gc death-point model). Quota changes take effect for subsequent writes only (no retroactive deletion).

#### Scenario: Put rejected when quota exceeded
- **WHEN** bucket quota is 100 KiB, live bytes are 96 KiB, client PUTs a 8 KiB object
- **THEN** gateway returns 507 with code QuotaExceeded; the new blob is tombstoned; used_bytes stays 96 KiB

#### Scenario: Quota raise restores writes
- **WHEN** the operator raises the bucket quota to 200 KiB
- **THEN** the same 8 KiB PUT succeeds (204-less 200) and used_bytes becomes 104 KiB

#### Scenario: Delete frees room
- **WHEN** an object is deleted (state=0), its size is subtracted from used_bytes
- **THEN** a subsequent PUT that previously hit the quota now succeeds

#### Scenario: Overwrite decrements the shadowed version
- **WHEN** a 100 KiB object is overwritten by a 20 KiB object
- **THEN** used_bytes reflects the 20 KiB live row (shadow-marked 100 KiB subtracted in the same transaction)

#### Scenario: Multipart complete counts logical size
- **WHEN** a multipart upload completes with parts totalling 300 KiB in a quota-limited bucket
- **THEN** 300 KiB (manifest logical size) is added atomically at complete time; parts themselves were never counted

#### Scenario: Unlimited by default
- **WHEN** no quota was ever set (quota_bytes = 0)
- **THEN** writes behave exactly as before this change

### Requirement: Private bucket access control
All bucket-scoped operations SHALL require the authenticated caller's credential owner to equal the bucket owner, with one carve-out: the read surface (GetObject / HeadObject / ListObjectsV2) of a `public-read` bucket, which SHALL be granted to any caller — anonymous (unsigned) requests, signed non-owner tenants, and the owner alike. Anonymous means strictly "no credential attached at all" (no `Authorization` header and no presigned query); requests carrying credentials that fail SigV4 verification SHALL be rejected (403) by the auth middleware and never downgraded to anonymous. Cross-tenant access on private surface returns 403 `AccessDenied`; anonymous access to any auth-required surface returns 403 `AccessDenied`; unknown buckets return 404 `NoSuchBucket` (existence is decided before ACL). CreateBucket / ListBuckets require a signed request (CreateBucket claims the name under the caller's owner; an already-taken name keeps the existing conflict semantics; ListBuckets still lists only the caller's own buckets).

The canned ACL is per-bucket: `private` (default — exactly the prior semantics) or `public-read`. It is set via the `x-amz-acl` header on CreateBucket or on `PUT /{bucket}?acl` (owner-only); ACL changes take effect for subsequent requests immediately. `GET /{bucket}?acl` (owner-only) returns the AccessControlPolicy XML, carrying an AllUsers READ grant iff the bucket is public-read. PutBucketAcl in the AccessControlPolicy-XML-body form (no canned header) SHALL return 501 `NotImplemented`; an unsupported `x-amz-acl` value SHALL return 400 `InvalidArgument`.

#### Scenario: Cross-tenant object write denied
- **WHEN** owner `bob` PUTs an object into a bucket owned by `default`
- **THEN** the gateway returns 403 AccessDenied and no object row or blob is written

#### Scenario: Cross-tenant read / list / delete denied on a private bucket
- **WHEN** owner `bob` issues GET / HEAD / DELETE on objects, ListObjectsV2, or batch delete in a private bucket owned by `default`
- **THEN** each returns 403 AccessDenied

#### Scenario: Cross-tenant multipart denied
- **WHEN** owner `bob` initiates a multipart upload (or any multipart op) in a bucket owned by `default`
- **THEN** the gateway returns 403 and no session is created

#### Scenario: Unknown bucket
- **WHEN** any authenticated caller addresses a bucket that does not exist
- **THEN** the gateway returns 404 NoSuchBucket

#### Scenario: Owner unaffected
- **WHEN** the bucket owner accesses their own bucket
- **THEN** all operations behave exactly as before (200/204 semantics unchanged)

#### Scenario: ListBuckets isolation
- **WHEN** owner `bob` lists buckets while owner `default` owns one
- **THEN** `default`'s bucket is absent from bob's listing

#### Scenario: Anonymous request on a private bucket is denied
- **WHEN** an unsigned GET fetches an object in a private (default) bucket
- **THEN** the gateway returns 403 AccessDenied; no body is disclosed

#### Scenario: Public-read grants anonymous reads
- **WHEN** the owner sets the bucket to `public-read` (via `PUT ?acl` with `x-amz-acl`, or creates it with the header) and an unsigned caller performs GET / HEAD on an object, or ListObjectsV2
- **THEN** each returns 2xx with the real content / listing

#### Scenario: Anonymous write still denied on a public bucket
- **WHEN** an unsigned caller PUTs, DELETEs, or initiates multipart in a `public-read` bucket
- **THEN** the gateway returns 403 AccessDenied and nothing is written

#### Scenario: Cross-tenant signed read granted only on public-read
- **WHEN** signed owner `bob` GETs an object in `default`'s bucket
- **THEN** the gateway returns 200 if the bucket is public-read, 403 AccessDenied if private

#### Scenario: Failed signature is never anonymous
- **WHEN** a request carries an `Authorization` header that fails SigV4 verification, against a public-read bucket
- **THEN** the gateway returns 403 at the auth middleware; the request is not treated as anonymous

#### Scenario: ACL switch is immediate
- **WHEN** the owner flips a bucket private → public-read (or back) via `PUT ?acl`
- **THEN** the very next request observes the new ACL (public GET 200 / private GET 403)

#### Scenario: GetBucketAcl is owner-only and reflects the ACL
- **WHEN** the owner GETs `?acl` on a public-read bucket (then flips it back to private)
- **THEN** the first response contains an AllUsers READ grant, the second does not; a non-owner GET `?acl` returns 403 in both cases

#### Scenario: Invalid canned ACL value
- **WHEN** CreateBucket or PUT `?acl` carries `x-amz-acl: public-read-write`
- **THEN** the gateway returns 400 InvalidArgument

#### Scenario: XML-body PutBucketAcl is explicitly out of scope
- **WHEN** PUT `?acl` arrives without the `x-amz-acl` header (the AccessControlPolicy XML body form)
- **THEN** the gateway returns 501 NotImplemented

