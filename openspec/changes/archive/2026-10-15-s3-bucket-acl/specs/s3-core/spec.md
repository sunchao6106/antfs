# s3-core 变更(s3-bucket-acl)

## ADDED Requirements

### Requirement: Private bucket access control
All bucket-scoped operations (PutObject / GetObject / HeadObject / DeleteObject / batch delete, ListObjectsV2 / HeadBucket / DeleteBucket, and the multipart family: Create / UploadPart / Complete / Abort / ListParts / ListMultipartUploads) SHALL require the authenticated caller's credential owner to equal the bucket owner. Cross-tenant access SHALL return 403 `AccessDenied`; operations on unknown buckets SHALL return 404 `NoSuchBucket`. CreateBucket is not gated (creation claims the name under the caller's owner; an already-taken name keeps the existing conflict semantics). ListBuckets lists only the caller's own buckets.

#### Scenario: Cross-tenant object write denied
- **WHEN** owner `bob` PUTs an object into a bucket owned by `default`
- **THEN** the gateway returns 403 AccessDenied and no object row or blob is written

#### Scenario: Cross-tenant read / list / delete denied
- **WHEN** owner `bob` issues GET / HEAD / DELETE on objects, ListObjectsV2, or batch delete in a bucket owned by `default`
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
