## MODIFIED Requirements

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
