## ADDED Requirements

### Requirement: bucket policy document storage and management APIs

The gateway SHALL persist an optional bucket policy JSON document per bucket (`buckets.policy`, migration 008, both dialects) and expose `PUT /{bucket}?policy` (owner-gated; body = the policy JSON, bounded to 20 KiB, fully validated server-side before storage), `GET /{bucket}?policy` (owner-gated; returns the stored document verbatim, `NoSuchBucketPolicy` 404 when absent), and `DELETE /{bucket}?policy` (owner-gated, idempotent no-content when absent or deleted). The three policy-management faces SHALL always be owner-only and SHALL never themselves be subject to policy evaluation (an explicit Deny can never lock the owner out of policy management). Validation SHALL reject loudly (400, naming the offending element): wrong or missing `Version` (must be `2012-10-17`), missing `Statement`/`Effect`, an unsupported principal form (IAM-style `arn:` principals), any `Condition`/`NotPrincipal`/`NotAction`/`NotResource` element, unknown action names, resource ARNs outside this bucket, and resources exceeding the trailing-`*` prefix form.

#### Scenario: policy management round-trip

- **WHEN** the owner PUTs a valid policy, GETs it, then DELETEs it
- **THEN** GET returns the document verbatim, DELETE is 204, and a later GET is 404 `NoSuchBucketPolicy` (delete stays idempotent for buckets without a policy)

### Requirement: bucket policy authorization evaluation on the data plane

The supported data-plane actions (`s3:GetObject`, `s3:ListBucket` [HeadBucket and bucket listings], `s3:PutObject` [object PUT, part upload, complete, create multipart], `s3:DeleteObject`, `s3:AbortMultipartUpload`, `s3:ListMultipartUploadParts`, `s3:ListBucketMultipartUploads`, and `*`) SHALL be authorized at the existing gateway decision points with the evaluation order: **explicit Deny wins**, then a matching **Allow** passes, then the pre-existing fallback applies (owner implicit allow; public-read canned fallback for read-family actions only; otherwise the owner-only/Private semantics). Principal matching SHALL use the credential owner id or `*` (anonymous requests match only `*`). Object-level actions SHALL match `arn:antfs:s3:::{bucket}/{key}` resources (exact or trailing-`*` prefix) and bucket-level actions the bare bucket ARN. An unparsable stored policy SHALL fail closed (500) rather than fall back to a weaker check. Buckets without a policy SHALL behave exactly as before this change (zero-behavior-delta for existing faces).

#### Scenario: cross-tenant prefix read via policy

- **WHEN** tenant A writes bucket `share` (Private) with a statement `Allow` `Principal {AWS: B}`, `Action s3:GetObject`, `Resource arn:antfs:s3:::share/Reports/*` and tenant B GETs `share/Reports/q1.bin`
- **THEN** the read succeeds even though the bucket is private; the same request to `share/Secrets/x` gets 403

#### Scenario: explicit Deny beats the owner's implicit allow

- **WHEN** the bucket policy contains `Deny` for the owner on `s3:PutObject` resource `arn:antfs:s3:::share/*`
- **THEN** the owner's PUT to that bucket fails 403, while `DELETE /share?policy` (management face, ungated by policy) still succeeds and removes the policy

#### Scenario: policy grants anonymous reads on a private bucket

- **WHEN** the policy allows `s3:GetObject` to Principal `"*"` on `arn:antfs:s3:::pub/*`
- **THEN** unauthenticated GET of `pub/anything` succeeds without any signature

#### Scenario: rejects are loud at PUT-policy time

- **WHEN** the policy document contains a `Condition` block, an `arn:aws:iam::…` principal, an action `s3:TagObjects`, or a resource for another bucket
- **THEN** the PUT is rejected 400 naming the offender and no policy is stored

#### Scenario: no policy = exact previous behavior

- **WHEN** a bucket has no policy document
- **THEN** every operation's authorization matches the pre-change semantics (canned public-read and owner-only rules, anonymous private-bucket reads rejected)

#### Scenario: corrupt stored policy fails closed

- **WHEN** the stored `buckets.policy` value does not parse as a valid policy (manually corrupted row)
- **THEN** data-plane authorization for that bucket returns an internal error (500) instead of falling back to a weaker check
