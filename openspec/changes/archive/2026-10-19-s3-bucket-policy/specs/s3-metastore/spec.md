## ADDED Requirements

### Requirement: bucket policy persistence (migration 008)

Migration `008_add_bucket_policy.sql` (both dialects, idempotent on PostgreSQL) SHALL add `buckets.policy TEXT NULL` holding the validated policy JSON document (absent = NULL). `BucketMeta` SHALL gain a `policy: Option<String>` field populated by `head_bucket` (other BucketMeta readers such as `list_buckets` may leave it unset). A `set_bucket_policy(bucket, policy: Option<&str>)` metastore method SHALL upsert the column (None clears it), update the row only when the bucket exists (NoSuchBucket otherwise), and be safe to retry idempotently.

#### Scenario: policy round-trips through head_bucket

- **WHEN** `set_bucket_policy("b", Some(doc))` commits and `head_bucket("b")` runs
- **THEN** the returned `BucketMeta.policy` equals the stored document verbatim

#### Scenario: clearing is an upsert, not a delete of the row

- **WHEN** `set_bucket_policy("b", None)` runs for a bucket with a stored policy
- **THEN** the bucket row remains (name/owner/acl untouched) with `policy` NULL, and re-setting a document afterwards persists again
