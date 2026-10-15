# s3-metastore 变更(s3-bucket-quota)

## ADDED Requirements

### Requirement: Quota accounting invariant
The MetaStore SHALL maintain `buckets.used_bytes = Σ size of the bucket's state=1 rows` transactionally: the write guard (single-statement conditional UPDATE on `buckets`) and size adjustments for insert/shadow/delete run in the same transaction as the row changes they accompany. GC purges state=0 rows and SHALL NOT touch used_bytes.

#### Scenario: Guard is statement-atomic
- **WHEN** two concurrent PUTs race the last 4 KiB of a quota
- **THEN** exactly one passes the single-statement guard; the other gets QuotaExceeded (no oversell)

#### Scenario: Shadow and delete subtract in-transaction
- **WHEN** put shadows an old live row or delete tombstones one
- **THEN** used_bytes is decremented by that row's size in the same transaction
