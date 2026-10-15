# s3-core 变更(s3-bucket-quota)

## ADDED Requirements

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
