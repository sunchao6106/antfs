## ADDED Requirements

### Requirement: ACL XML grant faces (bucket and object)

PUT /{bucket}?acl WITHOUT the `x-amz-acl` header SHALL accept an AccessControlPolicy XML body: Owner ID must equal the bucket owner (else 403), permissions restricted to FULL_CONTROL and READ (WRITE/READ_ACP/WRITE_ACP → 400), grantees restricted to canonical owner ids and the AllUsers group URI. An AllUsers READ grant SHALL map to the existing canned public-read column; other-owner READ grants SHALL persist as a JSON grant set (`buckets.acl_grants`, migration 009) consulted as a lift of the authorize fallback for `s3:ListBucket`. GET ?acl SHALL serialize the granted form when grants exist, the canned form otherwise. PUT/GET /{key}?acl SHALL provide the same grant syntax for a single object: READ grants persist on the object row (`named_objects.acl_grants`) and lift the authorize fallback for `s3:GetObject` on that key only (lazily consulted on the deny path; policy Deny and policy Allow always outrank grants). ACL management faces SHALL be owner-gated and never subject to policy/grant evaluation. The evaluation order across the change batch SHALL be: policy explicit Deny > policy Allow > ACL grants > owner implied / canned fallback.

#### Scenario: bucket XML grant lifts listing for another tenant

- **WHEN** the owner PUTs a bucket ACL granting READ to tenant B and tenant B lists the bucket (ListObjectsV2)
- **THEN** listing succeeds while GETs of objects remain 403 until an object-level grant exists (bucket READ grants lift only the bucket face)

#### Scenario: object XML grant lifts a single object read

- **WHEN** the owner PUTs an object ACL granting READ to tenant B on key `share.bin`
- **THEN** tenant B's GET of `share.bin` succeeds while sibling keys stay 403, and the policy Deny for B (if any) still wins over the grant

#### Scenario: malformed grants are rejected loudly

- **WHEN** the XML body carries a WRITE permission, an unknown grantee, or a non-owner Owner ID
- **THEN** the PUT fails 400/403 respectively and no grant is stored
