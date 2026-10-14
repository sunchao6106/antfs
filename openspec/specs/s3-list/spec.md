## Purpose

ListObjectsV2 list objects (prefix/delimiter/CommonPrefixes/pagination) semantics for the S3 gateway.

## Requirements

### Requirement: ListObjectsV2 lists objects in a bucket with prefix and delimiter
The S3 gateway SHALL accept `GET /<bucket>?list-type=2` with optional `prefix`, `delimiter`, `max-keys`, `start-after`, `continuation-token` parameters. It SHALL query MetaStore for READY, non-deleted objects matching the prefix, apply delimiter logic to produce CommonPrefixes, and return S3-conformant XML.

#### Scenario: Simple list
- **WHEN** bucket has objects `a.txt`, `b.txt`, `c.txt` and client sends `GET /mybucket?list-type=2`
- **THEN** response XML lists all 3 objects with Key, Size, ETag, LastModified

#### Scenario: Prefix filtering
- **WHEN** bucket has `logs/2024/a.log`, `logs/2024/b.log`, `data/c.bin` and client sends `prefix=logs/`
- **THEN** response lists only the two `logs/` objects

#### Scenario: Delimiter produces CommonPrefixes
- **WHEN** bucket has `logs/2024/a.log`, `logs/2025/b.log`, `data/c.bin` and client sends `prefix=logs/&delimiter=/`
- **THEN** response has CommonPrefixes `logs/2024/` and `logs/2025/`, no Contents entries for those keys

#### Scenario: Pagination with continuation token
- **WHEN** bucket has 2000 objects and client sends `max-keys=1000`
- **THEN** first response has `IsTruncated=true` and `NextContinuationToken`; second request with that token returns remaining 1000

#### Scenario: Empty bucket
- **WHEN** bucket exists but has no objects
- **THEN** response has empty `<Contents>` list and `KeyCount=0`

### Requirement: CreateBucket creates a new bucket
The S3 gateway SHALL accept `PUT /<bucket>` (no key), insert a row into `buckets` MetaStore table, and return 200.

#### Scenario: Successful creation
- **WHEN** client sends `PUT /newbucket`
- **THEN** gateway returns 200, bucket appears in ListBuckets

#### Scenario: Bucket already exists (same owner)
- **WHEN** client sends `PUT /existingbucket` and the bucket was created by the same owner
- **THEN** gateway returns 200 (idempotent)

#### Scenario: Invalid bucket name
- **WHEN** client sends `PUT /INVALID_NAME` (uppercase, underscore)
- **THEN** gateway returns `InvalidBucketName` error (400)

### Requirement: DeleteBucket deletes an empty bucket
The S3 gateway SHALL accept `DELETE /<bucket>`, verify the bucket contains no objects (query MetaStore), delete the `buckets` row, and return 204.

#### Scenario: Successful deletion of empty bucket
- **WHEN** client sends `DELETE /emptybucket` and no READY non-deleted objects exist
- **THEN** gateway returns 204, bucket no longer appears in ListBuckets

#### Scenario: Bucket not empty
- **WHEN** client sends `DELETE /fullbucket` and objects exist
- **THEN** gateway returns `BucketNotEmpty` error (409)

### Requirement: HeadBucket checks bucket existence
The S3 gateway SHALL accept `HEAD /<bucket>` and return 200 if the bucket exists, 404 otherwise.

#### Scenario: Bucket exists
- **WHEN** client sends `HEAD /mybucket` and the bucket exists
- **THEN** gateway returns 200 with empty body

#### Scenario: Bucket not found
- **WHEN** client sends `HEAD /nonexistent`
- **THEN** gateway returns 404

### Requirement: ListBuckets lists all buckets for the authenticated owner
The S3 gateway SHALL accept `GET /` and return XML listing all buckets owned by the authenticated user.

#### Scenario: List buckets
- **WHEN** authenticated user owns buckets `alpha` and `beta`
- **THEN** response XML lists both with Name and CreationDate
