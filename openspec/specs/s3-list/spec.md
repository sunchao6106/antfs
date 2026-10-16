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

### Requirement: ListObjectsV1 lists objects without list-type=2
The S3 gateway SHALL accept `GET /{bucket}` **without** `list-type=2` (and without other subresources) as a ListObjectsV1 request. It SHALL honor `prefix`, `delimiter`, `max-keys` (default 1000), and `marker` (exclusive start key) parameters, SHALL query MetaStore with the same list core as ListObjectsV2 (marker maps to the exclusive start-after position), and SHALL return V1-shaped XML: `Name`, `Prefix` (possibly empty), `Marker` (echoed, possibly empty), `MaxKeys`, `IsTruncated`; `Delimiter` only when provided; `Contents` entries with `Key`, `LastModified`, `ETag`, `Size`, and `StorageClass` (always `STANDARD`); `CommonPrefixes` when a delimiter groups keys. `NextMarker` SHALL be returned **only** when a delimiter is present and the listing is truncated, carrying the MetaStore's exclusive continuation position (the last key scanned — possibly a key not visibly returned when the delimiter grouped it). A subsequent request with `NextMarker` as `marker` SHALL continue the listing without omitting or duplicating any key. Known shared simplification: because pagination truncates raw rows before delimiter grouping, one common prefix MAY appear on more than one page; no key SHALL be lost or duplicated across the paged union.

#### Scenario: Simple V1 list
- **WHEN** bucket has objects `a.txt`, `b.txt`, `c.txt` and client sends `GET /mybucket` (no list-type)
- **THEN** response XML has `Marker` (empty), `IsTruncated=false`, `Contents` with Key/LastModified/ETag/Size/StorageClass, and no `KeyCount` or `NextContinuationToken`

#### Scenario: V1 pagination with marker
- **WHEN** bucket has 3 objects and client sends `GET /mybucket?max-keys=2` then echoes the truncated response's continuation as `marker`
- **THEN** first response has `IsTruncated=true` with 2 `Contents`; the second returns the 3rd object with `IsTruncated=false`; the union is all 3 keys

#### Scenario: V1 delimiter produces CommonPrefixes and NextMarker
- **WHEN** bucket has `logs/2024/a.log`, `logs/2025/b.log`, `data/c.bin`, and client sends `GET /mybucket?prefix=logs/&delimiter=/&max-keys=1`
- **THEN** response has `IsTruncated=true`, at least one `CommonPrefixes` entry, and a `NextMarker` (delimiter present); the next page with that marker completes the listing

#### Scenario: V1 without delimiter omits NextMarker
- **WHEN** a truncated V1 listing is produced without a delimiter
- **THEN** the response has `IsTruncated=true` but no `NextMarker` element (per S3 V1 semantics the client uses the last key as the next marker)

### Requirement: GetBucketLocation returns the bucket region
The S3 gateway SHALL accept `GET /{bucket}?location`, restricted to the bucket owner (`AccessDenied` for non-owners), and SHALL return a `LocationConstraint` XML document whose content is the gateway's configured region, except that region `us-east-1` SHALL render as an empty element (S3 classic-region behavior).

#### Scenario: Location for us-east-1
- **WHEN** the gateway is configured with region `us-east-1` and the owner sends `GET /mybucket?location`
- **THEN** response is a `LocationConstraint` element with empty content and 200 status

#### Scenario: Location for an explicit region
- **WHEN** the gateway is configured with region `cn-north-1` and the owner sends `GET /mybucket?location`
- **THEN** response contains `cn-north-1` inside the `LocationConstraint` element

#### Scenario: Non-owner is denied
- **WHEN** an authenticated non-owner principal sends `GET /mybucket?location`
- **THEN** gateway returns 403 `AccessDenied`

