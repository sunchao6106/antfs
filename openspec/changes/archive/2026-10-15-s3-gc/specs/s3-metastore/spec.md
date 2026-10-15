# s3-metastore 变更(s3-gc)

## MODIFIED Requirements

### Requirement: Soft-delete with cleanup
DeleteObject SHALL mark the row `state = 0` and set `deleted_at` (soft-delete; the mapping row survives until GC). The blob's death point SHALL be the semantic action itself: the handler that performs delete / overwrite-shadowing / abort SHALL call `router.delete` on the affected blob_id within the same request (tombstone, idempotent). A background GC task SHALL periodically purge soft-deleted rows with `deleted_at < now - deletion_retention_secs` (default 24 h) and reclaim orphaned multipart sessions with their parts (past `upload_ttl_secs`, default 72 h) — deleting each part blob. GC claiming SHALL be safe to run on multiple gateway instances concurrently: on PostgreSQL claim queries SHALL use `FOR UPDATE SKIP LOCKED` semantics so a row is claimed exactly once per cycle; on MySQL the delete uses `ORDER BY ... LIMIT` under the storage engine's current-read semantics. GC SHALL NOT be the blob deleter for delete/overwrite paths (blobs are already tombstoned at action time; GC only reclaims rows and orphan parts).

#### Scenario: Soft-deleted object invisible to GET
- **WHEN** object is deleted
- **THEN** GET returns NoSuchKey; the row remains in DB with `state = 0` until GC

#### Scenario: Blob dies at action time
- **WHEN** object is deleted (or its latest version is shadowed by an overwrite)
- **THEN** the gateway handler calls `router.delete` on the affected blob_id in the same request; only the MetaStore row (state = 0, deleted_at = action time) is left for GC

#### Scenario: GC purges rows past retention
- **WHEN** `deletion_retention_secs` elapses after the delete
- **THEN** GC deletes the soft-deleted row; deleted blobs were already tombstoned at action time

#### Scenario: Orphaned multipart upload expires
- **WHEN** a multipart session is abandoned and `upload_ttl_secs` elapses
- **THEN** GC removes the session and its parts rows and `router.delete`s each part blob

#### Scenario: Concurrent GC instances claim exactly once
- **WHEN** two gateway instances run the GC loop simultaneously
- **THEN** each purged row / taken orphan session is handled by exactly one instance per cycle
