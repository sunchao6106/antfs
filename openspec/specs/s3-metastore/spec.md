## Purpose

The MetaStore trait over PostgreSQL/MySQL: connection pooling, schema migration, object version ordering, and soft-delete with cleanup.
## Requirements
### Requirement: MetaStore trait abstracts metadata backend
The S3 gateway SHALL define a `MetaStore` async trait covering object CRUD, bucket CRUD, multipart session management, and credential lookup. All handlers interact only with the trait, never with a concrete database driver.

#### Scenario: Handler uses trait not concrete type
- **WHEN** any S3 handler needs metadata
- **THEN** it calls `MetaStore` methods via `Arc<dyn MetaStore>`, not `PgPool` or `MySqlPool` directly

### Requirement: PostgreSQL MetaStore implementation
The S3 gateway SHALL provide a `PgMetaStore` that implements `MetaStore` using `sqlx::PgPool`. Schema SHALL be managed by `sqlx migrate` with migration files under `crates/s3/src/metastore/migrations/postgres/`.

#### Scenario: PutObject + GetObject roundtrip on PostgreSQL
- **WHEN** MetaStore is configured with `driver = "postgres"`
- **THEN** PutObject inserts a row and GetObject retrieves it correctly

#### Scenario: ListObjects prefix query on PostgreSQL
- **WHEN** 1000 objects exist with prefix `logs/` and 500 with prefix `data/`
- **THEN** `list_objects(prefix="logs/")` returns only the 1000 `logs/` objects (paginated)

### Requirement: MySQL MetaStore implementation
The S3 gateway SHALL provide a `MysqlMetaStore` that implements `MetaStore` using `sqlx::MySqlPool`. Schema SHALL be managed by `sqlx migrate` with migration files under `crates/s3/src/metastore/migrations/mysql/`.

#### Scenario: PutObject + GetObject roundtrip on MySQL
- **WHEN** MetaStore is configured with `driver = "mysql"`
- **THEN** PutObject inserts a row and GetObject retrieves it correctly

### Requirement: Schema migration on startup
The S3 gateway SHALL run pending `sqlx` migrations on startup (idempotent), creating tables if they don't exist and applying incremental changes.

#### Scenario: Fresh database
- **WHEN** gateway starts against an empty database
- **THEN** all tables (`buckets`, `named_objects`, `multipart_uploads`, `multipart_parts`, `s3_credentials`) are created

#### Scenario: Already migrated database
- **WHEN** gateway starts against a database with current schema
- **THEN** startup completes without errors, no schema changes

### Requirement: Connection pool with health check
The MetaStore implementations SHALL use a bounded connection pool (`max_connections` configurable) and surface pool exhaustion as S3 `InternalError` (500).

#### Scenario: Pool exhaustion
- **WHEN** all connections are busy and a new request arrives
- **THEN** request waits up to a timeout, then returns 500 `InternalError` (not hang indefinitely)

### Requirement: Object version ordering
Named objects SHALL use a version number `(timestamp_ns / 1000 + random_component)` as tie-breaker. GET/HEAD/DELETE SHALL always resolve to the latest READY, non-deleted version.

#### Scenario: Concurrent overwrites resolve to latest
- **WHEN** two PutObject requests for the same key complete, version A < version B
- **THEN** GetObject returns version B's data

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

