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
DeleteObject SHALL set `deleted_at` on the MetaStore row (soft-delete). A background GC task SHALL periodically remove rows with `deleted_at < now - retention_period` and their associated blob data.

#### Scenario: Soft-deleted object invisible to GET
- **WHEN** object is soft-deleted
- **THEN** GET returns NoSuchKey; row still exists in DB for GC

#### Scenario: GC cleans old soft-deleted rows
- **WHEN** retention period elapses
- **THEN** GC deletes the MetaStore row and calls `router.delete` on the orphaned blob
