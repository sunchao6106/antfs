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

### Requirement: Quota accounting invariant
The MetaStore SHALL maintain `buckets.used_bytes = Σ size of the bucket's state=1 rows` transactionally: the write guard (single-statement conditional UPDATE on `buckets`) and size adjustments for insert/shadow/delete run in the same transaction as the row changes they accompany. GC purges state=0 rows and SHALL NOT touch used_bytes.

#### Scenario: Guard is statement-atomic
- **WHEN** two concurrent PUTs race the last 4 KiB of a quota
- **THEN** exactly one passes the single-statement guard; the other gets QuotaExceeded (no oversell)

#### Scenario: Shadow and delete subtract in-transaction
- **WHEN** put shadows an old live row or delete tombstones one
- **THEN** used_bytes is decremented by that row's size in the same transaction

### Requirement: Object checksum persistence
The MetaStore SHALL persist an optional flexible checksum with each `named_objects` row via migration `005_add_object_checksum` (PostgreSQL and MySQL): nullable columns `checksum_algo VARCHAR(8)` and `checksum_b64 VARCHAR(64)`, storing at most one algorithm name (`CRC32`/`CRC32C`/`SHA1`/`SHA256`) and its Base64 value over the object's plaintext. Existing rows, multipart-assembled rows, and copy products carry NULL (no backfill). `put_object` SHALL insert the provided pair; `get_object`/`head_object` readers SHALL surface the pair on `ObjectMeta` when present. NULL columns SHALL NOT affect any existing query, listing, pagination, ordering, or quota behavior.

#### Scenario: Migration is additive and idempotent
- **WHEN** a gateway with existing data starts against either dialect
- **THEN** migration 005 adds the two nullable columns; all pre-existing rows read back with no checksum; no other behavior changes

#### Scenario: Put and read round-trip the checksum
- **WHEN** a PutObject uploads with `x-amz-checksum-sha256` and the object is read back
- **THEN** `get_object` returns the pair (`SHA256`, Base64 value) on the metadata struct

#### Scenario: Overwrite replaces the checksum
- **WHEN** a key with a stored CRC32 checksum is overwritten without any checksum header
- **THEN** the latest row carries no checksum (echo surfaces nothing)

### Requirement: Reclaim WAL records blobs pending tombstone
Every semantic action whose commit removes or replaces a blob's last referencing row SHALL record that blob id into a `pending_reclaims` table **inside the same transaction** (migration `006_add_pending_reclaims`, PostgreSQL and MySQL; `blob_id` primary key, `queued_at` for observability only; inserts idempotent on conflict). Recorded actions: PutObject overwrite shadowing (including the nested put of CompleteMultipartUpload), DeleteObject soft-delete, UploadPart retransmit replacement, CompleteMultipartUpload discarded (unlisted) parts, AbortMultipartUpload all parts. An insert failure SHALL abort the surrounding semantic transaction — a committed action without its WAL rows SHALL NOT exist. The WAL SHALL expose read-and-resolve primitives: a bounded batch read (ascending by nothing in particular, non-destructive) and a `resolve` deletion by blob ids used only after a successful engine tombstone; rows SHALL only ever disappear via resolve. The table is expected to be empty in steady state (handlers resolve immediately after their same-request tombstones).

#### Scenario: Overwrite queues the shadowed blob transactionally
- **WHEN** a PutObject overwrite shadows a previous version
- **THEN** the shadowed blob id is inserted into `pending_reclaims` in the same committed transaction

#### Scenario: Retransmit queues the replaced part blob
- **WHEN** an UploadPart re-upload replaces a part row
- **THEN** the replaced blob id is queued in the same transaction

#### Scenario: Abort and complete-discard queue their blobs
- **WHEN** an abort deletes parts rows, or a complete discards unlisted parts
- **THEN** each affected blob id is queued in the same transaction

#### Scenario: WAL insert failure rolls the action back
- **WHEN** the WAL insert fails
- **THEN** the semantic transaction aborts; no commit without the WAL record

#### Scenario: Steady state is an empty table
- **WHEN** handlers complete their same-request tombstones successfully
- **THEN** each resolved blob's WAL row is deleted and the table holds nothing

#### Scenario: Duplicated queue attempts deduplicate
- **WHEN** the same blob id is queued twice
- **THEN** a single WAL row exists (primary-key idempotent insert)

### Requirement: SSE row scan for offline key rotation

The MetaStore trait SHALL expose `scan_sse_objects(after, limit)` returning full `ObjectMeta` rows for live encrypted objects: `state = 1`, `deleted_at IS NULL`, `sse IS NOT NULL`, ordered by `(bucket, key)` ascending, with an exclusive cursor `after: Option<(bucket, key)>` and a bounded `limit`. The scan SHALL be co-transactional-free read-only (no row state is modified). Pagination SHALL be stable under in-place row rewrites at or behind the cursor: a rewritten row (same bucket/key, higher version) is never re-returned by later pages, because the exclusive `(bucket, key) > after` comparison cannot match its key pair again. Both PostgreSQL and MySQL implementations SHALL be behaviorally identical (same ordering, filtering, and cursor semantics).

#### Scenario: bounded pages with an exclusive cursor

- **WHEN** the store holds more than `limit` live sse rows and the caller pages with the last row of each page as the cursor
- **THEN** every live sse row is returned exactly once, in `(bucket, key)` order, and non-sse or soft-deleted rows are never returned

#### Scenario: rewriting a scanned row does not loop pagination

- **WHEN** a row at or behind the cursor is superseded in place (same bucket/key, new version, old row shadowed) by the tool that is iterating the scan
- **THEN** subsequent pages do not return that key pair again (no phantom loop)

### Requirement: Part checksum persistence and object checksum kind (migration 007)

Migration `007_add_part_checksum.sql` (PostgreSQL and MySQL, idempotent) SHALL extend the checksum persistence contract: `multipart_parts` gains `part_checksum_algo VARCHAR(8) NULL` + `part_checksum_b64 VARCHAR(64) NULL` holding the verified part-level checksum (the strongest claim when several algorithms were declared at UploadPart, mirroring object-row semantics); `named_objects` gains `checksum_type VARCHAR(12) NULL` (`FULL_OBJECT` | `COMPOSITE`) and `checksum_b64` is widened to TEXT (composite values reach ~53 KB at the 10 000-part cap). NULL `checksum_type` SHALL read as `FULL_OBJECT` (pre-007 rows) so `StoredChecksum` reconstruction stays fully backward compatible. Read helpers (`from_pair` family) SHALL treat half-written column sets as absent with a warning, exactly like the migration-005 contract. Both dialect implementations SHALL be behaviorally identical, including migration ordering after `006_add_pending_reclaims`.

#### Scenario: Old rows read as FULL_OBJECT after migration

- **WHEN** a row written before migration 007 has `checksum_algo`/`checksum_b64` set and `checksum_type` NULL
- **THEN** the reconstructed `StoredChecksum` reports kind FULL_OBJECT and echoes exactly as before

#### Scenario: Part rows round-trip the strongest claim

- **WHEN** an UploadPart declares both crc32 and sha256 (both verified)
- **THEN** the persisted part row carries the sha256 pair (strongest-one-wins, same ranking as object rows)

### Requirement: bucket policy persistence (migration 008)

Migration `008_add_bucket_policy.sql` (both dialects, idempotent on PostgreSQL) SHALL add `buckets.policy TEXT NULL` holding the validated policy JSON document (absent = NULL). `BucketMeta` SHALL gain a `policy: Option<String>` field populated by `head_bucket` (other BucketMeta readers such as `list_buckets` may leave it unset). A `set_bucket_policy(bucket, policy: Option<&str>)` metastore method SHALL upsert the column (None clears it), update the row only when the bucket exists (NoSuchBucket otherwise), and be safe to retry idempotently.

#### Scenario: policy round-trips through head_bucket

- **WHEN** `set_bucket_policy("b", Some(doc))` commits and `head_bucket("b")` runs
- **THEN** the returned `BucketMeta.policy` equals the stored document verbatim

#### Scenario: clearing is an upsert, not a delete of the row

- **WHEN** `set_bucket_policy("b", None)` runs for a bucket with a stored policy
- **THEN** the bucket row remains (name/owner/acl untouched) with `policy` NULL, and re-setting a document afterwards persists again

### Requirement: 网关每请求双查缓存 TTL 装饰器
S3 网关 SHALL 经 `CachingMetaStore` 装饰器缓存每请求两条热路径 —— `lookup_credential`(SigV4 验签)与 `head_bucket`(授权门):正条目 SHALL 按 `s3.metastore.meta_cache_ttl_secs`(默认 30)缓存,负条目(查无)SHALL 按 ≤5s 缓存;`meta_cache_ttl_secs = 0` SHALL 不装配缓存(原对象直返,零行为变化)。本进程的凭证增删与桶元数据写(create/delete bucket、set_bucket_quota/acl/policy/acl_grants、create/delete credential)SHALL 在调用后即时失效对应缓存条目(同实例管理面改立即可见);多实例共享同一 metastore 时变更 SHALL 在 ≤ TTL 内收敛。对象面(put/get/list/multipart)与错误 SHALL 一律穿透不缓存;缓存 SHALL 以 `s3_meta_cache_lookups_total{kind, result}` 观测(hit/miss/error)。

#### Scenario: 每请求双查命中缓存
- **WHEN** 同一 access_key 连续验签两次而期间无凭证变更
- **THEN** 仅第一次打 metastore,第二次由缓存供给,指标记 hit

#### Scenario: 本进程删除凭证立即生效
- **WHEN** 管理面删除某 access_key 后数据面立即以该 key 验签
- **THEN** 查询穿透并见到删除后的真相(403),不再命中旧正条目

#### Scenario: 负条目短 TTL 防挡新凭证
- **WHEN** 某 access_key 先被负缓存(查无)随后同进程创建
- **THEN** 创建成功即摘除负条目,新凭证立即可用(不被 5s 负缓存挡)

#### Scenario: ttl=0 零行为逃生口
- **WHEN** 配置 `s3.metastore.meta_cache_ttl_secs = 0`
- **THEN** 装饰器不装配,所有请求逐次打 metastore,行为与无该特性逐项一致

#### Scenario: 对象面不缓存
- **WHEN** 任意对象操作(put/get/list/multipart)执行
- **THEN** 每次穿透到 metastore,S3 读写一致性不受 TTL 影响

