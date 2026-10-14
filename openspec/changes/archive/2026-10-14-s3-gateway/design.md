## Context

antfs 是 Ambry 的 Rust 移植,当前只有 REST 原生接口(`/blobs`,blobId 直寻址)。S3 协议要求 **命名空间(bucket + key)**——这与 antfs 的 D3("blobId 自带 partition 定位,无中心每对象元数据")在 blob 层成立但在网关层必须破例:三家参考系统(SeaweedFS / CubeFS / Ambry)无一例外引入了独立的 name→blob 映射存储。

调研结论:Ambry 的模式(外部 MySQL `named_blobs_v2` 表,版本化 PK,两阶段写)最对齐 antfs 架构。antfs 扩展为同时支持 PostgreSQL 和 MySQL,网关本身无状态。

现有可复用组件:
- `antfs-router`:号段缓存 + W 多数派写 + R=1 读。`put_auto`/`put_auto_with_ttl`(内部发号 + 507 换分区重试)、`put_file_with_ttl`(大文件流式)、`get`、`delete`。
- `antfs-frontend`:同层参考(axum + Router SDK + Prometheus + CORS + body 分档)。
- `antfs-config`/`antfs-metric`:配置加载与指标注册。

## Goals / Non-Goals

**Goals:**
- S3 核心对象操作:PutObject / GetObject / DeleteObject / HeadObject / DeleteObjects(批删)。
- S3 多段上传:CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads。
- S3 列表与桶管理:ListObjectsV2 / CreateBucket / DeleteBucket / HeadBucket / ListBuckets。
- AWS Signature V4 鉴权:Header 签名 / Presigned URL / `aws-chunked` 流式签名。
- MetaStore trait + PostgreSQL 实现 + MySQL 实现,schema 可迁移(`sqlx migrate`)。
- 网关无状态,可 N 实例水平扩容。
- devcluster 集成(`--s3-port`)。
- Prometheus 指标 + `GET /metrics`。

**Non-Goals:**
- S3 版本控制(Versioning)/ Object Lock / SSE 加密 / Bucket Policy 引擎 / 跨区域复制。
- 多租户配额与计费。
- S3 Select / Glacier 层。
- 替代现有 REST frontend(两者并存)。
- 在 master Raft 里做 name mapping(选了外部 DB 方案)。

## Decisions

### D1: 元数据后端 — 外接 PostgreSQL / MySQL,trait 抽象

**选定**:MetaStore trait + `sqlx`(编译期 SQL 检查 + async + 同时支持 PG/MySQL)双后端。

**否决 A(Master Raft)**:master 是 Raft 写路径瓶颈,加每对象元数据写入量上 master 不合适;且 ListObjects 需要前缀查询,Raft KV 不擅长。
**否决 B(SQLite 嵌入)**:单实例绑定,S3 网关无法水平扩容。
**否决 C(独立 filer Raft)**:工程量过大,Phase 1 不必要。

**trait 设计**:
```rust
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    // Object CRUD
    async fn put_object(&self, req: PutObjectMeta) -> Result<ObjectVersion>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<DeletedObject>>;
    async fn delete_objects(&self, bucket: &str, keys: &[&str]) -> Result<Vec<DeleteResult>>;
    async fn list_objects(&self, req: ListObjectsReq) -> Result<ListObjectsResp>;

    // Bucket CRUD
    async fn create_bucket(&self, name: &str, owner: &str) -> Result<()>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn head_bucket(&self, name: &str) -> Result<Option<BucketMeta>>;
    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>>;

    // Multipart
    async fn create_multipart(&self, bucket: &str, key: &str, meta: &UserMeta) -> Result<String>;
    async fn put_part(&self, upload_id: &str, part: PartMeta) -> Result<()>;
    async fn complete_multipart(&self, upload_id: &str, parts: &[CompletePart]) -> Result<ObjectMeta>;
    async fn abort_multipart(&self, upload_id: &str) -> Result<Vec<BlobId>>;
    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> Result<ListPartsResp>;
    async fn list_multipart_uploads(&self, bucket: &str, opts: ListUploadsOpts) -> Result<ListUploadsResp>;

    // Auth
    async fn lookup_credential(&self, access_key: &str) -> Result<Option<S3Credential>>;
}
```

### D2: Schema 设计(Ambry 启发,5 表)

```sql
-- buckets
CREATE TABLE buckets (
    name        VARCHAR(63)  NOT NULL PRIMARY KEY,
    owner       VARCHAR(128) NOT NULL,
    created_at  BIGINT       NOT NULL
);

-- named_objects: 版本化 PK,支持覆盖写
CREATE TABLE named_objects (
    bucket       VARCHAR(63)  NOT NULL,
    key          VARCHAR(1024) NOT NULL,
    version      BIGINT       NOT NULL,
    blob_id      BINARY(8)    NOT NULL,
    state        SMALLINT     NOT NULL DEFAULT 1,  -- 0=IN_PROGRESS, 1=READY
    size         BIGINT       NOT NULL,
    etag         VARCHAR(128),
    content_type VARCHAR(256),
    deleted_at   BIGINT,                          -- epoch_ms, NULL=live
    created_at   BIGINT       NOT NULL,
    user_meta    TEXT,                             -- JSON map for x-amz-meta-*
    PRIMARY KEY (bucket, key, version)
);
CREATE INDEX idx_no_list ON named_objects(bucket, key, state, deleted_at);

-- multipart_uploads
CREATE TABLE multipart_uploads (
    upload_id    VARCHAR(64)  NOT NULL PRIMARY KEY,
    bucket       VARCHAR(63)  NOT NULL,
    key          VARCHAR(1024) NOT NULL,
    created_at   BIGINT       NOT NULL,
    content_type VARCHAR(256),
    user_meta    TEXT
);
CREATE INDEX idx_mu_bucket ON multipart_uploads(bucket, key);

-- multipart_parts
CREATE TABLE multipart_parts (
    upload_id    VARCHAR(64)  NOT NULL,
    part_number  INT          NOT NULL,
    blob_id      BINARY(8)    NOT NULL,
    size         BIGINT       NOT NULL,
    etag         VARCHAR(128) NOT NULL,
    PRIMARY KEY (upload_id, part_number)
);

-- s3_credentials
CREATE TABLE s3_credentials (
    access_key   VARCHAR(128) NOT NULL PRIMARY KEY,
    secret_key   VARCHAR(256) NOT NULL,
    owner        VARCHAR(128) NOT NULL,
    created_at   BIGINT       NOT NULL
);
```

版本号 = `now_ns / 1000 + random % 1000`(Ambry 同理),GET 取 `WHERE state=READY AND deleted_at IS NULL ORDER BY version DESC LIMIT 1`。

### D3: S3 操作 → Router SDK 映射

| S3 操作 | MetaStore | Router SDK | 说明 |
|---|---|---|---|
| PutObject(≤ body 阈值) | `put_object` → INSERT | `put_auto_with_ttl` | 内存 body,内部发号+507 重试 |
| PutObject(> body 阈值) | `put_object` → INSERT | `put_file_with_ttl` | 落暂存文件流式,同有 507 重试 |
| GetObject | `get_object` → SELECT | `router.get(&blob_id)` | R=1 |
| HeadObject | `get_object` → SELECT | 只返回元数据,不读 blob | |
| DeleteObject | `delete_object` → soft-delete | `router.delete(&blob_id)` | 先标记后删数据 |
| DeleteObjects | `delete_objects` → batch soft-delete | 逐个 `router.delete` | |
| ListObjectsV2 | `list_objects` → SELECT prefix% | 无 Router 调用 | 纯元数据查询 |
| CreateMultipartUpload | `create_multipart` → INSERT | 无 | 生成 upload_id |
| UploadPart | `put_part` → INSERT/REPLACE | `put_auto_with_ttl` | 每片独立 blob |
| CompleteMultipartUpload | `complete_multipart` → 合并 | 可选 stitch,或直接记最终 blob_id | 见 D4 |
| AbortMultipartUpload | `abort_multipart` → DELETE | 逐片 `router.delete` (或靠 TTL) | |

### D4: Multipart 完成策略

**选定:元数据拼接(不用 stitch)**。CompleteMultipartUpload 时在 `named_objects` 里插一行,`blob_id` 指向一个"描述符"概念——但实际做法更简单:**不做数据拼接**,用一个 JSON 字段 `parts_manifest` 记住各片 blob_id + offset + size;GetObject 时流式依次从各片 blob 读出拼接返回。

否决方案:用 frontend 的 stitch 机制——它会把描述符当 blob 存,引入 4 字节 MAGIC 嗅探歧义问题(已知 bug,见 `blob-io-end-to-end` 待议项)。

### D5: SigV4 鉴权

**选定:自实现核心 SigV4**。`aws-sigv4` crate 是签名**生成**库(客户端用),不是验证库;验证侧需要自己解析 Authorization header → canonical request → 查凭证 → 比对签名。参考 SeaweedFS `auth_signature_v4.go` 的实现(15 分钟时钟偏移、presigned URL 过期、signed headers 覆盖检查)。

凭证存 MetaStore 的 `s3_credentials` 表。网关启动时不缓存全量凭证——每次请求查一次(带连接池,PG/MySQL 单次查询 < 1ms)。

### D6: 网关 crate 结构

```
crates/s3/
├── Cargo.toml
├── src/
│   ├── main.rs            # 二进制入口
│   ├── lib.rs
│   ├── config.rs          # S3GatewayConfig
│   ├── server.rs          # axum 路由 + handler 分发
│   ├── auth/
│   │   ├── mod.rs
│   │   ├── sigv4.rs       # SigV4 解析与验证
│   │   └── middleware.rs  # axum auth 中间件
│   ├── handlers/
│   │   ├── mod.rs
│   │   ├── object.rs      # PutObject/GetObject/DeleteObject/HeadObject
│   │   ├── multipart.rs   # Create/Upload/Complete/Abort/ListParts/ListUploads
│   │   ├── bucket.rs      # CreateBucket/DeleteBucket/HeadBucket/ListBuckets
│   │   └── batch.rs       # DeleteObjects
│   ├── metastore/
│   │   ├── mod.rs         # MetaStore trait + types
│   │   ├── pg.rs          # PostgreSQL 实现
│   │   ├── mysql.rs       # MySQL 实现
│   │   └── migrations/    # sqlx migrations (PG + MySQL)
│   ├── xml.rs             # S3 XML 请求/响应序列化
│   ├── metrics.rs         # Prometheus 指标
│   └── error.rs           # S3 错误码映射
└── tests/
    ├── object.rs
    ├── multipart.rs
    ├── list.rs
    └── auth.rs
```

### D7: ListObjectsV2 实现

SQL 查询:
```sql
SELECT key, version, blob_id, size, etag, content_type, created_at, user_meta
FROM named_objects
WHERE bucket = ? AND key > ? AND key LIKE ?
  AND state = 1 AND deleted_at IS NULL
ORDER BY key ASC
LIMIT ?
```
- `key > ?` = continuation token(上一页最后一个 key)。
- `key LIKE ?` = prefix%(`text_pattern_ops` 索引在 PG 上高效)。
- delimiter / CommonPrefixes 在网关侧后处理(与 SeaweedFS / CubeFS 同做法)。
- 返回 `maxKeys + 1` 行,多出的那行的 key 作为 NextContinuationToken。

### D8: 配置

```toml
[s3]
listen_addr = "0.0.0.0:7500"
max_body_bytes = 5368709120  # 5 GiB (S3 单对象上限)
inline_body_max = 1048576    # 1 MiB,以下走内存
region = "us-east-1"

[s3.metastore]
driver = "postgres"          # "postgres" | "mysql"
url = "postgres://user:pass@localhost:5432/antfs_s3"
max_connections = 20

[s3.router]
master_addr = "127.0.0.1:7101"
cluster_secret = "..."
```

## Risks / Trade-offs

| 风险 | 影响 | 缓解 |
|---|---|---|
| 外部 DB 成为可用性依赖 | S3 网关全挂 | PG 主从 / MySQL Group Replication;网关侧连接池 + 优雅降级 |
| 名字映射与 blob 数据不一致(网关崩溃时) | 孤儿 blob 或悬空映射 | PutObject:先写 blob 再写映射(失败时 blob 靠 TTL 或 GC 清理);DeleteObject:先标记映射再删 blob |
| ListObjects 大前缀扫描慢 | 高延迟 | 索引 `(bucket, key)` + LIMIT 分页;大桶建议用 delimiter 分层 |
| Multipart 孤儿(Abort 失败或客户端放弃) | 片 blob 泄漏 | 后台 GC 任务扫描 `created_at < now - 7d` 的未完成 upload,自动 abort |
| SigV4 自实现的正确性 | 兼容性问题 | 用 AWS SDK 的集成测试覆盖;参考 SeaweedFS 已验证的实现 |
| D3 破例(网关层引入每对象元数据) | 架构纯度 | 映射层严格只在 S3 网关,blob 层不感知;REST frontend 不受影响 |

## Open Questions

1. **Multipart 片的 TTL**:UploadPart 写的片 blob 是否带短 TTL(7 天)?好处是放弃的 multipart 片自动过期;代价是 Complete 后需要把片的 TTL 改为永久(当前 antfs 没有 update_ttl 接口)。**倾向:不带 TTL,靠后台 GC abort 过期 upload**。
2. **CopyObject**:首批不做还是做?不做的话 `aws s3 cp` 跨桶复制不可用。**倾向:首批不做,后续 change 加**。
3. **虚拟主机风格 vs 路径风格**:S3 新版 SDK 默认虚拟主机(`<bucket>.s3.region.amazonaws.com`)。antfs 自部署场景路径风格(`/bucket/key`)更实际。**倾向:首批只做路径风格,留接口后续加虚拟主机**。
