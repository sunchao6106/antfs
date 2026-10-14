## 1. Crate 骨架与配置

- [x] 1.1 `crates/s3/Cargo.toml`（`antfs-s3`，bin `antfs-s3`）:依赖 `antfs-router`/`antfs-config`/`antfs-metric`/`antfs-blob`、`axum`/`tokio`/`bytes`/`hex`/`serde`/`serde_json`/`thiserror`/`tracing`/`tracing-subscriber`、`sqlx`(features: `runtime-tokio`/`tls-rustls`/`postgres`/`mysql`)、`quick-xml`/`hmac`/`sha2`/`subtle`/`hex`/`uuid`/`chrono`；加入根 `Cargo.toml` workspace members
- [x] 1.2 `config.rs`:`S3GatewayConfig`（listen_addr / max_body_bytes / inline_body_max / region）+ `MetaStoreConfig`（driver / url / max_connections）+ 内嵌 `RouterConfig` 子表 + `Default` + `Validate`
- [x] 1.3 `main.rs`：加载配置（`Loader::load_table("s3")`）→ 连 MetaStore → 连 Router → 起 HTTP 服务 + graceful shutdown
- [x] 1.4 `lib.rs`：公开 `config`/`server`/`metastore`/`error`/`metrics` 模块
- [x] 1.5 `metrics.rs`：`S3Metrics`（`s3_requests_total{op,status}`/`s3_request_duration_seconds{op}`/`s3_metastore_queries_total{op}`/`s3_metastore_errors_total`）

## 2. MetaStore trait 与类型

- [x] 2.1 `metastore/mod.rs`：`MetaStore` async trait（全部方法签名,见 design D1）+ 类型定义（`PutObjectMeta`/`ObjectMeta`/`BucketMeta`/`PartMeta`/`CompletePart`/`S3Credential`/`ListObjectsReq`/`ListObjectsResp`/`ListPartsOpts`/`ListPartsResp`/`ListUploadsOpts`/`ListUploadsResp`/`DeletedObject`/`DeleteResult`/`ObjectVersion`/`UserMeta`）
- [x] 2.2 `metastore/mod.rs`：`MetaStoreKind` enum（`Postgres`/`Mysql`）+ `connect(cfg: &MetaStoreConfig) -> Result<Arc<dyn MetaStore>>` 工厂函数

## 3. PostgreSQL MetaStore 实现

- [x] 3.1 `metastore/migrations/postgres/001_init.sql`：5 表 DDL + 索引（buckets / named_objects / multipart_uploads / multipart_parts / s3_credentials）
- [x] 3.2 `metastore/pg.rs`：`PgMetaStore { pool: PgPool }` + `MetaStore` trait 实现——object CRUD（`put_object` INSERT + version 生成 / `get_object` SELECT latest READY / `delete_object` soft-delete / `delete_objects` batch / `list_objects` prefix+delimiter+pagination）
- [x] 3.3 `metastore/pg.rs`：bucket CRUD（`create_bucket` INSERT / `delete_bucket` 检查空+DELETE / `head_bucket` / `list_buckets`）
- [x] 3.4 `metastore/pg.rs`：multipart（`create_multipart` INSERT+生成 upload_id / `put_part` UPSERT / `complete_multipart` 验证+合并+INSERT named_objects+DELETE session / `abort_multipart` DELETE+返回 blob_ids / `list_parts` / `list_multipart_uploads`）
- [x] 3.5 `metastore/pg.rs`：`lookup_credential` SELECT
- [x] 3.6 `metastore/pg.rs`：startup 时 `sqlx::migrate!` 自动建表
- [x] 3.7 单测：PG roundtrip（put→get→delete→get=None / overwrite 返回最新 / list prefix / list delimiter CommonPrefixes / multipart create→upload→complete→get / abort 返回 blob_ids / credential lookup / concurrent overwrite 版本排序 / soft-delete invisible / bucket CRUD）—— 用 `sqlx::test` 或 testcontainers

## 4. MySQL MetaStore 实现

- [x] 4.1 `metastore/migrations/mysql/001_init.sql`：同等 5 表 DDL（MySQL 语法:VARCHAR charset utf8mb4 / BIGINT / VARBINARY / ENGINE=InnoDB）
- [x] 4.2 `metastore/mysql.rs`：`MysqlMetaStore { pool: MySqlPool }` + `MetaStore` trait 实现（SQL 改为 MySQL 方言：`INSERT ... ON DUPLICATE KEY UPDATE` / `LIMIT` 语法 / `LIKE` + index hints）
- [x] 4.3 单测：MySQL roundtrip（与 PG 同等覆盖）

## 5. S3 错误码与 XML 序列化

- [x] 5.1 `error.rs`：`S3Error` enum（`NoSuchBucket`/`NoSuchKey`/`BucketAlreadyExists`/`BucketNotEmpty`/`InvalidBucketName`/`InvalidPart`/`NoSuchUpload`/`AccessDenied`/`SignatureDoesNotMatch`/`InvalidAccessKeyId`/`RequestTimeTooSkewed`/`InternalError`/`MalformedXML`/`EntityTooLarge`/`MissingContentLength`）→ HTTP status + S3 XML error body
- [x] 5.2 `xml.rs`：S3 XML 请求解析（`DeleteObjectsRequest`/`CompleteMultipartUploadRequest`）+ 响应序列化（`ListBucketResult`/`ListAllMyBucketsResult`/`InitiateMultipartUploadResult`/`CompleteMultipartUploadResult`/`ListPartsResult`/`ListMultipartUploadsResult`/`DeleteResult` XML）—— `quick-xml` + `serde`
- [x] 5.3 单测：XML roundtrip（序列化→反序列化一致 / 空列表 / 大量 CommonPrefixes / 特殊字符转义）

## 6. AWS Signature V4 鉴权

- [x] 6.1 `auth/sigv4.rs`：解析 `Authorization: AWS4-HMAC-SHA256 Credential=.../SignedHeaders=.../Signature=...`（提取 access_key / date / region / service / signed_headers / signature）
- [x] 6.2 `auth/sigv4.rs`：计算 canonical request（method / URI encode / query string 排序 / canonical headers / signed headers / payload hash）→ string-to-sign → signing key（HMAC 链:date→region→service→aws4_request）→ 预期 signature
- [x] 6.3 `auth/sigv4.rs`：presigned URL 解析（`X-Amz-Algorithm`/`X-Amz-Credential`/`X-Amz-Date`/`X-Amz-Expires`/`X-Amz-SignedHeaders`/`X-Amz-Signature` query params）+ 过期校验
- [x] 6.4 `auth/sigv4.rs`：`UNSIGNED-PAYLOAD` 支持（signed headers 含 `x-amz-content-sha256` 值为 `UNSIGNED-PAYLOAD` 时 payload hash = 该常量字符串）
- [x] 6.5 `auth/sigv4.rs`：时钟偏移校验（±15 分钟）+ constant-time 签名比对（`subtle::ConstantTimeEq`）
- [x] 6.6 `auth/middleware.rs`：axum middleware——从 request 提取 auth info（header 或 query）→ 从 MetaStore `lookup_credential` → 验证签名 → 注入 `AuthContext { owner }` 到 request extensions → 失败返回 S3 XML error
- [x] 6.7 单测：SigV4 计算正确性（AWS 官方测试向量 `https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html`）/ presigned 过期 / 时钟偏移拒绝 / 未知 access_key / 常量时间比对（计时不因字符位置变化）

## 7. S3 Handler — 核心对象操作

- [x] 7.1 `server.rs`：axum 路由分发（路径风格 `/<bucket>/<key...>` + bucket-only `/<bucket>` + root `/`）+ auth middleware 接入 + 按 HTTP method 和 query params 分派到各 handler
- [x] 7.2 `handlers/object.rs`：`put_object`——解析 Content-Type/x-amz-meta-*/Content-Length → 小 body `router.put_auto_with_ttl` / 大 body 暂存+`router.put_file_with_ttl` → `metastore.put_object` → 200 + ETag
- [x] 7.3 `handlers/object.rs`：`get_object`——`metastore.get_object` → multipart manifest? 逐片流式拼接 : `router.get` → 200 + headers + body stream
- [x] 7.4 `handlers/object.rs`：`head_object`——`metastore.get_object` → 200 + 元数据 headers,无 body
- [x] 7.5 `handlers/object.rs`：`delete_object`——`metastore.delete_object` → `router.delete` → 204
- [x] 7.6 `handlers/batch.rs`：`delete_objects`——解析 XML → 逐 key `metastore.delete_object` + `router.delete` → XML 响应

## 8. S3 Handler — Multipart

- [x] 8.1 `handlers/multipart.rs`：`create_multipart_upload`——`metastore.create_multipart` → XML `InitiateMultipartUploadResult`
- [x] 8.2 `handlers/multipart.rs`：`upload_part`——body → `router.put_auto_with_ttl` → `metastore.put_part` → 200 + ETag
- [x] 8.3 `handlers/multipart.rs`：`complete_multipart_upload`——解析 XML → `metastore.complete_multipart`（验证 ETags+构建 manifest）→ XML `CompleteMultipartUploadResult`
- [x] 8.4 `handlers/multipart.rs`：`abort_multipart_upload`——`metastore.abort_multipart` → 逐片 `router.delete` → 204
- [x] 8.5 `handlers/multipart.rs`：`list_parts`——`metastore.list_parts` → XML `ListPartsResult`
- [x] 8.6 `handlers/multipart.rs`：`list_multipart_uploads`——`metastore.list_multipart_uploads` → XML `ListMultipartUploadsResult`

## 9. S3 Handler — 桶管理与列表

- [x] 9.1 `handlers/bucket.rs`：`create_bucket`——`metastore.create_bucket` → 200
- [x] 9.2 `handlers/bucket.rs`：`delete_bucket`——`metastore.delete_bucket` → 204
- [x] 9.3 `handlers/bucket.rs`：`head_bucket`——`metastore.head_bucket` → 200 / 404
- [x] 9.4 `handlers/bucket.rs`：`list_buckets`——`metastore.list_buckets(owner)` → XML `ListAllMyBucketsResult`
- [x] 9.5 `handlers/bucket.rs`：`list_objects_v2`——`metastore.list_objects` → delimiter/CommonPrefixes 后处理 → XML `ListBucketResult`

## 10. S3 Handler — 凭证管理

- [x] 10.1 `handlers/admin.rs`：`POST /admin/credentials`（operator token 校验 → 生成 access_key/secret_key → INSERT s3_credentials → JSON 响应）
- [x] 10.2 `handlers/admin.rs`：`DELETE /admin/credentials/{access_key}`（operator token → DELETE → 204）

## 11. Multipart GetObject 流式拼接

- [x] 11.1 `handlers/object.rs`：multipart 对象的 GetObject——从 `ObjectMeta.parts_manifest`（JSON `[{blob_id, size}]`）依次 `router.get` 每片 → `axum::body::Body::from_stream` 拼接流式返回,Content-Length = Σ size

## 12. devcluster 集成

- [x] 12.1 `crates/devcluster`：新增 `--s3-port`（默认 7500）+ `--s3-metastore-url`（默认空 = 不起 S3 网关）flag
- [x] 12.2 `crates/devcluster`：S3 网关启动逻辑（连 metastore → 连 router → serve）+ summary 打印 S3 端点

## 13. 集成测试

- [x] 13.1 `tests/object.rs`：PutObject→GetObject roundtrip（小 body + 大 body）/ HeadObject / DeleteObject → 404 / 覆盖写返回最新 / ETag 正确（MD5 hex）
- [x] 13.2 `tests/multipart.rs`：CreateMultipart→UploadPart×3→CompleteMultipart→GetObject 拼接一致 / AbortMultipart→ListParts 空 / ETag 不匹配 400 / ListMultipartUploads prefix
- [x] 13.3 `tests/list.rs`：ListObjectsV2 prefix / delimiter CommonPrefixes / pagination / empty bucket / CreateBucket / DeleteBucket 空+非空 / HeadBucket / ListBuckets
- [x] 13.4 `tests/auth.rs`：有效签名 → 200 / 错误签名 → 403 / 未知 access_key → 403 / presigned URL 成功+过期 / UNSIGNED-PAYLOAD / 凭证 CRUD
- [x] 13.5 `cargo test -p antfs-s3` 全绿 + `cargo clippy -p antfs-s3 --all-targets` 无告警

## 14. 文档

- [x] 14.1 `docs/design/s3.md`：架构图 + S3 操作映射表 + MetaStore schema + 配置项清单 + 监控项清单 + 与 Ambry/SeaweedFS/CubeFS 的对比
- [x] 14.2 `docs/design/README.md`：加 S3 网关行（进行中）
- [x] 14.3 `README.md`：组件表加 S3 gateway 行
- [x] 14.4 openspec change 档案（本文件勾选）

## 15. 后续（不在本 change）

- [ ] S3 版本控制(Versioning) / Object Lock / SSE 加密
- [ ] CopyObject / GetObjectAttributes
- [ ] 虚拟主机风格路由（`<bucket>.s3.endpoint`）
- [ ] 多租户配额与计费
- [ ] `aws-chunked` 流式签名（Transfer-Encoding: aws-chunked）
- [ ] 后台 GC 清理过期 soft-delete + 孤儿 multipart
- [ ] 外部凭证源（LDAP / OIDC federation）
