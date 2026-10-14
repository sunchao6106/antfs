## Why

antfs 目前只有 REST 原生接口（`/blobs`），不兼容 S3 协议——AWS SDK、MinIO Client、Terraform 等标准工具无法直接对接。S3 是对象存储的事实标准协议，缺少它严重限制了 antfs 的采用面。三家参考系统（SeaweedFS / CubeFS / Ambry）都在 blob 层之上加了一层"命名空间 + S3 协议翻译"，验证了这条路径可行。

## What Changes

- **新增 `crates/s3` crate**（二进制 `antfs-s3`）：无状态 S3 网关，axum HTTP 服务，内嵌 Router SDK 做 blob I/O。
- **新增 MetaStore trait + PostgreSQL / MySQL 双后端实现**（`sqlx`）：存储 bucket/key → blobId 映射、multipart 会话、S3 凭证。网关本身无持久状态，可水平扩容。
- **新增 AWS Signature V4 鉴权中间件**：解析 Authorization / 预签名 URL，从 MetaStore 查凭证，校验签名。
- **新增 S3 XML 请求/响应序列化**：`quick-xml` + `serde`。
- **devcluster 集成**：`--s3-port`（默认 7500）一并起 S3 网关（需外部 PG/MySQL 或 `--s3-metastore=none` 跳过）。
- **文档**：`docs/design/s3.md`，配置项 / 监控项清单。

**不做（Non-goals）**：
- S3 版本控制（Versioning）/ Object Lock / SSE 加密 / ACL Policy 引擎 / 跨域复制 / S3 Select / Glacier 层。
- 多租户配额（后续 change）。
- 替代现有 REST frontend（两者并存,同层）。

## Capabilities

### New Capabilities

- `s3-core`: S3 核心对象操作（PutObject / GetObject / DeleteObject / HeadObject / DeleteObjects 批删）。
- `s3-multipart`: S3 多段上传（CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads）。
- `s3-list`: S3 列表与桶管理（ListObjectsV2 / CreateBucket / DeleteBucket / HeadBucket / ListBuckets）。
- `s3-auth`: AWS Signature V4 鉴权（Header / Presigned URL / Chunked）+ 凭证管理。
- `s3-metastore`: 名字映射元数据层（MetaStore trait + PostgreSQL 实现 + MySQL 实现 + Schema 迁移）。

### Modified Capabilities

（无——S3 网关是新增层,不改变现有 blob/store/master/router/frontend 的行为。）

## Impact

- **新 crate**：`crates/s3`（依赖 `antfs-router`、`antfs-config`、`antfs-metric`、`sqlx`、`axum`、`quick-xml`、`aws-sigv4` 或自实现）。
- **新外部依赖**：运行时需要 PostgreSQL 或 MySQL 实例（元数据存储）。
- **devcluster**：`crates/devcluster` 新增 `--s3-port` flag + 可选 S3 网关启动。
- **workspace Cargo.toml**：新增 `crates/s3` member。
- **CI**：需加 S3 网关的 build + test。集成测试需要 PG/MySQL 可用（可用 testcontainers 或 `sqlx::test`）。
- **文档**：`docs/design/s3.md`（新）+ `docs/design/README.md`（加行）+ `README.md`（组件表加行）。
