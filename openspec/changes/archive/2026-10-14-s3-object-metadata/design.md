## Context

A1 裁决为乙路线(仅 S3、MetaStore、与 blob 隔离;见 `phase1-exit-closeout/design.md` 决策 2)。复核证实实现链完整:`PutObjectMeta`/`ObjectMeta` 的 `content_type`/`user_meta` 字段、`001_init.sql` 的列、`pg.rs`/`mysql.rs` 的 INSERT/SELECT、`object.rs` put 采集(18-28 行)与 get/head 回吐(96-148 行)、`complete_multipart` 从 `multipart_uploads` 取回 meta 传 `PutObjectMeta`。缺口是三处边界 + 零测试 + 文档未写行为。本 change 是「证据与边界收口」,不是功能开发。

## Goals / Non-Goals

**Goals:**
- 边界:meta 总量 2 KiB 上限(400 MetadataTooLarge)、Content-Type 超列宽 400(不再 500)、无 Content-Type 存储默认 `binary/octet-stream`。
- 证据:PG-gated 集成测试覆盖 meta 采集/回吐/传递/边界(现 tests/ 中 metadata 断言为零)。
- 文档:s3.md 操作映射补「对象元数据」段(采集/回吐/上限/默认/旧行差异);DEVLOG 43。

**Non-Goals:**
- 引擎/record 元数据、REST 层元数据(甲路线,未裁决触发)。
- MySQL 方言测试(既有边界:仅 PG gated)。
- ListObjectsV2 metadata(S3 XML 语义不含)。

## Decisions

### 1. 上限值取常量,不进配置

- `MAX_USER_META_BYTES = 2048`(S3 语义官方值,全生态一致期望)与 `MAX_CONTENT_TYPE_BYTES = 256`(对齐既有 `VARCHAR(256)` 列宽)以具名常量放 `handlers/object.rs`,加注释;**不**引入 `s3.*` 配置键(S3 语义边界不是部署参数,CLAUDE.md「不做投机可配置性」)。
- 备选(否决):加长 content_type 列(要写双方言迁移,为一个错误输入场景不值;400 拒绝即可)。

### 2. 校验位置:handler 采集段,一次性完成

- put/multipart-create 的 header 采集循环后立刻校验(meta Σ(key.len+value.len)、content_type 头长度),失败直接 `S3Error::MetadataTooLarge`/`InvalidArgument` —— xml.rs 既有错误→XML 映射补两个 code(400)。边界校验属「系统边界处校验」(CLAUDE.md §2),不进 MetaStore 层(trait 是存储抽象,不懂 S3 语义)。

### 3. 存储默认 `binary/octet-stream`,而不是读取时兜底

- put 侧 None → 存字符串默认(AWS 语义:上传时就定了类型);读侧 NULL(旧行)维持不吐头。**写新默认、读旧行不动**——避免代码里出现「两个真源」;旧行差异文档记档。
- 备选(否决):读侧 fallback——会让「NULL 行」与「默认行」在读路径表现一致但存储不同,新旧行为暗中分叉,比明示差异更难排查。

### 4. 测试按既有 PG-gated 套路

- 沿用 `#[ignore]` + `TEST_S3_POSTGRES_URL` 基建(本地 PG 在);\_\_\_ 复用现有测试 fixture/helper,不新建 harness。multipart 传递用例走 create 带 meta → upload part → complete → get 断言回吐

## Risks / Trade-offs

- **[PUT 无 Content-Type 的 GET 行为变化]** → 从「无头」到「binary/octet-stream」;aws cli/s3 测试套件按后者预期,风险低;旧行(NULL)行为不变,文档明示差异。
- **[2 KiB 措辞]** → S3 官方文档口径是「user metadata 最大 2 KB」;按 UTF-8 字节数(键+值)计,实现处注释写清口径,防歧义。
- **[multipart create 的 meta 校验]** → create 时校验(请求头在 create 携带),complete 不再校验(meta 已存库,非当次输入)。

## Migration Plan

无 schema 迁移。部署顺序任意(纯 handler 侧);回滚 = revert。

## Open Questions

无。
