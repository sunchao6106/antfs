## Why

A1 裁决(见 `phase1-exit-closeout`):对象元数据只做 S3、基于 MetaStore、与底层 blob 引擎隔离。裁决后复核发现**实现链路已全部存在**——schema 列(`named_objects.content_type/user_meta`、`multipart_uploads` 同名)、双方言 INSERT/SELECT、put 侧 `Content-Type`/`x-amz-meta-*` 采集、get/head 回吐、multipart create→complete 传递。但这条链**零测试覆盖**(`crates/s3/tests/` 无任何 metadata 断言),且有三处边界行为缺失或错误。本 change 不新增特性,只把既有实现**补上证据、修齐边界、写进文档**,让乙路线可宣布「已落地且被验证」。本 change 同时是 Phase 1 收官矩阵的最后一格。

## What Changes

- **边界行为补齐(put 侧)**:
  1. `x-amz-meta-*` 总量(键+值 UTF-8 字节数)超 **2048** → S3 语义 `MetadataTooLarge`(400);现状是无校验直接落库。
  2. `Content-Type` 头长度超 `VARCHAR(256)` 列宽 → `InvalidArgument`(400);现状是 INSERT 失败打 **500 InternalError**(契约级 bug)。
  3. put 不带 `Content-Type` 时**存储默认值 `binary/octet-stream`**(AWS 同语义);现状存 NULL → get 不回吐 Content-Type 头,客户端只能猜。
- **测试补齐(PG-gated 集成,既有 `#[ignore]` 套路)**:put→get/head 的 `Content-Type`/`x-amz-meta-*` 往返、multipart create 带 meta → complete 后 get 回吐、无 Content-Type put 的默认回吐、超限 400 ×2。
- **文档**:`docs/design/s3.md` 操作映射段补「对象元数据」行为(采集/回吐/上限/默认/旧行差异);DEVLOG 步骤 43。
- **不改**:引擎层、REST/frontend、MetaStore schema(列已存在,无迁移)、handler 语义主干。

## Capabilities

### New Capabilities
<!-- 无:全部是对既有能力的行为补齐。 -->

### Modified Capabilities
- `s3-core`:PutObject 增加元数据边界校验(`MetadataTooLarge`/`InvalidArgument`/存储默认 content-type);GetObject/HeadObject 的回吐行为从「spec 声称」落为「被测试证实」。
- `s3-multipart`:CreateMultipartUpload 携带的 `content_type`/`user_meta` 经 Complete 落入 named_object 行并回吐 —— 增加边界校验与测试场景。

## Impact

- **crate**:`crates/s3`(handlers/object.rs, multipart.rs 采集段加校验+默认;tests/object.rs、tests/multipart.rs 增用例)。
- **契约**:新增两个 400 错误形态(`MetadataTooLarge`、`InvalidArgument`);PUT 无 Content-Type 的 GET 行为从「无头」变「`binary/octet-stream`」——对外更符合 S3 语义,严格意义上是行为变化,记入档案。
- **兼容性**:旧对象行 `content_type` 为 NULL → GET 维持不吐头(与新写入行为不同但无害,文档记差异)。无 schema 迁移(列已存在);MySQL 方言测试维持既有边界(仅 PG gated,沿用现状)。
- **不涉**:MetaStore trait 签名、表结构、blob 引擎、REST 前端。

## Non-goals

- 不做磁盘 record 层元数据(乙路线已定;将来若升甲,另立提案)。
- 不做 metadata 的管理界面/ListObjectsV2 返回 metadata(S3 XML 语义本就不含)。
- 不补 MySQL 方言的集成测试(既有验证边界:仅 PG gated)。
