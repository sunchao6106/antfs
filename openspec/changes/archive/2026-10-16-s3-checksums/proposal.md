# s3-checksums —— flexible checksums:PUT/UploadPart 校验 + trailer 承接 + GET/回显 + attributes 面开

## Why

S3 读协议补齐收官(s3-read-compat,路线档 1/6)后,路线档 2(用户点名「flexible
checksums」)锁定的写读完整性缺口:

1. **上传零校验**——新版 AWS CLI/SDK 默认随 PUT/UploadPart 发 `x-amz-checksum-*`
   (CRC32/CRC32C/SHA1/SHA256 四族),网关不认也不算,坏数据毫不知觉落放;
2. **aws-chunked trailer 已解析未校验**——分帧上传把校验值放在流尾 trailer
   (`x-amz-trailer` 声明),`2026-10-16-s3-chunked-payload` 归档时明记「明示不验证
   (=UNSIGNED 信任面)」:值被 decoder 读到后即丢弃,信任面留档待本档承接;
3. **GetObjectAttributes 的 `Checksum` 显式 501**——属性三属回显
   (ETag/ObjectSize/StorageClass)早已支持,Checksum 面挂着 501 招牌,值无处可取。

## What Changes

- **四算法 registry**(`crates/s3/src/checksum.rs` 新模块):`CRC32`(IEEE)/
  `CRC32C`/`SHA1`/`SHA256`,值一律 **base64(std, 带填充)编码**;请求头
  `x-amz-checksum-{crc32|crc32c|sha1|sha256}`;**CRC64NVME 不实现 → 遇到同族头
  显式 501**(升级路径);`x-amz-sdk-checksum-algorithm` 信息头忽略(不参与判定);
  `x-amz-checksum-type` 只认 `FULL_OBJECT`(其他 400)。
- **PUT/UploadPart 验证(先于任何 blob 写)**:逐个收到的校验头/trailer 值与服务端
  对**明文** body 的计算值比对——失配 400 **`BadDigest`**(新增错误变体,报文点名
  头形态);base64 坏/长度不符算法 → 400 `InvalidArgument`;全部通过才进入既有写
  路径(失败零 blob 写、零孤儿)。
- **trailer 承接(s3-chunked 信任面闭合)**:分帧 decoder 把 trailer 段各头行收进
  `TrailerSink` 挂到 request extensions;put/part 在 body 读完(EOF 后)消费——
  与显式头同一验证管线;`x-amz-trailer` 声明了校验头而 EOF 未到 → 400;
  同算法「头+trailer」并存 → 400(载体互斥)。**只验值,不改块签名链/UNSIGNED 框架
  面语义(SIGNED-TRAILER 的 trailer 段自身不签名,维持)**。
- **持久化(migration 005,PG+MySQL)**:`named_objects` 增
  `checksum_algo VARCHAR(8) NULL` + `checksum_b64 VARCHAR(64) NULL`(可空列:历史行/
  multipart/copy 产物无值);多算法同发时全部验证、按 **SHA256 > SHA1 > CRC32C >
  CRC32** 优先级恰好存一份;SSE 对象存**明文**校验(与 size/etag 明文口径同源)。
- **回显**:PUT/UploadPart 响应带回 `x-amz-checksum-{algo}` + `x-amz-checksum-type:
  FULL_OBJECT`;GET/HEAD 仅当请求带 `x-amz-checksum-mode: ENABLED`(其他值 400)时
  回存储值(200/206 面回;304/412 不回;mbedtls multipart/无值行不回)。
- **GetObjectAttributes 的 `Checksum` 面开**:存储值存在且被请求时回
  `<Checksum><Checksum{ALGO}>…` + `<ChecksumType>FULL_OBJECT</ChecksumType>`;
  请求而无值(multipart/老行)→ 元素省略;`ObjectParts` 501 维持。
- 无新配置、无新指标(400/501 走 `s3_requests_total{op,status}`)。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-core`:PutObject(校验头/trailer 验证 + 持久化 + 响应回显);GetObject/HeadObject
  (`x-amz-checksum-mode: ENABLED` 回显);object attributes retrieval(Checksum 面开)。
- `s3-auth`:decoded-length accounting(trailer 校验值从「容忍不验证」升级为「上抛
  写路径验证」;块签名链要求一字不动)。
- `s3-multipart`:UploadPart(同样的验证+回显,不持久化)。
- `s3-metastore`:新增(ADDED)对象校验持久化要求(005 迁移、可空、无回填)。

## Impact

- 代码:`crates/s3/src/checksum.rs`(新)、`auth/chunked.rs`(TrailerSink)、
  `handlers/object.rs`(put/get/head/attributes)、`handlers/multipart.rs`(upload_part)、
  `error.rs`(BadDigest)、`metastore/{types,pg,mysql}.rs` + `migrations/*/005_*.sql`、
  `Cargo.toml`(crc32fast/crc32c/sha1/base64 依赖)。
- 契约:PUT/Part 增 400/501 概率面(此前静默忽略);POST/PUT 分帧流的 trailer 值
  开始参与验证(此前丢弃)——**行为收紧只影响带坏校验的请求,合法请求零变化**;
  GET/HEAD 新回显头(查询前无该头)。不触碰 blob 引擎/SigV4 块签名/配额/GC。
- 明示不做:CRC64NVME(501)、multipart complete 级校验(composite/FULL_OBJECT
  派生,留后续档)、为拷贝产物补算(拷贝行校验为空,文档记)、`Content-MD5`
  老族(维持 etag-only)。
- 测试:算法已知向量单测 + e2e 双言矩阵(四族回路/失配/坏 base64/长度/多算法优先
  /trailer 三态/mode 回显/attributes 面/501 面)。
