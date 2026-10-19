# s3-multipart-complete-checksum — Design

## Context

- complete 装配 = 零物理拼接:multipart_parts 行集 + parts_manifest(JSON:逐片
  blob_id+size)+ 首片 blob 作 primary;GET 走 `PartStream` 顺序逐片流式。**对象字节
  = manifest 列入片的字节串接**(未列入片即丢弃)。
- UploadPart 已验四族校验(头+trailer)且应答回显;但值不落库——complete 级裁决
  无原料(这正是档 2 记档的依赖「需 per-part 持久化」)。

## D1:composite 推导在会话锁内(metastore),不在 handler

推导原料 = 行元数据(值已随 put_part 验证)。放进 `complete_multipart` 既有事务
(`multipart_uploads FOR UPDATE` 快照)内:读到的一定是终版 part 集,与 etag 校验/
弃片清点同快照,无「handler 读一组、事务提交另一组」的缝。请求若带 composite 值,
比对也在锁内——失配回滚 = 零副作用。

## D2:FULL_OBJECT 预检在 handler(metastore 之外),流式哈希

整对象哈希须读全部字节(跨 engine IO),不能进 metastore 事务。形态:handler 先按
请求 manifest(XML parts)映射 session 存片 → `PartStream` 逐片流式喂哈希(内存上界
与 GET 同:至多一片)→ 逐声明比对 → 通过才调 `complete_multipart`(事务内 etag
+ 快照重验)。预检与提交之间的并发窗口:complete 事务按 etag 严格校验(XML etag vs
锁内快照),任何替换/新片都会使提交失败(InvalidPart)——校验通过的数据即最终装配
数据,一致性由既有 etag 闸保证(记档留观,不做双重确认)。

## D3:行存 kind 列(NULL = FULL_OBJECT,migration 007)

`checksum_type VARCHAR(12) NULL`:历史行(005 时代)与 007 后单写行语义全是
FULL_OBJECT,NULL 读作 FULL_OBJECT——后向兼容零特判;composite 行存 `COMPOSITE`。
`checksum_b64` 扩 TEXT:composite 上界 = 10000 片×4B → b64 53.3KB,PG TEXT 天然容,
MySQL `TEXT`(64KB)恰容,取此不取截断。part 行校验两列为单片完整值(四族最大
sha256 b64 = 44 chars),`VARCHAR(64)` 恰容。

## D4:complete 请求语义(值头 + type 头)

- `x-amz-checksum-type`:缺省按 FULL_OBJECT(PUT 同缺省);值 ∈ {FULL_OBJECT,
  COMPOSITE} 否则 400 `InvalidArgument`。
- **FULL_OBJECT**(值头四族可多枚)→ 全验全比(PUT「多算法同发」同语义),行存
  最强一份 + kind FULL_OBJECT;type 有而值头无 → 400 `InvalidRequest`(声明而无值,
  响亮拒)。
- **COMPOSITE**(仅 CRC32/CRC32C 单族)→ 无值头:锁内推导+存+回显(部件有同族
  值才有原料);带值头:与推导比对,失配 400 `BadDigest`(报文点名);SHA 家族 或
  CRC 两族同发 配 composite → 400 `InvalidRequest`。
- 部件校验面规则(锁内裁决,composite 路径才查):manifest 片**全员同族 CRC** 才可
  推导;有片无值/混族 → 400 `InvalidRequest`(报文说明)。FULL_OBJECT 路径不依赖
  部件校验(直接验字节)。
- CRC64NVME 头 → 501(族政策贯穿 PUT/Part/complete 三面)。

## D5:回显面 kind 传染范围

- PUT/UploadPart 应答:恒 `FULL_OBJECT`(各自验的是完整单体)。
- GET/HEAD(mode-on)/GetObjectAttributes/complete 应答:读行 kind → COMPOSITE 行
  回 `x-amz-checksum-type: COMPOSITE`。
- 304/412 无回显、无值行静默缺省——既有口径零变化。

## 交付边界(记档不做)

- complete 应答不做 XML 体级 Checksum 元素(头即权威,`?attributes` 有持久值面);
- 拷贝产物回填、`Content-MD5` 老族、trailer 段签名——沿用档 2 记档不动;
- CRC64NVME 族实现仍 501。
