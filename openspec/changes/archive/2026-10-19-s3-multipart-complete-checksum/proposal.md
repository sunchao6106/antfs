# s3-multipart-complete-checksum —— complete 级校验:composite 合并 + FULL_OBJECT 验证

## Why

`s3-checksums`(档 2)收官时明记(`s3.md` 灵活校验节「明示不做」):

> multipart complete 级校验(composite 合并 / FULL_OBJECT 派生需 per-part 持久化,另立档)

PUT/Part 级四族校验已闭环,但 CompleteMultipartUpload 是**盲的**:装配产物无任何完整性
裁决,multipart 通道相对单写通道缺了校验语义的一整层(AWS `COMPOSITE`/`FULL_OBJECT`
两面都没有)。用户 2026-10-19 指令「全力完善 s3 功能,把这个先完结」——本档 = 完结
批 C1。

## What Changes

- **per-part 校验持久化(migration 007,双方言)**:`multipart_parts` 增
  `part_checksum_algo VARCHAR(8) NULL` + `part_checksum_b64 VARCHAR(64) NULL`——
  UploadPart 已验证的值(select 多算法同发时的既有胜者)随行落库,为 complete 级裁决
  提供原料;`named_objects` 增 `checksum_type VARCHAR(12) NULL`(`FULL_OBJECT` |
  `COMPOSITE`;NULL = 历史行,回显按 FULL_OBJECT)`checksum_b64` 扩 TEXT
  (composite 上限 10000 片 × 4B ≈ 53KB)。
- **complete 级 COMPOSITE(memtable 会话锁内推导)**:所有列入 manifest 的 part 带
  同族 CRC 校验 → 事务内 composite 值 = base64(逐片 raw CRC 字节串接,order =
  manifest 序);完成行存 `{algo, composite, COMPOSITE}` 并在 complete 应答以
  `x-amz-checksum-{crc 家族}` + `x-amz-checksum-type: COMPOSITE` 回显;随部件无同族
  校验时请求却又声明 composite → 400 `InvalidRequest`。请求自带 composite 值(SDK
  行为)→ 与推导比对,失配 400 `BadDigest`(零副作用:session/parts 原样,可重试)。
- **complete 级 FULL_OBJECT(handler 预检)**:请求带四族头 + type FULL_OBJECT(缺省
  即此)→ 网关按 manifest 顺片流式哈希(复用 GET 的 `PartStream`,至多一片在内存),
  逐声明比对,验证行存 `{algo, value, FULL_OBJECT}`、按 005 既有「恰存最强一份」;
  失配 400 `BadDigest` 零副作用。`COMPOSITE` 配 SHA 家族 → 400;CRC64NVME → 501(族
  政策不变)。
- **回显面按行 kind**:GET/HEAD/GetObjectAttributes 的 `x-amz-checksum-type` 不再
 恒 `FULL_OBJECT`,按行存 kind(composite 行回 `COMPOSITE`);PUT/Part 应答恒
  FULL_OBJECT 不变。历史行(NULL)按 FULL_OBJECT——回滚兼容。
- complete 应答/门禁:多算法同发 complete(除 composite 限 CRC 单族)按 PUT 语义
  全验全比;session 内容导出零变化(blob/manifest 结构不动)。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-multipart`:UploadPart requirement 的「Part checksums SHALL NOT be persisted」
  句翻转为持久化 + 新增(ADDED)「CompleteMultipartUpload 校验裁决」要求(composite
  推导与比对/FULL_OBJECT 预检/类型规则/回显/零副作用)。
- `s3-metastore`:新增(ADDED)「per-part 校验持久化与对象 kind 列」要求(migration
  007、双方言、`StoredChecksum` 形态扩展 + from_pair 后向兼容)。

## Impact

- 代码:`migrations/{postgres,mysql}/007_*.sql`、`metastore/{types,mod,pg,mysql}.rs`
  (列 + 派导 SQL + trait 签名)、`handlers/multipart.rs`(put_part 落库 + complete
  裁决弧)、`checksum.rs`(`ChecksumKind` 枚举出门)、`handlers/object.rs`(回显 kind)。
- 契约:complete 失败新增 400/501 概率面(既有指标观测);GET/attributes 回显对复合行
  type 变 COMPOSITE——对只认 AWS 语义的客户端为**补齐**而非破坏;磁盘 migration 007
  双方言幂等。
- 持久性风险分析(§3.3):complete 弧的原有顺序(blob 已在、行一次性写、遮蔽/弃片
  走 WAL)不变;新增校验均发生在 metastore 事务**之前/之内**,失败零副作用可重试
  (幂等);FULL_OBJECT 预检只读 blob(get),无写面。
- 测试:e2e 双言九件(composite 自动推导/SDK 值比对善/比对恶/混算法拒/半带校验拒/
  FULL_OBJECT 验证善/验证恶 sha256+大对象/缺省 type 全验/composite×sha 400+CRC64NVME
  501)+ 回显 kind 断言 + 既有套件零回归;migration 后向(历史行回 FULL_OBJECT)。
