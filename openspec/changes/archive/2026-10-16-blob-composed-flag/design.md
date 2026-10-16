# blob-composed-flag 设计

## Context

现状(实码核对):

- **记录与索引自带 flags 字节**:`Log` 记录头 `[magic u16|version u8|flags u8|key_suffix
  u32|expiration i64|data_len u32|data|crc u32]`;`.idx` 的 `IndexValue`
  `[offset(8)|size(4)|flags(1)|expiration(8)]`(21B)。目前只用了 `FLAG_DELETED =
  1<<0`,位段 1-7 空闲(value.rs 注释明示)。
- **flags 家族已在所有持久/恢复/复制路径上透传**(实核):恢复重建
  `index_value_from_record` 逐字复制 `decoded.flags`;复制 apply 与 compaction 均
  以 `rec.flags` 重编码记录并落索引(`blob.rs` 复制/压实区段);索引漂移核对
  (`v.flags != decoded.flags`)对任意位均成立。**即:给 flags 启用保留位,四条
  路径零代码即可继承**。
- **写口**:`put_ttl_inner` / `put_from_reader_inner` 硬编码 flags=0(`encode_record
  (…, 0, …)` / `encode_header_into(…, 0, …)`),共用 `publish_put` 落索引。
- **读口(frontend)**:`download` 先 `stat`,`size ≤ DESCRIPTOR_MAX_BYTES` 才整体
  读回比对 `ASTC` MAGIC;`HEAD` 同弧(为算逻辑 size 读描述符)。两失真模式
  (ASTC 前缀普通 blob 502;合法描述符被当普通文件上传 → 静默返回片数据)的根因
  均为「存储层无类型区分」。
- **描述符写点恰两处**:frontend `POST /stitch`(`router.put(key, descriptor)`)
  与自动分片管线(`router.put_auto_with_ttl(descriptor, ttl)`)。

## Goals / Non-Goals

**Goals:** 记录级 composed 位端到端落地(写口/读口/数据面/router);新描述符置信读
(不受 MAGIC 碰撞影响);存量描述符行为零回退(嗅探兜底);滚动升级先读后写可行
(双向 serde 兼容);§3.4 兼容义务用例兑现。

**Non-Goals:** 翻转「无位即不嗅探」(消除失真模式 ①② 的存量歧义)——需要回填
迁移或等存量老去,**独立后续项**(D5 记录条件);HEAD 的数据体读节省(同因受制于
兜底嗅探,置信翻转后自然兑现);描述符格式变更(位只标类型,格式不动);S3 网关(
其 multipart manifest 在 MetaStore 非 blob 描述符);校验/加解密(无涉)。

## Decisions

### D1:位选 `1<<4`,布局零变化——构造级双向兼容(§3.4)

`FLAG_COMPOSED = 1<<4`(IndexValue 与 Log 记录共用语义;位段 4-7 空闲)。
**不引入任何字节布局变化**——flags 字节先前即存在,启用其一个保留位:

- **前向(新数据经旧读路径)**:旧代码从不解析/掩码位 4(它只认 `FLAG_DELETED`),
  `from_bytes` 原样保留 flags 字节、`get`/`stat` 语义不受影响 → 旧二进制读写含
  位数据完全正常;用例:置位值 `to_bytes/from_bytes` roundtrip + 引擎
  `get`/漂移核对全绿。
- **后向(旧数据经新读路径)**:旧值位 4 = 0 → `is_composed() == false` → 走旧路径(
  嗅探兜底);用例:flags=0 值解析为 not-composed。
- **升级顺序**("先读后写"):新 router 的 `BlobStat.composed` 带
  `#[serde(default)]`(旧 store 的 metadata JSON 无该字段 → false,不炸);旧 store
  对未知查询参数 `composed=true` 静默忽略(serde urlencoded 默认行为)→ 期间写下的
  描述符无位 → 新 frontend 嗅探兜底仍正确。**降级不腐化**。

### D2:引擎写口 = 既有核的 flags 参数化 + 组合包装(签名零破坏)

`put_ttl_inner` / `put_from_reader_inner` / `publish_put` 增私有 `flags: u8`;公开
面新增 `put_composed_ttl(key, data, expiration_ms)` 与 `put_composed_from_reader
(key, len, src, expiration_ms)`(`FLAG_COMPOSED`),既有 `put/put_ttl/
put_from_reader` 签名与 flags=0 语义逐字不动(store 三写路径、引擎测试、bench
零改动)。**组合+tombstone 不组合出现**:位只在活记录写口进入,delete/apply 维持
既有 flags 透传,不引入新不变量。

### D3:数据面与 SDK——查询参数 + serde 双向宽(滚动升级的接缝面)

- store:`PUT ...?composed=true`(`PutParams` 增 `#[serde(default)] composed:
  Option<bool>`;三写路径按位分派到引擎组合口/既有口);`GET .../metadata` JSON 增
  `composed`。对齐 SeaweedFS needle 上传 `?cm=true`(先例:`FlagIsChunkManifest`)。
- router:`put_url` 增 composed 位(与 `expiration_ms` 组合成合法 query);`BlobStat`
  增 `#[serde(default)] composed: bool`。仲裁环参数化提取(D4)后提供
  `put_composed` / `put_composed_with_ttl` / `put_auto_composed_with_ttl`。
  `put_file_*` 不加组合面(描述符按构造 ≤ 1 MiB,恒走 inline 弧)。

### D4:router 组合方法 = 既有仲裁环提取参数化(不复制环体)

`put_with_ttl` 与 `put_auto_with_ttl` 的环体各自提取为私有
`put_keyed(key, data, exp, composed)` / `put_auto_keyed(data, exp, composed)`
(代码逐字搬运,仅 `put_body` 调用带位);原公开名成为 `composed=false` 的包装,
新增组合名为 `composed=true` 包装。**仲裁/重试/507 换分区/metrics 语义零变化**
(行为等价由既有 e2e 全量守望),不复制 ~100 行环体。

### D5:读序裁定——「flag 优先,无位兜底嗅探」(存量歧义冻结,翻转条件记档)

- stat `composed: true` → **描述符权威路径**:整体读回(仍 ≤ DESCRIPTOR_MAX,
  超限即 502 上游损坏——信任位即不静默降级);描述符解析失败 ≠ 回退普通对象(502
  既有口径)。**自此新写描述符免疫 MAGIC 碰撞**。
- 无位(存量描述符/一切普通对象)→ 维持嗅探。失真模式 ①(普通 blob 恰以 ASTC
  开头→502)与 ②(伪描述符静默展开)**在同一集群上不可避免继续存在**——它们
  的消除条件 = 集群内全部描述符带位(回填迁移,或新集群/存量老去后的翻转开关),
  **独立后续项,本档只留机制不翻转**。HEAD 曲线同因不省数据体读(注释如实)。

### D6:无观测/配置新增

存的是类型位,不是行为开关;读写路径行为对既有数据逐字节不变。frontend.md /
cluster.md / store.md 数据面表文案同步(§7 无表可动)。

## Risks / Trade-offs

- **"留兜底 = 歧义还在"的沟通面**:本档后仍能复现失真 ①②(未置位行)。选择的
  是渐进(机制先行、翻转另立项)而非断代(直接 flag-only 会让存量拼装对象
  HEAD/GET 语义回退成描述符本体)——取舍已在 D5 记录,升级路径明确。
- **`?composed=true` 落到旧 store 被静默忽略**:描述符以无位形态落库,读走兜底
  ——语义仍正确(约当旧世界),不构成损坏;升级窗口的已知中间态。
- **位语义的排他理解**:bit4 只由本层写口赋予;未来若引入更多 flags 位,需守住
  "DELETED 与 COMPOSED 不同时置位的一生"(构造上已保证:两种写口互斥)。
