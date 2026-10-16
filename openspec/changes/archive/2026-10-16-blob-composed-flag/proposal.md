# blob-composed-flag —— 拼装描述符类型位:记录级 composed 标志换嗅探(含存量兼容)

## Why

frontend 拼装读路径用 **MAGIC 嗅探**判定"这个 blob 是不是拼装描述符"
(`GET`/`HEAD` 把 ≤1 MiB 的对象整体读出来比对前 4 字节 `ASTC`),两个已知失真
(`openspec/changes/blob-io-end-to-end/tasks.md` 待办首条,方案与先例推演完备):

1. **普通 blob 恰以 `ASTC` 开头** → frontend 读回 **502**(`corrupt stitch descriptor`;
   直连数据面仍 200,字节一致 —— 数据没丢,是网关拒绝服务);
2. **恰好合法的描述符 bytes 被当普通文件上传** → 读回 **200 但内容是它引用的片数据**
   —— **confused deputy**(静默返回别人的内容)。

根因:描述符与普通对象在存储层**无类型区分**,读方只能猜。

## What Changes

- **引擎(`antfs-blob`)记录级类型位**:`FLAG_COMPOSED = 1<<4`(log 记录 flags 字节与
  `.idx` IndexValue flags 字节同位置位;位段 4-7 本就空闲)。**布局零变化** —— flags
  字节一直存在,启用保留位 = 旧读者忽略未知位、新读者见位为零走旧路径(§3.4 双向
  天然兼容,佐证与测试见 design D1)。新增写口 `put_composed_ttl` / 
  `put_composed_from_reader`(既有签名不动,内部共用带 flags 的私有核);
  `stat`(即 `.../metadata` 的数据源)回 `BlobMeta.composed`。恢复/重建/复制/压实
  四路径已按记录自描述 flags 全程透传(实核:`index_value_from_record` 复制
  `decoded.flags`;复制 apply 与 compaction 重编码均持 `rec.flags`)。
- **store 数据面**:`PUT .../blobs/{key}?composed=true` 置位(对齐 SeaweedFS needle
  上传 `?cm=true`);`GET .../metadata` JSON 增 `composed: bool`。
- **router SDK**:put URL 携 `composed=true`(`BlobStat` 增字段,**serde default 兼容
  旧 store 响应**);新增组合写方法 `put_composed` / `put_composed_with_ttl` /
  `put_auto_composed_with_ttl`(取自既有仲裁环的参数化提取,旧签名与语义零变化)。
- **frontend 写读闭环**:
  - **写**:`POST /stitch` 与自动分片的描述符落库改走组合写(置位);
  - **读**:`flag 优先,无位兜底嗅探`——stat 见 `composed: true` → 描述符路径
    (**权威**:不再受 MAGIC 碰撞影响;超长即 502 不静默降级);无位(存量行)→
    维持既有嗅探(存量歧义冻结,见 D5);
  - HEAD 结构不改(小对象读描述符算逻辑 size 的弧对两形态同流),注释更新。
- 无新配置、无新指标;文案与表格同步(frontend.md / cluster.md / store.md 数据面表)。

## Capabilities

### New Capabilities
- `blob-composed-flag`(本 change 新增 spec):记录级组合位的不变量家族(位定义/
  持久透传/写口/读口)、数据面 `?composed` 与 `metadata` 契约、网关组合写/置信读
  契约与存量兜底。

### Modified Capabilities
(无既有 spec 触碰——frontend 描述符/数据面此前无 spec 能力;`frontend-liveness-endpoint`
等无关。)

## Impact

- 代码:`crates/blob/src/index/value.rs`(位+判定+测试)、`crates/blob/src/blob.rs`
  (flags 核化+组合写口+`BlobMeta`/`stat`)、`crates/store/src/server.rs`(PutParams/
  三写路径/MetadataDto)、`crates/common/router/src/lib.rs`(put_url/BlobStat/仲裁环
  参数化+三组合方法)、`crates/frontend/src/server.rs`(两描述符写点、download
  flag 优先、注释)。S3 网关**零改动**(其 multipart manifest 在 MetaStore,不是
  blob 描述符);benchmarks 复用旧签名零改动。
- 滚动升级:先铺新读(新 router 对旧 store 响应 serde default 兼容;新 store 对旧
  router 的无参 PUT 默认不置位)再开写(frontend 发 `?composed=true`)——期间描述符
  位缺失即回嗅探,**降级语义正确不腐化**(D3);升级后写入的描述符全部置信读。
- §3.4 兼容测试义务:布局不变论证 + 双向用例(前向:置位值经旧读路径;后向:
  未置位旧值经新读路径)齐(D1);`BlobStat`/`PutParams` 的 serde 兼容落实+用例。
