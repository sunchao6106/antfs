## Why

当前实现是"忠实翻译 Ambry"的产物:为了支持三个变更类操作,引擎里建了三套各自独立的机制 ——

- `undelete`(复活已删对象):靠记录与索引里的 `life_version` 解"删除 / 复活"竞态(**复制合并规则也必须按 `life_version` + flags 逐条裁决**);
- `update_ttl`(上传后改过期时间):靠 `operation_time_ms` 定序,而且因为"索引每 key 只存一个赢家"必须把整条数据体重写一遍;
- `hard_delete`(隐私擦除):按对象把已删对象的原数据体清零(COW 重写整段),为此索引里必须留 `original_offset` 记住"数据体到底在哪"。

这三件事的成本并不只落在三个函数上,而是**摊进了磁盘格式的每一层**:记录头(固定开销 38B,含 `op_time`/`life_version`)、索引条目(67B,含 `original_offset`/`life_version`/`op_time`)、复制合并裁决、数据面 HTTP 契约、frontend 的 `x-ambry-*` 响应头、router 的方法表、请求静默的操作枚举,以及配置项、监控项与四份设计文档。

用户决定**整体简化**:`blob` 不可变 + **删除不可逆** —— `undelete` / `update_ttl` / `hard_delete` 全部去掉。本 change 承接该决定,记录简化后的语义、随之精简的磁盘格式(段头 v2 / 记录 v4 / 索引 v2)与被连带移除的能力清单。

> **状态:已实施(2026-09-23)**。下文保留为**提案当时的原貌**;实现结果与提案的偏差(最重要的几条:没有任何版本化兼容层、段头 CRC 位置未后移、索引条目仍是 8B key + 21B value、索引 flags 无记录版本位)见 [`tasks.md` 的「与原提案的偏差」](./tasks.md#与原提案的偏差实现后回填)与 [`docs/DEVLOG.md`](../../../docs/DEVLOG.md) 2026-09-23 条。
> 其中原提案"前后兼容策略"一节已不适用:**实施时无已部署数据,不做任何兼容层**。

简化收益:

- 复制合并从"比 `life_version` + flags"退化为**墓碑恒胜** —— 单调、幂等、天然收敛,**不再需要每对象定序字段**(删/复活竞态在语义上直接消失);
- 每记录固定开销 **38B → 24B**、索引条目 **67B → 29B**;段头新增 4B `key_prefix`,换来记录里 key 只存 4B 后缀;
- 复制不再需要拉**已删对象**的数据体(墓碑只写状态就能收敛),顺带**省带宽**;
- 引擎少三条写路径,其中 `hard_delete` 那条"COW 重写整段 + 原子换 `Inner`"是并发/崩溃风险最高的一条,一并消失。

代价(用户明确接受):

- **误删不可恢复**;
- **上传后不能改过期时间**(TTL 与数据体一样,写入时定死);
- 隐私擦除**从"对象级按需擦除"降级为"段级周期擦除"**:删除后原数据体靠 compaction 回收文件,字节**不被主动清零**(取证上可能残留到块被复用)。这是一条**可逆的降级**,不是堵死的路 —— 将来若需要,用"compaction 时把死记录(含被 DELETE 取代的 PUT 数据体)置零再落盘"实现,**不需要每对象元数据**。

## What Changes

- **语义简化**:删除不可逆;复制 apply 的合并规则改为**墓碑恒胜**;TTL 只能在写入时给定。
- **能力移除**:`undelete` / `update_ttl` / `hard_delete` 从引擎 API、数据面端点、frontend 路由与响应头、router 方法表、请求静默的操作枚举、错误类型、指标、本地 worker loop 与配置项上全部去掉(逐项清单见 [`design.md` D9](./design.md))。
- **磁盘格式演进**(只写新格式;**无兼容层** —— 实施时无已部署数据):
  - **段头 v2**(实现后校正):每段存 **4B `key_prefix`**(offset **32..36**);CRC **仍在 36..40**,覆盖 `[0,36)`;`version = 2`,open 时**校验**(不符 → `Corrupt`)。
  - **记录 v4**:`magic(2)|version(1)|flags(1)|key_suffix(4)|expiration_ms(i64,8)|data_len(u32,4)|data|crc32(4)`,固定开销 **24B**;去掉 `op_time`/`life_version`,key 只存 **4B 后缀**(完整 key = 所在段的 `key_prefix` ‖ 记录后缀)。**只有 v4 一种记录格式**(不做 v3 dual-read)。
  - **索引 v2**:条目 **29B**(原 67B)。`offset` 由"段号 u64 + 段内偏移 u64"打包成**一个 u64**;`size` 为 **u32 = 整条记录的字节数**(不是提案里写的数据体长度);`flags` **只有 `FLAG_DELETED`,无"记录版本位"**;删掉 `original_offset(16)`、`life_version(2)`、`operation_time_ms(8)`;**保留 `expiration_ms(i64)`**(读路径要能零 IO 判过期、重建索引后不丢 TTL)。
- **新配置约束**:`store.segment_size_bytes ≤ 4 GiB`,在 `StoreConfig::validate` 里**显式校验**(默认值已是 4 GiB,不动默认)。
- **兼容策略(实现后校正)**:**不做任何 dual-read** —— 段头只认 v2、记录只认 v4,版本 / CRC 不符即 `Corrupt`。唯一保留的"迁移故事"是索引:`.idx` 头里的 `key_size/value_size` 就是布局指纹,旧索引文件被判 `Corrupt` 后走**从 log 全量重建**(既有安全网与测试);**不做数据迁移**(实施时无已部署数据)。
- **文档同步**:`docs/design/{store.md,cluster.md,router.md,frontend.md,replication.md}`,受影响的 `openspec/changes/*` 档案加"后续变更"注记,并在 `docs/DEVLOG.md` 追加记录。

## Capabilities

### New Capabilities

- `simplify-delete-model`:不可变 blob + **删除不可逆**的存储语义(墓碑恒胜的复制合并、写入时定 TTL)、随之精简的磁盘格式(段头 v2 / 记录 v4 / 索引 v2)、被移除的变更类操作清单。**无兼容层**:只写新格式,唯一兼容面是索引旧布局走"判 `Corrupt` → 从 log 全量重建"。

### Modified Capabilities

<!-- 既有能力被削减(不是新增):
     blob 引擎 —— 记录/索引/段头格式、删除语义、复制 apply 合并规则,undelete/update_ttl/hard_delete 三条操作移除;
     store 数据面 —— POST .../undelete 与 POST .../ttl 两条端点、?replication=1 的 include_deleted 语义、MetadataDto/MetadataEntry 字段、DataOp 枚举、hard_delete worker 与配置/指标;
     router —— undelete/update_ttl 方法与变更类扇出的 op 集合;
     frontend —— 两个路由与 x-ambry-blob-undeleted/-ttl-updated/-life-version 三个响应头(x-ambry-ttl-ms 保留);
     admin —— scrub/metadata 侧对 life_version 等字段的读取。 -->

## Impact

- **crate**:
  - `blob`(**主战场**):`log_segment.rs`(段头 v2)、`blob.rs`(记录 v4、删除语义、复制 apply、compaction 相关偏移算术)、`index/value.rs` + `index/segment.rs`(索引 v2)、`config.rs`(4 GiB 校验)、`error.rs`/`metrics.rs`(移除项)、`types.rs`(记录内 key 后缀/前缀拼装);
  - `store`:数据面端点与状态码映射、复制拉取(`replication.rs`)、请求静默枚举(`request_control.rs`)、指标、本地 worker 与配置(`worker.rs`/`config.rs`)、per-replica 的 4B 前缀下传(`replica.rs`);
  - `common/router`:方法表、变更类扇出、`BlobStat`、metrics;
  - `frontend`:路由、响应头、上传时的 TTL 传递、metrics;
  - `admin`:scrub 与 `replica-metadata` 侧的 `life_version` 等字段(连带面,**已落地**);
  - 各 crate 的**测试夹具**:key 构造必须改成"与副本的 4B 前缀一致",可由 `StoreKey::from_parts(0, n)` 承担。
- **契约变化**:数据面删两条端点(原 `POST .../undelete`、`POST .../ttl`),`?replication=1` 不再返回已删对象数据体;frontend 删两条路由与三个响应头;`MetadataDto`/`MetadataEntry` 的 JSON 字段减少(运维/巡检脚本需同步);`store.segment_size_bytes` 新增上界校验;`store_server.hard_delete_interval_secs` 移除。
- **持久性红线([CLAUDE.md §3.4](../../../CLAUDE.md))(实现后校正)**:段头/记录/索引三层格式同时变更,但**实施时无已部署数据,故不做 dual-read 兼容**;保留的兼容面只有索引 —— 旧布局走"判 `Corrupt` → 从 log 全量重建"这条既有安全网。持久化边界本身不变(仍以 `flush` 推进 header 的 `data_end`;段头新字段不改变恢复语义)。
- **安全边界**:删除不可逆 → **误删即数据丢失**(无回收站、无宽限期)。这是本 change 明确接受的产品代价,运维文档与 API 文档须写明;隐私侧的口径从"对象级擦除"降级为"段级、惰性、最终由块复用覆盖"(见 design.md D3)。
- **被否决方案**:见 [`design.md`](./design.md)(只删 `op_time` 而保留另外三条机制、保留 `original_offset` 但去掉 undelete、把 `expiration` 也从记录/索引里去掉)。
