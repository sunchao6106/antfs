## Why

blobId 的 objectId 部分怎么生成,是绕不开的架构决策,牵动 [D4 写路径](../../../docs/design/cluster.md)、[D1 内建 Raft master](../../../docs/design/cluster.md)、[§3.1 持久性红线](../../../CLAUDE.md) 和 [§3.4 blobId schema 纪律](../../../CLAUDE.md)。本 change 固化选型:**objectId 由 master 按 partition 单调发号(号段/block 分配)**,并锁定其正确性骨架(两层批处理 + Raft 水位持久化 + 容忍空洞 + 永不复用)。

选型经核实过 SDFS(网易 NOS)本地源码(`~/code/opensource/sdfs`)的 fsi(client)↔ mds 发号实现——它是 **MySQL 号段 + client 号段缓存 + 后台预取**,本 change 借其机制、用 Raft 取代其 MySQL 单点,并明确**不采纳** SDFS 的中心化每对象元数据模型(与 D3 冲突)。

本 change 只固化决策,部分位宽/参数留待实现标定(见 design.md 待定项)。

## What Changes

- **发号主体**:objectId 由 **master 按 partition 发号**(对齐 [cluster.md 线 271"master 发 id"](../../../docs/design/cluster.md),解决 [§7.6 Q7](../../../docs/design/cluster.md) 的"是否进 per-object 写路径")。
- **号段机制**:master 一次发一整段 objectId `[start, start+N)`,client 本地缓存号段、热路径 `offset++` 取号不碰 master;client 号段将尽时**后台异步预取**下一段(双 buffer)。
- **持久化骨架**:master 只把 per-partition **已预留高水位 `reserved_upto[P]`** 提交进 Raft(粗粒度块,非每号一次);**发号只发已提交水位以内**;崩溃/切主从已提交水位续 → **绝不重发**,水位与内存 `next` 间未用号 = **空洞(无害)**。
- **持久性约束(承 §3.1 红线)**:Raft 水位耐久性 = **多数派 fsync**,落点在 Raft 日志存储适配器"**先 fsync 再 ack commit**",用 [Q3 redb](../../../docs/design/cluster.md) + 崩溃一致性属性测试守住。
- **借鉴 SDFS**:client 号段缓存 + 预取 watcher、发号 anti-affinity(避开已存副本节点)、可选"master 选 partition"。
- **不采纳 SDFS**:MDS 存每对象 md5/size/expiry 的中心元数据模型(与 [D3 无中心每对象元数据](../../../docs/design/cluster.md) 冲突)。

## Capabilities

### New Capabilities
- `blobid-master-assign`: objectId 的 master 按 partition 号段发号——两层批处理、Raft 水位持久化(多数派 fsync)、容忍空洞永不复用、client 缓存预取;被否决/待定项与 SDFS 借鉴边界。

### Modified Capabilities
<!-- 与 cluster.md §8/§7.6 Q7/线22/271 及 master-node change 的既有方向互补,待相应 spec 落地时对齐。 -->

## Impact

- **代码(发号)**:master 侧新增 per-partition 水位状态 + Raft 提交 + AllocId RPC;client SDK 新增号段缓存/预取(对齐 SDFS `IDRangeAllocator`/`IDPoolWatcher`)。未实现,本 change 固化决策。
- **代码(blobId 32B→8B,待协调)**:`crates/blob/src/types.rs` `KEY_SIZE=32→8`(或 `StoreKey` 改 `u64`)+ 注释;波及 `crates/blob/src/{index/segment.rs,blob.rs}`(排序段/Fuse 布局按 8B key)、`crates/store/src/server.rs` 与 `crates/store-server`(去掉"key must be 32 bytes / 64 hex"校验、改 16 hex)、`crates/store/tests/data_plane.rs` 等测试夹具。**⚠ 当前另有 agent 在改 `crates/store`,代码改动待协调后再做,本 change 仅固化决策 + 记录范围。**
- **文档(已改)**:[cluster.md §8 blobId 结构 + §7.6 Q7 + 线 22/445](../../../docs/design/cluster.md)、[store.md §5 StoreKey](../../../docs/design/store.md) 已更新为 64bit/8B、32/32、无 version、master 号段发号。
- **持久性红线**:新增"Raft 日志存储 fsync 为持久边界"的崩溃一致性测试要求([CLAUDE.md §6](../../../CLAUDE.md))。
- **安全边界**:递增 objectId 可枚举 → MVP 无 ACL + 数据口直连需靠网络隔离/数据口鉴权兜底(非本 change 解决,记录为已知取舍)。
