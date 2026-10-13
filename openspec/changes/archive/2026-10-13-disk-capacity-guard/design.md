## Context

`select_disks`（`placement.rs`）是纯函数，选盘时依赖心跳软状态的 `free_bytes`（`statfs`）和盘健康。当前没有任何逻辑承诺概念——盘上 30 个 partition 各自只写了 10 GiB（物理 300 GiB），但逻辑承诺 3 TB；master 看到 7.7 TB free 就继续放，最终物理装不下。

调研了 Ambry（离线静态布局 + `getAllocatedRawCapacityInBytes` 逻辑记账）、SeaweedFS（`MaxVolumeCount` 槽位计数 + `TryReserveCapacity` 原子两阶段预留）、CubeFS（`TotalPartitionSize` 心跳上报 + `overSoldFactor` + `AvailableSpace -= leaderSize` 内存即时扣减 + 单调递减守卫）。antfs 不适合 CubeFS 的 `overSoldFactor`——CubeFS 支持随机覆写和 `fallocate()` 打洞删除，partition 物理占用双向波动，超卖有合理性；antfs 是 append-only blob 存储，每个 partition 终态就是写满，超卖必定暴雷。

master 已持有完整 Raft partition table（哪些 partition 在哪块盘上），可直接计算 `committed`。`select_disks` 的所有调用点（建分区、再复制、balance move 落地校验）共用同一个 `placement_usage(&soft)` 路径。

## Goals / Non-Goals

**Goals:**
- G1：选盘时拒绝逻辑超卖——一块盘上所有 partition 写满后不超过盘可用容量
- G2：为非 log-segment 开销（index 文件、filter、tmp、FS 元数据）预留盘空间
- G3：Raft apply 后立即反映容量变化，不等下次心跳
- G4：分区接近写满时提前停止路由，不等 store 侧 507
- G5：leader 切换后仍然安全（committed 从 Raft 状态恢复，adjusted_free 至多丢一个心跳周期）

**Non-Goals:**
- 不引入 `overcommit_factor`（antfs partition 终态写满，超卖无意义）
- 不改 store 侧心跳协议（master 已有足够信息）
- 不改引擎层（`crates/blob/` 冻结）
- 不改 `near_full_ratio` 的语义（它仍是物理层安全网，本次加的是逻辑层）

## Decisions

### D1 逻辑承诺检查：committed 从 Raft 状态计算，per-partition capacity

**选择**：`PartitionInfo` 新增 `capacity_bytes: u64` 字段，`CreatePartition` apply 时从当前 `config.partition_capacity_bytes` 快照写入，此后不变。committed 按每个 partition 自己的 `capacity_bytes` 累加：

```rust
committed_on_disk(node, disk) = Σ partition.capacity_bytes  // 遍历该盘上所有副本
```

由 `select_disks` 调用方（`placement_usage`）从 Raft partition table 扫描后通过 `DiskFree` 传入。

**包含所有状态的副本**：遍历 partition table 中 **RW + RO + SEALED** 的所有副本，不区分分区状态——物理都在盘上。DROPPED 副本已从 table 移除，自然不计入。

**`partition_capacity_bytes` 动态调整安全**：改 config 只影响新建的 partition。老 partition 用创建时的值，committed 精确反映每个 partition 的实际容量上限，不会虚高或偏低。

**balance move 期间的双份 committed**：move 添加目标副本（`AddReplica`）后、删除源副本（`RemoveReplica`）前，同一个 partition 在源和目标盘上各计一份 committed。保守正确——move 过程中确实需要双份物理空间。

**替代方案**：
- (a) `committed = count × config.partition_cap`（全局一刀切）→ 改 config 后老 partition 按新值算，虚高或偏低。
- (b) store 侧心跳上报 `TotalPartitionSize`（CubeFS 做法）→ 需改心跳协议，且仍依赖心跳延迟。
- (c) `select_disks` 内部访问全局 partition table → 破坏纯函数性。

**理由**：对齐 Ambry `replica.getCapacityInBytes()`（per-replica 存储），权威且无延迟。`placement_usage` 签名改为 `(&SoftState, &MasterState, &MasterConfig)` 一次性组装完整 `DiskFree`，5 个调用点透明。

### D2 磁盘预留空间：`disk_reserved_ratio`

**选择**：新增配置 `master.disk_reserved_ratio`（f64，默认 0.05 即 5%）。可用容量上限 `usable = disk_capacity × (1 − ratio)`。逻辑承诺检查和物理空间检查都用 `usable` 而非 `disk_capacity`。

预留覆盖：每个 partition 的 index 文件（封存段 Binary Fuse Filter ≈1.08 B/对象、`.idx` 每条目定长）、`COMPACT_POLICY_STATE`、暂存 `.tmp`、文件系统自身元数据（inode/journal）。按 100 GiB partition / 4 KiB 均值 blob 估算：~25M 对象 × ~5 B/对象(fuse+idx) ≈ 125 MiB index/partition；80 partition/盘 → ~10 GiB index；8 TB 盘的 5% = 400 GiB，远够覆盖。5% 偏保守但安全。

**替代方案**：
- (a) 绝对值 `reserved_bytes`（如 50 GiB）→ 不随盘大小缩放，4 TB 小盘上 50 GiB 太贵，20 TB 大盘上又太少。
- (b) 不预留，依赖 `near_full_ratio` → `near_full_ratio` 是物理层防线（盘将满时转 RO），触发时已经没救了，不是预防措施。

### D3 内存即时扣减 + 单调递减守卫

**选择**：`DiskUsage` 新增两个字段：

```
adjusted_free: u64         // 选盘决策用的"有效可用空间"
last_heartbeat_time: Instant  // 最近一次心跳的时刻
```

三条更新规则：

1. **首次心跳**：`adjusted_free = free_bytes`（初始化）。
2. **CreatePartition 成功后**：`adjusted_free = adjusted_free.saturating_sub(partition_cap)`。
   **实现位置（偏离原稿）**：不在 Raft `apply` 回调里做，而在 leader 侧 `server.rs::place_one_partition` 中、`client_write(CreatePartition).await` 返回 `Ok` 之后扣减。原因：`RedbStateMachine`（`store.rs`）的 apply 路径**拿不到 `SoftState`**（确定性铁律：apply 只依赖命令本身），而 `place_one_partition` 是生产环境唯一的建分区路径且仅 leader 执行，等价且更简单。leader 切换后 soft state 丢失 → 扣减丢失，但 committed 检查（Raft 状态）仍在兜底（见 D1）。
3. **心跳更新**：
   - `reported_free < adjusted_free` → 覆盖（实际比预期更少 = 盘上有新写入）
   - `reported_free >= adjusted_free` 且 `now − last_heartbeat_time < adjusted_free_timeout`（默认 120 秒）→ **不覆盖**（防止滞后心跳回滚扣减）
   - `reported_free >= adjusted_free` 且超时 → 覆盖（compaction 释放空间 / 节点重启后恢复）

**为什么固定 120 秒超时够用**：compaction 只有两种触发方式——每天定时（凌晨）或运维手动。两种情况下 120 秒的可见延迟都不是问题。不值得为此引入更复杂的守卫机制。

**`record_beat` 改为 merge 语义**：当前 `record_beat` 直接 `insert` 覆盖整个 `Liveness`。改造后需逐字段 merge——`capacity_bytes` 和 `health` 总用最新心跳值，`adjusted_free` 按上述守卫规则更新。`free_bytes` 保留最新心跳原始值（监控/观测用）。

**理由**：借鉴 CubeFS `data_node.go:223-226` 的单调递减守卫。master 侧扣减过的量不会被滞后心跳覆盖。leader 切换后 soft state 丢失 → usage map 为空 → 不分配 → 第一轮心跳恢复（同现有行为）。committed 检查（Raft 状态）始终在，兜底安全。

**替代方案**：
- (a) SeaweedFS 式两阶段原子预留 → antfs Raft 已串行化 `CreatePartition`，不需要额外并发控制。
- (b) 不做内存扣减，纯靠 committed 检查 → 物理空间检查 `free_bytes >= partition_cap` 仍依赖心跳，快速连续建分区时可能超过盘物理余量。committed 防的是长期超卖，adjusted_free 防的是短期连续分配。
- (c) 基于 `last_deduction_time` 的扣减窗口守卫 → 增加了字段和配置项，compaction 不频繁，120 秒固定超时已足够。

### D4 分区级写满感知

**选择**：reconciler 巡检时对每个 RW 分区检查 `max_used >= partition_cap − segment_capacity`（即剩余空间不足一个段），满足则 Raft 提交 `SetPartitionState(RO)`。

**理由**：当前的盘级转 RO（`used_ratio > near_full_ratio`）太粗——一个 partition 快满但盘还有大量空闲时不触发。写路由权重（`capacity_weight_permille`）虽然会降低但不为零（下界=1），仍有千分之一概率被选中 → 507。直接转 RO 是确定性止血。

`max_used` 取自 `SoftState::used_bytes_by_partition()`（各副本 `used_bytes` 最大值，已有基础设施）。`segment_capacity` 取自 `store.segment_size_bytes`（默认 4 GiB）——需在 master config 中镜像或心跳携带；最简方案：在 master config 中加 `segment_capacity_bytes` 配置（默认 4 GiB，与 store 侧对齐）。

**替代方案**：
- (a) 只降权重不转 RO → 权重=1 仍被选中，客户端吃 507。
- (b) store 侧主动上报 `is_full` 标记 → 需改心跳协议；且 master 已有 `used_bytes`，信息足够。

### D5 Router 侧 StoreFull 重试

**问题**：D4（reconciler 转 RO）有最多一个 tick（~5 秒）的延迟窗口。窗口内 `pick_writable_partition` 仍可能选中已满分区 → 发号给 client → client PUT → store 报 507。当前 `StoreFull` 在 router 侧 `is_retryable = false`，写入直接失败。

**约束**：blobId 编码了 partition_id，不能拿 partition 42 的 blobId 去写 partition 53。所以"换个 partition 重试"必须**重新分配 blobId**。

**选择**：根据 API 语义分两种处理：

**`put_file` / `put_file_with_ttl`**（router 内部分配 blobId，调用方只拿最终结果）：
```
loop (max_retries = 2):
    key = allocate_one()                // 从权重池取号
    result = quorum_put(replicas, key, data)
    if all_replicas_StoreFull:
        evict_partition(key.partition_id())  // 踢出号段缓存
        continue                            // 用新 blobId 重试
    return result
```

**`put` / `put_with_ttl`**（调用方提供 key，key 绑定了 partition）：
```
result = quorum_put(replicas, key, data)
if all_replicas_StoreFull:
    evict_partition(key.partition_id())  // 踢出号段缓存
    return Error::StoreFull              // 调用方需重新 allocate
```

**`evict_partition(pid)`**：从 router 本地号段缓存中移除 partition pid 的所有剩余号段。不通知 master——master reconciler 会在下个 tick 自行转 RO。下次 `allocate_one()` 号段池为空时会向 master 重新拉号段，此时 master 不再选该 partition。

**全量 507 判定**：只有**所有副本都报 507** 时才判为 partition 写满并重试。如果只是部分副本 507（理论上同一 partition 的副本应同步写满，但存在复制延迟），quorum 写仍可能成功。

**D4 + D6 协同**：
- D4（master 转 RO）= **全局收敛**——所有 router 实例不再选该 partition，消除后续 507
- D6（router 重试）= **即时止血**——碰到 507 的那个 router 实例立即重试成功，用户无感
- 缺任一个都不够：只有 D4 → 窗口内的 507 对用户可见；只有 D6 → 每个 router 实例都要碰一次 507

**不改 `is_retryable`**：`StoreFull` 对 `put(key, data)` 来说 key 绑定了 partition，重试同一个 key 没意义。重试逻辑内化到 `put_file*` 系列方法中。

## Risks / Trade-offs

- **[盘利用率下降]** → 预留 5% + 逻辑硬限制意味着盘不能用到 100%。但这是正确行为——用到 100% 会导致 index 写入失败、FS journal 满等不可恢复故障。5% 是合理安全垫。
- **[committed 扫描开销]** → O(partitions) 每次选盘调用。集群 10 万分区级别下微秒级，不是瓶颈。如果将来成为问题，可缓存在 soft state 中（每次 Raft apply 增量维护）。
- **[leader 切换后 adjusted_free 丢失]** → usage map 为空 → 不分配 → 第一轮心跳恢复。committed 检查（Raft 状态）始终在位兜底。
- **[分区级转 RO 的 used_bytes 精度]** → `used_bytes` 来自心跳，有一个周期延迟。最坏情况：延迟一个周期才转 RO → 这个周期内 store 侧可能报 507。D6 的 router 侧重试兜底。
- **[segment_capacity_bytes 在 master/store 间对齐]** → master 需要知道 segment 大小才能判断"剩余不足一个段"。master config 加配置项镜像 store 侧值（默认 4 GiB）；错配时 master 侧判断偏保守（提前一点转 RO），不危险。
- **[Compaction 临时双倍空间]** → `compact()` 全量 compaction 创建 `log.new`，旧+新同时在盘上。临时额外 ≤ 一个 partition 的 live data ≤ `partition_cap`（100 GiB）。compaction loop 是顺序的（同一时刻最多一个 partition 在 compact），5% 预留（8 TB 盘 = 400 GiB）足够覆盖。如果将来 compaction 改为盘级并行，需增大预留或加并发控制。
- **[balance move 双份 committed]** → move 进行中，源和目标盘都计一份 committed。保守正确——move 确实临时需要双份空间。可能减少可用落点但不会导致数据丢失。
- **[`ext4` reserved blocks 与 `disk_reserved_ratio` 的关系]** → `statfs` 的 `f_bavail` 已排除 ext4 的 reserved blocks（`f_bfree − f_bavail`）。antfs 的 `disk_reserved_ratio` 是在此基础上**额外**预留，不与 ext4 冲突。
