## Context

antfs 的引擎层（`blob` crate）已实现三种 compact 方法：

- `compact()`：CompactAll，重写整个副本的 log
- `compact_incremental()`：按段死占比选脏段增量压缩
- `compact_cost_benefit()`：按 cost/benefit 比选最优连续段区间压缩

当前 `worker.rs` 的定时 compaction 只调用 `compact_if_needed` → `compact_incremental`。`compact_cost_benefit` 已实现但无调用方。

Ambry 的生产实践是 `HybridCompactionPolicy`：大多数轮次走 `StatsBasedCompactionPolicy`（cost-benefit），每隔 `storeCompactionPolicySwitchTimestampDays`（默认 6 天）穿插一次 `CompactAllPolicy`。这在 LinkedIn 大规模生产环境下验证过：cost-benefit 日常省 IO，CompactAll 周期性清碎片。

改动范围限于 `crates/store/`（`worker.rs` + `config.rs` + 指标），不改引擎层接口。

## Goals / Non-Goals

**Goals:**

- 定时 compaction 支持三种策略可配切换：`incremental`、`cost-benefit`、`hybrid`
- `hybrid` 为新默认：日常走 cost-benefit，每 N 天穿插一次 CompactAll
- Hybrid 的"上次 CompactAll 时间"持久化到磁盘，重启后不丢
- 每次 compact 按实际使用的策略打指标，方便观测策略分布
- 配置项对齐 Ambry 命名与默认值

**Non-Goals:**

- 不改引擎层 `blob` crate 的任何接口或实现
- 不做集群级 compaction 调度（独立 scheduler 角色留后续 P1）
- 不做 IO 限流（每盘池已有入口控制）
- 不改手动 compact 端点行为

## Decisions

### D1：策略选择枚举化，在 worker 层分发

**选择**：在 `config.rs` 新增 `compaction_policy` 枚举字段（`incremental` / `cost-benefit` / `hybrid`），`worker.rs` 的 `compact_all` 函数根据配置分发到不同的引擎方法。

**为什么不是引擎层做策略选择**：引擎层（`blob`）是纯存储引擎，只提供"能力"（三种 compact 方法）。"什么时候用哪种"是运维策略，属于 store 层（对齐 Ambry：`BlobStore` 只有 `compact(details)`，策略由 `CompactionManager` + `CompactionPolicy` 决定）。

**替代方案**：在 `BlobStore` 上加一个 `compact_with_policy(policy_enum)` 方法 → 违反分层，引擎不该知道调度策略。

### D2：Hybrid 状态持久化用文件而不是内存

**选择**：每个副本目录下写一个 `COMPACT_POLICY_STATE` 文件，JSON 格式 `{"last_compact_all_epoch_secs": <i64>}`。每次 CompactAll 完成后更新。

**为什么**：进程重启后必须知道"上次全库重写是什么时候"。不持久化的话，每次重启都会立刻触发一轮 CompactAll（因为不知道刚做过），对大副本是不必要的重 IO。

**对齐 Ambry**：Ambry 用 `compactionPolicyInfoV2.json` 做同样的事（`lastCompactAllTime` + `nextRoundIsCompactAllPolicy`）。antfs 简化为只存时间戳（是否该做 CompactAll 由时间差算出，不需要额外 flag）。

**替代方案**：存在索引里 / 存在 master 侧 → 过度复杂，这只是本地调度状态。

### D3：默认策略改为 hybrid

**选择**：新默认 `compaction_policy = "hybrid"`（而非保持 `incremental`）。

**为什么**：hybrid 是 Ambry 验证过的生产策略，cost-benefit 选段更精明（避免"压了但段数没减"的白干），CompactAll 穿插保证碎片不累积。对现有部署的影响：首次升级后第一轮可能触发一次 CompactAll（因为 `COMPACT_POLICY_STATE` 不存在 → 视为"从未做过"），之后正常交替。

**配置兼容**：想保持旧行为只需显式配 `compaction_policy = "incremental"`。

### D4：cost-benefit 的 min_reclaim 参数

**选择**：新增配置 `compaction_min_log_segment_count_to_reclaim`，默认 1。传给 `compact_cost_benefit(policy, min_reclaim)`。

**对齐 Ambry**：`store.min.log.segment.count.to.reclaim.to.trigger.compaction` 默认 1。含义：cost-benefit 选出的区间必须能净减 ≥ 1 个段文件才值得动手。

### D5：策略分发不改变现有闸门逻辑

**选择**：`compact_all` 里的候选集闸门（分区可写态、BOOTSTRAP 跳过、盘健康、墓碑闸门）**不变**——这些与"选哪些段"正交，是"要不要对这个副本动手"的前置判断。策略只影响"动手后用哪种方式压"。

## Risks / Trade-offs

**[首次升级触发 CompactAll]** → 首次升级后 `COMPACT_POLICY_STATE` 不存在，hybrid 会在第一轮就做 CompactAll。对大副本可能是一次较重的 IO。**缓解**：这与 Ambry 的行为一致（Ambry 也是"没有历史记录就先做一次"）；且 CompactAll 走既有崩溃安全路径，只是 IO 重一些，不影响正确性。运维可配 `compaction_policy = "incremental"` 先观察再切换。

**[cost-benefit 选不中任何区间]** → 如果所有段都很满（存活数据多、没什么能净减的），`compact_cost_benefit` 返回 `None`。这是正常的——说明不值得压。hybrid 模式下到了 CompactAll 轮次还是会做全库重写。**不是 bug，是设计。**

**[COMPACT_POLICY_STATE 文件损坏/被删]** → 按"从未做过 CompactAll"处理（下一轮触发全库重写）。安全方向：多做一次全库重写只是费 IO，不丢数据。
