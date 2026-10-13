## Why

当前 `worker.rs` 的定时 compaction 只有一条路径：`compact_if_needed` → `compact_incremental`（按段死占比选段）。这条路径有两个问题：

1. **选段不够精明**：只看"这个段脏不脏"（废数据占比 ≥ 阈值就选），不考虑"压它划不划算"。一个段废数据 35% 但存活数据搬出去还是要一整个新段来装，净减段数为 0 — IO 白花了，盘空间一点没省（段是预分配定长的，省空间 = 减少文件数）。
2. **缺少周期性彻底清理**：增量 compact 只动脏段，干净段里的零星废数据永远不会被碰到。Ambry 的做法是大多数时候跑 cost-benefit（省 IO），每隔 N 天穿插一次 CompactAll（彻底清理），两者配合才不会让碎片慢慢堆积。

Ambry 已有成熟的 `HybridCompactionPolicy`（`StatsBasedCompactionPolicy` + `CompactAllPolicy` 交替）和 `CompactionManager` 调度模型。antfs 引擎层的 `compact_cost_benefit` 已经实现但没有调用方。本次改造把 Hybrid 策略接入 worker 调度。

## What Changes

- **选段策略升级（`worker.rs`）**：定时 compaction 从固定调 `compact_if_needed`（增量按段死占比）改为**策略可选**：
  - `incremental`（现状）：按段死占比选段，调 `compact_if_needed`；
  - `cost-benefit`：按 cost/benefit 比选最优连续段区间，调 `compact_cost_benefit`；
  - `hybrid`（**新默认**）：大多数轮次走 `compact_cost_benefit`，每隔 `compaction_policy_switch_days`（默认 6 天，对齐 Ambry）穿插一次 `compact()`（CompactAll 全库重写）。
- **Hybrid 状态持久化（`worker.rs` / 磁盘文件）**：每个副本记录"上一次 CompactAll 的时间戳"，持久化到副本数据目录下的一个小文件（对齐 Ambry `compactionPolicyInfoV2.json`），进程重启后读回，避免每次重启都立刻触发全库重写。
- **配置项新增（`config.rs`）**：
  - `compaction_policy`：枚举 `incremental` / `cost-benefit` / `hybrid`，默认 `hybrid`；
  - `compaction_policy_switch_days`：Hybrid 模式下两次 CompactAll 之间的最少天数，默认 6（对齐 Ambry `store.compaction.policy.switch.timestamp.days`）；
  - `compaction_min_log_segment_count_to_reclaim`：cost-benefit 策略的最小回收段数阈值，默认 1（对齐 Ambry `store.min.log.segment.count.to.reclaim.to.trigger.compaction`）。
- **手动 compact 不变**：admin 端点 `POST /admin/partitions/{pid}/compact` 仍走 CompactAll，不受策略配置影响。
- **指标新增**：`store_server_compaction_policy_used`（label `policy`=`incremental`/`cost_benefit`/`compact_all`）counter，每次实际执行 compact 时按实际走的策略计数。

## Capabilities

### New Capabilities

- `compaction-hybrid-scheduling`：Hybrid compaction 调度策略 — 策略选择、CompactAll 穿插、状态持久化、配置。

### Modified Capabilities

（无；不修改已有 spec 的需求层面行为。引擎层 `compact_cost_benefit` 已实现，本次只是在 worker 层接入调用方。）

## Impact

- **crate**：`store`（`worker.rs` 策略分发、`config.rs` 新配置项、指标）。
- **crate 不动**：`blob`（引擎层 `compact` / `compact_cost_benefit` / `compact_if_needed` 已就绪，接口不变）。
- **磁盘**：副本目录下新增一个 `COMPACT_POLICY_STATE` 文件（JSON，~50 字节），记录 `last_compact_all_epoch_secs`。缺失时按"从未做过 CompactAll"处理（首轮即触发一次全库重写，安全方向）。
- **配置兼容**：新配置项都有默认值；不配任何新项时行为从"每天一次增量"变为"每天一次 hybrid（大多数天 cost-benefit，每 6 天一次 CompactAll）"。如需保持旧行为，显式配 `compaction_policy = "incremental"`。
- **持久性**：不改写路径、不改磁盘格式、不改 fsync 边界。策略只影响"选哪些段来压"和"用哪种 compact 方法"，压缩本身走既有崩溃安全实现。
