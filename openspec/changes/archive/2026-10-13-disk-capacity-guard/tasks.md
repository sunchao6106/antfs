## 1. 配置层：`disk_reserved_ratio` + `segment_capacity_bytes` + `adjusted_free_timeout_secs`

- [x] 1.1 在 `crates/master/src/config.rs` 的 `MasterConfig` 新增 `disk_reserved_ratio: f64`（默认 0.05，范围 0.0..1.0）、`segment_capacity_bytes: u64`（默认 4 GiB，对齐 store 侧 `store.segment_size_bytes`）、`adjusted_free_timeout_secs: u64`（默认 120）
- [x] 1.2 在 `MasterConfig::validate()` 校验 `disk_reserved_ratio`（`0.0 ≤ ratio < 1.0`）、`segment_capacity_bytes ≥ 1`、`adjusted_free_timeout_secs ≥ 1`
- [x] 1.3 补单测：默认值正确、非法范围拒绝

## 2. Raft 状态：`PartitionInfo` 新增 `capacity_bytes` + 逻辑承诺检查

- [x] 2.1 在 `crates/master/src/types.rs` 的 `PartitionInfo` 新增 `capacity_bytes: u64` 字段
- [x] 2.2 在 `CreatePartition` 的 apply 逻辑中，从 `MasterCommand` 新增的 `partition_capacity` 字段（由 leader 提案时从当前 config 快照写入）设置 `capacity_bytes`
- [x] 2.3 兼容：已有 partition（无 `capacity_bytes`）反序列化时用 `config.partition_capacity_bytes` 作为默认值（`#[serde(default)]` + `placement_usage` 回退）
- [x] 2.4 在 `placement.rs` 的 `DiskFree` 中新增 `committed_bytes: u64` 和 `usable_capacity: u64`
- [x] 2.5 在 `select_disks` 的候选过滤中新增逻辑承诺检查：`committed_bytes + partition_cap > usable_capacity` 时跳过
- [x] 2.6 改造 `server.rs` 的 `placement_usage()` 签名为 `(&SoftState, &MasterState, &MasterConfig)`：遍历 Raft partition table 用 `partition.capacity_bytes` 累加 committed（RW + RO + SEALED 全计入，DROPPED 已从 table 移除自然不计入），计算 `usable_capacity = disk_capacity × (1 − reserved_ratio)`，读取 `adjusted_free`
- [x] 2.7 适配 `placement_usage()` 的所有调用点，传入 Raft state 和 config（含 balance planner：`BalanceInputs` 新增 `reserved_ratio`，`plan_balance` 自行从 partition table 算 committed）
- [x] 2.8 补 `select_disks` 单测：盘逻辑写满拒绝、盘有余量允许、空盘正常、分区删除释放配额、RO/SEALED 分区仍计入 committed、不同 capacity_bytes 的 partition 混合累加正确
- [x] 2.9 补 proptest：committed 检查不变量——选出的盘 committed + partition_cap ≤ usable_capacity

## 3. 内存即时扣减：`adjusted_free` + 单调递减守卫

- [x] 3.1 在 `soft.rs` 的 `DiskUsage` 新增 `adjusted_free: u64` 和 `last_heartbeat_time: Instant` 字段；保留原始 `free_bytes` 字段（监控/观测用）
- [x] 3.2 改造 `record_beat` 为 merge 语义（不再直接 `insert` 覆盖整个 `Liveness`）：`capacity_bytes` 和 `health` 总用最新心跳值；`free_bytes` 存最新心跳原始值；`adjusted_free` 按单调递减守卫更新——`reported_free < adjusted_free` 则覆盖，`reported_free >= adjusted_free` 且未超时则保持，超过 `adjusted_free_timeout`（默认 120 秒）则重置为心跳值；首次心跳 `adjusted_free = free_bytes`
- [x] 3.3 `placement_usage()` 中 `DiskFree.free_bytes` 填入 `adjusted_free`（物理空间检查用 adjusted 值）
- [x] 3.4 建分区后扣减对应盘的 `adjusted_free`：`adjusted_free = adjusted_free.saturating_sub(partition_cap)`。**实现位置偏离原稿**：放在 leader 侧 `server.rs::place_one_partition` 中 `client_write(CreatePartition).await == Ok` 之后，而非 Raft apply 回调——`RedbStateMachine::apply` 拿不到 `SoftState`（apply 只依赖命令本身），`place_one_partition` 是唯一生产建分区路径且仅 leader 执行，等价。见 `design.md` D3 第 2 条。
- [x] 3.5 补 SoftState 新方法 `deduct_disk_free(node, disk, amount)` 供扣减路径调用
- [x] 3.6 补单测：连续扣减递减、心跳不回滚（未超时+心跳更大→保持）、心跳向下更新（心跳更小→覆盖）、超时后接受心跳值、首次心跳初始化

## 4. 分区级写满感知

- [x] 4.1 在 reconciler 巡检循环中（`server.rs` 的 reconcile tick），对每个 RW 分区计算 `max_used = max(各副本 used_bytes)`
- [x] 4.2 当 `max_used >= partition.capacity_bytes − segment_capacity` 时，提交 `SetPartitionState(RO)` 并记日志（含 partition_id、max_used、阈值）；用 `partition.capacity_bytes`（per-partition 值），不用全局 config
- [x] 4.3 跳过已是 RO/SEALED 的分区；`used_bytes` 无上报时视为 0 不触发
- [x] 4.4 新增指标 `master_partitions_full_ro: IntCounter`（因写满而转 RO 的次数）
- [x] 4.5 补单测：接近写满触发转 RO、远未满不触发、已 RO 不重复处理、无上报不误判

## 5. Router 侧 StoreFull 重试

- [x] 5.1 在 `crates/common/router/src/lib.rs` 中实现 `evict_partition(pid)`：从本地号段缓存中移除 partition pid 的所有剩余号段
- [x] 5.2 改造 `put_file_with_ttl`：全副本 507 时调用 `evict_partition` → 重新 `allocate_one` → 重试 PUT（最多 2 次重试）
- [x] 5.3 改造 `put_with_ttl`：全副本 507 时调用 `evict_partition` → 返回 `Error::StoreFull`（调用方需重新 allocate；不改 `is_retryable`）
- [x] 5.4 新增指标 `router_put_store_full_retries: IntCounter`（StoreFull 触发的重试次数）
- [x] 5.5 补单测：`put_file` 碰到 507 自动重试成功、`put` 碰到 507 踢掉 partition 返回错误、evict 后 allocate 选到不同 partition

## 6. 文档

- [x] 6.1 更新 `docs/design/cluster.md` §9 配置表：新增 `disk_reserved_ratio`、`segment_capacity_bytes`、`adjusted_free_timeout_secs` 三行
- [x] 6.2 更新 `docs/design/cluster.md` 指标表：新增 `master_partitions_full_ro`、`router_put_store_full_retries` 行
- [x] 6.3 `devcluster.toml.example` 补新配置项（注释状态）

## 7. 集成测试与验证

- [x] 7.1 新建 `crates/master/tests/capacity_guard.rs`：模拟多分区放置直到 committed 上限，验证 `select_disks` 拒绝
- [x] 7.2 测试 adjusted_free 扣减 + 心跳交互：建分区后 adjusted_free 下降、守卫活跃期心跳不回滚、守卫不活跃时接受心跳
- [x] 7.3 回归：`cargo test -p antfs-master` 全绿
- [x] 7.4 回归：`cargo test -p antfs-router` 全绿（router 改动）
- [x] 7.5 `cargo test` 全绿 + `cargo clippy` 无告警
