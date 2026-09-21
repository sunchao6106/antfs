## Why

M1(`master-node`)已落地 master 骨架:Raft 集群、节点自注册、单调 partition id、控制面 gRPC。但 master 还**不会放置分区** —— `Partitions` hard state 恒空,数据节点注册后无事可做。M2 补上 master 的核心职责:**partition→replica 放置 + 读写路由**(承 [`docs/design/cluster.md` §7.3](../../../docs/design/cluster.md))。

放置全部在 master 侧完成,**不依赖 store server**:用注册的节点/盘信息 + 心跳上报的实时余量(软状态)驱动;store server 后续 change 只需按 master 写下的 placement 承载副本。

本 change 落地 **M2a(放置内核)**;M2b(路由 gRPC)、M2c(Q5 自动扩容)在 tasks.md 列出,分步实现。

## What Changes

- **`CreatePartition` Raft 命令**:分配单调 id + 写入 `Partitions` placement(state=RW,replicas 带 target_state);确定性 apply(具体选盘在 leader 提案前算好,命令里带定选中的副本,保 apply 跨节点确定)。
- **容量感知选盘(纯函数)**:从软状态各盘 `free_bytes` 里,选**余量 ≥ 分区上界、彼此相近**的 N 块盘,按故障域(dc/rack)打散;**盘将满(≥ `near_full_ratio`)跳过**,不放新分区;候选不足 → 明确错误(§7.3)。
- **新配置** `master.partition_capacity_bytes`(分区容量上界,选盘阈值依据)+ `master.disk_near_full_ratio`(盘将满阈值,对齐 Ambry ≈95%)。
- **新指标** `master_partitions_created_total` / `master_partition_create_failed_total`。

M2b(本 change 后续):`CreatePartitions`/`LookupPartition`/`AssignPartition` gRPC + `RoutingIndex` 软状态心跳重建。
M2c(本 change 后续):Q5 自动扩容水位 loop + **防抖/限速/上限**护栏。

**M2 不做**:副本状态转移执行(M3)、数据面/拉复制(store server change)、再平衡/EC(M4)。

## Capabilities

### New Capabilities

- `master-placement`: 容量感知、故障域感知的 partition→replica 放置;`CreatePartition` 命令与确定性 apply;选盘算法(阈值/打散/跳过将满/不足报错);读写路由(`LookupPartition`/`AssignPartition`,M2b)与自动扩容护栏(M2c)。

### Modified Capabilities

<!-- 复用 M1 的 master-raft-core / master-control-plane;本 change 只新增 placement 能力。 -->

## Impact

- **crate**:`crates/master` 新增 `placement.rs`(纯逻辑);`types.rs` 扩 `MasterCommand::CreatePartition`;`config.rs`/`metrics.rs` 增字段;M2b 扩 `server.rs`/`proto`。
- **决策**:采纳 M2a=先纯逻辑;`partition_capacity_bytes` 默认 100 GiB(对齐 §7.3「100GB/16TB」例子);放置在 leader 侧算(软状态非确定,不能进 apply)。
- **保留契约**:apply 确定性(选盘结果随命令进 Raft,apply 不读软状态/时钟);Raft 边界不变(Q2)。

## Non-goals

- 不实现 store server / 数据面 / 拉复制。
- 不实现副本状态机转移执行(M3)、再平衡 / EC(M4)。
- 不改动 M1 已定契约。
