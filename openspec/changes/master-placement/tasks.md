# tasks — master-placement (M2)

## M2a — 放置内核(本期交付)

### 1. 变更文档
- [x] 1.1 `proposal.md` / `design.md` / spec delta 写就,`openspec validate master-placement --strict` 通过。

### 2. 命令与状态机
- [x] 2.1 `types.rs`:`MasterCommand::CreatePartition { replication_factor, replicas }` + `apply`(分配 id + 写 `Partitions`,state=RW);`MasterResponse` 复用 `PartitionId`。
- [x] 2.2 apply 确定性单测(id 单调、placement 写入、跨重放一致)。

### 3. 选盘纯函数
- [x] 3.1 `placement.rs`:`select_disks(nodes, usage, rf, partition_cap, near_full_ratio) -> Result<Vec<ReplicaPlacement>, PlacementError>`;无 IO/时钟/随机。
- [x] 3.2 `PlacementError`(`thiserror`):`InsufficientDisks { needed, available }` 等。

### 4. 配置 / 监控
- [x] 4.1 `config.rs`:`partition_capacity_bytes`(默认 100 GiB,≥1)+ `disk_near_full_ratio`(默认 0.95,(0,1])+ `Validate`。
- [x] 4.2 `metrics.rs`:`master_partitions_created_total` + `master_partition_create_failed_total{reason}`。

### 5. 测试
- [x] 5.1 属性测试(`proptest`):容量阈值、跳过将满、故障域打散、候选不足报错、确定性。
- [x] 5.2 `cargo test` 绿 + `cargo clippy` 无告警。

### 6. 文档
- [x] 6.1 `docs/design/cluster.md` §7.7 增补 M2 配置/监控项;§7.6 M2 checkbox 进度标注。

## M2b — 路由 gRPC
- [x] `AssignPartition`(只回 RW 分区)+ `LookupPartition`(副本位置 + §7.2 门控)。路由由 hard-state placement + 软状态 liveness 直接联表,**未单设 `RoutingIndex` 缓存**(hard state 即路由源,liveness 叠加 up/down;缓存留作优化)。
- [x] gRPC + 3 节点集成测试(注册带盘 + 心跳余量 → CreatePartitions → 跨机架断言 → Assign/Lookup)。

## M2c — Q5 自动扩容
- [x] 后台水位监测自动 `CreatePartition`(仅 leader)+ 护栏(防抖滞回 / 限速 batch / 总量上限);`auto_grow_enabled` 默认关。
- [x] 单测:水位抖动不触发过量建分区(`autogrow::plan_auto_grow` 纯函数:达标返 0、限速、上限)。
