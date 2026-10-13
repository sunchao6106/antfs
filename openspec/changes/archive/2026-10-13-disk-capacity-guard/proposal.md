## Why

`select_disks` 当前仅凭心跳上报的物理 `free_bytes` 决定是否往一块盘放新分区。segment 按需分配（lazy，每次 4 GiB fallocate）但 partition 容量承诺是预设的（100 GiB），造成物理占用远低于逻辑承诺——master 看到"盘上还有 7 TB 空闲"就持续放 partition，直到这些 partition 逐步写满时盘已装不下，后来的 partition 连第一个段都 fallocate 不出来，浪费分区号和路由条目。同时盘上的 index 文件（Binary Fuse Filter、`.idx`）、`COMPACT_POLICY_STATE`、暂存 `.tmp` 等非 log-segment 开销也没有被选盘和容量计算考虑进去。

## What Changes

- **逻辑承诺硬限制**：`select_disks` 新增过滤条件——`committed + partition_cap <= usable_capacity`，其中 `committed = 该盘上副本数 × partition_cap`，`usable_capacity = disk_capacity × (1 − disk_reserved_ratio)`。`committed` 从 Raft partition table 计算，不依赖心跳，无延迟。
- **磁盘预留空间**：新增配置 `disk_reserved_ratio`（默认 5%），为 index 文件、文件系统元数据、暂存文件等预留空间。预留比例同时影响逻辑承诺上限和物理 `near_full_ratio` 判断。
- **内存即时扣减**：Raft apply `CreatePartition` 后，master 在 soft state 的 `free_bytes` 中立即扣减 `partition_cap`；心跳更新时采用单调递减守卫（只在报的 free 更小时才覆盖），防止被滞后心跳"回滚"。leader 切换后 adjusted_free 丢失但 committed 检查（Raft 状态）仍在。
- **分区级写满感知**：reconciler 巡检时，对每个 RW 分区检查 `max(各副本 used_bytes) >= partition_cap − segment_capacity`，满足则将其写路由权重降至最低或直接转 RO，提前止血，不等 store 侧 507。

## Capabilities

### New Capabilities
- `disk-capacity-guard`: 逻辑承诺检查、磁盘预留空间比例、内存即时扣减、分区级写满感知

### Modified Capabilities

（无已有 spec 需修改）

## 后续优化

- **混合 committed 口径**：对高删除集群，RO/SEALED 副本的 committed 可用心跳 `used_bytes`（实际占用）替代 `capacity_bytes`（逻辑上限），释放 compaction 回收的逻辑配额。需处理 fallback（心跳不可用时退回 `capacity_bytes`）和 leader 切换时 committed 跳变。作为独立 change 在本次上线后实施。

## Impact

- `crates/master/src/placement.rs`：`select_disks` 签名变更（新增 `committed` 参数 + `usable_capacity` 替代 `partition_cap` 做物理上限），所有调用点需适配
- `crates/master/src/server.rs`：reconciler 巡检加分区级写满检测；`placement_usage` 改为携带 `adjusted_free`；`CreatePartition` apply 后扣减 soft state
- `crates/master/src/soft.rs`：`DiskUsage` 加 `adjusted_free` + 单调递减更新逻辑
- `crates/master/src/config.rs`：新增 `disk_reserved_ratio` 配置项
- `docs/design/cluster.md`：配置表 + 指标表更新
- 向后兼容：`disk_reserved_ratio` 默认 5%，新部署即生效；`committed` 检查只要 partition table 存在就自动工作
