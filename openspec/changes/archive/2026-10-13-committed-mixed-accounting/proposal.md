## Why

`disk-capacity-guard` 用 `partition.capacity_bytes`（逻辑上限）统一计入所有副本的 committed。对 RO/SEALED 分区，其物理占用只减不增（RO 可被 compaction 缩小，SEALED 永不变），用 capacity_bytes 会多预留——高删除集群中 compaction 大幅缩小了 RO 分区的实际占用，但 committed 仍按满容量算，导致盘逻辑配额被浪费、利用率偏低。

## What Changes

- `placement_usage()` 中 committed 累加逻辑改为混合口径：RW 副本用 `capacity_bytes`，RO/SEALED 副本用心跳 `used_bytes`（实际占用）
- Fallback：`used_bytes` 不可用时退回 `capacity_bytes`（保守方向）
- 不改 `select_disks`、`adjusted_free`、D4 转 RO、D5 router 重试——只改 committed 的累加公式

## Capabilities

### New Capabilities
- `committed-mixed-accounting`: RO/SEALED 副本的 committed 用实际占用替代逻辑上限

### Modified Capabilities

（无已有 spec 需修改）

## Impact

- `crates/master/src/server.rs`：`placement_usage()` 中 committed 累加逻辑
- 改动面极窄：一个函数内的累加公式变更 + fallback 分支
