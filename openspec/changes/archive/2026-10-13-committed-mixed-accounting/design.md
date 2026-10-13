## Context

`disk-capacity-guard` 已上线，committed 统一用 `partition.capacity_bytes`。对高删除集群，RO/SEALED 分区经 compaction 后物理占用远小于 `capacity_bytes`，但 committed 仍按满容量计 → 盘逻辑配额浪费。

RO 分区物理占用只减不增（不接受新写入，compaction 只缩不涨）；SEALED 分区物理占用恒定不变（不入 compaction 候选）。两者的 `used_bytes`（心跳上报）是安全的承诺上界。

`used_bytes` 来自 soft state（心跳），leader 切换时丢失。需要 fallback 到 `capacity_bytes`（Raft 状态，零丢失）。

## Goals / Non-Goals

**Goals:**
- G1：RO/SEALED 副本的 committed 用实际占用，释放 compaction 回收的逻辑配额
- G2：`used_bytes` 不可用时安全退回 `capacity_bytes`
- G3：不改 `select_disks`、`adjusted_free`、D4、D5 等已有机制

**Non-Goals:**
- 不改心跳协议
- 不改 RW 副本的 committed 口径（终态写满，必须用 `capacity_bytes`）
- 不解决 leader 切换时 committed 跳变（保守方向，不危险）

## Decisions

### D1 混合 committed 口径

**选择**：`placement_usage()` 中 committed 累加按分区状态分流：

```rust
for (_, partition) in &state.partitions {
    for replica in &partition.replicas {
        let cap = effective_capacity(partition, config);
        let contribution = match partition.state {
            PartitionState::ReadWrite => cap,
            PartitionState::ReadOnly | PartitionState::Sealed => {
                // used_bytes: 各副本最大值（已有 used_bytes_by_partition）
                match used_map.get(&pid) {
                    Some(&used) if used > 0 => used,
                    _ => cap,  // fallback: 心跳不可用 → 保守按满容量
                }
            }
        };
        *committed.entry((replica.node_id, replica.disk_id.clone()))
            .or_default() += contribution;
    }
}
```

**Fallback 逻辑**：`used_bytes` 不可用（`None` 或 `0`）→ 退回 `capacity_bytes`。对 RO/SEALED 来说 `used_bytes == 0` 只可能是心跳缺失（它们从 RW 转来时一定有数据），不存在"真空"场景。

**leader 切换行为**：soft state 丢失 → 所有 RO/SEALED 的 `used_bytes` 不可用 → 全部 fallback 到 `capacity_bytes` → committed 跳到最大值 → 心跳恢复后回落。跳变方向是**保守**的（多预留，不超卖），几秒后自动恢复。

**替代方案**：
- (a) 把 `used_bytes` 持久化到 Raft → 改动量大，心跳数据不适合进 Raft（高频、非确定性）
- (b) fallback 用 0 → 危险：leader 切换后 committed 归零 → 超卖

### D2 `used_bytes` 复用已有基础设施

`SoftState::used_bytes_by_partition()` 已返回 `BTreeMap<PartitionId, u64>`（各副本最大值）。`placement_usage()` 调用它一次，传入 committed 累加逻辑。不新增字段或方法。

## Risks / Trade-offs

- **[leader 切换 committed 跳变]** → 从混合值跳到全 `capacity_bytes`，几秒后心跳恢复。保守方向，不超卖。可能短暂拒绝本可放的 partition → 可接受。
- **[used_bytes 精度]** → 心跳延迟（~3 秒）。RO/SEALED 的 used_bytes 只减不增，延迟意味着报的值略高于实际 → 保守方向。
- **[compaction 期间临时占用]** → compaction 中旧+新同时在盘上，`used_bytes` 可能短暂偏高 → 更保守，不危险。compaction 完成后下一轮心跳恢复正常值。
