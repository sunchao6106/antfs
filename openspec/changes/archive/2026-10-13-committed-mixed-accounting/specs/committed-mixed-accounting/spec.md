## ADDED Requirements

### Requirement: 混合 committed 口径
`placement_usage()` 计算 committed 时 SHALL 按分区状态分流：RW 副本用 `partition.capacity_bytes`（逻辑上限），RO/SEALED 副本用心跳 `used_bytes`（实际占用，各副本最大值）。`used_bytes` 不可用（`None` 或 `0`）时 MUST 退回 `capacity_bytes`。

#### Scenario: RO 分区用实际占用
- **WHEN** 盘上有 40 个 RO 分区（`capacity_bytes=100G`，compaction 后平均 `used_bytes=40G`）
- **THEN** 这 40 个 RO 副本的 committed 贡献 = 40 × 40G = 1.6T（而非 40 × 100G = 4T）

#### Scenario: SEALED 分区用实际占用
- **WHEN** 盘上有 10 个 SEALED 分区（`capacity_bytes=100G`，`used_bytes=80G`）
- **THEN** 这 10 个 SEALED 副本的 committed 贡献 = 10 × 80G = 0.8T

#### Scenario: RW 分区仍用逻辑上限
- **WHEN** 盘上有 30 个 RW 分区（`capacity_bytes=100G`，当前 `used_bytes=50G`）
- **THEN** 这 30 个 RW 副本的 committed 贡献 = 30 × 100G = 3T（不用 50G）

#### Scenario: 混合盘的 committed 精确累加
- **WHEN** 盘上 30 个 RW（cap=100G）+ 40 个 RO（used=40G）+ 10 个 SEALED（used=80G）
- **THEN** committed = 3T + 1.6T + 0.8T = 5.4T（而非全 capacity 的 8T）

### Requirement: used_bytes 不可用时安全退回
RO/SEALED 副本的 `used_bytes` 不可用时（心跳缺失、leader 切换后 soft state 丢失），committed MUST 退回 `capacity_bytes`。不得退回 0 或其他值。

#### Scenario: leader 切换后退回 capacity_bytes
- **WHEN** leader 切换，soft state 丢失，所有 `used_bytes` 不可用
- **THEN** committed = 所有副本 × capacity_bytes（全保守值）

#### Scenario: 心跳恢复后回到混合值
- **WHEN** leader 切换后第一轮心跳到达，RO/SEALED 的 `used_bytes` 恢复
- **THEN** committed 从全保守值回落到混合值

#### Scenario: 部分副本无上报
- **WHEN** 盘上 10 个 RO 分区，其中 3 个无 `used_bytes` 上报
- **THEN** 有上报的 7 个用 `used_bytes`，无上报的 3 个用 `capacity_bytes`
