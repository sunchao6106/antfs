# master-placement

master 的容量感知、故障域感知 partition→replica 放置与读写路由(承 cluster.md §7.3)。

## ADDED Requirements

### Requirement: 容量感知选盘

master SHALL 依据各盘的实时可用容量选择放置目标:仅选可用容量 ≥ 分区容量上界(`partition_capacity_bytes`)、且未将满(用量 ≤ `disk_near_full_ratio`)的盘;候选不足以满足复制因子时 SHALL 返回明确错误,SHALL NOT 静默降低复制因子。

#### Scenario: 只选容量足够的盘

- **WHEN** 为一个复制因子 N 的分区选盘
- **THEN** 选中的每块盘可用容量 SHALL ≥ 分区容量上界,且用量 SHALL 未超过将满阈值

#### Scenario: 跳过将满的盘

- **WHEN** 某盘用量已超过 `disk_near_full_ratio`
- **THEN** 该盘 SHALL NOT 被选为新分区的放置目标

#### Scenario: 候选不足报错

- **WHEN** 满足条件的候选盘少于复制因子 N
- **THEN** 选盘 SHALL 返回 `InsufficientDisks` 错误,SHALL NOT 用更低副本数建分区

### Requirement: 故障域打散

一个分区的 N 个副本 SHALL 分布在不同故障域(优先不同 rack,至少不同 node),SHALL NOT 把同一分区的两个副本放在同一节点。

#### Scenario: 副本跨节点

- **WHEN** 为一个分区选出 N 块盘
- **THEN** 这 N 块盘 SHALL 分属 N 个不同节点

### Requirement: CreatePartition 确定性 apply

选盘在 leader 提案前完成(读软状态);`CreatePartition` 命令携带已选定的副本集,Raft `apply` SHALL 只做确定性动作(分配单调 id + 写入 placement),SHALL NOT 在 apply 内读取软状态/时钟/随机。

#### Scenario: apply 分配单调 id 并写入放置

- **WHEN** 一条 `CreatePartition` 命令被 apply
- **THEN** 它 SHALL 分配一个单调递增的 partition id 并把 `PartitionInfo`(state=RW + 选定副本)写入 `Partitions`

#### Scenario: 相同命令序列产出相同状态

- **WHEN** 同一 `CreatePartition` 命令序列在任意节点重放
- **THEN** 产出的 `Partitions` 与 id 分配器状态 SHALL 完全一致
