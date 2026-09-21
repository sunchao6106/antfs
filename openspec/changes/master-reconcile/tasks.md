# tasks — master-reconcile (M3)

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate master-reconcile --strict` 通过。

## 2. proto
- [x] 2.1 `NodeBeat` 加 `repeated ReplicaBeat replicas`(partition_id / disk_id / current_state / lag_bytes)。
- [x] 2.2 `MasterDirective` 加 `repeated Transition transitions`(partition_id / disk_id / target_state)+ `repeated Task tasks`(再复制提示)。

## 3. 命令与状态机
- [x] 3.1 `types.rs`:`MasterCommand::AddReplica { partition_id, replica }` + 确定性 apply(追加,幂等)。
- [x] 3.2 apply 单测(追加、幂等、跨重放一致)。

## 4. 软状态
- [x] 4.1 `soft.rs`:`ReplicaCurrent` map<(PartitionId,NodeId),(current_state,lag)>;心跳写、查询读;`record_replicas`。

## 5. 纯决策函数
- [x] 5.1 `reconcile.rs`:`plan_transitions(node, partitions, reported) -> Vec<Transition>`。
- [x] 5.2 `reconcile.rs`:`plan_rereplication(partitions, down_nodes) -> Vec<PartitionId>`。
- [x] 5.3 属性/单测:转移(≠产/=不产/缺失当需转移)、再复制(<RF 且有 down→补 / =RF→不补 / 全活→不补)。

## 6. server 接线
- [x] 6.1 `Heartbeat` 处理:更新 `ReplicaCurrent` + 回 `transitions`(plan_transitions);`master_transitions_issued_total`++。
- [x] 6.2 leader-only 后台 reconciler loop(`reconcile_interval_secs`):失联检测 → `plan_rereplication` → 选盘(排除已有节点)→ `AddReplica`;`master_rereplications_total`++。

## 7. 配置 / 监控 / 文档
- [x] 7.1 `config.rs`:`reconcile_interval_secs`(默认 5,≥1)+ `Validate`。
- [x] 7.2 `metrics.rs`:`master_transitions_issued_total` + `master_rereplications_total`。
- [x] 7.3 `docs/design/cluster.md` §7.7 增补 M3 配置/监控;§7.6 M3 checkbox 勾。

## 8. 测试
- [x] 8.1 集成测试:(a) 心跳带 current≠target → 响应含 transition;(b) 节点停心跳 → 分区补出存活节点新副本。
- [x] 8.2 `cargo test` 绿 + `cargo clippy` 无告警。
