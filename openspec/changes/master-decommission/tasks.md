# tasks — master-decommission (M4:节点下线)

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate master-decommission --strict` 通过。

## 2. 命令与状态机
- [x] 2.1 `types.rs`:`MasterState.decommissioning: BTreeSet<NodeId>` + `DecommissionNode { node_id }` / `RemoveReplica { partition_id, node_id }` 命令 + 确定性 apply(RemoveReplica 幂等)。
- [x] 2.2 apply 单测(下线入集合、移除副本、幂等、跨重放一致)。

## 3. 纯决策函数
- [x] 3.1 `reconcile.rs`:`plan_evacuation(partitions, decommissioning, up_nodes) -> Vec<EvacAction>`(Add/Remove,先补后删)。
- [x] 3.2 单测:healthy<rf→Add;healthy≥rf→Remove;无下线→空;补副本前不删。

## 4. proto + server
- [x] 4.1 `proto`:`DecommissionNode(DecommissionRequest) returns (DecommissionResponse)`(带 version + secret + leader_addr)。
- [x] 4.2 `server.rs`:`DecommissionNode` handler(准入 + leader-only + `client_write`)。
- [x] 4.3 reconciler evacuation:对下线节点分区跑 `plan_evacuation` → Add(选盘排除下线+已有节点)/ Remove(`RemoveReplica`);`master_replicas_removed_total`++。

## 5. 监控 / 文档
- [x] 5.1 `metrics.rs`:`master_replicas_removed_total`。
- [x] 5.2 `docs/design/cluster.md` §7.7 增补 M4 监控;§7.6 M4 checkbox(decommission 已实现;rebalance/EC 推迟)。

## 6. 测试
- [x] 6.1 集成测试:分区放 101/102/103 全存活 → `DecommissionNode(103)` → 先在 104 补副本 → 后移除 103 → 副本 = {101,102,104}。
- [x] 6.2 `cargo test` 绿 + `cargo clippy` 无告警。

## 推迟(M4 后续,另立 change)
- [ ] 容量再平衡:转由 `openspec/changes/balance-and-scrub/` 承接(计划式均衡)。
- [ ] D5 EC 转换钩子 —— **EC 仍锁定**(依赖封存段语义,§5)。
