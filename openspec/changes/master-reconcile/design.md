# master-reconcile 设计(M3)

承 [`docs/design/cluster.md` §7.2/§7.3](../../../docs/design/cluster.md)。master 只**决策 + 下发**,执行在 store server(D1)。

## 1. 状态:target(应然,hard)vs current(实然,soft)

- `target_state`:master 决定,存于 Raft `Partitions[pid].replicas[].target_state`(placement 一部分)。M2/M3 副本目标恒 `ACTIVE`(Q4);新补副本也以 `ACTIVE` 为目标,经 `BOOTSTRAP` 追平。
- `current_state`:节点心跳上报,存内存软状态 `ReplicaCurrent[(pid, node)]`(+ lag)。

## 2. 转移下发(反应式,心跳同步返回)

节点每次心跳带各副本 current_state;`Heartbeat` 处理器:
1. 更新 `ReplicaCurrent`(软状态)。
2. 对该节点承载的每个副本,查 placement 的 target_state;`current != target` → 放进本次 `MasterDirective.transitions`(partition_id, disk_id, target_state)同步回给该节点。

纯逻辑 `plan_transitions(node_id, partitions, reported) -> Vec<Transition>`:遍历 placement 中属于 `node_id` 的副本,reported current 与 target 不符即产 Transition(reported 缺失视为需要转移)。**只对上报的活节点算**(down 节点走再复制)。

## 3. 失联检测 + 再复制(主动式,leader-only 后台 loop)

`reconcile_interval_secs` 周期,仅 leader:
1. **判活**:`last_beat` 超 `heartbeat_timeout_secs` = down。
2. **再复制决策** `plan_rereplication(partitions, down_nodes) -> Vec<PartitionId>`:某分区存活副本数 `< replication_factor` 且存在 down 副本 → 该分区需补 1 个副本。
3. 对每个待补分区:`select_disks` 选盘(**排除该分区已有节点**,故障域打散)取 1 块 → `AddReplica{pid, replica(target=ACTIVE)}` 经 Raft 提交。补上的副本 current 从 OFFLINE→BOOTSTRAP→ACTIVE 由 store server 后续驱动。

> 不删死副本:再复制只**加**健康副本恢复冗余;删除 down 副本 = decommission,归 M4(先保冗余,契合 §3 持久性红线)。

## 4. AddReplica 命令(确定性)

```
MasterCommand::AddReplica { partition_id, replica: ReplicaPlacement }
  → apply: partitions.get_mut(pid)?.replicas.push(replica)  // 幂等性:同 (pid,node,disk) 已存在则不重复加
  → MasterResponse::Registered(或 Empty)
```
选盘在 leader 侧算(读软状态),结果随命令进 Raft;apply 确定(不读软状态/时钟)。

## 5. 配置 / 监控(CLAUDE.md §7)

- `master.reconcile_interval_secs`(u64,默认 5,≥1):调谐/失联检测周期。
- 指标:`master_transitions_issued_total`(counter)、`master_rereplications_total`(counter)。

## 6. 验证(M3)

- **纯函数单测/属性测试**:`plan_transitions`(current≠target 产转移、相等不产、缺失当需转移);`plan_rereplication`(存活<RF 且有 down 副本→需补、存活=RF→不补、全活→不补)。
- **AddReplica apply**:追加副本 + 幂等 + 跨重放确定。
- **集成测试(3 节点 master,无 store server)**:
  - (a) 心跳带一个 `current=BOOTSTRAP`、target=ACTIVE 的副本 → 心跳响应 `transitions` 含该副本的 ACTIVE 目标。
  - (b) 建分区后某数据节点停止心跳(超时)→ reconciler 在该分区补出一个落在**存活**节点的新副本(placement replicas 数增加)。
- `cargo test` 绿 + `cargo clippy` 无告警。
