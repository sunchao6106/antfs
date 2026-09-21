# master-decommission 设计(M4:节点下线)

承 [`docs/design/cluster.md` §7.3](../../../docs/design/cluster.md)。**先补后删**是本设计的持久性铁律(§3.1)。

## 1. 下线状态(hard state)

`MasterState` 加 `decommissioning: BTreeSet<NodeId>`。`DecommissionNode{node_id}` 命令把节点加入该集合(确定性 apply)。下线中的节点:
- 不再作为新副本 / 再复制 / 补副本的候选(选盘排除)。
- 其承载的副本被逐步迁走。

## 2. 命令(确定性 apply)

```
DecommissionNode { node_id }        → decommissioning.insert(node_id)
RemoveReplica { partition_id, node_id } → partitions[pid].replicas.retain(|r| r.node_id != node_id)
```
`RemoveReplica` 幂等(不存在则无操作)。apply 不读软状态/时钟。

## 3. Evacuation 调谐(先补后删)

reconciler(leader-only,复用 M3 loop)对每个下线中节点承载的分区:

`plan_evacuation(partitions, decommissioning, up_nodes) -> Vec<EvacAction>`,纯函数:
- 对含下线副本的分区,`healthy = replicas.filter(up && !decommissioning).count()`:
  - `healthy < rf` → `EvacAction::Add(pid)`(需在健康节点补一个副本,**先补**)。
  - `healthy >= rf` → 对该分区每个下线副本产 `EvacAction::Remove(pid, node)`(**后删**)。

执行:
- `Add(pid)`:`select_disks` 选盘(**排除该分区已有节点 + 所有下线节点**,只在存活健康节点里选)→ `AddReplica`。
- `Remove(pid, node)`:`RemoveReplica{pid, node}`;`master_replicas_removed_total`++。

铁律:只有健康副本 ≥ RF 时才删下线副本 → 任何时刻冗余不低于 RF(§3.1)。补副本失败(无备选)则本轮不删,等下轮(不破坏冗余)。

> 与 M3 失联再复制的区别:失联是被动(节点已挂,live<rf 触发补);下线是主动(节点还活着,healthy 把下线节点排除在外,从而先补后删地迁移)。二者共用 `AddReplica`/选盘。

## 4. gRPC

`Master` 服务加 `rpc DecommissionNode(DecommissionRequest) returns (DecommissionResponse)`(准入校验 + leader-only,follower 回 leader_addr)。

## 5. 验证(M4)

- **纯函数单测**:`plan_evacuation`——下线节点副本且 healthy<rf → Add;healthy≥rf → Remove;无下线节点 → 空。
- **命令 apply**:`DecommissionNode` 入集合;`RemoveReplica` 移除 + 幂等;跨重放确定。
- **集成测试(3 节点 master + 4 数据节点,无 store server)**:分区放 101/102/103 且全存活 → `DecommissionNode(103)` → reconciler 先在 104 `AddReplica`(healthy 达 3)→ 再 `RemoveReplica(103)` → 分区副本最终 = {101,102,104},且**过程中健康副本数不低于 RF**。
- `cargo test` 绿 + `cargo clippy` 无告警。

## 6. 推迟项(M4 后续)

- **容量再平衡**:检测盘用量不均 → 迁移副本削峰填谷(纯决策可先做,执行依赖 store server)。
- **D5 EC 转换钩子**:封存段转 EC 的 master 侧决策——依赖 store server + 封存段语义(§5),另立 change。
