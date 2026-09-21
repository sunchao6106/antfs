# master-reconcile

master 的副本状态调谐:current→target 转移下发、失联检测、再复制补副本(承 cluster.md §7.2/§7.3)。

## ADDED Requirements

### Requirement: 副本状态转移下发

master SHALL 依据节点心跳上报的副本 `current_state` 与 placement 的 `target_state` 差异,向该节点下发状态转移指令(`current != target` 时);状态一致时 SHALL NOT 下发。master 本身 SHALL NOT 执行转移或搬运数据(只决策下发)。

#### Scenario: current 与 target 不一致下发转移

- **WHEN** 节点心跳上报某副本 `current_state=BOOTSTRAP`,而 placement 中该副本 `target_state=ACTIVE`
- **THEN** master SHALL 在心跳响应里包含该副本的转移指令(目标 ACTIVE)

#### Scenario: current 与 target 一致不下发

- **WHEN** 节点上报的副本 current 与 target 相同
- **THEN** master SHALL NOT 为该副本下发转移指令

### Requirement: 节点失联检测

master SHALL 把心跳超过 `heartbeat_timeout_secs` 未到的节点视为失联(down),据此判断其承载副本不可用。判活基于软状态,SHALL NOT 依赖 Raft 复制。

#### Scenario: 超时判失联

- **WHEN** 某节点在超时时间内未发送心跳
- **THEN** master SHALL 将其视为 down,其承载的副本计入不可用

### Requirement: 再复制恢复冗余

当某分区存活副本数低于其复制因子且存在失联副本时,master(leader)SHALL 选一块新盘(排除该分区已有节点、故障域打散)追加一个目标为 ACTIVE 的副本以恢复冗余;SHALL NOT 删除失联副本(先保冗余,删除属 decommission/M4)。

#### Scenario: 失联导致副本不足则补副本

- **WHEN** 一个 RF=3 的分区有 1 个副本所在节点失联(存活副本降至 2)
- **THEN** leader SHALL 通过 `AddReplica` 在一个存活且不同的节点上追加一个新副本,使 placement 副本数增加

#### Scenario: 副本充足不补

- **WHEN** 分区所有副本节点均存活(存活副本数 = 复制因子)
- **THEN** master SHALL NOT 追加副本

### Requirement: AddReplica 确定性 apply

选盘在 leader 提案前完成;`AddReplica` 命令携带已选定副本,Raft `apply` SHALL 只做确定性动作(向 placement 追加副本),SHALL NOT 在 apply 内读软状态/时钟/随机;重复追加同一 (partition, node, disk) SHALL 幂等。

#### Scenario: apply 追加副本

- **WHEN** 一条 `AddReplica` 命令被 apply
- **THEN** 对应分区的 placement 副本列表 SHALL 增加该副本,且相同命令在任意节点重放产出一致结果
