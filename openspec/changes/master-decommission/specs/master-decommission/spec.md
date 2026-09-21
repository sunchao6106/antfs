# master-decommission

master 主动节点下线的安全副本迁移(先补后删,守 §3.1 持久性红线)。

## ADDED Requirements

### Requirement: 标记节点下线

master(leader)SHALL 支持把一个节点标记为"下线中"(hard state);下线中的节点 SHALL NOT 再被选为新副本 / 补副本的目标。标记 SHALL 经准入校验且 leader-only(follower 回 leader 地址重定向)。

#### Scenario: 下线节点不再作为放置目标

- **WHEN** 一个节点被标记为下线中
- **THEN** 后续选盘(建分区 / 再复制 / evacuation 补副本)SHALL NOT 选中该节点

### Requirement: 先补后删的安全迁移

对下线中节点承载的分区,master SHALL **先**在健康节点(存活且非下线)补足到复制因子,**再**移除下线节点上的旧副本;当健康副本数 < 复制因子时 SHALL NOT 移除任何旧副本(任何时刻冗余不低于 RF,§3.1)。

#### Scenario: 补足后才移除旧副本

- **WHEN** 一个 RF=3 分区的 1 个副本在下线中的节点上(健康副本=2 < 3)
- **THEN** master SHALL 先在一个健康节点上 `AddReplica`,待健康副本达到 3 后,SHALL 再移除下线节点上的旧副本

#### Scenario: 补副本失败不删旧副本

- **WHEN** 没有可用的健康备选盘来补副本
- **THEN** master SHALL NOT 移除下线节点上的旧副本(保持冗余,待下一轮重试)

### Requirement: RemoveReplica 确定性 apply

`RemoveReplica` 命令 SHALL 确定性地从分区 placement 移除指定节点的副本,重复移除同一副本 SHALL 幂等,apply SHALL NOT 读软状态/时钟/随机。

#### Scenario: 移除副本且幂等

- **WHEN** 对某分区 `RemoveReplica` 一个已存在的副本,随后重放同一命令
- **THEN** 该副本 SHALL 被移除一次,重放 SHALL 不产生错误或额外副作用
