## Why

M1–M3 让 master 具备完整控制面:选主、注册、放置、路由、自动扩容、副本调谐 + 失联再复制。M4 补上**主动节点下线(decommission)**:运维要退役一台机时,master 需把它承载的副本**安全迁走**——**先在健康节点补够副本,再移除下线节点上的旧副本**(守 §3.1 持久性红线:任何时刻冗余不低于安全线,绝不先删后补)。

下线决策仍在 master 侧,**不依赖 store server**:master 决定"补哪、删哪",实际数据搬运由 store server 后续执行。M4 的其余项(容量再平衡、D5 EC 转换钩子)**推迟**:EC 需要 store server + 封存段语义(§5,自有设计),再平衡优先级更低。

## What Changes

- **`DecommissionNode` gRPC + Raft 命令**:标记一个节点为"下线中"(hard state `decommissioning` 集合)。
- **`RemoveReplica` Raft 命令**:从分区 placement 移除一个副本(确定性 apply)。
- **调谐循环扩展(evacuation)**:对下线中节点承载的每个分区——**健康副本(存活且非下线)< RF 时先 `AddReplica` 补(排除下线节点)**;**健康副本 ≥ RF 后才 `RemoveReplica` 移除下线节点上的旧副本**。
- **新指标** `master_replicas_removed_total`。

**M4 本 change 不做**:容量再平衡、D5 EC 转换钩子(留后续,EC 依赖 store server);副本搬运的**执行**(store server)。

## Capabilities

### New Capabilities

- `master-decommission`: 节点下线的安全副本迁移——`DecommissionNode`/`RemoveReplica` 命令、evacuation 调谐(先补后删)、下线状态 hard state。

### Modified Capabilities

<!-- 复用 master-reconcile 的再复制机制;本 change 新增 decommission 能力。 -->

## Impact

- **crate**:`crates/master` `types.rs` 加 `decommissioning` 集合 + `DecommissionNode`/`RemoveReplica` 命令;`reconcile.rs` 加 `plan_evacuation`;`server.rs` 加 `DecommissionNode` handler + evacuation 接线;`proto` 加 `DecommissionNode` RPC。
- **决策**:先补后删(add-before-remove),守持久性红线;补副本排除下线节点 + 已有节点(故障域打散);均 leader-only + 准入校验。
- **保留契约**:apply 确定(命令携带 leader 侧选好的副本);冗余不低于 RF 才删旧副本;Raft 边界不变(Q2)。

## Non-goals

- 不实现容量再平衡、D5 EC 转换钩子(后续;EC 依赖 store server)。
- 不实现副本搬运执行 / 拉复制(store server)。
- 不改动 M1–M3 已定契约。
