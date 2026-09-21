## Why

`store` 单机引擎已成型,但 store 之上的**集群/服务层**尚无实现。集群大脑(partition→replica 放置、节点/副本状态机、故障处置)是承载千亿(10¹¹)小文件、多节点可扩展/可用的前提。

选型已在 [`docs/design/cluster.md`](../../../docs/design/cluster.md) 锁定(D1–D5)并对 master 细化设计(§7)做完探索。本 change **固化这些决策并落地 master 节点的第一阶段(M1 骨架)**:内建 Raft(取代 Ambry 的 Helix+ZK / SDFS 的 MySQL+ZK 重外部依赖),自包含的集群元数据大脑。

本 change 落地 M1(骨架:Raft 集群 + 节点自注册 + 单调 id + 控制面 gRPC),M2–M4(放置 / 调谐 / EC 钩子)在 [`tasks.md`](./tasks.md) 列出但分期实现。

## What Changes

新增 `crates/master`(数据节点之上的集群协调进程),M1 范围:

- **内建 Raft(D1)**:用 [`openraft`](https://docs.rs/openraft) 起多节点、选主、成员变更;**只管集群元数据**,不碰数据副本一致性(D4)。
- **Raft 边界最小化(Q2)**:进 Raft 状态机(hard state)的仅 `IdAllocator` / `Nodes` / `Partitions` / `Policy`;liveness / 磁盘余量 / 实际副本态全是心跳重建的**软状态**(内存)。
- **Raft 持久化后端(Q3)**:log + 状态机快照落 [`redb`](https://docs.rs/redb)(纯 Rust 嵌入式、崩溃安全);**不上 RocksDB**(规模估算 ~20MB,嵌入式 KV 足够)。
- **节点自注册(Q1)**:数据节点启动后经心跳自报(节点身份 + 各盘 `statfs` 探到的**真实容量**),master 固化进 `Nodes`(不采 Ambry 写死 HardwareLayout 配置的做法)。注册需**准入身份校验**(防乱入,安全点)。
- **单调 partition_id 分配**:经 Raft 提案分配全局单调 id(类 SeaweedFS `MaxVolumeId`)。
- **控制面 gRPC(D2)**:`tonic` + `prost`,proto **带版本字段**;M1 暴露 `Heartbeat(stream)` / `RegisterNode` / `ListNodes` / `GetClusterState`。master↔master 的 Raft 内部 RPC 走单独的 `RaftInternal` gRPC 服务。**禁**自研二进制 TCP 协议。
- **软状态重建**:新 leader 上任后一个心跳周期内从心跳重建 `Liveness` / `DiskUsage`(内存,`ArcSwap`/`RwLock`)。

M1 **明确不做**:partition→replica 放置、容量感知选盘、副本状态转移调谐、数据面 HTTP、拉复制、EC(分别属 M2/M3/M4 与 store server change)。

## Capabilities

### New Capabilities

- `master-raft-core`: master 的 Raft 内核——`RaftTypeConfig`、log storage + state machine over `redb`、tonic 传输的 `RaftNetwork`、集群 bootstrap / 成员变更 / 选主、client_write 提案路径;hard-state 数据模型(`IdAllocator`/`Nodes`/`Partitions`/`Policy`)与 Q2 边界。
- `master-control-plane`: 控制面 gRPC 表面——节点自注册(Q1,含准入校验)、双向流心跳协议(`NodeBeat`↔`MasterDirective`,M1 仅上行 liveness/容量、下行暂空)、软状态重建(`Liveness`/`DiskUsage`)、`ListNodes`/`GetClusterState` 只读查询;proto 版本化(D2)。

### Modified Capabilities

<!-- 无既有 spec;master 层全部为新建。store 引擎不改。 -->

## Impact

- **新 crate**:`crates/master`,加入 workspace `members`。
- **新依赖**:`openraft`(版本落地前核对当前稳定版并 pin)、`redb`、`tonic` + `prost` + `tonic-build`(`build.rs` 编 proto)、`tokio`;复用 `common/config`(`serde`+`config` 分层配置)、`common/metric`(`prometheus` 打点)、`tracing`、`thiserror`。
- **proto**:新增 `crates/master/proto/master.proto`(控制面 + `RaftInternal`),消息带 `version` 字段(§3.4 前后兼容纪律)。
- **文档**:`docs/design/cluster.md` 补 M1 的「配置项清单」+「监控项清单」两张表(CLAUDE.md §7 必须项),并翻 §7.6 M1 checkbox;§6 D1 目标随实现推进标注。
- **store 引擎**:M1 不改动 store。store server(数据节点外壳)是后续独立 change(见 cluster.md §9,依赖本 change 的 gRPC/心跳协议)。
- **保留契约**:Raft 只用于集群元数据(D4,不做每 data-partition Raft);Raft 状态机快照以 `redb` 事务提交为持久化边界;proto/磁盘格式前后兼容(§3.4)。

## Non-goals

- 不实现数据面(put/get HTTP)、拉复制、副本状态转移执行——那是 store server change。
- 不实现放置算法 / 容量感知选盘(M2)、调谐循环(M3)、再平衡/EC 钩子(M4)。
- 不实现 admin web(Q6,独立服务,后续)。
- 不做多 DC / LEADER 区分(Q4:副本统一 `ACTIVE`)。
