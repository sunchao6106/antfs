# master-node 设计(M1 定案)

本文把 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §7 的探索产出**定案为可实现的 M1 设计**,并归档 Q1–Q7 / N1–N4 的最终决策(含被否决方案)。M2–M4 只列边界,细节留各自实现期。

## 1. 定案决策(归档)

| 决策 | 定案 | 被否决 / 备选 | 理由 |
|---|---|---|---|
| **D1 集群大脑** | 内建 Raft(`openraft`),自包含 | Ambry Helix+ZK;SDFS MySQL+ZK | 去外部协调器,运维轻(取 SeaweedFS/CubeFS) |
| **Q2 Raft 边界** | 仅 `IdAllocator`/`Nodes`/`Partitions`/`Policy` 进 Raft(hard);其余软状态 | CubeFS 全量塞 raft+RocksDB | 规模 ~20MB,最小化 hard state(取 SeaweedFS 仅 MaxVolumeId 精神) |
| **Q3 Raft 后端** | `redb`(纯 Rust 嵌入式) | RocksDB;纯内存+快照 | 崩溃安全 + 无 C++ 依赖;规模无需 RocksDB |
| **Q1 节点识别** | 自注册:心跳自报身份 + `statfs` 真实容量,master 固化;**需准入校验** | Ambry 写死 HardwareLayout 配置 | 取 SeaweedFS「目录声明 + 运行时探测」;容量真实、可自恢复 |
| **Q4 副本区分** | 不区分,统一 `ACTIVE`(不选 leader) | Ambry LEADER/STANDBY 分级 | 单 DC 无需;多 DC 省带宽再议 |
| **Q5 分区扩容** | 自动扩容:master 监测水位自动建分区(M2)+ `CreatePartitions` 机制 | 纯手动 | 运维少人工;⚠ 需防抖/限速/上限 |
| **Q7 blobId 对象标识** | `partitionId` + objectId;objectId **不锁为 uuid** | 固定 128-bit uuid | 形式(随机/单调/其他)留实现定 |
| **N2 拉复制归属** | 内嵌 store server | 独立进程 | 对齐 Ambry ReplicationManager,同进程贴本地副本 |

> D2/D3/D5/N1/N3/N4 见 cluster.md §2/§8/§5/§9,本 change M1 只依赖 D1/D2 与 Q1/Q2/Q3。

## 2. Raft hard state 数据模型(进状态机)

对齐 cluster.md §7.1。**只有"必须一致 + 需 failover 存活"的进 Raft**:

```
IdAllocator   { next_partition_id: u64 }                 // 单调,类 SeaweedFS MaxVolumeId
Nodes         map<NodeId, {                               // 自注册后固化的身份 + 容量
                  dc, rack, addr,
                  disks: map<DiskId, { capacity_bytes, tag }>,
                  registered_at,
              }>
Partitions    map<PartitionId, {                          // M1 空;M2 起写入
                  replication_factor,
                  state: RW | RO | SEALED,
                  replicas: [ { node_id, disk_id, target_state } ],  // ideal placement(应然)
              }>
Policy        { default_replication_factor, placement_constraints }
```

**Raft 应用命令(client_write 提案)**——M1 只实现前两个:

```
enum MasterCommand {
    AllocatePartitionId,                        // → 返回 next_partition_id 并自增
    RegisterNode { node_id, dc, rack, addr, disks },  // 幂等:重复注册覆盖身份(容量以最新为准)
    // M2+: CreatePartition{...}, SetPartitionState{...}, UpdatePlacement{...}
}
enum MasterResponse { PartitionId(u64), Registered, ... }
```

- **幂等**(CLAUDE.md §3.2):`RegisterNode` 以 `node_id` 为键覆盖,重放/重连安全;`AllocatePartitionId` 通过 Raft 线性化,不重不漏。
- 状态机 apply 必须**确定性**:同一 log 序列在任意节点产出同一状态(不依赖本地时钟/随机;`registered_at` 由 leader 在提案里带入,不在 apply 里取 `now()`)。

## 3. 软状态(内存,心跳重建)

对齐 §7.1 soft state。**不进 Raft**,新 leader 上任后一个心跳周期重建:

```
Liveness       map<NodeId, { last_beat: Instant, up: bool }>
DiskUsage      map<(NodeId, DiskId), { free_bytes, load }>
// M2+: ReplicaCurrent, RoutingIndex
```

- 存储:`ArcSwap<Snapshot>` 或 `RwLock<HashMap>`(共享可变状态必须同步,CLAUDE.md §2 并发)。M1 读多写少(心跳周期写),用 `RwLock` 起步。
- 失联检测(M3)先不做;M1 仅记录 `last_beat`,`ListNodes` 据此标 up/down 供观测。

## 4. openraft 集成(M1 内核)

> 具体 trait 方法签名以落地时核对的当前 openraft 稳定版为准(实现前先核 docs.rs / 官方 examples,API 跨版本变动大)。

- `RaftTypeConfig`:`D = MasterCommand`、`R = MasterResponse`、`NodeId = u64`、`Node = { addr, ... }`、其余按当前版本 `declare_raft_types!` 要求填。
- **log storage + state machine**:落 `redb`。log entries 一张表(key=log index)、状态机快照 + applied 状态一张表;每次 apply 在一个 `redb` 写事务里提交(**事务提交 = 持久化边界**,守 §3.1 持久性)。崩溃后从 redb 恢复 last-applied + 重放。
- **网络层**:`RaftNetwork`/`RaftNetworkFactory` 用 tonic 客户端调对端 `RaftInternal` gRPC 服务(append_entries / vote / install_snapshot)。
- **bootstrap**:M1 支持单节点 `initialize` 起集群,再 `add_learner` + `change_membership` 加节点到 3 节点;或用固定初始成员集初始化。测试用 3 节点。
- 崩溃一致性:openraft 保证的线性化 + redb 事务持久化;属性测试断言"事务提交是持久化边界"(§6)。

## 5. 控制面 gRPC 表面(M1)

对齐 §7.4,M1 子集(proto 带 `version`):

```proto
service Master {
  rpc RegisterNode(RegisterRequest) returns (RegisterResponse); // 显式注册(准入校验)
  rpc Heartbeat(stream NodeBeat) returns (stream MasterDirective); // M1: 上行 liveness+容量;下行暂空
  rpc ListNodes(Empty) returns (NodeList);        // 只读,读 Nodes(hard)+ Liveness(soft)
  rpc GetClusterState(Empty) returns (ClusterState); // 只读:leader、成员、term、节点/盘概览
}
service RaftInternal { /* append_entries / vote / install_snapshot */ }
```

- **准入校验(Q1 安全点)**:M1 用**共享集群密钥/token**(配置项 `master.cluster_secret`,注册与心跳校验)起步;不做完整 mTLS(留后续)。校验失败 → `Unauthenticated`,记 `warn`(预期拒绝不打 error)。
- 只有 leader 能处理写(RegisterNode、AllocatePartitionId);follower 收到写请求返回 leader 地址供 client 重定向(router SDK 处理)。只读查询任意节点可答(可能读到略旧软状态,可接受)。

## 6. 验证策略(M1)

- **单测**:hard-state 状态机 apply 的确定性 + 幂等(RegisterNode 重放、AllocatePartitionId 单调);redb log/snapshot 存取往返;proto 编解码。
- **属性测试(`proptest`)**:随机命令序列 → 崩溃点(截断 redb 事务)→ 重启恢复,断言 last-applied 一致、不丢已提交命令("事务提交是持久化边界")。
- **集成测试**:内存/本地起 3 节点 raft → 选出 leader(轮询带超时,**不用 `sleep`**,CLAUDE.md §6)→ 一个数据节点 `RegisterNode` → `ListNodes` 在所有节点最终可见该节点。
- **指标断言**:对 master 实际接入的那个 `Registry` 断言(不新建 Metrics 实例,CLAUDE.md §6)。
- 全程 `cargo test` 绿 + `cargo clippy` 无告警。

## 7. 配置项 / 监控项

M1 落地时同步写入 `docs/design/cluster.md` 的「配置项清单」「监控项清单」两表(CLAUDE.md §7 必须项),与 `impl Default`+`Validate` / 指标注册处逐项对齐。预期 M1 配置:`master.node_id`、`master.raft_addr`、`master.grpc_addr`、`master.data_dir`(redb 路径)、`master.initial_members`、`master.cluster_secret`、`master.heartbeat_timeout_secs` 等;预期指标:raft term/leader gauge、apply 计数、注册节点数 gauge、心跳计数等。
