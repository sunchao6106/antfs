# tasks — master-node

分期实现,每期独立验收(对齐 cluster.md §7.6)。本 change 落地 **M1**;M2–M4 列出边界,后续 change 或本 change 续期实现。

## M1 — 骨架(本期交付)

### 1. change 文档与决策归档
- [x] 1.1 `proposal.md` / `design.md` / `.openspec.yaml` 写就,`openspec validate master-node --strict` 通过。

### 2. crate 脚手架
- [x] 2.1 新建 `crates/master`(`Cargo.toml` 对齐 store/common 风格:workspace edition/license),加入根 `Cargo.toml` `members`。
- [x] 2.2 依赖:`openraft`(核对当前稳定版并 pin)、`redb`、`tonic`/`prost`/`tonic-build`、`tokio`、`common-config`、`common-metric`、`tracing`、`thiserror`、`serde`;dev-dep `proptest`。
- [x] 2.3 `proto/master.proto`(控制面 `Master` + `RaftInternal`,消息带 `version` 字段),`build.rs` 用 `tonic-build` 生成。

### 3. Raft 内核(`master-raft-core`)
- [x] 3.1 `MasterCommand`/`MasterResponse` + hard-state 数据模型(`IdAllocator`/`Nodes`/`Partitions`/`Policy`),`serde` 可序列化。
- [x] 3.2 `RaftTypeConfig`(`declare_raft_types!`,按当前版本填associated types)。
- [x] 3.3 redb 后端:log storage + state machine(applied 状态 + 快照);apply 在单个 redb 写事务提交(持久化边界)。
- [x] 3.4 tonic 传输的 `RaftNetwork`/`RaftNetworkFactory` 调对端 `RaftInternal`。
- [x] 3.5 集群 bootstrap:`initialize`(固定 3 成员集起集群,M1 已实现)。动态 `add_learner`/`change_membership` **推迟**到需要在线扩缩容时(非 M1 固定集群所需)。
- [x] 3.6 client_write 提案路径:`RegisterNode`(幂等) + `AllocatePartitionId`(单调)。

### 4. 控制面(`master-control-plane`)
- [x] 4.1 `Master` gRPC 服务实现:`RegisterNode`(准入校验:`master.cluster_secret`)、`Heartbeat(stream)`(上行 liveness+容量,下行 M1 暂空)、`ListNodes`、`GetClusterState`。
- [x] 4.2 软状态 `Liveness`/`DiskUsage`(`RwLock`,心跳写、查询读);记 `last_beat`。
- [x] 4.3 非 leader 收写请求 → 返回 leader 地址(供 client 重定向)。

### 5. 配置 / 监控 / 文档(CLAUDE.md §7 必须项)
- [x] 5.1 `MasterConfig`(`serde`+`Validate`,`impl Default`):node_id / raft_addr / grpc_addr / data_dir / initial_members / cluster_secret / heartbeat_timeout_secs。
- [x] 5.2 `MasterMetrics`(接 common/metric 的 Registry):raft term/leader gauge、apply 计数、注册节点 gauge、心跳计数。
- [x] 5.3 `docs/design/cluster.md` 补「配置项清单」「监控项清单」两表,与代码逐项对齐;翻 §7.6 M1 checkbox 为 [x]。

### 6. 测试与验收
- [x] 6.1 单测:状态机 apply 确定性 + 幂等(RegisterNode 重放 / AllocatePartitionId 单调);redb 往返;proto 编解码。
- [x] 6.2 属性测试(`proptest`):随机命令 + 崩溃点重启,断言 last-applied 一致、已提交命令不丢。
- [x] 6.3 集成测试:3 节点起集群 → 选出 leader(轮询带超时,**无 `sleep`**)→ 数据节点 `RegisterNode` → `ListNodes` 全节点最终可见。
- [x] 6.4 指标断言打在实际接入的 Registry 上。
- [x] 6.5 `cargo test` 绿 + `cargo clippy` 无告警(workspace 全量)。

## M2 — 放置(后续)
- [x] `CreatePartitions`(分配 id + 故障域 rack-aware 选盘 + 写 raft placement) + 容量感知选盘(余量 ≥ 分区上界且相近的 N 块盘)。已落地:`crates/master/src/placement.rs::select_disks` + proto `CreatePartitions`。
- [ ] 盘将满**转 RO**:`near_full_ratio` 目前只在选盘时**跳过**该盘(`placement.rs`:74),不会把已落在将满盘上的分区置 RO——这条仍未做。
- [x] `LookupPartition` + 路由索引心跳重建 + `AssignPartition`(取 RW 分区)。已落地。
- [x] Q5 自动扩容:水位监测自动建分区 + 防抖/限速/上限。已落地 `crates/master/src/autogrow.rs`。

## M3 — 调谐(后续)
- [x] 副本状态机(BOOTSTRAP→STANDBY/ACTIVE,§7.2)+ 心跳失联检测 → 产再复制任务(下发 `MasterDirective`)。已落地:`soft.rs` 软状态 + `reconcile.rs::plan_rereplication`。

## M4 — 再平衡 / EC 钩子(后续)
- [x] `DecommissionNode`。已落地(详见 `openspec/changes/master-decommission/`)。
- [ ] 容量均衡:转由 `openspec/changes/balance-and-scrub/` 承接(计划式均衡,含 planner/RPC/执行器)。
- [ ] 对接 D5 EC 转换决策 —— **EC 仍锁定**,等解锁后再做。
