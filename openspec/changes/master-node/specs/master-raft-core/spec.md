# master-raft-core

master 的 Raft 内核:自包含集群大脑的一致性地基(D1)。

## ADDED Requirements

### Requirement: 内建 Raft 管理集群元数据

master SHALL 用内建 Raft(`openraft`)在多节点间就**集群元数据**达成线性一致,不依赖任何外部协调器(ZooKeeper/etcd/MySQL),也不介入数据副本一致性。

#### Scenario: 多节点选主

- **WHEN** 起 3 个 master 节点并以固定初始成员集初始化集群
- **THEN** 集群 SHALL 选出唯一 leader,其余为 follower,且所有节点对当前 term/leader 达成一致

#### Scenario: leader 故障后重新选主

- **WHEN** 当前 leader 进程退出
- **THEN** 剩余多数派 SHALL 在选举超时内选出新 leader,已提交的集群元数据 SHALL 不丢失

#### Scenario: Raft 只用于集群元数据

- **WHEN** 数据副本发生写入
- **THEN** 该写入 SHALL NOT 经过 master 的 Raft(数据一致性由异步 quorum + 拉复制负责,D4)

### Requirement: Raft hard state 边界最小化

进入 Raft 状态机的持久状态 SHALL 仅限 `IdAllocator`、`Nodes`、`Partitions`、`Policy`(Q2);liveness、磁盘余量、实际副本态 SHALL 为心跳重建的内存软状态,不进 Raft。

#### Scenario: 软状态不进 Raft

- **WHEN** 检查 Raft 状态机内容
- **THEN** 它 SHALL 只含 id 分配器 / 节点身份 / 分区放置 / 策略,不含 liveness 或磁盘实时余量

### Requirement: Raft 持久化后端为 redb

Raft 的 log 与状态机快照 SHALL 持久化在 `redb`(纯 Rust 嵌入式 KV,Q3),每次状态机 apply SHALL 在单个 redb 写事务中提交,该事务提交即持久化边界。

#### Scenario: 崩溃后从 redb 恢复

- **WHEN** master 节点在 apply 一批命令后崩溃并重启
- **THEN** 它 SHALL 从 redb 恢复到最后一次成功提交事务对应的 applied 状态,已提交命令不丢、不重复应用

### Requirement: 单调 partition id 分配

master SHALL 通过 Raft 提案分配全局单调递增的 `partition_id`,保证跨节点、跨重启不重不漏。

#### Scenario: 并发分配单调唯一

- **WHEN** 多个分配请求经由 Raft 线性化处理
- **THEN** 每个请求 SHALL 得到唯一且严格递增的 id,任何两个请求不得得到相同 id

### Requirement: 控制面与 Raft 内部传输分离且版本化

master↔master 的 Raft RPC SHALL 走独立的 `RaftInternal` gRPC 服务;所有 gRPC 消息 SHALL 带 `version` 字段以保前后兼容(D2)。SHALL NOT 使用自研二进制 TCP 协议。

#### Scenario: proto 带版本字段

- **WHEN** 检查任一控制面或 RaftInternal 消息定义
- **THEN** 它 SHALL 含 `version` 字段,且新增字段以可跳过/有默认的方式演进
