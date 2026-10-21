## Why

S1 让数据节点能本地存取 blob,但**与 master 脱节**:master 不知道它存在、不能把 placement 落到它、盘坏了也无人再复制。S2 把 store server 接进集群控制面(cluster.md §9 + §7):注册 + 心跳 + 消费 master 指令 + 磁盘健康监控。

磁盘健康**参考 Ambry 两级**(见调研):**被动**——Phase 1 已给 `store` 引擎补的连续 IO-error 自关 + 回调,store server 把回调翻译成"该 replica `current_state=ERROR`"上报 master,直接喂**已实现的 M3 失联/异常再复制**;**主动**——周期写读校删探测盘(带超时抓 hang),健康随心跳上报。

## What Changes

- store-server 依赖 `master` crate 复用其 gRPC pb(控制面契约)。
- **RegisterNode**:启动时向 master 注册(node_id + 各盘 `statfs` 容量)。
- **HeartbeatClient**:周期双向流心跳——上报 node + 各盘余量 + **各 replica `current_state`**;接收 `MasterDirective.transitions` 交 ReplicaLifecycle。
- **ReplicaLifecycle**:消费 transitions——target `ACTIVE`/`BOOTSTRAP` → `ensure_replica`(建/开 `BlobStore`),target `OFFLINE`/`DROPPED` → 关闭 replica;维护每 replica 的 `current_state`。
- **被动升级**:给每个 `BlobStore` 设 `io_error_hook`(Phase 1)——触发 → 该 replica 置 `ERROR` → 心跳上报 → **master 再复制**。
- **主动探测(DiskHealthCheck)**:周期 write/read/verify/delete 探测(带超时),`DiskHealth` 随心跳 `DiskBeat` 上报(观测 + 后续放置避让)。
- **master 侧小增强**:reconcile 把"节点存活但 replica 上报 `ERROR`"视为不健康副本 → 触发再复制(接住被动升级)。

## Capabilities

### New Capabilities
- `store-server-heartbeat`: 数据节点接入集群——注册、心跳(容量 + 副本态 + 盘健康)、ReplicaLifecycle 消费指令、主动/被动磁盘监控与到 master 再复制的闭环。

### Modified Capabilities
- `master-reconcile`: 再复制判定纳入 replica `current_state=ERROR`(节点存活但副本坏也补副本)。

## Impact

- **crate**:`store-server` 新增 `heartbeat.rs`(client + lifecycle)、`health.rs`(主动探测);依赖 `master`(pb)+ `tonic`/`tokio-stream`/`prost`。`replica.rs` 记 `current_state` + 挂 `io_error_hook`。
- **master**:`reconcile.rs` `plan_rereplication` 纳入 errored 副本;`server.rs` reconciler 传入 ERROR 集合(来自已有软状态 `ReplicaCurrent`)。proto `DiskBeat` 加 `health` 字段(§3.4 版本化,复用之前 `reserved 4` 之外的新号)。
- **保留契约**:master 只决策不搬数据(D1);先补后删(M3/M4 已守);EC 仍锁定(暂不解锁)。

## Non-goals

- **EC 暂不解锁**。
- 不做拉复制 / bootstrap 实拷贝(S3);不做本地 worker(S4);不做变更类 HTTP 端点(Phase 3)。
- ReplicaLifecycle 的 `DROPPED` 只关闭不删目录(删盘回收 = store `drop`,后续)。
