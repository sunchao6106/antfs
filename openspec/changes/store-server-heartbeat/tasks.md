# tasks — store-server-heartbeat (S2)

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate store-server-heartbeat --strict` 通过。

## 2. master 侧增强
- [x] 2.1 proto `DiskBeat` 加 `health` 字段(string);`master` 重新 codegen。
- [x] 2.2 `reconcile.rs` `plan_rereplication` 增 `errored: &HashSet<(PartitionId,NodeId)>` 参数(healthy = up && !errored);更新单测。
- [x] 2.3 `server.rs` reconciler 从软状态 `ReplicaCurrent`(state==ERROR)构建 errored 传入;evacuation 健康计数同样排除 errored。

## 3. store-server 接入
- [x] 3.1 依赖 `master`(pb)+ `tonic`/`tokio-stream`/`prost`;`StoreServerConfig` 增 master_addr/cluster_secret/heartbeat_interval_secs/disk_health_*。
- [x] 3.2 `replica.rs`:`ReplicaHandle` 加 `current_state`(RwLock<String>);`ensure_replica` 建好后 `set_io_error_hook`(触发 → current_state=ERROR);`get_replica_states()`。
- [x] 3.3 `heartbeat.rs`:`register`(启动)+ 心跳双向流(上报 disks/replicas,接收并应用 transitions via lifecycle)。
- [x] 3.4 `health.rs`:主动 `DiskHealthCheck`(write/read/verify/delete + timeout)→ `DiskHealth`;周期任务(默认关)。
- [x] 3.5 `StoreServer::serve` 起 register + heartbeat + (可选)disk-health 后台任务;指标接入。

## 4. 测试
- [x] 4.1 master 单测:`plan_rereplication` 纳入 errored。
- [x] 4.2 store-server 单测:lifecycle target→动作、DiskHealthCheck 健康盘→HEALTHY。
- [x] 4.3 集成测试:1 master + 1 store-server —— 注册可见 → CreatePartitions → 心跳建 replica → PUT/GET 可用 → 触发 hook → replica ERROR 上报 → master 再复制补副本。
- [x] 4.4 `cargo test` 绿 + `cargo clippy` 无告警。

## 5. 文档
- [x] 5.1 `docs/design/cluster.md` §9.1 更新 S2 状态 + 配置/监控清单。

## 后续(不在 S2)
- [ ] S3 拉复制 + bootstrap;S4 本地 worker;放置据 DiskHealth 避让;`DROPPED` 删目录;**EC 仍锁定**。
