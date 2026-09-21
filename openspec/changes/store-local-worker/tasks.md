# tasks — store-local-worker (S4)

## 1. 变更文档
- [x] 1.1 proposal / spec delta,`openspec validate store-local-worker --strict` 通过。

## 2. 配置 / 监控
- [x] 2.1 `config.rs`:`local_worker_enabled`(默认 true)/`flush_interval_secs`/`compaction_interval_secs`/`compaction_dead_ratio`/`hard_delete_interval_secs` + `Default` + `Validate`(周期 ≥1,ratio ∈ (0,1])。
- [x] 2.2 `metrics.rs`:`store_server_flushes_total`/`store_server_compactions_total`/`store_server_hard_deleted_total`。

## 3. worker 实现
- [x] 3.1 `replica.rs`:`ReplicaManager::handles()`(快照全部 `(pid, ReplicaHandle)`)。
- [x] 3.2 `worker.rs`:`flush_all`/`compact_all(threshold)`/`hard_delete_all` 纯函数(返回计数,可单测)+ `spawn_local_worker`(三个独立周期 loop,经 `spawn_blocking`)。
- [x] 3.3 `server.rs`:`serve()` 里若 `local_worker_enabled` 起 worker。

## 4. 测试
- [x] 4.1 单测:put→flush_all→重开可读;put+delete→compact_all 回收;delete→hard_delete_all 擦零。
- [x] 4.2 `cargo test` 绿 + `cargo clippy` 无告警 + workspace build。

## 5. 文档
- [x] 5.1 `docs/design/cluster.md` §9.1:S4 本地 worker 状态 + 配置项 / 监控项清单补项。

## 后续(不在本 change)
- [ ] 每盘专用线程池 / IO 调度(N4);P1 集群级 scheduler(再复制/再平衡/EC);**EC 仍锁定**。
