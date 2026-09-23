# tasks — store-server-replication (S3a)

> **后续变更(2026-09-23,`simplify-delete-model`)**:下文的生命周期版本 / 操作时间判定与"复活对象保留"测试已随"对象不可变 + 删除不可逆"作废;`apply_replicated` 现按**墓碑恒胜**合并。
> 本清单保留为实现当时的记录。

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate store-server-replication --strict` 通过。

## 2. store 引擎:忠实 apply
- [x] 2.1 `blob.rs`:`ReplicatedRecord` + `ApplyOutcome` 公有类型;`lib.rs` 导出。
- [x] 2.2 `BlobStore::apply_replicated`(墓碑恒胜判定;inline seg_bytes/valid_stats;`is_available` 门控 + `track`;只增不删幂等)。
- [x] 2.3 单测(§3.3 持久性):缺失存活复制、重复 apply Skipped、墓碑传播、Unavailable 门控。

## 3. store-server 数据面端点
- [x] 3.1 `server.rs`:`GET /partitions/{pid}/replica-metadata?token&max_bytes` → JSON FindInfo(token 用 base64/整数编码;404 无 replica)。
- [x] 3.2 blob 读加 `?replication=1`(include_deleted/expired,供 apply)。

## 4. store-server 复制器
- [x] 4.1 `replication.rs`:`PeerSource` trait(metadata / fetch_data)+ `replicate_from_peer` 收敛循环 + per-peer token。
- [x] 4.2 HTTP adapter(`reqwest`)实现 `PeerSource`(拉 metadata JSON + 拉 blob `?replication=1`)。
- [x] 4.3 单测:进程内 `PeerSource`(包裹另一 `BlobStore`)驱动收敛——PUT 传播、幂等、bootstrap 全量追平、delete 传播。

## 5. 配置 / 监控 / 装配
- [x] 5.1 `config.rs`:`replication_enabled`(默认 false)/`replication_interval_secs`/`replication_max_batch_bytes` + `Validate`。
- [x] 5.2 `metrics.rs`:`store_server_replication_rounds_total` / `_applied_total` / `_bytes_total` / `_errors_total`。
- [x] 5.3 `serve()`:若 `replication_enabled` 起后台复制器;peer 地址经 master `LookupPartition` 发现(排除自身、按 `up` 过滤)。

## 6. 测试
- [x] 6.1 集成测试:两个 store-server 真 HTTP 端口——PUT 到 A,B 对 A 跑复制 → B 可读;delete 传播。确定性等待(轮询 metadata 追平,不 sleep)。
- [x] 6.2 `cargo test` 绿 + `cargo clippy` 无告警 + 全 workspace build。

## 7. 文档
- [x] 7.1 `docs/design/cluster.md` §9.1 补 S3a 状态 + §10 复制传输标注(HTTP 决策,S3a/S3b 分期);配置项 / 监控项清单补新增项。

## 后续(S3b,不在本 change)
- [x] 封存段物理拷贝(整段 + 段头)+ 接收侧装载重建:`bootstrap_from_peer`(`replication.rs`:263)拉整段 → `install_bootstrap` 重建。注:整段在内存(非流式)。
- [ ] disk-aware 每盘并发限流(N4):复制/bootstrap/worker 共用同一个阻塞池,无 per-disk 上限。
- [ ] FindToken 的 sessionId / resetKey:token 只有 `(segment, offset)`,无会话语义。
- [ ] router SDK(暂不做)。
- [ ] **EC 仍锁定**。
