# tasks — store-server (S1:数据节点骨架 + 数据面)

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate store-server --strict` 通过。

## 2. 脚手架
- [x] 2.1 新建 `crates/store-server`(Cargo.toml 对齐现有 crate 风格),加入根 `members`。
- [x] 2.2 依赖:`axum`/`tokio`/`bytes`/`libc`/`hex`、`store`/`config`/`metric`/`tracing`/`thiserror`/`serde`;dev-dep `reqwest`/`tempfile`。

## 3. 配置 / 磁盘
- [x] 3.1 `config.rs`:`StoreServerConfig`(node_id / data_dirs / http_addr / partition_capacity_bytes)+ `Validate`。
- [x] 3.2 `disk.rs`:`DiskManager` + `statfs`(libc `statvfs`,macOS/Linux)探 capacity/free。

## 4. 副本管理
- [x] 4.1 `replica.rs`:`ReplicaManager`(`map<PartitionId, ReplicaHandle>`)+ `ensure_replica`(幂等,选盘 create_with/open_with)+ `get`。

## 5. 数据面 + 指标
- [x] 5.1 `server.rs`:axum `PUT/GET /partitions/{pid}/blobs/{key}` + `POST /admin/partitions/{pid}`;同步 store 调用经 `spawn_blocking`;错误映射(400/404/409/500)。
- [x] 5.2 `metrics.rs`:`StoreServerMetrics`(put/get 计数、字节、replica gauge、errors)。
- [x] 5.3 `StoreServer` 装配 + `serve()`(绑定 http_addr)。

## 6. 测试
- [x] 6.1 集成测试:建 replica → PUT→GET 字节一致 → 重复 PUT 409 → 未知 key GET 404 → 非法 key 400 → statfs capacity>0。
- [x] 6.2 单测:hex↔StoreKey、ensure_replica 幂等。
- [x] 6.3 `cargo test` 绿 + `cargo clippy` 无告警。

## 7. 文档
- [x] 7.1 `docs/design/cluster.md` §9 标注 S1 已实现 + 配置/监控清单(新 `docs/design/store-server.md` 或并入)。

## 后续(不在 S1)
- [x] S2 HeartbeatClient + RegisterNode + ReplicaLifecycle(接 master transitions)。已落地 `crates/store/src/heartbeat.rs`。
- [x] S3 ReplicationPuller + bootstrap(N3)。已落地 `crates/store/src/replication.rs`。
- [x] S4 LocalWorker(flush/compaction/hard_delete)。已落地 `crates/store/src/worker.rs`。
- [x] 变更类端点(delete/undelete/ttl)。已落地(详见 `openspec/changes/store-server-mutations/`)。
- [x] 真流式 put(消除内存聚合)。已落地:`BlobStore::put_from_reader`(`crates/blob/src/blob.rs`:605)+ store 侧 `INLINE_BODY_MAX` 以内内联、超过走流式(`crates/store/src/server.rs` put handler)。
- [ ] **EC 仍锁定**(暂不解锁)。
