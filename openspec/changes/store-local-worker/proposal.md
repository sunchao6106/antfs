## Why

S1–S3 让数据节点能服务读写/变更、接入集群、拉复制 + bootstrap。但**本地维护**还没编排:`blob` 引擎的 `flush`/`compact_if_needed`/`hard_delete` 都已实现,却没有常驻线程周期性驱动它们。缺 S4 会导致:W=2 ack 的 page-cache 写迟迟不落盘(掉电窗口大)、死数据(墓碑 + 被覆盖旧记录)不回收(盘白占)、已删对象数据体不擦除(隐私)。cluster.md §9 明确 `LocalWorker` = store 节点**常驻、本地、轻**的维护(对齐 Ambry `CompactionManager` 跑在 `StorageManager` 里),与 P1 集群级 scheduler 分工。

## What Changes

- **store 数据节点新增本地 worker**(`worker.rs`):对本节点每个本地 replica(`ReplicaManager`)周期性:
  - **flush**:周期 fsync 推进持久化边界(缩小 W=2 page-cache 掉电窗口)。
  - **compaction**:`compact_if_needed(dead_ratio)` —— 死数据占比超阈值才压缩。
  - **hard_delete**:擦除已删对象数据体(隐私)。
  - 同步引擎调用经 `spawn_blocking` offload(承 N4;专用每盘池后续)。
- **配置(周期时长全部可控,用户要求)**:`local_worker_enabled`、`flush_interval_secs`、`compaction_interval_secs`、`compaction_dead_ratio`、`hard_delete_interval_secs`。
- **监控**:`store_server_flushes_total`、`store_server_compactions_total`、`store_server_hard_deleted_total`。

## Capabilities

### New Capabilities
- `store-local-worker`:数据节点常驻本地维护(周期 flush / 单店 compaction / hard_delete),周期时长可配置。

### Modified Capabilities
<!-- 复用 blob 引擎既有 flush/compact_if_needed/hard_delete(不改其语义);store 节点加编排 + 配置/监控。 -->

## Impact

- **crate**:`store`(新增 `worker.rs`;`replica.rs` 加 `handles()`;`config.rs`/`metrics.rs` 扩展;`server.rs` 起 worker)。
- **持久性(§3.1)**:worker 只做**引擎已有的安全维护**——flush 是持久化边界(只增强持久性);compaction/hard_delete 走引擎既有的崩溃安全实现(tmp→fsync→rename + COMPACT_COMMIT 标记)。不改写路径语义。

## Non-goals

- 不做每盘专用线程池 / IO 调度(N4,后续);MVP 用共享 `spawn_blocking`。
- 不做集群级再复制/再平衡/EC 调度(那是 P1 scheduler,非本地 worker);**EC 仍锁定**。
