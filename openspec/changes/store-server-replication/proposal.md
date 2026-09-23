## Why

S1/S2/mutations 让数据节点能 `PUT`/`GET`/变更 blob 并接入集群,但**副本间还不会同步**:router SDK 的 W=2 异步 quorum 写(§10)靠拉复制把第 3 副本补齐;失联/ERROR 后 master 补的新副本(M3 再复制,target=BOOTSTRAP)也要靠拉复制追平。没有 S3,多副本只是"各写各的",冗余不收敛,§3.1 持久性红线落不了地。

`store` 引擎已具备拉复制所需的全部读侧原语:`find_entries_since`(Ambry `findEntriesSince`)、`find_missing_keys`、`get_with_options`、`FindToken`/`MessageInfo`。**唯一缺口是写侧**:没有"把 peer 的一条记录按其原样(flags/lifeVersion/expiration/op_time)写进本地"的忠实 apply——现有 `put/delete/undelete/update_ttl` 都是**发起者语义**(`op_time=now`、lifeVersion 本地推导),不能用来复制(否则 bootstrap 一个已 undelete/高 lifeVersion 的 blob,本地会退回 lifeVersion 0,与 peer 永久发散)。

## What Changes

- **store 引擎**新增忠实复制-apply 原语(持久化写路径,守 §3.3):
  - `ReplicatedRecord`(key + flags + expiration + op_time + life_version + 可选 data)与 `ApplyOutcome{Applied,Skipped}`。
  - `BlobStore::apply_replicated(rec) -> Result<ApplyOutcome>`:按 **newest-wins + lifeVersion** 与本地赢家比较;需应用时把 peer 记录**逐字节原样**追加(保留其 flags/lifeVersion/expiration/op_time),更新索引/`seg_bytes`/`valid_stats`;已 ≥ peer 状态则 `Skipped`(幂等)。经 `is_available` 门控 + IO-error `track`(承 Phase 1)。
- **store-server 数据面(HTTP,承本 change Q2=HTTP)**:
  - `GET /partitions/{pid}/replica-metadata?token=<b64>&max_bytes=<u64>` → JSON `{entries:[MessageInfo],token,bytes_read}`(Ambry `ReplicaMetadataRequest`,step-1)。
  - blob 读加 `?replication=1`,对已删除/过期记录也返回数据体(供忠实 apply,step-2)。
- **store-server 复制器**(`replication.rs`):`PeerSource` trait 抽象 peer(生产=HTTP adapter,测试=进程内 `BlobStore`);`replicate_from_peer` 收敛循环:拉 metadata → 逐条判定需否 apply → 仅"本地缺失且 peer 存活"才拉数据体 → `apply_replicated` → 推进 per-peer token。后台任务对每个本地 replica 的每个 peer(经 master `LookupPartition` 发现地址)周期跑;BOOTSTRAP 副本从 `token=Uninitialized` 全量逻辑回放追平(N3 的逻辑路径)。
- **配置**:`replication_enabled`(默认 false)、`replication_interval_secs`、`replication_max_batch_bytes`。
- **监控**:`store_server_replication_rounds_total`、`store_server_replication_applied_total`、`store_server_replication_bytes_total`、`store_server_replication_errors_total`。

## Capabilities

### New Capabilities
- `store-server-replication`:副本间拉式 anti-entropy(metadata 交换 + 缺失数据拉取 + 忠实 apply)与 BOOTSTRAP 全量逻辑追平。

### Modified Capabilities
<!-- 复用 store 读侧原语;store 引擎新增写侧 apply_replicated(不改既有 op 语义)。 -->

## Impact

- **crate**:`store`(`blob.rs` 新增 apply_replicated + 类型;`lib.rs` 导出);`store-server`(`replication.rs` 新增;`server.rs` 加 2 端点;`config.rs`/`metrics.rs` 扩展;`serve()` 起复制器;`Cargo.toml` 加 `reqwest`)。
- **保留契约**:blob 不整缓(数据体流式/按需拉);同步引擎经 `spawn_blocking`;blob id 永久占用;newest-wins + lifeVersion(store 已实现)。守 §3.1 先补冗余(拉复制**只增不删**本地数据,幂等可重试)。

## Non-goals

- **N3 物理段拷贝(sealed segment stream-copy)推迟到 S3b**(文档化的分期,非代码就地另选;见 design.md 决策 D-A)。本 change 只落**逻辑拉复制**(含 bootstrap 全量逻辑回放),已能正确收敛冗余;物理拷贝是 MTTR 优化,后续叠加。
- 不改副本状态机转移的 master 侧决策(M3 已有);BOOTSTRAP→ACTIVE 的自动上报追平作为本 change 的 current_state 更新。
- 不做 router SDK(用户指示暂不做);**EC 仍锁定(暂不解锁)**。
- FindToken 的 sessionId/resetKey(compaction 失效重定位)仍按 store 现状推迟(见 store `find_entries_since` 注释)。
