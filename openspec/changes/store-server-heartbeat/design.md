# store-server-heartbeat 设计(S2:接入集群 + 磁盘监控)

承 cluster.md §9/§7,参考 Ambry 两级磁盘监控(调研见会话记录)。

## 1. 依赖与契约

store-server 依赖 `master` crate,复用其 `pb`(`master_client::MasterClient`、`RegisterRequest`、`NodeBeat`/`ReplicaBeat`/`DiskBeat`、`MasterDirective`/`Transition`)。控制面契约单一来源,不复制 proto。

## 2. 注册 + 心跳(HeartbeatClient)

- **启动**:`MasterClient::connect(master_addr)` → `register_node(RegisterRequest{node_id, disks[statfs cap], secret})`。
- **心跳循环**:每 `heartbeat_interval_secs` 发一条 `NodeBeat`:
  - `disks`:各盘 `statfs` 实时余量 + `health`(主动探测结果,§4)。
  - `replicas`:本地每 replica 的 `ReplicaBeat{partition_id, disk_id, current_state, lag=0(S2 占位)}`。
- **接收**:同一双向流的 `MasterDirective.transitions` → 交 ReplicaLifecycle。

## 3. ReplicaLifecycle(消费 transitions)

`ReplicaManager` 扩每 replica 的 `current_state: RwLock<String>`(默认建好即 `ACTIVE`,Q4 折叠)。lifecycle 处理 transition:

| target_state | 动作 | current 上报 |
|---|---|---|
| `ACTIVE` / `BOOTSTRAP` | `ensure_replica`(幂等建/开 BlobStore) | `ACTIVE`(S2 无真实 bootstrap,建好即 ACTIVE;真实追平留 S3) |
| `OFFLINE` / `DROPPED` | 关闭 replica(从 map 移除;删目录留后续 `drop`) | 不再上报 |

## 4. 磁盘监控(参考 Ambry 两级)

### 4.1 被动(driven by IO errors,真正触发再复制)
- Phase 1 已给 `BlobStore` 补 `io_error_count`/阈值自关/`set_io_error_hook`。
- `ensure_replica` 建好 BlobStore 后,store-server `set_io_error_hook(move || replica.current_state = ERROR)`。
- 触发链:盘坏 → 连续 IO 错误达阈值 → BlobStore 自关 + hook → replica `current_state=ERROR` → 心跳上报 → **master reconcile 再复制**(§5)。
- 对齐 Ambry `BlobStore.onError` → `diskManager.onBlobStoreIOError`。

### 4.2 主动(DiskHealthCheck,observability + 放置避让)
- 每 `disk_health_interval_secs`(默认关,对齐 Ambry `enabled=false`)对每盘做 5 步探测:查空间/可达 → 建文件 → 写(带 `spawn_blocking` + `tokio::timeout` 抓 hang)→ 读校验 → 删。
- 结果 `DiskHealth`(HEALTHY / MOUNT_NOT_ACCESSIBLE / WRITE_TIMEOUT / WRITE_ERROR / READ_TIMEOUT / READ_ERROR / READ_MISMATCH / DELETE_ERROR),随心跳 `DiskBeat.health` 上报。S2 只上报(不据此自动下线;放置避让留后续)。

## 5. master 侧再复制纳入 ERROR(小增强)

- master 软状态 `ReplicaCurrent`(M3 已建)已含各 replica `current_state`。
- `plan_rereplication` 增参 `errored: &HashSet<(PartitionId,NodeId)>`:副本"健康"= 节点存活 **且** 未 ERROR。`healthy < rf 且 存在(down 或 errored)副本` → 再复制。
- reconciler loop 从 `ReplicaCurrent` 构建 `errored` 集合传入;evacuation/选盘同样排除 errored 节点副本作为健康计数。
- 效果:节点还活着但某盘/replica 坏(ERROR)也能触发补副本,闭合被动检测。

## 6. 配置 / 监控

- store-server 增:`master_addr`、`cluster_secret`、`heartbeat_interval_secs`(默认 3)、`disk_health_enabled`(默认 false)、`disk_health_interval_secs`(默认 60)、`disk_health_timeout_secs`(默认 10)。
- 指标增:`store_server_heartbeats_total`、`store_server_replica_errors_total`、`store_server_disk_health{status}`(或 gauge)。

## 7. 验证(S2)

- **单测**:master `plan_rereplication` 纳入 errored(errored 副本使 healthy<rf → 补;无 errored 全活 → 不补)。lifecycle target→动作映射。DiskHealthCheck 正常→HEALTHY。
- **集成测试(1 master 单节点 raft + 1 store-server)**:
  1. store-server 注册 → master `ListNodes` 见该节点。
  2. master `CreatePartitions` 放到该节点 → 心跳带 directive → store-server `ensure_replica` 建库 → HTTP `PUT`/`GET` 可用。
  3. 触发该 replica 的 `io_error_hook`(测试直接调 `BlobStore` 私有 hook 或注入 IO 错误)→ replica 上报 `ERROR` → master 再复制(需 ≥1 空闲健康节点/盘)→ placement 增补副本。
- `cargo test` 绿 + `cargo clippy` 无告警。

## 8. 后续(不在 S2)
- S3 拉复制 + bootstrap(N3);S4 本地 worker;放置据 DiskHealth 避让;`DROPPED` 删目录回收;**EC 仍锁定**。
