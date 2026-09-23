# store-server 设计(S1:数据节点骨架 + 数据面)

承 [`docs/design/cluster.md` §9](../../../docs/design/cluster.md)。S1 只做**本地数据面**,不接 master、不复制、**不碰 EC**。

## 1. 层次(对齐 §9)

```
StoreServer(一个进程 / 一台数据节点)
  └─ DiskManager ×每盘(持有盘目录 + statfs 容量/余量)
       └─ ReplicaManager: map<PartitionId, ReplicaHandle{ disk_id, BlobStore }>
            └─ BlobStore(store crate,一个 partition-replica)
  └─ DataHttpServer(axum):PUT/GET 路由到对应 BlobStore
```

## 2. DiskManager + statfs(Q1)

- 配置给盘目录列表 `data_dirs`;每目录一个 `Disk { id, dir, capacity_bytes, free_bytes }`。
- `id` = 目录 basename(或 `disk{i}`);容量/余量由 `statfs`(libc `statvfs`,macOS/Linux 通用)运行时探测,**不写死**(取 SeaweedFS 式,反 Ambry 写死配置)。
- `statfs`:`total = f_blocks * f_frsize`,`free = f_bavail * f_frsize`。

## 3. ReplicaManager

- `replicas: RwLock<HashMap<PartitionId, ReplicaHandle>>`。
- `ensure_replica(pid)`:已存在 → 幂等返回;否则选一块有余量的盘,在 `<disk_dir>/partition_<pid>` 下 `BlobStore::create_with(dir, partition_capacity_bytes, &StoreConfig, &StoreMetrics, "p<pid>")`(已存在目录则 `open_with`),插入 map。
  - S1 选盘:第一块 `free_bytes >= partition_capacity_bytes` 的盘(简单;S2 起用 master 指令里的 disk_id)。
- `get(pid) -> Option<ReplicaHandle>`:数据面路由用。

## 4. DataHttpServer(D2 流式)

`axum` 路由:
- `PUT /partitions/{pid}/blobs/{key}`:`key` = 64 hex(32B blobId,§8);body = blob 字节。→ 查 `ReplicaManager.get(pid)`(无 → 404)→ `spawn_blocking(move || store.put(key, &data))`。
  - `Ok` → 201;`Err(AlreadyExists)` → 409;其它 → 500。
- `GET /partitions/{pid}/blobs/{key}`:→ `spawn_blocking(store.get(&key))` → 200 + body;`NotFound` → 404;其它 → 500。

**同步引擎 offload**:store 的 put/get 是同步阻塞(路线乙),在 async handler 里必须 `tokio::task::spawn_blocking`,避免堵死 reactor(衔接 N4「每盘小线程池」;S1 先用 tokio blocking 池,专用池留 S4/性能期)。

**流式/不整缓**:S1 body 先用 `Bytes`(axum 默认聚合到内存)——**注意**:这与"不整缓"有张力。S1 为骨架简单先聚合;真正的边收边写流式(`put` 接 `Read`/分块)是 store 引擎 API 的后续增强,记入 §6 待办,不在 S1 破坏 store 接口。

## 5. 配置 / 监控(CLAUDE.md §7)

- `StoreServerConfig`:`node_id`(u64)、`data_dirs`(Vec<String>,非空)、`http_addr`(String)、`partition_capacity_bytes`(u64,≥1,默认 100 GiB,对齐 master `partition_capacity_bytes`)、复用 `store` 的 `[store]` 段(段大小等)。
- `StoreServerMetrics`:`store_server_put_total` / `store_server_get_total` / `store_server_put_bytes_total` / `store_server_get_bytes_total` / `store_server_replicas`(gauge)/ `store_server_errors_total{op}`。

## 6. 验证(S1)

- **集成测试(仅数据节点,无 master)**:起 StoreServer(临时目录当盘)→ `POST /admin/partitions/0` 建 replica → `PUT /partitions/0/blobs/<64hex>` 写入 → `GET` 取回**字节一致** → 重复 `PUT` 同 key → 409 → `GET` 不存在 key → 404 → DiskManager 报告 `capacity_bytes > 0`。
- 单测:hex↔StoreKey 解析、DiskManager statfs 非零、ReplicaManager `ensure_replica` 幂等。
- `cargo test` 绿 + `cargo clippy` 无告警。

## 7. 后续(不在 S1)

- **S2**:HeartbeatClient(报 node+disk statfs+replica current_state)+ RegisterNode + ReplicaLifecycle(消费 master transitions:target→BlobStore create/open/close)。
- **S3**:ReplicationPuller(拉式 anti-entropy,`find_entries_since` + HTTP 拉 blob)+ bootstrap(混合,N3)。
- **S4**:LocalWorker(周期 flush / 单店 compaction 编排)。原「隐私擦除」loop 已于 2026-09-23 随"对象不可变 + 删除不可逆"移除(见 `openspec/changes/simplify-delete-model/`)。
- **变更类端点**:delete HTTP。原「复活」「改 TTL」两条已于 2026-09-23 移除;TTL 改为写入时经 `PUT ?expiration_ms=` 给定。
- 真流式 put(边收边写,消除内存聚合)。
- **EC 仍锁定**(用户指示暂不解锁)。
