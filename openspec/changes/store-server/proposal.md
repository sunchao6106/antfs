## Why

master(M1–M4)已能选主、放置、路由、调谐,但**数据面还不存在**:没有进程真正承载 `store` 引擎、对外收发 blob 字节。`store server`(数据节点,cluster.md §9)是 store 引擎的**多副本编排 + 网络外壳**——挂 N 块盘、每盘一堆 `BlobStore`(每个 = 一个 partition-replica)、对外走 HTTP 数据面。

本 change 落地 **S1(数据节点骨架 + 数据面)**:crate 脚手架、DiskManager(`statfs` 探真实容量,Q1)、ReplicaManager(按 partition 管 `BlobStore`)、DataHttpServer(HTTP `PUT`/`GET` 流式,D2)。**接 master(心跳/注册/ReplicaLifecycle)、拉复制、本地 worker、EC 均不在 S1**,分期实现。

## What Changes

- 新 crate `crates/store-server`(数据节点进程)。
- **DiskManager**:声明目录 + 运行时 `statfs` 探真实容量/余量(取 SeaweedFS 式,不写死;Q1)。
- **ReplicaManager**:`map<PartitionId, BlobStore>`;`ensure_replica(pid)` 在某盘目录下 `BlobStore::create_with/open_with`(复用 store 的 DI 构造,容量 = `partition_capacity_bytes`)。
- **DataHttpServer**(`axum`/`hyper`):`PUT/GET /partitions/{pid}/blobs/{key}`;blob 走 `Bytes` 流式不整缓(D2/§5.3);store 是**同步阻塞**引擎 → 经 `spawn_blocking` offload,不卡 tokio reactor(衔接 N4)。
- **管理入口(S1 用)**:`POST /admin/partitions/{pid}` 建一个 replica(S2 起改由 master 心跳指令驱动)。
- `StoreServerConfig` + `StoreServerMetrics`。

## Capabilities

### New Capabilities

- `store-server`: 数据节点的 blob 数据面与本地副本管理——磁盘容量探测、partition→`BlobStore` 管理、HTTP 流式 put/get、同步引擎的异步 offload。

### Modified Capabilities

<!-- 复用 store 引擎(BlobStore),不改 store;master 侧不改。 -->

## Impact

- **新 crate**:`crates/store-server`,加入 workspace `members`。
- **依赖**:`axum`/`hyper`/`tokio`(数据面)、`bytes`、`libc`(statfs)、`store`/`config`/`metric`/`tracing`/`thiserror`/`serde`;dev-dep `reqwest`/`tempfile`。
- **复用**:`store::BlobStore::{create_with,open_with,put,get,...}`(不改 store)。
- **保留契约**:blob 不整缓、流式(§5.3);同步引擎经线程池 offload(不阻塞 reactor);容量真实探测(Q1)。

## Non-goals

- **EC 暂不解锁**(D5):S1 不含任何纠删码 / 封存段转 EC 逻辑(依用户指示推迟)。
- 不接 master:心跳 / 注册 / ReplicaLifecycle(消费 transitions)= **S2**。
- 不做拉复制 / anti-entropy / bootstrap = **S3**;不做本地 worker(flush/compaction 编排)= **S4**。
- 不做变更类 HTTP 端点(S1 只 put/get;变更类后续)。

> **后续变更(2026-09-23,`simplify-delete-model`)**:上述 S4 与变更类端点后来都落地了;其中「隐私擦除」「复活」「改 TTL」又随"对象不可变 + 删除不可逆"**被移除**(数据面只剩 `DELETE`,TTL 改为写入时经 `PUT ?expiration_ms=` 给定)。
