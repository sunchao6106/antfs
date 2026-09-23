## Why

数据面(store 的 PUT/GET/变更端点)与 master 控制面都在了,但**写读闭不了环**:没有「谁给 blobId、写哪几个副本、怎么算成功」这一层。
`crates/master/src/types.rs` 里的 `AllocateObjectId`/`ObjectIdRange` 状态机代码早就落好了(`blobid-master-assign` 变更),
但**没有 proto、没有 handler、没有客户端**;`cluster.md §10` 定义的 W=2/R=1 语义也只停在文档上。

同时有三处现状挡着"整体连调压测":

1. **axum 的 2 MiB body 上限**(`axum-core` 的 `DEFAULT_LIMIT`)让 >2 MiB 的上传直接 413;
2. **整个 PUT 路径把 body 全量缓进内存**(`body: Bytes` + 引擎 `put(key, &[u8])`),大文件上传的内存随文件大小线性涨,
   与 CLAUDE.md §5.3「不把整个 blob 缓进内存」相悖(该偏离在 `cluster.md §9.1` 已登记为"真流式 put 待续");
3. **没有 HTTP 入口**:外部/非 Rust 客户端、`curl`、通用压测工具都进不来。

而且 `cluster.md` 现在的架构决策是「**router SDK 取代 frontend**(无独立 frontend 集群)」—— 本 change 依用户指示
**修正该决策**:frontend 保留并落地(`frontend` 角色不是被否决,而是排期),与 router SDK 并存。

## What Changes

- **master 新增 `GetBlobIds` RPC**(写路径入口):leader-only + 准入密钥,返回
  `(可写 partitionId, objectId 号段 [start,start+count), 副本位置)`;水位经 Raft 抬升(每次预留
  `max(请求数,N) × raft_batch_multiple`),**只发已提交水位以内的号**,切主/重启从水位续发、**永不复用**,
  未发完的号成空洞(计数器可观测)。配置:`master.id_alloc_segment_size`(1024)、`master.id_alloc_raft_batch_multiple`(2)。
  新指标:`master_blob_ids_{requests,allocated,failures}_total`、`master_id_alloc_{raft_commits,holes}_total`。
- **选 partition 落地 D10**:未指定 partition 时按**剩余容量加权**随机挑可写(RW)分区(权重 =
  `partition_capacity_bytes − used_bytes`,取自心跳软状态 `used_bytes_by_partition`);候选优先取"至少一个副本节点存活"的,
  全不活则退回全部 RW(避免 master 刚重启、软状态未重建时出现"完全不可写"的假故障)。
- **新 crate `crates/common/router`(router SDK + CLI)**:号段缓存、**W = 实际副本数/2+1(多数派,实算,不做配置项)**、
  **R=1 首个成功**,变更类(删/复活/TTL)同样多数派;`put_file`/`get_file` 流式;CLI `antfs-bench-router`
  提供 `put/get/head/delete/alloc/bench`。
- **新 crate `crates/frontend`(HTTP REST 对象接口层)**:`POST|GET|HEAD|DELETE /blobs[/{blobId}]`、
  `POST /blobs/{blobId}/undelete`、`POST /blobs/{blobId}/ttl?expiration_ms=`、`POST /stitch`(分片拼装,无状态)、
  `GET /metrics`;内嵌同一个 `Router`,**不重复实现 W/R**。
- **store 侧配套**:
  - `PUT ...?expiration_ms=`(建对象时即带 TTL,分片孤儿回收需要);
  - 新端点 `GET .../blobs/{key}/metadata`(只读元数据不读数据体,供 frontend 的 `HEAD`);
  - **流式 put**:有 `Content-Length` 时 body 边收边落**该副本所在盘**的暂存文件(内存 O(64KiB)),
    再交引擎新 API `BlobStore::put_from_reader` 按已知长度顺序写;无 `Content-Length` 回落原缓冲路径(向后兼容);
    暂存文件成功/失败都删,崩溃遗留由 `DiskManager::mount` 清理;
  - `Error::StoreFull` → HTTP **507**(不再混进 500);
  - 沿用既有 `store_server.max_body_bytes`(默认 2 MiB,**不擅自改默认**)作为单请求上限。
- **`crates/blob` 引擎**:`LogSegment::{reserve,write_reserved,commit_reserved}` + `Log::reserve`(`Reserved` 句柄)+
  `BlobStore::put_from_reader`;记录仍是**连续前缀 + 锁内顺序写**,**不改磁盘格式、不引入洞**。
- **devcluster**:一并起 frontend、修过期示例(示例 key 是 64 hex,现在只认 16 hex)、等到副本真建出来再报"就绪"、
  打印可复制的 curl 与两条 bench 命令。
- **文档修正**:`cluster.md` 删掉「router SDK 取代 frontend / 去掉 frontend 的连带」,改为 **SDK + frontend 并存**;
  新增 `docs/design/{client.md,frontend.md}`;`README.md` 模块表补 admin/client/frontend。

## Capabilities

### New Capabilities
- `blob-io-end-to-end`:master 发号 → client/frontend 直连副本的**完整写读路径**(W 多数派写、R=1 读、流式大文件、分片拼装),
  以及对外 REST 入口与端到端压测工具。

### Modified Capabilities
<!-- master 控制面新增一个写 RPC;store 数据面 PUT 增加 TTL 查询参数与流式路径、新增 metadata 端点、StoreFull 改 507;
     blob 引擎新增流式写原语;cluster.md 的 frontend 角色决策被修正。 -->

## Impact

- **crate**:`blob`(流式写原语)、`master`(proto + idalloc + config/metrics + handler)、`store`(PUT TTL/流式/metadata/507)、
  **新增** `client`、`frontend`、`devcluster`(接线)、根 `Cargo.toml`(members)。
- **契约变化**:`GetBlobIds` RPC;store `PUT` 新增可选 `?expiration_ms=`、新增 `.../metadata` 端点、写满由 500 改 **507**;
  PUT 在**带 `Content-Length`** 时改为流式(无 `Content-Length` 的 chunked 请求行为不变)。
- **持久性红线(§3.1)**:流式写复用既有"记录 = 连续前缀 + 锁内顺序写"不变式,记录在 `Reserved::commit` 前不对读者可见,
  崩溃语义与整块写**完全一致**(恢复只认 header 里上一次 flush 的边界);暂存文件从不参与持久性(未提交即丢弃)。
  发号"只发已提交水位以内 + 永不复用"由 Raft 多数派 fsync 保证。
- **安全边界**:`GetBlobIds` 需准入密钥;数据面仍无鉴权(既有取舍,D6:靠网络隔离/网关 ACL 兜底)。
- **被否决方案**:见 `design.md`(in-log 预留槽零拷贝写、服务端 session 表、把 W 做成配置项、给 frontend 加 account/container)。
