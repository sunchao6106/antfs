# router(客户端 SDK / 通用组件)

> 对标:Ambry 的 **`NonBlockingRouter`**(不是 `ambry-frontend`)。Ambry 把 router 做成库嵌进 frontend/应用;
> antfs 同样把它做成**纯库**(`crates/common/router`,package `antfs-router`,主类型 `Router`)——
> [`frontend`](./frontend.md)(REST 网关)与 `benchmark/router`(压测)都依赖它,**W/R 逻辑只有一份实现**。

## 1. 定位

```
① 直连(SeaweedFS 式:调用方内嵌 router,低延迟少一跳)
   app ──gRPC GetBlobIds──▶ master   回 (可写 partitionId, objectId 号段, 副本位置)
       ──HTTP PUT ×N──────▶ store ×N  收 W 个 ack 即返回

② REST 网关(frontend 内嵌**同一个** Router)
   curl ──HTTP──▶ frontend ──GetBlobIds + PUT ×N──▶ master / store ×N
```

`Router` 负责:向 master 取 **blobId 号段**、按 blobId 定位副本、**W 多数派写**、**R=1 读**、变更类操作(删除)与**写入时定 TTL**。
它**不做**协议翻译(frontend 的事)、不做 ACL/配额(网关层远景)、不碰数据字节的存储语义(store 的事)。
本 crate **不提供任何二进制**——压测见 `benchmark/router`,REST 入口见 `crates/frontend`。

## 2. 写路径(W = 实际副本数 / 2 + 1)

```
allocate_one(): 号段内有号 → 本地 offset++(零 RPC)
                号段耗尽  → GetBlobIds 向 master 取一段(master 侧先 Raft 抬水位再发,见 blobid-master-assign)
put(key, bytes): 解析 key 的高 32 位得 partitionId
                → 取该 partition 的**全部**副本位置(发号响应带回,或 LookupPartition)
                → 并行 HTTP PUT 到所有副本
                → 收满 W = 副本数/2+1 个 ack 即返回成功(其余请求被取消,由拉复制补齐)
```

- **W 是实算的,不是配置项**:RF=3 → 2(等于 Ambry `routerPutSuccessTarget=2`)、RF=2 → 2、RF=1 → 1、RF=5 → 3。
  低于多数派的写会破坏"已确认 ⇒ 多数副本有"的前提,故**不提供配置开关**(避免有人调低它静默削弱持久性)。
  副本数取**放置全量**(不是"活着的副本数"):RF=3 掉一个节点仍写得进,掉两个就写不进(正确 —— 少数派不接受写)。
- **提前返回 = 取消剩余请求**:第 3 份靠异步拉复制收敛(`cluster.md §10`),这是 W 语义的应有之义,也是延迟的来源。
- **失败即报错,不偷偷换 partition 重写**:换分区重写会掩盖问题且可能与已部分写入的副本产生分叉;重试由调用方决定。
- 变更类(`delete`)同样走多数派扇出。**TTL 只能在写入时给**:`put_with_ttl(key, data, expiration_ms)` / `put_file_with_ttl(path, expiration_ms)` 把绝对过期时刻(`expiration_ms`)随写一并扇出(单次多数派写);`put` / `put_file` 是 `NO_EXPIRATION`(常量由 `antfs_router` 再导出)的包装,等价于"永不过期"。写入之后**没有任何改 TTL 的接口**。

## 3. 读路径(R = 1)

```
get(key): 解析 partitionId → 副本位置(**`up` 的排在前面**)
        → 逐个副本 HTTP GET,**第一个 200 即返回**
        → 全部 404 → NotFound;还有副本不可达 → 报最后一个真实故障(不能把"读不到"当"不存在")
```

blob 不可变 → 不需要 `R + W > N`,R=1 足够;W=2(及更多)保证 ≥ 多数副本有,连试几个必命中。

## 4. 失败语义(稳定契约)

`Error::kind()`(见 `crates/common/router/src/error.rs`)既是 Prometheus label,也是压测错误分桶口径:

| kind | 触发 | 含义 / 处置 |
|---|---|---|
| `connect` / `rpc` | 连不上 master / gRPC 传输或状态错误 | 检查 `router.master_addr` 是否指向 leader |
| `rejected` | master 明确 `ok=false`(非传输错误) | 文案里带原因(如 "not leader …") |
| `no_writable_partition` | 集群没有可写(RW)分区 | 建分区 / 把分区解封 |
| `id_exhausted` | 该 partition 的 u32 objectId 空间耗尽 | **封存该分区**(绝不复用号) |
| `http` | 数据面连不上/超时 | 副本不可达;换副本或等它恢复 |
| `status` | 副本返回非预期状态码 | 见 detail |
| `already_exists` | 副本返回 409 | 新号段却被判已存在 → **号被复用**(blobId 永久占用),属严重问题,必须查(重复 `DELETE` 已幂等回 204,不再产生 409) |
| `store_full` | 副本返回 507 | 该分区写满 → 加分区(别当 500 忽略) |
| `no_replica` | 副本返回 404「no replica for partition」 | 路由到了不持有该分区的节点 |
| `silenced` | 副本返回 503 + `Retry-After` | 运维**请求静默**止血,不是故障 |
| `not_found` | 所有副本都 404「对象不存在」 | 真不存在(或已删/过期) |
| `quorum` | 未收满 W 个 ack | detail 里给出逐类失败计数 |
| `partition_mismatch` | blobId 自带 partition 与目标不一致 | 拼错了 |

## 5. 配置项清单

`RouterConfig`,TOML **`[router]`** 表(CLAUDE.md §7 必须项;默认值/校验与 `crates/common/router/src/config.rs` 的
`impl Default` + `Validate` 逐项对齐)。frontend 内嵌它时放在 **`[frontend.router]`** 子表下(同一份结构)。

| key(TOML) | 类型 | 默认值 | 取值范围/校验 | 含义/影响 | 对齐的 Ambry 原配置 |
|---|---|---|---|---|---|
| `router.master_addr` | String | `127.0.0.1:7101` | 非空 | master 控制面 gRPC 地址。**须指向 leader**(见 §7 待办) | Ambry 从 clustermap 拿地址,不配 |
| `router.cluster_secret` | String | `""` | — | 集群准入密钥(与 master 一致);空 = 关校验(仅本地测试) | 无(Ambry 用 account/container 鉴权) |
| `router.request_timeout_secs` | u64 | `10` | ≥1 | 单请求超时(同时用于 master gRPC 与数据面 HTTP) | `router.routerRequestTimeoutMs`(默认 20s) |
| `router.id_segment_size` | u32 | `1024` | ≥1 | **一次向 master 申请的 blobId 个数**(号段 N):越大越省 RPC、router 崩溃留下的空洞越大(空洞无害,D3) | 无(Ambry 的 id 是本地随机生成,无发号) |

> **请求哪个 partition**:`GetBlobIds` 的 `partition_id` 是 proto3 `optional` —— **缺省 = 由 master 按剩余容量加权挑**
> (router 走的就是这条);显式点名则要求该分区 RW 且**至少有一个副本**,否则 `ok=false`。
> (早期版本用"`0` 表示自动",导致第 0 号分区无法被点名,已改。)

> **没有 W/R 配置项**(见 §2)、**没有重试次数配置项**(失败即返回,重试由调用方决定)。
>
> 若要从 TOML 文件加载(C 里目前只有 CLI flag / 代码内构造两条路径),同样要像 frontend 那样包一层表名
> (`[router]` 表 → 一个顶层 `{ router: RouterConfig }` 包装),否则 `[router]` 表会被静默忽略。

## 6. 监控项清单

`RouterMetrics`,接入调用方进程的 `Registry`(frontend 用自己那份;压测自建一个)。

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐的 Ambry 原指标 |
|---|---|---|---|---|---|
| `router_id_segments_total` | counter | — | 次 | 向 master 取号段的次数 | 无 |
| `router_id_segment_ids_total` | counter | — | 个 | master 发回的 blobId 总数(含预留未用的号) | 无 |
| `router_put_total` | counter | `outcome` | 次 | 写请求数(成功 `ok`,失败为错误 `kind`) | `RouterMetrics.putBlobOperationRate` |
| `router_put_bytes_total` | counter | — | 字节 | 成功写入的字节数 | 无 |
| `router_put_seconds` | histogram | `outcome` | 秒 | 单次写延迟(含取号段/扇出等待) | `RouterMetrics.putBlobOperationLatencyMs` |
| `router_put_replica_errors_total` | counter | `kind` | 次 | **单个副本**写失败分类(kind 同错误表) | 无 |
| `router_get_total` | counter | `outcome` | 次 | 读请求数 | `RouterMetrics.getBlobOperationRate` |
| `router_get_bytes_total` | counter | — | 字节 | 成功读出的字节数 | 无 |
| `router_mutation_total` | counter | `op`, `outcome` | 次 | `delete`/`stat` 操作数 | `RouterMetrics.*OperationRate` |

## 7. 与 Ambry 的差异 / 已知取舍 / 待办

**相对 Ambry 的差异(有意为之)**:
1. **blobId 由 master 发号**(号段批处理 + Raft 水位),不是 router 本地随机铸 —— 见 `blobid-master-assign` D1–D4。
   代价:写路径多一次 master RPC(经号段摊薄至 ≈0)+ 号可枚举(D6 已知取舍);
   收益:id 连续可数、partition 由 master 按容量兼顾均衡挑选(D10)、无"随机撞陈旧分区"问题。
2. **partition 由 master 选**(D10):客户端不再本地挑可写分区。
3. `stat`/`delete` 直接对数据面端点,无 account/container 维度(D7:blobId 64 位塞不下)。

**已知取舍**:
- 号段内已发放但未使用的号 = **空洞**(router 崩溃、换段时丢弃);master 侧 `master_id_alloc_holes_total` 可观测。
  **任何逻辑不得假设"发了号 = 对象存在"**(D3)。
- 读路由缓存(`partitionId → 副本位置`)**不过期**:副本迁移后可能试到已迁走的副本 —— 读会换副本成功(R=1),
  写扇出则可能打不满 W。当前靠"重启进程 / 重新 LookupPartition"收敛。
- 写失败**不做跨 partition 重试**(避免掩盖问题)。

**待办**:
- [ ] **leader 重定向**:master 现在回填的 `leader_addr` 是 **raft 内部地址**(membership 里存的是 `RaftInternal` 端口),
  不能直接当 gRPC 端用 → SDK 目前**不自动改拨**,只把地址写进错误文案。修法:membership 之外另存 grpc 地址,或由
  调用方在前面挂一层转发。**这是既有写 RPC 的共性问题,不只本 SDK。**
- [ ] 号段**后台预取**(D2 的双 buffer):现在按需取段,号段边界有 RPC 尖峰。
- [ ] 读路由缓存 TTL / 失败后强制刷新。
- [x] ~~`put*` 增加 `expiration_ms` 参数~~ —— **已落地为 `Router::put_with_ttl` / `put_file_with_ttl`**(写入时定 TTL,frontend 的 `x-ambry-ttl-ms` 已是单次原子写)。
- [ ] 大对象靠分片 + `frontend` 的 `/stitch`(本 crate 不感知)。

## 8. 配套工具(不在本 crate 内)

| 工具 | 位置 | 作用 |
|---|---|---|
| `antfs-frontend` | `crates/frontend` | REST 入口(依赖本 crate) |
| `antfs-bench-router` | `benchmark/router` | **直连模式**端到端压测(写 + 可选读 + `--verify`) |
| `antfs-bench-frontend` | `benchmark/frontend` | **REST 模式**端到端压测(只打 HTTP) |
| `antfs-bench-blob` | `benchmark/blob` | 单机引擎层压测(不含集群) |

例:
```bash
antfs-bench-router --master 127.0.0.1:7101 --secret s --ops 20000 --concurrency 32 --size 4096 --verify
antfs-bench-frontend --frontend http://127.0.0.1:7400 --ops 20000 --concurrency 32 --size 4096
```
