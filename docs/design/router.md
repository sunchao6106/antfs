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
allocate_one(): 本地号段里**按 partition 写权重**取(平滑加权轮转) → offset++(零 RPC)
                所有分区的号段都取完 → 补段:ListPartitions 拿当前可写(RW)分区列表 + **写权重**
                                      → 总预留(分区数 × id_segment_size)按写权重切份额
                                      → 对每个缺段的分区各 GetBlobIds 该份额
                                        (master 侧先 Raft 抬水位再发,见 blobid-master-assign)
put(key, bytes): 解析 key 的高 32 位得 partitionId
                → 取该 partition 的**全部**副本位置(发号响应带回,或 LookupPartition)
                → 并行 HTTP PUT 到所有副本
                → 收满 W = 副本数/2+1 个 ack 即返回成功(其余请求被取消,由拉复制补齐)
```

- **发号按 partition 写权重分配(既打散写 IOPS,又按容量分配)**:稳态下 router **每个可写分区各持一段**号,取号
  在它们之间**按写权重**挑 —— 相邻写仍落在不同分区(进而是不同副本、不同盘),不在一个分区上连续耗尽一段号,
  同时**越空的分区拿到越多号**。权重由 **master 下发**(`PartitionEntry.write_weight`,**千分比** `0..=1000`):
  `1000` = 该分区完全空,越接近 `1` 越满;`0` = **未下发 / 无意见 → 按均权处理**(旧 master 兼容,CLAUDE.md §3.4);
  计算值**下限为 1**,故 `0` 永不表示"零分配"。router **只当相对权重用** —— **不自算容量、不解释 policy**(容量
  口径只活在 master;权重怎么算、将来是否由 admin 手动设置,与客户端无关)。取号用**平滑加权轮转(SWRR)**:每拍给
  在册分区各加 `weight` 信用、取最大者、再减去在册权重总和,于是**任意窗口**内的取号比例都趋近权重之比。这一条
  **不能只靠"按权重切段大小 + 顺序轮转取号"实现**:顺序轮转每轮给每个分区各 1 个号,任何中途窗口内各分区计数都
  **相等**,权重只能决定"最满的那个何时停止发号";所以**取号本身必须按权重挑**,段大小再按权重配比(让各分区同步
  耗尽)。分区集合仍由 **master** 给(router 不自造 id):补段用 `ListPartitions(state=RW)` 取权威可写列表 + 显式
  点名 `partition_id` 的 `GetBlobIds`(master 复核该分区确实可写、有副本)。单个分区取号失败(期间转 RO/封存)只
  跳过它。收口了上一轮 change 自认的偏离(均匀轮转),见 `openspec/changes/router-weighted-partition-alloc`。
- **号段份额与段尾收敛**:号段总预留 = `id_segment_size × 可写分区数`(**与均权轮转时不变** —— 空洞上界、换段频率
  都不变),按权重切成各分区份额 `max(1, pool × w_i / Σw)`(余数按小数部分降序、pid 升序分配)。下界 `1` 保证不
  存在 0 长度的段 —— "这个分区不发号"应当由 **master** 把它转 RO 来表达,不是靠发个空段。**均权时每份恰好
  `id_segment_size`**,逐位退化为原来的均权轮转。**补段 / 后台刷新还按新份额收敛已持有号段的段尾**
  (`end = min(end, next + share)`):收敛掉的余号成**空洞**(D3 无害),收敛到没号的段直接丢掉 —— 这一步把
  "权重改动何时生效"压在**一个刷新周期**内,不必等整池号段自然轮完。
- **反馈滞后与生效粒度(如实)**:权重来自 store 心跳上报的 `used_bytes`(`store_server.heartbeat_interval_secs`
  默认 3s),router 只在**补段 / 后台刷新**那一拍拉取(后台周期 `router.partition_refresh_secs` 默认 30s)。它是
  **心跳级的粗粒度容量反馈,不是精确配额** —— 只做粗粒度修正,不声称各分区能被用到毫厘不差。`partition_refresh_secs
  = 0`(关后台刷新)时退化为"整池轮完才更新权重";可接受:号段亏空的写路径自己会同步一次。
- **补段的代价**:一次补段 = 1 次 `ListPartitions` + 「分区数」次 `GetBlobIds`,整轮在号段锁里单飞(并发调用各补一轮会放大
  RPC 并可能重号)。故 `id_segment_size` 是**每个分区**的号段大小,稳态持有「可写分区数 × N」个号。
- **可写分区列表由后台任务保鲜**:`connect` 起一个后台任务,按 `partition_refresh_secs` 周期拉一次列表(与**写权重**
  同一份来源)—— 已不可写的分区**丢掉号段**(余号成空洞,无害,D3)、新出现的可写分区**补一段**、已有的段按新份额
  收敛段尾;**信用只留给手上还有号段的分区**(补段的分区从 0 起,不留空转期攒下的信用变成突发)。号段耗尽的写路径
  自己也会同步一次(故周期配 `0` 也能跑,只是缓存跟不上分区增减)。任务持号段池的 `Weak`,最后一个 `Router` 析构后
  自行退出;一次什么都没补的空刷新不会把"下一个轮到谁"清空重来(由信用高低决定相位)。

- **W 是实算的,不是配置项**:RF=3 → 2(等于 Ambry `routerPutSuccessTarget=2`)、RF=2 → 2、RF=1 → 1、RF=5 → 3。
  低于多数派的写会破坏"已确认 ⇒ 多数副本有"的前提,故**不提供配置开关**(避免有人调低它静默削弱持久性)。
  副本数取**放置全量**(不是"活着的副本数"):RF=3 掉一个节点仍写得进,掉两个就写不进(正确 —— 少数派不接受写)。
- **提前返回 = 取消剩余请求**:第 3 份靠异步拉复制收敛(`cluster.md §10`),这是 W 语义的应有之义,也是延迟的来源。
- **失败即报错,不偷偷换 partition 重写**:换分区重写会掩盖问题且可能与已部分写入的副本产生分叉;重试由调用方决定。
- 变更类(`delete`)同样走多数派扇出。**TTL 只能在写入时给**:`put_with_ttl(key, data, expiration_ms)` / `put_file_with_ttl(path, expiration_ms)` 把绝对过期时刻(`expiration_ms`)随写一并扇出(单次多数派写);`put` / `put_file` 是 `NO_EXPIRATION`(常量由 `antfs_router` 再导出)的包装,等价于"永不过期"。写入之后**没有任何改 TTL 的接口**。
- **`put_auto` / `put_auto_with_ttl`(内部发号 + 507 换分区重试)**:适用于上层**不关心落在哪个分区**的场景(frontend 小 body 上传、S3 网关)。内部 `allocate_one` → 多数派写 → 全副本 507 时摘掉该分区、重新发号到另一个分区重试(最多 `MAX_STORE_FULL_RETRIES=2` 次,共 3 次尝试),语义与 `put_file_with_ttl` 的重试逻辑完全一致,返回 `(StoreKey, PutReport)`。`put_with_ttl(key, data)` 的 key 由调用方预分配,因此无法换分区——只摘掉号段并返回 `StoreFull`。
恒切管线(2026-10-21 下沉,`upload-zero-spool` design §12):`put_with_ttl` /
`put_auto_with_ttl` 收到超过 `put_bytes_chunk_bytes` 的 body 时内部切片 —— 每片 =
一次**普通 blob 多数派写**(经 `put_auto_keyed` 内环:发号 + 507 换分区重试;SWRR 自然
把片散向不同可写分区),尾笔把拼装描述符以 composed 位落库(既定 key 形落在调用方
key 上)。失败 = 已入库片逐片**尽力墓碑** + 原错误透传;超容量(`MAX_CHUNKS × 片宽`)
前置 `too_large` 拒绝(一字节未写)。网关侧恒切(auto 段)与本管线共用同一底层。

## 3. 读路径(R = 1;读口四形态,2026-10-21 读侧下沉)

| 读口 | 语义 | 典型调用方 |
|---|---|---|
| `get(key)` | **逻辑对象整读**(Bytes):拼装对象自动逐片拼回真数据;普通对象原字节。实现 = `stat ‖ 裸读` 并发(`composed` 位是唯一类型判定)再分岔 | SDK 直连 / 压测(bench) |
| `get_object_stream(key)` | **逻辑对象流式读**:`(逻辑长度, 流)`,内存 = 传输块级,不落盘不整体进内存 | 网关下载(REST `/blobs` GET) |
| `get_object_range_stream(key, start, len)` | **逻辑对象窗口流式读**(blob-range-read):普通对象走数据面 `?offset=&length=`;拼装对象映射到**覆盖片**(边界片裁边,只取窗口相关片) | 网关部分读(REST/S3 Range) |
| `get_blob(key)` | **裸 blob 读**(受控口):字节原样、单请求无前置 stat;拼装对象返回描述符字节。只服务“已知普通 blob”热路径(S3 单体读) | S3 网关 |
| `stat_object(key)` | 逻辑对象**元数据**:`size` = 逻辑大小(拼装 = 各片之和)、`chunks` = 片数、deleted/expired | 网关 HEAD |

- R=1 逐副本(**`up` 的排在前面**),**第一个 200 即返回**;全部 404 → `NotFound`;还有副本不可达 → 报最后一个真实故障(不能把"读不到"当"不存在")。
- 窗口读的**覆盖片映射**对齐 Ambry `GetBlobOperation.getStoreKeysInByteRange`:片完整性核对入流内(尺寸不符 = 响亮错,不静默截短)。
- 已删/已过期对象:与数据面 GET 拒读语义对齐(逻辑口回 `NotFound`)。
- **不做落盘式读**(裁决⑭):`get_file` 已随读侧下沉移除,文件级消费由调用方对流自管。
- blob 不可变 → 不需要 `R + W > N`,R=1 足够;W=2(及更多)保证 ≥ 多数副本有,连试几个必命中。

## 4. 失败语义(稳定契约)

`Error::kind()`(见 `crates/common/router/src/error.rs`)既是 Prometheus label,也是压测错误分桶口径:

| kind | 触发 | 含义 / 处置 |
|---|---|---|
| `connect` / `rpc` | 连不上 master / gRPC 传输或状态错误 | 检查 `router.master_addr` 是否指向 leader |
| `not_leader` | master 是 follower 而发号打到它(`master-leader-address` 2026-10-20) | 回执带 leader 的 gRPC 提示;router 发号路径**自愈**:单飞切换 master 通道(1s 防抖)并重试一次,切主后指错配置自动恢复;无提示(注册窗口)原样上抛 |
| `rejected` | master 明确 `ok=false`(非传输错误) | 文案里带原因(如配额/维护等业务性拒绝) |
| `no_writable_partition` | 集群没有可写(RW)分区 | 建分区 / 把分区解封 |
| `id_exhausted` | 该 partition 的 u32 objectId 空间耗尽 | **封存该分区**(绝不复用号) |
| `http` | 数据面连不上/超时 | 副本不可达;换副本或等它恢复 |
| `status` | 副本返回非预期状态码 | 见 detail |
| `already_exists` | 副本返回 409 | keyed 写「多数派未达且已收集失败全为 409」时仲裁环直接透传(`router-conflict-surfacing` 2026-10-20,混合失败仍 Quorum);auto 路径出现 = **号被复用**(严重问题,不换号重试、直接尖叫)。重复 `DELETE` 已幂等回 204,不产生 409 |
| `store_full` | 副本返回 507 | 该分区写满 → 加分区(别当 500 忽略) |
| `no_replica` | 副本返回 404「no replica for partition」 | 路由到了不持有该分区的节点 |
| `silenced` | 副本返回 503 + `Retry-After` | 运维**请求静默**止血,不是故障 |
| `not_found` | 所有副本都 404「对象不存在」 | 真不存在(或已删/过期) |
| `config` / `metric` | `RouterConfig` 加载/校验失败、指标注册失败 | 启动即败类,不是运行期路由问题 |
| `http_client` | 构建数据面 HTTP 客户端失败 | 初始化问题(如 TLS/超时配置非法) |
| `io` | `put_file` 本地文件 IO 失败 | 本地盘问题;请求报错,重试另选文件 |
| `too_large` | 恒切管线容量拒绝(body > `put_bytes_chunk_bytes × MAX_CHUNKS`,一字节未写) | 调用方缩小重试或走流式口(2026-10-21 恒切下沉) |
| `bad_composed` | 拼装对象读回时描述符/片不一致(上游损坏类) | 响亮上抛不静默(CLAUDE.md §3.3);网关映射 502 类口径 |
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
| `router.id_segment_size` | u32 | `1024` | ≥1 | **均分时**每个可写分区一次向 master 申请的 blobId 个数(号段 N)。总预留 = 分区数 × 该值(SWRR 分配的**总**号码池;**实际按写权重配比**切成各分区份额)。越大越省 RPC、router 崩溃留下的空洞越大(空洞无害,D3) | 无(Ambry 的 id 是本地随机生成,无发号) |
| `router.partition_refresh_secs` | u64 | `30` | ≥0(`0` = 关闭) | 后台刷新**可写分区列表**的周期(秒):已不可写分区的号段被丢掉、新出现的可写分区补一段 | 无 |
| `router.latency_aware_routing` | bool | `true` | — | **读路由按观测延迟排序副本**(慢盘处置的客户端侧那一半):读恒为 R=1(第一个成功即返回),故"先试哪个副本"直接决定读延迟。开着时记录每副本请求耗时的 EWMA,读时 **`up` 优先 → 延迟升序 → master 原序**;未测副本按"已测均值"当先验;无数据时是**恒等变换**。**只改顺序、不改候选集** ⇒ 不改变 W/R 与冲突解决语义。关掉用于定位/对照压测。详见 [`cluster.md` §9.3](./cluster.md) | 无 |
| `router.put_bytes_chunk_bytes` | u64 | `4194304`(4 MiB) | `0`(关)或 64 KiB ..= 1 GiB | **SDK 直连大 Bytes 写的恒切片宽**(2026-10-21 下沉,`upload-zero-spool` design §12):body 超过它时 `put`/`put_auto` 族内部切片(片 = 普通多数派 blob,发号 SWRR 散向可写分区)+ 尾笔拼装描述符,返回描述符 blobId;读回走 `get`/`get_object_stream` 透明拼装。**网关内嵌 `[router]` 表须交叉校验"网关自身片/单体宽 ≤ 本值"**(frontend/s3 的 `validate_all` 已带,防止 gateway 片被二次切)。对齐 Ambry `router.max.put.chunk.size.bytes`(恒切无开关,此间以 0 表达"不切") | `router.max.put.chunk.size.bytes`(Ambry 4 MiB) |

> **请求哪个 partition**:补段时先 `ListPartitions(state=RW)` 拿可写分区列表,再对**每个**分区显式点名
> `partition_id` 申请一小批(master 复核该分区 RW 且**至少有一个副本**,否则 `ok=false` 只跳过该分区)。
> `partition_id` 缺省 = 由 master 按剩余容量加权挑一个 —— **分区分派始终由 master 定**,router 不自选(D10)。
> **权重的来源**是同一份列表里的 `PartitionEntry.write_weight`(千分比;`0` 或未下发 = 按均权,见 §2);
> 客户端只把它当**相对**权重用,不自算容量、不解释 policy。
> (早期版本用"`0` 表示自动",导致第 0 号分区无法被点名,已改。)

> **没有 W/R 配置项**(见 §2)、**没有重试次数配置项**(失败即返回,重试由调用方决定)。
 > 唯一例外:`put_file_with_ttl` 与 `put_auto_with_ttl` 在**全部副本都报 507(分区写满)**时内部换分区重试(写死最多 2 次,
> 见 §6 `router_put_store_full_retries_total`);`put_with_ttl` 的 key 已绑定分区,只摘掉该分区号段并返回
> `StoreFull`,不重试。
>
> 若要从 TOML 文件加载(正式入口目前只有 benchmark 的 CLI flag 与代码内构造两条,无独立 router 二进制),同样要像 frontend 那样包一层表名
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
| `router_put_store_full_retries_total` | counter | — | 次 | 因分区写满(全副本 507)而**换分区重试**的次数(`put_file_with_ttl` / `put_auto_with_ttl` 内部重试,最多 2 次) | 无 |
| `router_get_total` | counter | `outcome` | 次 | 读请求数 | `RouterMetrics.getBlobOperationRate` |
| `router_get_bytes_total` | counter | — | 字节 | 成功读出的字节数 | 无 |
| `router_mutation_total` | counter | `op`, `outcome` | 次 | `delete`/`stat` 操作数 | `RouterMetrics.*OperationRate` |
| `router_partition_refresh_total` | counter | `outcome` | 次 | 后台刷新可写分区列表的次数(失败为 `error`) | 无 |
| `router_writable_partitions` | gauge | — | 个 | 本地**持有号段的分区数**(后台刷新后刷新;0 = 当前无可写分区) | 无 |
| `router_put_chunked_total` | counter | `outcome`(ok / fail / reject) | 次 | 经恒切管线的**逻辑对象**写入数(对象粒度;reject = 超容量前置拒绝,一字节未写) | 无(Ambry 无此分立观测) |
| `router_put_chunked_chunks_total` | counter | — | 次 | 恒切管线成功写入的片数(片级多数派写本身另计 `router_put_total`) | 无 |
| `router_put_chunked_orphan_deletes_total` | counter | — | 次 | 恒切失败回收发出的片**墓碑尝试**数(尽力口径,删成败不在此区分) | 无 |
| `router_get_chunked_total` | counter | — | 次 | 逻辑读(`get`/`get_object_stream`/`get_object_range_stream`)自动拼装返回的逻辑对象数;每次片的实际传输已由 `router_get_total`/`router_get_bytes` 覆盖 | 无 |
| `router_topology_refreshes_total` | counter | `reason` | 次 | 读拓扑重拉次数(`ttl` = TTL 到期 / `evict` = 全副本不可达驱逐;`router-read-topology-refresh` 2026-10-20) | 无 |
| `router_id_prefetch_errors_total` | counter | — | 次 | 号段后台预取失败数(成功不打 —— 成功即用耗尽次数的下降体现;`router-id-prefetch` 2026-10-20) | 无 |

> **权重是否生效怎么观测(本次不新增指标)**:发号按写权重分配本身**不打点** —— 它是一条分配策略,不是故障。
> 要核它是否生效,看这两处:`Router::write_weights() -> Vec<(PartitionId, u32)>`(当前生效的权重只读快照,pid 升序)
> 与 `benchmark/router` 打印的分配分布报告 —— 后者把「权重 → 期望 %」与实际落点分布**并列**输出,一眼可核。
> 实战读法:若实际分布与期望比例长期背离,先怀疑**刷新滞后**(权重只在补段/刷新那一拍生效,见 §2)与
> `partition_refresh_secs` 的取值,而不是怀疑发号逻辑。

## 7. 与 Ambry 的差异 / 已知取舍 / 待办

**恒切下沉 + 逻辑读口**(2026-10-21,`upload-zero-spool` design §12;读侧裁决⑨/⑩/⑭):
- 写:`put`/`put_auto` 族遇超片宽 body 内部切片(片 = 普通多数派 blob,SWRR 散分区)+ 拼装描述符;
  失败 = 已入库片尽力墓碑 + 原错误透传;超容量 = `too_large` 前置拒绝。网关内嵌本表须交叉校验
  "网关片/单体宽 ≤ `put_bytes_chunk_bytes`"(防二次切;frontend/s3 `validate_all`)。
- 读:`get/get_object_stream/get_object_range_stream/stat_object` 以**逻辑对象**为口径(拼装自动拼回、
  窗口只取覆盖片);裸字节只剩 `get_blob` 受控口。`get_file`(落盘式读)已删(⑭)。
- 与 Ambry `NonBlockingRouter` 同型(恒切 + composite 重组),差异 = 描述符为普通 blob 落库
  (composed 位类型判定)而非 Ambry 的元数据侧复合。

**SDK 透明压缩已整体移除**(2026-10-21 所有者裁决⑬;曾以 `router-blob-compression` /
`router-putfile-compression` 落地):五旋钮(`compression_enabled`/`algorithm`/
`min_size_bytes`/`min_ratio`/`level`)、帧形态(17B 头自描述)、三闸、流式临时帧文件、
读侧 bulk/流式解压、相关指标与 e2e 全数退场。**记档约束**:存量的压缩帧 blob 将按**原字节**读回
(不再透明解压);**重开时的 TODO**(所有者明示):等读接口定型后,基于流式读口
(`get_object_stream`/窗口流)重设计 —— 帧头兼容存量对象是硬前提;`get_object_range_stream`
的窗口必须落在解压后坐标(或显式拒绝帧对象,勿切坏帧)。在此之前任何仓勿开启旧帧写入路径(已无)。

**相对 Ambry 的差异(有意为之)**:
1. **blobId 由 master 发号**(号段批处理 + Raft 水位),不是 router 本地随机铸 —— 见 `blobid-master-assign` D1–D4。
   代价:写路径多一次 master RPC(经号段摊薄至 ≈0)+ 号可枚举(D6 已知取舍);
   收益:id 连续可数、partition 由 master 按容量兼顾均衡挑选(D10)、无"随机撞陈旧分区"问题。
2. **partition 由 master 选**(D10):客户端不再本地挑可写分区。它现在**消费 master 下发的相对写权重**
   (`PartitionEntry.write_weight`)来决定"同一时刻该用哪个可写分区发号",但**仍不自选分区、不自算容量**
   —— 权重怎么算(policy)只活在 master 侧。
3. `stat`/`delete` 直接对数据面端点,无 account/container 维度(D7:blobId 64 位塞不下)。

**已知取舍**:
- 号段内已发放但未使用的号 = **空洞**(router 崩溃、换段时丢弃;按新份额**收敛段尾**也会留下空洞);
  master 侧 `master_id_alloc_holes_total` 可观测。**任何逻辑不得假设"发了号 = 对象存在"**(D3)。
- 读路由缓存(`partitionId → 副本位置`)**不过期**:副本迁移后可能试到已迁走的副本 —— 读会换副本成功(R=1),
  写扇出则可能打不满 W。当前靠"重启进程 / 重新 LookupPartition"收敛。
- **写失败不做跨 partition 重试**(避免掩盖问题)—— 指 `put_with_ttl`(key 已绑定分区)。`put_auto` / `put_auto_with_ttl` / `put_file_with_ttl` 因内部持有发号权,在**全副本 507** 时会换分区重试(最多 2 次);部分 507(多数派仍可达)不重试。
- **按权重发号只是"按容量粗粒度分配",不是精确配额**:权重来自心跳级的 `used_bytes`(§2 的反馈滞后)。
  极端偏斜(如权重 1000:1)时最满的分区每轮只拿 1 个号(≈0.1%)—— 它**仍是可写分区**,写进去仍可能 507,
  **错误照旧上报**,不静默吞掉;正确处置是运维把它转 RO / 封存。
- **SWRR 每次发号 O(n)**(n = 本地持段分区数,即"可写分区数")。它与 `id_segment_size` 相乘构成总预留,
  实际规模有限;十万级分区需要换 O(log n) 结构(见待办)。

**待办**:
- [x] ~~leader 重定向~~ —— **已完结**(`master-leader-address` 2026-10-20:master 节点启动自报
      gRPC 端点进状态机(幂等 upsert,可丢可重建),`leader_addr` 回填以自报表为**唯一真值**
      (所有者裁定:无历史包袱,不做 raft 地址回退/缺字段容错);router 发号遇 `NotLeader{hint}`
      **单飞切换 master 通道**(1s 防抖,Mutex 内整体换 client)+ 重试一次,切主后配置指错自动
      恢复;无提示窗口期原样上抛供上层重试)。
- [x] ~~号段后台预取~~ —— **已完结**(`router-id-prefetch` 2026-10-20:发号时段位进入低水位
      (剩余 < ¼ 段长或 ≤ 2 号)→ 单飞预取同分区一段(同区一拍一次,失败静默计数
      `router_id_prefetch_errors_total`);回填**无缝续段**(master 连号恰对上即零空洞续,
      对不上保守丢弃不造额外空洞),拓扑条目顺带刷新;耗尽的同步补段路径原样保留 = 正确性
      零依赖)。
- [x] ~~读路由缓存 TTL / 失败后强制刷新~~ —— **已完结**(`router-read-topology-refresh`
      2026-10-20:条目带时戳,TTL = `partition_refresh_secs`(与后台刷新共用旋钮,0=关)，
      过期读前 Lookup 重拉(失败保旧表,读可用性优先);R=1 全副本失败且含不可达类 → 驱逐
      该分区条目 + 重拉一次,新列表不同则本次读重试一次(单请求有界);纯 NotFound miss 不
      驱逐。观测 `router_topology_refreshes_total{reason=ttl|evict}`)。
- [x] ~~`put*` 增加 `expiration_ms` 参数~~ —— **已落地为 `Router::put_with_ttl` / `put_file_with_ttl`**(写入时定 TTL,frontend 的 `x-antfs-ttl-ms` 已是单次原子写)。
- [ ] 大对象靠分片 + `frontend` 的 `/stitch`(本 crate 不感知)。
- [x] ~~上传/下载路径零本地临时盘(读写两侧)~~ —— **读侧已全清**(2026-10-21:网关下载走
      router 逻辑流(`get_object_stream`,裁决⑫);SDK 落盘式读口 `get_file` 已删(裁决⑭);
      `put_file` 的压缩帧临时文件随压缩整体移除(裁决⑬)一并消失,`put_file` 直接流式发
      副本文件)。**写侧网关面已零盘**(`upload-zero-spool` ④ 恒切+收账,前期落地);剩余 =
      B 段(router 压缩重开后的超上界两遍流式 —— **待压缩接口定型**,见 §7 移除记档)与
      A 段(store 暂存 `<data_dir>/tmp/` 的引擎侧去留,原方案归档
      [`openspec/changes/upload-zero-spool/`](../../openspec/changes/upload-zero-spool/) design §4)。
- [ ] **admin 手动权重入口 → master**:现在写权重只有"由容量现算"一个来源(`capacity_weight_permille`)。要做人工
  压权重,路径是 admin → master 写进同一处 → 同一个 `write_weight` 字段下发,**客户端零改动**(router 只当相对权重用)。
- [ ] **SWRR 的 O(n) → O(log n)**:分区数上千时每次发号遍历在册分区不划算,需要换堆 / 树的加权调度结构。当前
  规模(`可写分区数 × id_segment_size`)下 O(n) 可接受,故本轮不做。
- [ ] **507 / silenced 驱动的本地降权**:现在客户端**不**根据写失败自行降权(失败照旧上报;它只消费 master 下发
  的相对权重)。若将来要让 507 立即影响本地分配,须先从"运维动作(转 RO)"与"客户端自降权"之间选定一侧。

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
