## Context

「谁给 blobId、写哪几个副本、怎么算成功、大文件怎么走」这四件事此前只有设计文档(`cluster.md §8/§10`、
`blobid-master-assign`),代码里缺一层。本 change 把它落成可跑通、可压测的一条完整路径,并把
`cluster.md` 中「router SDK 取代 frontend」的决策**修正**为两者并存。

## Goals / Non-Goals

**Goals:**
- master 发号 RPC(号段 + Raft 水位 + 永不复用)可被客户端使用,且有持久性/不重用的测试。
- 写读语义有唯一实现:W = 实际副本数/2+1、R = 1;直连(SDK)与 REST(frontend)共用同一份实现。
- 大文件可传且**内存不随文件大小涨**(网络流式 + 落盘暂存 + 引擎按长度顺序写)。
- 一条命令起全栈(devcluster 含 frontend),两条命令压测(直连 / 经 frontend)。

**Non-Goals:**
- 不做 account/container、ACL、配额、多租户、S3 协议、signed URL(网关层远景;blobId 64 位塞不下租户信息,见 D8)。
- 不做 chunked upload 的**服务端会话/签名片 id**(D6);`x-ambry-session` 是客户端自述、只做一致性校验。
- 不做 client 后台预取(D2 待办)、不做 W/R 配置项(D2 明确禁止)。
- 不做 in-log 预留槽零拷贝写(D5 被否决)。
- 不改任何既有配置项的默认值(尤其 `store_server.max_body_bytes` 保持 2 MiB)。

## Decisions

### D1:frontend 保留,与 router SDK 并存(修正 `cluster.md` 原决策)
原决策「router SDK 取代 frontend,无独立 frontend 集群」的理由是"少一跳、内部可信"。但缺了 frontend 就缺了:
非 Rust 客户端/`curl` 的入口、通用 HTTP 压测工具能打的目标、以及将来 ACL/配额/多租户的**唯一落点**
(blobId 无 account/container 位,只能挂网关层)。故:
- **router SDK = router 库**(选分区/发号/W/R/副本切换),直连模式由应用内嵌;
- **frontend = 该库的 HTTP 壳**,提供 REST 入口。
两者**共用同一份 W/R 实现**(frontend 依赖 `antfs-bench-router`),不重复实现。

### D2:W 由实际副本数实算,不做配置项
`W = 副本数 / 2 + 1`(多数派)。RF=3 → 2(与 Ambry `routerPutSuccessTarget=2` 一致)、RF=1 → 1、RF=5 → 3。
- **不做配置项**:低于多数派的写会破坏"已确认 ⇒ 多数副本有"的前提;给开关等于给"静默削弱持久性"的机会。
- 副本数取**放置全量**(不是活着的副本数):RF=3 掉 1 个仍可写(2 ack),掉 2 个写失败(正确)。
- 收满即返回 ⇒ 取消剩余请求 ⇒ 第 3 份靠**异步拉复制**补齐(`cluster.md §10` 的应有之义)。
- 读 **R=1**:blob 不可变,不需要 `R+W>N`;副本按 `up` 排序,第一个 200 即返回。

### D3:发号沿用 `blobid-master-assign` 骨架,配置 key 扁平化
两级批处理:client 一次要 N 个;master 内存池不够时一次向 Raft 预留 `B = max(请求数,N) × raft_batch_multiple`。
**池上界恒 ≤ 已提交水位** ⇒ 切主/重启后池空、从水位续发 ⇒ 永不重叠;换池时旧池未发完的号成**空洞**(无害,计数观测)。
- 配置 key **扁平化**为 `master.id_alloc_segment_size` / `master.id_alloc_raft_batch_multiple`(改该 change D9 表里的
  点号写法):`[master]` 下现网配置项都是扁平的,为一个子表破坏一致性不值。
- 顶端不足一整批时(`ObjectIdExhausted`)退回按本次请求量再申请一次,**宁可少发也不回绕复用**。

### D4:选 partition 按剩余容量加权(D10 落地)
未指定 partition 时:候选 = RW 分区(优先"至少一个副本节点存活"的子集;若空则退回全部 RW —— 避免 master 刚重启、
软状态未重建时"完全不可写"的假故障);权重 = `partition_capacity_bytes − used_bytes[pid]`(下界 1,防止"报满"分区被饿死);
按权重随机挑。随机只在 leader 侧发生(不进 `apply`,守状态机确定性铁律)。
落地前提已具备:`ReplicaBeat.used_bytes` 与 `SoftState::used_bytes_by_partition` 已在(`metrics-ops-parity` 一并落的)。

### D5:流式 put = 网络流式 + 落盘暂存 + 引擎按长度顺序写(**否决** in-log 预留槽)
- 落法:有 `Content-Length` → body 边收边写**该副本所在盘**的暂存文件(内存 O(64KiB))→ 调
  `BlobStore::put_from_reader(key, len, file, expiration_ms)`:引擎在**既有写锁内**预留 `RECORD_OVERHEAD+len`,
  从 reader 分块拷入,CRC 边写边算,最后 `Reserved::commit` 发布。
- 为什么先落盘再写:**锁内只做磁盘→磁盘拷贝**,不把网络传输时间串进该盘所有写。
- 正确性:记录仍是连续前缀 + 顺序写,**不引入洞**;崩溃时半截记录由既有 CRC 扫描截断淘汰;暂存文件从不提交,
  崩溃遗留由 `DiskManager::mount` 清理。
- 代价:一次额外落盘(双写)。换来内存不随文件大小涨(CLAUDE.md §5.3)。
- **否决方案(零拷贝,值得记录)**:让多个流式写并行占互不重叠的 log 区间、完成顺序串行化后按序发布(可省掉双写)。
  否决理由:它把"已 ack ⇒ 连续前缀"这条不变量变成需要新机制维护的东西,且一个失败的流式写会连带拖死其后所有在飞写;
  按 CLAUDE.md §3.3 属"写路径改动需显式持久性审查"的范畴,不在本 change 冒这个风险。记为后续可选优化。
- 与分片的关系:Ambry 单条记录上限就是 chunk 大小(默认 4 MiB),**任意大对象本来就靠分片 + stitch 承载**;
  流式 put 解决的是**单条大记录的内存占用**,不是对象大小上限。
- **落地细化(实测驱动)**:`≤ INLINE_BODY_MAX`(1 MiB,常量非配置)的 body **走内存路径**(`Bytes` → 引擎整块写),
  **只有更大**的 body 才落盘暂存 + `put_from_reader`。原因:暂存要为每个 PUT 做
  `create/write/close/unlink` 一个文件(~5 次文件系统元数据操作),小对象是绝对主流,在大并发下把 PUT
  p50 从 ~1ms 拖到 ~9ms。实测(devcluster + `antfs-bench-router`,release,4 KiB 对象 / 并发 32):
  **1.77k ops/s → 15.7k ops/s,p50 13.4ms → 1.46ms**。内存上界 = 在途请求数 × 1 MiB,
  仍远优于"整个 blob 缓进内存"——即对**大对象**的流式承诺不变(CLAUDE.md §5.3 针对的正是大对象)。

### D6:stitch 无状态(描述符当 blob 存)
Ambry 用"签名片 id + 客户端会话"把会话信息编进 id;antfs 无签名体系(无 ACL/密钥),故:
- `/stitch` 只校验 `x-ambry-session` 非空 + 片数 ≤ 4096 + 每个片 id 是 16 hex,并对每片 `stat` 一次拿尺寸;
- 把 `[片 id + 尺寸 + 过期时刻]` 列表编成**自描述描述符**(`MAGIC "ASTC" | ver | count | session | entries`)当**普通 blob** 写入;
  ⚠ **已知待议**:描述符与普通对象在存储层**无类型区分**,读路径只能靠"数据体前 4 字节 == MAGIC"嗅探 →
  任何 ≤1 MiB 且以 `ASTC` 开头的普通 blob 会被误判(502 或静默返回别人的内容)。复现、三个选项与
  SeaweedFS 先例(needle 头 `FlagIsChunkManifest` 位)见 `tasks.md` 待办首条。
- 读时若数据体以 MAGIC 开头则解析并逐片取、**流式拼接**;描述符上限 1 MiB,大对象按 `size` 判断避免整体读进内存。
- **不做**:签名片 id、服务端 session 表、递归删片(孤儿片靠 TTL + store 本地 worker 回收)、强制片大小。

### D7:HEAD 需要 store 新增元数据端点
`HEAD /blobs/{id}` 不能靠 `GET` 实现(会把数据体读出来)。store 侧新增
`GET .../blobs/{key}/metadata`(用既有 `BlobStore::stat`,不读数据体);已删/已过期也返回 200 + 标志,
"元数据可读"与"数据体可读"是两回事。

### D8:不做 account/container(连"只作标签"也不做)
blobId 是 64 位(D7 布局永久冻结,无 account/container 位),没有配额/ACL 可挂。REST 路径就用
`/blobs`、`/blobs/{id}`,不引入一个"存不下来又不管事"的路径段。将来多租户落在网关层(与 Ambry
AccountService 同位置),届时再加。

### D9:`StoreFull` → 507
分区写满此前映射成 500,压测/运维看不出"该加分区了"。改 507 Insufficient Storage,并在 client 的错误分类里
单列 `store_full`。

## Open Questions

无阻塞项。已知待办(记在 `docs/design/router.md §7` 与 tasks.md):
leader 重定向地址(`leader_addr` 目前回填 raft 地址,不能直接当 gRPC 端用)、号段后台预取、读路由缓存 TTL、
in-log 预留槽零拷贝写。
