## Context

blobId 计划为 **64bit long**:高位 `partitionId`(自带定位,D3)、低位 `objectId`(partition 内唯一)。objectId 怎么生成有两条路:

- **client 本地随机**(Ambry 式):免发号 RPC、天然支持 W=2 并行写、不可枚举;代价是放弃"连续可数"。
- **中心单调发号**(SeaweedFS/SDFS 式):id 连续、可编码顺序;代价是发号方进写路径 + 可枚举。

本 change 选**中心单调发号**,发号主体为 **master 按 partition**。为避免 master 进"每对象"写路径,采用**号段(block)+ 两层批处理**;为守持久性红线,水位走 **Raft 多数派 fsync**。

### 已核实的参考:SDFS fsi ↔ mds 发号(本地源码)

核实 `~/code/opensource/sdfs`:

- **DocID = 64bit**:`bucket = docId >> 40`(高 24bit 桶 + 低 40bit 桶内序列);桶经 MySQL `BucketInfo(BID,DPID,...)` 映射到放置组。源码 `hmail-sdfs-fsi/.../id/IDRangeAllocator.java:210`。
- **MDS 发号 = MySQL 号段**:事务内 `SELECT maxID FROM BucketInfo WHERE bid=? FOR UPDATE`(行锁)→ `region=[maxID, maxID+idNum)` → `UPDATE ... SET MaxID=maxID+idNum` → commit。源码 `sdfs-mds/.../dao/dbimpl/IdDaoDbImpl.java:40` + `SqlStatement.java:122`。
- **client 号段缓存 + 后台预取**:`IDRangeAllocator.getId()` 本地 `offset++`;`IDPoolWatcher`(后台线程)在缓存低于阈值时异步补段(双 buffer);`NOREQUEST_MAX_INTERVAL=15min`;发号在多桶轮转 + `allocId(storedIds)` 避开已存副本 SN。源码 `id/IDAllocator.java`、`id/IDPoolWatcher.java`。
- **崩溃语义**:`MaxID` 为持久水位,MDS 只发已提交块;client 崩溃丢的号成空洞(无害)。

**借鉴**:号段机制、client 缓存 + 预取、anti-affinity、可选 MDS 选桶。**不采纳**:SDFS 把 md5/size/expiry 等**每对象元数据**存 MDS/MySQL(`DocID` bean + `BucketInfo`)——中心化每对象元数据,与 D3 相反。**升级点**:SDFS 水位在单点 MySQL(SPOF),我们放 Raft(HA)。

## Goals / Non-Goals

**Goals:**
- 固化 objectId 由 master 按 partition 号段发号的机制与正确性骨架(两层批处理、水位持久、容忍空洞、永不复用)。
- 明确 Raft 水位持久性 ≥ 单 DB 的依据与落点(适配器 fsync + 崩溃一致性测试)。
- 记录 SDFS 借鉴边界与被否决/待定项。

**Non-Goals:**
- 不写实现代码。
- 不定死位宽/号段大小(留实现标定,见待定项)。
- 不做 account/container ACL(推网关层);不解决可枚举(记为已知取舍)。

## Decisions

### D1:objectId 由 master 按 partition 单调发号
- 选中心单调而非 client 随机,对齐 [cluster.md 线 271](../../../docs/design/cluster.md);发号主体 master、粒度 per-partition。
- 解决 [§7.6 Q7](../../../docs/design/cluster.md):发号进写路径,但经号段批处理后**不进"每对象"路径**。

### D2:号段 + 两层批处理(防瓶颈)
- **层 1 master→client**:一次发号段 `[start, start+N)`,client 缓存本地消费,热路径 `offset++` 零 RPC。
- **层 2 master→Raft**:master 内存 `next[P]`,仅在将撞 `reserved_upto[P]` 时**一次 Raft commit** 抬水位 +B;Raft 提交次数 = 写量/B,非写量。
- client 缓存将尽时**后台异步预取**下一段(借 SDFS `IDPoolWatcher` 双 buffer),发号正常不阻塞。

### D3:水位持久化 + 容忍空洞 + 永不复用
- master **只发已提交水位以内**的号(先 commit 水位,再发号)。
- 崩溃/切主:新主 `next[P] = 已提交 reserved_upto[P]`,从水位往上续 → 发出的号恒在旧号之上,**绝不重复**。
- 水位与内存 `next` 间预留未用的号 = **空洞**;client 崩溃缓存丢失亦成空洞。**空洞无害**(递增容忍跳号);**任何逻辑不得假设"发了号=对象存在"**。

### D4:Raft 水位持久性 ≥ 单 DB(承 §3.1 红线)
- 一条已提交 Raft 水位 = **已 fsync 到多数派**,耐久性等价 DB 已提交事务,且扛 f/(2f+1) 整机+整盘丢失 + 自动切主(单 MySQL 是 SPOF)。
- **保证落点**:openraft 委托落盘给 `RaftLogStorage`,**必须"先 fsync 再 ack commit"**;用 [Q3 redb](../../../docs/design/cluster.md)(崩溃安全)+ **崩溃一致性属性测试**(断言 flush/fsync 是持久边界,[CLAUDE.md §6](../../../CLAUDE.md))守住。
- 代价:抬水位 = 多数派 RTT + fsync(比本地 DB fsync 略高),但仅补段时发生、经号段 + 缓存摊薄至每对象趋零,换来无 SPOF + 自动切主,净赚。

### D5(不采纳):SDFS 中心化每对象元数据
- SDFS MDS/MySQL 记每对象 md5/size/expiry(做 md5 去重)。**否决**:与 [D3 blobId 自带定位、无中心每对象元数据](../../../docs/design/cluster.md) 冲突;我们只借发号机制,不引入元数据服务器模型。

### D6(已知取舍):递增 objectId 可枚举
- `partitionId, 1..N` 可猜;MVP 无 ACL + 数据口直连 → 单 partition 有被枚举拖库风险。选中心单调即接受此取舍,靠**网络隔离 / 数据口鉴权 / 上层网关 ACL** 兜底,不靠 blobId 不可猜性(区别于 Ambry 随机 id / SeaweedFS cookie)。

### D8:blobId = 64bit,`32bit partitionId / 32bit objectId`
- **决定**:blobId 定长 **64bit(8B)**,高 32bit `partitionId` + 低 32bit `objectId`,无 version(见 D7)。
- **依据**:平均文件 16–64KB、目标 10¹¹:objectId 32bit(42.9亿)覆盖到整盘 16TB partition @16KB 仍有 ~4× 余量;partitionId 32bit 远超全生命周期累计 partition 数(~2^21–2^23 估算)。两半皆富余,对定容容量 C 的取值不敏感。
- **连带**:blobId 从原 §8 的 **32B StoreKey 缩到 8B** → `store.md` 的 `StoreKey`/`IndexValue`/Binary Fuse 尺寸同步缩小(索引每条省 24B,10¹¹ 省 ~2.4TB,净赚);需同步更新 cluster.md §8 + store.md §5。

### D7(显式偏离 §3.4/§8):blobId 不含 version 字段
- **决定**:64bit 全部用于 `partitionId + objectId`,**不留 version 位**。
- **理由**:对齐 SDFS DocID(`24/40` 全吃满)与 SeaweedFS fid(均无 version);把全部 64bit 让给定位 + 序列空间。
- **后果(接受)**:blobId 布局**永久冻结,不可前后兼容演进**——将来无法在 blobId 内加 `dc`(多 DC)、flag、或重分位宽;此类需求只能在 blobId 之外(如网关层/独立元数据)解决。
- **偏离登记**:这与 [CLAUDE.md §3.4](../../../CLAUDE.md)"持久格式必须带版本字段"及 [cluster.md §8 线 332](../../../docs/design/cluster.md) 相悖;按 CLAUDE.md 纪律,更新 cluster.md §8 时须**同步删除"blobId 带 version"的表述并引用本 D7 作为理由**。

### D9:号段大小 N + Raft 批次 = N × 倍数(倍数可配,默认 2)
- **N**(client 一次向 master 申请的 objectId 号段大小):client 本地缓存 N 个,消费完再取;N 大 → 摊薄 client↔master RPC,但 client 崩溃空洞大。
- **B**(master 一次向 Raft 预留的水位块)= **N × `raft_batch_multiple`**,`raft_batch_multiple` **默认 2、可配**;倍数大 → 摊薄 Raft 提交,但 master 切主空洞大。默认 2 = 每 2 个 client 号段触发一次 Raft 提交,失主空洞 ≤ 2N。
- 配置项(实现时入 cluster.md master 配置清单,承 [CLAUDE.md §7](../../../CLAUDE.md)):

  | key(TOML) | 类型 | 默认 | 范围 | 含义 |
  |---|---|---|---|---|
  | `master.id_alloc.segment_size` | u32 | 1024 | ≥1 | N:client 单次申请号段大小 |
  | `master.id_alloc.raft_batch_multiple` | u32 | 2 | ≥1 | B = N × 该倍数;master 向 Raft 预留水位的批次倍数 |

- 二者皆"越大越省事、崩溃/切主空洞越大";因空洞无害(D3),可按压测放大。

### D10:partition 由 master 选(按剩余容量加权),client 缓存多桶号段轮转
- **决定**:写入时 partition 由 **master 选**(不 client 自选);client 请求号段时不指定 partition,master 挑一个可写 partition 返回 `(partitionId, 号段)`。
- **precedent 全票**:SeaweedFS `topology.PickForWrite` → `pickWeightedByRemaining`(可写卷按剩余空间加权随机),SDFS `allocDocID(bucket=-1)` 由 MDS 选桶——两家均 master/mds 选。
- **依据**:master 号段发号已定,client 本就要为号段打 master,master 选 partition **几乎零额外成本**;换来全局容量感知均衡 + 权威新鲜(不撞陈旧的满/RO partition)+ 薄 client。
- **落地形态**:master 按剩余容量加权挑 partition 给号段;client 缓存**多个** master 分配的 partition 号段,写时在它们间轮转分散(+ 可选 anti-affinity,SDFS 式)。client 仍缓存拓扑用于**读**(partitionId→副本位置)。
- **统一 cluster.md 矛盾**:line 22"client 本地挑可写分区"→ 收敛到 line 271"master 发 id + 位置";写路径 client 不再自选 P。

### D11(DR 兜底,承 §3.1 红线):master 全丢时扫副本索引回填水位
- **场景**:仅在 **master 全丢**(整个 Raft 多数派持久状态一次性丢失:相关性多数盘损毁 / 全体存储丢失 / 误删 / 灾备"数据节点在但 master 元数据没了")时触发;正常 ≤f 节点故障由 Raft 透明恢复,不属此列。
- **不兜底的后果**:新 master 从水位 0 重发 objectId → 与已存 blobId 撞号 → 数据损坏(踩持久性红线)。
- **兜底**:master 无状态冷启时,**扫各副本索引拿每 partition 已用最大 objectId,水位设 max+1**(对齐 SeaweedFS `SetMax` / SDFS 同款)。
- **正确性**:数据里的 max-已写 可能低于崩前 max-已预留(空洞),设 max-已写+1 仍安全——只会重发"预留过但从未写成对象"的空号(无对象=不撞),对已写对象绝不复用。
- **非热路径**:仅灾备冷启跑,需 store server 暴露"per-partition 最大 objectId"(可由索引最大 key 的低 32bit 导出)。

## Open Questions

无——发号方案关键项(D1–D11)已全部拍定;剩号段大小 N/B 的具体数值(D9 已给默认 1024/×2)待压测微调,不阻塞设计。
