## Why

写路径的**分区分派**实际退化成了"同一时刻只有一个活跃分区":`Router::allocate_one`
(`crates/common/router/src/lib.rs`)在缓存里 `find` 第一个没取完的号段,取完才向 master 要下一段 ——
而"要下一段"只在**所有**段都空时才发生,于是 `IdCache.segments` 恒为 0 或 1 个元素,一段号被
**连续耗尽在同一个分区**上。

后果:同一 `Router` 实例(一个 frontend 进程只有一个 `Router`,`crates/frontend/src/server.rs:225`;
压测二进制同理 `benchmark/router/src/lib.rs`)的全部并发写落在**同一个分区的 RF 个副本**上,即同一组盘上。
分区之间没有任何写入均衡,只有"这段用完再换下一个"的时间上串行 —— 单进程写在磁盘上是一个热点。

`blobid-master-assign` D10 的**落地形态**本来就写着"client 缓存**多个** master 分配的 partition 号段,
写时在它们间轮转分散(+ 可选 anti-affinity,SDFS 式)",本次把它真正实现。

## What Changes

- **router:发号改成「每个可写分区各持一段 + 轮转」**
  - 补段(`Router::refill`):先 `ListPartitions(state=RW)` 取当前可写分区列表(pid 升序),再对**每个**
    分区**显式点名 `partition_id`** 申请一小批(`id_segment_size` 个)号段;取段并发进行。
  - 取号(`IdCache::take_rr`):在持有的各分区号段之间**顺序轮转**,某段取完即丢,全部取空才补段。
  - 单个分区取号失败(期间转 RO/封存、号耗尽、未知)只**跳过该分区**,不影响其余分区;一个都没取到才报错。
  - 已不在可写列表里的分区,其未用完的号段丢弃(余号成空洞,无害,D3)。
- **router:后台任务保鲜"可写分区列表"**(`Router::connect` 起,周期 `partition_refresh_secs`,**0 = 关闭**)
  - 周期向 master 拉 `ListPartitions(state=RW)` 并调账本地号段池:已不可写的分区**丢掉号段**(余号成空洞,无害),
    新出现的可写分区**补一段**(master 复核后发号)。
  - 与写路径的补段共用同一份 `sync_segments`(两者都在号段锁里跑;插入只发生在"该分区当前没段"时,
    故并发也不会互相覆盖号段);刷新失败只计 `router_partition_refresh_total{outcome="error"}` 并**保留旧缓存** ——
    后台刷新是**增强**,号段耗尽的写路径自己会同步一次,不能成为正确性依赖。
  - 任务持号段池的 `Weak`:最后一个 `Router` 析构后自行退出(不留常驻任务);重建轮转顺序时**保持轮转相位**
    (否则一次空刷新会把游标打回起点、让同一分区连出两个号)。
- **配置**:`router.id_segment_size` 语义由"一次申请的号段大小"改为"**每个可写分区**一次申请的号段大小"
  (默认值 `1024` / 范围 ≥1 均不变);**新增** `router.partition_refresh_secs`(默认 `30`,0 = 关闭后台刷新)。
- **监控**:新增 `router_writable_partitions`(gauge,本地持有号段的分区数)、`router_partition_refresh_total{outcome}`。
- **文档**:`router.md`(§2 写路径 + §5 配置表 + §6 监控表 + 分区选择说明)、`frontend.md`(内嵌配置表)、
  `cluster.md`(proto 片段 + §7.5 写路径时序;顺带修掉"`partition_id=0` 表示自动"的陈旧表述 ——
  该字段早已是 proto3 `optional`)。

## Capabilities

### Modified Capabilities

- `router`(见 `docs/design/router.md`):发号由"单分区号段顺序耗尽"改为"多可写分区号段轮转"。

## Impact

- **crate**:`antfs-router`(lib + 配置文档;新增单测钉住轮转语义,e2e 增加断言)。
- **协议**:不动 proto —— `ListPartitions` 与 `GetBlobIds.partition_id`(proto3 `optional`)均已存在。
- **与 D10 的偏离(显式记录)**:D10 定"partition 由 master 按剩余容量加权挑、client 不自选"。
  本次客户端**枚举全部可写分区**并逐个点名,负载因此由"每段一次容量加权随机"变为"可写分区上的**均匀轮转**"。
  router 仍**不自造分区 id、不缓存容量判断**,只取 master 权威 RW 列表里的分区,且每个分区在发号时被 master
  复核(RW + 至少一个副本,`crates/master/src/server.rs:1763`);但"按剩余容量加权"的全局容量感知确实被弱化了。
  容量反馈环本身是心跳级(`used_bytes` 每 3s 上报、秒级且按字节口径),只够做粗粒度修正。
- **代价(接受)**:一次补段 = 1 次 `ListPartitions` + 「可写分区数」次 `GetBlobIds`,整轮在号段锁里**单飞**;
  分区数很大时会拉长换段停顿 —— 用 `id_segment_size` 调小可保持"总预留不变、换段频率不变、停顿不变"。
- **持久性**:不触及 blob 落盘/索引/水位语义。号仍来自 master、仍在已提交水位内、永不复用;
  丢弃的未用完号段只是**空洞**(D3 无害);"发了号 ≠ 对象存在"的既有前提不变。
