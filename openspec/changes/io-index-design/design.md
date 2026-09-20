## Context

目标是**一套集群承载千亿(10¹¹)小文件**,每节点为 **36×16T(裸盘 576TB)** 的存储服务器,访问模式「新数据热、老数据偶尔归档读」。**高效**首要指性能(吞吐/延迟),同时要扩展性/可用性(可分片、可扩容),要求均衡。

**规模锚点(本设计的铁律):每份 Log 必有容量/文件数上限,写满自动滚动到新 partition。** 这个「定容」约束是整个设计的支点——它保证每个 store(一份 Log+Index)的段数、索引规模、过滤器大小**都有常量上界**,而非随对象总量 N 线性膨胀。千亿的总量靠「海量定容 partition 铺开 + 集群分片到多机」承载,单 store 始终受控、索引可常驻内存。具体上限值本 change 不定(留作按负载标定的设计参数),但「必然有界」是硬约束。

> **实现范围注记(当前阶段)**:每 Log 定容上界**暂不实现**——先落地同步 IO 地基与索引同步化/并发化/mmap/Fuse 持久化,**不强制上界**。定容作为后续阶段补入。

本 change 记录一次**地基级战略反转**:初版方向是 Seastar 式路线甲(`monoio` thread-per-core + io_uring + O_DIRECT + 自管块缓存 + 每核分片),经深入评估后**放弃,改为更贴近 Ambry 原版的路线乙**(阻塞 IO + buffered page cache + 并发索引 + mmap 索引段 + 定容 partition)。

**为什么反转**:参照源码核实过的 Ambry 存储层(`ambry-store`)
- IO:**同步阻塞** `FileChannel` 读写在池线程上(`LogSegment:258` `fileChannel.write`,`:404` `fileChannel.read`),buffered 靠 OS page cache,`FileChannel.force(true)`(`:237`)做 fsync;没有自管用户态块缓存,也没有任何异步/AIO——线程池只是「并发承载多个阻塞 IO」,单次 IO 本体是同步阻塞。
- 索引:活跃段 `ConcurrentSkipListMap<Offset, IndexSegment>`(`PersistentIndex`),内部 key→多值并发结构;封存段 `IndexSegment.map()`(mmap)+ 每段 Bloom + mmap 上二分(`IndexSegment` 类注释、`sealedIndex.find`)。
- 并发:共享内存 + `ReentrantReadWriteLock`(读写锁),compaction 与读路径用锁协调,非 thread-per-core。
- 索引恢复:启动常设 `recover()` 扫 log 校正/重建;compaction: `BlobStoreCompactor` 把存活数据拷进 swap space 后原子替换段。
- **定容**:partition 容量固定,写满即封存并滚动新 partition(Ambry 生产验证),用「海量定容 partition」而非 LSM 压住单个索引。

**决定**:路线甲的 Seastar 式自管缓存/O_DIRECT 收益需整套工程才能兑现、负载体量大,故移除 monoio、退回更贴近 Ambry 的同步阻塞模型。关于「伪异步 / 盘 hang 泄漏线程」的旧评语需澄清:**ambry-store 的 log 本就是同步阻塞,这与本方向一致,不算退化;**那句话批评的其实是「用大线程池硬扛阻塞 IO,单盘 hang 时池线程被永久占住」的**可用性/容错**风险,应在**集群层用磁盘健康检测(`DiskHealthStatus`)+ 多副本**兜底,而非在引擎层硬换 io_uring 异步。局部保留 Binary Fuse 过滤器(较 Bloom 更小更快,并持久化落盘避免启动重建)。

## Goals / Non-Goals

**Goals:**
- 从当前基于 monoio 的实现,迁移到**阻塞 IO + buffered + 共享锁并发 + 定容分区索引**的 Ambry 式架构。
- 由「**定容 partition**」保证单 store 索引有界、可常驻内存;千亿由海量定容分区 + 集群分片承载。
- compaction/GC 定时运行、按死数据阈值触发,log 与 index 共同回收空间(对齐 `BlobStoreCompactor`)。
- 保持正确性契约:数据 fsync 持久化边界、崩溃恢复、从 log 重建索引(recover)。

**Non-Goals:**
- 不实现 Seastar 式自管用户态块缓存、O_DIRECT、io_uring、(每核 share-nothing)分片——路线甲明确否决。
- 不做 LSM 分层 / 稀疏索引(定容分区方案已能压住索引,不引入)。
- 本 change 不写实现代码,只固化设计决策与方向。
- 不做分布式/replication 层设计(集群分片、复制协议不在本 change)。

## Decisions

### D1:运行时——移除 monoio,采用同步阻塞 IO 模型
- 采用 `std::fs::File`(`read_at`/`write_at`)+ 显式线程池/后台线程执行 IO 及 fsync,统一为**同步阻塞语义**,对齐 ambry-store(其 log 读写即同步阻塞 `FileChannel`)。
- **盘 hang 容错不在引擎层**:阻塞 IO 在单盘 hang 时会占住池线程——这是**可用性/容错**问题,由集群层的磁盘健康检测(`DiskHealthStatus`,超时/隔离)与多副本兜底,不靠把单次 IO 改异步解决(对本质阻塞的磁盘设备,异步不能凭空提速,只会少占线程)。
- 可选:`tokio` + `spawn_blocking` 承载阻塞 IO(需异步便利时的选项),但引擎核心为同步阻塞语义。
- **否决**:monoio(thread-per-core 无阻塞池,阻塞 IO 卡整核)、io_uring/O_DIRECT(需 Seastar 式整套缓存才经济)、以「伪异步」为由强换异步 IO(ambry-store 本就是同步阻塞,此理由不成立)。

### D2:数据路径——buffered 写 + 周期性 fsync,OS page cache 抗读
- 写:阻塞 `File::write_at`(进 page cache),**周期性/按阈值 `sync_data`** 落盘;不每写必 fsync(对齐 Ambry 定时 persist + `force`)。
- **不变式**:header 的 `data_end` **只在 fsync 点更新**。保证 buffered 下断电时磁盘 header 恒为「上一次 fsync 的状态」,恢复严格截断到最后 fsync 边界,避免 torn-state。
- 读:阻塞 buffered 读,依赖 **OS page cache** 缓存热档(定容 partition 让热档工作集可控、可常驻);不做自管块缓存、不做按段年龄准入。
- 文件布局:保留预分配(`posix_fallocate`)、段式只追加;**4KiB 对齐/O_DIRECT 不再要求**,记录按字节粒度打包(对齐读降级为可选性能优化)。
- **否决**:自管有界块缓存、group-commit、O_DIRECT 直落盘。

### D3:索引——并发活跃段 + mmap 封存段 + Fuse + 二分
- **前提(墙1/墙3 的闭合依赖定容)**:定容上界暂缓时,墙1(平铺扫描 O(段数))与墙3(过滤器匿名内存)暂不强制闭合;当每 Log 段数仍大时,`find` 扫描与过滤器内存随段数增——**留待定容阶段一并解决**。当前先做索引的同步化/并发化/mmap/Fuse 持久化,不断言「常量上界」。
- **活跃段**:并发有序映射(优先 `crossbeam-skipmap`,粒度对齐 Ambry `ConcurrentSkipListMap`),支持并发读/写。
- **过滤器**:每段 **Binary Fuse 过滤器**(`xorf`,较 Ambry Bloom 更小更快,且 O(1) 一次性构建适合封存段);**持久化到段文件,加载时反序列化**,避免「启动读全量重建」(Ambry 把 bloom 存段文件同理)。常驻内存。
- **封存段**:常驻内存(现实现)或超预算时 mmap(`memmap2`) + 页缓存驻留;**本阶段实现 mmap + 二分**,并**持久化 Fuse 过滤器**(`xorf` 序列化,避免启动重建);命中对内存/mmap 区间二分。
- **否决**:全段 `std::fs::read` 载入 RAM(缓冲语义上没必要,mmap/常驻二选一)、io_uring 块按需读(路线甲)、稀疏索引/LSM(留待定容阶段再评估)。

### D4:并发/共享模型——共享内存 + 锁
- 从 `&mut self` 单写者、thread-per-core 改为**共享实例 + `RwLock` / 并发结构**,对齐 Ambry。
- **锁作用域必须覆盖完整读(索引查找 + log 读)**:读者持读锁直到取完数据;compaction 持写锁。防止「索引先解出 offset、释放锁后段被 compaction 删除」→ 读失败(UAF/悬空)。
- **索引段引用计数**作为读写锁补充:mmap/常驻索引段被并发读时不可 unmap,直到引用归零。
- **否决**:每核 share-nothing 分片(与路线甲同弃)。

### D5:compaction / GC——定时 + 阈值触发,log+index 一起
- 周期性后台任务,当**死数据(已删除/过期/被新版本取代)超过段级阈值**才执行(避免空转);活跃段不压缩。
- 算法对齐 Ambry `BlobStoreCompactor`,必须覆盖以下硬问题(逐一写死):
  1. **索引 offset 重写**:拷存活记录到新 log 段后,LogOffset 变化,须重写索引条目指向新 offset;建立 旧offset→新offset 映射并原子更新索引。
  2. **组件顺序 + fsync**:`拷存活→fsync 新 log 段→写新索引段→fsync 新索引段(先 fsync 再 rename)→原子换入(rename/swap)→删旧段`。**任何引用新 log 段的索引换入前,新 log 段必须已 fsync**,否则断电后索引指向未持久数据。
  3. **原子替换**:用 tmp+rename(替换索引段)/换名(日志段);换入用写锁,读用读锁,见 D4。
  4. **崩溃恢复协议**:compaction 中途崩溃是常态。新段以**临时前缀**命名;启动扫描:未换入的临时新段清理;已换入的,旧段成孤儿清理。用持久化的「compaction 进度/标记」保证重入安全。
  5. **log/index 协同**:log 段按容量(定容)滚动,index 段按条目数封存——两者**非 1:1**。compaction 以 offset 区间为单位协同:选定一段 log + 落在其上的 index 条目,存活的重写到新区间,连带重建对应 index 段。
- 是「回收空间」必要机制(删除/覆盖下无 GC 会耗尽磁盘与段数;定容 partition 全靠 GC 把写满的 partition 释放回可用)。

#### 参考 Ambry 的具体机制(本地源码 `ambry-store`,实现时按此对齐)

**触发(两级)**
```
后台线程 CompactionManager.CompactionExecutor 每周期轮询所有 store:
  details = compactionPolicy.getCompactionDetails(...)   ← 决策
  if (details != null) store.compact(details, buf)        ← 才执行
触发器配置 storeCompactionTriggers ∈ {PERIODIC, ADMIN}:
  PERIODIC 周期重排队;ADMIN 手工唤醒
决策策略 StatsBasedCompactionPolicy(Hybrid 内部切换 CompactAllPolicy):
  blobStoreStats 算出每段存活数据量;从两端收拢,取「首尾段存活% ≤ 阈值(默认 20%)」的
  连续区间做 candidates → 无合格区间则返回 null(本轮不压)
```
- rs 对应:周期定时线程 + 每 store 用「死数据占比 ≥ 阈值(如 ≥80% 死=≤20% 存活)」判定是否进入 compaction;把「死数据统计」接入现有索引/日志统计(或入门版用段内已删条目占比近似)。

**执行(四阶段循环,直至 `CompactionLog.Phase.DONE`)**
```
while (phase != DONE) switch (phase):
  PREPARE : 落 CompactionLog,标记开始(崩溃可从此续跑)
  COPY    : 用 IndexSegmentValidEntryFilter 过滤出存活条目
            (去 deleted/expired/被新版本取代的重复 key)
            → 拷进 temp log 段(swap space,名带 TEMP 后缀)
  COMMIT  : renameLogSegments(temp→正式)
            → addNewLogSegmentsToSrcLog(新段加进源 log 映射)
            → updateSrcIndex(PersistentIndex.changeIndexSegments(),持 write lock 原子换索引段集合)
  CLEANUP : waitTillRefCountZero() 等读旧段的 in-flight 请求释放
            → 删旧 log 段 + 旧索引
崩溃恢复:  CompactionLog 持久化当前 phase;启动 maybeResumeCompaction 从 phase 续跑,
          不会留下半新半旧状态。
```
- rs 对应:持久化 compaction phase 进度;COPY 用索引过滤存活条目写 temp 段;COMMIT 按「fsync 新 log 段 → 写索引 → fsync → rename 换入 → 持写锁切索引段集合」原子完成;CLEANUP 等读旧段 refcount 归零再删(见 D4 锁作用域)。

### D6:索引持久化 / 恢复
- **加载已落盘索引 + 水位回放**(对齐 Ambry 真实模型,非每次全量重建):
  - 后台 periodic persist 活跃索引段(先 fsync tmp **再 rename**,补现有 std::fs::write+rename 缺 fsync 的缺陷)。
  - 记录**索引持久化水位**(已落盘的 log 区间);启动**加载索引 + 从水位回放 log 尾部**,只补增量。
- 仅当索引损坏/缺失时**全量 `open_rebuild_index`**(扫 log 重建)。索引比数据宽松(可重建),数据 fsync 是持久化边界。
- **TTL 契约缺口**:当前 log 记录不带 expiration/op_time,rebuild 会丢 TTL;本 change 标记:后续实现须**把 expiration 落进 log 记录**,否则「从 log 重建」不完整。

### D7:文件组织与命名(对齐 Ambry)
- 每对象属于一个 partition(BlobId 编码 partition id);节点按 replica/store 承载,每 store 一份 Log + Index(定容,写满滚新 partition)。
- 段文件带文件名序列/起始 offset 编码,目录扫描按序恢复(对齐 Ambry 文件名即 offset)。

## Risks / Trade-offs

- **[mmap 缺页在阻塞模型下不再致命 → ]但仍有资源风险** → 阻塞线程池里缺页只阻塞该单线程,不冻结全局;需足够线程池深度。定容下常驻为主,mmap 仅超预算路径。
- **[page cache 冷扫描污染热档]** → 定容 + 分区把每 store 工作集压小;OS 2-list 抗扫描;冷归档扫罕见,不致命。
- **[写缓冲+周期性 fsync,断电丢尾部]** → 刻意契约:fsync 前非持久,由多副本兜底;本层保证 fsync 边界。
- **[并发锁竞争 / compaction 持写锁过久]** → 读写锁 + 并发跳表;compaction 按阈值+定时、分小批持写锁;必要时粒度化到 offset 区间。
- **[compaction 中途崩溃残留]** → 临时前缀 + 进度标记 + 启动清理,见 D5.4。
- **[迁移破坏性大]** → 分期:先 log/IO 同步化,再索引定容/并发化,再 compaction。数据格式尽量复用;`aligned.rs` 与 O_DIRECT 机制删除。

## O_DIRECT 遗留项删留清单

| 现有(crates/store) | 处理 |
|---|---|
| `aligned.rs`(`AlignedBuf`,monoio `IoBuf/IoBufMut`) | **整删**——monoio 一并移除,无需对齐分配 |
| `LogSegment` tail 暂存 / `flushed_blocks` / 补零整块写 | **删**——O_DIRECT 对齐专用;改 buffered 直接 write_at + 内存 data_end |
| `log_segment.rs` `open_direct`(O_DIRECT flag) | **删** |
| `read` 的 `align_down/align_up` 对齐读 | **删**——buffered 按字节粒度读 |
| `monoio::fs::File` | **删**——换 `std::fs::File` + `read_at/write_at` |
| header block + `data_end` 布局 | **留**——作为恢复/持久化边界载体(data_end 仅 fsync 点更新) |
| 段式只追加、预分配 `posix_fallocate` | **留**——利于顺序 IO 与稳定文件大小 |
| 崩溃一致性测试 | **改**——buffered 下「close 不 flush」不再模拟崩溃(page cache 跨进程存活);改用真 drop page cache / 断电模拟 |

## Open Questions

- 阻塞 IO 承载:`tokio+spawn_blocking` vs 纯手写线程池?倾向 tokio,待实现敲定。
- 并发活跃段:`crossbeam-skipmap` vs `std::collections`+`RwLock`?按读写比与争用实测选。
- **每 Log 定容上限值**:容量字节 / 文件数,先到者触发?具体值待负载 profile 标定(本 change 只定「必有界」)。
- compaction 死数据阈值与周期初值:实测标定;封存的 partition 何时释放。
- index 段按条目数封存 vs 按 offset 区间:与 compaction 单位协同选定。