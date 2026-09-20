## Why

工程目标是单机高密度、聚合**千亿(10¹¹)小文件**存储。该目标决定了索引不可能全量常驻 RAM,读写策略必须兼顾「新数据热、老数据偶尔归档读」的时间分层访问。

本 change 记录一次**地基级战略反转**:原设计基于 **monoio(thread-per-core + io_uring + O_DIRECT)**(Seastar 式路线甲),经评估决定**放弃 monoio,退回更贴近 Ambry 原版的路线乙**——用 **阻塞 IO + buffered(OS page cache)+ 并发索引结构 + mmap 索引段**。理由:千亿小文件负载以高 QPS 点查为主,自管块缓存/O_DIRECT 的收益需整套 Seastar 式用户态缓存才能兑现,维护成本高且偏离被生产验证过的 Ambry 模型;贴近 Ambry 的成熟结构(线程池 + 阻塞 IO + page cache + mmap 索引 + compaction)更契合该负载。本 change 只**固化设计决策**,不写实现代码。

## What Changes

- **移除 monoio / io_uring / O_DIRECT** 这一技术栈(原 DESIGN.md 的核心替换点作废)。
- **数据路径**:写 = 阻塞 `File` 写 + buffered(进 OS page cache)+ **周期性/阈值 `sync_data/sync_all`(fsync)**;读 = 阻塞 buffered 读,靠 **OS page cache** 抗热点,不做自管块缓存,不引入 group-commit。文件按需预分配,整段写保留。
- **索引引擎**:活跃段用**并发有序结构**(对齐 Ambry `ConcurrentSkipListMap`),封存段 **mmap + Binary Fuse 过滤器 + 二分查找**(Fuse 保留,bloom 换);索引 IO 走内存映射 + 阻塞读;**compaction/GC** 设为**周期性定时任务,死数据超过固定阈值才执行**,log 与 index 一起回收。
- **线程/并发模型**:从 thread-per-core / share-nothing 改为**共享内存 + 锁**(读写锁 + 并发结构),对齐 Ambry。`&mut self` 单写者改写为并发安全。
- **持久化契约**:数据以 fsync 为持久化边界;索引周期性落盘,损坏时靠扫 log 重建兜底(保留 `open_rebuild_index` 思想,对齐 Ambry `recover()`)。

## Capabilities

### New Capabilities
- `data-path-io`: 数据(blob 本体)读写策略——阻塞 buffered IO 走 page cache、周期性 fsync、文件布局与对齐;被否决的 io_uring/O_DIRECT/自管缓存及理由。
- `index-engine`: key→IndexValue 本地元数据引擎——并发活跃段、mmap 封存段 + Fuse 过滤器 + 二分、compaction/GC(定时 + 阈值触发)、索引持久化/恢复契约。

### Modified Capabilities
<!-- 无既有 spec,全部为新建。 -->

## Impact

- **删除依赖**:`monoio`。同步 IO 改用 `std::fs::File` + `read_at/write_at` + `sync_data/sync_all`。
- **代码重写范围**:`crates/store/src/aligned.rs`(O_DIRECT 对齐缓冲不再必需)、`log.rs`/`log_segment.rs`(monoio→std File + 阻塞 IO + fsync)、`blob.rs`(同步化)、`index/`(并发活跃段 + mmap 封存段 + compaction)。`crates/smoke` 适配新运行时。
- **新依赖**:并发有序映射(`crossbeam-skipmap` 或 std 并发结构)、mmap(`memmap2`);compaction 随引擎实现。
- **文档**:`docs/DESIGN.md`、`docs/DEVLOG.md`、`docs/RUSTROVER.md` 与本 change 对齐。
- **保留契约**:数据 fsync 持久化边界、崩溃恢复、从 log 重建索引。