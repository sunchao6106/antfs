# ambry-rs 设计文档

Rust 重写 Ambry 存储引擎,大原则:**整体照搬 Ambry 架构,局部替换为业界最新最佳实现**。

## 0. 战略方向(重要)

目标:一套**集群承载千亿(10¹¹)小文件**,每节点 36×16T(裸盘 576TB),访问模式「新数据热、老数据偶尔归档读」,高效首指性能(吞吐/延迟)同时要扩展性/可用性。

**曾反转一次**:初版采用 Seastar 式路线甲(`monoio` thread-per-core + io_uring + O_DIRECT + 自管块缓存),经评估放弃,退回更贴近 Ambry 原版的**路线乙**——**同步阻塞 IO + buffered(page cache)+ 共享锁并发 + mmap 索引**。完整决策见 `openspec/changes/io-index-design/`。核心结论:
- ambry-store 的 log 本就是**同步阻塞** `FileChannel`(非异步),路线乙是对齐原版而非退化。
- 「伪异步/盘 hang 泄漏线程」是**可用性/容错**层面问题,由集群层的磁盘健康检测 + 多副本兜底,不在引擎层硬换异步。
- 每 Log **定容上界 + 写满自动滚动新 partition** 是设计锚点(本轮暂缓实现)。

## 1. 存储层级(与 Ambry 一致)

```
Partition(逻辑分区,句柄里编码,永不变)
  → Replica(某机某盘上的一份副本)
    → Store(一个 replica 的本地引擎)
      → Log → LogSegment(数据)   +   PersistentIndex(索引)
        → Disk / DataNode(物理)
```

## 2. 局部优化替换点(相对 Ambry)

| 组件 | Ambry 原做法 | 本项目替换 | 理由 |
|---|---|---|---|
| 磁盘 IO | 阻塞 `FileChannel` 读写在池线程上(同步) | **同步阻塞 `std::fs::File`(`read_at/write_at`)** | 同语义、无 JVM 开销;盘 hang 容错归集群层 |
| 读缓存 | OS page cache | **OS page cache**(buffered) | 同 Ambry,热档靠内核缓存,不自管块缓存 |
| sealed 段存在性过滤器 | 增量 Bloom(~1.25 B/对象) | **Binary Fuse Filter**(`xorf`,~1.08 B/对象,查询更快,且**持久化落盘**) | 封存段不变,天然匹配一次性构建 + 落盘复用 |
| sealed 段载入 | mmap(`MappedByteBuffer`) | **mmap(`memmap2`)** | 同 Ambry;索引不整段载入 RAM |
| 活跃段结构 | 有序并发结构(`ConcurrentSkipListMap`) | **并发 SkipMap(`crossbeam-skiplist`)** | 并发读/写,对齐 Ambry |
| 校验 | CRC32 | crc32fast(硬件加速) | |
| 正确性保证 | 单测为主 | **参考模型 + proptest 属性测试 + 崩溃注入**(学 ShardStore) | 把存储正确性做成回归 |

## 3. 数据路径(同步阻塞 IO + buffered)

- 写:`File::write_at`(进 page cache),**周期性/按阈值 `sync_data`** 落盘;不每写必 fsync。
- **持久化边界 = flush**:`flush()` 更新 header 的 `data_end`,`data_end` **只在 fsync 点更新**——断电时磁盘 header 恒为「上一次 flush 的状态」,恢复严格截断到该边界(见崩溃测试)。
- 读:阻塞 buffered 读,依赖 **OS page cache** 缓存热档;不做自管块缓存。
- 无 O_DIRECT / io_uring / AlignedBuf;记录按字节粒度顺序打包。
- 预分配:`posix_fallocate` 真实占盘,对齐 Ambry。

## 4. LogSegment 磁盘布局

```
[ Header ] [ 数据区(变长记录顺序打包) ]
```
Header(小端 + crc32 覆盖其前):`magic("AMBS") | version | capacity | data_end | created_ns | crc`
- 追加走 buffered `write_at`,推进内存 `data_end`;对象 offset 0 映射到物理 `HEADER_SIZE`。
- `flush()` = 写 header(data_end)+ `sync_all`;恢复时按 header 的 data_end 还原。
- 未 flush 数据恢复时不可见(header 未更新)——刻意的持久化契约。

## 5. Log(多段)

- 总容量须为段容量整数倍;单条记录不超过一个段的数据容量(与 Ambry 一致)。
- 活跃段放不下 → 先 flush 旧段 → 滚动到新段;段文件名 `{position}_log.seg`。
- `LogOffset { segment, offset }` 为跨段全局地址(PersistentIndex 引用它)。
- `open()` 扫描目录、按位置排序恢复所有段。

## 5.5 PersistentIndex(索引)

key(`StoreKey`,32B)→ `IndexValue`(offset/size/flags/expiration/op_time/life_version,定长 43B)。

- **活跃段**:并发 `SkipMap`,写入即插入;无过滤器。
- **封存段**:活跃段达阈值 → 写 `.idx`(排序条目)+ `.fuse`(**Binary Fuse 过滤器持久化**:
  descriptor+fingerprints,`BinaryFuse8Ref` 零拷贝引用)→ **mmap(`memmap2`)载入**。
- **查找 newest-wins**:活跃段 → 封存段按 seq 从新到旧;待定容上界补齐后端遍数有界。
- **删除**:往活跃段写一条带 `FLAG_DELETED` 的新条目,不改旧段(契合不变性)。
- **IO**:同步 `std::fs`;**写走 tmp → fsync → rename → fsync 目录**,先 fsync 再 rename;header/body 各带 crc32。
- 已知简化(见路线图):单段内一 key 只留最新值(跨段历史保留,足够 PUT/DELETE;undelete 后续做)。**定容上界、compaction 本轮未做**。

## 6. 持久化 / 崩溃语义

- 单机崩溃(掉电/panic):最近一次 flush 之后、未 fsync 的数据可能丢——由**多副本**兜底(未来在复制层)。
- 本层保证:**flush 之前的数据崩溃后可完整恢复**(崩溃一致性属性测试)。因 buffered,测试直接断言恢复出的 data_end == 最后 flush 边界。

## 7. 测试策略(学 ShardStore)

- 单元测试:round-trip、段满、重开恢复、rollover、log 满、索引增删查/重开。
- **崩溃一致性属性测试**:随机 Append/Flush → 关闭 → 重开 → 断言「flush 是持久化边界」。
- 后续:为 Log/PersistentIndex 建内存参考模型做全量对拍。

## 8. 开发环境

macOS 宿主 + colima Linux VM(Ubuntu 24.04)。源码在宿主(编辑器可见),`./vm-cargo.sh <cmd>`
转进 VM 编译/测试;数据与 cargo target 落 VM 原生 ext4。RustRover 配置见 `docs/RUSTROVER.md`。

## 9. 路线图

- [x] LogSegment(同步阻塞 IO + buffered + fsync 持久边界)
- [x] Log(多段 + rollover + 恢复)
- [x] 崩溃一致性属性测试
- [x] PersistentIndex(并发活跃段 + mmap 封存段 + 持久化 Fuse filter + add/find/recover)
- [x] BlobStore(串起 Log + Index;自描述记录 + put/get/delete/flush/recover)
- [x] 从 log 扫描重建索引(`open_rebuild_index`:索引丢失/损坏时的兜底)
- [ ] （已废弃）monoio / io_uring / O_DIRECT / 自管块缓存——路线甲,已撤销
- [x] compaction/GC(CompactAll 首版:全库重写留存活赢家 + 重建索引 + 标记式崩溃修复)
- [ ] 增量 compaction(按段死数据占比选区间,对齐 `StatsBasedCompactionPolicy`)
- [ ] 每 Log 定容上界(容量/文件数,写满自动滚动新 partition)
- [ ] 记录落 expiration/op_time(`从 log 重建`会丢 TTL 的缺口)
- [ ] 索引持久化水位回放(对齐 Ambry `recover()`,替代每次全量/损坏重建)
- [ ] 参考模型全量对拍