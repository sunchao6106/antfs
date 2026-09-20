# ambry-rs 设计文档

Rust 重写 Ambry 存储引擎,大原则:**整体照搬 Ambry 架构,局部替换为业界最新最佳实现**。

## 1. 存储层级(与 Ambry 一致)

```
Partition(逻辑分区,句柄里编码,永不变)
  → Replica(某机某盘上的一份副本)
    → Store(一个 replica 的本地引擎)
      → Log → LogSegment(数据)   +   PersistentIndex(索引)
        → Disk / DataNode(物理)
```

本仓库 crate `ambry-store` 目前实现最底层的 **Log / LogSegment**,索引下一步做。

## 2. 局部优化替换点(相对 Ambry)

| 组件 | Ambry 原做法 | 本项目替换 | 理由 |
|---|---|---|---|
| 磁盘 IO | JDK 线程池阻塞 pread/pwrite(伪异步,盘 hang 泄漏线程) | **monoio + io_uring + O_DIRECT** | 真异步、无 GC、绕 page cache;盘 hang 只超时不泄漏应用线程 |
| sealed 段存在性过滤器 | 增量 Bloom(~1.25 B/对象) | **Binary Fuse Filter**(`xorf`,~1.08 B/对象,查询更快) | sealed 段封存即不变,天然匹配"一次性构建" |
| 定长记录编解码 | 手撸 ByteBuffer | `zerocopy`/`bytemuck`(规划) | 定长头零拷贝 |
| 校验 | CRC32 | crc32fast(硬件加速) | |
| 正确性保证 | 单测为主 | **参考模型 + proptest 属性测试 + 崩溃注入**(学 ShardStore) | 把存储正确性做成回归 |

## 3. IO 层:AlignedBuf + O_DIRECT

O_DIRECT 要求缓冲地址/文件偏移/长度都按块对齐。`AlignedBuf`(`aligned.rs`):
- 4096 对齐堆分配(`ALIGN=4096`,覆盖 512/4096 扇区盘),分配即清零(按容量读取无 UB);
- 实现 monoio `IoBuf`/`IoBufMut`(owned-buffer 完成式 IO);
- 结构体移动不移动底层堆内存 → 满足 monoio 对缓冲地址稳定的要求。

## 4. LogSegment 磁盘布局

```
[ 第0块(4096):Header ] [ 数据块0 ] [ 数据块1 ] ... [ 数据块N ]
```
Header(首块内,小端 + crc32 覆盖 [0,32)):
`magic("AMBS") | version | capacity | data_end | created_ns | crc`

写入模型(O_DIRECT 对齐暂存):
- 变长记录先拷进 4096 对齐的 `tail` 暂存块;
- 满一块整块写盘;`flush()` 把不满尾块补零整块写 + `sync_all`(fsync);
- **持久化边界 = flush**(更新 header.data_end + fsync)。

读取:已落盘部分对齐读盘、未落盘尾部读内存,自动拼接;支持局部/跨块读。

恢复:`open()` 校验 header magic/crc → 按 `data_end` 还原 `flushed_blocks`/`tail`。
未 flush 的数据在恢复时不可见(即使物理块已落盘,header 未更新)——这是刻意的持久化契约。

预分配:`posix_fallocate` 真实占盘(非稀疏),对齐 Ambry。

## 5. Log(多段)

- 总容量必须是段容量整数倍;单条记录不能超过一个段的数据容量(与 Ambry 一致)。
- 活跃段放不下 → 先 flush 旧段 → 滚动到新段;段文件名 `{position}_log.seg`。
- `LogOffset { segment, offset }` 是跨段全局地址(将来 PersistentIndex 引用它)。
- 段数达 `total/segment` 上限仍需滚动 → `SegmentFull`。
- `open()` 扫描目录、按位置排序恢复所有段。

## 5.5 PersistentIndex(索引)

key(`StoreKey`,32B 紧凑 BlobId)→ `IndexValue`(offset/size/flags/expiration/op_time/life_version,定长 43B)。

- **活跃段**:内存 `BTreeMap`,写入即插入;**无过滤器**(直接查 map)。
- **封存段**:活跃段达阈值 → 排序落盘(sealed=1)+ 构建 **Binary Fuse 过滤器**(`xorf`)→ 只读。
- **查找 newest-wins**:活跃段 → 封存段按 seq 从新到旧;首个命中返回。
- **删除**:往活跃段写一条带 `FLAG_DELETED` 的新条目,不改旧段(契合不变性)。
- **过滤器不落盘**:加载时由全部 key 一次性重建(散列进程内一致即可)。
- **IO**:索引文件用**同步 std::fs**(小元数据、非热路径),写用 tmp+rename 原子替换;
  header/body 各带 crc32。数据热路径(Log)仍是 monoio+io_uring+O_DIRECT。
- 已知简化(见路线图):sealed 段全量载入内存(未 mmap);单段内一 key 只留最新值
  (跨段历史保留,足够 PUT/DELETE 最新态;完整版本历史/undelete 后续做)。

## 6. 持久化 / 崩溃语义

- 单机崩溃(掉电/panic):最近一次 flush 之后、未 fsync 的数据可能丢——由**多副本**兜底(未来在复制层)。
- 本层保证:**flush 之前的数据崩溃后可完整恢复**(见崩溃一致性属性测试)。

## 7. 测试策略(学 ShardStore)

- 单元测试:round-trip、跨块、段满、重开恢复、rollover、log 满。
- **崩溃一致性属性测试**(`tests/crash_consistency.rs`):随机 Append/Flush 序列 →
  崩溃(不 flush 关闭)+ 重开 → 断言"flush 是持久化边界"。
- 后续:为 Log/PersistentIndex 建**内存参考模型**做全量对拍。

## 8. 开发环境

macOS 宿主 + colima Linux VM(Ubuntu 24.04 / kernel 6.8 / io_uring)。源码在宿主(编辑器可见),
`./vm-cargo.sh <cmd>` 转进 VM 编译/测试;数据与 cargo target 落 VM 原生 ext4。

## 9. 路线图

- [x] AlignedBuf(O_DIRECT 对齐缓冲)
- [x] LogSegment(monoio + io_uring + O_DIRECT)
- [x] Log(多段 + rollover + 恢复)
- [x] 崩溃一致性属性测试
- [x] PersistentIndex(活跃内存段 + sealed 段 + Binary Fuse filter + IndexValue + add/find/recover)
- [x] BlobStore(串起 Log + Index;自描述记录 + put/get/delete/flush/recover)
- [ ] 索引 mmap 化(当前 sealed 段全量载入内存;大规模需改 mmap 省 RAM)
- [ ] 索引改走 io_uring(当前索引文件用同步 std::fs)
- [ ] 从 log 扫描重建索引(索引文件损坏时的兜底)
- [ ] 参考模型全量对拍
