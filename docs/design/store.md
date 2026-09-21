# store 设计:单机存储引擎

本文档为 **`store`** crate(单机存储引擎,对标 Ambry `ambry-store` 模块)的设计;跨模块战略方向、系统层级、开发环境见 [README.md](./README.md)。

组件:`Log` / `LogSegment`(数据)+ `PersistentIndex`(索引)+ `BlobStore`(串联对象存储)。

## 1. 局部优化替换点(相对 Ambry)

| 组件 | Ambry 原做法 | 本项目替换 | 理由 |
|---|---|---|---|
| 磁盘 IO | 阻塞 `FileChannel` 读写在池线程上(同步) | **同步阻塞 `std::fs::File`(`read_at/write_at`)** | 同语义、无 JVM 开销;盘 hang 容错归集群层 |
| 读缓存 | OS page cache | **OS page cache**(buffered) | 同 Ambry,热档靠内核缓存,不自管块缓存 |
| sealed 段存在性过滤器 | 增量 Bloom(~1.25 B/对象) | **Binary Fuse Filter**(`xorf`,~1.08 B/对象,查询更快,且**持久化落盘**) | 封存段不变,天然匹配一次性构建 + 落盘复用 |
| sealed 段载入 | mmap(`MappedByteBuffer`) | **mmap(`memmap2`)** | 同 Ambry;索引不整段载入 RAM |
| 活跃段结构 | 有序并发结构(`ConcurrentSkipListMap`) | **并发 SkipMap(`crossbeam-skiplist`)** | 并发读/写,对齐 Ambry |
| 校验 | CRC32 | crc32fast(硬件加速) | |
| 正确性保证 | 单测为主 | **参考模型 + 差分对拍 + 崩溃注入**(学 ShardStore) | 把存储正确性做成回归 |

## 2. 数据路径(同步阻塞 IO + buffered)

- 写:`File::write_at`(进 page cache),**周期性/按阈值 `sync_data`** 落盘;不每写必 fsync。
- **持久化边界 = flush**:`flush()` 更新 header 的 `data_end`,`data_end` **只在 fsync 点更新**——断电时磁盘 header 恒为「上一次 flush 的状态」,恢复严格截断到该边界(见崩溃测试)。
- 读:阻塞 buffered 读,依赖 **OS page cache** 缓存热档;不做自管块缓存。
- 无 O_DIRECT / io_uring / AlignedBuf;记录按字节粒度顺序打包。
- 预分配:`posix_fallocate` 真实占盘,对齐 Ambry。

## 3. LogSegment 磁盘布局

```
[ Header ] [ 数据区(变长记录顺序打包) ]
```
Header(小端 + crc32 覆盖其前):`magic("AMBS") | version | capacity | data_end | created_ns | crc`
- 追加走 buffered `write_at`,推进内存 `data_end`;对象 offset 0 映射到物理 `HEADER_SIZE`。
- `flush()` = 写 header(data_end)+ `sync_all`;恢复时按 header 的 data_end 还原。
- 未 flush 数据恢复时不可见(header 未更新)——刻意的持久化契约。
- `overwrite_at`:就地覆盖已写区间(不改 data_end),供 hard_delete 原地擦除。

## 4. Log(多段)

- 总容量须为段容量整数倍;单条记录不超过一个段的数据容量(与 Ambry 一致)。
- 活跃段放不下 → 先 flush 旧段 → 滚动到新段;段文件名 `{position}_log.seg`。
- `LogOffset { segment, offset }` 为跨段全局地址(PersistentIndex 引用它)。
- `open()` 扫描目录、按位置排序恢复所有段。

## 5. PersistentIndex(索引)

key(`StoreKey`,32B)→ `IndexValue`(定长 **59B**:offset(16)/size(8)/flags(1)/expiration(8)/op_time(8)/life_version(2)/original_offset(16))。

- **活跃段**:并发 `SkipMap`,写入即插入;无过滤器。
- **封存段**:活跃段达阈值 → 写 `.idx`(排序条目)+ `.fuse`(**Binary Fuse 过滤器持久化**:
  descriptor+fingerprints,`BinaryFuse8Ref` 零拷贝引用)→ **mmap(`memmap2`)载入**。
- **newest-wins**:同一 key 在 log 里可能有多条记录(PUT/DELETE/UNDELETE/TTL_UPDATE),索引只留**最新一条**作为赢家;查找先活跃段、再封存段按 seq 从新到旧,首个命中返回。重建时按 log 追加顺序(旧→新)插入,后者覆盖前者,天然还原 newest-wins。
- **单值索引 + 数据重写**:每 key 只存一个 `IndexValue`(不同于 Ambry 的每 key 多值)。故 undelete/updateTtl **复制整条数据**写成新赢家(保 offset-相等的 compaction/rebuild/seg_bytes 机制不变);`original_offset` 记录承载数据的位置(墓碑指向被删前 PUT,供 undelete 取原数据)。
- **IO**:同步 `std::fs`;**写走 tmp → fsync → rename → fsync 目录**,先 fsync 再 rename;header/body 各带 crc32。

## 6. 持久化 / 崩溃语义

- 单机崩溃(掉电/panic):最近一次 flush 之后、未 fsync 的数据可能丢——由**多副本**兜底(未来在复制层)。
- 本层保证:**flush 之前的数据崩溃后可完整恢复**(崩溃一致性属性测试)。因 buffered,测试直接断言恢复出的 data_end == 最后 flush 边界。
- 恢复:加载已落盘索引 + 按**持久化水位**回放 log 尾部(增量,对齐 Ambry `recover()`);索引损坏时自动回退全量扫 log 重建(数据 fsync 是持久边界,索引可重建)。
- compaction 崩溃:`COMPACT_COMMIT` 标记 + open 时收敛 + 清理残留 `.tmp` 新段。

## 7. 测试策略(学 ShardStore)

- 单元测试:round-trip、段满、重开恢复、rollover、log 满、索引增删查/重开、undelete/updateTtl/hard_delete。
- **崩溃一致性属性测试**:随机 Append/Flush → 关闭 → 重开 → 断言「flush 是持久化边界」。
- **内存参考模型全量对拍**:确定性 PRNG 随机穿插 put/delete/flush/reopen/compact,每步全量比对 `get()` 与纯内存模型,并对拍增量 seg_bytes vs 全量扫。

## 8. 并发模型(D4):快照式无锁读 + COW compaction(设计,未实现)

> 目标:多线程服务下**读全程无锁、与写/compaction 互不阻塞**;仅在极小范围内串行。
> 现状:`BlobStore` 是 `&mut self` 单写者——写方法(`put`/`delete`/`undelete`/`update_ttl`/`compact`/`hard_delete`)独占,`get` 是 `&self`,靠 Rust 借用规则在**编译期**保证互斥,代价是零并发。D4 用运行时结构替换这个编译期保证,同时放开并发。

### 8.1 核心不变式

append-only 日志的地基不变式:**已提交的字节永不改变**。因此:
- 普通写(put/delete/undelete/update_ttl)只在活跃段**尾部追加** + 原子换索引指针,**从不动老字节** → 读老记录永远安全,**读与普通写天然并发,无需锁**(活跃段是无锁 `SkipMap`,日志是定位读写,读的 offset 恒 < data_end,永不追着尾巴跑)。
- 只有两类操作打破不变式,需要额外协调:**compaction**(搬走/删已提交段)、**hard_delete**(就地改已提交字节)。

### 8.2 读:不可变快照(RCU 读侧)

把 `log + index + seg_bytes` 打包成**一个不可变** `Inner`,用可原子替换的指针持有:
```rust
struct Inner { log: Log, index: PersistentIndex, seg_bytes: BTreeMap<u64, SegBytes> }
inner: ArcSwap<Inner>            // arc_swap::ArcSwap
// reader(无锁):
let snap = self.inner.load();    // 一次性拿整份快照(Arc clone,极廉价)
let v = snap.index.find(k)?;     // ← index 与 log 必须同版
snap.log.read(v.offset, len)     // 老快照 Arc 还活着,老文件就还在
```
**关键:swap 单位必须是「log+index 一整份快照」**。reader 解 offset 靠 index、读数据靠 log,若两者分开 swap 会出现**撕裂快照**(新 index 的 offset 配老 log)→ offset 对不上。所以整个 `Inner` 一起换。

### 8.3 compaction:copy-on-write + 原子换入 + 段级 refcount 延迟回收

不就地改索引/立即删旧段(当前 `compact` 那样),而是:
1. 后台基于**老快照**构造一份新 `Inner`(新 compacted 段 + 重建的新索引),**不碰老文件**;
2. `self.inner.store(Arc::new(new_inner))` **原子换入**——之后的新操作走新版;
3. 老 reader 手里的 `Arc<Inner>` 让老版本继续存活到它读完;
4. 旧段随其 `Arc<Segment>` **refcount 归零**时在 `Drop` 里 `unlink` **自动回收**。

于是**读与 compaction 完全无锁、互不阻塞**(RCU:读侧零成本,写侧造新版 + 原子发布 + 宽限期回收)。这比"读锁作用域覆盖 find+read + compaction 持写锁"更好,后者是退而求其次。

两个必须拿捏的点:
- **段级 refcount(增量 compaction 必需)**:CompactAll 造全新 log,老 `Inner` drop 即删老文件即可;但**增量 compaction 新旧共享大部分未动段**,不能"老 Inner drop 就删所有老文件"。段做成 `Arc<Segment>`,新旧 `Inner` 共享未动段的 `Arc`,只有被替换掉的旧段在 refcount 归零时 `Drop`→`unlink`(对齐 Ambry 的段引用计数)。
- **Unix `unlink` 语义兜底(mmap 关键)**:换入后即便**立刻** `unlink` 旧段,只要有 reader 仍持其 fd/`Mmap`,inode 数据就不释放(OS 延迟到最后引用关闭)——持有老 `Mmap` 的二分查找**不会 UAF**;新 reader 走新快照不会按老路径 open。`Arc` refcount 管逻辑生命周期、OS unlink 管物理回收,双保险。

### 8.4 需要串行/排他的极小范围

- **写 vs 写**:两个写者同时追加活跃段会争 `data_end`/写位置;`delete` 的"已删?"检查、`undelete`/`update_ttl` 的读改写都是 RMW → **写者互斥**(单写锁 or 单写线程)。读不受影响。
- **compaction 换入的并发写边界**:构造新版期间前台仍在往活跃段追加。换入时须把这些并发新写带进新版(Ambry 做法:compaction 只处理到某固定 offset,该点之后的写落活跃段,活跃段整体延续进新版)。这是"写 vs swap",**读仍不进锁**。
- **hard_delete**:就地改已提交字节,不吃 COW 红利。两个选项:(a) 改成 COW——把整段拷出、擦除后写成新段再 swap;(b) 保留对**目标记录读者**的排斥。倾向 (a),与 compaction 统一到"造新换旧"。

### 8.5 为什么不用一把对称 `RwLock`

对称读写锁会让读在写期间被挡 → **过度串行化**,浪费了活跃段无锁 SkipMap + append-only 的并发潜力。正确形状是**非对称**:读无锁(快照)、写者之间互斥、结构性重写(compaction/hard_delete)走 COW+原子换入。对齐 Ambry:一把 `storeWriteLock` 只串行化写,读基本畅通,compaction 靠 RW 锁 + 段 refcount 协调。

### 8.6 Open Question(实现前需拍板)

- 写者串行用 `std::sync::Mutex`(单写锁)还是单写线程 + channel?
- 承载阻塞 IO 的运行时:纯线程池 vs `tokio` + `spawn_blocking` vs 分片锁(sharded store)?按读写比与争用实测定。
- 段 refcount 用 `Arc<Segment>` 隐式,还是显式计数 + 手动 `waitTillRefCountZero`(对齐 Ambry)?倾向前者(Rust 原生、无需手写宽限期)。

### 8.7 实现方案与分阶段计划(经 Oracle 论证)

**目标结构**:
```rust
struct BlobStore {
    inner: ArcSwap<Inner>,          // {log, index};仅 rollover(廉价)与 compaction 换入
    write_lock: Mutex<()>,          // 串行化写者 + compaction
    seg_bytes: Mutex<BTreeMap<u64, SegBytes>>,  // 仅统计,【不在 Inner】(否则每写都要换 Inner)
}
struct Inner { log: Arc<Log>, index: Arc<PersistentIndex> }
```

**对 §8.1–8.6 的关键修正**:
1. **seg_bytes 不放 Inner**:它每写都变、但只被 stats 读,放 Inner 会逼每写换 Inner。用独立 `Mutex`,不在 get 路径上。
2. **reader 必须有重试循环(§8.2 遗漏的最关键一点)**:活跃段 SkipMap 跨 Inner 版本共享,可能返回一条指向「比 reader 快照更新的段」的条目(rollover 竞态)→ 读取时该段不在快照 → **重载快照重试**(罕见,仍无锁)。compaction 因换的是**全新 index**(offset 全变、非共享 SkipMap),老 index+老段 / 新 index+新段各自自洽,**不需重试**。
3. **活跃段共享 + `AtomicU64` data_end**:活跃段不随每写换 Inner;`append/flush/overwrite_at` 改 `&self`(`write_all_at` 本就是 `&self`),data_end 用 `AtomicU64`(reader acquire-load、writer release-store,配合 write_at 系统调用形成 happens-before,读 offset<data_end 安全无撕裂)。
4. **索引封存也需内部可变**:`sealed: ArcSwap<Vec<SealedSegment>>` + `active_seq/next_seq: AtomicU64`;`seal_active` 必须**先 swap sealed 再 clear active**(否则 reader 丢条目)。`add/find` 改 `&self`。
5. **compaction 持 `write_lock`**:期间无并发写 → §8.4 的「并发写边界」问题消失,reader 全程不阻塞。是**简化选择**(Ambry 允许写中 compaction),写中可 compaction 留作 P4+。
6. **hard_delete 必须 COW**(非可选):就地改与无锁读并发 → 撕裂读(crc mismatch 误报 Corrupt)。改为「拷段→擦除→写新段→swap」。隐私注记:RCU 下老快照/page cache 仍可能读到擦前数据,保证是**最终**擦除。
7. **compaction 放弃目录 rename**:改同目录建新段 + 段 `Drop` 时 `unlink`(容忍 ENOENT);`COMPACT_COMMIT` 恢复相应改写。`LogSegment` 需记 `path`(供 Drop-unlink)。
8. reader 用 `ArcSwap::load_full()`(持真 Arc,避免跨 syscall 钉住 slot)。

**分阶段(先易后难,每阶段全测试绿)**:
- **P1 内部可变性重构(无语义变更,现有测试即验收)**:`LogSegment` data_end→`AtomicU64`、`append/flush/overwrite_at`→`&self`、记 `path`;`PersistentIndex` `sealed`→`ArcSwap`、seqs→`AtomicU64`、`add/find`→`&self`(先 swap 后 clear)。`BlobStore` 仍 `&mut self`。
- **P2 并发读写**:引入 `Inner`+`ArcSwap<Inner>`+`write_lock`+`seg_bytes: Mutex`;`BlobStore` 方法→`&self`;writer 走写锁(rollover 时先 `inner.store` 再 `index.add`),reader 走重试循环。
- **P3 COW compaction + hard_delete**:compaction 持写锁、同目录建新段 + fresh index + `inner.store` + 段 Drop-unlink;重做 `COMPACT_COMMIT` 恢复;hard_delete 改 COW。
- **P4 后台调度器 + 性能**:周期 flush/compaction 线程 + stress/loom 测试 + 基准。


## 9. 路线图

- [x] LogSegment(同步阻塞 IO + buffered + fsync 持久边界)
- [x] Log(多段 + rollover + 恢复)
- [x] 崩溃一致性属性测试
- [x] PersistentIndex(并发活跃段 + mmap 封存段 + 持久化 Fuse filter + add/find/recover)
- [x] BlobStore(串起 Log + Index;自描述记录 + put/get/delete/flush/recover)
- [x] 从 log 扫描重建索引(`open_rebuild_index`)+ open 索引损坏自动回退重建
- [x] 记录落 expiration/op_time/life_version(重建不丢 TTL 与生命版本)
- [x] 索引持久化水位回放(对齐 Ambry `recover()`,替代每次全量重建)
- [x] compaction/GC:CompactAll + 按段增量(死占比阈值触发)+ 标记式崩溃修复
- [x] 死数据统计增量维护(seg_bytes,对齐 `BlobStoreStats`)
- [x] undelete / lifeVersion(复活已删除对象)
- [x] updateTtl(移除过期时间)
- [x] 只读查询与自省(stat / get 选项 / size_in_bytes / is_empty)
- [x] HardDeleter(软删对象数据就地清零,隐私擦除)
- [x] 内存参考模型全量对拍
- [ ] **并发模型 D4**(见 §8;分阶段 P1 内部可变性 → P2 ArcSwap+写锁+&self → P3 COW compaction → P4 后台调度)
- [ ] 后台调度器(周期 compaction + 定时 persist 线程;D4 P4)
- [x] compaction 策略:CompactAll + 按段增量 + **cost-benefit 选段**(对齐 `StatsBasedCompactionPolicy`)
- [ ] 每 Log 定容上界(容量/文件数,写满自动滚动新 partition)——设计锚点,暂缓
- [ ] (已废弃)monoio / io_uring / O_DIRECT / 自管块缓存——路线甲,已撤销
