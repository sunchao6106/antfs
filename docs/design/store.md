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
- **~~P4 后台调度器 + 性能~~(移出 store 层)**:见下「P4 归属」。

**实现落地对上述计划的偏差(Option B,已实现 P1 + P2 + P3,较原案更简)**:

原案(P2 上面)让 `Inner` 每次 rollover 也换、活跃段 SkipMap 跨版本共享,故需 reader 重试循环(修正 2);并依赖 `arc-swap` crate。落地改为等价但更简的 **Option B**:

1. **`Log` 段表自带内部锁**:`entries: RwLock<Vec<Arc<LogSegment>>>`;`append/read/flush/roll_over/…` 全 `&self`。rollover 在**写正常记录之前**就把新段 push 进共享段表(写锁),故读者一旦经索引看到某条目,其目标段必已在段表 → **无需重试循环**(消解修正 2)。段内可见性仍由 `LogSegment` 的 `AtomicU64` data_end 保证(修正 3)。
2. **`Inner` 仅 compaction/hard_delete 换**(非每 rollover):正常写经**当前** `Inner` 的内部可变 `log`/`index` 就地改,不换 `Inner`。`Inner{log, index}` 整体只在 compaction / COW hard_delete 时构建新的并原子换入,读者不见「新 log + 旧 index」撕裂态。
3. **不引 `arc-swap`**:`inner: RwLock<Arc<Inner>>`,读者取读锁**仅克隆 `Arc`**(纳秒级)后即释放、无锁读;写者/compaction 取写锁换 `Arc`。避免动共享 `Cargo.toml`/`Cargo.lock`;真无锁的 `arc-swap` 留作后续按压测决定。`PersistentIndex.sealed` 同理用 std `RwLock`(非 `ArcSwap`)。
4. **compaction 用 RCU + Unix open-fd 语义**(替代修正 7 的 Drop-unlink):持 `write_lock`,建新段/rename/`remove_file` 旧段后原子换 `Inner`。老读者持有的旧 `Arc<Inner>` 握有旧段 fd,Unix 下 rename/unlink 不影响其读,末次释放后 inode 才回收。CompactAll 保留目录 rename,增量 compaction 同目录建新段。
5. **hard_delete 改 COW(P3 已完成,修正 6 消解)**:逐个受影响段重写为「目标 PUT 数据清零 + `FLAG_HARD_DELETED`」的新段(tmp→fsync→原子 rename 覆盖,段位置/记录偏移不变),再原子换 `Inner`——**不再就地 `overwrite_at`**(该原语已从 `Log`/`LogSegment` 删除,杜绝与无锁读并发的撕裂读footgun)。老读者经旧 Inner 的旧段 fd 仍读到擦前明文(Unix open-fd),故保证是**最终**擦除。幂等(已擦记录带 `FLAG_HARD_DELETED`,重入跳过)。并发正确性由 `blob.rs` 的 `concurrent_shared_access_*` 与 `concurrent_hard_delete_never_torn_reads` 测试守护。

**P4 归属:后台调度移出 store 层 → 抽象 blob-server 角色**(对齐 Ambry:`BlobStore` 只提供 compaction 机制,`CompactionManager`/`StorageManager` 才做调度)。`store::BlobStore` 层**只需暴露 compact/flush 能力**(已具备:`compact` / `compact_incremental` / `compact_cost_benefit` / `flush`,均 `&self` 可被后台线程调用);**周期 flush/compaction 线程、触发策略、多 store 编排** 属上层 blob-server 组件(待建),不在 store crate 内实现。故 **D4 在 store 层到 P3 即完成**;stress/loom/基准随 blob-server 或后续性能阶段再做。




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
- [x] updateTtl(设/移除过期时间,支持任意 expiration,一次性 + 已删/不存在守卫)
- [x] 只读查询与自省(stat / get 选项 / size_in_bytes / is_empty / is_key_deleted)
- [x] 有效数据字节(`valid_data_size()`:未删除赢家 size 之和,墓碑不计;**不区分过期**——过期但未回收对象仍占盘故计入,避免按过期时刻分桶的复杂度)。增量计数 O(1) 查询,写路径维护、compaction/open 重算,全扫基准差分对拍守护
- [x] HardDeleter(软删对象数据就地清零,隐私擦除)
- [x] 内存参考模型全量对拍
- [x] **复制增量拉取(A):`Journal`(内存 offset→key 有序游标,超限逐出)+ `find_entries_since(token, max_size)`(journal 快路径 + 扫 log 慢路径,批内按 key 去最新态,`max_size` 限量)+ `find_missing_keys`;对齐 Ambry `Journal`/`findEntriesSince`。** 简化:`FindToken` 仅 offset 游标,sessionId/incarnationId/resetKey(重启/compaction 失效重定位)属复制层职责,待其落地补
- [x] **并发模型 D4(store 层完成 P1–P3)**(见 §8;Option B:`Log` 段表 `RwLock`、`BlobStore` 方法 `&self`、`inner: RwLock<Arc<Inner>>` 仅 compaction/COW hard_delete 换入、无重试循环、不引 `arc-swap`;hard_delete 改 COW、删除就地 `overwrite_at` footgun;并发测试守护)
- [ ] 后台调度器(周期 compaction + 定时 persist 线程)—— **移出 store 层,归属抽象 blob-server 角色**(对齐 Ambry `CompactionManager`/`StorageManager`);store 只暴露 `compact*`/`flush` 机制
- [x] compaction 策略:CompactAll + 按段增量 + **cost-benefit 选段**(对齐 `StatsBasedCompactionPolicy`)
- [x] 每 Log 定容上界(store 层):`total_capacity`(per-replica 构造参数,对齐 Ambry `capacityInBytes`)+ 写满归一为 `Error::StoreFull` + 容量自省(`total_capacity`/`available_capacity`/`is_full`)。**「写满自动滚动新 partition」是集群/路由层职责,不在 store**
- [x] 压测:`benchmark/store`(`bench-store` 二进制)—— 多线程共享 `Arc<BlobStore>` 压 PUT/GET,报吞吐(ops/s、MB/s)+ 延迟分位(p50/p99/max);`benchmark/` 下后续加 blob-server / S3 frontend 压测
- [ ] **封存段 EC 冷层**(热副本→冷 EC,封存段级;转换/读/修复在服务层)——见 [`cluster.md` §5 (D5)](./cluster.md);store 侧需支持封存段的 EC 读路径(有整副本直读,否则读 k 分片重构)
- [ ] (已废弃)monoio / io_uring / O_DIRECT / 自管块缓存——路线甲,已撤销

## 10. 配置项清单(对齐 Ambry `StoreConfig`)

> **状态:部分接入**。[`StoreConfig`](../../crates/store/src/config.rs) 已落地 `[store]` 组的 `segment_size_bytes`
> 与 `index_max_inmem_elements`(serde + `Default` + `Validate`),经 `BlobStore::create_with/open_with`
> 依赖注入(对齐 Ambry `StorageManager`→`BlobStore`)。`total_capacity` 是 per-replica 属性(对齐 Ambry
> `capacityInBytes`),仍作构造参数显式传入,不在 config。**其余各组仍为规划**,待对应行为实现时逐项接入。
> 范式见 [`common.md` §1](./common.md)。默认值/范围以 Ambry `StoreConfig` **构造函数**为准。

### 10.1 `[store]` 核心 / 索引 / Log
| key(TOML 路径) | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.data_flush_interval_seconds` | u64 | 60 | ≥1 | 数据周期 flush 间隔 | `store.data.flush.interval.seconds` |
| `store.data_flush_delay_seconds` | u64 | 5 | ≥0 | 启动后首次 flush 延迟 | `store.data.flush.delay.seconds` |
| `store.index_max_memory_size_bytes` | u64 | 20971520 | ≥1 | 活跃索引常驻内存上限(触发封存) | `store.index.max.memory.size.bytes` |
| `store.index_max_inmem_elements` | u64 | 10000 | ≥1 | 活跃索引内存条目上限 | `store.index.max.number.of.inmem.elements` |
| `store.max_journal_entries_to_return` | u32 | 5000 | 1–10000 | journal 单次返回上限 | `store.max.number.of.entries.to.return.from.journal` |
| `store.index_persisted_entry_min_bytes` | u32 | 115 | ≥1 | 索引条目持久化最小字节(注:Rust `IndexValue` 定长 59B,值需按实际布局重定) | `store.index.persisted.entry.min.bytes` |
| `store.segment_size_bytes` | u64 | **待定** | ≥1 | 单 log 段大小(Ambry 默认 `Long.MAX`=单巨段;ambry-rs 需按"每 Log 定容上界"设计锚点定具体值) | `store.segment.size.in.bytes` |
| `store.index_mem_state` | enum | `mmap_without_force_load` | 枚举 | 索引内存策略 | `store.index.mem.state` |

### 10.2 `[store.compaction]`
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.compaction.triggers` | Vec\<String\> | `[]` | ⊆{periodic,admin} | 触发器,空=禁用 | `store.compaction.triggers` |
| `store.compaction.check_frequency_hours` | u32 | 168 | 1–8760 | 检查可否 compaction 频率 | `store.compaction.check.frequency.in.hours` |
| `store.compaction.min_used_capacity_to_trigger_percentage` | u8 | 50 | 0–100 | 触发的最小已用容量占比 | `store.min.used.capacity.to.trigger.compaction.in.percentage` |
| `store.compaction.min_log_segment_count_to_reclaim` | u32 | 1 | 1–1000 | 触发所需最小可回收段数 | `store.min.log.segment.count.to.reclaim.to.trigger.compaction` |
| `store.compaction.max_valid_data_percentage_to_qualify` | f64 | 0.30 | 0.0–1.0 | 段有效数据占比 ≤ 此值才够格 | `store.max.log.segment.valid.data.percentage.to.qualify.compaction` |
| `store.compaction.stats_min_cost_percentage` | f64 | 0.06 | 0.0–1.0 | stats 策略单段最小成本 | `store.stats.based.compaction.min.cost.in.percentage` |
| `store.compaction.stats_weight_on_benefit` | bool | false | — | 更看重收益而非 IO 成本 | `store.stats.based.weight.on.benefit.enabled` |
| `store.compaction.policy_switch_timestamp_days` | u32 | 6 | 1–14 | Hybrid 切 CompactAll 周期 | `store.compaction.policy.switch.timestamp.days` |
| `store.compaction.operations_bytes_per_sec` | u64 | 1048576 | ≥1 | compaction IO 限速 | `store.compaction.operations.bytes.per.sec` |
| `store.compaction.min_operations_bytes_per_sec` | u64 | 524288 | ≥1 | compaction 最小 IO 速率 | `store.compaction.min.operations.bytes.per.sec` |
| `store.compaction.operations_adjust_k` | f64 | 1.0 | -100.0–100.0 | 期望拷贝速率调节系数 | `store.compaction.operations.adjust.k` |
| `store.compaction.io_per_mb_read_latency_threshold_ms` | u32 | 20 | ≥0 | 归一化读延迟阈值(超则降速) | `store.compaction.io.per.mb.read.latency.threshold.ms` |
| `store.compaction.io_per_mb_write_latency_threshold_ms` | u32 | 20 | ≥0 | 归一化写延迟阈值 | `store.compaction.io.per.mb.write.latency.threshold.ms` |
| `store.compaction.min_buffer_size_bytes` | u64 | 10485760 | ≥0 | copy 阶段最小缓冲 | `store.compaction.min.buffer.size` |
| `store.compaction.purge_delete_tombstone` | bool | false | — | 清除过期删除墓碑 | `store.compaction.purge.delete.tombstone` |

> Ambry 的 `store.compaction.enable.direct.io` / `direct.io.buffer.size` / `throttler.check.interval.ms` / `filter` 与本项目路线乙(buffered IO)不匹配或实现细节化,**暂不纳入**。

### 10.3 `[store.stats]`(死数据统计,对齐 `BlobStoreStats`)
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.stats.bucket_count` | u32 | 0 | 0–10000 | 分桶数(0=禁用 bucketing) | `store.stats.bucket.count` |
| `store.stats.bucket_span_minutes` | u64 | 60 | 1–10000 | 每桶时间跨度 | `store.stats.bucket.span.in.minutes` |
| `store.stats.recent_entry_processing_interval_minutes` | u64 | 2 | 1–60 | 队列处理器频率 | `store.stats.recent.entry.processing.interval.in.minutes` |
| `store.stats.valid_size_interval_secs` | u64 | 120 | 1–3600 | validDataSize 采集频率 | `store.get.valid.size.interval.in.secs` |
| `store.stats.wait_timeout_secs` | u64 | 120 | 0–1800 | 等待分桶构建上限 | `store.stats.wait.timeout.in.secs` |
| `store.stats.index_entries_per_second` | u32 | 240000 | ≥1 | stats 扫描限速 | `store.stats.index.entries.per.second` |

### 10.4 `[store.hard_delete]` / TTL / 保留期
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.hard_delete.enabled` | bool | false | — | 是否启用硬删(数据擦除) | `store.enable.hard.delete` |
| `store.hard_delete.operations_bytes_per_sec` | u64 | 102400 | ≥1 | 硬删 IO 限速 | `store.hard.delete.operations.bytes.per.sec` |
| `store.hard_delete.deleted_message_retention_minutes` | u64 | 10080 | ≥1 | 软删后多久可硬删(Ambry 默认 168h) | `store.deleted.message.retention.hours/minutes` |
| `store.hard_delete.ttl_update_buffer_seconds` | u32 | 86400 | ≥0 | TTL 更新须早于过期的缓冲 | `store.ttl.update.buffer.time.seconds` |

### 10.5 `[store.filter]`(存在性过滤器;ambry-rs 用 Binary Fuse 替 Bloom)
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.filter.rebuild_on_startup` | bool | false | — | 启动时重建 `.fuse`(损坏兜底) | `store.index.rebuild.bloom.filter.enabled` |

> Ambry 的 `bloom.max.false.positive.probability` / `uuid.based.bloom.filter.enabled` / `bloom.filter.maximum.page.count` 是 Bloom 专属参数;Binary Fuse 的假阳率与体积由构造固定(~1.08 B/key),**无对应项**。

### 10.6 `[store.files]`(权限 / IO 错误 / 部分恢复)
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.files.set_permission_enabled` | bool | false | — | 是否设置文件权限 | `store.set.file.permission.enabled` |
| `store.files.data_file_permission` | String | `rw-rw----` | POSIX 权限串 | 数据文件权限 | `store.data.file.permission` |
| `store.files.operation_file_permission` | String | `rw-rw-r--` | POSIX 权限串 | 运维文件权限 | `store.operation.file.permission` |
| `store.files.io_error_count_to_trigger_shutdown` | u32 | u32::MAX | ≥1 | 触发 store 关闭的 IO 错误阈值 | `store.io.error.count.to.trigger.shutdown` |
| `store.files.partial_log_segment_recovery_enabled` | bool | false | — | 尾段部分损坏时截断续恢复 | `store.enable.partial.log.segment.recovery` |
| `store.files.partial_log_segment_recovery_remaining_threshold_bytes` | u64 | 5242880 | 0–1GiB | 上者剩余数据阈值 | `store.partial.log.segment.recovery.remaining.data.size.threshold` |

## 11. 监控项清单(对齐 Ambry `StoreMetrics`)

> **状态:部分接入**。[`StoreMetrics`](../../crates/store/src/metrics.rs) 已落地 **BlobStore 层子集**,经
> `BlobStore::create_with/open_with` 注入,按 `store="<id>"` label 区分多实例(见 [`common.md` §2.2](./common.md))。
> **已接入**:`store_request_duration_seconds{store,op}`(op = put/get/delete/undelete/ttl_update/flush)、
> `store_already_exists_total`、`store_compaction_reclaimed_bytes_total`、`store_compaction_duration_seconds`、
> `store_hard_delete_total`、`store_hard_delete_duration_seconds`、`store_log_used_capacity_bytes`、
> `store_log_invalid_data_bytes`、`store_log_segment_count`。
> **仍规划**:Log 的 overflow、PersistentIndex 的 recovery/find/flush 计时与 token 计数、封存段/Fuse 过滤器命中、
> BlobStoreStats 扫描等——这些打点在 `Log`/`PersistentIndex`/`IndexSegment` 内部,需把 handle 透传进这些模块后接入。
> 命名规范见 [`common.md` §2](./common.md)。裸构造器(`create`/`open`)用一个丢弃的私有 Registry(打点不导出),仅测试/无监控场景。

### 11.1 BlobStore(顶层操作)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_request_duration_seconds` | histogram | `store`,`op`(get/put/delete/undelete/ttl_update/find_key…) | 秒 | 各操作耗时(合并 Ambry 的分操作 Timer) | `Store{Op}Response` 系列 |
| `store_start_duration_seconds` | histogram | `store` | 秒 | 启动耗时 | `StoreStartTime` |
| `store_shutdown_duration_seconds` | histogram | `store` | 秒 | 关闭耗时 | `StoreShutdownTimeInMs` |
| `store_start_failure_total` | counter | `store` | 次 | 启动失败 | `StoreStartFailure` |
| `store_io_error_shutdown_total` | counter | `store` | 次 | IO 错误触发关闭 | `StoreIoErrorTriggeredShutdownCount` |
| `store_already_exists_total` | counter | `store` | 次 | put 命中已存在 key | `IdenticalPutAttemptCount` |

### 11.2 Log(数据层)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_log_used_capacity_bytes` | gauge | `store` | 字节 | 已用容量 | `CurrentCapacityUsed` |
| `store_log_used_capacity_ratio` | gauge | `store` | 比例 | 已用占比 | `PercentageUsedCapacity` |
| `store_log_segment_count` | gauge | `store` | 个 | 段数 | `CurrentSegmentCount` |
| `store_log_invalid_data_bytes` | gauge | `store` | 字节 | 死数据量(seg_bytes 推导) | `CurrentInvalidDataSize` |
| `store_log_overflow_error_total` | counter | `store`,`kind`(read/write) | 次 | 读/写越界错误 | `OverflowReadError`/`OverflowWriteError` |

### 11.3 PersistentIndex(索引)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_index_recovery_duration_seconds` | histogram | `store` | 秒 | 恢复耗时 | `IndexRecoveryTime` |
| `store_index_find_duration_seconds` | histogram | `store` | 秒 | 查找耗时 | `IndexFindTime` |
| `store_index_flush_duration_seconds` | histogram | `store` | 秒 | 索引落盘耗时 | `IndexFlushTime` |
| `store_index_nonzero_message_recovery_total` | counter | `store` | 次 | 恢复出非零消息 | `NonZeroMessageRecovery` |

### 11.4 封存段 / Fuse 过滤器(对齐 Ambry `IndexSegment` Bloom 打点)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_filter_query_total` | counter | `store`,`result`(hit/miss) | 次 | 过滤器查询命中/未命中 | `BloomAccessedCount`/`BloomPositiveCount` |
| `store_filter_false_positive_total` | counter | `store` | 次 | 假阳(过滤器过、段内无) | `BloomFalsePositiveCount` |
| `store_filter_crc_failure_total` | counter | `store` | 次 | `.fuse` CRC 校验失败 | `BloomCRCValidationFailureCount` |
| `store_filter_rebuild_total` | counter | `store` | 次 | 加载失败触发重建 | `BloomRebuildOnLoadFailureCount` |
| `store_sealed_segment_mmap_loaded_total` | counter | `store`,`loaded`(yes/no) | 次 | 查找时封存段是否已 mmap 载入 | `MappedSegmentIsLoaded/NotLoadedDuringFindCount` |
| `store_segments_accessed_per_blob` | histogram | `store` | 个 | 每次查找访问的段数 | `SegmentsAccessedPerBlobCount` |

### 11.5 HardDeleter
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_hard_delete_duration_seconds` | histogram | `store` | 秒 | 硬删耗时 | `HardDeleteTime` |
| `store_hard_delete_total` | counter | `store`,`result`(done/failed) | 次 | 硬删完成/失败 | `HardDeleteDoneCount`/`HardDeleteFailedCount` |
| `store_hard_delete_progress_bytes` | gauge | `store` | 字节 | 硬删进度 | `CurrentHardDeleteProgress` |

### 11.6 Compaction(对齐 Ambry `BlobStoreCompactor`)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_compaction_in_progress` | gauge | `store` | 0/1 | 是否正在 compaction | `CompactionInProgress` |
| `store_compaction_copied_bytes_total` | counter | `store` | 字节 | 累计拷贝字节 | `CopyRateInBytes`(Meter→counter,速率查询端算) |
| `store_compaction_reclaimed_bytes_total` | counter | `store` | 字节 | 累计回收字节 | `CompactionBytesReclaimedCount` |
| `store_compaction_duration_seconds` | histogram | `store`,`kind`(full/partial) | 秒 | 单次 compaction 耗时 | `CompactionFinishedForAStoreTimeInMs`/`PartialRange…` |
| `store_compaction_cost` / `store_compaction_benefit` | gauge | `store` | — | 选段 cost/benefit | `CompactionCost`/`CompactionBenefit` |

### 11.7 BlobStoreStats(死数据扫描)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_stats_scan_duration_seconds` | histogram | `store`,`kind`(on_demand/bucketing) | 秒 | 扫描耗时 | `StatsOnDemandScanTotalTimeMs`/`StatsBucketingScanTotalTimeMs` |
| `store_stats_scan_error_total` | counter | `store`,`kind`(scanner/queue) | 次 | 扫描/队列错误 | `BlobStoreStatsIndexScannerErrorCount`/`…QueueProcessorErrorCount` |
| `store_stats_forward_scan_entries` | histogram | `store` | 个 | 前向扫描条目数 | `StatsForwardScanEntryCount` |
