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
**段头(v3)** —— 物理固定 64 B(其中 44 B 有意义),小端;末尾 crc32 覆盖其前:

| 偏移 | 字段 | 长度 | 说明 |
|---|---|---|---|
| 0..3 | `magic("ant")` | 3B | ASCII `a`,`n`,`t`;段文件身份锚(2026-10-21 起替代 4B `"AMBS"`;无历史兼容包袱,布局原地改、段头 version 值仍 `= 3`) |
| 3..5 | `version` | u16 | `= 3` |
| 5..8 | `reserved` | 3B | |
| 8..16 | `capacity` | u64 | 段容量 |
| 16..24 | `data_end` | u64 | 持久化水位 |
| 24..32 | `created_ns` | u64 | |
| 32..36 | `key_prefix` | u32 | blobId **高 32bit**(该副本的 partitionId) |
| 36..40 | `generation` | u32 | 段的**代次**(v3 新增;与段名同值,见下) |
| 40..44 | `crc` | u32 | 覆盖 `[0,40)` |

- **段名即段身份**:文件名 `{position}_{generation}_log.seg`;段头**自述**同一个 `generation`(两者不一致 →
  `Corrupt`:段文件被替换或名字被改过)。`generation` 在**同一 log 目录内单调递增**(每滚一段 = 当前最大代次 + 1),
  且**跨全库 `compact` 接着旧 log 的最大代次往后取**,故"段身份(位置 + 代次)在整个副本历史上永不复用"。
  自述的用途:物理拷贝/校验时**不必信文件名** —— `peek_generation` 直接从段文件前缀字节读出代次。
- `key_prefix` **每段一份、整副本恒定**:一个 log 段只属于一个 partition-replica。`create` 时写入,rollover 建新段继承同一前缀;`open` 时读回并核对(**所有段必须一致,否则 `Corrupt`**)。
- 追加走 buffered `write_at`,推进内存 `data_end`;对象 offset 0 映射到物理 `HEADER_SIZE`。
- `flush()` = 写 header(data_end)+ `sync_all`;恢复时按 header 的 data_end 还原。
- 未 flush 数据恢复时不可见(header 未更新)——刻意的持久化契约。
- **段容量硬上界 4 GiB**(`log_segment::MAX_CAPACITY`,亦由 `StoreConfig::validate` 的 `MAX_SEGMENT_SIZE_BYTES` 校验,见 §10.1)。理由是**格式不变量而非调优旋钮**:段内偏移要放进索引条目 u32 的偏移半区,且"单条记录 ≤ 段 − 64 < `u32::MAX`"使记录的 u32 `data_len` 结构性无法回绕。

**段身份 = 位置 + 代次(`LogSegmentName`)** —— 段文件名 `{position}_{generation}_log.seg`,段头也**自述**同一个代次
(v3 新增,见下表)。`position` 是段在本 log 内的逻辑序号(全库 `compact` 后从 0 重来);
`generation` 在**同一 log 目录内单调递增**(每滚一段 = 当前最大代次 + 1),且**跨全库 `compact` 继续递增**
(新目录接旧 log 的最大代次往后取)—— 故段身份在整个副本历史上**永不复用**。凡"整体重写 log、段位置从头开始"
的操作(全库 `compact`),新段身份必然陌生 ⇒ 旧游标认不出自己的段;增量 compaction 也往同目录建**更高代次**
的新段(位置更高)、并删掉被压的旧段 ⇒ 指向被压段的游标同样认不出(方向是多拉,安全)。段身份不参与读写路径,
只用于复制侧判定"游标是否还在同一个坐标系里"(对齐 Ambry `StoreFindToken` 的**段名**校验;见 §9.2)。

**副本身份 = `LOG_INCARNATION`(u64 文本)** —— 每个副本目录一份,记「这份副本是第几代」。**由 master 在
"从零建这份副本"时颁发**(单调、永不复用;见 cluster.md §7.4 `AssignReplicaIncarnation`),副本重建、
**换盘后重来**都是新号 ⇒ 别的节点手里的旧游标一律作废。身份本身不参与读写路径,只用于复制侧判定
"对面这份副本是不是我游标所在的那一代"(对齐 Ambry `StoreFindToken` 的 incarnation 语义;见 §9.2)。

- **两层各管一件事**:**段身份**(位置 + 代次)由引擎在 `find_entries_since` 入口校验,认不出 = 我方 log 被 compaction 改写;
  **副本 incarnation** 由服务层在 `replica_metadata` 校验(引擎看不到副本级身份),不符 = 对面整份副本换了代。
- 之所以落成**文件**而不是每次从首段现算:增量 compaction 有可能把首段压掉删掉,那时"首段"就换人了,
  身份会无故改变(→ 每次 compaction 都迫使拉取方全量重拉)。
- **不含盘级计数器**:身份由 master 发放(单机/离线时从首段兜底),故"换盘导致计数复位"这类窗口在结构上不存在
  —— **盘级计数器方案已被否决**(换盘即复位,旧游标会看似合法)。
- 文件缺失或损坏(如物理 bootstrap 只拷段):`BlobStore::open` 用**本机当前时刻**(纳秒)兜底并落盘 ——
  **绝不用段头里的 `created_ns`**:物理拷贝来的段诞生在**对端**,那时刻正是对端自己的兜底身份,拿它当
  本副本的身份,重建两次就可能与对端**撞号**,"重建 ⇒ 身份必换"（下面这条保证）就会在"申领没成功"时失效。
  用本机时钟则身份永远**本机独有**:重建必得新号,重启重开因文件已落盘而保持不变。
  master 不可达 / 拒绝发号时同样保留这个本地兜底值,副本照常建、照常服务(下次从零建这份副本时再申领)。
- 崩溃一致性:先建首段、再落身份;崩在中间只会留下"没有身份文件"的目录,由 `BlobStore::open` 自愈。

**记录(v4)** —— `version(1) | flags(1) | key_suffix(4) | expiration_ms(8) | data_len(4) | data | crc(4)`,固定开销 **22 B**(一条记录的物理总长 = `22 + data_len`):

| 偏移 | 字段 | 长度 | 说明 |
|---|---|---|---|
| 0 | `version` | u8 | `= 4` |
| 1 | `flags` | u8 | 0 = PUT,`FLAG_DELETED` = 墓碑,`FLAG_COMPOSED`(bit 4)= 拼装描述符类型位(`blob-composed-flag`,布局不变);位 1-3 预留 |
| 2..6 | `key_suffix` | u32 | 大端 = blobId **低 32bit**(objectId) |
| 6..14 | `expiration_ms` | i64 | `-1` = 不过期 |
| 14..18 | `data_len` | u32 | 数据体长度 |
| 18.. | `data` | | 数据体 |
| 末尾 4 B | `crc` | u32 | 覆盖其前所有字节 |

- **无帧 magic**(2026-10-21 密度优化:无历史兼容包袱,v4 布局原地改、版本值不动):记录的**身份/位置**校验由
  偏移 0 的 `version` 字节承担(`!= 4` 即 `Corrupt`,坏记录重同步的落点确认同理),**完整性**仍由尾部
  `crc`(2⁻³²)把关 —— 磁盘上的数据正确性红线不变,削掉的是每条 2B 的身份哨兵(旧 `"MB"`)。

- **key 高 32bit 不在记录里,只在段头存一次**:同一副本(同一分区)内所有 key 的高 4B 恒等 = 段头的 `key_prefix`,故记录只存 4B 后缀;读取方用 `key_prefix ‖ key_suffix` 拼回完整 8B blobId。**写路径校验 key 的 partition 与该副本前缀一致,不一致 → `BadArgument`**(绝不静默拼出一个错 key)。
- `expiration_ms` **只在写入时给定**(`-1` = 不过期),写完即定、之后无处可改。

### 3.1 记录不做对齐 padding、offset 全宽存储(选型 + 被否决方案)

**决策**:数据区变长记录**紧密顺序打包,不做 N 字节对齐补齐**;`IndexValue` 的 offset 用**全宽字节偏移**(`LogOffset { segment, offset }`,段内 offset 直接存真实字节号),**不做 ÷N 压缩**。对齐 Ambry:`IndexValue.OFFSET_SIZE_IN_BYTES = 8`,段内 offset 是原始 `long`,无对齐、无压缩(源码 `ambry-store/.../IndexValue.java` 实证)。

**为什么**——offset 编码有三种做法,权衡三方:**寻址范围 ↔ 数据文件浪费 ↔ 索引条目大小**:

| 方案 | 数据浪费 | 索引 offset 占用 | 寻址范围 | 复杂度 | 代表 |
|---|---|---|---|---|---|
| ① ÷N 对齐(存 `offset/N`,强制记录起点 N 对齐) | 平均 ~N/2 B/对象 padding | 最小(如 4B) | `2^(8·宽) · N` | 中(维护"必须对齐"不变量) | SeaweedFS(N=8→32GiB) |
| **② 全宽 offset(直接存字节号,不对齐)** | **0** | 大(8B:段号 u32 ‖ 段内偏移 u32) | 近乎无限 | **最简单** | **Ambry / antfs 本方案** |
| ③ 余数塞索引(存 `offset/N` + 低位余数进空闲 bit) | **0** | 小(4B + ~3bit) | `2^(8·宽) · N` | 高(需腾空闲位 + 破坏扫描步进) | — |

**分水岭是「索引驻留在 RAM 还是磁盘」,不是 key 长度**:
- 方案①(÷N 压缩)只在**索引全量常驻 RAM**时才划算——每字节 ×十亿条都要命,故牺牲磁盘 padding 换 RAM。这是 SeaweedFS 的约束(needle map 全内存)。
- antfs 的索引是**磁盘上的 mmap 封存段 + 过滤器,并非全内存**(见 §5、§1),offset 多占几字节是**磁盘账(便宜)**,不吃 RAM。故选方案②:**换来 log 零 padding + 无"必须对齐"不变量(少一类崩溃一致性 footgun,对齐持久性红线)+ 单段寻址上限由容量策略而非位宽决定**。

**被否决:方案①(÷N 对齐)**——理由:我们索引不常驻 RAM,①省的 RAM 收益不存在,却白背 padding 浪费 + 对齐不变量的维护/崩溃风险。**保留:方案③** 仅作未来"把热索引子集缓进 RAM 做小对象吞吐"时的二段优化,base 设计不用。

> 注:小对象的存储放大主要来自**每对象固定头开销**(header/crc/时间戳等),对齐 padding 只是其中最小一项;真要压放大,高杠杆在**上层对象聚合/chunking**(对齐 Ambry composite blob),而非抠 offset 位编码。

## 4. Log(多段)

- 单条记录不超过一个段的数据容量(与 Ambry 一致);**总容量不必是段容量的整数倍** —— 末段为"残段"
  (容量 = `total − k × segment`),装不下记录时该部分空间按写满处理(`SegmentFull` → `StoreFull`),
  故配置项之间解耦(如 100 GiB 分区 + 3 GiB 段 = 33 整段 + 1 个 1 GiB 残段;段上限 4 GiB,见 §10.1),`total < segment` 亦退化为单段。
- 活跃段放不下 → **滚动到新段**(同目录新建,代次取当前最大 + 1;段文件名 `{position}_{generation}_log.seg`)。**旧段 flush 不在滚转临界区做**:`store.defer_seal_flush`(默认开)将其延迟到既有 flush 通道(worker 周期/`flush()`/`close()`)收敛 —— 满段 fsync 在写锁内实测 4 GiB 段为秒级停摆;`false` 保留旧行为(滚点同步落盘)。见 `openspec/changes/log-rollover-off-critical/`(archive 前的工作目录)与 `benchmark/BASELINE.md`「修复后决算」。
- `LogOffset { segment, offset }` 为跨段全局地址(PersistentIndex 引用它);`segment` 只是**位置序号**,
  **不含代次** —— 代次只活在**段名**与**复制游标**里(见 §3、§9.2),故索引值保持 21B 定长布局不变。
- `open()` 扫描目录、按段名解析出段身份、按**位置**排序恢复所有段(代次只做身份,不参与排序);
  段头自述的代次与文件名不一致 → `Corrupt`。

## 5. PersistentIndex(索引)

key(`StoreKey`,**8B** = 64bit blobId:高 32bit partitionId + 低 32bit objectId,见 cluster.md §8 / `openspec/changes/blobid-master-assign`)→ `IndexValue`(**定长 21B**,小端):

```
0..8   offset        u64 = (段号 u32 << 32) | 段内偏移 u32
8..12  size          u32 = 整条记录的字节数(记录头 + 数据体 + crc)
12     flags         u8  (FLAG_DELETED 墓碑 / FLAG_COMPOSED 描述符类型位 bit 4;位 1-3 预留)
13..21 expiration_ms i64 (-1 = 不过期)
```

索引条目 = 8B key + 21B value = **29B**(`VALUE_SIZE = 21`、`ENTRY_SIZE = 29`、`RECORD_OVERHEAD = 24`),较上一版 67B(59B value + 8B key)每条省 38B。**索引的 key 仍是完整 8B blobId**(它是跨段的全局查找键;段内前缀只有 log 段才知道,故只有记录里才缩成 4B 后缀,见 §3)。

**21B 布局未因段身份改版而变**:offset 半区里只有段**位置**(u32)与段内偏移(u32),**段代次不进索引** —— 代次只活在段名与复制游标里(见 §3、§9.2)。故"段身份 = 位置 + 代次"落地后,索引值仍是 21B 定长、`.idx` 条目仍是 29B。

- **活跃段**:并发 `SkipMap`,写入即插入;无过滤器。
- **封存段**:活跃段达阈值 → 写 `.idx`(排序条目)+ `.fuse`(**Binary Fuse 过滤器持久化**:
  descriptor+fingerprints,`BinaryFuse8Ref` 零拷贝引用)→ **mmap(`memmap2`)载入**。
- **newest-wins**:对象不可变 + 删除不可逆 ⇒ 同一 key 在 log 里**至多两条**记录(PUT、墓碑),索引只留**最新一条**作为赢家;查找先活跃段、再封存段按 seq 从新到旧,首个命中返回。重建时按 log 追加顺序(旧→新)插入,后者覆盖前者,天然还原 newest-wins。
- **单值索引、无"数据重写"**:每 key 只存一个 `IndexValue`(不同于 Ambry 的每 key 多值)。`put`/`delete` 都是"往 log 尾部追加一条新记录 + 覆盖索引赢家",**永不改写已提交字节**。故 `IndexValue.offset` 恒等于承载该状态的那条记录的位置,不破坏"offset 相等的 compaction/rebuild/seg_bytes"机制。
- **IO**:同步 `std::fs`;**写走 tmp → fsync → rename → fsync 目录**,先 fsync 再 rename;header/body 各带 crc32。

## 6. 持久化 / 崩溃语义

- 单机崩溃(掉电/panic):最近一次 flush 之后、未 fsync 的数据可能丢——由**多副本**兜底(未来在复制层)。
- 本层保证:**flush 之前的数据崩溃后可完整恢复**(崩溃一致性属性测试)。因 buffered,测试直接断言恢复出的 data_end == 最后 flush 边界。
- 恢复:加载已落盘索引 + 按**持久化水位**回放 log 尾部(增量,对齐 Ambry `recover()`);索引损坏时自动回退全量扫 log 重建(数据 fsync 是持久边界,索引可重建)。
- compaction 崩溃:`COMPACT_COMMIT` 标记 + open 时收敛 + 清理残留 `.tmp` 新段。进程内 COPY 失败(如 ENOSPC / 读到坏记录):哨兵就地删掉半成品(`log.new` / `.tmp` 段)并撤掉标记——都还没 rename,无副作用,不留到重启。

## 7. 测试策略(学 ShardStore)

- 单元测试:round-trip、段满、重开恢复、rollover、log 满、索引增删查/重开、put/delete(墓碑恒胜、删除不可逆)。
- **崩溃一致性属性测试**:随机 Append/Flush → 关闭 → 重开 → 断言「flush 是持久化边界」。
- **内存参考模型全量对拍**:确定性 PRNG 随机穿插 put/delete/flush/reopen/compact,每步全量比对 `get()` 与纯内存模型,并对拍增量 seg_bytes vs 全量扫。

## 8. 并发模型(D4):快照式无锁读 + COW compaction(设计,未实现)

> 目标:多线程服务下**读全程无锁、与写/compaction 互不阻塞**;仅在极小范围内串行。
> 现状:`BlobStore` 是 `&mut self` 单写者——写方法(`put`/`delete`/`compact`)独占,`get` 是 `&self`,靠 Rust 借用规则在**编译期**保证互斥,代价是零并发。D4 用运行时结构替换这个编译期保证,同时放开并发。

### 8.1 核心不变式

append-only 日志的地基不变式:**已提交的字节永不改变**。因此:
- 普通写(put/delete)只在活跃段**尾部追加** + 原子换索引指针,**从不动老字节** → 读老记录永远安全,**读与普通写天然并发,无需锁**(活跃段是无锁 `SkipMap`,日志是定位读写,读的 offset 恒 < data_end,永不追着尾巴跑)。
- 只有 **compaction** 打破不变式(搬走/删已提交段),需要额外协调。写入与删除都只追加,**已提交字节永不改写**。

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

- **写 vs 写**:两个写者同时追加活跃段会争 `data_end`/写位置;`delete` 的"是否已墓碑?"检查(读索引再追加墓碑)是 RMW → **写者互斥**(单写锁 or 单写线程)。读不受影响。
- **compaction 换入的并发写边界**:构造新版期间前台仍在往活跃段追加。换入时须把这些并发新写带进新版(Ambry 做法:compaction 只处理到某固定 offset,该点之后的写落活跃段,活跃段整体延续进新版)。这是"写 vs swap",**读仍不进锁**。

### 8.5 为什么不用一把对称 `RwLock`

对称读写锁会让读在写期间被挡 → **过度串行化**,浪费了活跃段无锁 SkipMap + append-only 的并发潜力。正确形状是**非对称**:读无锁(快照)、写者之间互斥、结构性重写(仅剩 compaction)走 COW+原子换入。对齐 Ambry:一把 `storeWriteLock` 只串行化写,读基本畅通,compaction 靠 RW 锁 + 段 refcount 协调。

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
3. **活跃段共享 + `AtomicU64` data_end**:活跃段不随每写换 Inner;`append/flush` 改 `&self`(`write_all_at` 本就是 `&self`),data_end 用 `AtomicU64`(reader acquire-load、writer release-store,配合 write_at 系统调用形成 happens-before,读 offset<data_end 安全无撕裂)。
4. **索引封存也需内部可变**:`sealed: ArcSwap<Vec<SealedSegment>>` + `active_seq/next_seq: AtomicU64`;`seal_active` 必须**先 swap sealed 再 clear active**(否则 reader 丢条目)。`add/find` 改 `&self`。
5. **compaction 持 `write_lock`**:期间无并发写 → §8.4 的「并发写边界」问题消失,reader 全程不阻塞。是**简化选择**(Ambry 允许写中 compaction),写中可 compaction 留作 P4+。
6. **compaction 放弃目录 rename**:改同目录建新段 + 段 `Drop` 时 `unlink`(容忍 ENOENT);`COMPACT_COMMIT` 恢复相应改写。`LogSegment` 需记 `path`(供 Drop-unlink)。
7. reader 用 `ArcSwap::load_full()`(持真 Arc,避免跨 syscall 钉住 slot)。

**分阶段(先易后难,每阶段全测试绿)**:
- **P1 内部可变性重构(无语义变更,现有测试即验收)**:`LogSegment` data_end→`AtomicU64`、`append/flush`→`&self`、记 `path`;`PersistentIndex` `sealed`→`ArcSwap`、seqs→`AtomicU64`、`add/find`→`&self`(先 swap 后 clear)。`BlobStore` 仍 `&mut self`。
- **P2 并发读写**:引入 `Inner`+`ArcSwap<Inner>`+`write_lock`+`seg_bytes: Mutex`;`BlobStore` 方法→`&self`;writer 走写锁(rollover 时先 `inner.store` 再 `index.add`),reader 走重试循环。
- **P3 COW compaction**:compaction 持写锁、同目录建新段 + fresh index + `inner.store` + 段 Drop-unlink;重做 `COMPACT_COMMIT` 恢复。
- **~~P4 后台调度器 + 性能~~(移出 store 层)**:见下「P4 归属」。

**实现落地对上述计划的偏差(Option B,已实现 P1 + P2 + P3,较原案更简)**:

原案(P2 上面)让 `Inner` 每次 rollover 也换、活跃段 SkipMap 跨版本共享,故需 reader 重试循环(修正 2);并依赖 `arc-swap` crate。落地改为等价但更简的 **Option B**:

1. **`Log` 段表自带内部锁**:`entries: RwLock<Vec<Arc<LogSegment>>>`;`append/read/flush/roll_over/…` 全 `&self`。rollover 在**写正常记录之前**就把新段 push 进共享段表(写锁),故读者一旦经索引看到某条目,其目标段必已在段表 → **无需重试循环**(消解修正 2)。段内可见性仍由 `LogSegment` 的 `AtomicU64` data_end 保证(修正 3)。
2. **`Inner` 仅 compaction 换**(非每 rollover):正常写经**当前** `Inner` 的内部可变 `log`/`index` 就地改,不换 `Inner`。`Inner{log, index}` 整体只在 compaction 时构建新的并原子换入,读者不见「新 log + 旧 index」撕裂态。
3. **不引 `arc-swap`**:`inner: RwLock<Arc<Inner>>`,读者取读锁**仅克隆 `Arc`**(纳秒级)后即释放、无锁读;写者/compaction 取写锁换 `Arc`。避免动共享 `Cargo.toml`/`Cargo.lock`;真无锁的 `arc-swap` 留作后续按压测决定。`PersistentIndex.sealed` 同理用 std `RwLock`(非 `ArcSwap`)。
4. **compaction 用 RCU + Unix open-fd 语义**(替代修正 6 的 Drop-unlink):持 `write_lock`,建新段/rename/`remove_file` 旧段后原子换 `Inner`。老读者持有的旧 `Arc<Inner>` 握有旧段 fd,Unix 下 rename/unlink 不影响其读,末次释放后 inode 才回收。CompactAll 保留目录 rename,增量 compaction 同目录建新段。
5. **`Inner` 的原子换入只发生在 compaction**:正常写只就地改当前 `Inner`,"已提交字节永不改写"无例外;并发正确性由 `blob.rs` 的 `concurrent_shared_access_*` 测试守护。

**P4 归属:后台调度移出 store 层 → 抽象 blob-server 角色**(对齐 Ambry:`BlobStore` 只提供 compaction 机制,`CompactionManager`/`StorageManager` 才做调度)。`store::BlobStore` 层**只需暴露 compact/flush 能力**(已具备:`compact` / `compact_incremental` / `compact_cost_benefit` / `flush`,均 `&self` 可被后台线程调用);**周期 flush/compaction 线程、触发策略、多 store 编排** 属上层 blob-server 组件(待建),不在 store crate 内实现。故 **D4 在 store 层到 P3 即完成**;stress/loom/基准随 blob-server 或后续性能阶段再做。




## 9. 路线图

- [x] LogSegment(同步阻塞 IO + buffered + fsync 持久边界)
- [x] Log(多段 + rollover + 恢复)
- [x] 崩溃一致性属性测试
- [x] PersistentIndex(并发活跃段 + mmap 封存段 + 持久化 Fuse filter + add/find/recover)
- [x] BlobStore(串起 Log + Index;自描述记录 + put/get/delete/flush/recover)
- [x] 从 log 扫描重建索引(`open_rebuild_index`)+ open 索引损坏自动回退重建
- [x] 记录落 `expiration_ms`(重建不丢**写入时定的** TTL)
- [x] 索引持久化水位回放(对齐 Ambry `recover()`,替代每次全量重建)
- [x] compaction/GC:CompactAll + 按段增量(死占比阈值触发)+ 标记式崩溃修复
- [x] 死数据统计增量维护(seg_bytes,对齐 `BlobStoreStats`)
- [x] 只读查询与自省(stat / get 选项 / size_in_bytes / is_empty / is_key_deleted)
- [x] 有效数据字节(`valid_data_size()`:未删除赢家 size 之和,墓碑不计;**不区分过期**——过期但未回收对象仍占盘故计入,避免按过期时刻分桶的复杂度)。增量计数 O(1) 查询,写路径维护、compaction/open 重算,全扫基准差分对拍守护
- [x] 内存参考模型全量对拍
- [x] **复制增量拉取(A):`Journal`(内存 offset→key 有序游标,超限逐出)+ `find_entries_since(token, max_size)`(journal 快路径 + 扫 log 慢路径,批内按 key 去最新态,`max_size` 限量)+ `find_missing_keys`;对齐 Ambry `Journal`/`findEntriesSince`。** 游标重定位已补齐(见 §9.2):`FindToken` 带**段身份**(位置 + 代次),引擎做段身份校验、服务层做**副本 incarnation** 校验(两层),认不出即重置为从头重扫;sessionId 仍未落(重启后游标本就只在内存里,清零即从头)
- [x] **并发模型 D4(store 层完成 P1–P3)**(见 §8;Option B:`Log` 段表 `RwLock`、`BlobStore` 方法 `&self`、`inner: RwLock<Arc<Inner>>` 仅 compaction 换入、无重试循环、不引 `arc-swap`;并发测试守护)
- [x] **存储模型简化:对象不可变 + 删除不可逆**(去掉复活/改 TTL/硬删;**段头 v2→v3**(每段 4B `key_prefix`;v3 起再带 4B 段代次 `generation`,见 §3)、**记录 v4**(22B 开销;2026-10-21 去帧 magic)、**索引 v2**(29B 条目)、复制合并改为**墓碑恒胜**;TTL 只在写入时定)—— 见 `openspec/changes/simplify-delete-model/`
- [x] ~~后台调度器(周期 compaction + 定时 persist)~~ —— **已落地为本地维护 worker**(`worker.rs`:flush 固定周期 + compaction「首跑钟点 + 周期」调度,`local_worker_enabled` 默认开;原「移出 store 层、归属抽象 blob-server 角色」的设想随 store server 落地作废,调度进 store server、判定进引擎层,见 §9.1)
- [x] compaction 策略:CompactAll + 按段增量 + **cost-benefit 选段**(对齐 `StatsBasedCompactionPolicy`)
- [x] **compaction 回收口径 + 候选集策略**(见 §9.1;`CompactPolicy{dead_ratio, now_ms, tombstone_retention_ms, tombstones_allowed}`:回收被取代的旧记录 + TTL 过期对象 + 过保留期墓碑(**还要过墓碑回收闸门**);墓碑删除时刻复用索引 `expiration_ms` 槽位,布局不变)—— 见 `openspec/changes/store-compaction-policy/`
- [x] 每 Log 定容上界(store 层):`total_capacity`(per-replica 构造参数,对齐 Ambry `capacityInBytes`)+ 写满归一为 `Error::StoreFull` + 容量自省(`total_capacity`/`available_capacity`/`is_full`)。**「写满自动滚动新 partition」是集群/路由层职责,不在 store**
- [x] 压测:`benchmark/blob`(`antfs-bench-blob` 二进制)—— 多线程共享 `Arc<BlobStore>` 压 PUT/GET,报吞吐(ops/s、MB/s)+ 延迟分位(p50/p99/max);**集群/数据面端到端压测见 `antfs-bench-router / antfs-bench-frontend`**(`crates/common/router`,直连或经 frontend,见 [`router.md` §8](./router.md))
- [ ] **封存段 EC 冷层**(热副本→冷 EC,封存段级;转换/读/修复在服务层)——见 [`cluster.md` §5 (D5)](./cluster.md);store 侧需支持封存段的 EC 读路径(有整副本直读,否则读 k 分片重构)
- [ ] (已废弃)monoio / io_uring / O_DIRECT / 自管块缓存——路线甲,已撤销

### 9.1 compaction 触发与回收口径(已实现)

> 见 `openspec/changes/store-compaction-policy/`。**调度在 store 层**(`worker.rs`),**判定在引擎层**(`blob::CompactPolicy`)。

**回收什么**:三类字节为"可回收",compaction 时真正释放(不再搬进新 log/新段):

| 类别 | 判据(索引赢家) | 回收后行为 |
|---|---|---|
| 被取代的旧记录 | 非当前赢家(由 `seg_bytes.dead` 记账) | 老行为,本就回收 |
| TTL 过期对象 | `!deleted && expiration_ms != -1 && now >= expiration_ms` | 记录与索引条目一并消失:读由 `Expired` 变 `NotFound` |
| 墓碑过保留期 | `deleted && deleted_at_ms != -1 && now - deleted_at_ms >= retention`**且过了墓碑回收闸门** | 同上:已删对象的墓碑与索引条目消失 |

**墓碑的删除时刻**:`delete` 把 `now` 写进墓碑记录的 `expiration` 字段**与**索引条目的 `expiration_ms`。布局不变(21B 值、`.idx` 条目 29B),旧数据 `-1` = 删除时间未知 ⇒ **永不自动回收**(CLAUDE.md §3.4 磁盘格式兼容,带测试)。复制按记录头带删时刻,各副本判定一致;`stat()` 的 TTL 口径走 `IndexValue::ttl_expiration_ms()`(墓碑对外仍报 `-1`)。

**墓碑回收闸门(`CompactPolicy.tombstones_allowed`)**:过保留期**只说明**"删除已足够久",还要过**跨副本传播**这一关 —— 墓碑一收,还有没追平的副本就永远学不到那条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。判据是**副本级**的:

| 项 | 口径 |
|---|---|
| master 判据 | 该分区**所有副本都已追平** = 各副本上报 `ACTIVE` **且** `lag_bytes == 0`;缺上报 / 节点 down / 空 placement 都算未追平(保守) |
| 下发路径 | 随心跳 `MasterDirective.partition_states` **每拍全量**下发,store 逐条落到对应副本句柄上(`ReplicaHandle.tombstones_reclaim_allowed`,初值 `false` = 没拿到权威放行前不回收) |
| 闸门关闭时 | 墓碑**一律照搬**(连已过保留期的也留),其余可回收口径不受影响 |
| 单机模式 | 未接 master(没有别的副本可滞后)⇒ **恒放行**;否则墓碑会在这类部署里永久堆在盘上 |

**闸门与选段**:先算 partition 整体可回收占比(索引遍历聚合过期/墓碑字节 + `seg_bytes.dead`,**不读数据体**)≥ `dead_ratio` 才动手;再对**非活跃段**按同一阈值选段压缩(活跃段不动)。cost-benefit 的 `cost` 用"剔除可回收字节后的存活量"。

**取消判活的例外**:`?replication=1` 的拉取仍能读到**尚未被回收**的过期对象(复制忠实 apply);被回收后该对象不再可复制 —— 对端本就视其为过期,可接受。

### 9.2 复制游标重定位(已实现)

> 见 `openspec/changes/store-replication-cursor-revalidate/`。**引擎做段身份校验,服务层做副本 incarnation 校验**(两层),复制侧据此"多拉"而非"误判已追上"。

**两层身份**:① **段身份** = `LogSegmentName`(位置 + 代次,见 §3)—— 全库 `compact` 段位置从 0 重来但代次接着涨,增量 compaction 与滚动只往同目录建**更高代次**的段,故"旧游标认不出自己的段"(段没了 / **位置还在而代次不同**)必然会发生;② **副本身份 = `LOG_INCARNATION`**(master 在**从零建这份副本**时发放,副本重建 / 换盘后重来必然换新号)。游标把段身份(位置 + 代次 + 段内偏移)跨请求传递,incarnation 由复制会话单独持有、随请求带上。

**为什么必须有**:复制游标(`FindToken`)是**对端 log 坐标系的绝对位置**,而 compaction 会改写坐标系 ——
全库 `compact` 段位置从 0 重来、增量 compaction 把记录搬到更高的段;副本重建 / 换盘则让"这份副本"整体换代。
把"拿陈旧游标问对端、对端返回空批"当成"已追上",就会在**作废的坐标系**上判追平:副本带着缺失数据转 `ACTIVE`
→ master 的删源闸门(`reconcile::plan_move`)误成立 → 源副本被 `DROPPED` → **静默丢数据**。对齐 Ambry `revalidateStoreFindToken`
(段名/代次认不出即 `new StoreFindToken()` 从头重扫)。

> **跟着数据走四遍(大白话,读不懂上面那段黑话就看这里)**
>
> **① 正常拉一轮**:分区 3 在 A、B 两台机上各一份(RF=2),B 是刚被指派的新副本。B 问 A"你有哪些 key",
> A 一次回一批,并附一句话:"我给到 **log 第 3 段、第 500 字节**处"。这句话就是游标,B 记下来。
> 下一轮 B 带着它继续问"第 3 段第 500 字节之后还有啥",直到 A 说"没了"。**这时 B 才向 master 报"我追上了"**。
> master 收到这句才敢删掉 A 的副本腾空间 —— 所以"报追上"必须诚实,早报一次就等于删掉还活着的源。
>
> **② 拉的中途 A 做了全库 compaction**:compaction 把 A 的存活数据重写进新 log,**段号从 0 重新数**。
> B 手里"第 3 段第 500 字节"就成了一句废话(第 3 段已经不是原来那段了)。
> **危险做法**:A 面对认不出的位置回一句"那段没了、后面也没了"→ B 以为拉完了 → 报追上 → master 删掉
> 还带着数据的 A → **数据永久没了,而且没有任何报错**。
> **现在的做法**:A 认出"这句话我认不出"(段不在了 / 段还在但版本号变了 / 位置越界 / 你说的副本编号对不上),
> 一律回答"从头给你重来",并明确告诉 B"这轮我把你的位置作废了";B 收到这句就**不许**报追上,
> 下一轮拿新位置继续拉,真拉齐了才报。
>
> **③ A 的副本被删掉重建(或换盘重来)**:新副本目录从零开始,段号又从 0 数起,内容也不一样。
> B 手里"第 3 段第 500 字节"**可能恰好**在新副本上也存在(段号又从头数了),**光看位置看不出问题**。
> 所以给"这份副本"本身也发一个编号:重建/换盘就换新号,B 每轮都带着它问,对不上同样作废 + 从头拉。
> 这个编号就存在副本目录里的 `LOG_INCARNATION` 小文件中。
>
> **④ 一个"全新副本"没领到正式编号会怎样**(见 §9.2 末尾的诚实边界):master 当时联系不上,它就先拿
> **本机时间**当临时编号开张。别的机器是**从它这里第一次**听说这个编号的,不存在"别人以为你还是上一代的你"
> 这种错觉 —— **不会丢数据**;唯一的不体面是 master 的账本里这份副本的编号栏还空着(0 = 没发过),要等它
> 下次从零重建才会补上。目前没有任何代码读那一栏(只有 master 自己的测试在读),所以放着不管没有影响。

**判定与动作**(动作里的短名 = `store_server_replication_cursor_resets_total{reason}` 的取值):

| 触发 | 判据 | 动作(短名) |
|---|---|---|
| 游标位置上的段不在段表 | 引擎 `find_entries_since` 入口(对齐 Ambry `floorKey` 名字不符) | 重置为 `Uninitialized`,`FindInfo.token_reset = Some(...)` → `segment_missing` |
| 位置还在、**代次不同**(该位置已换成另一个段) | 同上(引擎按段名表比对代次) | `segment_generation_mismatch` |
| 偏移越过该段数据区 | 同上 | `offset_out_of_range` |
| 游标声明的 incarnation ≠ 本副本 incarnation,**或四件套残缺** | 服务层 `replica_metadata`(incarnation 是**本副本**的事实,引擎无从校验) | 从头重扫,`token_reset` → `incarnation_mismatch` |
| 本轮对端 incarnation 变化 | 复制侧会话(`PullSession`)+ `sync_round` | 记 `incarnation_mismatch`,**本轮不判追平** |
| 连续两轮游标没动且没 apply | 同上 | 丢弃游标(`no_progress`),下轮从头重拉 |

**不变量**:

- `scan_candidates` **不得**把"段不存在"静默当成"已越过"(那是"看似追上"的根源);重定位之后才允许从游标扫。
- 响应里 `token_reset` 非空、或本轮对端 incarnation 变化 ⇒ `caught_up_with_peer` **一律返回 false**(`ACTIVE` 是 master 删源闸门的输入,判据收紧)。
- 比较只比**位置**:`local_has_nothing_past` 用纯位置的 `BlobStore::has_content_after`(不校验段身份、不触发重定位)—— peer 游标的段身份属**对端坐标系**(本地段位置/代次与对端并不一一对应),拿它进 `find_entries_since` 会被重定位判定误伤。
- 游标必须**四件套齐全**(`pos` / `gen` / `off` / `incarnation`)才可信;残缺游标按"身份不符"处理(服务层从头重扫并如实报 `incarnation_mismatch`)。

**物理 bootstrap 与对端 compact 的竞态(已加固)**:分段拷贝**前后各列一次封存段清单**,任一条成立即**丢弃整份拷贝**、
回落纯逻辑拉取:① 取段 **404 / 出错**(段身份已变,或网络失败);② 取回的字节**段头自述代次**与清单不符
(`peek_generation` 再核一道,防"不发 404 的换段路径");③ 拷完再列清单时对端的 **incarnation 变了**(整份副本被重建)。
理由:身份一变,"同段号"在两个代次里指向不同记录,逐段取来的字节会横跨两代副本,装出来是**混合快照**
(还可能带着对端已删的活记录)。丢弃重来是幂等的、必然收敛;不这么做就只能指望后续"墓碑恒胜"把混进来的活记录修回去
—— 而墓碑一旦过保留期被回收,就修不回来了。

**不做兼容层**:线上尚未启用,不存在新旧节点并存,故不写任何回滚 / 兼容分支 —— 不认的旧格式一律按"认不出"处理(从头重扫,方向是多拉),不保留旧段名 / 旧段头 / 旧清单形状的解析分支。

**观测**:`store_server_replication_cursor_resets_total{reason}`(reason 集合见 cluster.md §9.1 监控项清单);
`warn`/`info` 日志带 `partition`、段 `position`/`generation` 与 incarnation 的 `prior`/`current`
(bootstrap 丢弃拷贝那几处是 `before`/`after`/`expected`)。非零是**正常**的(说明发生过 compaction/重建后的续拉)。

**覆盖测试**:`crates/store/tests/compaction_replication.rs`(**9 个场景**:追赶中遇全库/增量 compact、对端副本重建、
已追平后 compact 的 delete 传播、对端变空、真实后台 loop + 指标、**一次排空过程中的批间重写**、
**物理 bootstrap 途中对端重写 ⇒ 丢弃拷贝**、**本端在追赶途中做 compaction**)+ `crates/store/tests/incarnation_and_gate.rs`(假 master + 真 gRPC 心跳 loop:
建副本时申领身份、重复指令不重领、OFFLINE 挂回不换号、墓碑闸门开合)+ `blob`/`replication` 单测(重定位各短名的映射、段代次同目录单调且永不复用、
全库 compact 后旧游标被重定位而**同一份副本的 incarnation 不变**、incarnation 落盘/重开读到同一号/采纳 master 权威值/缺失时用**本机值**兜底且不继承对端身份、
`PullSession` 的"重定位 + 换代 + 停滞"状态推进)。

## 10. 配置项清单(对齐 Ambry `StoreConfig`)

> **状态:部分接入**。[`StoreConfig`](../../crates/blob/src/config.rs) 已落地 `[store]` 组的 `segment_size_bytes`
> 与 `index_max_inmem_elements`,以及 `io_error_count_to_trigger_shutdown`(被动盘检自关)与
> `defer_seal_flush`(Phase 2 滚转停摆修复)(serde + `Default` + `Validate`),经 `BlobStore::create_with/open_with`
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
| `store.io_error_count_to_trigger_shutdown` | u32 | 0(=禁用) | 任意(0 禁用,≥1 阈值) | **已接入**:连续 IO 错误达此值则 store 自关(被动磁盘检测)+ 触发回调;0=禁用,对齐 Ambry 默认 `MAX_VALUE`。**上线建议显式打开(3–10)**:它是"设备层 EIO → 引擎自关 → replica `ERROR` → 心跳上报 → master 再复制"这条**被动**链路的唯一开关;默认关着时,盘 hang 只能靠主动探测与 IO 看门狗(见 §12.12 与 [cluster.md §9.2 部署前提](./cluster.md))观察 | `store.io.error.count.to.trigger.shutdown` |
| `store.defer_seal_flush` | bool | `true` | — | **已接入**:滚转时旧段 flush 是否延迟到既有 flush 通道(worker 周期 `flush()`/`close()`)收敛。`true`(默认):写路径临界区不做旧段 fsync(4 GiB 满段 fsync 实测秒级停摆;64 MiB 段口径 p99 17.5 ms→0.92 ms、吞吐 +19%);持久性口径不变,flush 仍是唯一持久化边界,封存段与活跃段的掉电窗口拉平(≤ flush 间隔)。`false`:旧行为,滚点同步落盘(封存段即刻越出 at-risk 窗口,代价=写路承受满段 fsync)。flush 失败两模式都进 `io_error_count`(EIO 自关链路) | —(antfs 自有,Phase 2 滚转停摆修复) |
| `store.max_journal_entries_to_return` | u32 | 5000 | 1–10000 | journal 单次返回上限 | `store.max.number.of.entries.to.return.from.journal` |
| `store.index_persisted_entry_min_bytes` | u32 | 115 | ≥1 | 索引条目持久化最小字节(注:Rust 索引条目为 8B key + `IndexValue` 21B = **29B**,值需按实际布局重定) | `store.index.persisted.entry.min.bytes` |
| `store.segment_size_bytes` | u64 | **4 GiB** | ≥1 且 **≤ 4 GiB** | 单 log 段大小(Ambry 默认 `Long.MAX`=单巨段;antfs 取段级定容)。**4 GiB 是格式不变量而非调优旋钮**:① 段内偏移要放进索引条目 u32 的偏移半区;② "单条记录 ≤ 段 − 64 < `u32::MAX`"使记录的 u32 `data_len` 结构性无法回绕(`record_data_len` 只是第二道保险)。上界由 `StoreConfig::validate`(`MAX_SEGMENT_SIZE_BYTES`)与 `log_segment::MAX_CAPACITY` 共同强制 | `store.segment.size.in.bytes` |
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
| `store_server.compaction_policy` | String | `"hybrid"` | incremental/cost-benefit/hybrid | 调度策略(incremental=按段死占比增量/cost-benefit=最优区间/hybrid=日常 cost-benefit + 周期 CompactAll) | `store.compaction.policy.factory` |
| `store_server.compaction_policy_switch_days` | u32 | 6 | 1–365 | Hybrid 模式下两次 CompactAll 最少间隔天数 | `store.compaction.policy.switch.timestamp.days` |
| `store_server.compaction_min_log_segment_count_to_reclaim` | usize | 1 | ≥1 | cost-benefit 选出的区间净减段数 < 此值时不动手 | `store.min.log.segment.count.to.reclaim.to.trigger.compaction` |

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

### 10.4 `[store.filter]`(存在性过滤器;antfs 用 Binary Fuse 替 Bloom)
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.filter.rebuild_on_startup` | bool | false | — | 启动时重建 `.fuse`(损坏兜底) | `store.index.rebuild.bloom.filter.enabled` |

> Ambry 的 `bloom.max.false.positive.probability` / `uuid.based.bloom.filter.enabled` / `bloom.filter.maximum.page.count` 是 Bloom 专属参数;Binary Fuse 的假阳率与体积由构造固定(~1.08 B/key),**无对应项**。

### 10.5 `[store.files]`(权限 / IO 错误 / 部分恢复)
| key | 类型 | 默认 | 范围/校验 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.files.set_permission_enabled` | bool | false | — | 是否设置文件权限 | `store.set.file.permission.enabled` |
| `store.files.data_file_permission` | String | `rw-rw----` | POSIX 权限串 | 数据文件权限 | `store.data.file.permission` |
| `store.files.operation_file_permission` | String | `rw-rw-r--` | POSIX 权限串 | 运维文件权限 | `store.operation.file.permission` |
| `store.files.io_error_count_to_trigger_shutdown` | u32 | u32::MAX | ≥1 | 触发 store 关闭的 IO 错误阈值 | `store.io.error.count.to.trigger.shutdown` |
| `store.files.partial_log_segment_recovery_enabled` | bool | false | — | 尾段部分损坏时截断续恢复 | `store.enable.partial.log.segment.recovery` |
| `store.files.partial_log_segment_recovery_remaining_threshold_bytes` | u64 | 5242880 | 0–1GiB | 上者剩余数据阈值 | `store.partial.log.segment.recovery.remaining.data.size.threshold` |

## 11. 监控项清单(对齐 Ambry `StoreMetrics`)

> **状态:部分接入**。[`StoreMetrics`](../../crates/blob/src/metrics.rs) 已落地 **BlobStore 层子集**,经
> `BlobStore::create_with/open_with` 注入,按 `store="<id>"` label 区分多实例(见 [`common.md` §2.2](./common.md))。
> **已接入**:`store_request_duration_seconds{store,op}`(op = put/get/delete/flush)、
> `store_already_exists_total`、`store_compaction_reclaimed_bytes_total`、`store_compaction_duration_seconds`、
> `store_log_used_capacity_bytes`、`store_log_invalid_data_bytes`、`store_log_segment_count`、
> `store_open_bad_records_total`、`store_open_abandoned_segments_total`。
> **仍规划**:Log 的 overflow、PersistentIndex 的 recovery/find/flush 计时与 token 计数、封存段/Fuse 过滤器命中、
> BlobStoreStats 扫描等——这些打点在 `Log`/`PersistentIndex`/`IndexSegment` 内部,需把 handle 透传进这些模块后接入。
> 命名规范见 [`common.md` §2](./common.md)。裸构造器(`create`/`open`)用一个丢弃的私有 Registry(打点不导出),仅测试/无监控场景。
>
> **导出**:store-server(`crates/store`)把注入的 `Registry` 经 **`GET /metrics`**(Prometheus 文本格式,无鉴权)直出,上表引擎指标与 `StoreServerMetrics`(见 [`cluster.md` §9.1](./cluster.md))同口暴露。

### 11.1 BlobStore(顶层操作)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_request_duration_seconds` | histogram | `store`,`op`(get/put/delete/find_key…) | 秒 | 各操作耗时(合并 Ambry 的分操作 Timer) | `Store{Op}Response` 系列 |
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
| `store_open_bad_records_total` | counter | `store` | 条 | 打开副本时跳过的坏记录数(无法解码/CRC 不符);盘上字节未被删改 | 无(容错恢复,Ambry `IndexRecovery` 方向) |
| `store_open_abandoned_segments_total` | counter | `store` | 个 | 打开时因不可重同步而放弃扫描剩余部分的段数(其它段仍恢复) | 无 |

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

### 11.5 Compaction(对齐 Ambry `BlobStoreCompactor`)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_compaction_in_progress` | gauge | `store` | 0/1 | 是否正在 compaction | `CompactionInProgress` |
| `store_compaction_copied_bytes_total` | counter | `store` | 字节 | 累计拷贝字节 | `CopyRateInBytes`(Meter→counter,速率查询端算) |
| `store_compaction_reclaimed_bytes_total` | counter | `store` | 字节 | 累计回收字节 | `CompactionBytesReclaimedCount` |
| `store_compaction_duration_seconds` | histogram | `store`(实码单标签,full/partial 暂不区分) | 秒 | 单次 compaction 耗时 | `CompactionFinishedForAStoreTimeInMs`/`PartialRange…` |
| `store_compaction_cost` / `store_compaction_benefit` | gauge | `store` | — | 选段 cost/benefit | `CompactionCost`/`CompactionBenefit` |
| `store_server_compaction_policy_used_total` | counter | `policy`(incremental/cost_benefit/compact_all) | 次 | 按实际使用的调度策略计数的 compaction 执行次数 | — |

### 11.6 BlobStoreStats(死数据扫描)
| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_stats_scan_duration_seconds` | histogram | `store`,`kind`(on_demand/bucketing) | 秒 | 扫描耗时 | `StatsOnDemandScanTotalTimeMs`/`StatsBucketingScanTotalTimeMs` |
| `store_stats_scan_error_total` | counter | `store`,`kind`(scanner/queue) | 次 | 扫描/队列错误 | `BlobStoreStatsIndexScannerErrorCount`/`…QueueProcessorErrorCount` |
| `store_stats_forward_scan_entries` | histogram | `store` | 个 | 前向扫描条目数 | `StatsForwardScanEntryCount` |

## 12. 数据节点(store-server)批 1 增量

> 本节只记本批次(数据节点侧)新增/变更的**语义**与**配置/监控项**。`StoreServerConfig` 与
> `StoreServerMetrics` 的**完整**清单仍以 [`cluster.md` §9](./cluster.md) 为准(本节不重复、避免两处漂移)。
> 代码位置:`crates/store/`(心跳见 `heartbeat.rs`、复制滞后见 `replication.rs`、数据面见 `server.rs`、
> 静默见 `request_control.rs`、compaction 台账见 `compaction_jobs.rs`)。

### 12.1 心跳上报:分区实际占用 + 复制滞后估算

`ReplicaBeat`(proto tag 4/5)两个字段由数据节点填充:

| 字段 | 取值 | 口径 |
|---|---|---|
| `used_bytes` | `BlobStore::valid_data_size()` | 该分区在本副本的**实际占用**:索引赢家中未删除记录的字节(含记录头/crc);**墓碑不计**(故 ≠ `stats()` 的含墓碑 live)。**不区分过期**——过期但未回收的对象仍占盘,故计入 |
| `lag_bytes` | **估算**(见下) | 拉复制滞后字节;0 = 已追平或尚无基线 |

**`lag_bytes` 估算口径**(对齐"从 peer 拉的游标 − 本地已 apply 的游标"):

- 每轮拉复制记录 `peer_token`(本轮 peer 返回的 `FindToken`)与本地已 apply 的游标(上轮 peer 游标),
  取 `(peer.segment − applied.segment) * segment_size_bytes + (peer.offset − applied.offset)`;
  **仅 peer 在前时为正**,追平/本地在前/游标未知(`Uninitialized`)一律记 0。
- 段容量取 store 引擎配置 `store.segment_size_bytes`(`ReplicaManager::segment_size_bytes()`)。
- 该值只写进一个**诊断用原子量**(`ReplicaHandle::lag`),复制/apply 语义完全不变 → 无一致性风险。

**已知误差**(故只作倾向性观测:master 判据/看板;不参与任何一致性决策):

1. 两游标都在**peer 的 log 坐标系**里且 offset 是记录字节(含记录头/crc),比对象负载略大;
2. 段容量按配置线性化,真实段数据容量还要扣 header,且只在本地/peer **段大小相同**时成立(同配置部署
   下成立);
3. 游标语义是"最后一条已审视记录的**起点**",故差值**不含新游标处那条记录本身** → 最多偏小一条记录:
   前后记录大小相近(稳态流)时基本准确,大小悬殊时该误差不可忽略(只落后一条大记录时,报出的可能是
   上一条小记录的长度);
4. 采样点是**滞后采样**(每轮一次,反映"上一轮以来积压了多少"),轮内已追平的部分要到下一轮才归 0。

### 12.2 blob 请求延迟:按对象大小分桶(对齐 Ambry)

`store_server_blob_request_duration_seconds{op,size}` 只在**成功**路径 observe(对齐 Ambry 只在 `NoError`
计数;参数校验早退、404/409/500 等错误路径**不**计入),put 用**请求 body 长度**、get 用**返回数据长度**分桶:

| 桶 | 边界(上界开区间) |
|---|---|
| `small` | `len < 50 KiB`(`SMALL_BLOB_MAX_BYTES = 50 * 1024`) |
| `medium` | `50 KiB ≤ len < 1 MiB`(`MEDIUM_BLOB_MAX_BYTES = 1024 * 1024`) |
| `large` | `len ≥ 1 MiB` |

边界值本身落到更大的桶(50 KiB 算 `medium`,1 MiB 算 `large`);常量与分桶函数在
`crates/store/src/metrics.rs`,边界有单测守护。与全量 `store_server_request_duration_seconds{op}` 的关系:
后者覆盖所有 op/所有出口(含早退),前者只覆盖 put/get 的成功出口并多一个 size 维度。

### 12.3 RequestControl(节点级请求静默,对标 Ambry `RequestControl`)

运维可在**节点级**或**分区级**临时静默 3 类数据面操作(`put`/`get`/`delete`),
命中即 `503 SERVICE_UNAVAILABLE` + 说明文本(有 TTL 时带 `Retry-After`,秒数向上取整),**在 extractor
之前短路**(被禁的 put 连 body 都不缓冲)。被拒请求只计 `store_server_rejected_requests_total{op,reason}`,
**不进** `errors_total`、**不计** `request_duration`/`in_flight`(那三个记的是真正进到引擎的请求)。

端点(必须在豁免白名单内,否则禁用后无法远程恢复):

- `POST /admin/request-control`:body `{op, partition_id?, enable, ttl_secs?}`;未知 `op` → 400;
  指定 `partition_id` 但本地无该副本 → 404;返回操作后的当前禁用集。
- `GET /admin/request-control`:当前禁用集 `[{op, partition_id, remaining_secs}]`(已过期条目剔除)。

语义:`enable = false` 且不带 `partition_id` 时解除该 op 的**全部**静默(节点级 + 各分区,便于"一键恢复
全开");带 `partition_id` 时只解该分区。`ttl_secs` 缺省用配置默认;**`ttl_secs = 0` = 0 秒有效期(立即到期,
配合惰性判定等于不禁)**。过期判定是**惰性**的:判定/列举时比较 deadline,过期即视为启用并顺手摘除条目,
**不**新起后台清理任务;数据面判定走**读锁快路径**(禁用是少见状态,常规请求不与静默写争锁),仅在需摘除
过期条目时才升级到写锁。

**豁免清单**(绝不静默;实现见 `request_control::classify`,单测逐条覆盖 `router()` 的全部路由):

| 豁免 | 理由 |
|---|---|
| `GET /partitions/{pid}/blobs/{key}?replication=1` | 与普通 GET **同 method + 同 path**,只能按 query 区分;它是拉复制 step-2 取数据体的通道,挡掉 ⇒ 副本永久不收敛/掉数据 |
| `GET /partitions/{pid}/replica-metadata` | 拉复制 step-1(元数据游标),挡掉 ⇒ 复制停摆 |
| `GET /partitions/{pid}/segments`、`GET /partitions/{pid}/segments/{position}_{generation}` | 拉复制 bootstrap 的物理段拷贝(取段按**段身份**:认不出 → 404、畸形名 → 400),挡掉 ⇒ 空副本无法 bootstrap |
| `GET /metrics` | 运维抓取入口 |
| 任何 `/admin/**`(**含 `/admin/request-control` 自身**) | 否则做出一个"无法远程恢复"的节点 |
| 其余未知路径/方法 | 默认豁免(失败方向刻意选"宁可禁不掉,不可禁出数据问题") |

**与 Ambry 的三条刻意偏离**:

1. **豁免拉复制/运维端点**:Ambry 的 `RequestControl` 按 `RequestType` 整类禁;这里额外保证 `?replication=1`、
   `replica-metadata`、`segments*`、`/metrics`、`/admin/**` 永不被禁(持久性红线:禁用不得导致副本停更)。
2. **不持久化**:状态只在内存,进程重启即恢复全开——静默是**临时**止血手段,刻意不做成持久配置(避免
   "残留的静默"在重启/扩容后成为难查的隐性故障)。
3. **带默认 TTL 的惰性过期**:`request_control_default_ttl_secs`(默认 1800s)保证"忘了开回来"也能自愈;
   Ambry 无 TTL 概念,需人工解除。过期在判定时惰性生效,无后台任务。

### 12.4 配置项清单(本批新增)

> 完整清单见 [`cluster.md` §9](./cluster.md)。默认值/范围与代码 `impl Default` + `Validate` 逐项对齐(CLAUDE.md §7)。

| key(TOML 路径) | 类型 | 默认 | 范围/校验 | 含义/影响 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server.max_body_bytes` | u64 | 5242880(5 MiB) | ≥1 | 数据面单请求 body 上限,PUT 的主闸:**带 CL 在收 body 前先比声明值,超限直接 413(不读 body);≤ 上限一律内存整收**(裁决 #18:暂存中转已退役);无 CL 的 chunked 回落走缓冲,同受此限(超限 413)。**每请求内存上界 = 该上限,聚合 = 在途请求数 × 上限 —— 调大即同步放大内存峰值**。**默认 5 MiB > router 默认恒切片宽 4 MiB**(2026-10-22 所有者显式指示,分体裸默认自洽;
  原"2 MiB 对齐 axum `DefaultBodyLimit` 默认值"的理由作废),要收真正的大 blob 仍须显式调大 | — — **对齐红线(2026-10-22)**:必须 ≥ 网关恒切片宽与 router 恒切片宽,否则每片 413;分体 TOML 无静态交叉校验,错配在运行期由 router `body_too_large` 错误家族显式报(见 router.md 错误表),devcluster 一体配置另有启动期断言 |
| `store_server.request_control_default_ttl_secs` | u64 | 1800 | ≥0(0 = 不设默认时长) | `/admin/request-control` 未指定 `ttl_secs` 时的默认禁用时长(秒);**0 = 永久禁用**(须显式开回) | 偏离 Ambry(其 RequestControl 无 TTL),见 12.3 |
| `store_server.scrub_max_batch_bytes` | u64 | 67108864(64 MiB) | ≥1 | `verify`(手动端点与后台巡检共用)单次扫描的**读取字节上限**;到点返回部分结果 + `truncated` + `end_cursor` | — |
| `store_server.scrub_background_enabled` | bool | **false** | — | 后台一致性巡检(bit-rot 蜗牛爬)总开关;关 = 与本批之前**逐字节等价**(零后台 IO)。开启后每 tick 对一个候选副本做容错深扫,只报警不修复(§12.13) | — |
| `store_server.scrub_background_interval_secs` | u64 | 60 | ≥1 | 后台巡检两 tick 之间的**基础间隔**(秒);实际睡眠叠 ±25% 抖动去相位。单 tick 批预算 = `scrub_max_batch_bytes` | — |
| `store_server.scrub_background_max_in_flight` | u32 | 1 | ≥1 | 全节点**跨盘**同时扫描的 batch 数上限(默认 1 = 同一时刻只有一块盘在爬);**同盘**内由每盘有界池(D1)自行串行,池饱和退避跳轮 | 家风同 master `balance_max_in_flight` |

### 12.5 监控项清单(本批新增)

> 完整清单见 [`cluster.md` §9](./cluster.md)。命名/label 与注册处(`metrics.rs`)逐项对齐(CLAUDE.md §7)。

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server_blob_request_duration_seconds` | histogram | `op`(put/get),`size`(small/medium/large) | 秒 | **成功** blob 请求延迟,按对象大小分桶(边界 50 KiB / 1 MiB;GET 的窗口读按**窗口长**分桶);错误/早退不计数 | `ServerMetrics` 的 small/medium/large blob 分桶计时器 |
| `store_server_rejected_requests_total` | counter | `op`,`reason`(目前仅 `disabled`) | 次 | 被 RequestControl 静默拒掉的请求数(在进引擎之前) | Ambry `RequestControl` 的静默拒绝计数 |
| `store_server_replicas_dropped_total` | counter | — | 次 | 收到 `DROPPED` 指令后**实际删除**本地副本数据的次数(见 §12.6);重复指令被幂等忽略,**不**计数 | — |
| `store_server_replication_paused` | gauge | — | 0/1 | 运维是否暂停本节点主动拉复制(1 = 暂停);见 §12.7 | — |
| `store_server_compaction_jobs_total` | counter | `state`(running/done/failed) | 次 | 异步手动 compaction 任务的提交/完成/失败计数(每个任务在 running 与 done/failed 各计一次);见 §12.8 | — |
| `store_server_scrub_background_batches_total` | counter | — | 次 | 后台巡检完成的扫描 batch 数(每 batch = 一次 `verify_from`,预算 `scrub_max_batch_bytes`);见 §12.13 | — |
| `store_server_scrub_background_bytes_total` | counter | — | 字节 | 后台巡检累计读取量(预算口径);配合 batches 可观测爬速 | — |
| `store_server_scrub_background_bad_records_total` | counter | — | 次 | 后台巡检发现的坏记录数(含 index_mismatch;与手动 verify 的 `store_server_verify_bad_records_total` 分开计数,分得清"机器发现/人发现");发现同时打 WARN 日志 | — |
| `store_server_scrub_background_paused` | gauge | — | 0/1 | 后台巡检循环被 `/admin/scrub-control` 掐停(1 = 暂停;不持久化,重启即恢复);见 §12.13 | — |

> 心跳的 `used_bytes` / `lag_bytes` 是**控制面 proto 字段**(`ReplicaBeat`),不是本节点 Prometheus 指标,
> 故不进上表;口径见 §12.1。
>
> 下三节(A6 副本删除 / A7 复制开关 / A3 异步 compaction)只新增上表那 3 个监控项,
> **不新增任何配置项**(复制开关与 compaction 台账都是端点驱动的运行期状态,刻意不进配置)。
>
> 后续批次新增的副本实然态监控项见 §12.9(`store_server_replicas_bootstrapping`);同样不进配置。

### 12.6 副本删除:`DROPPED` 指令的语义(幂等、只在 master 确认后)

master 下发 `Transition{partition_id, disk_id, target_state: "DROPPED"}` 的语义是
**「该副本已从集群 placement 正式移除,且继任者已 ACTIVE」**——这是"可以删本地数据"的**唯一触发条件**;
master 侧何时产生该指令由 master 决定(见 [`cluster.md`](./cluster.md) 与 `openspec/changes/master-reconcile/`),
**proto 不变**。三条指令的本地动作:

| 指令 `target_state` | 本地动作 | 盘上数据 |
|---|---|---|
| `ACTIVE` / `BOOTSTRAP` | `ensure_replica`(建/开副本) | 创建(实然态初值一律 `BOOTSTRAP`,追平后由复制器转 `ACTIVE`,见 §12.9) |
| `OFFLINE` | `remove_replica`:只摘内存句柄 | **保留**(副本可能再回来) |
| `DROPPED` | `drop_replica`:摘句柄 + 删整目录 | **删除** |

`ReplicaManager::drop_replica(pid) -> Result<bool>`(`bool` = 是否真的删了):

- **幂等**:本地没有该副本/目录已不在 → `Ok(false)`,**不报错**(重复、重放的 `DROPPED` 必须安全);
  真的删了 → `Ok(true)` 并计一次 `store_server_replicas_dropped_total`(`false` 不计)。
- **删的就是整目录**:`<盘目录>/partition_{pid}` 一次 `remove_dir_all`(`NotFound` 视为成功 —— 竞态下已被删掉也算达到目的)。
  定位方式:句柄在 → 按该副本的 `disk_id` 找盘;句柄已被 `OFFLINE` 摘掉(盘上数据还在)→ 按盘列表找残留目录。
  两条路径都覆盖,不留孤儿副本目录。删除前后各记一条 `tracing::info!`(pid + 目录)。
- **先摘句柄再删**:句柄持有 `BlobStore` 的段文件 fd/mmap,反过来会让已删除的文件仍被句柄钉住
  (Unix 下 inode 到最后一个引用关闭才释放)。
- **等在飞持有者退出(有界 2s)再删**:摘出 map 后不会再有**新**持有者,但**老**持有者(在飞请求、
  本地 worker 的 flush/compaction 快照)各自 clone 过 `Arc<BlobStore>`,它们写盘的 `create_dir_all`
  (索引水位、段封存)能把刚删掉的目录**复活成只剩 `index/` 的空壳**;空壳会让该分区日后在本节点
  **再也建不起来**(`ensure_replica` 走 `BlobStore::open`,空壳里没有 log 段 → `Corrupt`)。
  故先摘句柄、轮询等 `Arc` 计数归 1(通常几毫秒)后删目录。超过 2s(如正在跑的长 compaction)照删不误,
  **不无限等** —— 本函数跑在阻塞池里,等太久会拖住心跳 loop,而残留空壳只是小概率的可运维问题。
- **不阻塞心跳**:删除(同步 IO + 上述等待)一律经 `spawn_blocking` offload;失败只记 `warn`,不影响后续指令处理。

### 12.7 运行期暂停拉复制(复制开关)

| 端点 | 请求 | 响应 |
|---|---|---|
| `POST /admin/replication-control` | JSON `{"paused": bool}` | 200 `{"paused": true\|false}`(回显操作后状态) |
| `GET /admin/replication-control` | — | 200 `{"paused": true\|false}` |

语义:

- **只停"我拉别人"**:后台拉复制器每 tick 读一次开关,暂停即**跳过本轮的主动拉取**(不做 peer 发现、不发拉取请求)。
- **"别人拉我"不受影响**:`GET /partitions/{pid}/blobs/{key}?replication=1`、`replica-metadata`、`segments*`
  照常服务(复制读端点是**入站**通道);心跳、本地维护 worker、数据面 put/get 同样不受影响。
- **与 RequestControl 正交**:静默是"拒入站请求",本开关是"停出站拉取";两者互不代偿。两个端点都在 `/admin/**` 下,
  因而都在 RequestControl 的豁免白名单内(见 §12.3)。
- **不持久化**:状态只在内存,进程重启即恢复拉取(与静默开关同一取向,避免"残留的暂停"变成难查的隐性故障)。
- **游标保留**:暂停不重置已积累的复制游标,恢复后从原游标继续拉(可能一次补上暂停期间的全部增量)。
- 状态变化时 `store_server_replication_paused`(0/1)同步更新,暂停/恢复各记一条 `tracing::info!`。

### 12.8 手动 compaction 的异步任务(台账)

| 端点 | 请求 | 响应 |
|---|---|---|
| `POST /admin/partitions/{pid}/compact?async=1` | 无 body | **立即** 200 `{"job_id": <u64>}` |
| `POST /admin/partitions/{pid}/compact`(不带 `async` 或 `?async=0`) | 无 body | (同步,行为/响应体**不变**)200 `{live_bytes,dead_bytes,segments_before,segments_after}` |
| `GET /admin/compactions` | — | 200 `{"jobs":[...]}`(最新在前) |
| `GET /admin/compactions/{job_id}` | — | 200 单条 job;**未知(或已被淘汰)`job_id` → 404** |

job 形状(未完成字段**不出现在 JSON 里** —— 是缺 key,不是 `null`):

```json
{"job_id": 3, "partition_id": 0, "state": "running", "started_at_ms": 1758000000000}
{"job_id": 3, "partition_id": 0, "state": "done", "started_at_ms": 1758000000000, "finished_at_ms": 1758000000123,
 "result": {"live_bytes": 1048576, "dead_bytes": 4096, "segments_before": 2, "segments_after": 1}}
{"job_id": 4, "partition_id": 0, "state": "failed", "started_at_ms": 1758000000000, "finished_at_ms": 1758000000009,
 "error": "io: ..."}
```

台账语义:

- **进程内内存态**:重启即丢;`job_id` 自增(从 1 起),只保留**最近 32 条**(超出淘汰最旧,含仍在跑的那条
  —— 其终态只是写不回表,不影响 compaction 本身,故 `done`/`failed` 的计数仍会 +1)。
- **只记异步任务**:同步 compact 不产生 job(它直接返回统计),故无需为同步路径维护台账。
- `result` 与同步响应**同字段**(同一 DTO 定义,避免两处漂移);`state` 取值 `running`/`done`/`failed`。
- 提交时计 `store_server_compaction_jobs_total{state="running"}`、完成/失败时分别计
  `{state="done"}`/`{state="failed"}`(各一次);每次提交也都计 `store_server_manual_compactions_total`。
  job 失败同时进 `store_server_errors_total{kind}`。
- `?async=` 的取值口径与 `?replication=` 一致(`非 0` 才算异步);无本地副本 → 404(与同步路径同码)。

### 12.9 副本实然态:`BOOTSTRAP → ACTIVE` 的追平判定

副本的实然态(`ReplicaBeat.current_state`)有三个取值,**建/开副本时一律从 `BOOTSTRAP` 起**,
只有真追平才转 `ACTIVE`:

| 取值 | 何时置位 | 含义 |
|---|---|---|
| `BOOTSTRAP` | `ReplicaManager` 建/开/重装副本时的**初值** | 尚未追平:新建空副本、重启重开的副本、物理 bootstrap 之后(活跃尾还要靠增量拉补) |
| `ACTIVE` | **仅**拉复制路径判定追平后(`sync_round` → `ReplicaHandle::mark_active`) | 已拉到 peer log 末端,可作数据源/可依赖 |
| `ERROR` | 引擎 IO 自关 hook 被动置位(S2) | IO 故障;**不被追平覆盖**(否则 master 的"再复制"判据会漏掉故障副本) |

**为什么这是数据安全问题**:master 侧搬迁/evacuation 的删源闸门是"继任者已 `ACTIVE`"
(`reconcile::plan_move` / `drop_confirmed`,守 CLAUDE.md §3.1 先补后删)。若新副本一出生就自称
`ACTIVE`,闸门瞬间成立 → 源副本被摘除 + 下发 `DROPPED` 删本地数据;源若是当时**唯一有数据**的副本
(例如它的增量拉复制正好被暂停/对端不可达),就是丢数据。故这里如实上报"我还没追平"。

**追平判据**(**逐 peer、每轮**判;代码 `crates/store/src/replication.rs` 的 `caught_up_with_peer` /
`local_has_nothing_past`,与 `sync_round` 同一入口,生产复制器与测试共用):

| 情形 | 判定 |
|---|---|
| peer 对 `replica-metadata` 返回 **404**(该 peer 上没有这个分区的副本) | **满足**:peer 没有该分区数据,无数据要同步(仅当本轮**也没从它身上拉到东西**;拉到一半对方把副本删了 → 保守不追平) |
| peer 正常应答且本轮**没有待 apply 的条目**(拉到空条目批),且**本地 log 末端与 peer 报的游标一致** | **满足**(两侧都空也算一致) |
| **本轮游标被重定位**(响应的 `token_reset` 非空)**或本轮发现对端换了一代副本**(incarnation 变化) | **不满足**:那种"末端一致"是在**刚作废的坐标系**上算出来的(守卫见 §9.2)→ 下一轮从对端给的新游标继续 |
| 其余(还有条目要 apply、本地落后于 peer 游标、拉取报错) | **不满足** → 保守留在 `BOOTSTRAP`,下一轮再看 |

与**某个** peer 的一轮满足判据即可转 `ACTIVE`(不等全部 peer;随后单向、不回退)。生产复制器每 tick
对该分区的**所有**可达 peer 各拉一轮,"追平到其中一个"通常就意味着最短路径已经走完。

"本地 log 末端与 peer 报的游标一致"的实现口径:把 peer 的游标(段位置 + 段内偏移)交给本地
`BlobStore::has_content_after` —— **纯位置扫描**:游标处那条记录**本身**不算"之后的内容"(游标是
exclusive 的),"之后什么都没有"即一致。**不校验段身份**:peer 游标里的段身份属于**对端坐标系**
(本地段位置/代次与对端并不一一对应),拿它进 `find_entries_since` 会被重定位判定误伤,故这里专门用
这个不触发重定位的探测。三道守卫:本地一条记录都没有时,只有 peer 游标也是"空"(`Uninitialized`)
才算一致;peer 游标为空而本地非空 ⇒ 保守不判追平;本地读失败一律按"还有内容"处理(不判追平)。

> **已知边界**(如实记录):store 只看得到"联系得上的 peer"的应答,分不清"整个分区真的空"与
> "唯一联系得上的 peer 恰好也空、而持有数据的副本暂时不可达"。故本地也空时会对空 peer 放行
> (否则全新空分区永远无法转正)。这一层的兜底在 master 的 placement 级闸门(`up` 过滤、健康副本
> 数 ≥ RF、源仍持有),store 侧只负责**如实**报自己追平了没有。

四个场景的落点:

1. **全新空分区**(分区刚建、所有节点都空):peer 无该副本(404)或双方都空 → 立即 `ACTIVE`。
   否则空分区会永远卡在 `BOOTSTRAP`(master 侧看着像"无健康副本")。
2. **物理 bootstrap 的新副本**(`install_bootstrap`):只拷了封存段,活跃尾还要增量拉 ——
   拷完**仍是 `BOOTSTRAP`**,增量拉那一轮判定追平才转 `ACTIVE`。
3. **重启后重开的副本**(`dir.exists()` → `BlobStore::open_with`):盘上数据可能是旧的,**故意**先报
   `BOOTSTRAP`,等一轮拉复制确认追平再 `ACTIVE` —— 重启期间短时显示"同步中"是**诚实状态**。
   它**不会**触发 master 的再复制风暴:master 算"要不要补副本"用的是 `up && !errored`
   (`reconcile::plan_rereplication`),不看 `BOOTSTRAP`;`BOOTSTRAP` 只进"欠副本主因"展示
   (`classify_partition` → `bootstrapping`,前端显示"同步中")。
4. **单副本分区**(RF=1 / 单节点集群):`LookupPartition` 返回的 placement 里本节点之外**没有**其他
   副本 = 没有 peer 可同步(判据的空集情形)→ 直接 `ACTIVE`。对端存在但 `up=false` 不算:那是
   "peer 暂时拉不到",保守留在 `BOOTSTRAP`。

其它语义:

- **单向**:`ACTIVE` 是持久事实,不会因为后续某轮暂时拉不到 peer 而回退(不抖动);`ERROR` 例外,
  它不被 `ACTIVE` 覆盖。
- **幂等**:重复判定通过只是重复置 `ACTIVE`,无害;状态只由创建路径(初值)与复制路径(转正)写,
  用现有 `RwLock<String>`,不新增锁。
- **心跳照旧**:`ReplicaBeat` 逐拍上报该值,只是值更真实;心跳指令 `ACTIVE`/`BOOTSTRAP` 的目标态
  行为不变(都走 `ensure_replica`——`BOOTSTRAP` 是如实上报的状态,不是要节点"进入同步模式")。
- 拒绝拉复制(§12.7)只停**主动拉**:暂停期间副本会**一直**留在 `BOOTSTRAP`(拿不到追平判定),
  这正是"先补后删"要的负例行为(源不会被误删)。

监控项(本节新增,注册处 `crates/store/src/metrics.rs`):

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server_replicas_bootstrapping` | gauge | — | 个 | 本地副本里**尚未追平**的数量(`current_state == "BOOTSTRAP"`,即"同步中");与 `store_server_replicas`(本地副本总数)同步刷新:心跳每拍、心跳指令处理后、复制 loop 每 tick 各一次 | — |

### 12.10 一致性巡检素材:`scrub-report` 与 `verify`(Wave3)

**两个只读端点**(都要求本地有该分区副本,否则 404;都在 RequestControl 的豁免白名单里):

- **`GET /partitions/{pid}/scrub-report`** → `{"partition_id","live_records","tombstones","live_bytes","sealed_segments","segments":[{"position","generation","data_end"}],"token_seg","token_off","damaged_records","abandoned_segments"}`。
  **只走索引与 mmap,不读数据体**(开销与 key 数成正比、与数据量无关),供 admin 做副本间摘要比对。
  `token_seg/token_off` = 本副本 log 末端游标(与 `replica-metadata` 同口径),两侧一致才说明该轮**可比**。
- **`GET /partitions/{pid}/verify?max_bytes=`** → `{"partition_id","records","bad_records","first_bad_offset":{"segment","offset"}|null,"index_mismatch","truncated","bytes_read","end_cursor":{"segment","offset"}|null}`。
  **会读全量数据体**(深扫),`max_bytes` 缺省取配置 `store_server.scrub_max_batch_bytes`,到点提前结束并置 `truncated`。
  预算到点时 `end_cursor` = 下一条未扫字节(`verify_from` 的续扫起点)、`bytes_read` = 本轮计入预算的读取量;
  预算内扫完则 `end_cursor` 为 `null`(自动巡检循环与手动分批运维都据此续,见 §12.13)。

**`BlobStore::verify` 的口径**(与生产读路径的关键差异):

- 逐记录读 log 并验 CRC,但**遇到坏记录"记下继续"**而不是整体失败(生产读路径仍是坏即 `Err(Corrupt)`):
  坏记录后尝试从记录头读 `data_len` 重同步到下一条;头也读不出则本段止步并置 `truncated`。
  这样运维能拿到"坏在哪个段/偏移"(`first_bad_offset`),而不是只知道"这副本坏了"。
- `index_mismatch`:对**活记录**(索引当前赢家)比对索引里的 `size`/`flags` 与实际记录值,
  不一致即计 1。**只能发现"索引与 log 对不上"这一类漂移;发现不了"索引指向了 log 中不存在的 offset"**(需反向遍历索引,属后续增强)。
- `BlobStore::index_stats()`(scrub-report 的数据源):`live_records`/`tombstones`/`live_bytes`(活记录数据字节,不含记录头/CRC)/`sealed_segments`。

**能发现什么 / 发现不了什么**(务必与运维对齐预期):

| 手段 | 能发现 | 发现不了 |
|---|---|---|
| `scrub-report` 摘要比对 | 副本整体落后/缺失(记录数、字节数、墓碑数、段数不同) | 具体哪一个 key 不同 |
| 元数据窗口 key 级比对(admin 侧) | 某副本缺 key、标志位不一致 | 窗口外的老 key(受 `max_bytes` 限制,报告里以 `partial` 标注) |
| `verify`(deep) | **本地静默损坏**(CRC 坏)、索引与 log 漂移 | 数据"内容不同但长度/CRC 都对"的情形;跨副本字节级差异 |

**假不一致**:扫描期间有并发写时,两侧窗口末端本就不同 → 报告会带各副本 `token`/`bytes_read`,
`partial` 为真时结论只作"部分比对";判为不一致后建议**重跑一次**再定论。

**带伤启动(容错恢复,已实现)**:记录损坏**不再让副本起不来** —— `BlobStore::open` / `open_rebuild_index`
走容错扫描(索引重建、段字节核算、有效数据核算三处都容错):

- 坏记录**跳过并记录**(可读的其余记录照常进索引、照常可读可写);
- 能从头读出可信 `data_len` 就重同步到下一条;长度字段也坏了(或跳过会越过段尾)则**放弃该段剩余部分**
  并计入 `abandoned_segments` —— 该段之后的**其它段仍会被恢复**;
- **不删、不改写、不截断**盘上任何字节(保留证据);
- **大声报错**:每条坏记录一条 `error!` 日志 + 打开时一条汇总 `error!`;损伤进
  `BlobStore::open_damage()` → 经 `GET /partitions/{pid}/scrub-report` 的
  `damaged_records` / `abandoned_segments` 暴露给运维 → **admin 的一致性巡检直接把它算成摘要级差异**
  (健康副本这两个值必为 0);指标 `store_open_bad_records_total` / `store_open_abandoned_segments_total`。

残留边界(如实记录):

- 带伤副本仍会被 master 当作正常副本计数(心跳不携带损伤信息);
- **逻辑数据可能自愈,但字节损伤不会**:拉复制比对 peer 的元数据窗口时,若 peer 仍持有该 key,
  会把它重新 apply 到本地(实测:重启后一个小分区里坏掉的那条被拉复制补回),于是**逻辑上恢复完整**;
  但盘上那串坏字节**不会被改写或删除**,`verify` 永远能看到它(`bad_records ≥ 1`),
  `open_damage`/`scrub-report` 的 `damaged_records` 也保留"曾带伤启动"的记录 —— 两者都如实呈现,不互相掩盖。
- 若 peer 也已丢失该记录(或损坏点落在 token 游标之后、不在拉取窗口内),则该 key **不会自愈**,
  需人工处置:先 `verify` 定位,再用 `RepairPartition`/`MoveReplica` 把副本迁走或让 master 重新补齐,
  之后下线该节点上的这份数据。

**指标**(§12.5 同步登记):`store_server_verify_runs_total`(counter)、`store_server_verify_bad_records_total`(counter)、
`store_server_verify_duration_seconds`(histogram)、`store_server_scrub_reports_total`(counter)。
后台自动巡检的独立计数(`store_server_scrub_background_*`)见 §12.13。

**配置**(§12.4 同步登记):`store_server.scrub_max_batch_bytes`(u64,默认 64 MiB,≥1)= verify 缺省扫描读取上限;
后台循环的三个开关/节拍/并发旋钮(`scrub_background_*`)见 §12.13。

### 12.11 盘维度打点 + 探测单飞(盘 hang 隔离 Phase 0)

> # ⚠️ 地基性变更:引擎 IO 的线程模型(每盘有界池)+ 磁盘探测守卫
>
> 本节记录一次**触及全节点数据面**的调整,不是普通优化:它改变了"谁在什么时候替我们执行磁盘 IO"。
> 改动前,**所有盘的引擎 IO 挤在同一个进程级阻塞池**(tokio `spawn_blocking`,默认 512 线程,全仓无一处
> 配置),且 `spawn_blocking` **不可取消**、卡在内核 D 态的线程**不可回收** —— 一块盘挂死就能把池吃满,
> 于是**连健康盘上的请求也一起排队**(节点级不可用)。改动后,**每块盘拥有自己的 worker 线程 + 有界队列**,
> 池满则短暂排队后快速失败(503),一块盘的损伤被限制在它自己的线程里。
>
> **一句话**:把"一块盘的故障"从**节点级**收敛到**盘级**;不改变任何持久化语义(引擎仍是同步阻塞 IO)。
>
> **必读**:配置项/监控项/测试证据/**残留风险**都在下面各节显式列出(残留风险那节请务必看完再上线)。

单进程多盘部署里,一块盘 hang 住(内核 D 态)害的不只是自己:引擎 IO 全是同步阻塞,在落地每盘池之前它们
**共用同一个进程级阻塞池**(tokio `spawn_blocking`,默认 512 线程,本仓库不配置它),卡住的线程**收不回来**
(D 态不可取消、`spawn_blocking` 不可取消),池满之后连健康盘上的请求也一起排队——本节自「一」起已把每盘
有界池**本批落地**(历史教训为它付了学费);健康闸门 / IO 看门狗 / 坏盘撤离与完整证据见
[`openspec/changes/store-disk-isolation/`](../../openspec/changes/store-disk-isolation/)。

**一、每盘有界池(线程模型;⚠️ 本节的核心改动)**

**改了什么**:引擎 IO 不再提交给进程级 `spawn_blocking`,而是提交给它所在的**那块盘自己的池**
(`crates/store/src/disk_pool.rs`):每盘 `pool_size` 条专属 worker 线程(线程名 `antfs-disk-<盘id>-<i>`)
+ `pool_queue_size` 个排队位;准入 = "拿到一个空位",没空位就最多等 `pool_acquire_timeout_ms`,超时即
调用方快速失败。

| 维度 | 改动前 | 改动后 |
|---|---|---|
| 执行者 | 全节点共享的 tokio 阻塞池(默认 512,不可配置) | **每盘自己的** worker 线程(默认每盘 16,可按盘指定) |
| 单盘 hang 的后果 | 线程被逐个占死 → **全节点**(含健康盘)请求一起排队 | 只占该盘的 `pool_size` 条线程;该盘后续请求 503,其它盘**无感** |
| 池满时 | 无界排队(tokio 阻塞池没有拒绝语义)→ 请求吊到客户端超时 | **短暂排队**(默认 200ms)→ 超时 **503 + `Retry-After`** |
| 线程能否回收 | 不能(卡在 D 态) | 仍不能,但**上限被限定在该盘内**(默认 16 条) |
| 任务 panic | join 失败 → 500,且旧实现下**一条 worker 线程被带走**(永久减并发) | panic 被逐任务接住:worker 存活、计数不漂、许可不漏,调用方得 500 + `join_error` |
| 线程生命周期 | —— | 池析构 ⇒ worker 自行退出(有测试守着) |

**准入语义的边界(刻意如此)**:超时只约束"等空位";一次**已准入**的操作**不再设超时** —— 盘慢不慢该由
健康闸门/看门狗(`store-disk-isolation` D2/D3)判定后让**新**请求快速失败,而不是把一次已经开始的写中途判死。

**谁在池上、谁不在**(如实列出,别以为"全都进了"):

| 路径 | 走哪 |
|---|---|
| 数据面 put / get / delete / metadata / replica-metadata / segments / verify / scrub-report / 手动 compact | ✅ 该分区所在盘的池 |
| 本地 worker:flush(逐 replica 并发提交)、compaction(顺序提交) | ✅ 该副本所在盘的池 |
| 拉复制每轮(本地盘读写) | ✅ 该 pid 所在盘的池 |
| **健康探测**(write/read/unlink)、**盘容量 `statvfs`** | ❌ 仍走进程级池,但**每盘至多一发在飞**(见「二」)—— 让探测去抢请求的池,等于坏盘把探测器一起拖进去 |
| **建副本前的目录创建**(`ensure_replica`) | ❌ 仍走进程级池(建之前还不知道盘 id)。**残留风险**,见下 |
| tonic / axum / 各客户端内部 | 与存储无关 |

**测试证据**(`cargo test -p antfs-store`;每条都带失败时限 —— 坏了当场红,不挂死):

| 用例 | 守什么性质 |
|---|---|
| `disk_pool::tests::runs_on_a_pool_thread_and_closes_the_loop` | 任务确实跑在池线程上(线程名带盘 id),收尾后计数归 0 |
| `workers_run_tasks_in_parallel` | N 个 worker **真并行**(防"被一把锁串起来"导致吞吐腰斩) |
| `capacity_is_exactly_workers_plus_queue_and_rejection_leaks_nothing` | 容量**恰好** = workers+queue;运行期计数可见;被拒不占容量 |
| `rejection_never_consumes_capacity` | 连拒 10 次后容量仍是 workers |
| `queue_is_fifo` / `queue_absorbs_a_burst_when_worker_frees_up` / `queue_full_waits_then_rejects` | 队列 FIFO;**等到** vs **等满超时才拒**(既不是一律拒,也不是无限排队) |
| `rejection_is_bounded_not_unbounded_queueing` | 拒绝可以**立刻**发生(不退化成无限排队) |
| `a_saturated_disk_does_not_block_another_disk` | **本改动存在意义的直接证明**:A 满,B 照常且 B 的队列始终为 0 |
| `each_disk_owns_its_own_named_threads` | 线程按盘分开且可辨认(排查"哪个盘的线程卡住了") |
| `panicking_job_neither_kills_the_worker_nor_drifts_the_counts` / `repeated_panics_do_not_degrade_the_pool` | 任务 panic **不打死 worker**、**不漂计数**、**不漏许可**(50 连炸后并发度不变) |
| `admitted_job_is_not_killed_by_the_acquire_timeout` | 准入之后不再有超时(把语义边界钉住) |
| `unknown_disk_is_rejected` / `job_payload_can_be_non_copy` | 未知盘明确失败;任意 `Send` 载荷(非 Copy)原样取回 |
| `worker_threads_exit_when_pools_are_dropped` | **线程随池析构退出**(反复建销 store-server 不攒线程) |
| `many_concurrent_submitters_lose_nothing` | 200 并发提交结果一个不丢、不错位 |
| `closing_a_pool_fails_queued_waiters` | 池关停时排队中的请求立刻得到明确失败(不永远吊着) |
| `worker_allocation_prefers_per_disk_size_then_default` / `zero_per_disk_pool_size_is_clamped_to_one` | 每盘参数优先、其余走每盘默认;0 被钳到 1 |
| `data_plane::disk_pool_sizing_from_config_is_wired_through` | 配置 → 池 → 指标在**真装配**里闭环 |
| `data_plane::saturated_disk_pool_returns_503_with_retry_after` | 用户可见面:**503 + `Retry-After`** + 指标 + 放行后恢复 |
| `worker::saturated_disk_pool_makes_worker_skip_not_hang` | 池满时 flush/compaction **跳过而不是卡住整轮** |
| `server::pool_reject_maps_overload_to_503_and_job_panic_to_500` | 过载 503 vs 任务 panic 500(+ `join_error` 计数) |
| `health::tests::skipped_round_keeps_the_previous_verdict` / `late_verdict_is_adopted_on_the_next_round` / `probe_round_covers_every_disk` | 探测"跳过轮不覆盖健康值"、"迟到结果被采纳"、"每块盘都被探到"(见「二」) |

**收益:结构性收益与吞吐收益分开说**

- **结构性(已被上表测试证明)**:单盘 hang/过载不再吃共享池;失败形态从"吊到客户端超时"变成
  "短暂排队后 503";panic 不再永久削减并发;线程生命周期有界。
- **吞吐收益:未标定(如实标注)**。每盘 worker 数是本改动新引入的并发上限,默认 16 是**经验值**
  (机械盘有用队列深度通常个位数、NVMe 可 32~128)。**上线前请用 `store_server_disk_pool_queued`
  (持续排队 = 不够)与 `store_server_disk_pool_rejected_total`(开始拒 = 过大或盘真慢)标定**,
  或用 `benchmark/` 下的压测做前后对比;高并发小对象场景**不要**不看这两个指标就直接套默认值。

**残留风险(如实标注,上线前逐条确认)**

1. **建副本前的目录创建**(`ensure_replica`)仍走进程级阻塞池:坏盘上的目录操作理论上仍是共享池的
   入口;彻底归口留给 `store-disk-isolation` 的后续阶段。(原第二条"流式 put 暂存 `unlink`"的暴露面
   已随暂存路径整体退役消除 —— 裁决 #18,2026-10-22:不再有暂存文件产生,自然无 unlink 可挂。)
2. **一次已准入的操作没有超时**:盘挂死时该盘最多 `pool_size` 个请求会一直挂着,直到客户端放弃或盘恢复。
   **健康闸门(D2)与 IO 看门狗(D3)已在 §12.12 落地** —— 新请求会对被隔离的盘**快速 503**;
   但"已经进到 syscall 里的那一发"**仍然无法取消**(Rust 既不能取消 blocking 任务,也不能 kill 线程),
   这是硬边界,不假装做到了。
3. **进程退出/重启期间**:池析构会清空队列 —— 排队中的请求拿到**明确失败**(不是静默丢),但不会被执行。
4. **任务 panic 之后**引擎侧可能留下半截记录 —— 由既有崩溃恢复(CRC 扫描 + `open_rebuild_index`)兜底,
   与"进程被杀"同一路径;池只保证"线程活下来 + 计数/许可正确"。
5. **配置耦合**:`pool_default_size` 是**每盘**的值(不是总预算) —— 加盘不会改变已有盘的并发,但节点总
   线程数会随盘数线性增长(16 × 盘数),大节点请注意进程线程上限。
6. **UI**:~~admin 的「磁盘指标」页目前展示 `disk_in_flight_requests`(在飞代偿)~~ —— **已修**:admin「磁盘指标」页已接入
   真队列深度(`store_server_disk_pool_queued` 等,见 §12.12 风险表与 DEVLOG 步骤 27 补);本条仅存档演进痕迹。

**二、探测单飞(`health.rs`)**

原先每轮对每块盘发 write→read 两道 `spawn_blocking`,各自套 `timeout`。但 **`timeout` 只放弃等待,线程仍卡在
盘里**:一块坏盘每轮漏 1 个池线程(默认 60s 一轮 → 约 8.5 小时吃光 512 个),而且这与业务流量无关 —— 是探测
自己在漏;`tokio::time::interval` 默认 `Burst`(一轮超时后紧接着再跑),多块坏盘时漏得更快。现在:

- 一发探测 = 一个阻塞任务(四步合并,`unlink` 也进超时:坏盘上删除同样会挂);
- 超时后**留住句柄**(`StuckProbe`),该盘**后续轮次不再叠加探测**(`SlotAction::Skip`),只按卡住的步骤报
  `WRITE_TIMEOUT` / `READ_TIMEOUT` / `MOUNT_NOT_ACCESSIBLE` / `DELETE_ERROR`;
- 那个线程回来了则**取它的迟到结果**作为本轮观测(`SlotAction::Adopt`)—— 对抖动的慢盘,这是自动恢复的正常路径;
- **跳过期间不写 `HealthMap`**:该盘保持上一轮的坏值,绝不因"这一轮没探到"而回到 `HEALTHY`(未知 ≠ 健康)。

线程永不回来时进程内无从得知盘况(任何新探测同样会卡死在同一设备队列里),那属于内核 fail-fast / 熔断 /
重启的范畴,见该 change 的 D3 / D7。

**三、盘维度打点(纯观测,不做任何拒绝)**

`OpTimer::with_disk`(拿到副本句柄后调)把每个数据面请求归到具体盘上。这是 `HealthMap` 之外**唯一**能回答
"哪块盘在出问题"的现网信号(admin 的「磁盘指标」页,hash `#hostdisks`,就用它);`disk_slow_requests_total`
的阈值**直接复用** `disk_health_timeout_secs` —— 纯观测项不为它单开配置。

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server_disk_requests_total` | counter | `disk_id`,`op` | 次 | **落到该盘上**的数据面请求数(`rate()` = 该盘 IOPS);含随后被拒的(413 body 超限、`store_full` 等)—— 它们在 `bytes_total` 里不计,故"有 IOPS、带宽为 0"是合法读数,两列要一起看 | — |
| `store_server_disk_bytes_total` | counter | `disk_id`,`op` | 字节 | 该盘上**成功**请求的字节数(`rate()` = 该盘带宽;失败/早退不计) | — |
| `store_server_disk_request_duration_seconds` | histogram | `disk_id`,`op` | 秒 | 该盘上请求的**端到端**延迟(含阻塞池排队,与 `request_duration` 同口径) | — |
| `store_server_disk_in_flight_requests` | gauge | `disk_id`,`op` | 个 | 该盘在途请求数;真队列深度看 `store_server_disk_pool_queued`(每盘池已落地,「一」) | — |
| `store_server_disk_slow_requests_total` | counter | `disk_id`,`op` | 次 | 端到端超过 `disk_health_timeout_secs` 的请求数(超时计数) | — |
| `store_server_disk_probe_stuck` | gauge | `disk_id` | 0/1 | 该盘是否有一发探测卡在盘里未归(仅 `disk_health_enabled` 时非 0) | — |
| `store_server_disk_probe_skipped_total` | counter | `disk_id` | 轮 | 因上一发探测未归而跳过的探测轮次数(× 周期 ≈ 卡住时长) | — |

**四、其它 reactor 阻塞点(一并出清)**

盘 IO 一律不再出现在 reactor 线程上。这类调用的共同点是"在坏盘上会挂,而挂在 reactor 上就是 HTTP 工作线程
被一块坏盘押住":

| 位置 | 处理 |
|---|---|
| `health.rs` 探测的 `exists` / `write` / `read` / `unlink` | 合并进**一个**阻塞任务(超时由异步侧加,见上) |
| `heartbeat.rs` 盘容量 `statvfs`(原 `dm.refresh()` 全盘同步调用) | 逐盘单飞刷新(`spawn_refresh` / `collect_refresh` + `DiskManager::apply_statfs`);未归的盘沿用上一拍容量值,而**心跳照发**(否则"盘抽风"会被 master 读成"节点死") |
| ~~`server.rs` 流式 put 暂存 `unlink`(`StagedFile`)~~ | **已随暂存路径退役(2026-10-22,裁决 #18)**:PUT 带 CL ≤ 上限一律内存整收,无暂存文件即无此清理契约;启动 `clean_tmp_dir` 只扫各盘 `tmp/` 的历史残渣 |

**五、暂存目录可配(`store_server.staging_dir`)—— 已退役(2026-10-22,裁决 #18)**

暂存中转路径(带 CL 且 >1 MiB 的单笔先落盘暂存、引擎回读、写后删除)连同 `staging_dir` 配置项整体移除。
理由:恒切(⑧)后 router 送来的单笔 = 片宽级(默认 4 MiB),`max_body_bytes`(默认 5 MiB)盖得住 ——
暂存只剩"每片在盘上多转一圈"的纯开销(实测小对象 p50 ~1ms → ~9ms)。现在 PUT 带 CL ≤ 上限**一律
内存整收**:每请求内存上界 = `max_body_bytes`;聚合上界 = 在途请求数 × `max_body_bytes`(与旧小对象
路径同口径,调大上限即同步放大内存峰值);无 CL 的 chunked 回落路径不变(缓冲,同受上限,413 口径同)。
不再产生任何中转文件 ⇒ 无 unlink 挂死暴露面、无"暂存目录单点/空间不进容量上报"两条历史代价。
启动时 `clean_tmp_dir` 仍扫一遍各盘 `tmp/` —— 仅为旧版本升上来的部署清一次历史 `*.tmp` 残渣。
`BlobStore::put_from_reader` / `put_composed_from_reader` 仍是引擎公开流式写原语(自有测试套
`crates/blob/tests/stream_put.rs`),server 侧唯一调用点已随本机制移除。

口径边界(如实记录):

- 全是**请求级**数字,不是块设备 IOPS/带宽;引擎内部 IO(flush / compaction / 入向复制 apply)尚未归口到盘维度,
  随每盘池(N4)一并接入 —— 故 UI 上这两个读数**偏低于**设备真实值,别拿它当设备计量;
- 延迟是**端到端**(含共享阻塞池排队),不区分"盘慢"还是"排队慢";真正的 syscall 耗时要等每盘看门狗落地时
  单独立的那组指标;
- 阻塞池自身的线程数/排队深度**没有**打点:tokio 那几个 API 需要 `RUSTFLAGS="--cfg tokio_unstable"`,不为纯观测
  给全体部署加一条隐藏构建要求(取舍记录见 `crates/store/src/server.rs` 里那段注释);
- **从未探过的盘**(如默认关探测的部署)在**本节这一版**里上报的 `health` 是空串,而 master `placement.rs`
  把空串当"可用"。**这一条已被 §12.12 取代**:现在上报显式的 `UNKNOWN`,master 侧也把 `UNKNOWN` 归入
  "可放新副本"(两边必须同时改,否则默认部署一块盘都放不了)。

**配置项清单(本批新增,⚠️ 含地基性变更)**

> 完整清单见 [`cluster.md` §9](./cluster.md);默认值/校验与代码 `impl Default` + `Validate` 逐项对齐(CLAUDE.md §7)。

| key(TOML 路径) | 类型 | 默认 | 范围/校验 | 含义/影响 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server.data_dirs` | Array\<String \| Table\> | (必填) | 至少一项;**每项 basename 必须唯一**(盘 id = basename);空目录/每盘池大小 <1 均拒 | 盘声明。两种写法:**裸目录**(`data_dirs = ["/data/d1"]`,池大小走默认)或**每盘参数**(`[[store_server.data_dirs]]` 下 `dir` + 可选 `pool_size`)。basename 重复以前能安静跑起来但池/心跳/master 放置会串,现在当场拒 | — |
| `store_server.pool_default_size` | u32 | 16 | ≥1 | **每盘默认池大小**(worker 线程数):`data_dirs` 里没给自己 `pool_size` 的盘都用它。每个 worker = 1 条 OS 线程、同时最多 1 次在飞 IO;机械盘有用队列深度通常个位数、NVMe 可 32~128,故默认取 16 这个偏保守的中间值。**上线前请按盘类型标定**(见上「一」的收益一节) | — |
| (每盘)`pool_size` | u32 | (可选) | ≥1 | 该盘自己的池大小,覆盖 `pool_default_size`。例:`[[store_server.data_dirs]]` 下 `dir = "/data/nvme0"` + `pool_size = 64` | — |
| `store_server.pool_queue_size` | u32 | 32 | ≥0(`0` = 不排队,满即拒) | 每盘池的排队容量(worker 全忙时允许等待的请求数)。**有界**是刻意的:无限排队会推高延迟、占死连接,还掩盖"这块盘已经不行了" | — |
| `store_server.pool_acquire_timeout_ms` | u64 | 200 | ≥0(`0` = 不等) | 池满时的**准入等待上限**(等的是"有空位",不是"这次 IO 做完")。超时 ⇒ **503 + `Retry-After`**;短暂排队能吸收瞬时突发,又不至于把请求拖成"看起来挂了" | — |

> 探测开关/周期/超时沿用既有三项(`disk_health_enabled` / `disk_health_interval_secs` /
> `disk_health_timeout_secs`,见 §12.4 与 cluster.md §9);慢请求阈值复用其中最后一项,**不新增配置**。

**监控项权威清单**见 [`cluster.md` §9](./cluster.md)(本节同步登记)。

### 12.12 健康闸门(D2)+ IO 看门狗(D3):把"盘 hang"收敛成"那块盘被隔离"

> # ⚠️ 地基性变更:数据面新增一层"盘不可服务 ⇒ 快速失败"的闸门
>
> 本节把 §12.11 留下的最关键缺口补上:**一块盘挂死时,落在这块盘上的新请求不再排队等死,而是立刻 503。**
> 这是**行为变更**(以前这类请求会一直挂着直到客户端超时),影响全节点数据面的可达性口径 ——
> 上线前请把下面「配置」「监控」「残留风险」三节看完。
>
> **一句话**:§12.11 让"单盘故障不吃别人的线程";本节让"单盘故障**快速失败 + 可观测 + 能自愈**"。
>
> **不做的**:不改引擎 IO 模型(仍是同步阻塞 `std::fs`);不取消已进 syscall 的那一发 IO(做不到);
> 不在 store 侧自动搬副本(那是 master 侧的 D4,见 [`cluster.md` §9](./cluster.md))。

#### 一、两层判定:原始观测 ≠ 可服务性

这是整个设计的**要害**。`health.rs` 给出的是**单轮**结论("这一轮探测写超时了");直接拿它摘盘就是
"偶尔一次抽风踢掉一块好盘"(盘越忙越容易闪一下)。所以拆成两层:

| 层 | 字段 | 谁用 |
|---|---|---|
| **原始轮次判定** | `DiskStatus::verdict`(最近一次观测的结论) | 心跳上报 / 日志 / 排查 |
| **可服务性(迟滞后)** | `DiskStatus::is_available()` | **闸门唯一判据** |
| **延迟劣化(迟滞后,DEGRADED)** | `DiskStatus::is_degraded()` | 上报 + master 停止放新副本;**不用于 503** |

三档的意义:只有"可用/不可用"两档时,一块 p99 从 1ms 涨到 2s 的盘**既不会被隔离、也没人停止往它放副本**
——它就这么一直烂着(离 30s 的看门狗 deadline 还差得远)。`DEGRADED` 补的正是这一格:
**不 503**(慢不等于坏,摘盘只会更动荡)、**不撤离**(撤离只认 `UNAVAILABLE`,避免"有点慢就搬 GB 数据")、
**只做两件便宜事**——让运维看见,以及不再往它放新副本。两台迟滞机共用同一套 `Latch` 逻辑(同一个测透的实现)。

判定状态在 `crates/store/src/disk_health.rs`(纯内存、不持久化)。**重启后从"从未观测"重来**(即
`UNKNOWN` ⇒ 可服务)是刻意的:重启后没有任何观测证据说它坏,把整节点的盘全锁死才是灾难。

#### 二、迟滞参数:两个方向都给了"轮数 + 时长",取先到者

| 方向 | 判据(任一满足) | 为什么两条都要 |
|---|---|---|
| 进入 `UNAVAILABLE` | 连续坏 ≥ `disk_health_bad_rounds` 轮 **或** 坏态持续 ≥ `disk_health_bad_secs` 秒 | 只按轮数:探测周期调到 300s 时"连续 3 轮"= 15 分钟才隔离一块真死的盘;只按时长:周期调到 5s 时"持续 30s"= 6 轮,等于没防抖 |
| 复位(恢复可服务) | 连续好 ≥ `disk_health_good_rounds` 轮 **且** 从进入连续好态起稳定满 `disk_health_recover_stable_secs` 秒 | 复位比进入**更严**是刻意的:刚缓过来的盘立刻接回全部流量,大概率又被压垮(同 master 侧"盘将满 RW→RO 只置不回"的防抖精神) |

**混轮次不抖**:好轮清零"连续坏"计数、坏轮清零"连续好"计数;坏态**片段**收尾时计数一并清零 ——
否则复位后**第一个**坏轮就会凭残留计数再次摘盘(迟滞等于没有)。这三条各有单测钉住。

#### 三、上报值域:未知 ≠ 健康

`DiskBeat.health`(字符串,proto 不变)现在取 `DiskStatus::reported_state()`:

| 上报值 | 含义 |
|---|---|
| `HEALTHY` | 可服务,且最近一轮观测也健康 |
| 具体故障值(`WRITE_TIMEOUT` / `READ_ERROR` / …) | 可服务(迟滞未触发),但最近一轮如实报了故障 —— master 侧据此**先避开新放置**,闸门仍放行 |
| `UNAVAILABLE` | **迟滞判定后的隔离态**(派生态压过具体故障值):闸门 503 |
| `UNKNOWN` | **从未有过**有效观测(刚启动 / 探测未启用)。**绝不报空串、绝不报 `HEALTHY`** |

新增一个故障值 **`IO_TIMEOUT`**:来自看门狗(不是某一轮探测的具体步骤),故不并进
`WRITE_TIMEOUT`/`READ_TIMEOUT` —— 那会谎报"我们在写/读",而实际超期的可能是 flush、compaction 或 `stat`。

> **⚠️ 联动改动(不做就是回归)**:store 侧把"没探过"从**空串**改成 `UNKNOWN` 之后,master 的
> `placement.rs` 必须把 `UNKNOWN` 归入**可放新副本**的集合 —— 因为 `disk_health_enabled` **默认就是 false**,
> 默认部署里**每块盘**都报 `UNKNOWN`。若 master 把 `UNKNOWN` 当坏,**默认部署一块盘都放不了,集群建不起来**。
> 可放集合 = `""`(旧节点)| `HEALTHY` | `UNKNOWN`;`UNAVAILABLE` 与具体故障值照旧不放。
> 两侧各有单测守着(`placement::unknown_health_stays_usable_so_default_deployments_keep_working`)。

#### 四、闸门作用面:哪些被挡、哪些必须照常

| 请求 | 闸门行为 | 理由 |
|---|---|---|
| 数据面全部 `/partitions/{pid}/...`(put/get/delete/metadata/scrub-report/verify) | ✅ 盘不可服务 ⇒ **503 + `Retry-After: 1`** | 落在这块盘上的请求必然失败,排队等死毫无收益 |
| **拉复制读**(`?replication=1`)/ **`replica-metadata`** / **`segments*`** | ✅ **一样 503**(**不沿用** `RequestControl` 的豁免) | 从一块已经坏了的盘上拉数据/拉段**本来就拉不动**,放进去只是再占一条卡死的线程。对端收到 503 后按既有语义"这轮拉不到"处理,保守留在 `BOOTSTRAP`,**永不被误判为追平**(方向是安全的) |
| `/metrics` 与**任何** `/admin/**` | ❌ **永不被短路** | 与 `RequestControl` 同一条硬规则:**不能做出一个无法远程恢复的节点**。盘坏了运维更要能看见、能下发指令 |
| 本地 worker(flush / compaction) | 跳过该盘上的副本 | 否则每轮都往一块卡死的盘上再塞任务 |
| 拉复制每轮 | 跳过该盘上的 pid,**且不动游标** | 恢复后从原游标续拉(与 `/admin/replication-control` 的暂停语义一致) |

**寻盘用同一个来源**:闸门在中间件里用 `ReplicaManager::get(pid)` 定位盘 —— 与 handler 里
`OpTimer::with_disk` 是**同一个**调用,不另起一套寻盘逻辑。闸门挂在 **handler 之前**(extractor 之前),
故被拒请求**连 body 都不读**,也不进 `errors_total` / `request_duration` / `in_flight`。

**拒绝计数**:`store_server_rejected_requests_total{op, reason="disk_unavailable"}`。三个 reason 的区别务必分清:
**`disk_unavailable`** = 盘被隔离 / **`pool_full` 类**(每盘池满,见 §12.11)= 盘被压满 / **`disabled`** = 人工静默。
`op` 标签由 `request_control::path_op` 给(含 `replication_get` / `replica_metadata` / `segments` 等豁免但受闸门管的端点),
新增数据面路由必须在那里归类(有单测 `path_op_names_every_data_plane_route` 把这条钉住)。

#### 五、IO 看门狗(D3):计时点必须在闭包内

**为什么不能量端到端**:端到端时长包含"在池里排队"。盘正常但池忙时,一次 5ms 的读会被量成 2s,
于是**健康盘被判成坏盘**。这是本设计最容易写错的一处,故:

- **计时点在阻塞闭包内、紧贴 syscall 前后**(`JobTimer` 守卫,panic 展开时也照样记一次 ——
  于是有一条干净的不变量:**每个真正开跑的 job 恰好产生一个时长样本**);
- 看门狗的"最久在途"来自 `IoWatch`:每盘一个 `BTreeMap<开始时刻, 条数>`。**用多重集而不是"最早开始时刻"标量**:
  任务完成的顺序与开始顺序无关(A 先开始、B 后开始、A 先结束 —— 标量会一直停在 A 的开始时刻,把健康盘的
  "最久在途"越算越大,最后误判)。**排队中的任务不盖戳**,故排队不推高"最久在途"。
- 判据:该盘最久一发在途 IO 超过**它那个 op 的门槛** ⇒ 记 `IO_TIMEOUT` **一次坏观测**,
  照走迟滞(`bad_rounds`/`bad_secs`)—— **单次超期不摘盘**。
- **延迟劣化的观测**(`DEGRADED`)也由看门狗每轮产生:池在闭包内按 `disk_degraded_io_ms` 分类每一发 IO
  (增量计数,不是直方图差分 —— "慢 IO 占比"要精确的分子分母),看门狗**读走并清零**该盘的计数
  (`swap(0)`,保证每发 IO 只被**一轮**统计到),算占比,喂给延迟迟滞机。
  **本轮一发 IO 都没有 ⇒ 不产生结论**(空闲 ≠ 慢),与探测"跳过轮不覆盖健康值"同一条纪律。
- **门槛按 op 分档**(`disk_watchdog_ms_by_op`,默认空 = 全部用 `disk_watchdog_ms`):compaction / flush 要
  拷整段,天然比一次 4KiB 读**慢几个数量级**;共用一个门槛时只有两个坏选择 —— 要么门槛大到"真死的盘
  半天不被隔离",要么小到"健康盘的 compaction 天天误报"。分档后每个 op 用自己那把尺子。
  `IoWatch` 因此按 `(disk, op)` 分层记账,看门狗**逐 op** 检查;一块盘上多个 op 同时超期只记一次
  "这块盘出了问题"(`tripped` 按盘去重),但每个 op 各计一次 trips(看得出是哪类 IO 在超期)。
  看门狗的 tick 周期取**所有门槛的最小值**,故给某个 op 设更紧的门槛不会被默认周期拖慢。
- **看门狗是独立任务**:不在任何盘的池里跑(否则它自己会被同一批卡死的线程挡住),
  且**不受 `disk_health_enabled` 约束** —— 默认部署(探测关着)才是绝大多数,若它也跟着关,
  `UNAVAILABLE` 就再没有第二个来源,整条链路在默认配置下全哑。
- tick 周期就用 `disk_watchdog_ms` 本身(不新增旋钮)⇒ **最坏发现延迟是它的 2 倍**,别按"30s"理解 SLA。

**诚实边界(不假装做到)**:已经卡在 syscall 里的那一发 IO **取消不掉**(Rust 既不能取消 blocking 任务,
也不能 kill 线程)。看门狗把"泄漏 + 无人知晓"变成"泄漏 + 已知 + 有界决策",它**不解决泄漏**;
"让在途请求快速失败"只对**排队/后续**请求成立。

#### 六、逃生舱:真挂死 ⇒ 进程自杀(默认关)

某盘池**全部 worker 都被超期 IO 占住**且最早那一发已超过 `disk_watchdog_escalate_secs` ⇒ `std::process::abort()`,
交 supervisor(`systemd Restart=always` / `docker-compose restart: always`)重启。

- **默认 0 = 永不升级**(CLAUDE.md §5.1)。为什么默认关:antfs 是**单进程多盘**(Ceph 是一 OSD 一进程),
  自杀会**带走健康盘上的服务** —— 误杀一次等于自造一次全节点故障。
- 配置校验强制 `escalate_secs ≥ disk_watchdog_ms/1000`(或 0):低于它等于"刚超期一次"就被判成
  "永远不会回来了"。
- 对齐 Ceph `osd_op_thread_suicide_timeout`;**偏离与代价如实记录**如上。开启时建议 ≥ 600。

#### 七、测试证据(`cargo test`;每条都带失败时限,坏了当场红,不挂死)

**单测(`disk_health.rs`,注入合成时刻,零 sleep)**

| 用例 | 守什么性质 |
|---|---|
| `never_observed_is_unknown_and_still_serves` | **从未观测 ⇒ `UNKNOWN` 且 ≠ `HEALTHY`,而且仍可服务**(默认部署行为不变) |
| `a_single_bad_round_does_not_take_the_disk_out` | **一次坏轮不摘盘**(这就是"别因抽风踢盘") |
| `bad_rounds_trigger_unavailable` / `bad_secs_triggers_first_when_the_interval_is_long` | 两条进入判据各自可达,且**先到者**生效 |
| `alternating_rounds_never_trip_the_disk` | 坏好交替 20 轮**一次都不摘盘**(防抖真的有效) |
| `recovery_is_stricter_than_entry` | 复位要"连续 M 轮好 **且** 稳定满 T2",缺一不可 |
| `a_bad_round_resets_the_good_streak` | 复位途中夹一个坏轮 ⇒ 连续好清零,不许"糊过去" |
| `counters_do_not_leak_across_episodes` | **复位后第一个坏轮不得凭旧计数立刻再摘**(迟滞不能因计数残留而失效) |
| `repeated_bad_rounds_while_unavailable_do_not_re_transition` | 已在坏态不重复产迁移事件(计数不被刷成噪声) |
| `observe_disk_writes_gauges_and_transitions` | 状态 gauge **整族摆平**(当前 1、其余 0);迁移只记"可服务性变了"那一次,且 `from` 是**变化前那个上报值** |
| `watchdog_round_only_touches_overdue_disks` | 只碰超期的盘;未超期的盘连一条 series 都不产生;单次超期不摘盘 |
| `repeated_watchdog_trips_take_the_disk_out_via_hysteresis` | 反复超期经**同一台状态机**累计到阈值才摘盘 |
| `escalation_requires_all_workers_stuck_and_is_off_by_default` | 逃生判据:`0 = 永不升级`;必须"每条 worker 都被占住 + 超时门槛" |

**单测(`disk_pool.rs`:看门狗的数据源)**

| 用例 | 守什么性质 |
|---|---|
| `io_watch_tracks_the_oldest_start_across_out_of_order_completion` | **完成顺序 ≠ 开始顺序**时,"最久在途"正确推进(钉死"用标量"那个反例) |
| `io_watch_is_per_disk_and_read_only_queries_do_not_leak_entries` | 按盘分开;只读查询不造空 entry |
| `queueing_is_excluded_from_the_io_watch_and_duration` | **排队不计入在途、不计入时长**(构造出"第二发至少排了 50ms",它记下的时长必须 ≪ 那个值)—— §五那个"最容易写错的地方"的守门人 |
| `a_panicking_job_still_releases_its_io_watch_entry` | panic 后不留"幽灵在途"(否则健康盘被看门狗误判),且照样产生一个时长样本 |

**集成(`tests/disk_health_gate.rs`:真 HTTP 数据面,故障注入走 `force_disk_unavailable`)**

| 用例 | 守什么性质 |
|---|---|
| `unknown_disk_state_does_not_gate` | **默认部署回归**:从未观测 ⇒ 一律放行 + `/metrics` 上显式可见 `UNKNOWN` |
| `unavailable_disk_fails_fast_on_every_data_plane_route` | 7 条数据面路由(含**拉复制读**/`replica-metadata`/`segments`)统一 **503 + `Retry-After: 1`**;`op` 标签逐个对得上;**被挡请求不进引擎**(盘维度请求数不增长、在途不残留) |
| `a_bad_disk_does_not_take_down_requests_on_another_disk` | **盘的隔离性**:另一块盘坏掉时这块盘上的读写完全不受影响 |
| `metrics_and_admin_stay_reachable_while_a_disk_is_unavailable` | **可远程恢复**:`/metrics` 与 `/admin/**` 照常 200 |
| `unknown_partition_is_still_404` | 闸门不吞 404(没有本地副本 ≠ 盘坏) |
| `recovering_the_disk_restores_service` | 迟滞**退出**方向:好轮数不够不许复位;补满后服务与数据一并回来,并留下 `UNAVAILABLE→HEALTHY` 迁移记录 |
| `background_worker_skips_the_unavailable_disk` | flush/compaction 跳过坏盘且**跳过可见**(`disk_worker_skipped_total`);健康时的基线也先证一遍 |

**其它**:`heartbeat::disk_beats_report_unknown_and_unavailable_never_a_blank_string`(上报值域闭环)、
`request_control::path_op_names_every_data_plane_route`(新增路由必须归类)、
`placement::unknown_health_stays_usable_so_default_deployments_keep_working` /
`unavailable_and_failure_values_are_both_excluded`(master 侧值域)、
`config::disk_health_defaults_match_documented_proposal` / `disk_health_validation_rejects_dangerous_values`(默认值与校验)。

**故障注入怎么做的**:`StoreServer::force_disk_unavailable(disk_id)` **不绕过任何判据** ——
它喂的是与生产探测/看门狗**完全相同**的迟滞状态机(`bad_rounds` 次合成 `IO_TIMEOUT` 观测),
只是省掉"真把一块盘弄坏"。真 D 状态在测试里造不出来,也不该在开发机上造。
生产代码里**没有**任何调用点,误用的代价(该盘请求全 503 且不会自行恢复)写在方法文档里。

#### 八、配置项清单(§12.4 / cluster.md §9 同步登记)

> 全部为**经验初值**(CLAUDE.md §5.1):采纳 `store-disk-isolation` 提案里已定的数字,实现时**没有**
> 顺手另定一个。上线前请按 [`cluster.md` §9.4 阈值标定手册](./cluster.md) 标定 —— 那里给出了每个数字的
> **测量查询与判据**,以及标定完成后必须做的三步(改代码 / 改文档表 / 改钉住默认值的单测)。
> **只能在真机上做的那些**(设备级 fail-fast、故障注入验收、告警接线)见同文 §9.5 核对清单。

| key(TOML 路径) | 类型 | 默认 | 范围/校验 | 含义/影响 |
|---|---|---|---|---|
| `store_server.disk_watchdog_ms` | u64 | 30000 | ≥1 | 单次引擎 IO 的**系统调用**时长软上限(闭包内测,不含池排队);超时 ⇒ 记一次 `IO_TIMEOUT` 坏观测。必须**远大于** p99.9 与 compaction 大块拷贝时长。**不受 `disk_health_enabled` 约束** |
| `store_server.disk_degraded_io_ms` | u64 | **0(= 关闭)** | ≥0 | **DEGRADED 的判定阈值**(毫秒):一次**闭包内**测得 ≥ 本值即记一次"慢 IO"。默认 **0 = 关**(新判定口径,先观测一周期再开)。**含义**:① 该盘仍**服务**(不 503);② master **不再往它放新副本**;③ **不触发撤离**(撤离只认 `UNAVAILABLE`) |
| `store_server.disk_degraded_ratio` | f64 | 0.1 | (0,1] | 一轮里"慢 IO"占比 ≥ 本值即算该轮劣化。为什么是占比:"偶尔一次慢 IO"在一百万次里是噪声,占比才反映"这块盘持续慢下来了"。与本项配合的是上面那个阈值(后者为 0 时本项无意义) |
| `store_server.disk_watchdog_ms_by_op` | Table\<op → u64\> | **空**(全部用 `disk_watchdog_ms`) | key 必须是已知池上 op([`KNOWN_OPS`](../../crates/store/src/disk_pool.rs));值 ≥1 | **按 op 覆盖**看门狗门槛(毫秒):给大 IO 单开一档,避免 compaction/flush 的天然长耗时误报 `IO_TIMEOUT`。覆盖值可小于默认(该 op 更紧);看门狗 tick 取所有门槛**最小值**。例:`[store_server.disk_watchdog_ms_by_op]` 下 `compaction = 300000`。**拼错的 key 会在配置校验期被拒**(否则门槛静默失效) |
| `store_server.disk_watchdog_escalate_secs` | u64 | 0(永不升级) | 0,或 ≥ `disk_watchdog_ms` 向上取整的秒数 | 某盘池全部 worker 被超期 IO 占住且持续此秒数 ⇒ `process::abort()` 交 supervisor 重启。**单进程多盘,误杀代价 = 全节点故障**,故默认关(见「六」) |
| `store_server.disk_health_bad_rounds` | u64 | 3 | ≥1 | 连续 N 轮坏 ⇒ `UNAVAILABLE` |
| `store_server.disk_health_bad_secs` | u64 | 30 | ≥1 | 与上者取**先到**:坏态持续 ≥ T 也置 `UNAVAILABLE` |
| `store_server.disk_health_good_rounds` | u64 | 3 | ≥1 | 复位要求连续 M 轮好 |
| `store_server.disk_health_recover_stable_secs` | u64 | 120 | ≥0(`0` = 只按轮数) | 复位还要求"稳定满 T2 秒" |
| `store_server.disk_health_enabled` | bool | false(**不动**) | — | **语义变更**:探测从"纯观测"变为**闸门的输入之一**。默认关 ⇒ 闸门只剩看门狗这一条输入(故看门狗**不可关**);启动时会记一条 `warn` 提醒 |
| `store_server.disk_health_interval_secs` | u64 | 60(**不动**) | ≥1 | **语义澄清**:与 `disk_health_timeout_secs` 及迟滞参数**联合**决定隔离延迟,最坏 ≈ `min(bad_rounds × interval, bad_secs + interval)`。另外探测是**逐盘串行**的,`盘数 × timeout` 超过 `interval` 时轮次会追尾,建议 `interval ≥ 盘数 × timeout` |
| `store_server.disk_health_timeout_secs` | u64 | 10(**不动**) | ≥1 | **它已被三处复用**:探测超时、拉复制 HTTP timeout、慢请求观测阈值(`disk_slow_requests`)。改它**会连带影响三处口径** |

#### 九、监控项清单(cluster.md §9 同步登记)

| 指标名 | 类型 | 标签 | 单位 | 含义 |
|---|---|---|---|---|
| `store_server_disk_io_duration_seconds` | histogram | `disk_id`,`op` | 秒 | **闭包内**测得的单次 IO 时长(syscall,**不含**池排队)—— "这盘自己快不快"的唯一正确口径。桶特意铺到 120s(看门狗 deadline 就是要看的长尾,Prometheus 默认桶 10s 就封顶了) |
| `store_server_disk_io_watchdog_trips_total` | counter | `disk_id` | 次 | 看门狗判定超期的次数(一次超期 = 一次坏观测,是否摘盘由迟滞决定) |
| `store_server_disk_health_state` | gauge | `disk_id`,`state` | 0/1 | 该盘当前**上报状态**,`state` ∈ `HEALTHY`/`UNAVAILABLE`/`UNKNOWN`/具体故障值。**整族每次摆平**(只置 1 不置 0 会让看板上出现"一块盘又健康又坏掉") |
| `store_server_disk_health_transitions_total` | counter | `disk_id`,`from`,`to` | 次 | 可服务性**迁移**次数(只有"可服务性变了"才计,原始判定变化不算)。频繁迁移 = 阈值定得太紧 |
| `store_server_disk_worker_skipped_total` | counter | `disk_id`,`task` | 次 | 因盘不可服务而跳过的后台工作(`flush`/`compaction`/`replication`)—— 让"坏盘被跳过"**可见**,否则表现为"这块盘上的副本悄悄不推进了" |
| `store_server_rejected_requests_total` | counter | `op`,`reason` | 次 | **`reason` 新增取值 `disk_unavailable`**(与 `disabled` 并列;三者区别见「四」) |

#### 十、残留风险(如实标注,上线前逐条确认)

1. ~~**只做了一半的链路**~~ **已补齐(master 侧 D4/D5/D6 已落地)**:坏盘上的副本会被**自动搬走**
   (先补后删、且继任者必须已追平才删源),判活加了迟滞(一次抖动不再判死节点),再复制加了每 tick 预算。
   详见 [`cluster.md` §7.7](./cluster.md) 与 §9。~~`DROPPED` 删目录回收、admin 页接入真队列深度与健康
   状态~~两者均已落地(`DROPPED` 删目录回收见 §12.6;admin 页接入见本节 6 的「已修」注)。**仍未做**:
   全部阈值的压测标定。
2. **阈值未压测标定**:上面 6 个数字都是经验值 —— **测量步骤与判据见
   [`cluster.md` §9.4 阈值标定手册](./cluster.md)**(全部 12 个数字一并列出,含"标定完成后必须做的三步")。
   每个数字都有单测钉着,改默认值必须显式改测试,不会悄悄漂。
3. ~~**`UNKNOWN` 参与撤离判据这件事还没法验证**~~ **已核对**(D4 落地后):`pick_evicted_disks` 只认
   `UNAVAILABLE`(且需持续 ≥ `disk_evac_min_bad_secs`),**`UNKNOWN` 不参与**撤离判据;而默认关探测的部署里
   `UNAVAILABLE` 只能由**看门狗迟滞**产出,这条链路每一环都有用例(store 侧
   `repeated_watchdog_trips_take_the_disk_out_via_hysteresis` 与
   `disk_beats_report_unknown_and_unavailable_never_a_blank_string`,master 侧撤离全链路用例)。
   **仍待真机**:物理触发(挂盘 / `dm-error`),见 [`cluster.md` §9.5](./cluster.md) B 组。
4. **已进 syscall 的 IO 无法取消**(见「五」的诚实边界):盘真挂死时,该盘最多 `pool_size` 个请求
   仍会挂着,直到客户端放弃或盘恢复;看门狗只保证"后续请求不再进来"。
5. ~~**`disk_watchdog_ms` 与 compaction 大 IO 的关系**~~ **已由"按 op 分档"解决**:
   `disk_watchdog_ms_by_op` 可以给 `compaction` / `flush` 单开一档(例如 300s / 120s),不必为了让
   compaction 不误报而把普通读的门槛一起放大。**仍待标定的是"那档该取多大"** —— 见
   [`cluster.md` §9.4](./cluster.md) 标定手册第 1、2 行(必须专门跑一次全库 compaction 看单次峰值)。
6. **看门狗口径只覆盖"该盘池里的 IO"**:`ensure_replica` 建目录、流式 put 的暂存 `unlink` 仍走共享
   阻塞池(§12.11 残留风险 1),它们不受本看门狗保护。
7. ~~**UI 未接**~~ **已接**:admin「磁盘指标」页现在展示**真队列深度**(`disk_pool_queued / disk_pool_workers`)、
   池在飞与池拒绝计数、**盘健康状态**(`disk_health_state`)、看门狗超期次数、后台跳过数,以及
   **两把 RT 尺子**(端到端 / syscall 闭包内)并排 —— 后者正是"这盘慢不慢"的判据。
   ~~仍未接:分位点(UI 只算平均,`parse.rs` 刻意极简、不聚合 `le` 桶)。~~ **分位点已做**(2026-09-24):
   `disk_json` 新增 `rt_p50_seconds` / `rt_p99_seconds`(端到端)与 `io_p50_seconds` / `io_p99_seconds`(syscall),
   页面上 RT 两列显示"平均 / p99"(p50 在 hover 提示里)。**实现口径与边界**(别把边界当缺陷,但也别忽略):
   - 分位点由 `_bucket` 的 `le` 桶**逐盘跨 op 求和**后**线性插值**得到(口径照抄 Prometheus
     `histogram_quantile`),与同一盘的 `_sum`/`_count` 是**同一批观测**;`parse.rs` 仍未改语义,
     桶的聚合在 admin 侧(`crates/admin/src/server.rs::histogram_quantile`,带单测)。
   - 最高桶是 `+Inf`:秩落在里面时**只给下界**(= 最高有限 `le`,即"真实分位点 ≥ 这个数"),
     **不声称超出最高桶的精度**。store 侧 IO 直方图的桶铺到 120s,故超过 120s 的观测只能得到"≥ 120s"。
   - **只聚合到分位点这一层,不往上传 `le` 桶**:admin JSON 里没有桶数组,前端也就无法做跨盘/跨节点合并,
     主机汇总卡的 p99 取的是**该主机最差盘的 p99**(分位点不能相加,它是主机 p99 的上界)。
   - 没有桶样本(老版本 store / 该盘零请求)→ `null` → UI 显示「—」,**不显示 0**:没数据 ≠ 零延迟。

### 12.13 后台一致性巡检:bit-rot 蜗牛爬(store-scrub-background,2026-10-19)

> 把 §12.10 的"按需巡检"升级为**常驻自动扫描**,同时绝不震荡整机。**默认关**
> (`scrub_background_enabled = false` ⇒ 现有部署逐字节等价,后台 IO 零增量)。
> 立项动机与逐条决策见 `openspec/changes/store-scrub-background/`。

**为什么需要它**:热数据读路径逐条验 CRC、追平窗口有拉复制流过 —— 两都不沾的**冷尾巴**
(没人读、已追平的段)里,坏字节没有任何机制会发现,直到一次冷读 500;更糟的是 peer 也把数据
压实丢了、自愈窗口关闭才发现。原 `balance-and-scrub` 勾过"可选 `scrub_interval_secs`"但从未落地
(勾账夸大,已修)—— 本批补腿。

**防震荡四条**(用户红线"别让整个机器震荡"的落地):

1. **同盘串行**:每批照手动 verify 端点**同一条路**进每盘有界池(`pool.run(disk, "verify")`),
   与 put/get/compaction 在一个池里排队;池饱和 ⇒ 本轮退避(debug 日志),下 tick 再试,
   **绝不重试轰炸**;
2. **跨盘总闸**:`scrub_background_max_in_flight`(默认 1)= 全节点同时在爬的盘数上限
   (家风同 master `balance_max_in_flight`);
3. **蜗牛爬节拍**:每 tick 每 lane 最多一个 batch(≤ `scrub_max_batch_bytes`),tick 间睡
   `scrub_background_interval_secs` 叠 **±25% 抖动**(去相位:恒速定时器会与 compaction 凌晨
   钟点/多节点同拍共振成周期性尖峰)。100 GiB 副本 ≈ 26 小时一个周期 —— 慢即特性;
4. **只报警不修复**:坏记录/索引漂移 ⇒ WARN 日志 + `store_server_scrub_background_*` 指标;
   处置永远是运维弧(手点 verify 定位 + `RepairPartition`/`MoveReplica`)。

**候选集**(与 compaction 刻意相反的部分已显式标注):

- **SEALED 照扫** —— compaction 不碰封存段是"别在 EC 对象身上搬字节";巡检只读不搬,
  恰是冷数据唯一看护人;
- 集群模式下 `BOOTSTRAP`(未追平)副本跳过(还在被拉复制填充,扫了也白扫);
  单机模式(未知状态下发)不受此闸 —— 与 compaction 候选判据同一家;
- 盘 `UNAVAILABLE/DEGRADED` 跳过,计 `store_server_disk_worker_skipped_total{task="scrub"}`。

**续扫游标**:blob 层新能力 `verify_from(start, max_bytes)`。游标 = 上轮预算耗尽处的"下一条
未扫字节";游标段被增量 compaction 摘走 ⇒ 从其后第一段接(新段位置恒大,不漏);段内坏墙 ⇒
弃该段剩余、后续段照扫(与 open 带伤恢复同语义)。游标只在循环内存里(重启即从各副本头部重扫
—— verify 只读幂等,多扫无害;不持久化,家风同 compaction 台账/复制开关);周期走完
(`end_cursor = None`)自动从头开新周期。手动 verify 端点同享该能力并回显
`bytes_read`/`end_cursor`,运维可手动分批接力。

**运维闸**:`GET|POST /admin/scrub-control`(`{"paused":bool}`)掐停/恢复后台循环。
手动 verify/scrub-report 端点**不受影响**(应急检查能力必须常在;`request_control` 的可静默集
仍只有 put/get/delete,不动)。开关不持久化、不动游标 —— 恢复后从原处续爬。

**看门狗折页提示**(纯配置建议,不改码):想让后台批的深扫 IO 按"大顺序 IO"档判,配
`[store_server.disk_watchdog_ms_by_op] verify = 300000`(compaction 同量级;`verify` 本就是
`KNOWN_OPS` 成员,拼错会在配置校验期被拒)。默认空表 = 不开,行为不变。

**配置**:三个新键见 §12.4 表(`scrub_background_enabled` / `interval_secs` / `max_in_flight`,
默认 false/60/1,Validate ≥1 ×2)。**监控**:四个新指标见 §12.5 表(`scrub_background_batches/
bytes/bad_records_total` + `scrub_background_paused` gauge)。

**测试证据**:blob `tests/verify.rs` 11/11(新增 5 件:预算恰停游标精指/多批接力恰好覆盖一次/
坏记录重同步后续扫/悬空游标两路一致/越全段空手);`scrub_worker` 单测 6/6(轮转+周期重开字节
守恒/UNAVAILABLE 跳过可见/BOOTSTRAP 集群跳过+单机照扫/SEALED 照扫/池满退避/抖动界);
`tests/scrub_background.rs` 5/5(真 serve() 循环:自动爬/在线翻坏字节被后台指标发现且不停摆/
掐停两 tick 零增长+暂停期手动 verify 可用+恢复续爬/端点回显契约/默认关零批)。

**残留边界(如实)**:① ~~admin 级跨副本逻辑比对的周期化(发现分歧→建议 Repair)未做,另议~~
✅ 已由 `admin-scrub-schedule`(2026-10-19)落地:admin 定时循环对健康分区跑浅层比对
(`GET|POST /api/scrub-schedule-control` 掐停;runner/口径见
[`cluster.md` §11.1](./cluster.md));② 小副本每 tick 全量重扫(预算钳幅 ≤64 MiB/拍,无害但
批次计数增长快);③ 游标内存态,重启重扫(幂等);④ 带伤启动的副本被巡到时 `bad_records`
会持续报(盘上坏字节不删不改是设计,见 §12.10 残留边界)。

### 12.15 数据面 GET 分块流式(store-read-streaming,2026-10-20)

**动机**:端到端读路径内存上界卡在 store 组包侧 —— 引擎 `get` 内部 `read_full_record`
一次 `vec![0; total]` 整条分配,HTTP 层一次性挂响应体;而中游(router `get_file →
bytes_stream()` 逐块落盘)与下游(frontend 暂存流式回)早已是流式。

**实现**:
- 引擎新增公开原语 `BlobStore::read_range(key, offset, len)`:18B 记录头同口径校验
  (version/key/墓碑位),数据体仅搬 `[offset, offset+len)` 切片;**不重验整条
  CRC**(切片不构成 CRC 覆盖单元 —— bit-rot 防线由复制整读 CRC + 后台 scrub 承担,
  记录头每块重验);`offset+len` 越界 → Corrupt。
- 数据面:首 stat(经每盘池)定 404/总长 → `Body::from_stream` 逐块(512 KiB
  `READ_CHUNK_BYTES`)经**每盘有界池**推进;单请求内存 = 单块,与对象大小无关;
  每块重新走索引定位(不跨 compaction 缓存旧 offset,段生命周期由 `Inner` 快照持住)。
- 指标口径(零新增):`get_total`/`get_bytes` 照旧;`blob_request_duration` 直方图在
  流首按总长分桶 observe;`disk_bytes_total` 经 `OpTimer`(随流持有,Drop 落账)——
  `disk_request_duration` 语义 = **含传输的端到端时长**。
- **`?replication=1` 复制读保持原整读路径**(`get_with_options`,含过期数据体 + 整条
  CRC):复制契约一字不动。
- 语义差异记档:不可读对象(不存在/已删/已过)404 时点从"整读后"提前到"流首";
  流中途对象被删/被 compaction 回收 → 连接中断(响应头已发;连接即一致性边界,
  客户端可换副本重试 —— R=1 语义)。

### 12.15.1 数据面窗口读 `?offset=&length=`(blob-range-read,2026-10-21)

**动机**:路由/网关有了部分读窗口(REST `Range`、S3 206)。若 store 只会整读,
byte-range 只能"整取回再切片" —— 10 GiB 对象取 1 MiB 也是 10 GiB 过网。
把既有的 `read_range` 分块流参数化个起点,窗口读就直达引擎(超越 Ambry:
其 store 协议无 offset/length,切片全在 router 侧)。

**实现**(挂在 §12.15 的同一条流式路径上):
- query:`offset`(缺省 0)+ `length`(缺省 = 读到尾,自动截到对象尾;对齐网关
  `bytes=s-` 的截尾语义);`start > 对象长` → **416**;零长窗口 = 合法空 200。
- `?replication=1` **不消费窗口参数**(整读 + include_expired + 整条 CRC 的复制
  契约零触碰;豁免清单同上,分类不受新 query 影响 —— `classify` 只认 `replication=`)。
- 引擎校验口径 = §12.15 的 `read_range` 原样(记录头逐块重验;CRC 取舍不变);
  窗口越界的引擎层 Corrupt 由流首 stat 的 416 前置挡住。
- 指标口径(零新增):请求一次 `get_total`;**字节数/时长分桶改按窗口长计**
  (服务了多少记多少);`disk_bytes_total` 同窗口实际传输。

### 12.16 数据面删除审计日志(2026-10-20)

`DELETE /partitions/{pid}/blobs/{key}` **接收即**打 `info` 级结构化日志:
`tracing::info!(partition_id, blob_id, client_ip, "blob delete requested")` —
删除是破坏性操作,幂等重删每请求一记(比"谁删成了"更宽的审计口径);"谁"取连接对端
IP(`into_make_service_with_connect_info`;发起方名义身份在 gateway 层
SigV4/owner,store 只见直连对端)。无连接信息(测试 mock/非 make-service 路径)→
`client_ip=unknown`。
