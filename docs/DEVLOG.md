# 开发日志(DEVLOG)

按实现步骤记录:做了什么、关键决策、验证证据。

---

## 步骤 0:开发环境

- macOS 宿主无 Rust/无法编译 monoio(Linux only)→ 起 colima Linux VM。
- 环境:Ubuntu 24.04 / kernel 6.8 / io_uring 启用 / Rust 1.98 / O_DIRECT 可用(ext4)。
- 源码经 virtiofs 挂载进 VM;`vm-cargo.sh` 把 cargo 转进 VM,target/数据落 ext4。
- 冒烟:`crates/smoke` 用 monoio 真 io_uring 读写闭环通过。

## 步骤 1:IO 层(AlignedBuf)

- 决策:O_DIRECT 需对齐 → 自实现 `AlignedBuf`(4096 对齐、alloc_zeroed、实现 monoio IoBuf/IoBufMut)。
- 核对 monoio 0.2.4 真实 API:`OpenOptionsExt::custom_flags` 设 O_DIRECT;`IoBuf{read_ptr,bytes_init}`、
  `IoBufMut{write_ptr,bytes_total,set_init}`;`BufResult<T,B>=(io::Result<T>,B)`。

## 步骤 2:LogSegment

- monoio + io_uring + O_DIRECT 全链路。对齐暂存块 `tail` + 满块写盘 + flush 补零尾块 + fsync。
- Header + crc32,`open()` 按 data_end 恢复。`posix_fallocate` 真实预分配。
- 验证:7 单测通过(round-trip / 跨块 / 段满 / 重开恢复 / AlignedBuf)+ clippy `-D warnings` 零告警。

## 步骤 3:Log(多段)

- 多 LogSegment + rollover(旧段先 flush 再滚动)+ `LogOffset{segment,offset}` 全局地址。
- 容量校验(total 为 segment 整数倍)、单记录不超段、段满 `SegmentFull`。
- `open()` 扫描目录按位置排序恢复。
- 单测:rollover / log 满 / 记录过大 / 重开恢复。

## 步骤 3.5:崩溃一致性属性测试

- `tests/crash_consistency.rs`:proptest 随机 Append/Flush → 崩溃(不 flush 关闭)+ 重开。
- 不变式:恢复的 data_end == 最后 flush 的 data_end;边界内记录原样可读。
- 另在打开态校验所有记录可读(覆盖 tail+disk 读路径拼接)。

## 步骤 4:PersistentIndex

- `StoreKey`(32B 定长)+ `IndexValue`(43B 定长)+ index 段(排序条目 + Binary Fuse 过滤器)。
- 活跃段内存 BTreeMap;满阈值封存落盘 + 建过滤器;newest-wins 查找;删除走新条目。
- 决策:索引文件用同步 std::fs(小元数据/非热路径),tmp+rename 原子写,header/body 双 crc;
  过滤器不落盘、加载时重建。xorf `BinaryFuse8::try_from(&[u64])`,散列去重避免构造失败。
- 验证:16 单测(index 5 + value 1 + log/segment/aligned)+ 崩溃属性测试通过;clippy `-D warnings` 零告警。

## 步骤 5:BlobStore(串联)

- 缝合 Log(数据,io_uring+O_DIRECT)+ PersistentIndex(索引,std::fs)成对象存储:put/get/delete。
- 自描述日志记录 `magic|version|flags|key(32)|data_len|data|crc`:key/flags 也落 Log,
  为将来"扫描 Log 重建索引"留出兜底路径;crc 覆盖记录全体,get 时校验并比对 key。
- 语义对齐 Ambry:put 命中已存在条目(PUT 或 DELETE 墓碑)一律报 `AlreadyExists`(见步骤 11);delete 写墓碑记录 + 删除条目;
  get 处理 NotFound/Deleted/Expired。flush 顺序先数据 fsync 再落索引,索引永不指向未落盘数据。
- 验证:21 单测(新增 BlobStore 5:put/get、拒重复、删除+重写、重开恢复、记录 crc 守卫)
  + 崩溃属性测试通过;clippy `-D warnings` 零告警。

## 步骤 6:从 Log 扫描重建索引(兜底)

- `Log::segment_spans()` 暴露各段 `(position, data_end)`;`BlobStore::open_rebuild_index()`
  打开 Log → 清掉旧 `.idx` → 顺序扫描全部自描述记录 → 按 Log 追加顺序(旧→新)重建索引 → flush。
- 正确性:记录自描述(key/flags/crc),追加顺序插入天然满足 newest-wins;只扫到 flush
  持久化边界,边界内无残记录(未落盘尾部对读不可见)。
- 已知限制:记录暂不落 expiration/op_time,重建后归零(当前无 TTL 写入,不影响可见性)。
- 验证:新增 `rebuild_index_from_log_scan`(put 25 + 删除+重写 → 抹掉索引 → 重建 → 全量校验 +
  重建后普通 open 可读);共 22 单测 + 崩溃属性测试通过;clippy `-D warnings` 零告警。

## 步骤 7:去 monoio → 同步阻塞 IO(战略反转,对标 Ambry 路线乙)

- **移除 monoio / io_uring / O_DIRECT 全栈**:`aligned.rs` 删除;`log_segment.rs`/`log.rs`/`blob.rs`
  改写为 `std::fs::File` 同步阻塞(buffered)IO;`data_end` 只在 `flush()` 更新(fsync 点),
  断电时恢复严格截断到持久边界;crash 测试改为直接断言该边界。
- **索引**:活跃段 `BTreeMap` → `crossbeam-skiplist::SkipMap`(并发);封存段整段载入 RAM →
  **`memmap2` mmap**(二分直接做在映射上);Fuse 过滤器**持久化为 `{seq}.fuse`**
  (descriptor+fingerprints,`BinaryFuse8Ref` 零拷贝引用),启动不再全量重建;
  索引段写入改「先 fsync 再 rename」。
- 动机与决策详 `openspec/changes/io-index-design/`。定容上界、compaction 本轮未做。
- 验证:20 单测 + 崩溃属性 + smoke 全绿;clippy `-D warnings` 零告警。

## 步骤 8:compaction / GC(CompactAll 首版)

- `BlobStore::compact()`(CompactAll,对齐 Ambry `CompactAllPolicy`):重写整个 log 只保留**存活赢家**
  记录(`index.find(key).offset == 该记录 offset`),丢弃被覆盖/删除的旧 PUT;重建索引;原子换入。
- 存活判定天然处理:删除只留小墓碑(原 PUT 回收)、覆盖只留最新(旧 PUT+DELETE 回收)。
- 崩溃安全:`log.new` 建好并 fsync → 写 `COMPACT_COMMIT` 标记 → 目录 rename 换入(log→log.old,
  log.new→log)→ 重建索引 → 删标记与 log.old。`open()` 前 `recover_compaction` 依标记收敛:
  有标记则保证 log 为 compacted 份并重建索引;无标记则丢弃残留 log.new(旧 log 完好)。
- 配套:`stats()`(存活/死亡字节)、`compact_if_needed(threshold)`(死占比达阈值才压)。
- 验证:新增 3 测(回收+状态不变、阈值不触发、提交中崩溃修复);共 23 单测 + 崩溃属性 + smoke 全绿;clippy 零告警。
- 已知简化:目前是 CompactAll(全库重写);Ambry 式按段区间的增量 StatsBased compaction 后续做。

## 步骤 9:增量 compaction(按段死数据阈值触发)

- `BlobStore::compact_incremental(threshold)`:只压缩死占比 ≥ 阈值的**非活跃**段(热/末段不动),
  区别于 `compact()` 的全库重写(对齐 Ambry 按段区间 StatsBased,而非 CompactAll)。
- 机制:被选中段的存活记录写入位置更高的新段(temp→fsync→rename)→ 删旧段 → 重建索引;
  存活赢家搬到更高 offset 仍是该 key 最新记录,newest-wins 不变。
- 配套:`segment_stats()`(每封存段 `(position, live, dead)` 字节);抽出 `scan_segment()` 单段遍历。
- 崩溃安全:复用 `COMPACT_COMMIT` 标记;`recover_compaction` 两分支接入 `clean_tmp_segments()`
  清理残留 `.tmp` 半成品新段,open 重建索引收敛。
- 验证:新增 1 测(只回收高死老段、状态不变、重开一致);clippy 零告警。

## 步骤 10:内存参考模型全量对拍

- `reference_model_full_crosscheck`:确定性 PRNG(多 seed)随机穿插 put/delete +
  flush/reopen/compact/compact_incremental,纯内存模型记录每 key 期望状态,每步全量比对 `get()`。
- 覆盖 newest-wins、墓碑、跨段封存(mmap+fuse)、compaction offset 重写、水位回放重开、
  扫 log 全量重建三态收尾。
- 验证:共 27 单测 + 崩溃属性 + smoke 全绿;clippy `-D warnings` 零告警。

## 步骤 11:PUT-on-deleted 语义对齐 Ambry

- 核对 Ambry `BlobStore.checkWriteSetStateInStore`:`index.findKey(..., EnumSet.of(PUT, DELETE))`
  命中且非幂等重试即 `COLLIDING → AlreadyExist`——**blob id 永久占用,删除后普通 put 不允许复活**,
  复活走独立 `undelete`(带 lifeVersion)。
- 反转原 rs「删后允许重写」:`put_ttl` 改为命中任何已存在条目(PUT 或墓碑)一律 `AlreadyExists`。
- 更新 `delete_then_reput_rejected`(原 `delete_then_get_and_reput`)与参考模型的 put 判定。
- 验证:27 单测 + 崩溃属性 + smoke 全绿;clippy 零告警。

## 步骤 12:open() 索引损坏自动重建(对齐 recover())

- 抽出 `load_index_with_replay`(加载已落盘索引 + 水位回放,增量恢复为主)。
- `open()` 捕获 `Corrupt` → 自动 `rebuild_index` 兜底(数据 fsync 是持久边界,索引可重建),
  不再要求手动 `open_rebuild_index`。
- 新增测试:损坏活跃索引段后 open 自动重建、逻辑状态(含墓碑)仍正确。

## 步骤 13:compact_if_needed 改为按段增量

- 周期性 GC 入口从全库 CompactAll 改为委托 `compact_incremental(threshold)`:
  只压死占比达阈的非活跃段(对齐 Ambry StatsBased 按段触发)。全库重写保留为显式 `compact()`。

## 步骤 14:死数据统计增量维护

- `BlobStore` 增 `seg_bytes: BTreeMap<u64, SegBytes{live,dead}>`:put 加 live、delete 把旧 PUT 转 dead + 墓碑计 live;
  compaction/open/rebuild 用 `scan_seg_bytes` 全量重算。`stats`/`segment_stats` 读计数,免每次全量扫 log
  (对齐 Ambry `BlobStoreStats` 增量维护)。
- 不变式:增量计数须与全量扫一致。新增 `incremental_stats_match_full_scan`;参考模型每 seed 收尾额外对拍。
- 验证:29 单测 + 崩溃属性 + smoke 全绿;clippy `-D warnings` 零告警。

## 步骤 15:undelete / lifeVersion(复活已删除对象)

- 记录格式 v3:头部加 `life_version u16`(在 op_time 与 data_len 之间),从 log 重建索引不丢生命版本。
- `IndexValue` 加 `original_offset`(承载数据的记录位置):PUT/UNDELETE 自带数据故 == `offset`,
  DELETE 墓碑指向被删前的 PUT(供复活取原数据)。`VALUE_SIZE` 43→59。
- `undelete(key)`:仅当当前为已删除才可复活;读回原 PUT 数据、写一条 `FLAG_UNDELETED`+`life_version+1`
  的**整记录**并成为新赢家,返回新 life_version。不存在→`NotFound`,未删除/已复活→`NotDeleted`。
- delete 保留 `life_version`;undelete = 当前 +1(对齐 Ambry `undelete`)。
- rebuild/replay 用统一 `index_value_from_record`:继承 life_version,墓碑 original_offset 沿用已入索引的数据位置。
- **与 Ambry 的实现分歧**:Ambry 用「小 undelete 标记 + 每 key 多值索引」;本引擎是单值索引,故 undelete
  **复制整条数据**成为赢家,保持 offset 相等的 compaction 存活判定/rebuild/seg_bytes 机制不变(代价:复活写放大,罕见)。
- **限制(对齐 Ambry 保留期到期语义)**:被删对象的 PUT 一旦被 compaction 回收,即不可复活(`undelete`→`NotFound`,
  对应 Ambry `IDDeletedPermanently`);本引擎暂无时间保留窗口。
- 验证:新增 5 测(复活+lv 递增、拒非删态、重开/重建保留、复活数据过 compaction、PUT 回收后永久删);
  共 34 单测 + 崩溃属性 + smoke 全绿;clippy 零告警。

## 步骤 16:updateTtl(移除过期时间)

- `update_ttl(key)`:对齐 Ambry「仅支持移除 TTL(置永久)」;写 `FLAG_TTL_UPDATED`、复制原数据、
  `expiration=NO_EXPIRATION`、`life_version` 不变的整记录成为赢家(数据重写模式,同 undelete)。
- 校验:不存在→`NotFound`,已删除→`Deleted`,TTL 已更新→`AlreadyUpdated`(新增 Error);启用此前未用的 `FLAG_TTL_UPDATED`。

## 步骤 17:只读查询与自省(对齐 Store 契约)

- `stat(key)→BlobMeta`:不读数据体拿状态(deleted/undeleted/ttl_updated/expired)+ expiration + life_version + data_len
  (对齐 Ambry `findKey`/`isKeyDeleted`)。
- `get_with_options(key, include_deleted, include_expired)`(对齐 `StoreGetOptions`):已删对象数据从原 PUT 读回。
- `size_in_bytes()`/`is_empty()`(对齐 `getSizeInBytes`/`isEmpty`)。

## 步骤 18:HardDeleter(隐私擦除)

- `hard_delete()`:扫墓碑对象,把其原 PUT 数据字节**就地清零** + 置 `FLAG_HARD_DELETED` + 重算 crc 整条写回,
  在 compaction 回收前抹除明文(对齐 Ambry `HardDeleter`)。幂等、多段末尾统一 fsync。
- 新增 `LogSegment::overwrite_at`/`Log::overwrite_at`/`Log::flush_all`(就地覆盖已写区间,不改 data_end)。
- 验证:新增 updateTtl 2 测 + 只读查询 1 测 + 硬删除 1 测;共 38 单测 + 崩溃属性 + smoke 全绿;clippy 零告警。

## 待办

- 每 Log 定容上界(容量/文件数,写满自动滚动新 partition)——设计锚点,暂缓。
- 并发/共享模型(D4):`&mut self` 单写者 → 共享实例 + `RwLock`/并发结构(运行时 tokio vs 手写线程池仍为 Open Question)。

## 步骤 19:存储模型简化 —— 对象不可变 + 删除不可逆(2026-09-23)

- **语义**:对象**不可变**、删除**不可逆**。整体移除三个变更类操作 `undelete` / `update_ttl` / `hard_delete`;
  随之删掉三个为"后写覆盖先写的定序"而存在的字段:`life_version`(生命版本)、`operation_time_ms`(op_time)、
  `original_offset`(墓碑指向被删前 PUT 的位置)。错误 `NotDeleted` / `AlreadyUpdated` 不再存在;`Deleted` 保留。
- **每个 key 至多两条记录**(PUT、墓碑)。`put` 命中已存在 key(存活或墓碑)→ `AlreadyExists`(blobId 永久占用);
  `delete` 命中不存在 → `NotFound`;命中已墓碑 → **幂等成功 204**(见本条末尾「后续拍板」)。
- **复制合并退化为墓碑恒胜**(单调、幂等、天然收敛):peer 墓碑覆盖本地 PUT;peer PUT 永不覆盖本地 PUT/墓碑;
  本地墓碑 → `Skipped`;两边都存活 → `Skipped`。不再需要任何每对象定序字段,墓碑也不再需要数据体(省带宽)。
- **磁盘格式三层同时精简**(实施时无已部署数据,**不做任何兼容层**;旧索引布局仍走"判 `Corrupt` → 从 log 重建"):
  - **段头 v2**:物理 64 B(40 B 有效),`magic(0..4) / version=2(4..6) / reserved(6..8) / capacity(8..16) /
    data_end(16..24) / created_ns(24..32) / key_prefix(32..36) / crc(36..40,覆盖 `[0,36)`)`;
    `key_prefix` = blobId 高 32bit(该副本的 partitionId),**每段一份、整副本恒定**,create 写入、rollover 继承、
    open 读回核对(不一致 → `Corrupt`)。
  - **记录 v4**:`magic(2)|version(1)|flags(1)|key_suffix(4)|expiration_ms(8)|data_len(4)|data|crc(4)`,
    固定开销 **38 B → 24 B**;**key 高 32bit 不在记录里,只在段头存一次**,记录只存 4B `key_suffix`,
    读时 `key_prefix ‖ key_suffix` 拼回完整 blobId;写路径校验 key 的 partition 与前缀一致(不一致 → `BadArgument`)。
  - **索引 v2**:value **59 B → 21 B**(`offset(8, (段号 u32 << 32) | 段内偏移 u32) | size(u32) | flags(1) |
    expiration_ms(i64)`,entry = 8 B key + 21 B = **29 B**,索引 key **仍是完整 8 B blobId**)。
  - **段容量硬上界 4 GiB**(`log_segment::MAX_CAPACITY` + `StoreConfig::validate` 的 `MAX_SEGMENT_SIZE_BYTES`):
    4 GiB 是**格式不变量**而非调优旋钮 —— 段内偏移要放进索引 u32 偏移半区,且"单条记录 ≤ 段 − 64 < `u32::MAX`"
    使记录的 u32 `data_len` 结构性无法回绕。
- **配置/监控**:删 `store_server.hard_delete_interval_secs`、`store_server_hard_deleted_total`、
  `store_hard_delete_total` / `store_hard_delete_duration_seconds`;本地 worker 从三条 loop 减到**两条**(flush + compaction)。
- **契约面**:数据面删 `POST .../undelete` 与 `POST .../ttl`;frontend 删两条路由与 `x-ambry-blob-undeleted` /
  `x-ambry-blob-ttl-updated` / `x-ambry-blob-life-version` 三个响应头(`CORS_EXPOSED_HEADERS` 现 8 项);
  router 删 `undelete` / `update_ttl`,新增 **`put_with_ttl` / `put_file_with_ttl`**(`put`/`put_file` 为 `NO_EXPIRATION` 包装)。
- **TTL 单次多数派写**:frontend 的 `x-ambry-ttl-ms` 从"先 `put` 再 `update_ttl`(两次多数派写)"改为
  **写入时一次带 TTL**(解析成绝对 `expiration_ms` → `PUT ?expiration_ms=`)—— TTL 与数据体一样写完即定,之后无处可改。
- **隐私口径降级**(记录在案):从"对象级擦除"降为"段级、惰性、最终由块复用覆盖";若将来需要真正擦除,
  落法是"compaction 时把死记录置零再落盘",不需要每对象元数据。
- **admin 连带**:`crates/admin` 读 store 元数据 JSON / `KeyState` 里的 `life_version` 等字段删除,假 store 夹具同步。
- **后续拍板:`DELETE` 幂等化(2026-09-23,同日)** —— 墓碑 key 重复删由 blob 引擎直接回成功(不写第二条墓碑),
  store→router→frontend 全链路 **204**;从未存在的 key 仍 404(经多数派聚合为 503,既有口径)。
  见 `openspec/changes/simplify-delete-model/` 与 `docs/design/frontend.md` §2。
- **文档同步**:`docs/design/{store.md,cluster.md,router.md,frontend.md,replication.md}`;
  受影响的 change 档案(`store-local-worker` / `store-server` / `store-server-mutations` / `store-server-replication` /
  `store-request-control` / `blob-io-end-to-end` / `balance-and-scrub`)加"后续变更"注记;
  `openspec/changes/simplify-delete-model/` 从提案回填为已实施(逐项偏差见其 `tasks.md`「与原提案的偏差」)。
- 本条目之前的各"步骤"描述的是当时真实的实现,不改写。
