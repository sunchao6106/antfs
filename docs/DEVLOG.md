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

## 步骤 20:compaction 策略升级 —— 候选集按可写态、低峰钟点调度、回收 TTL 过期与墓碑(2026-09-23)

- **回收口径(引擎 `blob`)**:`compact` / `compact_if_needed` / `compact_incremental` / `compact_cost_benefit`
  接 `CompactPolicy{dead_ratio, now_ms, tombstone_retention_ms}`;判活从"是不是当前赢家"扩展为
  **赢家 且 未过期 且 墓碑未过保留期**。三类字节真正释放:被取代的旧记录(老行为)、**TTL 过期对象**、
  **过保留期的墓碑**。`CompactionStats` 增 `tombstone_reclaimed_bytes` / `expired_reclaimed_bytes`。
- **墓碑的删除时刻**:复用索引条目的 `expiration_ms` 槽位(墓碑恒 `-1` → 改存 `deleted_at_ms`)。**磁盘布局不变**
  (值 21 B、`.idx` 条目 29 B),故不涉格式版本;旧数据 `-1` = 删除时间未知 ⇒ **永不自动回收**(§3.4 兼容,
  单测 `legacy_tombstone_on_disk_is_readable_and_never_reclaimed` 走"重开重建索引 + 保留期给 0"两段)。删时刻随
  记录头复制到对端,各副本判定一致。`stat()` 的 TTL 口径走新方法 `ttl_expiration_ms()`,墓碑对外仍报 `-1`。
- **闸门与选段**:新增 `reclaimable_by_segment`(索引遍历聚合过期/墓碑字节 + `seg_bytes.dead`,**不读数据体**)
  —— partition 整体可回收占比 ≥ 阈值才动手(partition 级闸门),再对**非活跃段**按同一阈值选段。
  旧实现只看 `seg_bytes` 死字节,导致"整整一段过期数据"永远不够格被压。
- **候选集(store `worker`)**:按 master 下发的分区可写态挑 replica —— `SEALED` 永不入选、`RO` 总是入选、
  `RW` 由 `compaction_include_rw`(默认纳入,只压其封存段)、未知状态(单机模式)按可压处理;未追平
  (`BOOTSTRAP`)的副本跳过(压了也要从 peer 拉回来,还会让复制游标失效)。
- **状态下发**:proto `PartitionStateInfo` + `MasterDirective.partition_states = 4`(每拍全量、只含该节点承载
  副本的分区);master 侧纯函数 `reconcile::plan_partition_states`(带单测);store 侧记在
  `ReplicaHandle.partition_state`,与"副本实然态" `current_state` 明确区分。
- **调度**:由"固定 interval 节拍"改为「**首跑钟点 + 周期**」—— 首次对齐本地 `compaction_start_time`,之后每
  `compaction_interval_secs` 一次。等待时长由纯函数 `next_delay_secs` 算出;本地时区用 libc
  `localtime_r`/`mktime`(本地日历顺延一天,**夏令时正确**),测试用假时钟故与机器时区无关。
- **配置**(用户显式指定,非擅自改默认):`compaction_interval_secs` 3600 → **86400**;
  `compaction_dead_ratio` 0.5 → **0.3**;新增 `compaction_start_time`(默认 `"01:00"`,严格 `HH:MM` 校验)、
  `compaction_include_rw`(默认 true)、`compaction_tombstone_retention_secs`(默认 86400)。
- **手动 compact** 不受状态/阈值闸门约束(运维强制),但同一套回收口径(仍守保留期)。
- **风险(记录在案)**:墓碑回收 = 抹掉"该 key 已删除"的唯一证据;副本若丢失 delete 且滞后超过保留期,修复时
  可能复活对象。缓解:保留期默认 24h 且可配,文档写明"保留期应 > 副本滞后上界"。另有既有缺口未动:
  compaction 改写 offset 会让在飞的复制游标失效(resetKey 属复制层)。
- **验证**:`cargo test --workspace` 全绿(53 个 test target);`cargo clippy --workspace --all-targets` 无告警。
  新增用例覆盖:过期对象回收 / 墓碑保留期内不回收·过保留期回收 / 重开后删时刻仍在 / partition 闸门看可回收
  占比 / 旧格式墓碑永不回收 / 候选集状态矩阵 / BOOTSTRAP 跳过 / 首跑与后续节拍(含超时立即跑)。
- **文档同步**:`docs/design/cluster.md`(proto 片段 + S4 叙述 + `[store_server]` 配置表)、
  `docs/design/store.md`(新增 §9.1 回收口径);决策档案 `openspec/changes/store-compaction-policy/`。

## 步骤 21:复制游标重定位 —— 段身份/副本身份两层 + 判据收紧 + 墓碑闸门(2026-09-23)

- **问题(静默丢数据链)**:拉复制游标(`FindToken`)记录的是**对端 log 的绝对位置**,而 compaction 会改写
  坐标系(全库 `compact` 把存活记录重写进新 log、段位置从 0 重来;副本重建 / 换盘 = 全新副本)。旧代码把
  "游标已作废"误判成"已追上":`scan_candidates` 对"段不存在"静默 `continue` ⇒ 空批 + 原游标 ⇒
  `replicate_from_peer` 认为到末端 ⇒ `caught_up_with_peer` 再拿同一陈旧游标探本地库也空 ⇒ **判追平 → 置
  ACTIVE** ⇒ master 删源闸门(`reconcile::plan_move`/`drop_confirmed` 的"搬迁要等继任者已 ACTIVE")误成立 ⇒
  源副本被摘除 + 下发 `DROPPED` 删源数据 ⇒ **丢数据且无声**。
- **跟着数据走四遍(大白话版,读不懂上面那段就看这里)**:
  1. **正常拉一轮**:分区 3 在 A、B 各一份(RF=2),B 是新副本。B 问 A"你有哪些 key",A 回一批并附一句
     "我给到 **log 第 3 段、第 500 字节**处"——这句话就是**游标**,B 记下;下轮带它继续问,直到 A 说"没了",
     **这时 B 才向 master 报"我追上了"**。master 收到这句才敢删 A 的副本腾空间:`docs/design/cluster.md`
     的搬迁规矩是"继任者 ACTIVE 之后才删源",所以早报一次就等于删掉还活着的源。
  2. **拉的中途 A 做了全库 compaction**:存活数据被重写进新 log、**段号从 0 重新数**,B 那句"第 3 段第 500 字节"
     成了废话。**危险做法**:A 回一句"那段没了、后面也没了"→ B 以为拉完了 → 报追上 → 删掉还带数据的 A →
     **数据永久没了且无报错**。**现在的做法**:A 认出这句位置认不出(段没了 / 段还在但版本号变了 / 位置越界 /
     副本编号不符)→ 回答"从头给你重来"+ 明确告知"这轮作废了你的位置";B 收到就**不许**报追上,下轮接着拉。
  3. **A 的副本被删掉重建 / 换盘**:新目录从零开始,段号又从 0 数,内容也不一样 —— 老游标"第 3 段第 500 字节"
     可能**恰好**在新副本上也存在(段号重数了),**光看位置看不出问题**。故给"这份副本"本身也发编号
     (**重建/换盘就换新号**),B 每轮带着它问,对不上同样作废 + 从头拉;编号存在副本目录的 `LOG_INCARNATION` 里。
  4. **一个全新副本没领到正式编号**:master 联系不上时它先用**本机时间**开张(全世界唯一,绝不会与对端撞号)。
     别的机器是**从它这里第一次**听说这个编号的,不存在"把你当成上一代的你"的错觉 ⇒ **不丢数据**;
     代价仅是 master 账本里这份副本的编号栏还空着(0 = 未发过),要等下次从零重建才补上 —— 目前没有任何
     代码读那一栏(只有 master 自己的测试在读),放着不管无影响。若要收掉它,最小改法是 `LOG_INCARNATION`
     分两种写法(`m:<n>` = master 发的 / `l:<n>` = 本机临时值)让心跳对 `l:` 的副本重试申领;**本次未做**
     (为"显示更体面"去动一个已落盘的文件格式,不划算)。
- **第一层身份:段身份 = 位置 + 代次**(`LogSegmentName`;段名 `{position}_{generation}_log.seg`,段头 **v3**
  自述 `generation`)。识别"**同一副本内** log 被 compaction 重写":同一位置、不同代次。代次在同一目录内
  **单调递增**(滚动 = `max(gen)+1`),**全库 compact 接着旧 log 的最大代次往后取** —— 于是段位置可以复用、
  段身份永不复用。校验在引擎(`find_entries_since` 入口 `revalidate_token`),理由短名 `segment_missing`
  (段不在段表)/ `segment_generation_mismatch`(位置上有段但代次不同)/ `offset_out_of_range`(偏移越过数据区);
  索引值 **21 B 定长布局没变**(代次不进索引,只活在段名与游标里)。
- **第二层身份:副本 incarnation**(master 发放,落盘 `{store_dir}/LOG_INCARNATION`;文件缺失/损坏时用
  **本机当前时刻**兜底并落盘 —— 物理 bootstrap 只拷段、换盘后重来走的都是这条路)。识别"**对面整个换了一代副本**"
  (重建 / 换盘)。核对放**服务层**(`server::replica_metadata`):引擎看不到副本级身份。理由短名
  `incarnation_mismatch`。
- **游标与协议**:`FindToken::{Uninitialized, Offset{segment: LogSegmentName, offset: u64}}`;请求参数
  `pos` / `gen` / `off` / `incarnation` **四件套齐全才可信** —— `pos`/`gen`/`off` 残缺即按"从起点重扫"处理
  (方向是多拉,安全);有游标却缺 `incarnation`、或 incarnation 不符 ⇒ 从头重扫并经 `token_reset` 报告原因短名。
- **判据收紧**:`caught_up_with_peer` 前置"本轮没有 `cursor_reset_reason`";`sync_round` 增 `prior_incarnation`,
  **对端 incarnation 变了那一轮绝不判 ACTIVE**。本地探测 `BlobStore::has_content_after` 是**纯位置**比较
  (peer 游标的段身份属于对端坐标系,本地无从校验)。会话状态机 `replication::PullSession`(游标 + 对端身份 +
  无进展计数)由**生产 loop 与集成测试共用同一份**;`incarnation_mismatch` + 连续两轮无进展 ⇒ `no_progress`
  丢弃游标重拉(防永远卡死在 BOOTSTRAP)。
- **取段 404 化**:`GET /partitions/{pid}/segments/{position}_{generation}` 按**段身份**寻址 —— 对端认不出
  该段 ⇒ **404**、名字畸形 ⇒ **400**;`/segments` 响应 = `{incarnation, segments:[{position,generation,data_end}]}`。
  物理 bootstrap 三条丢弃判据(丢弃后回落**纯逻辑拉取**,幂等、必然收敛,**绝不装混合快照**):①取段 404/出错
  ②段头自述代次与清单不符 ③拷完再列清单 incarnation 变了。
- **从零建副本时申领身份**:store 心跳里 `ensure_replica`(**本地本来就有**该副本时是幂等空转,不算从零建)与
  物理 `install_bootstrap` 成功(`bootstrap.segments > 0`)之后调 RPC `AssignReplicaIncarnation(partition_id,
  node_id)`;master **每次发一个全新更大的号**(不是取回旧值),leader-only + 经 Raft 提交,`next_incarnation`
  由 **apply 发放**并落进 placement(`ReplicaPlacement.incarnation`,`0` = 未分配),store 采纳到本地。**申领失败
  绝不影响建副本**(warn + 保留本地兜底值),**幂等心跳绝不重复申领** —— 否则身份每拍换新,别的节点手里的游标被
  反复作废(甚至判不出追平)。
- **墓碑回收闸门(配套)**:`PartitionStateInfo.tombstones_reclaim_allowed`(master 判据 = 该分区**所有副本
  ACTIVE 且 lag == 0**;空 placement 保守不放行)→ store `ReplicaHandle.tombstones_reclaim_allowed`(初值
  `false`)→ `CompactPolicy.tombstones_allowed`。关着时墓碑**一律照搬**(连过保留期的也不回收):还有副本没追平
  就收墓碑,那份副本永远学不到这条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。**单机模式(未接 master)
  恒放行**(没有别的副本可滞后)。定时 compaction 与手动 compact 共用同一判据。
- **指标**:`store_server_replication_cursor_resets_total{reason}` 的 reason 集合 = `incarnation_mismatch` /
  `segment_generation_mismatch` / `segment_missing` / `offset_out_of_range` / `no_progress`;非零是正常现象
  (compaction / 重建后的续拉),持续增长才值得查。
- **不做兼容层**:线上尚未启用,不存在新旧节点并存 ⇒ 不写任何回滚 / 兼容分支(旧格式清单、可选字段都不加)。
- **被否决的方案**:①**盘级计数器**当身份 —— 换盘会让计数从 1 重数,新盘上的身份"看似旧",那个窗口消不掉;
  ②**首段 `created_ns`** 当身份 —— 除了"时钟回拨 / 同纳秒重建即可撞号、区分不出'重建'与'重启重开'",
  它还**取自拷贝来的字节**:物理 bootstrap 装载的副本其首段诞生在**对端**,那个时刻正是对端自己的兜底身份,
  于是"重建 ⇒ 身份必换"在申领没成功时会失效(与对端撞号 → 对端手里的旧游标看似合法)。最终改为**本机时刻**;
  ③**滚动升级兼容层**(新节点容忍旧节点不回身份)—— 线上未启用,兼容层只会把"认不出"
  退化成"认成合法",正是要堵的那条丢数据路径。
- **验证**:`cargo test -p antfs-blob -p antfs-store -p antfs-master` 全绿。新增
  `crates/store/tests/compaction_replication.rs` **9 个场景**(边复制边 compact 一族:①追赶中遇对端全库 compact
  ②追赶中遇增量 compaction ③对端副本 drop 后重建(身份必变大、新数据拉到、本地不丢)④已追平后 compact 的
  delete 传播 ⑤对端变空而本地有数据(绝不误判追平)⑥**真实后台复制 loop + 指标断言** ⑦**一次排空过程中的
  批间重写**(`sync_round` 走真链路,断言**重写那一轮绝不转 ACTIVE** —— 旧代码上必然变红:40 条初始数据被静默
  跳过)⑧**bootstrap 拷段途中对端重写 → 丢弃拷贝**回落逻辑拉取后逐 key 一致)⑨**本端(接收方)在追赶途中做
  全库 compaction**(游标是对端坐标,本端重写不得让它少拉一段 / 触发重定位;`max_segment_generation` 接续);
  `crates/store/tests/incarnation_and_gate.rs` **5 个**(申领身份并落盘 / 重复指令不重领 / OFFLINE 挂回不换号 / 闸门关 / 闸门开);
  `crates/store/tests/data_plane.rs::segment_endpoint_addresses_segments_by_identity`(取段按段身份 200/404/400);
  引擎侧 `blob.rs` 单测:revalidate 三分支与理由短名 / 合法游标不重定位 / 代次单调与重建换号 / incarnation
  落盘·本机值兜底·**bootstrapped 副本不继承对端身份**·采纳;master 侧 `crates/master/tests/replica_incarnation.rs`(发放单调、每次换新号、三节点经 Raft
  一致)、`crates/master/tests/tombstone_gate.rs`(闸门随副本追平)。
- **文档**:`docs/design/store.md`(§3 + 新增 §9.2 游标重定位)、`docs/design/cluster.md`(S3a 加固段 + 监控项清单);
  决策档案 `openspec/changes/store-replication-cursor-revalidate/`。

## 步骤 22:router 发号按可写分区轮转 —— 把写 IOPS 摊到多个分区/盘上(2026-09-23)

- **问题**:`Router::allocate_one` 在号段缓存里 `find` 第一个没取完的段,而"要下一段"只在**所有**段都空时发生 ——
  `IdCache.segments` 恒为 0 或 1 个元素,一段号**连续耗尽在同一个分区**上。于是一个 frontend 进程(= 一个 `Router`,
  `AppState.router` 单实例)的全部并发写落在同一分区的 RF 个副本、也就是**同一组盘**上;压测二进制同理。
- **决策**:落实 `blobid-master-assign` D10 的落地形态 —— 补段时 `ListPartitions(state=RW)` 拿可写分区列表,
  对**每个**分区显式点名 `partition_id` 各申请一小批(`router.id_segment_size` 个)号段,取号在持有的号段之间
  **顺序轮转**(`IdCache::take_rr`)。取段并发(`MasterClient` 克隆共享同一 channel),整轮仍**在号段锁里单飞**
  (并发各补一轮会放大 RPC 并可能重号)。单个分区失败(期间转 RO/封存/号耗尽)只跳过它,全失败才报错;
  已不在可写列表里的分区其未用完段丢弃(余号成空洞,无害,D3)。
- **`Segment` 语义**:`next` 含 / `end` 不含(旧代码是 `next = start + 1` + 单独返回首个号,单号段 `count=1` 会漏发)。
- **偏离 D10(显式记)**:负载由"每段一次容量加权随机"变为"可写分区上的**均匀轮转**"。router 仍不自造分区 id、
  不缓存容量判断(分区集合与合法性都由 master 给并复核)。代价:一次补段 = 1 次 `ListPartitions` + N 次 `GetBlobIds`
  (N = 可写分区数),稳态持有 N × `id_segment_size` 个号 —— 调小 `id_segment_size` 可保持总预留与换段频率不变。
- **验证**:`cargo test -p antfs-router` 全绿 —— 4 个新单测(轮转顺序 / 单号段 / 段空跳过与空缓存 / `order` 收缩后游标回绕)
  + e2e 断言两可写分区下 `allocate_batch(10)` 的分区序列**严格交替**;`cargo clippy -p antfs-router -p antfs-frontend
  -p antfs-bench-router --all-targets` 无告警。**真集群多盘 IOPS 摊开待压测确认**(tasks 未勾项)。
- **文档**:`router.md` §2/§5、`frontend.md` 内嵌配置表、`cluster.md`(proto 片段 + §7.5 写路径时序,顺带修掉
  "`partition_id=0` 表示自动"的陈旧表述);决策档案 `openspec/changes/router-partition-round-robin/`。

## 步骤 23:router 后台刷新可写分区列表 —— 分区增减后自动调账号段池(2026-09-23)

- **问题**:步骤 22 的"每个可写分区各持一段"只在**号段耗尽时**才对一次列表 —— 分区增减(扩容建新分区、盘满转 RO、
  运维封存)后本地要等整轮号段写完才跟上;更糟的是**已不可写分区的号段还留在本地**,后续写继续往里落。
- **决策**:`Router::connect` 起一个**后台任务**(`spawn_refresher`),按 `router.partition_refresh_secs`(默认 30s,
  **0 = 关闭**)周期 `ListPartitions(state=RW)` 并调账号段池:已不在列表里的分区**丢掉号段**(余号成空洞,无害,D3),
  新出现的可写分区**补一段**。与写路径的补段共用同一份 `sync_segments`(都在号段锁里跑,天然互斥;插入只发生在
  "该分区当前没段"时,故两边并发也不会互相覆盖号段)。
  - 任务持号段池的 `Weak<Mutex<IdCache>>`:最后一个 `Router` 析构后引用计数归零,任务下一拍自行退出(不留常驻任务)。
  - 刷新失败只计 `router_partition_refresh_total{outcome="error"}` 并**保留旧缓存** —— 后台刷新是**增强**:号段耗尽的
    写路径自己会同步一次,不能让它成为正确性依赖,所以失败不返回给调用方。
  - 重建轮转顺序时**保持相位**(记下"下一个本该轮到谁",再在新顺序里找回位置):否则一次什么都没补的空刷新会把游标
    打回起点、让同一分区连出两个号 —— e2e 断言语义时抓到了这个(表现为分区序列不再严格交替)。
- **配置/指标**:新增 `router.partition_refresh_secs`;新增 `router_writable_partitions`(gauge,本地持有号段的分区数)
  与 `router_partition_refresh_total{outcome}`。`router.md` §2/§5/§6 与 `frontend.md` 配置表同步。
- **验证**:`cargo test -p antfs-router` 全绿(7 个 e2e,新增 `background_refresh_tracks_writable_partitions`:
  两分区各持一段 → 把 p0 置 RO 后其号段被丢掉、取号只剩 p1 → 新建分区后自动补段并参与发号);
  `cargo test --workspace` + `cargo clippy --workspace --all-targets` 见下。

- **补:物理 bootstrap 与对端 compact 的竞态(同日)** —— 分段拷贝**逐段按段身份取**(认不出即 404)、拷贝
  **前后各列一次封存段清单**,三条判据任一成立就**丢弃拷贝**、回落纯逻辑拉取:①取段 404/出错;②取回字节
  自述的代次与清单不符;③拷完再列清单时对端 **incarnation** 变了。对端一重写 log / 换一代副本,"同段位置"
  就指向别的记录,逐段取来的字节会横跨两代(混合快照 + 可能带回对端已删的活记录);丢弃重来是幂等的、
  必然收敛。测试:集成场景 8(拷段途中对端整库重写 → 断言 `bootstrap.segments == 0` 且最终与对端逐 key
  一致,含墓碑)。
