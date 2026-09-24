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

## 步骤 24:单盘 hang 的前置止血 + 盘维度观测 + admin 磁盘指标页(2026-09-24)

- **问题**:单进程多盘部署里,一块盘 hang 住(内核 D 态)拖垮的是**整台节点**,而且原因不止一条:
  ①引擎 IO 全是同步阻塞且**共用同一个进程级阻塞池**(tokio `spawn_blocking`,默认 512,全仓无一处配置),
  而 `spawn_blocking` 不可取消、D 态线程不可回收 —— 卡住的线程永久占住,池满则连健康盘上的请求一起排队;
  ②服务端**没有任何请求级超时**;③`worker`/`replication` 的后台 loop 是**串行跨盘**的(一块盘卡住,全体盘的
  flush/compaction/复制一起停摆);④健康探测**自己就在漏线程**(超时只放弃等待、线程照丢,零流量约 8.5h 就能
  吃满 512);⑤`heartbeat` 在异步任务里同步 `statvfs` 全盘(盘抽风 ⇒ 心跳停 ⇒ master 15s 判节点死 ⇒
  全节点再复制);⑥盘健康**只上报不使用**,请求路径/worker/复制都不看它。
- **本步只做"不改行为"的两件**:止血(探测自身泄漏 + 盘 IO 出 reactor)+ 观测(盘维度打点 +
  admin 页),把"每盘有界池 / 健康闸门 / IO 看门狗 / 坏盘撤离"整体留给
  `openspec/changes/store-disk-isolation/`(D1–D7,默认值待压测标定)。
- **探测单飞(`health.rs`)**:`timeout` 只能放弃**等待**,故唯一能控的变量是"别再往那块盘上叠新探测"。
  现在:一发探测 = 一个阻塞任务(`exists`/`write`/`read`/`unlink` 四步合并,步骤槽位 `Step` 供超时判类型);
  超时**留住 `JoinHandle`**(`StuckProbe`)→ 后续轮次 `SlotAction::Skip` 且**不写 `HealthMap`**(保持上一轮
  坏值,绝不因"没探到"回到 `HEALTHY`);线程回来则 `SlotAction::Adopt` **采纳它的迟到结果**(抖动的慢盘
  走的就是这条自动恢复路径)。线程永不回来时进程内无从得知盘况(新探测同样会卡死在同一设备队列),那属于
  内核 fail-fast / 熔断 / 重启的范畴。
- **盘 IO 出 reactor 的三处**:①探测四步(含原先在超时之外裸跑的 `unlink`);②心跳的盘容量 `statvfs`
  —— 从 `dm.refresh()` 全盘同步改为**逐盘单飞刷新**(`spawn_refresh`/`collect_refresh` +
  `DiskManager::apply_statfs`),未归的盘沿用上一拍容量值而**心跳照发**,`DiskManager::refresh` 随之删除
  (唯一调用点就是这里);③流式 put 的暂存文件 `unlink`。
- **踩到的坑(如实记)**:暂存文件删除最初改成"Drop 里丢一个分离任务",**破坏了既有契约**"响应发出 ⇒
  暂存文件已清" —— 集成测试 `record_too_big_for_segment_is_400_not_500` 当场变红。改法:`StagedFile::remove()`
  正常出口**显式等**一次(删除仍跑在阻塞池里),取消出口才靠 `Drop` 丢分离任务兜底(先 `take` 路径再派任务,
  故等待中途被取消也不会漏删、不会重复删)。
- **盘维度打点(7 项,纯观测、不做拒绝)**:`OpTimer::with_disk(&handle.disk_id)` 把每个数据面请求归到盘上,
  产出 `disk_requests_total`(rate = IOPS)、`disk_bytes_total`(成功字节,rate = 带宽)、
  `disk_request_duration_seconds`(端到端)、`disk_in_flight_requests`、`disk_slow_requests_total`
  (阈值**复用** `disk_health_timeout_secs`,不为纯观测新增配置)、`disk_probe_stuck` / `disk_probe_skipped_total`。
  口径边界写进文档:**请求级**(不是块设备 IOPS)、**含池排队**(不是 syscall 耗时)、`requests_total` 含随后被拒的
  与后台复制读(故"有 IOPS、带宽 0"是合法读数);**阻塞池自身水位刻意不打点** —— tokio 那几个 API 要
  `RUSTFLAGS="--cfg tokio_unstable"`,不为观测给全体部署加隐藏构建要求,改用每盘在飞作**排队深度代偿**。
- **admin「磁盘指标」页**:`scrape_store` 重构为**单一 JSON 构造**(成功/失败两条分支共用,键集不会漂移)+
  `disks` 逐盘明细(盘列表从样本的 `disk_id` label 枚举、跨 `op` 求和、按盘升序);新增 tab
  **「磁盘指标」/ Host disks**(`#hostdisks`)按 store 节点分组:主机级汇总(Σ IOPS / Σ 带宽 / 平均 RT /
  Σ 超时速率 / Σ 在飞 / 容量水位 + 最差健康) + 逐盘表(盘/健康/容量/IOPS/RT/带宽/超时速率/在飞/探测卡住),
  盘清单按 `(节点,盘)` 两侧并集(缺一侧显示「—」),速率走共享窗口差分(抓取失败按断线,不差分成假速率)。
- **配置**:无新增(探测开关/周期/超时沿用既有三项,慢请求阈值复用其中最后一项)。
- **验证**:`cargo test --workspace` → **430 passed / 0 failed**;`cargo clippy --workspace --all-targets` 无告警;
  `cargo doc -p antfs-store` 警告数与改动前一致。新增单测均为确定性等待(探测超时用**零超时**而非 `sleep`;
  刷新单飞用 channel + `yield_now` 有界轮询)。**真集群**:`antfs-devcluster --disk-health` + frontend POST 流量
  → 手算核对 `disk_bytes_total 720896 = 8×8192 + 40×16384`、`disk_requests_total 524 = 8 put + 276 replica_metadata
  + 240 segments`,admin `/api/metrics` 的 `disks` 与之一致;浏览器实开新 tab,中英双语渲染出主机分组、汇总卡、
  逐盘表与口径说明,IOPS 随后台复制流量实时变化。
- **文档**:`store.md` 新增 §12.11(探测单飞 + 三个 reactor 阻塞点 + 7 项指标 + 口径边界 + 无新增配置);
  `cluster.md` §9 store 指标表 +7 行、S2 段落补探测单飞、§11 补 `/api/metrics` 的 `disks` 契约与新 tab;
  决策档案 `openspec/changes/store-disk-isolation/`(proposal / design / tasks / spec,`openspec validate --strict` 通过;
  其中"前置 commit"一栏如实标注本步实际落地到哪一格)。
- **补:流式 put 的暂存目录可配(`store_server.staging_dir`)** —— 中转文件默认落**各数据盘的 `tmp/` 子目录**
  (逐盘隔离:哪个盘上的分区,就暂存在哪个盘上),配了该键则本节点**所有盘共用一个暂存目录**(要把大对象中转
  从数据盘挪到专用 scratch / 本地 SSD 时用)。实现:`DiskManager::mount_with(dirs, Option<PathBuf>)`
  (原 `mount(dirs)` 保留为"无覆盖",故既有调用点不动)、`DiskManager::tmp_dir_for_disk`
  (`ReplicaManager::tmp_dir_for` 改走它;未知盘 id 仍返回 `None`),启动挂载时创建目录(嵌套路径也建)+
  清遗留 `*.tmp`(与各盘 `tmp/` 同一收尾口径)。**代价如实写进文档**:该目录成为**单点**(它所在的盘坏 ⇒
  本节点所有分区的大对象上传一起失败;≤1 MiB 的小对象走内存路径、不经暂存,不受影响)、空间口径变成
  "在途大对象数 × 单对象大小"且**不进任何容量上报**(撑爆表现为上传失败,而不是"盘将满"告警);明确劝阻
  指向 tmpfs。**默认留空 = 落点不变**(CLAUDE.md §5.1)。测试:disk 单测 3 条(默认逐盘 / 共用目录被创建+清理+
  所有盘共用+未知盘仍 `None` / 嵌套路径自动创建)、config 单测 2 条(默认空 / 带前后空格被 Validate 拒)、
  集成 1 条(2 MiB 流式 put 成功 + 5 MiB 被拒 400,两条路径都不留残渣,且**数据盘的 `tmp/` 从不被创建** ——
  这是"确实落在配置目录里"的可判定判据)。

### ⚠️ 步骤 24 补记(地基性变更):引擎 IO 改用**每盘有界池**

> **醒目标注**:这一条不是局部优化,而是**改了"谁替我们执行磁盘 IO"** —— 影响全节点数据面。完整设计、
> 配置/监控清单、**测试证据表**与**残留风险清单**见 [`docs/design/store.md` §12.11](design/store.md)。

- **为什么必须改**:原先所有盘的引擎 IO 挤同一个进程级 `spawn_blocking` 池(默认 512,**全仓无一处配置**),
  而 `spawn_blocking` 不可取消、卡在内核 D 态的线程不可回收 —— 一块盘挂死就能把池吃满,**连健康盘上的请求
  也一起排队**(节点级不可用)。现在每块盘有自己的 worker 线程 + 有界队列:单盘损伤被限制在它自己的线程里。
- **落地形态(与最初设想的三处差异)**:①每盘**自建 OS 线程池**(不是每盘 tokio runtime);②池满**短暂排队
  后超时才拒**(不是立即拒),排队容量与准入等待上限都可配;③配置形态是**每盘一条 `data_dirs` 项**
  (`dir` + 可选 `pool_size`)+ 一个**每盘默认池大小** `pool_default_size`(不做"总预算按盘数均分"—— 那样
  加一块盘就会悄悄改掉所有盘的并发)。
- **实现中发现并修掉的**两个真缺陷**(都是"测试逼出来的",不是自嗨用例)**:
  1. **任务 panic 会打死 worker 线程**(原先只有最外层 `catch_unwind`):反复 panic 就把该盘的并发永久削到 0,
     表现为"所有请求都要等满准入超时才 503"而没人知道为什么 ⇒ 改成**逐任务**接住 panic + 大声报错;
  2. **panic 时收尾代码执行不到**:`in_flight` 永久漂高、调用方拿到的是误导性的 `closed`(503)⇒ 改成
     **记账守卫** `Drop` 收尾,并把"任务 panic"与"池关停"分成两个明确的拒绝原因(前者 500 + `join_error`
     计数,后者 503)。
- **测试**:`cargo test -p antfs-store` 97 项(其中盘池 20 项、健康探测 7 项、HTTP 映射 1 项、池满不卡住
  维护 1 项),`cargo test --workspace` 与 `cargo clippy --workspace --all-targets` 全绿。用例表与"每条守什么
  性质"见 store.md §12.11;所有等待都带失败时限(坏了当场红,不挂死 —— 挂死会拖垮 CI)。
- **如实标注的两件事**:①**吞吐收益未标定** —— 每盘 worker 数是新引入的并发上限,默认 16 是经验值,
  上线前用 `disk_pool_queued` / `disk_pool_rejected_total` 标定;②**残留风险**共 6 条(建副本前建目录与
  暂存 `unlink` 仍走共享池、已准入操作无超时、重启时排队者被明确丢弃、panic 后引擎半截写靠既有崩溃恢复兜底、
  每盘默认值未压测、UI 尚未展示真队列深度),逐条写在 store.md §12.11。


## 步骤 25:测试临时目录改为 RAII 自回收(2026-09-24)

> **醒目标注**:这是**测试基建**改动,不碰任何生产代码路径 —— 但它是"跑一次测试就往
> `$TMPDIR` 里漏几十个目录、失败时永久留下"这个长期问题的收口。改动面在**测试代码**,
> 生产 IO 语义、磁盘格式、配置默认值**一律不动**(CLAUDE.md §5.1)。

**问题(实拍)**:`/var/tmp` 下积了 336 项、175 MB 的 `ambry_*` 残留(`ambry_store_cm_*` /
`ambry_blob_*` / `ambry_verify_*` / `ambry_prop_*` / `ambry_idx_*` / `ambry_idxseg_*` /
`ambry_ls_*` / `ambry_log_*`)。根因是**测试自己拼时间戳路径 + 在函数末尾手工 `remove_dir_all`**:
删除代码在**成功路径**上,一旦断言失败或 panic 就根本执行不到,残留只增不减(而且同一测试
并行/重跑时会互相踩着看)。

**处置**:统一改成 `tempfile` 的 RAII 守卫,`tempfile::Builder::new().prefix("ambry_<tag>_").tempdir()`:

- **目录型**的辅助函数直接返回 `tempfile::TempDir`(如 `unique_dir()` / `temp_dir(tag)`),
  调用点从 `&dir` 改成 `dir.path()`;
- **单文件型**的返回 `(TempDir, PathBuf)` —— 目录必须由调用方持有,否则临时目录在函数返回时
  就被删掉、文件也没了;
- 手工的 `remove_file` / `remove_dir_all` 尾巴**全部删掉**(再留着就是双重删除 + 掩盖真实语义)。

**踩到的坑(值得记一笔)**:把 `unique_dir()` **内联**写进调用参数(如
`BlobStore::create_with(unique_dir(), ...)`)会把 `TempDir` 变成一个**临时值**,在那一行语句
末尾立刻析构 —— 目录在 store 打开前就没了。本改动把这类调用一律改成具名 `let d1 = unique_dir();`
再传 `d1.path()`。这也是"RAII 好,但临时值的生命周期只有一条语句"这条 Rust 常识在测试代码里
最常被绊的地方。

**改动部位(8 处)**:

| 文件 | 形态 |
|---|---|
| `crates/blob/tests/crash_consistency.rs` | `unique_path()` → `(TempDir, PathBuf)`(proptest,每条 case 一个) |
| `crates/blob/tests/verify.rs` | `unique_dir()` → `TempDir`(7 个用例 + 头注释) |
| `crates/blob/tests/config_metrics.rs` | `unique_dir()` → `TempDir` |
| `crates/blob/src/blob.rs`(单测模块) | `temp_dir(tag)` → `TempDir`(~35 个用例) |
| `crates/blob/src/log.rs`(单测模块) | `temp_dir(tag)` → `TempDir` |
| `crates/blob/src/log_segment.rs`(单测模块) | `temp_path(tag)` → `(TempDir, PathBuf)` |
| `crates/blob/src/index/mod.rs`(单测模块) | `temp_dir(tag)` → `TempDir` |
| `crates/blob/src/index/segment.rs`(单测模块) | `tmp(tag)` → `(TempDir, PathBuf)` |

**刻意不动的两处**(有理由,不是漏):
- `crates/smoke/src/main.rs`:`antfs_smoke.bin` 是**冒烟工具的正常工作产物**(成功路径自删),
  且用户可经 `argv` 覆盖路径 —— 换成 `tempfile` 反而让"失败时能拿到现场文件"这件事变难;
- `benchmark/blob/src/main.rs`:**已经**尊重 `$TMPDIR`(`std::env::var("TMPDIR")`),
  压测要的就是"大文件写在指定的快盘上",不能被 RAII 绑死。

**验证**:
- `cargo test -p antfs-blob` 全绿;`cargo clippy -p antfs-blob --all-targets` 无告警;
- `cargo test --workspace` 全绿 + `cargo clippy --workspace --all-targets` 无告警;
- **泄漏证明(本次改动的可判定判据)**:跑前 `ls -d "${TMPDIR:-/tmp}"/ambry_* | wc -l` = 0;
  跑完 blob 全部测试**再数仍是 0**(目录随每个用例的 `TempDir` 析构即回收);
  `/var/tmp/ambry_*` 也为 0 —— 历史残留已一并清除(只删 `ambry_*`,未动 `/var/tmp` 下其它内容)。

**收益**:测试可重复、可并行、CI 磁盘不再被慢慢吃掉;顺带把"删除代码只在成功路径上"这类
**假清理**从测试里彻底去掉 —— 断言失败时才是调试者最需要现场的时刻。

## 步骤 26:单盘故障隔离 —— 健康闸门(D2)+ IO 看门狗(D3)在 store 侧落地(2026-09-24)

> # ⚠️ 地基性变更(第二次)
>
> 步骤 24 让"一块盘挂死不再吃别人的线程"(每盘有界池);**本步骤让"一块盘挂死时,落在它上面的请求
> 快速失败、而且这件事可观测、能自愈"**。这是**行为变更**:以前这类请求会一直挂着直到客户端超时,
> 现在会拿到 `503 + Retry-After: 1`。
>
> 完整设计、配置/监控清单、**测试证据表**与**残留风险 7 条**见
> [`docs/design/store.md` §12.12](design/store.md);master 侧的 D4/D5/D6 **仍未实现**(见该节残留风险 1)。

- **为什么这两块必须成对**:只有健康闸门而没有第二个观测源,在**默认配置**下就是废的 ——
  store 的 `disk_health_enabled` **默认 false**(对齐 Ambry),于是"这块盘坏了"这件事永远没人说出口。
  所以看门狗**不受该开关约束**:探测可以关,看门狗不能跟着关,否则整条"隔离 + 撤离"链路在默认部署里全哑。
- **两层判定(本设计的要害)**:`health.rs` 给的是**单轮**结论("这一轮探测写超时了");
  **直接拿它摘盘 = 偶尔一次抽风踢掉一块好盘**(盘越忙越容易闪一下)。故拆成
  **原始轮次判定**(`verdict`,上报/排查用)与**可服务性**(`is_available()`,**闸门唯一判据**),中间隔着迟滞。
- **迟滞两头都卡**:进坏态 = 连续 N 轮坏 **或** 坏态持续 T 秒(**先到者**);复位 = 连续 M 轮好 **且**
  稳定满 T2 秒(复位比进入更严是刻意的)。混轮次时坏态**片段**收尾即清零计数 —— 否则复位后**第一个**
  坏轮就会凭残留计数再次摘盘,迟滞等于没有(有专门单测钉住)。
- **上报值域:`UNKNOWN` 取代空串**。改造前"没探过"上报空串,master 侧把空串当"可用" ⇒ "从没检查过"
  与"检查过且健康"混为一谈。现在显式报 `UNKNOWN`。
  **⚠️ 联动改动(不做就是回归)**:`disk_health_enabled` 默认 false ⇒ 默认部署里**每块盘**都报 `UNKNOWN`,
  故 master 的 `placement.rs` 必须把 `UNKNOWN` 归入**可放新副本** —— 否则默认部署**一块盘都放不了**。
  两侧各有单测守着(`placement::unknown_health_stays_usable_so_default_deployments_keep_working`)。
- **看门狗的计时点必须在闭包内**(design 点名的"最容易写错处"):端到端口径含池排队,盘正常但池忙时
  一次 5ms 的读会被量成 2s,**健康盘被判成坏盘**。落地时做了两处加强:
  ① "最久在途"用每盘 `BTreeMap<开始时刻, 条数>` **多重集**(任务完成顺序与开始顺序无关:若只记
     "最早那一发",A 先开始 B 后开始、A 先结束之后标量仍停在 A 的时刻,把健康盘的"最久在途"越算越大);
  ② 时长观测做成**守卫**(`JobTimer`)而不是"函数尾部一行" ⇒ 不变量:**每个真正开跑的 job 恰好产生
     一个时长样本**(panic 也照记),直方图与"准入后在飞数"能一一对上。
- **闸门落点与提案不同(如实记录)**:提案说"挂在既有 `OpTimer::with_disk` 的同一位置",实现改成
  **数据面中间件**。原因:① `scrub_report`/`verify` 两个 handler **根本不调** `with_disk`;
  ② 提案要求"复制读 / `replica-metadata` / `segments` **一并被闸**",而 `request_control::classify`
  恰恰**豁免**这三类 —— 挂在 `with_disk` 位置覆盖不到它们。中间件用**同一个** `ReplicaManager::get(pid)`
  定位盘(没另起寻盘逻辑),覆盖 `/partitions/{pid}/**` 全部路由;**豁免靠路由结构**保证
  (`/metrics` 与 `/admin/**` 是另外的路由,从不经过它),并额外显式判一次路径前缀 + 集成用例守住。
  副产品:闸门在 extractor 之前短路 ⇒ 被拒请求**连 body 都不读**。
- **`statfs` 刷新未归**:**不**判 `UNAVAILABLE`(与提案的字面表述不同)。设计 §3.3 自己就写了
  "`UNAVAILABLE` 只能由探测/看门狗得出,不能被正常排队现象污染" —— 一次 `statvfs` 拿不到值只说明
  "容量读数暂时不可得",不等于"盘坏"。故落到:容量沿用上一拍已知值 + 心跳照发 + 健康值如实报
  (来自探测/看门狗的状态机)。**这条差异写在 tasks.md §3 的说明里**。
- **逃生舱(进程自杀)按提案实现,默认关**:某盘池全部 worker 被超期 IO 占住且持续超时 ⇒ `abort()`。
  **单进程多盘,误杀一次 = 自造一次全节点故障**,故默认 0、配置校验强制它 ≥ 看门狗 deadline、执行前
  留一条结构化 ERROR。这条的判定是纯函数,单测直接打(`escalate = 0` 时**绝不** abort)。
- **测试**:`cargo test --workspace` 与 `cargo clippy --workspace --all-targets` 全绿。新增单测/集成
  共 30 余项,分三组:**迟滞状态机**(disk_health,注入合成时刻,零 sleep)、**看门狗数据源**
  (disk_pool:IoWatch 乱序完成、排队不计入)、**真 HTTP 数据面**(tests/disk_health_gate.rs:7 条
  覆盖"UNKNOWN 不拦"、"每条路由 503 + Retry-After + op 标签对得上"、"另一块盘不受影响"、
  "`/metrics` 与 `/admin/**` 仍 200"、"没副本仍 404"、"复位自动回归"、"后台维护跳过且可见")。
  每条都有失败时限(坏了当场红,不挂死 —— 挂死会拖垮 CI)。
- **故障注入怎么做**:`StoreServer::force_disk_unavailable(disk_id)` **不绕过任何判据** —— 它喂的是与
  生产探测/看门狗**完全相同**的迟滞状态机(`bad_rounds` 次合成 `IO_TIMEOUT`),只是省掉"真把一块盘弄坏"。
  生产代码里没有任何调用点,误用代价写在方法文档里。
- **文档**:`store.md` §12.11(步骤 24)与 **§12.12(本节)** 都带 ⚠️ 横幅;`cluster.md` §9 配置/监控表
  新增 6 个配置键 + 5 个指标,新增 **§9.2 部署前提:设备级 fail-fast(D7)**(SCSI/NVMe/multipath/NFS
  超时 + supervisor `Restart=always` + 建议打开引擎自关);README 双版新增"单盘故障隔离"四层表。
- **仍未做(如实标注)**:master 侧 **D4(从坏盘撤离)**、**D5(判活迟滞)**、**D6(每 tick 再复制预算)**
  未实现 —— 也就是说坏盘上的副本现在**只被"不再接新副本"避开,没人把它搬走**;一次抖动仍可能让 master
  把节点判死并触发**无上限**的再复制。另外 4.3 的"盘判坏后丢弃该盘池里已排队的请求"、admin 页接入
  真队列深度与健康状态,以及全部阈值的压测标定,都还没做(逐条列在 store.md §12.12「十」与 tasks.md)。

## 步骤 27:master 侧盘故障处置 —— 判活迟滞(D5)+ 再复制预算(D6)+ 从坏盘撤离(D4)(2026-09-24)

> **⚠️ 地基性变更(第三次)**:步骤 26 让"一块盘坏"在 **store 侧**被隔离;本步骤补上 **master 侧**那一半 ——
> 否则前面的隔离只做到"不再往坏盘放新副本",**没人把坏盘上已有的副本搬走**,而且判活仍是单窗口
> (一次心跳抖动 = 判节点死 = **无上限**地再复制)。三条一起落地后才闭环:
> **坏盘上的副本会被自动搬走 · 一次抖动不再判死节点 · 再复制有速率上限**。
>
> 三处默认值都偏保守:撤离**默认关**、判活迟滞 3 拍、每 tick 预算 4。设计见
> [`cluster.md` §7.7](design/cluster.md) 与 `openspec/changes/store-disk-isolation/` §5/§6。

- **D5 判活迟滞**(`master.node_down_debounce_ticks`,默认 3):节点被判 down 需要**连续 K 个 reconcile
  tick** 都观察到心跳过期,有效判死窗口从 15s 拉长到 ≈30s。为什么必要:3s 心跳在 30s 窗口里本应有 10 拍,
  **少一两拍不该等于死亡**,而判死的代价是"该节点承载的全部分区一起搬数据"(可能是几万个)。
  连击**单调累积**:收到心跳才清零,长期 down 不会被清零。
- **D5 的坑(差点写进去,单测钉死)**:第一版清理连击用的是"保留 `up` 集合里的节点",而判 down 的节点**不在**
  `up` 里 ⇒ 连击被清掉 ⇒ 下一拍变成"第 1 拍未达标 ⇒ 先当它活着" ⇒ 节点在 up/up/down 之间**每 K 拍翻转一次**,
  每次翻转把全节点再复制又触发一遍 —— 正是迟滞要防的动荡,只是换了个形式。修法是只清"已不在册"的节点,
  用例 `a_long_down_node_never_flips_back_to_up` 连跑 20 拍盯死它。
- **D6 再复制每 tick 预算**(`master.rereplication_max_per_tick`,默认 4):按 `PartitionId` **确定性**取前 K,
  其余下拍继续;被压下的量暴露成 `master_rereplication_deferred` —— 让"我们正在**限速**"可见,
  而不是表现为"系统不动了"。注意它限的是**每 tick 数量**,不是 `balance_max_in_flight` 那种并发信号量。
- **D4 从坏盘撤离**(`master.disk_evac_enabled`,**默认关**):把已有 evacuation 闸门的作用域从"整台机下线"
  放大到"`(node, disk)` 粒度",保留**先补后删**。四条实现要点(每条都是踩出来的):
  1. **坏盘撤离的删源闸门比节点下线更严** —— 只有"由坏盘触发"的分区才额外要求"数得上的每一份都已
     `ACTIVE`"。节点下线是**运维主动、看着做的**动作;坏盘撤离是**自动**动作,且此刻新补的副本很可能
     还在 `BOOTSTRAP`,把"还在追平的那份"算成已有数据 ⇒ 删掉**唯一一份可用数据** ⇒ 丢数据。
  2. **内部算两个健康计数**(`healthy` 决定要不要补 / `caught_up_healthy` 决定能不能删)。这不是冗余:
     只用严格口径决定"要不要补"⇒ 替代副本一进 placement 就不算数 ⇒ **每 tick 重补一发**,RF 静默膨胀
     (实测踩到);只用宽松口径决定"能不能删"⇒ 删源丢数据。两数之间的窄缝 = "替代已就位、还在追平"
     ⇒ 本拍**什么都不做**。
  3. **在途预算用 leader 本地 `evac_started` 记账**,不是"硬放置里含坏盘副本的分区" —— 后者在开出
     **第一发之前**就成立,会把预算一开始占满,于是**永远开不出第一发**(实测踩到)。
  4. **持续证据**:只认 `UNAVAILABLE`(store 迟滞的产物)且**已持续** ≥ `disk_evac_min_bad_secs`
     (默认 300s);健康值一变就重置计时,故反复抖动的盘永远攒不满 —— 抖动中的盘不该被搬。
- **测试**:`cargo test -p antfs-master` 全绿。新增:`reconcile.rs` 单测 11 条(迟滞三个方向 + 预算确定性 +
  坏盘撤离四条闸门 + 持续证据)、集成 3 条(**D5 抖动不判死而持续静默仍判死**、**D6 限速可见但绝不停摆**、
  **D4 先补后删全链路** —— 后者在"继任者已就位、尚未追平"的窗口里**反复断言源仍在**,把"删源丢数据"
  那道闸门钉死)。全部有失败时限,不用 sleep 当同步。
- **文档**:`cluster.md` §7.7 配置表 +3 / 监控表 +2,§9「待续」改写(store 侧与 master 侧全部标已落地);
  `store.md` §12.12 残留风险 1 从"只做了一半"改为"已补齐";README 双版四层表把 D4/D5/D6 标为已落地。
- **仍未做(如实标注)**:`DROPPED` 删目录回收;admin「磁盘指标」页接入真队列深度与盘健康状态;
  **全部阈值的压测标定**(`pool_default_size` / `disk_watchdog_ms` / 迟滞三档 / 撤离门槛 / 预算,
  全是经验初值);store 侧 4.3「盘判坏后丢弃该盘池已排队的请求」。
- **已知不一致(记录在案)**:节点下线与坏盘撤离的删源闸门严格程度不同(见要点 1),偏保守一侧;
  统一到严格口径会改变已上线的下线语义,不在本次范围内。

### ⚠️ 步骤 27 补记:admin「磁盘指标」页接入**真队列深度**与盘健康状态(2026-09-24)

> **醒目标注**:这条把步骤 24 留下的两个"占位"换成真值 —— 之前那一页的「在飞(排队代偿)」是**代偿**,
> 现在有真队列深度了;盘健康状态也第一次在页面上可见。

- **后端**(`crates/admin/src/server.rs::disk_json`):逐盘新增 `pool_workers` / `pool_queued` /
  `pool_in_flight` / `pool_rejected_total` / `health_state` / `io_watchdog_trips_total` /
  `worker_skipped_total` / `io_sum_seconds` / `io_count`。
  `health_state` 取 `disk_health_state{disk_id,state}` 里**值为 1 的那一个 `state`**(那是一族 0/1 gauge,
  求和没有意义);取不到任何样本 → 返回 **`UNKNOWN`** 而不是空串或 `HEALTHY` —— 与 store 侧"从未观测"
  同一口径,**不谎报健康**(否则页面上"没数据"会被读成"盘很好")。
- **前端**(`HostDisksTab.tsx` + `api.ts` + `i18n.ts` 双版):逐盘表新增两列并排的 RT ——
  **端到端**(含池排队)与 **syscall**(闭包内,不含排队),把"用户看到多慢"与"这盘自己快不快"摆在一起
  (池忙而盘健康时两者会明显分叉,这正是慢盘排查的第一眼);新增**队列**(`queued / workers`,真队列深度)、
  池在飞、池拒绝/看门狗/后台跳过作为副标题计数;健康列 pilling 四色(HEALTHY / UNKNOWN / 其它故障 / UNAVAILABLE),
  主机汇总加「Σ 队列深度」与「RT(syscall)」两张卡。
- **验证**:`npx tsc --noEmit` 无错、`vite build` 成功,并**逐字符串核验产物**:重建后的
  `dist/index.html` 里确实含 `pool_queued` / `health_state` / `队列深度` / `RT(syscall` /
  `io_watchdog_trips_total` / `hostSumQueue`(单文件内联,`include_str!` 直接吃到新构建)。
- **仍未做**:分位点(页面只算平均 RT —— `antfs_metric::parse` 刻意极简、不聚合 `le` 桶);
  store 侧 4.3「盘判坏后丢弃该盘池已排队的请求」。

### ⚠️ 步骤 27 补记(二):坏盘这条链收口(统一删源闸门 + 4.3 丢弃排队请求 + 标定手册)(2026-09-24)

> **醒目标注**:这一步把"坏盘处置"这条链上剩下的三个子项做完,并对**两个已知不一致**做了决断。
> 至此 D1–D7 全部落地;慢盘(按 op 分档、`DEGRADED` 中间态、router 延迟选副本、分位点)按用户要求
> **另起一轮**,不混在这里。

- **① 统一删源闸门(消灭一处已知不一致)**:原先只有"坏盘撤离"要求"替代副本**已追平(`ACTIVE`)**"才删源,
  "节点下线"只按"存活副本数 ≥ RF"。现在**两条路径一视同仁**都要 `ACTIVE`。
  理由很硬:新补的副本可能还在 `BOOTSTRAP`,只按存活数删源等于把"还在追平的那份"算成已有数据 ⇒
  可能删掉盘上**唯一一份可用数据**(CLAUDE.md §3.1)。代价是下线变慢一点(等追平),按"宁可迟,不可早"接受。
  随之把 `decommission_evacuates_replicas_add_before_remove` 集成用例改成**节点持续上报 `ACTIVE`**
  (真实集群里 store 本来就一直上报 `current_state`,原用例发空副本列表是简化过度)。
- **② 4.3 盘判坏后丢弃该盘池已排队的请求** —— **落地形态与提案不同**:
  - 不是"状态变化时置标志位 + 清队列",而是**任务开跑前查一次实时健康状态**,命中即丢弃、连 syscall 都不碰。
    **为什么换**:标志位要**同步**,同步总有一拍延迟 —— 盘**恢复**之后那一拍里所有任务都会被误拒
    (设计时真踩了这个陷阱)。查实时状态没有这个窗口。
  - 结果通道改成 `Result<T, PoolReject>`:让"没跑就被丢"(`DiskUnavailable`,503 + 退避)与"跑了但 panic"
    (`JobPanicked`,500 + `join_error`)**精确区分**。用池级标志位猜原因会把**引擎 bug 误报成"盘忙,换个副本就行"**。
  - 四个新单测盯住:排队那发被丢且**确实没碰盘**(用 `AtomicBool` 证伪"其实跑了")、**已开跑**的那发照常完成
    (硬边界:进 syscall 就取消不掉)、**恢复后立刻**恢复服务、以及 panic 归因不被抢走。
- **③ 阈值标定:写手册,不填数字**。`cluster.md` 新增 **§9.3 阈值标定手册(待执行)**,把盘故障链上**12 个**
  数字逐个列出:要测什么、用哪条查询(全部用**闭包内**口径,绝不用端到端口径去定隔离阈值)、**判据是什么**
  (偏小/偏大的后果各是什么),并写明标定完成后必须做的**三步收口**(改代码 `impl Default` / 改文档表 /
  改钉住默认值的单测)。
  **为什么只写方法不写数字**:数字必须来自真实机器上的真实负载 —— 抄一组数字进来填上,比留着"未标定"更危险。
  同时把 master 侧那 5 个新默认值也用单测钉住(`config::tests::disk_fault_defaults_are_pinned_pending_calibration`),
  改默认值必须显式改测试,不会悄悄漂。
- **一处自造的测试缺陷(记一笔)**:`a_panicking_job_still_reports_job_panicked_even_when_the_disk_is_unavailable`
  第一版**在放行 worker 之前就 `await` 那一发排队任务** ⇒ 自己把自己锁死(排队任务要等 worker 腾出来才会被
  取走判死)。改成"先 spawn + 等它真的入队 → 放行 worker → 再 await"。这类死锁在 CI 里表现为"跑不完",
  比断言失败更耗时间。
- **测试**:`cargo test --workspace` 与 `cargo clippy --workspace --all-targets` 全绿;新增/改写单测 6 条 +
  集成用例 1 条;`openspec validate --strict` 通过。

## 步骤 28:慢盘处置(与"坏盘"分开的一轮)(2026-09-24)

> # ⚠️ 这一轮改的是**判定口径与路由策略**,不是"再补一个止血阀"
>
> 上一轮(步骤 27)把"盘坏"这条链做完了 —— 但那条链只认**明确的故障**(探测失败 / syscall 超期)。
> 一块 p99 从 1ms 涨到 2s 的盘:离 30s 的看门狗 deadline 差得远 ⇒ **既不会被隔离,也没人停止往它放副本**
> —— 它就这么一直烂着。这一轮补三件事:**给不同 op 各自的尺子、加一档"慢但可用"、
> 让客户端先试快的副本**。

- **① 按 op 分档的看门狗门槛**(`disk_watchdog_ms_by_op`,默认空 = 全部用 `disk_watchdog_ms`):
  compaction / flush 要拷整段,天然比一次 4KiB 读**慢几个数量级**;共用一个门槛时只有两个坏选择 ——
  要么门槛大到"真死的盘半天不被隔离",要么小到"健康盘的 compaction 天天误报"。分档后每个 op 用自己那把尺子。
  实现上 `IoWatch` 从"按盘"改成**按 `(盘, op)` 分层**记账;看门狗**逐 op** 检查;一块盘多个 op 同时超期只记
  一次"这块盘有问题"(`tripped` 按盘去重),但每个 op 各计一次 trips(看得出是哪类 IO 在超期);
  tick 周期取**所有门槛的最小值**,故给某 op 设更紧的门槛不会被默认周期拖慢。
  **拼错的 key 在配置校验期就被拒**(否则"设了门槛却毫无作用"这种错在运行期完全无声)。
- **② `DEGRADED` 中间态(慢但可用)**:池在闭包内按 `disk_degraded_io_ms` 给每一发 IO 分类
  (**增量计数**,不是直方图差分 —— "慢 IO 占比"要精确的分子分母),看门狗每轮**读走并清零**
  (`swap(0)`,保证每发 IO 只被一轮统计到)算占比,喂给**第二台迟滞机**。三条语义**必须记清**:
  **① 该盘仍然服务(绝不 503)、② master 不再往它放新副本、③ 不触发撤离**(撤离只认 `UNAVAILABLE`,
  避免"有点慢就搬 GB 数据")。默认 `disk_degraded_io_ms = 0`(**关**,先观测一周期再开)。
  - **实现上做了一次干净的重构**:把迟滞逻辑抽成 `Latch`,两台机器(可服务性 / 延迟劣化)共用**同一套测透的
    实现** —— 含"计数不跨坏态片段泄漏"、"长期坏不翻转"、"复位比进入更严"这些坑,而不是把那段抄第二遍。
    抽完后原有 17 条 `disk_health` 单测**一条不改就全绿**,证明行为不变。
  - 本轮的 IO 本轮没有就不产生结论(`None`):**空闲 ≠ 慢**,与探测"跳过轮不覆盖健康值"同一条纪律。
  - 有一条例用**真跑慢 IO + 真看门狗任务**端到端验证,最后一条断言是
    `is_available` **仍为真** —— 若哪天有人把 DEGRADED 接到闸门上,这条会红。
- **③ 客户端侧:延迟感知读路由**(`router.latency_aware_routing`,默认 `true`)。
  读恒为 R=1(第一个成功即返回)⇒ **试副本的顺序直接决定读延迟**,而慢副本正是最该被绕开的东西。
  路由顺序变为 **`up` 优先 → 观测延迟(EWMA,alpha 0.2 写死不做配置)→ master 原序**。
  **为什么敢默认开着**:它**只改顺序、不改候选集** —— 读的候选、写的 W 多数派、冲突解决全都不变,
  **不可能引入新的正确性问题**;而且没有测量数据时它是**恒等变换**(全没测 ⇒ 键相等 ⇒ 保持 master 序)。
  两个细节:未测副本按**已测均值**当先验(不奖励也不惩罚新人);`up = false` 的副本**永远排最后**,
  哪怕它历史最快。`alpha` 刻意不做配置项 —— 这个排序错了的代价只是"这次读多花一点",
  不像 `disk_watchdog_ms` 错了就摘盘,不值得为它加旋钮。
- **诚实边界(写在文档里,不假装做到)**:顺序变化会改变"你**先**看到哪个副本的状态"。对象不可变 +
  删除是墓碑 ⇒ 滞后副本可能暂时读不到(404 → 试下一个)或暂时看到已删对象仍活着 —— **这个风险在任意
  顺序下都存在**,本节只改变"先撞上哪一个",不引入新的语义类别。
- **测试**:`antfs-store` 单测 126 条(本轮 +10)、`antfs-router` 单测 10 条(本轮 +6)、全部有失败时限。
  `cargo test --workspace` / `cargo clippy --workspace --all-targets` 结果见本轮收口报告。
- **文档**:`store.md` §12.12(五、看门狗末尾补按 op 分档与延迟观测;一、两层判定扩成三档;
  配置表 +2;残留风险 5 从"未标定"改为"已由按 op 分档解决,仍待标定的是那档取多大"),
  `cluster.md` 新增 **§9.3 慢盘:客户端侧的延迟感知读路由** 并把标定手册顺延为 **§9.4**(补 DEGRADED 那一行),
  `router.md` 配置表 +1。
- **有意没做**:`disk_degraded_io_ms` 的**按 op 分档**(目前一个阈值管所有 op —— 若将来要按 op 判"慢",
  那才是该扩的时候);`DEGRADED` 触发**从慢盘搬副本**(撤离只认 `UNAVAILABLE`,避免 churn —— 这条是**决定**,
  不是遗漏);admin 的分位点(交给另一个改动)。

### ⚠️ 步骤 28 补记:admin 逐盘延迟分位点(p50 / p99)(2026-09-24)

> **醒目标注**:这一条就是上一段末尾说的"交给另一个改动"的**那个改动** —— 步骤 24 起「磁盘指标」页的
> RT 一直只有**平均**,平均恰恰是最会**掩盖长尾**的那个数;现在 RT 两列都是**平均 / p99**,p50 在 hover 提示里。

- **为什么补(不是凑指标)**:「平均 3ms、p99 900ms」与「平均 3ms、p99 5ms」在**平均那一列长得一模一样**,
  可前者已经在超时了。盘 hang 是"少数请求极慢 + 大量请求正常"的形态 ⇒ 平均天然看不见它。
- **口径:`le` 桶线性插值**(`crates/admin/src/server.rs::histogram_quantile`,纯函数,无 `unwrap`/`panic`):
  按 `le` **逐盘跨 op 求和**桶计数 → 单调化(前缀最大值,防瞬时导出非单调)→ 找目标秩所在桶 → **桶内线性插值**,
  照抄 Prometheus `histogram_quantile`。跨 op 求和是为了与同一盘的 `_sum`/`_count` **是同一批观测** ——
  否则"平均"与"p99"来自两批样本,并排看会误导。**两把尺子照旧分开**:`rt_*`(端到端,含池排队)与
  `io_*`(syscall 闭包内)各自出各自的分位点,`parse.rs` **一个字没动**(桶的聚合在 admin 侧)。
- **边界(如实标注)**:最高桶是 `+Inf`,秩落在里面时**只能给下界**(= 最高有限 `le`,即"p99 ≥ X"),
  **不声称超出最高桶的精度**;没有桶样本 / 总计数为 0 → JSON `null` → UI 显示「—」而不是 `0.00 ms`
  (**没数据 ≠ 零延迟**,与健康列"不谎报 HEALTHY"同一条纪律)。store 侧 IO 桶铺到 120s ⇒ 超过 120s 只能得到"≥ 120s"。
- **后端**:`disk_json` 新增 `rt_p50_seconds` / `rt_p99_seconds` / `io_p50_seconds` / `io_p99_seconds`。
  新增单测 3 条:纯函数边角(空输入、总计数 0、只有一个桶、`+Inf` 兜底、边界上的秩、非单调计数、NaN 参数)、
  桶聚合(跨 op 求和 + 别的盘/漏 `le` 的畸形样本不混入)、以及端到端一条(同一响应里算出 p50 = 0.0075 /
  p99 = 0.01 下界 / syscall 那套 0.55 与 1.0 下界)。
- **前端**:`api.ts` 加 4 个字段(带中文口径注释);`HostDisksTab.tsx` 的 RT 两列改为「平均 / p99」
  (**不新增列**,复用原两列;单侧缺数据该侧显示「—」),主机汇总两张 RT 卡也带 p99 —— 主机级 p99 取
  **该主机最差盘的 p99**(分位点不能相加,它是主机 p99 的**上界**,比伪造一个"平均 p99"诚实);
  `i18n.ts` en/zh 双版更新表头、hint 与 `hostDisksNote`。
- **验证**:`npx tsc --noEmit` 无错;`vite build` 成功;`cargo test -p antfs-admin` **57 passed, 0 failed**;
  `cargo clippy -p antfs-admin --all-targets` **0 告警**;重建后**逐字符串核验产物** `dist/index.html`:
  `rt_p99_seconds`(×2)/ `io_p99_seconds`(×2)/ `rt_p50_seconds` / `io_p50_seconds` / `RT 端到端(平均 / p99 ms)` /
  `RT syscall(平均 / p99 ms)` / `RT 端到端(平均 / p99 · 最差盘)` / `p50 = {v}` 全部命中(单文件内联,`include_str!` 直接吃新构建)。
- **文档**:`cluster.md` §11 的"磁盘指标页"更新块(原句"仍未做:分位点"已改写)+ `store.md` §12.12 残留风险 7
  (标为已做,并写明桶插值 / `+Inf` 下界 / **不把 `le` 桶传出去**这三个边界)。
- **有意没做(记一笔)**:**不做分位点热力图,也不做 p99 趋势折线**(图表库仍是现成那套、不新增依赖;
  趋势要看 Prometheus + Grafana);**不做按 op 拆分的分位点**(JSON 只出逐盘聚合数,要看 `put` 与
  `compaction` 各自的长尾得先让桶出到 admin 层 —— 那是一次契约扩张,不顺手夹带);**不聚合 `le` 到分位点之外**
  (admin 不吐桶数组,故前端无法跨盘/跨节点合并分位点,主机级只能取最差盘)。

## 步骤 29:router 发号按 partition 写权重分配(2026-09-24)

> # ⚠️ 这一轮改的是**分配口径**,不是"再加一层缓存"
>
> 步骤 22 把发号从"单分区号段连续耗尽"改成"每个可写分区各持一段 + 轮转",并在那一轮的 proposal 里**自己
> 记下了一处偏离**:D10 定的是"partition 由 master **按剩余容量加权**挑",落地却成了客户端**均匀轮转** ——
> 全局容量感知被弱化。这一轮把它**收口**:router 发号按 master 下发的 partition **写权重**加权分配。
>
> 关键不在"加个字段",而在**取号环节必须按权重挑**:只把段大小按权重切、取号仍顺序轮转是**不成立**的
> (论证见下)。本轮把 master 侧的容量权重提成对外字段,并让发号本身变成**平滑加权轮转(SWRR)**。

- **协议:加字段,不动既有字段**。`PartitionEntry` 新增 `uint32 write_weight = 10;` —— **千分比 `0..=1000`**,
  `1000` = 该分区完全空,越接近 `1` 越满;`0` = **未下发 / 无意见 → 客户端按均权处理**;**计算值下限固定为 1**,
  所以 `0` 永远不会表示"零分配",可以安全地让给"未下发"。客户端只用**相对**权重,容量口径(policy)只活在 master。
  **前后兼容(CLAUDE.md §3.4)**:旧 client 忽略新字段;新 client 接**旧 master** 时字段回 `0` → 归一成 `1` →
  行为与改动前**逐位一致**(兼容落点就是 `list_writable` 里的 `write_weight.max(1)`)。
- **master:一个定义,两处复用**(否则集群里会有两套自相矛盾的容量权重)。新增纯函数
  `capacity_weight_permille(cap, used) -> u32`(内部 `u128` 乘防 `cap = u64::MAX` 溢出;下限 1;`cap == 0`
  即未配容量上限 → `1000` = 无意见)与常量 `WEIGHT_PERMILLE_MAX = 1000`,同时用于:① `partition_id` 缺省时
  `pick_writable_partition` 的加权随机(口径由"字节余量"统一为千分比);② `list_partitions` 下发的 `write_weight`。
  `used` 口径 = `SoftState::used_bytes_by_partition()`(**每分区取各副本最大值**,RF 不重复计数)。权重是**软状态
  派生视图,不进 Raft / apply**(切主后由心跳重建)。
- **为什么必须 SWRR(本轮最该写清的一步)**:只把**段大小**按权重切、取号仍顺序轮转**不成立** ——
  顺序轮转每轮给每个分区**各 1 个号**,于是**任何中途观察窗口内各分区计数都相等**;权重只能决定"最满的那个
  何时停止发号",短窗口内的偏差方向甚至是**反的**(最满分区的段先耗尽后,剩下的号全给别的分区)。所以**取号
  本身必须按权重挑**:`take_weighted()` 每拍给在册分区各加 `weight` 信用、取最大者、再减去在册权重总和 ——
  **任意窗口**内的取号比例都按权重之比。段大小再按权重配比,只是让各分区**同步耗尽**。
  两个细节:手上没号段的**不参与也不攒信用**;同分取 pid 小的(确定、可单测);**均权时逐位退化为原顺序轮转**。
- **份额与收敛**:总预留 `pool = id_segment_size × 可写分区数`(**与改动前不变** —— 空洞上界、换段频率不变),
  `share_i = max(1, pool × w_i / Σw)`,余数按「小数部分降序,pid 升序」。下界 `1` 保证**没有 0 长度的段**
  ("这个分区不发号"该由 master 转 RO 表达)。每次补段/刷新还按新份额**收敛已有段的段尾**
  (`end = min(end, next + share)`,只减不增),收敛掉的余号成**空洞**(D3 无害),收敛到没号的段直接丢掉;
  `credit` 只留给手上仍持段的分区(新分区从 0 起,不留空转期攒下的信用变成突发)。
- **反馈滞后(如实写)**:权重来自 store 心跳上报的 `used_bytes`(`store_server.heartbeat_interval_secs` 默认 3s),
  只在**补段 / 后台刷新**那一拍生效(后台周期 `router.partition_refresh_secs` 默认 30s)。它是**心跳级的粗粒度
  容量反馈,不是精确配额**。**"收敛段尾"这一步是关键**:它把"权重改动何时生效"从"整池轮完"压到**一个刷新周期**;
  `partition_refresh_secs = 0` 时退化为"整池轮完才更新权重"(可接受:号段亏空的写路径自己会同步一次)。
- **配置 / 监控**:`RouterConfig` **不新增配置项** —— `router.id_segment_size` 语义改为"**均分时**每个可写分区
  一次申请的 blobId 个数(总预留 = 分区数 × 该值,实际按写权重配比)"。**不新增指标**;"权重是否生效"用
  `Router::write_weights()`(新增只读快照)+ 压测打印的「权重 → 期望 %」列观测。
- **测试**:`antfs-router` 单测 **14 条**(本轮 +8:均权逐位等价、20 个号按 1000:500:500 精确 10:5:5、
  权重 0 = 均权、单号段边界、段空跳过、`order` 收缩不 panic、`segment_shares` 含 `128 → [85, 43]` 与极端偏斜
  `2048 → [2046, 2]` 不产生 0 长段、段尾收敛只减不增且没号即弃);e2e **8 条**,新增 `partition_write_weight_steers_allocation`
  (**软状态直注、不依赖心跳**:p1 报用掉一半容量 → `ListPartitions` 里 `p1.write_weight == 500`、`p0 == 1000`
  → 256 个号实际 **p0=170 : p1=86**;把占用对调后**翻转为 86:170**,并断言 client 侧 `write_weights()` 快照跟随);
  `antfs-master` 新增单测 `capacity_weight_is_remaining_ratio_with_floor_one`(空 = 1000 / 半满 = 500 /
  满或超卖 = 1 / 只剩 1 字节 = 1 / `cap = 0` = 1000 / u64 上界不溢出)。
- **文档**:`router.md`(§2 写路径 + ASCII 流程 + 份额/收敛/反馈滞后/SWRR 论证、§5 配置表 + 表下"请求哪个 partition"
  的权重来源、§6 表后"权重是否生效怎么观测"、§7 差异与取舍 + 待办三条)、`cluster.md`(§7.4 `write_weight` 语义 +
  §7.5 第 ④ 条写路径时序)、`frontend.md`(内嵌配置表)、决策档案 `openspec/changes/router-weighted-partition-alloc/`。
- **有意没做**:① **盘级摊开 / 副本放置**不动(与上一轮同一判定:杠杆在 `crates/master/src/placement.rs`);
  ② **不做 admin 手动权重入口、不做 `SetPartitionWeight`** —— 本轮只留 `write_weight` 字段 + master 侧
  `capacity_weight_permille` 这个唯一插口(后期 admin → master → 同一字段下发,**客户端零改动**);
  ③ **不做按权重的号段后台预取**;**不做 507 / silenced 驱动的本地降权**。
- **已知取舍(写进文档,不假装做到)**:极端偏斜时最满的分区每轮只拿 1 个号(≈0.1%)—— 它**仍是可写分区**,
  写进去仍可能 507,**错误照旧上报**;正确处置是运维把它转 RO / 封存。SWRR 每次发号 **O(n)**
  (n = 本地持段分区数),十万级分区需换 O(log n) 结构(记待办)。
- **验证**:`cargo test -p antfs-router`(单测 14 + e2e 8)、`cargo test -p antfs-master`(lib 85 + 各集成套件,
  含 `blob_ids` 里的容量加权用例)、`cargo test --workspace` 全绿;`cargo clippy --workspace --all-targets` 通过,
  本轮触及的三个 crate(`antfs-router` / `antfs-master` / `antfs-bench-router`)**无新增告警**。

## 步骤 30:devcluster 支持配置文件 + 本地分区数下调(2026-09-24)

- **为什么**:本地集群参数只能靠命令行,于是"本机盘偏满"这类**环境差异**每次都得重敲一遍
  (`--disk-near-full-ratio 0.95`);而默认建 6 个分区、每个 store 都要多开几份引擎,对纯联调是浪费。
- **做法**(`crates/devcluster/src/config.rs`,新):
  - 参数来源统一成**内置默认 < 配置文件 < 环境变量 `AMBRY_*` < 命令行**,复用仓库同一条 loader
    (`antfs_config::Loader`);键名即字段名,单层 flat。样例:仓库根 `devcluster.toml.example`(入库)。
  - 文件来源:显式 `--config PATH`(不存在即报错),或**当前目录下的 `devcluster.toml`**(存在才读)。
    用了哪个文件会打日志、并在启动横幅的"参数来源"里回显 —— 隐式读取必须看得见。
  - `#[serde(deny_unknown_fields)]`:拼错的键当场拒(不静默回落默认值),报错列出全部合法键。
  - 解析期校验:`rf > store_count`(放不下)、`partitions` / `bench_*` 非正、`disk_near_full_ratio` 不在 (0,1]、
    `run_bench` 不在枚举内 —— 都在启动前报错,不再等 20s 选盘失败。
  - 命令行解析改为**返回值**(不再 `expect` panic),错误带上 flag 名与给的值;`--run-bench` 的枚举校验
    收敛到 `Validate`。`print_summary` 的 8 个位置参数收成一个 `Banner` 结构体(避开 clippy 参数上限)。
- **默认值**:`partitions` **6 → 3**(3 分区 × RF3 = 9 个副本,本地联调足够)。顺带订正 help 里
  `--disk-near-full-ratio` 的错标(此前写 default 0.95,与代码的 0.8 不符)。
- **验证**:`cargo test -p antfs-devcluster` 8 条新用例(默认值自洽 / 文件只覆盖写了的键 / 显式路径缺失报错 /
  错键被拒 / 校验拒 RF>节点数 / 命令行覆盖文件 / 坏值点名 flag / 未知 flag 不 panic)。
  真机冒烟三连:① `--config /tmp/dc-e2e.toml` → `partitions created: [0,1,2]`、`replicas 9/9`、0 ERROR;
  ② 无 `--config` → 自动读 `./devcluster.toml`,横幅回显 `devcluster.toml (+ 命令行覆盖)`;
  ③ 错键配置 → `unknown field \`partitons\`, expected one of ...`,exit 2。
- **边界**:`devcluster.toml` 进 `.gitignore`(各机不同);样例文件入库作为格式存档。

## 步骤 31:上线前真机核对清单落文档([`cluster.md` §9.5](./design/cluster.md))(2026-09-24)

- **为什么**:盘故障处置这条链的验收证据**全是合成故障注入**(`force_disk_unavailable` 喂的是与生产相同的
  迟滞状态机,但**造不出真 `D` 状态**、也不经过内核 fail-fast)。"只能在真机上做"的事此前散在 §9.2、
  §9.4 与 openspec 的未勾任务里,没有一张可执行的清单。
- **落点**:[`cluster.md` §9.5](./design/cluster.md) 新开一节(**⚠️ 标注"条目全部还没在真机上做过"**),分五组:
  - **A 逐机部署前置**:设备级 fail-fast、`io_error_count_to_trigger_shutdown`(建议显式开 3~10)、进程守护、
    `data_dirs` 逐机核对、`staging_dir` 决策、线程预算(`pool_default_size × 盘数`)、观测通道不被 LB 挡。
  - **B 真机故障注入验收**(9 条表格):首条就是**最初那个问题** —— 单盘 IO hang 时,同进程内其他盘的
    P50/P99/IOPS 必须与无故障基线一致;其余覆盖单盘 EIO、慢盘、池满、逃生舱、坏盘撤离、节点抖动、
    掉电/内核 panic、再复制预算。
  - **C 标定与告警接线**:§9.4 的 12 个数字 + 告警指标(全名)+ admin 逐盘与 `fio`/`iostat` 对账 +
    读路由收益的 A/B 压测。
  - **D 上线前对齐的"不做"**:同步阻塞 IO / 已进 syscall 的 IO 不可取消 / 不自动回填 / 不自动换硬件 /
    单进程多盘 abort 会带走健康盘 / `ensure_replica` 与暂存 `unlink` 仍走进程级池。
  - **E 仍缺端到端证据的三处**:盘恢复不自动回填、游标失效、`UNKNOWN` 参与撤离判据的可达性。
- **交叉引用**:§9.2 与 §9.4 指向 §9.5;README 中英各一句;`store.md` §12.12 配置表下方注明
  "只能在真机上做的那些见 §9.5"。
- **说明**:本节只列**待执行动作**,不含新代码/新配置;执行过程中拿到的数字按 §9.4 末尾的三步收口
  (改 `impl Default` → 改文档表并写标定日期与机型 → 改钉住默认值的单测)。

## 步骤 32:devcluster 建分区改幂等(补到目标数,复跑不叠加)(2026-09-24)

- **踩到的坑**:步骤 30 把本地默认分区数降到 3 之后,复跑同一个数据目录会发现分区**又变成 6 个** ——
  `CreatePartitions(count)` 每次都会**新建** `count` 个,而 devcluster 的数据目录是保留的(退出不删)。
  于是"把本地分区数调低"被"每跑一次 +N"抵消掉了,越跑越多。
- **改法**(`crates/devcluster/src/main.rs`):
  - 建分区前先 `ListPartitions(limit=0)` 问已有多少,`missing = partitions - existing`,只补差额;
    建完**再问一次**,横幅与"应有副本数"(`expected = 分区总数 × RF`)都按**总数**算(复跑时本次没建、
    但总数并不为 0,否则会打印 `expected=0` 这种假就绪)。
  - 日志改成 `partitions ensured: [..] existing=N created=M total=K`,三件事一眼看清。
  - `--partitions` 的语义从"启动后创建的分区数"订正为"**目标分区总数(幂等)**",help 与
    `devcluster.toml.example` 同步改。
- **实测**(同一临时目录连跑两次):第 1 次 `existing=0 created=3 total=3`;第 2 次
  `existing=3 created=0 total=3`,0 ERROR,盘上仍是 3 个 `partition_*` 目录 —— 不再叠加。
- **注意**:幂等只保证"不少于目标",**不会替谁删分区**。本地若已经跑出 6 个分区,想回到 3 个得删掉
  数据目录重来(或把 `partitions` 提到 6)。
- **验证**:`cargo test -p antfs-devcluster`(8 条配置用例)仍全绿;`cargo fmt` 干净。
