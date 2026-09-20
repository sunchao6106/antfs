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
- 语义对齐 Ambry:put 遇已存在且未删除的 key 报 `AlreadyExists`;delete 写墓碑记录 + 删除条目;
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
- 覆盖 newest-wins、墓碑、删后重写、跨段封存(mmap+fuse)、compaction offset 重写、水位回放重开、
  扫 log 全量重建三态收尾。
- 验证:共 27 单测 + 崩溃属性 + smoke 全绿;clippy `-D warnings` 零告警。

## 待办

- **PUT-on-deleted 语义与 Ambry 的分歧**:Ambry `checkWriteSetStateInStore` 对 `EnumSet.of(PUT, DELETE)`
  命中即 `COLLIDING/AlreadyExist`——**删除后的 key 不允许再 PUT**,复活走 `undelete`(带 lifeVersion)。
  当前 rs 允许「删后重写」。待定:是否对齐 Ambry(拒绝再 PUT + 后续实现 undelete/lifeVersion)。
- 每 Log 定容上界(容量/文件数,写满自动滚动新 partition)——设计锚点,暂缓。
- 并发/共享模型(D4):`&mut self` 单写者 → 共享实例 + `RwLock`/并发结构(运行时 tokio vs 手写线程池仍为 Open Question)。
- 死数据统计接入更精确来源(增量维护而非每次全量扫 log)。
