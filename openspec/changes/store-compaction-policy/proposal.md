## Why

S4 的本地 worker 落地后,compaction 只有"周期 reachable + 按封存段死数据占比触发"这一条粗糙规则,落地为生产运维时有三个硬伤:

1. **回收范围不完整**:引擎判活只看"是不是当前索引赢家",所以 **TTL 已过期的对象**与**墓碑本身**永远占盘 —— 过期的数据体还在、删掉的对象的墓碑还在,盘只增不减。参考 Ambry:`BlobStoreCompactor` 会回收过期对象,墓碑按 `tombstone.retention.time` 保留期后回收。
2. **触发条件不贴实际**:只看单段死数据占比,运维无法表达"只压那些确实值得压的分区",也没有可预测的低峰窗口(固定 interval 会在任意钟点开跑,与业务高峰撞车)。
3. **对象不可写之后没人管**:`RO` 分区(盘将满被 master 自动置只读、或运维手动置)是"不再写新数据"的分区,正是做空间回收的最佳对象 —— 但当前 worker 对所有分区一视同仁,"什么时候压、压哪些"无法按可写态区分。

本次改造把 compaction 从"单店定时扫一遍"升级为**按分区可写态 + 可回收占比挑候选、在指定低峰钟点回收三类数据**的运维可控机制。

## What Changes

- **回收口径(引擎,`blob`)**:
  - 墓碑记录与索引条目复用 `expiration_ms` 槽位**存删除时刻** `deleted_at_ms`(布局不变,无需索引格式迁移);
  - 新增 `CompactPolicy{dead_ratio, now_ms, tombstone_retention_ms}`:`compact` / `compact_if_needed` / `compact_incremental` / `compact_cost_benefit` 全部按它判活 —— **被取代的旧记录 + TTL 已过期对象 + 墓碑已过保留期**三类字节真正回收;
  - partition 级闸门(可回收占比 ≥ `dead_ratio`)先筛,再做段级选择(同一阈值);`CompactionStats` 增加 `tombstone_reclaimed_bytes` / `expired_reclaimed_bytes` 两个分项。
- **候选集(store,`worker.rs`)**:定时 compaction 按 master 下发的**分区可写态**挑 replica —— `SEALED` 永不入选、`RO` 总是入选、`RW` 由 `compaction_include_rw` 决定(默认纳入,只压其封存段)、未知状态(单机模式)按可压处理;未追平(`BOOTSTRAP`)的副本跳过(压了也要从 peer 拉回来,还会让复制游标失效)。
- **调度(store,`worker.rs`)**:改为「**首跑钟点 + 周期**」(默认本地 `01:00`、周期 86400s = 每天一次);等待时长由纯函数 `next_delay_secs` 算出(本地日历顺延一天,跨夏令时正确)。
- **状态下发(master ↔ store)**:`MasterDirective` 增 `repeated PartitionStateInfo partition_states`(每拍全量、只含该节点承载副本的分区);store 侧记在 `ReplicaHandle.partition_state`。
- **配置(store)**:
  - `compaction_interval_secs` 默认 `3600` → **`86400`**,语义 = 周期;
  - `compaction_dead_ratio` 默认 `0.5` → **`0.3`**,语义 = partition/段级**可回收**占比阈值;
  - 新增 `compaction_start_time`(默认 `"01:00"`)、`compaction_include_rw`(默认 true)、`compaction_tombstone_retention_secs`(默认 86400)。
- **手动 compact**:仍不受状态/阈值闸门约束(运维强制),但回收口径与定时一致(同 `CompactPolicy`)。

## Capabilities

### New Capabilities

- (无新增 capability 名;本次是 `store-local-worker` 与 `blob` compaction 语义的修改)

### Modified Capabilities

- `store-local-worker`:候选集按分区可写态 + 首跑钟点调度 + 三类数据回收。
- `blob`(store 设计 §9.1):compaction 判活口径含 TTL 过期与墓碑保留期。

## Impact

- **crate**:`blob`(policy + 判活 + 选段 + `delete_inner` 写 `deleted_at`)· `master`(proto 字段 + 心跳填状态)· `store`(worker 闸门/调度、`ReplicaHandle.partition_state`、心跳消费、配置、手动 compact 传策略)。
- **磁盘格式(§3.4)**:`IndexValue` 21B 布局与 `.idx` 文件格式**不变**,只是墓碑条目的 `expiration_ms` 语义从"恒为 -1"变为"删除时刻";**旧数据(=-1)按"删除时间未知"处理 → 永不自动回收**(安全方向),带兼容性测试。
- **持久性(§3.1)**:只新增"哪些字节不再保留"的判定,不改写路径与 fsync 边界;compaction 仍走既有 `COMPACT_COMMIT` + tmp→fsync→rename 崩溃安全实现。
- **风险**:墓碑回收 = 删掉"该 key 已删除"的唯一证据;若某副本滞后超过保留期且期间丢过 delete,修复时可能把对象复活。缓解:保留期默认 24h(可配)、只回收过保留期墓碑、文档写明"保留期应 > 副本滞后上界"。

## Non-goals

- 不做 EC 转换(`SEALED` 只是"不进 compaction 候选集";EC 仍锁定)。
- 不修 compaction 改写 offset 导致复制游标失效的既有缺口(留复制层 resetKey)。
- 不做 per-disk IO 限流(N4)。
- 不做"RO→RW 自动回退"(master 侧仍只单向置 RO)。
