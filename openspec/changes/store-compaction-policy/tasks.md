# tasks — store-compaction-policy

## 1. 变更文档
- [x] 1.1 proposal / spec delta(`specs/store-compaction-policy/spec.md`)。

## 2. 引擎：判活口径与三类回收(`crates/blob`)
- [x] 2.1 `IndexValue` 增 `ttl_expiration_ms` / `deleted_at_ms` / `is_expired_at` / `is_reclaimable_tombstone` / `is_reclaimable_at`(墓碑复用 `expiration_ms` 槽位存删除时刻,布局不变)。
- [x] 2.2 新增 `CompactPolicy{dead_ratio, now_ms, tombstone_retention_ms}` + `preserve_legacy`(历史口径,供旧测试/调用方)。
- [x] 2.3 `compact` / `compact_if_needed` / `compact_incremental` / `compact_cost_benefit` 接策略;两处判活改为"赢家 且 未过期 且 墓碑未过保留期"。
- [x] 2.4 `reclaimable_by_segment`(索引遍历聚合过期/墓碑字节 + `seg_bytes.dead`,不读数据体)→ partition 级闸门 + 段级选择同一口径。
- [x] 2.5 `delete_inner` 把删除时刻写进墓碑记录头与 `IndexValue`(复制/重建随记录带回来)。
- [x] 2.6 `stat()` 的 TTL 口径走 `ttl_expiration_ms`(墓碑不再把删除时刻当过期时刻汇报)。
- [x] 2.7 `CompactionStats` 增 `tombstone_reclaimed_bytes` / `expired_reclaimed_bytes`。

## 3. 分区状态下发(`crates/master` + `crates/store`)
- [x] 3.1 proto:`PartitionStateInfo` + `MasterDirective.partition_states = 4`。
- [x] 3.2 `reconcile::plan_partition_states`(纯函数,只报该节点上报过的分区)+ 单测。
- [x] 3.3 master 心跳把状态填进指令。
- [x] 3.4 `ReplicaHandle.partition_state` + `ReplicaManager::set_partition_state`;心跳消费 `partition_states`。

## 4. store 调度与候选集
- [x] 4.1 `worker::compact_all(policy, include_rw)`:SEALED 排除 / RW 受开关 / RO 与未知入选;BOOTSTRAP 副本跳过。
- [x] 4.2 compaction loop 改"首跑钟点 + 周期":`next_delay_secs` 纯函数 + `SystemLocalClock`(libc `localtime_r`/`mktime`,本地日历顺延一天)。
- [x] 4.3 配置:`compaction_interval_secs`→86400、`compaction_dead_ratio`→0.3、新增 `compaction_start_time` / `compaction_include_rw` / `compaction_tombstone_retention_secs` + `Validate`(`HH:MM` 校验)。
- [x] 4.4 手动 compact(同步 + 异步)改用手动策略(阈值 0,仍守保留期)。

## 5. 测试
- [x] 5.1 blob:过期对象回收 / 墓碑保留期内不回收、过保留期回收 / 重开后删除时刻仍在 / partition 闸门看可回收占比 / **旧格式墓碑(=-1)永不回收**(§3.4 兼容)。
- [x] 5.2 store:候选集状态矩阵(RO/RW/SEALED/未知 × include_rw)、BOOTSTRAP 跳过、首跑与后续节拍(假时钟,含超时立即跑)。
- [x] 5.3 master:`plan_partition_states` 只报本节点承载的分区。
- [x] 5.4 `cargo test --workspace` 全绿 + `cargo clippy --workspace --all-targets` 无告警。

## 6. 文档
- [x] 6.1 `docs/design/cluster.md`:S4 叙述 + `[store_server]` 配置表(3 改 + 3 新)。
- [x] 6.2 `docs/design/store.md`:compaction 触发与回收口径(§9.1)+ 路线图条目更新。
- [x] 6.3 `docs/DEVLOG.md` 追加实现记录。
