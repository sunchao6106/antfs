# store-compaction-policy

定时 compaction 的**候选集**按分区可写态挑选、按本地低峰钟点调度,并把回收范围从"被取代的旧记录"扩展到 **TTL 过期对象 + 过保留期的墓碑**。

## ADDED Requirements

### Requirement: compaction 可回收口径

compaction SHALL 把以下三类字节视为可回收并真正释放:**被更新版本取代的旧记录**、**TTL 已过期且未被回收的对象**、**删除已满保留期的墓碑**。墓碑的删除时刻 SHALL 随记录落盘(复用索引条目的 `expiration_ms` 槽位,磁盘布局不变),使重建/复制后判定一致。

#### Scenario: 过期对象被回收

- **WHEN** 某副本存在 TTL 已过期对象且该分区的可回收占比达到阈值,执行 compaction
- **THEN** 这些对象的记录 SHALL 从 log 与索引中消失(读接口由 `Expired` 变为 `NotFound`),`expired_reclaimed_bytes` SHALL 大于 0

#### Scenario: 墓碑保留期内不回收

- **WHEN** 墓碑的删除时刻距今不足 `compaction_tombstone_retention_secs`
- **THEN** 墓碑 SHALL 被保留(已删对象仍报 `Deleted`),`tombstone_reclaimed_bytes` SHALL 为 0

#### Scenario: 旧格式墓碑永不自动回收

- **WHEN** 盘上墓碑的 `expiration_ms` 为 `-1`(改造前的数据,删除时间未知)
- **THEN** 该墓碑 SHALL 可读且 SHALL 永不被自动回收(即使保留期配为 0)

### Requirement: 分区级闸门与段级选择

compaction SHALL 先按 partition 整体的**可回收字节占比 ≥ `compaction_dead_ratio`** 做闸门,再对达阈的**非活跃段**(活跃段不动)按同一阈值选段压缩。可回收占比 SHALL 用索引遍历聚合(不读数据体),口径包含 `seg_bytes` 死字节与可回收赢家字节。

#### Scenario: 占比不足不压缩

- **WHEN** 分区的可回收占比低于阈值
- **THEN** compaction SHALL 不做任何段重写(返回 `None`),盘上字节 SHALL 不变

### Requirement: 候选集按分区可写态

store 本地维护 SHALL 按 master 下发的分区状态挑选 compaction 候选集:`SEALED` 永不入选;`RO` 总是入选;`RW` 由 `compaction_include_rw` 决定(默认纳入,仅其封存段);状态未知(未接 master 的单机模式)SHALL 按可压处理。未追平(`BOOTSTRAP`)的副本 SHALL 跳过。

#### Scenario: SEALED 分区不参与

- **WHEN** 分区状态为 `SEALED` 且存在达阈可回收数据
- **THEN** 本地维护 SHALL 不压缩该分区,盘上字节 SHALL 不变

#### Scenario: RW 分区由开关决定

- **WHEN** `compaction_include_rw=false` 且分区状态为 `RW`
- **THEN** SHALL 不压缩该分区;设为 `true` 时 SHALL 压缩其封存段(活跃段不动)

### Requirement: 首跑钟点调度

定时 compaction SHALL 按「首跑钟点 + 周期」调度:首次 SHALL 在下一个本地 `compaction_start_time`(`HH:MM`)触发(今天未到即今天,已过即明天),之后每 `compaction_interval_secs` 一次。`compaction_start_time` SHALL 在配置校验阶段被校验为严格 `HH:MM`。

#### Scenario: 首跑对齐钟点

- **WHEN** 配置 `compaction_start_time="01:00"`、周期 86400s,进程在 23:00 起跑
- **THEN** 首次 compaction SHALL 在次日 01:00 触发,之后每天同一钟点一次

#### Scenario: 非法钟点被拒

- **WHEN** 配置 `compaction_start_time` 不是严格 `HH:MM`(如 `"1:00"`、`"24:00"`)
- **THEN** 配置校验 SHALL 失败并给出该 key
