# store-local-worker

数据节点常驻本地维护 worker(周期 flush / 单店 compaction),周期时长可配置。承 cluster.md §9。

> **后续变更(2026-09-23,`simplify-delete-model`)**:对象不可变 + 删除不可逆落地后,本地 worker 只有 **flush + compaction 两条 loop**。
> 保留为当时的设计记录。

## ADDED Requirements

### Requirement: 周期性本地维护

启用本地 worker 时,store 数据节点 SHALL 对本节点每个本地 replica 周期性执行 flush、按死数据阈值执行 compaction;各动作周期时长 SHALL 由配置项独立控制。同步引擎调用 SHALL 经阻塞线程池 offload,不阻塞异步 reactor。

#### Scenario: 周期时长可配置

- **WHEN** 配置 `flush_interval_secs` / `compaction_interval_secs`
- **THEN** 各维护动作 SHALL 按对应配置的周期触发

#### Scenario: 关闭开关不跑

- **WHEN** `local_worker_enabled=false`
- **THEN** SHALL 不启动任何本地维护线程

### Requirement: flush 推进持久化边界

本地 worker 的 flush SHALL 调用引擎 `flush`,把已写入 page-cache 的记录推进到持久化边界(fsync);flush 是持久化边界(§3),仅增强持久性,不改变可见数据。

#### Scenario: flush 后重开数据仍在

- **WHEN** 写入若干对象后由 worker flush,随后重开该 replica
- **THEN** 这些对象 SHALL 仍可读

### Requirement: 阈值触发的 compaction

compaction SHALL 仅在某 replica 死数据占比达到配置阈值 `compaction_dead_ratio` 时执行;SHALL 复用引擎既有崩溃安全实现,可安全重试。

#### Scenario: 回收死数据

- **WHEN** 某 replica 死数据占比超过阈值,由 worker 执行 compaction
- **THEN** 死数据 SHALL 被回收,活数据 SHALL 仍可读
