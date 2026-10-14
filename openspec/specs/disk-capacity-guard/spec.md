## Purpose

Store engine logical-commitment hard limits and disk reserved-space protection.

## Requirements

### Requirement: 逻辑承诺硬限制
`select_disks` SHALL 拒绝将新分区放置到 `committed + partition_cap > usable_capacity` 的盘上，其中 `committed = 该盘上现有副本数 × partition_cap`，`usable_capacity = disk_capacity × (1 − disk_reserved_ratio)`。`committed` MUST 从 Raft partition table 计算，不依赖心跳软状态。

#### Scenario: 盘逻辑写满后拒绝新分区
- **WHEN** 8 TB 盘上已有 76 个 partition（`partition_cap=100G`，`reserved_ratio=0.05`，`usable=7.6T`），`committed = 76 × 100G = 7.6T`
- **THEN** `select_disks` 因 `7.6T + 100G > 7.6T` 跳过该盘

#### Scenario: 盘逻辑有余量时允许新分区
- **WHEN** 8 TB 盘上已有 70 个 partition，`committed = 7T`，`usable = 7.6T`
- **THEN** `select_disks` 因 `7T + 100G = 7.1T ≤ 7.6T` 允许该盘作为候选

#### Scenario: 空盘正常接受首个分区
- **WHEN** 8 TB 盘上无任何 partition，`committed = 0`
- **THEN** `select_disks` 因 `0 + 100G ≤ 7.6T` 允许该盘

#### Scenario: 分区被删除后逻辑配额释放
- **WHEN** 盘上 76 个 partition 中有 5 个被删除（副本 DROPPED 并清理），`committed` 降为 `71 × 100G = 7.1T`
- **THEN** `select_disks` 重新允许该盘（`7.1T + 100G = 7.2T ≤ 7.6T`）

#### Scenario: leader 切换后 committed 检查不丢失
- **WHEN** leader 切换发生
- **THEN** 新 leader 从 Raft partition table 重新计算 `committed`，结果与老 leader 一致

### Requirement: 磁盘预留空间
master SHALL 配置 `disk_reserved_ratio`（默认 0.05），可用容量上限 `usable_capacity = disk_capacity × (1 − disk_reserved_ratio)`。预留空间覆盖 index 文件、Binary Fuse Filter、暂存文件、文件系统元数据等非 log-segment 开销。`select_disks` 的逻辑承诺检查和物理空间检查 MUST 使用 `usable_capacity` 而非原始 `disk_capacity`。

#### Scenario: 默认预留 5%
- **WHEN** 配置未设置 `disk_reserved_ratio`
- **THEN** 8 TB 盘的 `usable_capacity = 8T × 0.95 = 7.6T`

#### Scenario: 预留比例校验
- **WHEN** `disk_reserved_ratio` 配置为负数或 ≥ 1.0
- **THEN** 配置校验报错

#### Scenario: 预留影响物理空间检查
- **WHEN** 盘 `adjusted_free = 500G` 但 `disk_capacity × disk_reserved_ratio = 400G`（即预留区 400G）
- **THEN** 物理空间检查中可用于放新分区的有效余量 MUST 考虑预留空间不被 partition 数据占用

### Requirement: 内存即时扣减
Raft apply `CreatePartition` 后，master SHALL 在 soft state 中将对应盘的 `adjusted_free` 扣减 `partition_cap`。心跳更新 `adjusted_free` 时 SHALL 采用单调递减守卫：仅在心跳报的 `free_bytes < adjusted_free` 时覆盖，否则保持扣减后的值；超过 `adjusted_free_timeout`（默认 120 秒）时重置为心跳值。`select_disks` 的物理空间检查 MUST 使用 `adjusted_free` 而非原始 `free_bytes`。

#### Scenario: 连续建两个分区不超卖
- **WHEN** 盘 `adjusted_free = 500G`，master 连续 apply 两个 CreatePartition（同一盘，`partition_cap=100G`）
- **THEN** 第一次 apply 后 `adjusted_free = 400G`，第二次 apply 后 `adjusted_free = 300G`

#### Scenario: 心跳不回滚扣减
- **WHEN** master 扣减 `adjusted_free` 至 400G，随后心跳上报 `free_bytes = 500G`（存储节点尚未创建新 partition）
- **THEN** `adjusted_free` 保持 400G 不变（500G > 400G → 不覆盖）

#### Scenario: 心跳可向下更新
- **WHEN** `adjusted_free = 400G`，心跳上报 `free_bytes = 350G`（存储节点上其他 partition 写入了数据）
- **THEN** `adjusted_free` 更新为 350G（350G < 400G → 覆盖）

#### Scenario: 超时重置
- **WHEN** `adjusted_free = 400G`，超过 `adjusted_free_timeout`（默认 120 秒）未因扣减或更小心跳值而更新，心跳上报 `free_bytes = 600G`（compaction 释放了空间）
- **THEN** `adjusted_free` 更新为 600G（超时 → 接受心跳值）

#### Scenario: leader 切换后 adjusted_free 丢失但安全
- **WHEN** leader 切换，soft state 丢失
- **THEN** usage map 为空 → `select_disks` 跳过所有盘（无心跳数据）→ 不分配 → 第一轮心跳恢复 `adjusted_free` → committed 检查（Raft 状态）始终兜底

### Requirement: 分区级写满感知
master reconciler 巡检 SHALL 对每个 RW 分区检查 `max(各副本 used_bytes) >= partition_cap − segment_capacity`。满足条件的分区 SHALL 被转为 RO（`SetPartitionState(RO)`），提前停止写路由。

#### Scenario: 分区接近写满时转 RO
- **WHEN** RW 分区的 `max(used_bytes) = 97G`，`partition_cap = 100G`，`segment_capacity = 4G`，阈值 = `100G − 4G = 96G`
- **THEN** reconciler 提交 `SetPartitionState(RO)`

#### Scenario: 分区远未写满时保持 RW
- **WHEN** RW 分区的 `max(used_bytes) = 50G`，阈值 = 96G
- **THEN** 不触发转 RO

#### Scenario: 已是 RO/SEALED 的分区不重复处理
- **WHEN** 分区已经是 RO 或 SEALED 状态
- **THEN** reconciler 跳过该分区的写满检查

#### Scenario: used_bytes 无上报时不误判
- **WHEN** 某 RW 分区无任何副本上报 `used_bytes`（心跳尚未建立）
- **THEN** 视为 `used = 0`，不触发转 RO

### Requirement: select_disks 保持纯函数
`select_disks` SHALL 不访问任何全局可变状态。`committed`、`usable_capacity`、`adjusted_free` 等所有新增输入 MUST 通过 `DiskFree` 结构体由调用方（`placement_usage`）组装后传入。

#### Scenario: 相同输入产出相同结果
- **WHEN** 以相同的 `nodes`、`usage`（含 `DiskFree` 新字段）、`rf`、`partition_cap`、`near_full_ratio` 调用两次
- **THEN** 两次返回结果完全一致

### Requirement: committed 包含所有状态的副本
`committed` 计算 SHALL 遍历 Raft partition table 中的**所有副本**，不区分分区状态（RW / RO / SEALED）。DROPPED 副本已从 table 中移除，自然不计入。

#### Scenario: RO 分区的副本仍计入 committed
- **WHEN** 盘上有 70 个 RW 分区 + 6 个 RO 分区
- **THEN** `committed = 76 × partition_cap`（RO 副本数据仍在盘上，必须计入）

#### Scenario: SEALED 分区的副本仍计入 committed
- **WHEN** 盘上有 60 个 RW + 10 个 RO + 5 个 SEALED
- **THEN** `committed = 75 × partition_cap`

#### Scenario: DROPPED 副本不计入
- **WHEN** 某副本已被 `RemoveReplica` 从 partition table 中删除
- **THEN** 该副本不出现在遍历中，committed 自然减少

### Requirement: Router 侧 StoreFull 重试
Router 的 `put_file` / `put_file_with_ttl` SHALL 在所有副本返回 507（StoreFull）时，踢掉该 partition 的号段缓存，重新分配 blobId 到其他 partition 并重试写入（最多 2 次重试）。`put` / `put_with_ttl` 在所有副本返回 507 时 SHALL 踢掉号段缓存并返回 `StoreFull` 错误（调用方需重新 allocate，因为 blobId 绑定了 partition）。

#### Scenario: put_file 碰到 507 自动重试成功
- **WHEN** `put_file_with_ttl` 写入 partition 42 时所有副本返回 507
- **THEN** router 踢掉 partition 42 号段 → 重新 `allocate_one()` 获得 partition 53 的 blobId → 写入 partition 53 成功 → 返回新 blobId 给调用方

#### Scenario: put 碰到 507 踢掉 partition 并报错
- **WHEN** `put_with_ttl(key, data)` 写入时所有副本返回 507
- **THEN** router 踢掉该 partition 号段 → 返回 `Error::StoreFull` → 后续 `allocate_one()` 不再选该 partition

#### Scenario: 部分副本 507 不触发重试
- **WHEN** 3 副本中 1 个返回 507，其余 2 个成功
- **THEN** quorum 写成功（多数派达成），不触发 StoreFull 重试

#### Scenario: evict 后 allocate 选到不同 partition
- **WHEN** partition 42 被 evict 后调用 `allocate_one()`
- **THEN** 号段池中 partition 42 已清空 → 选到其他 partition 的号段（或向 master 重新拉取时 master 不再选已转 RO 的 42）
