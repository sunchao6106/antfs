# blobid-master-assign

objectId 由 master 按 partition 单调号段发号(承 cluster.md §8 / §7.6 Q7,借鉴已核实的 SDFS fsi↔mds 机制)。骨架:号段两层批处理 + Raft 水位持久化(多数派 fsync)+ 容忍空洞永不复用 + client 缓存预取。

## ADDED Requirements

### Requirement: master 按 partition 号段发号，不进每对象写路径

objectId SHALL 由 master 按 partition 单调分配。master SHALL 以号段（block `[start, start+N)`）为单位发放，client SHALL 本地缓存号段并从中取号，热路径取号 SHALL NOT 每次访问 master。master 参与发号 SHALL NOT 退化为"每对象一次 RPC/一次 Raft 提交"。

#### Scenario: 号段内取号不触达 master

- **WHEN** client 已缓存一个未耗尽的 objectId 号段并连续分配多个对象
- **THEN** 这些分配 SHALL 从本地号段取号（递增），期间 SHALL NOT 产生对 master 的发号 RPC

#### Scenario: 缓存将尽时后台预取

- **WHEN** client 号段剩余量低于阈值
- **THEN** client SHALL 后台异步向 master 预取下一段，正常情况下取号 SHALL NOT 因等待 master 而阻塞

### Requirement: 水位持久化、容忍空洞、永不复用

master SHALL 仅发放"已提交高水位以内"的 objectId：抬高 per-partition 水位 `reserved_upto` SHALL 先提交进 Raft，再据此发号。崩溃或切主后新主 SHALL 从已提交水位续发，SHALL NOT 重发任何已越过水位的号。水位与实际发放之间未用的号 SHALL 允许成为空洞，系统 SHALL NOT 假设"已发号即对象存在"。

#### Scenario: 切主后不复用

- **WHEN** master 崩溃/切主，新主接管发号
- **THEN** 新主 SHALL 从已提交 `reserved_upto` 往上发号，之前已发放的任何 objectId SHALL NOT 被再次发放

#### Scenario: 空洞无害

- **WHEN** client 缓存了号段但崩溃，未用完的号丢失
- **THEN** 这些号 SHALL 作为空洞被跳过，SHALL NOT 影响后续发号的正确性或唯一性

#### Scenario: master 全丢后从副本索引回填水位不复用

- **WHEN** 整个 master Raft 多数派持久状态一次性丢失，master 无状态冷启（数据节点及其索引仍在）
- **THEN** master SHALL 扫描各副本索引取每 partition 已用最大 objectId，将水位回填为 `max+1`，之后发号 SHALL NOT 复用任何已写对象的 objectId

### Requirement: Raft 水位持久性 ≥ 单数据库，落点在存储适配器 fsync

per-partition 水位的耐久性 SHALL 由 Raft 多数派持久化提供（一条已提交水位 = 已 fsync 到多数派）。Raft 日志存储适配器 SHALL 在数据 fsync 落盘后才 ack commit；该持久边界 SHALL 由崩溃一致性测试守护（承 CLAUDE.md §3.1/§6）。

#### Scenario: 多数派存活则水位不丢

- **WHEN** 少数派节点（≤ f，共 2f+1）连同其磁盘永久丢失后集群恢复
- **THEN** 已提交的 per-partition 水位 SHALL 不丢失，发号 SHALL 从不低于该水位处继续

#### Scenario: fsync 是持久边界（崩溃一致性）

- **WHEN** 在 Raft 日志 append 的 fsync 前后注入崩溃
- **THEN** 仅 fsync 已完成（已 ack commit）的水位推进 SHALL 在重启后可见，未 fsync 的 SHALL 不导致水位倒退或号复用

### Requirement: 借鉴 SDFS 机制但不采纳其中心元数据模型

本方案 SHALL 借鉴 SDFS 的号段缓存、后台预取、发号 anti-affinity 机制。本方案 SHALL NOT 采纳 SDFS 将每对象元数据（md5/size/expiry）集中存于元数据服务器/数据库的模型（与 D3"无中心每对象元数据"冲突）。

#### Scenario: 不引入中心每对象元数据

- **WHEN** 设计 blobId 发号与对象定位
- **THEN** master SHALL 只维护 partition 级映射与 per-partition 发号水位，SHALL NOT 为每个对象在中心侧登记 md5/size/expiry 等元数据
