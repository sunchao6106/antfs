# store-server-replication

副本间拉式 anti-entropy(拉复制)与 BOOTSTRAP 全量逻辑追平。承 cluster.md D4/§10。

## ADDED Requirements

### Requirement: 忠实复制 apply(store 引擎)

`BlobStore` SHALL 提供 `apply_replicated(rec)`,把一条来自 peer 的记录按 **newest-wins + lifeVersion** 与本地赢家比较后**原样**(保留 peer 的 flags/lifeVersion/expiration/op_time)写入;本地已 ≥ peer 状态时 SHALL 返回 `Skipped` 且不改动本地;底层因 IO 自关(Phase 1)时 SHALL 返回 `Unavailable`。apply SHALL **只增不删**(不移除本地已有更新的记录),使部分失败后重试安全(幂等,守 §3.1)。

#### Scenario: 缺失的存活对象被原样复制

- **WHEN** 本地无某 key,peer 有其存活 PUT(lifeVersion=L、带数据)
- **THEN** `apply_replicated` SHALL 以 lifeVersion=L 写入该对象,随后本地 `stat` 的 lifeVersion 等于 L、`get` 可读到相同数据

#### Scenario: 已复制记录重复 apply 幂等

- **WHEN** 对本地已存在且状态相同的记录再次 `apply_replicated`
- **THEN** SHALL 返回 `Skipped`,本地字节数与索引 SHALL 不变

#### Scenario: 高 lifeVersion 的 undelete 被忠实保留

- **WHEN** peer 的对象为已复活(undeleted、lifeVersion=2),本地缺失
- **THEN** `apply_replicated` 后本地 `stat` 的 lifeVersion SHALL 为 2(不退回 0)

#### Scenario: 墓碑传播

- **WHEN** peer 上某 key 为已删除(墓碑),本地为其存活 PUT
- **THEN** `apply_replicated` 后本地对该 key 的 `get` SHALL 返回不可读(已删除)

### Requirement: 复制元数据端点

store server SHALL 提供 `GET /partitions/{pid}/replica-metadata`,入参 `token`(游标)与 `max_bytes`(批上限),返回该 token 之后按 log 追加序去重的 `MessageInfo` 列表 + 新游标 + `bytes_read`;分区无本地 replica 时返回 404。

#### Scenario: 增量枚举

- **WHEN** 携带上次返回的 token 再次请求
- **THEN** SHALL 只返回该游标之后的新条目;已追平时 SHALL 返回空条目 + 原游标

### Requirement: 复制用 blob 读

store server 的 blob 读 SHALL 支持 `?replication=1`,对已删除/已过期对象也返回其数据体(供接收方忠实 apply);普通读(无该参数)对已删除/过期仍返回 404。

#### Scenario: 复制读可取已删除数据

- **WHEN** 对一个已删除但数据未回收的 key 带 `?replication=1` 读
- **THEN** SHALL 返回其数据体(200)

### Requirement: 拉复制收敛

启用复制后(`replication_enabled=true`),每个本地 replica SHALL 周期性地对其每个存活 peer 拉复制,使本地最终包含 peer 的全部记录(newest-wins 收敛);复制 SHALL 幂等且可安全重试。

#### Scenario: PUT 经拉复制传播到 peer

- **WHEN** 仅向副本 A 写入某 blob,B 未写;随后 B 对 A 跑一轮拉复制
- **THEN** B SHALL 能读到该 blob(数据一致)

#### Scenario: BOOTSTRAP 全量追平

- **WHEN** 一个空副本从 `token=Uninitialized` 对已有数据的 peer 拉复制直至追平
- **THEN** 追平后该副本 SHALL 含 peer 的全部存活对象,且元数据请求返回空增量
