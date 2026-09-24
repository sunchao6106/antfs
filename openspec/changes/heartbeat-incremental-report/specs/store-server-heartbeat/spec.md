# store-server-heartbeat

心跳从"每拍全量"改为"增量 + 全量摘要 + 不符重发自愈",master 每拍处理降为 O(本节点副本数)。

## MODIFIED Requirements

### Requirement: 数据节点注册与心跳

store server 启动时 SHALL 向 master 注册(node_id + 各盘 `statfs` 容量),并周期发送心跳上报各盘
实时余量与本地 replica 实然态。副本实然态上报 SHALL 支持增量模式:每拍只携带与上次上报相比变化
的条目、显式摘除的 `removed_replicas`、以及覆盖**全部持有**副本的 `replica_digest`;首拍与收到
`resend_full_report` 后 SHALL 全量上报。store 仅在 master 经 `delta_report_supported` 声明理解
增量心跳后才 SHALL 切增量模式。

#### Scenario: 稳态增量心跳

- **WHEN** 集群稳态(无写、无 compaction、无副本转移)且 master 已声明支持增量
- **THEN** store 的每拍心跳 SHALL 只含摘要(副本列表为空),`full_report=false`

#### Scenario: 副本摘除显式上报

- **WHEN** store 消费 `DROPPED`/`OFFLINE` 指令后本地不再持有某副本
- **THEN** 下一拍心跳 SHALL 在 `removed_replicas` 中携带该 partition,master SHALL 从软状态删除对应条目

#### Scenario: 摘要不符全量自愈

- **WHEN** master 按软状态重算的摘要与心跳携带的 `replica_digest` 不一致
- **THEN** master SHALL 在该拍响应中置 `resend_full_report=true`,store 下一拍 SHALL 全量上报,
  之后摘要 SHALL 恢复一致

#### Scenario: 旧 master 前不启用增量

- **WHEN** store 未在任何 `MasterDirective` 中见到 `delta_report_supported=true`
- **THEN** store SHALL 保持每拍全量上报(`full_report=true`)

### Requirement: 消费 master 指令管理副本

store server SHALL 消费心跳返回的 `MasterDirective.transitions`:目标 `ACTIVE`/`BOOTSTRAP` →
幂等确保本地 replica 存在;目标 `OFFLINE`/`DROPPED` → 关闭该 replica。master 下发的
`partition_states` 改为**增量**(只含与上次下发相比变化的条目),store SHALL 按 pid 合并应用;
master 对分区状态与墓碑回收闸门 SHALL 每拍重算,不得因增量下发而陈旧。

#### Scenario: 指令驱动建副本后可读写

- **WHEN** master 把某分区放置到该节点并通过心跳下发转移
- **THEN** store server SHALL 建立该分区的本地 `BlobStore`,之后对该分区的 HTTP `PUT`/`GET` SHALL 可用

#### Scenario: 应建未建的副本仍被令重建

- **WHEN** placement 含某节点副本但该节点心跳未上报它(如本地数据丢失)
- **THEN** master SHALL 在响应中下发该副本的转移指令(与全量上报时代行为一致)

#### Scenario: 增量模式下集群收敛

- **WHEN** 增量模式运行中创建分区 / 删除副本
- **THEN** 副本落地转正与 `pending_drop` 清除 SHALL 与全量模式同样在有限心跳周期内完成
