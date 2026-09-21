# store-server-heartbeat

数据节点接入集群 + 磁盘健康监控(承 cluster.md §9,照搬 Ambry 两级)。**EC 仍锁定。**

## ADDED Requirements

### Requirement: 数据节点注册与心跳

store server 启动时 SHALL 向 master 注册(node_id + 各盘 `statfs` 容量),并周期发送心跳上报各盘实时余量与本地每个 replica 的 `current_state`。

#### Scenario: 注册后 master 可见

- **WHEN** store server 启动并向 master 注册
- **THEN** master 的 `ListNodes` SHALL 包含该节点及其盘容量

### Requirement: 消费 master 指令管理副本

store server SHALL 消费心跳返回的 `MasterDirective.transitions`:目标 `ACTIVE`/`BOOTSTRAP` → 幂等确保本地 replica 存在;目标 `OFFLINE`/`DROPPED` → 关闭该 replica。

#### Scenario: 指令驱动建副本后可读写

- **WHEN** master 把某分区放置到该节点并通过心跳下发转移
- **THEN** store server SHALL 建立该分区的本地 `BlobStore`,之后对该分区的 HTTP `PUT`/`GET` SHALL 可用

### Requirement: 被动磁盘故障升级到再复制

当某 replica 的 `BlobStore` 因连续 IO 错误自关(Phase 1 机制)时,store server SHALL 把该 replica 的 `current_state` 置为 `ERROR` 并经心跳上报;master SHALL 将"节点存活但 replica 为 ERROR"视为不健康副本并触发再复制(先补后删,守持久性红线)。

#### Scenario: replica IO 故障触发补副本

- **WHEN** 某 replica 的存储层触发 IO-error 自关(hook 触发)
- **THEN** 该 replica 心跳上报 `current_state=ERROR`,master SHALL 在一个健康节点上补一个新副本使健康副本恢复到复制因子

### Requirement: 主动磁盘健康探测

store server SHALL 支持周期性主动磁盘探测(write/read/verify/delete,带超时以捕获挂起),结果作为 `DiskHealth` 随心跳上报;默认关闭(对齐 Ambry)。主动探测 SHALL NOT 自行下线副本(仅上报观测)。

#### Scenario: 健康盘探测为 HEALTHY

- **WHEN** 对一块正常盘执行主动探测
- **THEN** 上报的 `DiskHealth` SHALL 为 `HEALTHY`
