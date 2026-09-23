## Why

admin 页面此前是"一排同尺寸小卡 + 长表格",节点/分区一多就挤,也看不到运维最关心的两件事:**可写分区集合还剩多少、正在以多快速度被吃掉**(对标 SeaweedFS master UI 的 volume layouts:每类 writable/crowded 卷数与容量)。同时 master 完全不知道分区的实际占用字节,既不支撑"可写容量增速",也让后续均衡只能按容量上界盲判。

## What Changes

- **store 上报分区占用**:心跳 `ReplicaBeat.used_bytes`(blob 活数据字节)+ 修掉恒为 0 的 `lag_bytes`(peer 游标与本地已 apply 游标按段容量线性化估算);master 软状态接住,`ReplicaCurrent.used_bytes` 回填给客户端。
- **master 新指标**:`master_writable_used_bytes`(RW 分区按"每分区取副本最大值"求和,RF 不放大)、`master_writable_free_bytes`、`master_replicas_by_disk{node_id,disk_id}`、`master_disk_skew`;`ListPartitionsRequest.state` 支持按状态过滤(过滤先于分页,非法值 InvalidArgument)。
- **admin/UI「容量」tab**:集群水位、可写集合统计与曲线、增速读数(字节/s 与 字节/min)、可写分区列表(占用 + 1 分钟增量 + 副本分布 + 迁移中徽标)。
- **概览页改版**:健康横幅 + 4 张大卡(容量水位/可写余量/在线节点/欠副本)+ 两列(容量趋势图 | 集群概况)+ 磁盘水位榜 Top8 + 可写分区速览 Top8,取代同质小卡阵列。
- **维护模式开关**:`SetMaintenance` RPC + `MasterState.maintenance`/`pause_autogrow`,reconciler 在 gauge 刷新后跳过自动动作(再复制/迁移/下线 evacuation),观测与心跳指令下发继续;autogrow 用独立子开关;手动 ops 不受阻。

## Capabilities

### New Capabilities
- `admin-capacity-view`:可写分区集合与增速的可视化 + 概览仪表盘改版。
- `cluster-maintenance-mode`:集群级维护开关(暂停自动收敛,可观可查)。

### Modified Capabilities
<!-- ListPartitions 支持 state 过滤;PartitionDto/ReplicaDto 增加 used_bytes;ClusterState 增加 maintenance/pause_autogrow。 -->

## Impact

- **crate**:`master`(proto 字段/RPC、状态机、reconciler 跳过点、5 个 gauge、过滤与回填)、`store`(心跳 used/lag)、`admin`(state 透传、maintenance 路由、metrics 字段)、`ui`(概览改版 + 容量 tab + 维护开关 + 大小分桶图)、`devcluster`(示例 key 长度修正)。
- **边界口径**:可写已用按"每分区副本最大值"求和(RF 副本不重复计);lag 是估算(段容量相同、按段线性化,游标指向记录起点故最多偏小一条记录)。
- **维护模式语义**:停再复制/迁移/下线 evacuation;不停心跳、gauge 刷新、`plan_transitions` 下发(否则 move 的新副本永不落地);手动 `RepairPartition`/`MoveReplica` 不受阻。
- **不动默认值**:`partition_capacity_bytes`、`max_body_bytes`(新增,默认仍 2 MiB,§5.1)、`request_control_default_ttl_secs`(新增,默认 1800)。

## Non-goals

- 不在概览页做实时告警(告警归 Prometheus/Grafana);不持久化历史曲线(滚动窗口仅内存)。
- 不做"维护窗口自动定时开启/结束"(手工开关即够,定时可由外部调度调 API)。
- blob 大小分桶的 send-time(网络发送)不实现:antfs 的 store 计时口径是"入口 → 引擎完成",响应写出在 handler 返回之后;只做 processing(含排队)按 size 分桶。
