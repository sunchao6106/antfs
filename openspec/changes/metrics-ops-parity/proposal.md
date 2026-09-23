## Why

`admin-metrics-ops` 打通了「全组件 `/metrics` 暴露 → admin 聚合 → SPA 图表」这条链路,但**指标覆盖面向运维决策仍不完整**:

1. **缺分配/容量/状态类视角**:admin dashboard 只能看到心跳/迁移速率与欠副本数,看不到「写请求打到哪里(Assign/Lookup)」「还剩多少可写容量」「分区状态分布」「副本落后多少」「多数副本失联的分区」——这些正是 SeaweedFS(`master` volume layout + assign 计数)、Ceph(`osd tree`/`ceph -s` 容量与 pg 状态分布)、Ambry(`isMajorityReplicasDown` 告警基线)、FastDFS(`tracker` 存储分布)运维面板的标配。
2. **store 侧错误不可分类、延迟不可见**:`store_server_errors_total` 混在一起,无法区分「引擎 IO 错」与「无本地副本」;也没有延迟直方图/在途请求数,单店是否过载只能靠猜。
3. **compaction 只能等周期**:本地 worker 按阈值周期压缩,运维在排障(死数据堆积、盘将满)时无法对一个分区立即触发一次,只能改配置或等(Ambry 有 `TriggerCompaction`,`ceph` 有 `osd` 类手动触发)。
4. **admin 无聚合/无入口**:即便 store 支持手动 compact,跨副本逐个 curl 不是可运维的姿势。

## What Changes

- **master 指标补全**(对标 SeaweedFS/Ceph/Ambry/FastDFS 的分配与容量视图):
  - counter:`master_assign_requests_total`、`master_assign_failures_total`、`master_lookup_requests_total`(写/读路由吞吐与失败率)。
  - gauge(仅 leader 的 reconciler 刷新,follower 恒 0):`master_partitions_rw` / `master_partitions_ro` / `master_partitions_sealed`(状态分布)、`master_writable_capacity_bytes`(可写容量上界 = RW 分区 × `partition_capacity_bytes`)、`master_nodes_up`、`master_cluster_free_bytes` / `master_cluster_used_bytes`、`master_max_replica_lag_bytes`、`master_majority_replicas_down_partitions`(对齐 Ambry `isMajorityReplicasDown`)。
- **store 指标与端点**:
  - `store_server_request_duration_seconds{op}`(histogram,数据面 handler 全程含 spawn_blocking 排队)、`store_server_in_flight_requests{op}`(gauge)、`store_server_errors_total{kind}`(counter,kind ∈ io/unavailable/join_error,把引擎错误与生命周期错误分开)、`store_server_manual_compactions_total`(counter)。
  - 新运维端点 `POST /admin/partitions/{pid}/compact`:对该分区**本地副本**立即全库 compact,回 `{live_bytes, dead_bytes, segments_before, segments_after}`;无本地副本 → 404(对齐 Ambry `TriggerCompaction`)。
- **admin `/api/metrics` 扩展**:master 段按「指标名去 `master_` 前缀」补 12 个字段(取不到填 0,契约稳定);stores 段补 `manual_compactions_total`(`errors_total` 语义不变 = 带 kind label 的 counter **按名求和**)。
- **admin 手动 compact 路由**:`POST /api/partitions/{id}/compact`(operator;viewer 由既有方法门控 RBAC 挡下)= 取 `partition_detail(id)` 的副本清单,对**所有副本节点并发**转发 store 的 compact 端点(2s 超时),回 `{"results":[{node_id,ok,live_bytes?,dead_bytes?,segments_before?,segments_after?,error?}],"error":""}`;单节点 404/超时/网络失败不中断其余节点。store 抓取/转发共用 `crates/admin/src/store_client.rs` 一套 client。
- **SPA 图表**:MetricsTab 增 KPI(可写容量 / 分区状态分布 / 多数副本失联分区,danger 风格)与图(分配速率、可写容量趋势、集群已用容量增速(有符号差分)、最大副本 lag、在线节点数、多数失联分区);gauge 类只在 `is_leader=1` 时绘制,否则显示「仅 leader 刷新」提示。PartitionsTab 行内增 Compact 按钮,逐节点聚合结果渲染到消息条(en/zh 双语)。

## Capabilities

### New Capabilities
- `metrics-ops-parity`:master 分配/容量/状态类指标、store 延迟/在途/错误分类/手动 compaction 指标与端点、admin compact 聚合路由与 UI 图表。

### Modified Capabilities
<!-- admin /api/metrics 契约加字段(不改既有字段名,前端无需改动即可继续工作);store 数据面新增一个运维端点(不影响既有读写路径)。 -->

## Impact

- **crate**:`master`(metrics.rs 注册 + reconcile 处更新 gauge)、`store`(metrics.rs + `POST /admin/partitions/{pid}/compact`)、`admin`(`/api/metrics` 字段 + compact 路由 + `store_client.rs`)、`admin/ui`(api.ts/MetricsTab/PartitionsTab/i18n)。
- **契约变化**:`/api/metrics` 的 `master`/`stores` 段新增字段(纯增量);store 新端点 `POST /admin/partitions/{pid}/compact`。
- **被否决方案**:在 admin 侧做 compaction 调度/排队(admin 保持无状态转发,触发权仍归运维);为 compact 单独起 admin 指标(store 侧 `manual_compactions_total` 已在抓取聚合里可见);把延迟直方图分位算在 admin(分位归 Prometheus,Grafana 侧查询)。

## Non-goals

- 不做自动 compaction 策略调优/盘级 compaction(仍是分区级手动触发 + 本地 worker 周期压缩)。
- 不做 admin 侧时序持久化与告警(滚动窗口仅内存;`majority_replicas_down_partitions` 等只作展示,Grafana/Prometheus 侧配告警)。
- 不做 EC 相关运维(EC 仍锁定)。
- 不把 compact 做成异步任务跟踪(无 job id/进度查询;2s 短超时下大库可能先在 admin 侧报超时)。
