# Tasks

- [x] common/metric:Prometheus 文本解析器 `parse()` + 单测
- [x] store:`GET /metrics` 直出(registry 存入 AppState)+ oneshot 测试
- [x] master:`GetMetrics` RPC(MasterNode/Service 持 registry)+ 集成测试
- [x] master:`RepairPartition` RPC(复用 try_add_replica;`master_repairs_triggered_total`)+ 集成测试
- [x] master:`MoveReplica` RPC + `MasterState.moving`(serde default + 兼容测试)+ `plan_move` + reconciler 接线 + `moving_out_nodes` 回填 + `master_replicas_moved_total` + add-before-remove 集成测试
- [x] admin:MasterApi 扩展(metrics_text/partition_detail/repair/move)+ `/metrics`(RBAC 外)+ `/api/metrics`(并发抓 store)+ `/api/partitions/{id}` + repair/move 路由 + `admin_store_scrape_errors_total` + 测试
- [x] 前端:tab 化重构 + 节点聚合/展开 + 分区筛选/批量/repair/move + uPlot 指标页 + 运维页(强制下线面板、分区查找)+ i18n + 构建
- [x] devcluster:admin registry 接线;真集群 curl 端到端(store/admin /metrics、/api/metrics、partition detail、repair/move)
- [x] 文档:cluster.md(新 RPC、§9.1 store /metrics、§11 admin 聚合与监控项)、store.md(/metrics 说明)
- [x] 浏览器端到端验证 UI(tabs/节点展开/分区筛选与批量/指标图表/运维面板):已用真集群 100 分区逐 tab 截图核验
