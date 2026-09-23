# Tasks

- [x] master:12 个新指标注册(`assign_requests/failures_total`、`lookup_requests_total`、`partitions_rw/ro/sealed`、`writable_capacity_bytes`、`nodes_up`、`cluster_free/used_bytes`、`max_replica_lag_bytes`、`majority_replicas_down_partitions`)+ 对实际 `Registry` 断言
- [x] master:Assign/Lookup 计数与 reconciler 周期刷新 gauge(仅 leader 有值)+ 集成测试
- [x] store:延迟直方图 `store_server_request_duration_seconds{op}` / 在途 `store_server_in_flight_requests{op}` / 错误分类 `store_server_errors_total{kind}` / `store_server_manual_compactions_total`
- [x] store:`POST /admin/partitions/{pid}/compact`(本地副本全库 compact,回 live/dead/segments;无本地副本 404)+ 测试
- [x] admin:`/api/metrics` master 段补 12 字段 `master_` 去前缀命名(取不到填 0)+ stores 段补 `manual_compactions_total`(errors_total 按名求和不变)+ 测试
- [x] admin:抽 `store_client.rs`(client/超时/scrape 与 compact 转发共用),`scrape_store` 改走该路径
- [x] admin:`POST /api/partitions/{id}/compact`(operator;副本节点并发转发 + 逐节点 ok/error 聚合 + 结果按 node_id 排序)+ 测试(真 loopback 假 store:200 / 404 / 连接失败 / 分区不存在 404 / 无副本)
- [x] 前端 `api.ts`:Metrics master/stores 字段补全 + `compactPartition(id)` 与 `CompactResult` 类型
- [x] 前端 MetricsTab:新增 KPI(可写容量、分区状态分布、多数失联分区 danger)+ 图(分配/失败速率、可写容量、已用容量增速、最大副本 lag、在线节点数、多数失联分区);gauge 类仅 leader 视角绘制 + 提示行
- [x] 前端 PartitionsTab:行内 Compact 按钮 + 逐节点结果渲染到消息条;i18n en/zh 双份
- [x] 文档:cluster.md §11 补 `/api/metrics` 新字段说明、`POST /api/partitions/{id}/compact` 契约与 UI 变化;§7.7/§9.1 监控项清单同步新指标
- [x] 验证:`cargo test -p antfs-admin` 全绿、`cargo clippy -p antfs-admin --all-targets` 无告警、`npm run build`(tsc + vite)成功、`cargo build` workspace 编过
- [x] 浏览器端到端验证新图表/RBAC 下 Compact 按钮(真集群):Metrics/KPI 与 6+ 张图渲染正常(修掉自定义 `values` 未设 `axis.size` 导致 y 轴标签被裁、样本 <2 不画图避免无意义时间轴)、分区页 Compact 三副本聚合并回显"节点 101/102/103 回收 …"
- [ ] 大库 compaction 的异步跟踪/进度(当前 2s 超时下 admin 可能先报超时)
