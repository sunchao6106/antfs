# Tasks

- [x] `request_control.rs`:`DataOp`(as_str/parse 单一真值)、`RequestControl`(节点级 + 分区级 + 惰性过期)、`classify` + 单测覆盖全部路由
- [x] store:`AppState.request_control` + `gate` 中间件(数据面路由之后)+ `POST|GET /admin/request-control`
- [x] store:指标 `store_server_rejected_requests_total{op,reason}`、配置 `request_control_default_ttl_secs`
- [x] store 测试:禁用 put→503 且计数 1;`?replication=1`/`replica-metadata`/`segments`/`/metrics`/`/admin/**` 仍 200;恢复;未知 op 400;TTL 过期自恢复
- [x] **回归防线**:源节点禁用 put+get 后,双 store 拉复制仍收敛(tests/replication.rs)
- [x] admin:`store_client` 支持 JSON body;`POST|GET /api/stores/{node_id}/request-control`(404/400/502 透出)+ 测试
- [x] 前端:节点页限流内联面板(op + 有效期 + 禁用/启用 + 当前禁用集)+ i18n
- [x] 文档:store.md §12.3(豁免清单 + 三条偏离)、cluster.md §9 端点与配置/指标表
- [x] 真集群验证:直连 store 与经 admin 转发均按预期;禁用 get 期间 `?replication=1` 仍 200
