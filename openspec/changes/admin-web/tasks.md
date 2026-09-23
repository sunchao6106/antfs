# tasks — admin-web

## 1. 变更文档
- [x] 1.1 proposal / design / spec delta 写就,`openspec validate admin-web --strict` 通过。

## 2. master:ListPartitions
- [x] 2.1 `proto/master.proto`:`PartitionEntry` / `ReplicaCurrent` / `ListPartitionsResponse` + `ListPartitions` rpc(带 version)。
- [x] 2.2 `server.rs`:实现 `list_partitions`(hard placement 联 soft current_state/lag,节点 addr + up)。
- [x] 2.3 单测:联表逻辑(有/无 current 上报)。

## 3. admin crate 后端
- [x] 3.1 `crates/admin`:Cargo(tonic/axum/serde/rust-embed…)+ 加入 workspace members。
- [x] 3.2 `config.rs`:`master_addr`/`listen_addr`/`operator_token`/`grafana_url` + `Default` + `Validate`。
- [x] 3.3 `MasterApi` trait + gRPC 实现(cluster/nodes/partitions 读 + create/decommission);便于测试注入 mock。
- [x] 3.4 axum 路由:`GET /api/{cluster,nodes,partitions}`、`POST /api/partitions`、`POST /api/nodes/{id}/decommission` + DTO(serde)。
- [x] 3.5 operator 鉴权中间件(Bearer;空 token 关校验;401)。
- [x] 3.6 内嵌 SPA(`rust-embed`)+ 非 API 回退 index。
- [x] 3.7 `metrics.rs`:`admin_api_requests_total`/`admin_ops_total`/`admin_master_rpc_errors_total`。

## 4. 后端测试
- [x] 4.1 `oneshot`:无/错 token→401;有效 token 放行;`/api/cluster` JSON(注入 mock MasterApi)。
- [x] 4.2 建分区/下线转调 mock MasterApi 且回传结果。

## 5. 前端 SPA(React + Vite)
- [x] 5.1 `crates/admin/ui`:Vite + React scaffold(概览/节点/分区/运维/Grafana 外链)。
- [x] 5.2 token 存 localStorage + 注入 Authorization + 401 登录框。
- [x] 5.3 `vite build` 通过,产物 `dist/` 供 `rust-embed` 内嵌。

## 6. 验证 / 文档
- [x] 6.1 `cargo test` 绿 + `cargo clippy` 无告警 + 全 workspace build。
- [x] 6.2 `docs/design/cluster.md`:admin 章节 + 配置项 / 监控项清单(§7 必须项)。

## 后续(不在本 change)
- [x] 分页(海量分区)。已落地(每页 10/20/50/100 + 状态过滤/仅欠副本 + 前端分页)。
- [x] RBAC(operator/viewer 双令牌)。已落地(详见 `openspec/changes/admin-enhancements/`)。
- [x] 更多 ops:compaction(手动 compact)、标 RO/SEALED。已落地(`POST /api/partitions/{id}/compact` 与 `/state`)。
- [ ] 复制开关:store 侧运行期暂停拉复制 —— 未做(现有的 `store_server.replication_enabled` 是启动期配置;`/admin/request-control` 只管入向请求)。
- [ ] 多用户 / 审计持久化:当时定为 non-goal(双固定 token 即够),仍未做。
- [ ] admin `--dev` 代理 vite:未做(本地联调目前靠独立 admin 进程或直接跑 devcluster)。
- [ ] **EC 仍锁定**。
