## Why

Q6(cluster.md §7.6)已锁:做独立 **admin web** 服务——(a) 运维操作经 master admin gRPC(建分区/下线节点等);(b) 控制面状态 dashboard(拓扑 + 节点/盘/分区/副本 状态/容量/健康,admin 自建);(c) 指标时序复用 Prometheus+Grafana(不自造图表);(d) operator 鉴权。对齐 CubeFS `console` / SeaweedFS `weed admin`。

master 已有 admin gRPC(`GetClusterState`/`ListNodes`/`CreatePartitions`/`DecommissionNode`/`LookupPartition`),但**缺一个"列全部分区 + 每副本实然态"的只读接口**——dashboard 要展示分区/副本拓扑必须补 `ListPartitions`(联 hard placement + soft `current_state`)。

## What Changes

- **master**:新增 `ListPartitions` gRPC(联 hard-state `partitions` 的 placement 与 soft-state `replica_state` 的 `current_state`/`lag`,附节点 addr/up),proto 带 `version`。
- **新 crate `admin`**(独立服务,master gRPC 客户端 + axum HTTP):
  - **JSON API**:`GET /api/cluster`(term/leader/节点数/分区数)、`GET /api/nodes`(节点 + 盘容量/余量/健康 + up)、`GET /api/partitions`(分区 + 副本 target/current/所在节点)、`POST /api/partitions`(建分区)、`POST /api/nodes/{id}/decommission`(下线)。
  - **operator 鉴权**:所有 `/api/*` 需 `Authorization: Bearer <operator_token>`(配置);缺/错 → 401。空 token = 关校验(仅本地)。
  - **静态资源**:内嵌构建好的 SPA(`/` 及静态文件),`/api/*` 之外回退到 SPA index(前端路由)。
- **前端 SPA**(`crates/admin/ui`,React + Vite):集群概览、节点/盘表、分区/副本拓扑表、建分区/下线表单、Grafana 外链;`vite build` 产物内嵌进 admin 二进制。
- **配置**:`admin.master_addr`、`admin.listen_addr`、`admin.operator_token`、`admin.grafana_url`。
- **监控**:`admin_api_requests_total`、`admin_ops_total`、`admin_master_rpc_errors_total`。

## Capabilities

### New Capabilities
- `admin-web`:控制面只读 dashboard + 运维操作 HTTP API(经 master admin gRPC)+ operator 鉴权 + 内嵌 SPA。

### Modified Capabilities
<!-- master 新增 ListPartitions 只读 RPC(不改既有放置/调谐语义)。 -->

## Impact

- **crate**:新增 `admin`(+ `admin/ui` 前端);`master` proto + server 加 `ListPartitions`。
- **保留契约**:master 只决策不搬数据;admin 只经 gRPC 调 master,不直接碰 store 数据面;指标时序不自造(外链 Grafana)。**EC 仍锁定**。

## Non-goals

- 不做用户/RBAC 多角色(单一 operator token MVP);不做审计日志持久化(仅指标计数)。
- 不自造指标图表(复用 Prometheus/Grafana,仅外链)。
- 不做 EC 相关运维(EC 仍锁定);不触发 compaction/复制开关等(后续按需加,先只读 + 建分区 + 下线)。
