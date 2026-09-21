# admin-web

控制面 dashboard + 运维 HTTP API(经 master admin gRPC),operator 鉴权,内嵌 SPA。承 cluster.md Q6。

## ADDED Requirements

### Requirement: master 列分区接口

master SHALL 提供 `ListPartitions` gRPC,返回每个分区的状态、复制因子、各副本 placement(节点/盘/target_state/存活)及其实然态(current_state/lag,来自软状态)。

#### Scenario: 列出分区及副本态

- **WHEN** 集群已有分区且节点在上报心跳
- **THEN** `ListPartitions` SHALL 返回这些分区,每个副本含 target_state 与(若已上报)current_state

### Requirement: 只读控制面 API

admin 服务 SHALL 提供只读 JSON 端点:`GET /api/cluster`(term/leader/节点数/分区数)、`GET /api/nodes`(节点 + 盘容量/余量/健康 + 存活)、`GET /api/partitions`(分区 + 副本 target/current)。数据经 master admin gRPC 获取。

#### Scenario: 读取集群概览

- **WHEN** 携带有效 operator token 请求 `GET /api/cluster`
- **THEN** SHALL 返回当前 term、leader、注册节点数、分区数

### Requirement: 运维操作 API

admin 服务 SHALL 提供 `POST /api/partitions`(建 N 个分区)与 `POST /api/nodes/{id}/decommission`(下线节点),转调 master 对应 admin gRPC;master 返回的错误 SHALL 以对应 HTTP 状态回传。

#### Scenario: 建分区转调 master

- **WHEN** 携带有效 token `POST /api/partitions` 请求建 2 个分区
- **THEN** admin SHALL 调 master `CreatePartitions` 并回传新分区 id

### Requirement: operator 鉴权

admin 的所有 `/api/*` 端点 SHALL 校验 `Authorization: Bearer <operator_token>`;缺失或不匹配 SHALL 返回 401。配置为空 token 时 SHALL 关闭校验(仅本地开发)。

#### Scenario: 无 token 被拒

- **WHEN** 在配置了非空 operator_token 的 admin 上,不带 `Authorization` 头请求任一 `/api/*`
- **THEN** SHALL 返回 401

#### Scenario: 有效 token 放行

- **WHEN** 携带匹配的 `Authorization: Bearer <token>` 请求 `/api/*`
- **THEN** SHALL 不因鉴权返回 401

### Requirement: 内嵌 SPA

admin 服务 SHALL 在非 `/api/*` 路径提供内嵌的单页应用(构建产物随二进制分发);未知非 API 路径 SHALL 回退到 SPA 入口以支持前端路由。

#### Scenario: 根路径返回 SPA

- **WHEN** 请求 `GET /`
- **THEN** SHALL 返回 SPA 的 HTML 入口
