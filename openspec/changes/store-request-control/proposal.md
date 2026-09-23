## Why

Ambry 的 `RequestControl`(`AmbryServerRequests` 的 `RequestControlAdminRequest`)允许运维按请求类型临时静默某个节点的数据面请求,用于故障隔离与降级。antfs 缺这个能力:节点出现盘故障/被大流量打爆时,只能停进程(会连心跳一起停、master 判失联,反而触发再复制风暴)。

## What Changes

- 新模块 `crates/store/src/request_control.rs`:`DataOp`(put/get/delete)、`RequestControl`(节点级 + 分区级禁用 + 可选 deadline,惰性过期)、`classify(method, path, query) -> Option<DataOp>`。
  > **后续变更(2026-09-23,`simplify-delete-model`)**:原先的「复活」/「改 TTL」两个 `DataOp` 已随对应端点移除,枚举由 5 项收窄为 3 项(`put`/`get`/`delete`)。
- `gate` 中间件挂在**数据面路由之后**(在 body extractor 之前短路,禁 put 时不缓冲 body);命中 → `503 SERVICE_UNAVAILABLE` + 原因文本,有 TTL 时带 `Retry-After`;不进 `errors_total`,不计 `request_duration`/`in_flight`。
- 端点:`POST /admin/request-control`(body `{op, partition_id?, enable, ttl_secs?}`,未知 op 400、无本地副本 404)、`GET /admin/request-control`(查当前禁用集);admin 侧转发 `POST|GET /api/stores/{node_id}/request-control`。
- 新指标 `store_server_rejected_requests_total{op,reason}`;新配置 `store_server.request_control_default_ttl_secs`(默认 1800,0 = 不设默认时长)。

## Capabilities

### New Capabilities
- `store-request-control`:节点级/分区级数据面请求静默,含查询、TTL 过期与拒绝计数。

### Modified Capabilities
<!-- 数据面 503 语义新增一类(reason=disabled);admin 增加按节点转发入口。 -->

## Impact

- **crate**:`store`(新模块 + 中间件 + 端点 + 指标/配置)、`admin`(转发路由 + store_client 支持 JSON body)、`ui`(节点页限流面板)。
- **必须豁免**(硬编码白名单,`classify` 返回 None):`GET .../blobs/{key}?replication=1`(与普通 GET 同 path+method,只能按 query 区分;**挡掉它会停掉拉复制,直接导致副本不收敛**)、`replica-metadata`、`segments`、`segments/{position}`、`GET /metrics`、任何 `/admin/**`(否则做出一个无法远程恢复的节点);gRPC 心跳不在 axum 路由内,天然豁免。

## Non-goals / 与 Ambry 的偏离(记录在案)

1. **豁免 `replica-metadata`/`segments`/`?replication=1`** —— Ambry 把 `ReplicaMetadataRequest` 列为可控;antfs 是 pull 复制,本节点禁用入向复制读会让自己成为"永远收不到数据"的孤岛,故必须豁免。
2. **不持久化** —— 重启即恢复全开。Ambry 同样是内存态;持久化会让"重启自救"这一最可靠手段失效。
3. **默认带 TTL 自动过期**(1800s,可配 0 = 永久)—— Ambry 无过期概念,"忘了开回来"会造成静默长期拒服务,按可运维性优先于形式对齐。
4. 不做按请求速率限流(只做开关);不做跨节点统一开关(逐节点,由 admin 逐个转发)。
