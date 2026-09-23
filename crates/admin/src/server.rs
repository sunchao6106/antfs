//! admin HTTP 服务:JSON API(经 [`MasterApi`])+ 多用户 RBAC + 审计 + 内嵌 SPA(D-A/D-C/A9)。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{AuditEntry, AuditLog, DEFAULT_AUDIT_LIMIT, MAX_AUDIT_LIMIT};
use crate::config::{AdminUser, Role};
use crate::error::Error;
use crate::master_api::{MasterApi, NodeDto, ReplicaDto};
use crate::metrics::AdminMetrics;
use crate::store_client;

/// 内嵌的单页应用(vite `singlefile` 构建产物,自包含 HTML+JS+CSS)。
const SPA_INDEX: &str = include_str!("../ui/dist/index.html");

#[derive(Clone)]
pub struct AppState {
    pub master: Arc<dyn MasterApi>,
    pub metrics: Arc<AdminMetrics>,
    /// admin 自身指标注册表(`/metrics` 与 `/api/metrics` 的 admin 段用;与 `metrics` 接入同一个)。
    pub registry: antfs_metric::Registry,
    pub grafana_url: String,
    /// 审计日志(打开失败时为降级实例:record/tail 皆 no-op,请求不受影响)。
    pub audit: Arc<AuditLog>,
    /// 最近若干轮 scrub 的紧凑摘要(进程内环形,重启即空)。
    pub scrub_runs: Arc<crate::scrub::ScrubRuns>,
}

/// 通过鉴权的调用者(token 对应的身份;RBAC 用它门控,审计用它填 `actor`/`role`)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actor {
    /// 操作者名:多用户 = 配置的 `name`;旧单令牌回落 `operator`/`viewer`;关校验时为 `local`。
    pub name: String,
    pub role: Role,
}

impl Actor {
    /// 关校验(仅本地)时的身份:名为 `local`,全权。
    fn local() -> Actor {
        Actor { name: "local".into(), role: Role::Operator }
    }
}

/// 多用户 + 旧双令牌 RBAC(A9)。三者皆空 = **关校验**(仅本地,actor 名 `local`)。
#[derive(Clone, Debug, Default)]
pub struct AuthConfig {
    /// `[[admin.users]]` 配置的多用户令牌(优先匹配)。
    pub users: Vec<AdminUser>,
    pub operator_token: String,
    pub viewer_token: String,
}

impl AuthConfig {
    fn disabled(&self) -> bool {
        self.users.is_empty() && self.operator_token.is_empty() && self.viewer_token.is_empty()
    }

    /// 令牌 → 身份:**先查 `users`**(按 token 精确匹配),再回落旧的两个令牌
    /// (actor 名分别为 `operator` / `viewer`);都不匹配 → `None`(401)。
    fn resolve(&self, token: &str) -> Option<Actor> {
        if let Some(u) = self.users.iter().find(|u| !u.token.is_empty() && u.token == token) {
            // 角色非法说明未过 `AdminConfig::validate`(直接构造 AuthConfig 的调用方);按最小权限处理。
            let role = Role::parse(&u.role).unwrap_or(Role::Viewer);
            return Some(Actor { name: u.name.clone(), role });
        }
        if !self.operator_token.is_empty() && token == self.operator_token {
            return Some(Actor { name: "operator".into(), role: Role::Operator });
        }
        if !self.viewer_token.is_empty() && token == self.viewer_token {
            return Some(Actor { name: "viewer".into(), role: Role::Viewer });
        }
        None
    }
}

/// `/api/*` 两层中间件的共享状态(RBAC + 审计都用同一份鉴权配置。
#[derive(Clone)]
struct ApiLayers {
    auth: AuthConfig,
    audit: Arc<AuditLog>,
}

/// 取 `Authorization: Bearer <token>` 的令牌部分。
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}

/// 请求 → 身份:关校验时 `local`;否则解析令牌(缺失/未知 → `None` → 401)。
fn resolve_actor(auth: &AuthConfig, headers: &HeaderMap) -> Option<Actor> {
    if auth.disabled() {
        return Some(Actor::local());
    }
    auth.resolve(bearer(headers)?)
}

/// 写操作(与既有门控一致:非 GET 即写)。
fn is_write(method: &Method) -> bool {
    *method != Method::GET
}

/// viewer 也不可读的端点:审计日志含操作者名与来源 IP,只对 operator 开放。
fn operator_only_read(path: &str) -> bool {
    path.starts_with("/api/audit")
}

/// 需要落审计的请求:① 所有写操作;② `/api/me`(登录/鉴权成功与失败都记)。
fn audited(method: &Method, path: &str) -> bool {
    is_write(method) || path == "/api/me"
}

/// 建路由:`/api/*` 经审计(最外层)+ RBAC(GET=viewer|operator,写=operator);
/// `/metrics` 无鉴权(Prometheus 抓取惯例);其余回退 SPA。
pub fn build_router(state: AppState, auth: AuthConfig) -> Router {
    let layers = ApiLayers { auth: auth.clone(), audit: state.audit.clone() };
    let api = Router::new()
        .route("/api/me", get(get_me))
        .route("/api/cluster", get(get_cluster))
        .route("/api/nodes", get(get_nodes))
        .route("/api/metrics", get(get_api_metrics))
        .route("/api/audit", get(get_audit))
        .route("/api/partitions", get(get_partitions).post(post_partitions))
        .route("/api/partitions/{id}", get(get_partition))
        .route("/api/partitions/{id}/state", post(post_partition_state))
        .route("/api/partitions/{id}/repair", post(post_repair))
        .route("/api/partitions/{id}/move", post(post_move))
        .route("/api/partitions/{id}/compact", post(post_compact))
        .route("/api/partitions/{id}/compact-jobs", get(get_compact_jobs))
        .route("/api/maintenance", post(post_maintenance))
        .route("/api/balance", get(get_balance))
        .route("/api/partitions/{id}/scrub", post(post_scrub))
        .route("/api/scrub", get(get_scrub_runs))
        .route("/api/balance/eval", post(post_balance_eval))
        .route("/api/balance/start", post(post_balance_start))
        .route("/api/balance/cancel", post(post_balance_cancel))
        .route(
            "/api/stores/{node_id}/request-control",
            get(get_store_request_control).post(post_store_request_control),
        )
        .route(
            "/api/stores/{node_id}/replication-control",
            get(get_store_replication_control).post(post_store_replication_control),
        )
        .route("/api/nodes/{id}/decommission", post(post_decommission))
        // 内层 RBAC(把 Actor 放进 extensions),外层审计(响应返回后按需落档)。
        .layer(from_fn_with_state(layers.clone(), require_auth))
        .layer(from_fn_with_state(layers, audit_requests))
        .with_state(state.clone());

    Router::new()
        .merge(api)
        // Prometheus 抓取口:聚合 admin 自身 + master 指标文本,只读、挂 RBAC 之外。
        .route("/metrics", get(get_prometheus_metrics).with_state(state))
        .fallback(spa_index)
}

async fn spa_index() -> Html<&'static str> {
    Html(SPA_INDEX)
}

/// RBAC 中间件:关校验放行;否则按角色 + 方法门控(viewer 只读,写 → 403,审计端点 → 403,
/// 未知/缺失令牌 → 401)。通过鉴权的 [`Actor`] 放进 request extensions(审计用)。
async fn require_auth(
    State(layers): State<ApiLayers>,
    mut req: axum::extract::Request,
    next: Next,
) -> Response {
    let Some(actor) = resolve_actor(&layers.auth, req.headers()) else {
        return (StatusCode::UNAUTHORIZED, "missing or invalid token").into_response();
    };
    if actor.role == Role::Viewer {
        if operator_only_read(req.uri().path()) {
            return (StatusCode::FORBIDDEN, "operator role required").into_response();
        }
        if is_write(req.method()) {
            return (StatusCode::FORBIDDEN, "viewer role is read-only").into_response();
        }
    }
    req.extensions_mut().insert(actor);
    next.run(req).await
}

/// 审计中间件(最外层,故鉴权失败 401 与 viewer 写 403 也落档):响应返回后追加一条 JSONL。
/// 记不了(降级实例/写失败)只 warn,**绝不影响响应**。
async fn audit_requests(
    State(layers): State<ApiLayers>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    // 与 RBAC 同一套判定(同一个纯函数):未知令牌 → None,只是这里记成 anonymous 而不是 401。
    let actor = resolve_actor(&layers.auth, req.headers());
    let remote_addr = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.to_string())
        .unwrap_or_default();
    let record = audited(&method, &path);

    let resp = next.run(req).await;

    if record {
        let summary = match &query {
            Some(q) => format!("{method} {path}?{q}"),
            None => format!("{method} {path}"),
        };
        layers.audit.record(&AuditEntry {
            ts_ms: now_ms(),
            actor: actor.as_ref().map(|a| a.name.clone()).unwrap_or_else(|| "anonymous".into()),
            role: actor.as_ref().map_or("none".to_string(), |a| a.role.as_str().to_string()),
            method: method.as_str().to_string(),
            path,
            status: resp.status().as_u16(),
            summary,
            remote_addr,
        });
    }
    resp
}

async fn get_me(axum::Extension(actor): axum::Extension<Actor>) -> Response {
    Json(json!({ "role": actor.role.as_str() })).into_response()
}

/// master 错误 → HTTP 状态。
fn map_err(st: &AppState, e: Error) -> (StatusCode, String) {
    st.metrics.master_rpc_errors.inc();
    let code = match e {
        Error::MasterRejected(_) => StatusCode::CONFLICT,
        Error::MasterRpc(_) => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, e.to_string())
}

async fn get_cluster(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();
    match st.master.cluster().await {
        Ok(c) => Json(json!({
            "this_node": c.this_node,
            "leader": c.leader,
            "term": c.term,
            "registered_nodes": c.registered_nodes,
            "next_partition_id": c.next_partition_id,
            "total_capacity_bytes": c.total_capacity_bytes,
            "free_bytes": c.free_bytes,
            "used_bytes": c.used_bytes,
            "nodes_up": c.nodes_up,
            "nodes_total": c.nodes_total,
            "under_replicated_partitions": c.under_replicated_partitions,
            "maintenance": c.maintenance,
            "pause_autogrow": c.pause_autogrow,
            "grafana_url": st.grafana_url,
        }))
        .into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

async fn get_nodes(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();
    match st.master.nodes().await {
        Ok(n) => Json(n).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct PageParams {
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    limit: u32,
    /// 分区状态过滤(空 = 全部);非 RW/RO/SEALED → 400(与 master 同一取值集,边界在此拦一道)。
    #[serde(default)]
    state: String,
}

async fn get_partitions(
    State(st): State<AppState>,
    axum::extract::Query(page): axum::extract::Query<PageParams>,
) -> Response {
    st.metrics.api_requests.inc();
    if !matches!(page.state.as_str(), "" | "RW" | "RO" | "SEALED") {
        return (
            StatusCode::BAD_REQUEST,
            format!("invalid state {} (want RW/RO/SEALED)", page.state),
        )
            .into_response();
    }
    match st.master.partitions(&page.state, page.offset, page.limit).await {
        Ok((items, total)) => Json(json!({
            "items": items,
            "total": total,
            "offset": page.offset,
            "limit": page.limit,
        }))
        .into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

#[derive(Deserialize)]
struct SetStateBody {
    state: String,
}

async fn post_partition_state(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Json(body): Json<SetStateBody>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.set_partition_state(id, &body.state).await {
        Ok(()) => (StatusCode::OK, "state updated").into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

#[derive(Deserialize)]
struct CreatePartitionsBody {
    count: u32,
    #[serde(default)]
    replication_factor: u32,
}

async fn post_partitions(
    State(st): State<AppState>,
    Json(body): Json<CreatePartitionsBody>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.create_partitions(body.count, body.replication_factor).await {
        Ok(ids) => Json(json!({ "partition_ids": ids })).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

#[derive(Deserialize)]
struct DecommissionParams {
    #[serde(default)]
    force: bool,
}

async fn post_decommission(
    State(st): State<AppState>,
    Path(id): Path<u64>,
    axum::extract::Query(p): axum::extract::Query<DecommissionParams>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.decommission(id, p.force).await {
        Ok(()) => (StatusCode::OK, "decommission scheduled").into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `GET /metrics`:admin 自身 Registry + master `GetMetrics` 文本拼接。master 不可达时
/// 仍 200(只留注释行),抓取侧不因单点故障丢 admin 自身指标。
async fn get_prometheus_metrics(State(st): State<AppState>) -> Response {
    let mut out = match antfs_metric::gather(&st.registry) {
        Ok(t) => t,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match st.master.master_metrics_text().await {
        Ok(t) => {
            out.push('\n');
            out.push_str(&t);
        }
        Err(e) => {
            st.metrics.master_rpc_errors.inc();
            out.push_str(&format!("\n# master metrics unavailable: {e}\n"));
        }
    }
    (StatusCode::OK, out).into_response()
}

/// 取第一个匹配 sample 的值(无 label 指标),缺省 0。
fn pick(samples: &[antfs_metric::Sample], name: &str) -> u64 {
    samples.iter().find(|s| s.name == name).map(|s| s.value as u64).unwrap_or(0)
}

/// 按指标名求和(同名带 label 变体也合并)。
fn sum_by_name(samples: &[antfs_metric::Sample], name: &str) -> u64 {
    samples.iter().filter(|s| s.name == name).map(|s| s.value as u64).sum()
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// `GET /api/metrics`:聚合 JSON(master 段 + admin 段 + 各 store 抓取)。master/store
/// 不可达不 5xx:对应段填 0 / scrape_ok=false,前端仍能渲染;但会回 `master_ok=false` +
/// `master_error`,供 UI 显示"master 不可达(数值为占位 0)"而不是把 0 当现状。
async fn get_api_metrics(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();

    let (master, master_ok, master_error) = match st.master.master_metrics_text().await {
        Ok(text) => (master_json(&antfs_metric::parse(&text)), true, String::new()),
        Err(e) => {
            st.metrics.master_rpc_errors.inc();
            tracing::warn!(error = %e, "master metrics unavailable");
            (master_json(&[]), false, e.to_string())
        }
    };

    let admin = match antfs_metric::gather(&st.registry) {
        Ok(text) => {
            let s = antfs_metric::parse(&text);
            json!({
                "api_requests_total": pick(&s, "admin_api_requests_total"),
                "ops_total": pick(&s, "admin_ops_total"),
                "master_rpc_errors_total": pick(&s, "admin_master_rpc_errors_total"),
            })
        }
        Err(e) => {
            tracing::warn!(error = %e, "admin registry gather failed");
            json!({ "api_requests_total": 0, "ops_total": 0, "master_rpc_errors_total": 0 })
        }
    };

    let nodes = match st.master.nodes().await {
        Ok(n) => n,
        Err(e) => {
            st.metrics.master_rpc_errors.inc();
            tracing::warn!(error = %e, "list nodes for store scrape failed");
            Vec::new()
        }
    };
    let client = store_client::new_client();
    let mut set = tokio::task::JoinSet::new();
    for node in nodes {
        set.spawn(scrape_store(client.clone(), st.metrics.clone(), node));
    }
    let mut stores = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(entry) = joined {
            stores.push(entry);
        }
    }

    Json(json!({
        "ts_ms": now_ms(),
        "master_ok": master_ok,
        "master_error": master_error,
        "master": master,
        "admin": admin,
        "stores": stores,
    }))
    .into_response()
}

/// master 段 JSON:无 label 的 `master_*` 指标按名取(取不到填 0,含 master 不可达的空样本)。
fn master_json(s: &[antfs_metric::Sample]) -> serde_json::Value {
    json!({
        "is_leader": pick(s, "master_is_leader"),
        "raft_term": pick(s, "master_raft_term"),
        "heartbeats_total": pick(s, "master_heartbeats_total"),
        "applied_commands_total": pick(s, "master_applied_commands_total"),
        "transitions_issued_total": pick(s, "master_transitions_issued_total"),
        "rereplications_total": pick(s, "master_rereplications_total"),
        "replicas_removed_total": pick(s, "master_replicas_removed_total"),
        "under_replicated_partitions": pick(s, "master_under_replicated_partitions"),
        "registered_nodes": pick(s, "master_registered_nodes"),
        "assign_requests_total": pick(s, "master_assign_requests_total"),
        "assign_failures_total": pick(s, "master_assign_failures_total"),
        "lookup_requests_total": pick(s, "master_lookup_requests_total"),
        "partitions_rw": pick(s, "master_partitions_rw"),
        "partitions_ro": pick(s, "master_partitions_ro"),
        "partitions_sealed": pick(s, "master_partitions_sealed"),
        "writable_capacity_bytes": pick(s, "master_writable_capacity_bytes"),
        "nodes_up": pick(s, "master_nodes_up"),
        "cluster_free_bytes": pick(s, "master_cluster_free_bytes"),
        "cluster_used_bytes": pick(s, "master_cluster_used_bytes"),
        "max_replica_lag_bytes": pick(s, "master_max_replica_lag_bytes"),
        "majority_replicas_down_partitions": pick(s, "master_majority_replicas_down_partitions"),
        "maintenance": pick(s, "master_maintenance"),
        "writable_used_bytes": pick(s, "master_writable_used_bytes"),
        "writable_free_bytes": pick(s, "master_writable_free_bytes"),
        "disk_skew": pick(s, "master_disk_skew"),
    })
}

type HttpClient = store_client::HttpClient;

/// 抓一个 store 节点的 `/metrics` 并聚合成契约字段;失败/超时 → scrape_ok=false + 计 `store_scrape_errors`。
/// `node.up=false` 也照抓(master 的心跳超时判定与 HTTP 活性可能不一致)。
async fn scrape_store(client: HttpClient, metrics: Arc<AdminMetrics>, node: NodeDto) -> serde_json::Value {
    let url = format!("http://{}/metrics", node.addr);
    match store_client::get_text(&client, &url).await {
        Some(text) => {
            let s = antfs_metric::parse(&text);
            json!({
                "node_id": node.node_id,
                "addr": node.addr,
                "up": node.up,
                "scrape_ok": true,
                "put_total": sum_by_name(&s, "store_server_put_total"),
                "get_total": sum_by_name(&s, "store_server_get_total"),
                "put_bytes": sum_by_name(&s, "store_server_put_bytes_total"),
                "get_bytes": sum_by_name(&s, "store_server_get_bytes_total"),
                "errors_total": sum_by_name(&s, "store_server_errors_total"),
                "replicas": sum_by_name(&s, "store_server_replicas"),
                "replication_bytes_total": sum_by_name(&s, "store_server_replication_bytes_total"),
                "compactions_total": sum_by_name(&s, "store_server_compactions_total"),
                "manual_compactions_total": sum_by_name(&s, "store_server_manual_compactions_total"),
                "blob_put_small_count": blob_count(&s, "put", "small"),
                "blob_put_medium_count": blob_count(&s, "put", "medium"),
                "blob_put_large_count": blob_count(&s, "put", "large"),
                "blob_get_small_count": blob_count(&s, "get", "small"),
                "blob_get_medium_count": blob_count(&s, "get", "medium"),
                "blob_get_large_count": blob_count(&s, "get", "large"),
            })
        }
        None => {
            metrics.store_scrape_errors.inc();
            json!({
                "node_id": node.node_id,
                "addr": node.addr,
                "up": node.up,
                "scrape_ok": false,
                "put_total": 0,
                "get_total": 0,
                "put_bytes": 0,
                "get_bytes": 0,
                "errors_total": 0,
                "replicas": 0,
                "replication_bytes_total": 0,
                "compactions_total": 0,
                "manual_compactions_total": 0,
                "blob_put_small_count": 0,
                "blob_put_medium_count": 0,
                "blob_put_large_count": 0,
                "blob_get_small_count": 0,
                "blob_get_medium_count": 0,
                "blob_get_large_count": 0,
            })
        }
    }
}

/// 成功 blob 请求数,按 `op` × `size` **精确取自** `store_server_blob_request_duration_seconds_count`
/// 的对应标签组合(该指标名下有 6 组 sample,按名求和会把这 6 组混成一锅)。
fn blob_count(s: &[antfs_metric::Sample], op: &str, size: &str) -> u64 {
    pick_labeled(s, "store_server_blob_request_duration_seconds_count", &[("op", op), ("size", size)])
}

/// 取同时匹配全部 `want` label 的样本值;取不到填 0(契约稳定,前端不做存在性判断)。
fn pick_labeled(samples: &[antfs_metric::Sample], name: &str, want: &[(&str, &str)]) -> u64 {
    samples
        .iter()
        .find(|s| {
            s.name == name
                && want.iter().all(|(k, v)| s.labels.iter().any(|(lk, lv)| lk == k && lv == v))
        })
        .map(|s| s.value as u64)
        .unwrap_or(0)
}

async fn get_partition(State(st): State<AppState>, Path(id): Path<u32>) -> Response {
    st.metrics.api_requests.inc();
    match st.master.partition_detail(id).await {
        Ok(p) => Json(p).into_response(),
        Err(Error::PartitionNotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("partition {id} not found")).into_response()
        }
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// master 侧 ok=false 也返回 200 + `{"ok":false,"error":...}`,错误文本交前端展示;
/// 仅传输层错误(master 不可达)走 map_err → 502。
async fn post_repair(State(st): State<AppState>, Path(id): Path<u32>) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.repair_partition(id).await {
        Ok(o) => Json(o).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

#[derive(Deserialize)]
struct MoveBody {
    from_node_id: u64,
    /// 0 / 缺省 = 由 master 自动选目标节点。
    #[serde(default)]
    to_node_id: u64,
}

async fn post_move(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Json(body): Json<MoveBody>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.move_replica(id, body.from_node_id, body.to_node_id).await {
        Ok(o) => Json(o).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `POST /api/partitions/{id}/compact`:对该分区的**所有副本节点**并发转发 store 手动 compact,
/// 聚合每个节点的结果(master 侧只提供副本清单,compact 由各 store 本地执行)。
/// 单节点失败(404/超时/网络)不中断其余节点,只在结果里 `ok:false` + `error`。
///
/// `?mode=async`:改为把 compact 提交到各 store 的**后台任务**(`?async=1`),立即回 `job_id`
/// `POST /api/partitions/{id}/scrub?deep=1`:副本一致性巡检(operator 触发)。
///
/// 逐副本并发取只读素材:`GET scrub-report`(摘要)+ `GET replica-metadata`(最近窗口,按 key 求最终态);
/// `deep=1` 时再加 `GET verify`(读全量数据体,慢)。admin 侧做**逻辑态比对**(见 [`crate::scrub::compare`]),
/// 结果同时进内存环形表(`GET /api/scrub`)。分区不存在 → 404;单副本失败不中断整体,只让结论变 `inconclusive`。
#[derive(serde::Deserialize, Default)]
struct ScrubParams {
    #[serde(default)]
    deep: Option<u8>,
}

/// 单副本元数据窗口上限:够覆盖小分区全量,大分区只比最近这一段(报告里用 `partial` 标注)。
const SCRUB_METADATA_MAX_BYTES: u64 = 16 << 20;

async fn post_scrub(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    axum::extract::Query(p): axum::extract::Query<ScrubParams>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    let deep = p.deep.unwrap_or(0) != 0;
    let detail = match st.master.partition_detail(id).await {
        Ok(p) => p,
        Err(Error::PartitionNotFound(_)) => {
            return (StatusCode::NOT_FOUND, format!("partition {id} not found")).into_response()
        }
        Err(e) => return map_err(&st, e).into_response(),
    };

    let client = store_client::new_client();
    let mut set = tokio::task::JoinSet::new();
    for r in detail.replicas {
        let client = client.clone();
        set.spawn(scrub_replica(client, id, r, deep));
    }
    let mut facts: Vec<crate::scrub::ReplicaFacts> = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(f) = joined {
            facts.push(f);
        }
    }
    facts.sort_by_key(|f| f.node_id);

    let verdict = crate::scrub::compare(&facts);
    st.metrics.scrub_runs.inc();
    if !verdict.divergences.is_empty() {
        st.metrics.scrub_divergences.inc_by(verdict.divergences.len() as u64);
    }
    let checked_at_ms = now_ms();
    st.scrub_runs.push(crate::scrub::ScrubRun {
        partition_id: id,
        checked_at_ms,
        deep,
        consistent: verdict.consistent,
        inconclusive: verdict.inconclusive,
        partial: verdict.partial,
        divergence_count: verdict.divergences.len(),
        replicas: facts
            .iter()
            .map(|f| {
                (
                    f.node_id,
                    f.ok,
                    f.report.as_ref().map(|r| r.live_records).unwrap_or(0),
                    f.report.as_ref().map(|r| r.live_bytes).unwrap_or(0),
                )
            })
            .collect(),
    });

    Json(scrub_report_json(id, checked_at_ms, deep, &facts, &verdict)).into_response()
}

/// `GET /api/scrub`(viewer 可读):最近若干轮 scrub 的紧凑摘要(最新在前)。
async fn get_scrub_runs(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();
    let runs: Vec<serde_json::Value> = st
        .scrub_runs
        .snapshot()
        .into_iter()
        .map(|r| {
            json!({
                "partition_id": r.partition_id,
                "checked_at_ms": r.checked_at_ms,
                "deep": r.deep,
                "consistent": r.consistent,
                "inconclusive": r.inconclusive,
                "partial": r.partial,
                "divergence_count": r.divergence_count,
                "replicas": r
                    .replicas
                    .iter()
                    .map(|(node_id, ok, live_records, live_bytes)| json!({
                        "node_id": node_id,
                        "ok": ok,
                        "live_records": live_records,
                        "live_bytes": live_bytes,
                    }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    Json(json!({ "runs": runs })).into_response()
}

/// 取单个副本的 scrub 素材(任一请求失败/404 → `ok=false` + 原因,不中断整体)。
async fn scrub_replica(
    client: store_client::HttpClient,
    pid: u32,
    replica: crate::master_api::ReplicaDto,
    deep: bool,
) -> crate::scrub::ReplicaFacts {
    use crate::scrub::{KeyState, ReplicaFacts, ReportSummary, VerifySummary};
    let base = format!("http://{}", replica.addr);
    let mut facts = ReplicaFacts {
        node_id: replica.node_id,
        ok: false,
        error: String::new(),
        report: None,
        keys: None,
        partial: false,
        verify: None,
    };

    // ① 摘要
    match store_client::get_status(&client, &format!("{base}/partitions/{pid}/scrub-report")).await {
        Ok((StatusCode::OK, body)) => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => {
                let segments = v["segments"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| {
                                Some((s["position"].as_u64()?, s["data_end"].as_u64()?))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                facts.report = Some(ReportSummary {
                    live_records: v["live_records"].as_u64().unwrap_or(0),
                    tombstones: v["tombstones"].as_u64().unwrap_or(0),
                    live_bytes: v["live_bytes"].as_u64().unwrap_or(0),
                    sealed_segments: v["sealed_segments"].as_u64().unwrap_or(0) as usize,
                    segments,
                    token_seg: v["token_seg"].as_u64(),
                    token_off: v["token_off"].as_u64(),
                });
            }
            Err(e) => {
                facts.error = format!("invalid scrub-report: {e}");
                return facts;
            }
        },
        Ok((code, body)) => {
            facts.error = format!("{code}: {body}");
            return facts;
        }
        Err(e) => {
            facts.error = e;
            return facts;
        }
    }

    // ② 元数据窗口(按 key 求最终态)
    let url = format!(
        "{base}/partitions/{pid}/replica-metadata?max_bytes={SCRUB_METADATA_MAX_BYTES}"
    );
    match store_client::get_status(&client, &url).await {
        Ok((StatusCode::OK, body)) => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => {
                let mut map: std::collections::BTreeMap<String, KeyState> =
                    std::collections::BTreeMap::new();
                if let Some(entries) = v["entries"].as_array() {
                    for e in entries {
                        let Some(key) = e["key"].as_str() else { continue };
                        map.insert(
                            key.to_string(),
                            KeyState {
                                deleted: e["deleted"].as_bool().unwrap_or(false),
                                life_version: e["life_version"].as_u64().unwrap_or(0) as u16,
                                size: e["size"].as_u64().unwrap_or(0),
                            },
                        );
                    }
                }
                facts.partial = v["bytes_read"].as_u64().unwrap_or(0) >= SCRUB_METADATA_MAX_BYTES;
                facts.keys = Some(map);
            }
            Err(e) => {
                // 不提前返回:元数据窗口读不出来(典型场景:该副本 log 有坏记录)**正是** deep verify
                // 最有价值的时候 —— 继续取本地校验,把"坏在哪"带回去;key 级比对标为部分比对。
                facts.error = format!("replica-metadata unavailable: {e}");
                facts.partial = true;
            }
        },
        Ok((code, body)) => {
            facts.error = format!("replica-metadata {code}: {body}");
            facts.partial = true;
        }
        Err(e) => {
            facts.error = format!("replica-metadata: {e}");
            facts.partial = true;
        }
    }

    // ③ 本地完整性校验(仅 deep)
    if deep {
        let url = format!("{base}/partitions/{pid}/verify?max_bytes={SCRUB_METADATA_MAX_BYTES}");
        match store_client::get_status(&client, &url).await {
            Ok((StatusCode::OK, body)) => match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(v) => {
                    facts.verify = Some(VerifySummary {
                        records: v["records"].as_u64().unwrap_or(0),
                        bad_records: v["bad_records"].as_u64().unwrap_or(0),
                        index_mismatch: v["index_mismatch"].as_u64().unwrap_or(0),
                        truncated: v["truncated"].as_bool().unwrap_or(false),
                    });
                }
                Err(e) => facts.error = format!("invalid verify: {e}"),
            },
            Ok((code, body)) => facts.error = format!("verify {code}: {body}"),
            Err(e) => facts.error = format!("verify: {e}"),
        }
    }

    // 摘要拿到即可比对(ok 表示"有素材可参与比对");元数据缺口由 `partial` + `error` 表达。
    facts.ok = true;
    facts
}

/// 组装 scrub 响应 JSON(对外契约见 `docs/design/cluster.md` §11)。
fn scrub_report_json(
    pid: u32,
    checked_at_ms: u64,
    deep: bool,
    facts: &[crate::scrub::ReplicaFacts],
    verdict: &crate::scrub::Verdict,
) -> serde_json::Value {
    use crate::scrub::Divergence;
    let replicas: Vec<serde_json::Value> = facts
        .iter()
        .map(|f| {
            json!({
                "node_id": f.node_id,
                "ok": f.ok,
                "error": f.error,
                "report": f.report.as_ref().map(|r| json!({
                    "live_records": r.live_records,
                    "tombstones": r.tombstones,
                    "live_bytes": r.live_bytes,
                    "sealed_segments": r.sealed_segments,
                    "segments": r.segments.iter().map(|(p, e)| json!({"position": p, "data_end": e})).collect::<Vec<_>>(),
                    "token_seg": r.token_seg,
                    "token_off": r.token_off,
                })),
                "key_count": f.keys.as_ref().map(|m| m.len()),
                "partial": f.partial,
                "verify": f.verify.as_ref().map(|v| json!({
                    "records": v.records,
                    "bad_records": v.bad_records,
                    "index_mismatch": v.index_mismatch,
                    "truncated": v.truncated,
                })),
            })
        })
        .collect();
    let divergences: Vec<serde_json::Value> = verdict
        .divergences
        .iter()
        .map(|d| match d {
            Divergence::Summary { field, detail } => {
                json!({"kind": "summary", "field": field, "detail": detail})
            }
            Divergence::Verify { node_id, detail } => {
                json!({"kind": "verify", "node_id": node_id, "detail": detail})
            }
            Divergence::Key { key_hex, nodes } => json!({
                "kind": "key",
                "key_hex": key_hex,
                "nodes": nodes.iter().map(|(n, s)| json!({"node_id": n, "state": s})).collect::<Vec<_>>(),
            }),
        })
        .collect();
    json!({
        "partition_id": pid,
        "checked_at_ms": checked_at_ms,
        "deep": deep,
        "replicas": replicas,
        "verdict": {
            "consistent": verdict.consistent,
            "inconclusive": verdict.inconclusive,
            "partial": verdict.partial,
            "divergences": divergences,
        },
        "error": "",
    })
}

/// (进度用 `GET .../compact-jobs` 轮询);缺省/`sync` 走原同步路径(等各 store 跑完)。
async fn post_compact(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    axum::extract::Query(p): axum::extract::Query<CompactParams>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    let async_mode = match p.mode.as_str() {
        "" | "sync" => false,
        "async" => true,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid mode {other} (want sync/async)"),
            )
                .into_response()
        }
    };
    let p_detail = match st.master.partition_detail(id).await {
        Ok(p) => p,
        Err(Error::PartitionNotFound(_)) => {
            return (StatusCode::NOT_FOUND, format!("partition {id} not found")).into_response()
        }
        Err(e) => return map_err(&st, e).into_response(),
    };
    if p_detail.replicas.is_empty() {
        return Json(json!({ "results": [], "error": format!("partition {id} has no replicas") }))
            .into_response();
    }

    let client = store_client::new_client();
    let mut set = tokio::task::JoinSet::new();
    for r in p_detail.replicas {
        let client = client.clone();
        if async_mode {
            set.spawn(submit_compact_replica(client, id, r));
        } else {
            set.spawn(compact_replica(client, id, r));
        }
    }
    let mut results = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(entry) = joined {
            results.push(entry);
        }
    }
    results.sort_by_key(|v| v["node_id"].as_u64().unwrap_or(0));
    Json(json!({ "results": results, "error": "" })).into_response()
}

/// `?mode=` 取值:缺省 = 同步(等各 store 跑完);`async` = 提交后台任务立即拿 `job_id`。
#[derive(Deserialize)]
struct CompactParams {
    #[serde(default)]
    mode: String,
}

/// 对一个副本节点发同步 compact(store 无本地副本 → 404;store 侧全库 compact 可能超 2s → 报超时)。
async fn compact_replica(client: HttpClient, pid: u32, replica: ReplicaDto) -> serde_json::Value {
    let url = format!("http://{}/admin/partitions/{pid}/compact", replica.addr);
    let node_id = replica.node_id;
    match store_client::post_empty(&client, &url).await {
        Ok((status, body)) if status.is_success() => {
            match serde_json::from_str::<store_client::CompactStats>(&body) {
                Ok(s) => json!({
                    "node_id": node_id,
                    "ok": true,
                    "live_bytes": s.live_bytes,
                    "dead_bytes": s.dead_bytes,
                    "segments_before": s.segments_before,
                    "segments_after": s.segments_after,
                }),
                Err(e) => node_failure(node_id, format!("invalid response: {e}")),
            }
        }
        Ok((status, body)) => node_failure(node_id, status_error(status, &body)),
        Err(e) => node_failure(node_id, e),
    }
}

/// 让一个副本节点把 compact 提交到后台(store `?async=1`)并带回 `job_id`。
async fn submit_compact_replica(
    client: HttpClient,
    pid: u32,
    replica: ReplicaDto,
) -> serde_json::Value {
    let url = format!("http://{}/admin/partitions/{pid}/compact?async=1", replica.addr);
    let node_id = replica.node_id;
    match store_client::post_empty(&client, &url).await {
        Ok((status, body)) if status.is_success() => {
            match serde_json::from_str::<store_client::CompactionSubmitted>(&body) {
                Ok(s) => json!({ "node_id": node_id, "ok": true, "job_id": s.job_id }),
                Err(e) => node_failure(node_id, format!("invalid response: {e}")),
            }
        }
        Ok((status, body)) => node_failure(node_id, status_error(status, &body)),
        Err(e) => node_failure(node_id, e),
    }
}

/// `GET /api/partitions/{id}/compact-jobs`:各副本节点上**该分区**的后台 compact 任务
/// (每节点最多最近 [`MAX_COMPACT_JOBS_PER_NODE`] 条,最新在前;join 不上的节点 `ok:false` + `error`)。
async fn get_compact_jobs(State(st): State<AppState>, Path(id): Path<u32>) -> Response {
    st.metrics.api_requests.inc();
    let p = match st.master.partition_detail(id).await {
        Ok(p) => p,
        Err(Error::PartitionNotFound(_)) => {
            return (StatusCode::NOT_FOUND, format!("partition {id} not found")).into_response()
        }
        Err(e) => return map_err(&st, e).into_response(),
    };

    let client = store_client::new_client();
    let mut set = tokio::task::JoinSet::new();
    for r in p.replicas {
        set.spawn(compact_jobs_replica(client.clone(), id, r));
    }
    let mut results = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(entry) = joined {
            results.push(entry);
        }
    }
    results.sort_by_key(|v| v["node_id"].as_u64().unwrap_or(0));
    Json(json!({ "results": results })).into_response()
}

/// 每个副本节点最多回最近几条任务(UI 只关心刚提交的那次;store 台账全量是 32 条)。
const MAX_COMPACT_JOBS_PER_NODE: usize = 5;

/// 取一个副本节点上该分区的最近若干条 compact 任务(store `GET /admin/compactions` 后本地过滤)。
async fn compact_jobs_replica(client: HttpClient, pid: u32, replica: ReplicaDto) -> serde_json::Value {
    let url = format!("http://{}/admin/compactions", replica.addr);
    let node_id = replica.node_id;
    match store_client::get_status(&client, &url).await {
        Ok((status, body)) if status.is_success() => {
            match serde_json::from_str::<store_client::CompactionsResponse>(&body) {
                Ok(r) => {
                    let jobs = r
                        .jobs
                        .into_iter()
                        .filter(|j| j.partition_id == pid)
                        .take(MAX_COMPACT_JOBS_PER_NODE)
                        .collect::<Vec<_>>();
                    json!({ "node_id": node_id, "ok": true, "jobs": jobs })
                }
                Err(e) => node_failure(node_id, format!("invalid response: {e}")),
            }
        }
        Ok((status, body)) => node_failure(node_id, status_error(status, &body)),
        Err(e) => node_failure(node_id, e),
    }
}

/// 单节点失败的结果 `{node_id, ok:false, error}`(store 状态码 + 错误文本,或传输失败原因)。
fn node_failure(node_id: u64, error: String) -> serde_json::Value {
    json!({ "node_id": node_id, "ok": false, "error": error })
}

/// store 的错误文本:有 body 用 `状态码: body`,无 body 只有状态码。
fn status_error(status: StatusCode, body: &str) -> String {
    if body.is_empty() {
        status.as_u16().to_string()
    } else {
        format!("{}: {body}", status.as_u16())
    }
}

#[derive(Deserialize)]
struct MaintenanceBody {
    /// `true` = 进入维护模式(暂停自动收敛),`false` = 恢复。
    paused: bool,
    /// 是否连带暂停自动扩容(master 侧仅 `paused=true` 时采纳)。
    #[serde(default)]
    pause_autogrow: bool,
}

/// `GET /api/balance`(viewer 可读):当前均衡计划 + master 派生的进度。
async fn get_balance(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();
    match st.master.balance_state().await {
        Ok(state) => Json(state).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `POST /api/balance/eval` 的请求体(`max_moves` 省略 = 用 master 配置默认)。
#[derive(Deserialize, Default)]
struct BalanceEvalBody {
    #[serde(default)]
    max_moves: u32,
}

/// `POST /api/balance/{start,cancel}` 的请求体。
#[derive(Deserialize)]
struct BalancePlanIdBody {
    plan_id: u64,
}

/// `POST /api/balance/eval`(operator):评估并落一份 PREVIEW 计划。
async fn post_balance_eval(State(st): State<AppState>, Json(body): Json<BalanceEvalBody>) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.balance_eval(body.max_moves).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `POST /api/balance/start`(operator):确认执行指定计划(plan_id 防陈旧确认)。
async fn post_balance_start(State(st): State<AppState>, Json(body): Json<BalancePlanIdBody>) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.balance_start(body.plan_id).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `POST /api/balance/cancel`(operator):取消计划(不回滚已 apply 的搬迁,在途由 reconciler 自然收敛)。
async fn post_balance_cancel(State(st): State<AppState>, Json(body): Json<BalancePlanIdBody>) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.balance_cancel(body.plan_id).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `POST /api/maintenance`(operator):维护模式开关,转发 master `SetMaintenance`。
async fn post_maintenance(State(st): State<AppState>, Json(body): Json<MaintenanceBody>) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    match st.master.set_maintenance(body.paused, body.pause_autogrow).await {
        Ok(()) => (StatusCode::OK, "maintenance updated").into_response(),
        Err(e) => map_err(&st, e).into_response(),
    }
}

/// `node_id → addr`:从 master 节点清单里找;无此节点 → 404。
async fn node_addr(st: &AppState, node_id: u64) -> std::result::Result<String, (StatusCode, String)> {
    match st.master.nodes().await {
        Ok(nodes) => nodes
            .into_iter()
            .find(|n| n.node_id == node_id)
            .map(|n| n.addr)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("node {node_id} not found"))),
        Err(e) => Err(map_err(st, e)),
    }
}

/// `POST /api/stores/{node_id}/request-control`(operator):把请求静默开关转发到该 store 的
/// `POST /admin/request-control`(body 原样转发,校验由 store 负责:未知 op → 400、无该副本 → 404)。
async fn post_store_request_control(
    State(st): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    let addr = match node_addr(&st, node_id).await {
        Ok(a) => a,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let client = store_client::new_client();
    let url = format!("http://{addr}/admin/request-control");
    forward_store(store_client::post_json(&client, &url, body.to_string()).await)
}

/// `GET /api/stores/{node_id}/request-control`:该 store 当前的禁用集(JSON 数组)。
async fn get_store_request_control(State(st): State<AppState>, Path(node_id): Path<u64>) -> Response {
    st.metrics.api_requests.inc();
    let addr = match node_addr(&st, node_id).await {
        Ok(a) => a,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let client = store_client::new_client();
    let url = format!("http://{addr}/admin/request-control");
    forward_store(store_client::get_status(&client, &url).await)
}

/// `POST /api/stores/{node_id}/replication-control`(operator):把复制开关转发到该 store 的
/// `POST /admin/replication-control`(body 原样转发:暂停的是**该节点主动拉复制**,不影响别人来拉)。
async fn post_store_replication_control(
    State(st): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    st.metrics.api_requests.inc();
    st.metrics.ops.inc();
    let addr = match node_addr(&st, node_id).await {
        Ok(a) => a,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let client = store_client::new_client();
    let url = format!("http://{addr}/admin/replication-control");
    forward_store(store_client::post_json(&client, &url, body.to_string()).await)
}

/// `GET /api/stores/{node_id}/replication-control`:该 store 当前的复制开关(`{"paused":bool}`)。
async fn get_store_replication_control(State(st): State<AppState>, Path(node_id): Path<u64>) -> Response {
    st.metrics.api_requests.inc();
    let addr = match node_addr(&st, node_id).await {
        Ok(a) => a,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let client = store_client::new_client();
    let url = format!("http://{addr}/admin/replication-control");
    forward_store(store_client::get_status(&client, &url).await)
}

#[derive(Deserialize)]
struct AuditParams {
    /// 返回条数(缺省 [`DEFAULT_AUDIT_LIMIT`],上限 [`MAX_AUDIT_LIMIT`])。
    #[serde(default)]
    limit: Option<usize>,
}

/// `GET /api/audit?limit=N`(operator):最近的审计记录(**最新在前**);审计不可用时回空表。
async fn get_audit(
    State(st): State<AppState>,
    axum::extract::Query(p): axum::extract::Query<AuditParams>,
) -> Response {
    st.metrics.api_requests.inc();
    let limit = p.limit.unwrap_or(DEFAULT_AUDIT_LIMIT).min(MAX_AUDIT_LIMIT);
    Json(json!({ "entries": st.audit.tail(limit) })).into_response()
}

/// store 响应**原样透出**(状态码 + body,JSON 则保留 JSON content-type);
/// 只有超时/连接失败这类传输层错误才转 502,并把原因(超时/network error)透给操作者。
fn forward_store(r: std::result::Result<(StatusCode, String), String>) -> Response {
    match r {
        Ok((status, body)) => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => (status, Json(v)).into_response(),
            Err(_) => (status, body).into_response(),
        },
        Err(e) => (StatusCode::BAD_GATEWAY, format!("store unreachable: {e}")).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_api::{
        BalanceMoveDto, BalanceOpOutcome, BalancePlanDto, BalanceStateDto, ClusterDto, MoveOutcome,
        NodeDto, PartitionDto, RepairOutcome, ReplicaDto,
    };
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::Request;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeMaster {
        /// 记录运维调用("repair {pid}" / "move {pid} {from} {to}"),供测试断言参数透传。
        calls: Mutex<Vec<String>>,
        /// 每个分区返回的副本(compact 测试用来把 addr 指向假 store)。
        replicas: Vec<ReplicaDto>,
        /// `nodes()` 返回值(/api/metrics 抓 store 测试用)。
        nodes: Vec<NodeDto>,
        /// 每个分区上报的 `used_bytes`(按副本顺序)。
        used_bytes: u64,
    }

    impl FakeMaster {
        fn take_calls(&self) -> Vec<String> {
            std::mem::take(&mut self.calls.lock().unwrap_or_else(|p| p.into_inner()))
        }

        /// 让 `nodes()` 回一个指向该地址的在线节点。
        fn with_node(mut self, node_id: u64, addr: &str) -> Self {
            self.nodes.push(NodeDto {
                node_id,
                dc: "dc1".into(),
                rack: "r1".into(),
                addr: addr.to_string(),
                up: true,
                disks: vec![],
            });
            self
        }

        /// 让每个分区的副本集含一个位于该地址的副本。
        fn with_replica(mut self, node_id: u64, addr: &str) -> Self {
            self.replicas.push(ReplicaDto {
                node_id,
                addr: addr.to_string(),
                disk_id: "d1".into(),
                target_state: "ACTIVE".into(),
                up: true,
                current_state: Some("ACTIVE".into()),
                lag_bytes: Some(0),
                used_bytes: 0,
            });
            self
        }
    }

    #[async_trait]
    impl MasterApi for FakeMaster {
        async fn cluster(&self) -> crate::error::Result<ClusterDto> {
            Ok(ClusterDto {
                this_node: 1,
                leader: Some(1),
                term: 3,
                registered_nodes: 2,
                next_partition_id: 5,
                total_capacity_bytes: 2000,
                free_bytes: 1500,
                used_bytes: 500,
                nodes_up: 2,
                nodes_total: 2,
                under_replicated_partitions: 0,
                maintenance: false,
                pause_autogrow: false,
            })
        }
        async fn nodes(&self) -> crate::error::Result<Vec<NodeDto>> {
            Ok(self.nodes.clone())
        }
        async fn partitions(
            &self,
            state: &str,
            offset: u64,
            limit: u32,
        ) -> crate::error::Result<(Vec<PartitionDto>, u64)> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("partitions {state} {offset} {limit}"));
            let all: Vec<PartitionDto> = (0..5u32)
                .map(|i| PartitionDto {
                    partition_id: i,
                    state: if i == 0 { "RO".to_string() } else { "RW".to_string() },
                    replication_factor: 3,
                    replicas: self.replicas.clone(),
                    used_bytes: self.used_bytes,
                    healthy_replicas: 3,
                    healable: true,
                    under_replicated_reason: String::new(),
                    moving_out_nodes: vec![],
                })
                .filter(|p| state.is_empty() || p.state == state)
                .collect();
            let take = if limit == 0 { all.len() } else { limit as usize };
            let page = all.iter().skip(offset as usize).take(take).cloned().collect();
            Ok((page, all.len() as u64))
        }
        async fn create_partitions(&self, count: u32, _rf: u32) -> crate::error::Result<Vec<u32>> {
            Ok((0..count).collect())
        }
        async fn decommission(&self, _node_id: u64, _force: bool) -> crate::error::Result<()> {
            Ok(())
        }
        async fn set_partition_state(&self, _id: u32, _state: &str) -> crate::error::Result<()> {
            Ok(())
        }
        async fn set_maintenance(
            &self,
            paused: bool,
            pause_autogrow: bool,
        ) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("maintenance {paused} {pause_autogrow}"));
            Ok(())
        }
        async fn master_metrics_text(&self) -> crate::error::Result<String> {
            Ok("master_is_leader 1\nmaster_raft_term 3\nmaster_heartbeats_total 7\n".to_string())
        }
        async fn partition_detail(&self, partition_id: u32) -> crate::error::Result<PartitionDto> {
            let (items, _) = self.partitions("", 0, 0).await?;
            items
                .into_iter()
                .find(|p| p.partition_id == partition_id)
                .ok_or(crate::error::Error::PartitionNotFound(partition_id))
        }
        async fn repair_partition(&self, partition_id: u32) -> crate::error::Result<RepairOutcome> {
            self.calls.lock().unwrap_or_else(|p| p.into_inner()).push(format!("repair {partition_id}"));
            Ok(RepairOutcome { ok: true, error: String::new(), added_node_id: 104, added_disk_id: "d1".into() })
        }
        async fn move_replica(
            &self,
            partition_id: u32,
            from_node_id: u64,
            to_node_id: u64,
        ) -> crate::error::Result<MoveOutcome> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("move {partition_id} {from_node_id} {to_node_id}"));
            Ok(MoveOutcome { ok: true, error: String::new(), to_node_id, to_disk_id: "d2".into() })
        }

        async fn balance_state(&self) -> crate::error::Result<BalanceStateDto> {
            Ok(BalanceStateDto {
                present: true,
                plan: Some(BalancePlanDto {
                    plan_id: 7,
                    status: "PREVIEW".into(),
                    created_at_ms: 1_758_600_000_000,
                    max_in_flight: 1,
                    total: 2,
                    pending: 2,
                    in_flight: 0,
                    done: 0,
                    failed: 0,
                    moves: vec![BalanceMoveDto {
                        partition_id: 3,
                        from_node_id: 101,
                        from_disk_id: "store-101".into(),
                        to_node_id: 104,
                        to_disk_id: "store-104".into(),
                        status: "pending".into(),
                        error: String::new(),
                    }],
                }),
            })
        }

        async fn balance_eval(&self, max_moves: u32) -> crate::error::Result<BalanceOpOutcome> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("balance eval {max_moves}"));
            let state = self.balance_state().await?;
            Ok(BalanceOpOutcome { ok: true, error: String::new(), plan: state.plan })
        }

        async fn balance_start(&self, plan_id: u64) -> crate::error::Result<BalanceOpOutcome> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("balance start {plan_id}"));
            let ok = plan_id == 7;
            Ok(BalanceOpOutcome {
                ok,
                error: if ok { String::new() } else { format!("plan {plan_id} not found") },
                plan: None,
            })
        }

        async fn balance_cancel(&self, plan_id: u64) -> crate::error::Result<BalanceOpOutcome> {
            self.calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(format!("balance cancel {plan_id}"));
            Ok(BalanceOpOutcome { ok: true, error: String::new(), plan: None })
        }
    }

    /// 测试用审计路径:父目录不存在 → 降级空实现(不产生文件,也不互相干扰)。
    fn no_audit() -> Arc<AuditLog> {
        Arc::new(AuditLog::open(std::env::temp_dir().join("antfs-admin-audit-none/t.jsonl")))
    }

    /// 每个测试独立的审计文件(并行测试 + 复用进程 pid,故带上 tag)。
    fn audit_file(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("antfs-admin-audit-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("audit.jsonl")
    }

    fn app_with(master: Arc<FakeMaster>, auth: AuthConfig) -> Router {
        app_audited(master, auth, no_audit())
    }

    /// 与 [`app_with`] 相同,但注入指定的审计日志(审计相关测试用)。
    fn app_audited(master: Arc<FakeMaster>, auth: AuthConfig, audit: Arc<AuditLog>) -> Router {
        let registry = antfs_metric::Registry::new();
        let state = AppState {
            master,
            metrics: Arc::new(AdminMetrics::new(&registry).unwrap()),
            registry,
            grafana_url: "http://grafana.local".into(),
            audit,
            scrub_runs: Arc::new(crate::scrub::ScrubRuns::default()),
        };
        build_router(state, auth)
    }

    fn app_auth(auth: AuthConfig) -> Router {
        app_with(Arc::new(FakeMaster::default()), auth)
    }

    fn app(token: &str) -> Router {
        app_auth(AuthConfig {
            operator_token: token.to_string(),
            ..Default::default()
        })
    }

    /// 审计文件里的原始行数(判断"没记"用)。
    fn audit_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    async fn status(router: &Router, req: Request<Body>) -> StatusCode {
        router.clone().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn missing_token_rejected_when_configured() {
        let router = app("secret");
        let req = Request::get("/api/cluster").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_token_allows_cluster() {
        let router = app("secret");
        let req = Request::get("/api/cluster")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn empty_token_disables_auth() {
        let router = app("");
        let req = Request::get("/api/cluster").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn viewer_reads_ok_but_writes_forbidden() {
        let router = app_auth(AuthConfig {
            operator_token: "op".into(),
            viewer_token: "view".into(),
            ..Default::default()
        });
        // viewer 读 → 200
        let get = Request::get("/api/partitions")
            .header(header::AUTHORIZATION, "Bearer view")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(&router, get).await, StatusCode::OK);
        // viewer 写 → 403
        let post = Request::post("/api/partitions")
            .header(header::AUTHORIZATION, "Bearer view")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"count":1,"replication_factor":1}"#))
            .unwrap();
        assert_eq!(status(&router, post).await, StatusCode::FORBIDDEN);
        // compact 同属写操作(方法门控,与路径无关)→ viewer 403
        let compact = Request::post("/api/partitions/1/compact")
            .header(header::AUTHORIZATION, "Bearer view")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(&router, compact).await, StatusCode::FORBIDDEN);
        // 转发类写操作同样门控(不触达 master/store,故节点是否存在无关)
        let repl = Request::post("/api/stores/101/replication-control")
            .header(header::AUTHORIZATION, "Bearer view")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":true}"#))
            .unwrap();
        assert_eq!(status(&router, repl).await, StatusCode::FORBIDDEN);
        // operator 写 → 200
        let post_op = Request::post("/api/partitions")
            .header(header::AUTHORIZATION, "Bearer op")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"count":1,"replication_factor":1}"#))
            .unwrap();
        assert_eq!(status(&router, post_op).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn me_reports_role() {
        let router = app_auth(AuthConfig {
            operator_token: "op".into(),
            viewer_token: "view".into(),
            ..Default::default()
        });
        let req = Request::get("/api/me")
            .header(header::AUTHORIZATION, "Bearer view")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["role"], "viewer");
    }

    #[tokio::test]
    async fn partitions_paginated_with_total() {
        let router = app("");
        let req = Request::get("/api/partitions?offset=1&limit=2").body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 5);
        assert_eq!(v["items"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn create_partitions_routes_to_master() {
        let router = app("");
        let req = Request::post("/api/partitions")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"count":3,"replication_factor":2}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["partition_ids"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn spa_served_on_root() {
        let router = app("secret");
        let req = Request::get("/").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);
    }

    async fn body_json(router: &Router, req: Request<Body>) -> serde_json::Value {
        let resp = router.clone().oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// 起一个假 store(真监听 `127.0.0.1:0`,axum app),返回其 `host:port`:
    /// `/metrics` 直出 `metrics` 文本;`with_compact` 时 `/admin/partitions/{pid}/compact` 回固定统计,
    /// 否则该路径 404(等价于 store 侧"无本地副本")。
    async fn spawn_fake_store(metrics: &'static str, with_compact: bool) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut app = Router::new().route("/metrics", get(move || async move { metrics }));
        if with_compact {
            app = app.route("/admin/partitions/{pid}/compact", post(fake_compact));
        }
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr.to_string()
    }

    /// 假 store 的 compact 响应(与 store 侧 `CompactResponse` 同 JSON 契约)。
    async fn fake_compact() -> Json<serde_json::Value> {
        Json(json!({
            "live_bytes": 1024,
            "dead_bytes": 2048,
            "segments_before": 3,
            "segments_after": 1,
        }))
    }

    /// 假 store 的 request-control 端点:POST 记录收到的 body 并按 `op` 回响应(`bogus` → 400);
    /// GET 回固定的禁用集。返回 `(host:port, 收到的 body 列表)`。
    async fn spawn_request_control_store() -> (String, Arc<Mutex<Vec<String>>>) {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let rec = seen.clone();
        let app = Router::new().route(
            "/admin/request-control",
            post(move |body: axum::body::Bytes| {
                let rec = rec.clone();
                async move {
                    let text = String::from_utf8_lossy(&body).to_string();
                    rec.lock().unwrap_or_else(|p| p.into_inner()).push(text.clone());
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    if v["op"] == "bogus" {
                        return (StatusCode::BAD_REQUEST, "unknown op: bogus").into_response();
                    }
                    Json(json!([{ "op": v["op"], "partition_id": null, "remaining_secs": 600 }]))
                        .into_response()
                }
            })
            .get(|| async {
                Json(json!([{ "op": "put", "partition_id": null, "remaining_secs": 900 }]))
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr.to_string(), seen)
    }

    #[tokio::test]
    async fn prometheus_metrics_no_auth_aggregates_admin_and_master() {
        let router = app("secret"); // 配了 operator token,/metrics 仍在 RBAC 之外
        let req = Request::get("/metrics").body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("admin_api_requests_total"), "admin 自身指标: {text}");
        assert!(text.contains("master_raft_term 3"), "master GetMetrics 文本: {text}");
    }

    #[tokio::test]
    async fn api_metrics_shape_has_master_admin_stores() {
        let router = app("secret");
        let req = Request::get("/api/metrics")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        let v = body_json(&router, req).await;
        assert!(v["ts_ms"].as_u64().unwrap() > 0);
        assert_eq!(v["master"]["is_leader"], 1);
        assert_eq!(v["master"]["raft_term"], 3);
        assert_eq!(v["master"]["heartbeats_total"], 7);
        assert!(v["admin"]["api_requests_total"].as_u64().unwrap() >= 1);
        assert_eq!(v["stores"].as_array().unwrap().len(), 0);
        // 新增 master 字段必须存在;master 文本里没有的填 0(契约稳定,前端不做存在性判断)。
        for f in [
            "assign_requests_total",
            "assign_failures_total",
            "lookup_requests_total",
            "partitions_rw",
            "partitions_ro",
            "partitions_sealed",
            "writable_capacity_bytes",
            "nodes_up",
            "cluster_free_bytes",
            "cluster_used_bytes",
            "max_replica_lag_bytes",
            "majority_replicas_down_partitions",
            "maintenance",
            "writable_used_bytes",
            "writable_free_bytes",
            "disk_skew",
        ] {
            assert_eq!(v["master"][f], 0, "field {f}: {v}");
        }
    }

    #[tokio::test]
    async fn cluster_reports_maintenance_flags() {
        let router = app("");
        let req = Request::get("/api/cluster").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["maintenance"], false);
        assert_eq!(v["pause_autogrow"], false);
    }

    #[tokio::test]
    async fn partitions_state_filter_is_forwarded_and_validated() {
        let master = Arc::new(FakeMaster { used_bytes: 7, ..Default::default() });
        let router = app_with(master.clone(), AuthConfig::default());

        // state=RW:透传给 master(过滤先于分页),DTO 带 used_bytes。
        let req = Request::get("/api/partitions?state=RW&offset=0&limit=2").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["total"], 4, "假 master 里 5 个分区有 1 个 RO: {v}");
        assert_eq!(v["items"].as_array().unwrap().len(), 2);
        assert_eq!(v["items"][0]["used_bytes"], 7);
        assert_eq!(master.take_calls(), vec!["partitions RW 0 2"]);

        // 空 state = 全部(仍透传空串)。
        let req = Request::get("/api/partitions").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["total"], 5);
        assert_eq!(master.take_calls(), vec!["partitions  0 0"]);

        // 非法 state 在 admin 边界即拒(不打扰 master)。
        let req = Request::get("/api/partitions?state=BOGUS").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::BAD_REQUEST);
        assert_eq!(master.take_calls(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn maintenance_route_forwards_flags() {
        let master = Arc::new(FakeMaster::default());
        let router = app_with(master.clone(), AuthConfig::default());

        let req = Request::post("/api/maintenance")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":true,"pause_autogrow":true}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // pause_autogrow 缺省 → false。
        let req = Request::post("/api/maintenance")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":false}"#))
            .unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);

        assert_eq!(master.take_calls(), vec!["maintenance true true", "maintenance false false"]);
    }

    #[tokio::test]
    async fn request_control_forwards_to_store_and_surfaces_errors() {
        let (addr, seen) = spawn_request_control_store().await;
        let router = app_with(Arc::new(FakeMaster::default().with_node(101, &addr)), AuthConfig::default());

        // GET:store 当前禁用集原样透出。
        let req = Request::get("/api/stores/101/request-control").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v[0]["op"], "put");
        assert_eq!(v[0]["remaining_secs"], 900);

        // POST:body 原样转发到 store(含 partition_id / ttl_secs)。
        let req = Request::post("/api/stores/101/request-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"op":"put","partition_id":3,"enable":true,"ttl_secs":600}"#))
            .unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v[0]["op"], "put");
        assert_eq!(v[0]["remaining_secs"], 600);
        let sent: serde_json::Value = serde_json::from_str(&seen.lock().unwrap()[0]).unwrap();
        assert_eq!(sent, json!({ "op": "put", "partition_id": 3, "enable": true, "ttl_secs": 600 }));

        // store 的 400(未知 op)原样透出。
        let req = Request::post("/api/stores/101/request-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"op":"bogus","enable":true}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("unknown op"), "store 错误文本应透出");

        // master 无此节点 → 404(不发起转发)。
        let req = Request::get("/api/stores/999/request-control").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);

        // 节点存在但连不上 → 502 + 原因。
        let dead_addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let router = app_with(Arc::new(FakeMaster::default().with_node(102, &dead_addr)), AuthConfig::default());
        let req = Request::post("/api/stores/102/request-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"op":"put","enable":true}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("network error"), "连接失败原因应透出");
    }

    /// 假 store 的复制开关端点:GET/POST `/admin/replication-control`(`{paused}` 进出 + 内部状态)。
    /// 返回 `(host:port, 收到的 body 列表, 当前开关)`。
    async fn spawn_replication_store() -> (String, Arc<Mutex<Vec<String>>>, Arc<std::sync::atomic::AtomicBool>)
    {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let rec = seen.clone();
        let st_post = paused.clone();
        let st_get = paused.clone();
        let app = Router::new().route(
            "/admin/replication-control",
            post(move |body: axum::body::Bytes| {
                let rec = rec.clone();
                let st = st_post.clone();
                async move {
                    let text = String::from_utf8_lossy(&body).to_string();
                    rec.lock().unwrap_or_else(|p| p.into_inner()).push(text.clone());
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    let want = v["paused"].as_bool().unwrap_or(false);
                    st.store(want, std::sync::atomic::Ordering::Relaxed);
                    Json(json!({ "paused": want })).into_response()
                }
            })
            .get(move || {
                let st = st_get.clone();
                async move {
                    Json(json!({ "paused": st.load(std::sync::atomic::Ordering::Relaxed) })).into_response()
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr.to_string(), seen, paused)
    }

    #[tokio::test]
    async fn replication_control_forwards_to_store_and_surfaces_errors() {
        let (addr, seen, paused) = spawn_replication_store().await;
        let router = app_with(Arc::new(FakeMaster::default().with_node(101, &addr)), AuthConfig::default());

        // GET:store 当前开关原样透出(初始未暂停)。
        let req = Request::get("/api/stores/101/replication-control").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["paused"], false);

        // POST:body 原样转发到 store,响应即操作后的状态。
        let req = Request::post("/api/stores/101/replication-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":true}"#))
            .unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["paused"], true);
        let sent: serde_json::Value = serde_json::from_str(&seen.lock().unwrap()[0]).unwrap();
        assert_eq!(sent, json!({ "paused": true }));
        assert!(paused.load(std::sync::atomic::Ordering::Relaxed), "假 store 内部状态应已暂停");

        // 再 GET:反映暂停;恢复 → false。
        let req = Request::get("/api/stores/101/replication-control").body(Body::empty()).unwrap();
        assert_eq!(body_json(&router, req).await["paused"], true);
        let req = Request::post("/api/stores/101/replication-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":false}"#))
            .unwrap();
        assert_eq!(body_json(&router, req).await["paused"], false);

        // master 无此节点 → 404(不发起转发)。
        let req = Request::get("/api/stores/999/replication-control").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);

        // 节点存在但连不上 → 502 + 原因。
        let dead_addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let router =
            app_with(Arc::new(FakeMaster::default().with_node(102, &dead_addr)), AuthConfig::default());
        let req = Request::post("/api/stores/102/replication-control")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":true}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("network error"), "连接失败原因应透出");
    }

    #[tokio::test]
    async fn api_metrics_blob_counts_picked_by_label() {
        // 同名 6 组 label 组合必须按标签精确取(按名求和会混成一锅)。
        let addr = spawn_fake_store(
            "store_server_blob_request_duration_seconds_count{op=\"put\",size=\"small\"} 3\n\
             store_server_blob_request_duration_seconds_count{op=\"put\",size=\"medium\"} 4\n\
             store_server_blob_request_duration_seconds_count{op=\"put\",size=\"large\"} 5\n\
             store_server_blob_request_duration_seconds_count{op=\"get\",size=\"small\"} 6\n\
             store_server_blob_request_duration_seconds_count{op=\"get\",size=\"medium\"} 7\n\
             store_server_blob_request_duration_seconds_count{op=\"get\",size=\"large\"} 8\n\
             store_server_blob_request_duration_seconds_sum{op=\"put\",size=\"small\"} 1.5\n",
            false,
        )
        .await;
        let router = app_with(Arc::new(FakeMaster::default().with_node(101, &addr)), AuthConfig::default());

        let req = Request::get("/api/metrics").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        let s = &v["stores"][0];
        assert_eq!(s["blob_put_small_count"], 3);
        assert_eq!(s["blob_put_medium_count"], 4);
        assert_eq!(s["blob_put_large_count"], 5);
        assert_eq!(s["blob_get_small_count"], 6);
        assert_eq!(s["blob_get_medium_count"], 7);
        assert_eq!(s["blob_get_large_count"], 8);
    }

    #[tokio::test]
    async fn api_metrics_aggregates_store_errors_by_name_and_manual_compactions() {
        // 假 store:errors_total 带 kind label(验证按名求和仍取到)+ manual_compactions_total。
        let addr = spawn_fake_store(
            "# HELP store_server_errors_total data-plane errors\n\
             store_server_errors_total{kind=\"io\"} 2\n\
             store_server_errors_total{kind=\"unavailable\"} 3\n\
             store_server_manual_compactions_total 7\n",
            true,
        )
        .await;
        let master = Arc::new(FakeMaster::default().with_node(101, &addr));
        let router = app_with(master, AuthConfig::default());

        let req = Request::get("/api/metrics").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        let s = &v["stores"][0];
        assert_eq!(s["node_id"], 101);
        assert_eq!(s["scrape_ok"], true);
        assert_eq!(s["errors_total"], 5, "带 kind label 的 counter 按名求和: {v}");
        assert_eq!(s["manual_compactions_total"], 7);
    }

    #[tokio::test]
    async fn partition_detail_200_and_404() {
        let router = app("");
        let req = Request::get("/api/partitions/3").body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["partition_id"], 3);
        assert_eq!(v["moving_out_nodes"].as_array().unwrap().len(), 0);

        let req = Request::get("/api/partitions/999").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn repair_and_move_route_args_to_master() {
        let master = Arc::new(FakeMaster::default());
        let router = app_with(master.clone(), AuthConfig::default());

        let req = Request::post("/api/partitions/7/repair").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["ok"], true);
        assert_eq!(v["added_node_id"], 104);
        assert_eq!(v["added_disk_id"], "d1");

        let req = Request::post("/api/partitions/7/move")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"from_node_id":101,"to_node_id":102}"#))
            .unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["ok"], true);
        assert_eq!(v["to_node_id"], 102);
        assert_eq!(v["to_disk_id"], "d2");

        // to_node_id 缺省 → 0(自动选目标)。
        let req = Request::post("/api/partitions/8/move")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"from_node_id":101}"#))
            .unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["ok"], true);
        assert_eq!(v["to_node_id"], 0);

        assert_eq!(master.take_calls(), vec!["repair 7", "move 7 101 102", "move 8 101 0"]);
    }

    #[tokio::test]
    async fn compact_forwards_to_all_replica_nodes_and_aggregates() {
        let ok_addr = spawn_fake_store("", true).await;
        let miss_addr = spawn_fake_store("", false).await;
        let master = Arc::new(
            FakeMaster::default().with_replica(101, &ok_addr).with_replica(102, &miss_addr),
        );
        let router = app_with(master, AuthConfig::default());

        let req = Request::post("/api/partitions/3/compact").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["error"], "");
        let rs = v["results"].as_array().unwrap();
        assert_eq!(rs.len(), 2);
        // 结果按 node_id 排序(并发完成顺序不定,契约稳定)。
        assert_eq!(
            rs[0],
            json!({
                "node_id": 101,
                "ok": true,
                "live_bytes": 1024,
                "dead_bytes": 2048,
                "segments_before": 3,
                "segments_after": 1,
            })
        );
        assert_eq!(rs[1]["node_id"], 102);
        assert_eq!(rs[1]["ok"], false);
        assert_eq!(rs[1]["error"], "404", "无本地副本 → 404 文本: {v}");
    }

    #[tokio::test]
    async fn compact_reports_unreachable_node_missing_partition_and_no_replicas() {
        // 已关闭的端口 → 连接失败;单节点失败不中断整体(master 不可达之外一律 200)。
        let dead_addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string() // listener 随即 drop,该端口无人监听
        };
        let master = Arc::new(FakeMaster::default().with_replica(103, &dead_addr));
        let router = app_with(master, AuthConfig::default());
        let req = Request::post("/api/partitions/3/compact").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["results"][0]["node_id"], 103);
        assert_eq!(v["results"][0]["ok"], false);
        assert!(
            v["results"][0]["error"].as_str().unwrap().contains("network error"),
            "连接失败应带原因: {v}"
        );

        // 分区不存在 → 404(与 GET /api/partitions/{id} 同一判定)。
        let router = app("");
        let req = Request::post("/api/partitions/999/compact").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);

        // 分区无副本 → 200 + 空结果 + 顶层 error。
        let router = app_with(Arc::new(FakeMaster::default()), AuthConfig::default());
        let req = Request::post("/api/partitions/3/compact").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["results"].as_array().unwrap().len(), 0);
        assert!(v["error"].as_str().unwrap().contains("no replicas"), "{v}");
    }

    /// 假 store 的 A3 端点:`/admin/partitions/{pid}/compact` 带 `async` query → 立即回 `job_id`
    /// (不带则回同步统计,与 store 侧同分流);`/admin/compactions` 回台账(含**别的分区**的任务)。
    async fn spawn_async_compact_store() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/admin/partitions/{pid}/compact", post(fake_compact_async))
            .route("/admin/compactions", get(fake_compactions));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr.to_string()
    }

    /// 带 `async` query → `{job_id}`;不带 → 同步统计(端点同一路径两种形态)。
    async fn fake_compact_async(
        axum::extract::Query(q): axum::extract::Query<std::collections::BTreeMap<String, String>>,
    ) -> Json<serde_json::Value> {
        if q.contains_key("async") {
            Json(json!({ "job_id": 42 }))
        } else {
            Json(json!({
                "live_bytes": 1024,
                "dead_bytes": 2048,
                "segments_before": 3,
                "segments_after": 1,
            }))
        }
    }

    /// 假台账:pid=3 的 done(带 result)+ running,pid=9 的任务(聚合时必须被过滤掉)。
    async fn fake_compactions() -> Json<serde_json::Value> {
        Json(json!({ "jobs": [
            {
                "job_id": 42,
                "partition_id": 3,
                "state": "done",
                "started_at_ms": 1000,
                "finished_at_ms": 2000,
                "result": { "live_bytes": 10, "dead_bytes": 20, "segments_before": 3, "segments_after": 1 },
            },
            { "job_id": 41, "partition_id": 9, "state": "running", "started_at_ms": 1000 },
            { "job_id": 40, "partition_id": 3, "state": "running", "started_at_ms": 900 },
            {
                "job_id": 39,
                "partition_id": 3,
                "state": "failed",
                "started_at_ms": 800,
                "finished_at_ms": 850,
                "error": "io: disk on fire",
            },
        ]}))
    }

    #[tokio::test]
    async fn compact_async_mode_submits_and_rejects_bad_mode() {
        let ok_addr = spawn_async_compact_store().await;
        let dead_addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let master = Arc::new(
            FakeMaster::default().with_replica(101, &ok_addr).with_replica(102, &dead_addr),
        );
        let router = app_with(master, AuthConfig::default());

        // mode=async:每副本回 job_id(单个节点连不上不中断其余)。
        let req = Request::post("/api/partitions/3/compact?mode=async").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        assert_eq!(v["error"], "");
        let rs = v["results"].as_array().unwrap();
        assert_eq!(rs[0], json!({ "node_id": 101, "ok": true, "job_id": 42 }));
        assert_eq!(rs[1]["node_id"], 102);
        assert_eq!(rs[1]["ok"], false);
        assert!(rs[1]["error"].as_str().unwrap().contains("network error"), "{v}");

        // 同步路径不变(mode 缺省 / mode=sync 都是同步统计)。
        for url in ["/api/partitions/3/compact", "/api/partitions/3/compact?mode=sync"] {
            let req = Request::post(url).body(Body::empty()).unwrap();
            let v = body_json(&router, req).await;
            assert_eq!(v["results"][0]["live_bytes"], 1024, "{url}: {v}");
        }

        // 非法 mode 在 admin 边界即拒(与 state 过滤同一策略),不打扰 store。
        let req = Request::post("/api/partitions/3/compact?mode=bogus").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::BAD_REQUEST);

        // 分区不存在 → 404。
        let req = Request::post("/api/partitions/999/compact?mode=async").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn compact_jobs_are_filtered_per_partition_and_aggregated_per_node() {
        let ok_addr = spawn_async_compact_store().await;
        let dead_addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let master = Arc::new(
            FakeMaster::default().with_replica(101, &ok_addr).with_replica(102, &dead_addr),
        );
        let router = app_with(master, AuthConfig::default());

        let req = Request::get("/api/partitions/3/compact-jobs").body(Body::empty()).unwrap();
        let v = body_json(&router, req).await;
        let rs = v["results"].as_array().unwrap();
        assert_eq!(rs.len(), 2, "每副本一行: {v}");

        // 只有该分区的任务,最新在前;done 带 result/finished_at_ms,running 不带。
        let jobs = rs[0]["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 3, "pid=9 的任务必须被过滤掉: {v}");
        assert_eq!(jobs[0]["job_id"], 42);
        assert_eq!(jobs[0]["state"], "done");
        assert_eq!(jobs[0]["result"]["dead_bytes"], 20);
        assert!(jobs[0]["finished_at_ms"].is_u64());
        assert_eq!(jobs[1]["job_id"], 40);
        assert_eq!(jobs[1]["state"], "running");
        assert!(jobs[1].get("result").is_none(), "running 不该有 result: {v}");
        assert_eq!(jobs[2]["state"], "failed");
        assert_eq!(jobs[2]["error"], "io: disk on fire");

        // 连不上的节点 → ok:false + 原因(不中断整体)。
        assert_eq!(rs[1]["node_id"], 102);
        assert_eq!(rs[1]["ok"], false);
        assert!(rs[1]["error"].as_str().unwrap().contains("network error"), "{v}");

        // 分区不存在 → 404;无副本 → 空 results。
        let req = Request::get("/api/partitions/999/compact-jobs").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);
        let router = app_with(Arc::new(FakeMaster::default()), AuthConfig::default());
        let req = Request::get("/api/partitions/3/compact-jobs").body(Body::empty()).unwrap();
        assert_eq!(body_json(&router, req).await["results"].as_array().unwrap().len(), 0);
    }

    /// 多用户令牌表(与旧单令牌模式二选一/共存)。
    fn users_auth() -> AuthConfig {
        AuthConfig {
            users: vec![
                AdminUser { name: "alice".into(), token: "tok-alice".into(), role: "operator".into() },
                AdminUser { name: "bob".into(), token: "tok-bob".into(), role: "viewer".into() },
            ],
            ..Default::default()
        }
    }

    fn with_bearer(mut req: Request<Body>, token: &str) -> Request<Body> {
        req.headers_mut().insert(header::AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        req
    }

    #[tokio::test]
    async fn users_config_maps_tokens_to_roles() {
        let router = app_auth(users_auth());
        for (token, role) in [("tok-alice", "operator"), ("tok-bob", "viewer")] {
            let req = with_bearer(Request::get("/api/me").body(Body::empty()).unwrap(), token);
            assert_eq!(body_json(&router, req).await["role"], role, "token {token}");
        }

        // operator 可写;viewer 写 → 403;未知令牌 → 401。
        let post = |token: &str| {
            with_bearer(
                Request::post("/api/maintenance")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"paused":true}"#))
                    .unwrap(),
                token,
            )
        };
        assert_eq!(status(&router, post("tok-alice")).await, StatusCode::OK);
        assert_eq!(status(&router, post("tok-bob")).await, StatusCode::FORBIDDEN);
        assert_eq!(status(&router, post("nope")).await, StatusCode::UNAUTHORIZED);

        // 审计日志只对 operator 开放(viewer 即使 GET 也 403)。
        let audit = |token: &str| with_bearer(Request::get("/api/audit").body(Body::empty()).unwrap(), token);
        assert_eq!(status(&router, audit("tok-alice")).await, StatusCode::OK);
        assert_eq!(status(&router, audit("tok-bob")).await, StatusCode::FORBIDDEN);

        // users 与旧单令牌可共存:旧令牌仍按原语义解析(actor 名 = operator)。
        let router = app_auth(AuthConfig {
            operator_token: "legacy-op".into(),
            ..users_auth()
        });
        let req = with_bearer(Request::get("/api/me").body(Body::empty()).unwrap(), "legacy-op");
        assert_eq!(body_json(&router, req).await["role"], "operator");
    }

    #[tokio::test]
    async fn audit_records_writes_and_me_but_skips_reads() {
        let path = audit_file("writes");
        let _ = std::fs::remove_file(&path);
        let router = app_audited(
            Arc::new(FakeMaster::default()),
            users_auth(),
            Arc::new(AuditLog::open(&path)),
        );

        // 读操作不落档。
        let req = with_bearer(Request::get("/api/cluster").body(Body::empty()).unwrap(), "tok-alice");
        assert_eq!(status(&router, req).await, StatusCode::OK);
        assert!(audit_lines(&path).is_empty(), "读操作不该落审计");

        // 写操作落档(query 保留;remote_addr 来自 ConnectInfo)。
        let mut req = Request::post("/api/maintenance?force=true")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":false}"#))
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 4040))));
        let req = with_bearer(req, "tok-alice");
        assert_eq!(status(&router, req).await, StatusCode::OK);

        // /api/me:成功失败都记。
        let req = with_bearer(Request::get("/api/me").body(Body::empty()).unwrap(), "tok-bob");
        assert_eq!(status(&router, req).await, StatusCode::OK);
        let req = with_bearer(Request::get("/api/me").body(Body::empty()).unwrap(), "bad");
        assert_eq!(status(&router, req).await, StatusCode::UNAUTHORIZED);
        // 被拒的写(viewer)同样落档(status=403)。
        let req = with_bearer(
            Request::post("/api/maintenance")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"paused":true}"#))
                .unwrap(),
            "tok-bob",
        );
        assert_eq!(status(&router, req).await, StatusCode::FORBIDDEN);

        let lines = audit_lines(&path);
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert_eq!(lines[0]["method"], "POST");
        assert_eq!(lines[0]["path"], "/api/maintenance");
        assert_eq!(lines[0]["summary"], "POST /api/maintenance?force=true");
        assert_eq!(lines[0]["status"], 200);
        assert_eq!(lines[0]["actor"], "alice", "多用户 actor 名: {lines:?}");
        assert_eq!(lines[0]["role"], "operator");
        assert_eq!(lines[0]["remote_addr"], "127.0.0.1:4040");
        assert!(lines[0]["ts_ms"].as_u64().unwrap() > 0);
        assert_eq!(lines[1]["summary"], "GET /api/me");
        assert_eq!(lines[1]["actor"], "bob");
        assert_eq!(lines[1]["role"], "viewer");
        assert_eq!(lines[2]["status"], 401, "鉴权失败也记: {lines:?}");
        assert_eq!(lines[2]["actor"], "anonymous");
        assert_eq!(lines[2]["role"], "none");
        assert_eq!(lines[3]["status"], 403, "被拒的写也记: {lines:?}");
        assert_eq!(lines[3]["actor"], "bob");

        // GET /api/audit 本身是读:不落档;limit 生效且最新在前。
        let req = with_bearer(Request::get("/api/audit?limit=2").body(Body::empty()).unwrap(), "tok-alice");
        let v = body_json(&router, req).await;
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2, "limit 生效: {v}");
        assert_eq!(entries[0]["status"], 403, "最新在前: {v}");
        assert_eq!(entries[1]["status"], 401);
        assert_eq!(audit_lines(&path).len(), 4, "读 /api/audit 不落档");
        // limit 超上限只截断,不报错;limit=0 回空表。
        let req = with_bearer(Request::get("/api/audit?limit=99999").body(Body::empty()).unwrap(), "tok-alice");
        assert_eq!(body_json(&router, req).await["entries"].as_array().unwrap().len(), 4);
        let req = with_bearer(Request::get("/api/audit?limit=0").body(Body::empty()).unwrap(), "tok-alice");
        assert_eq!(body_json(&router, req).await["entries"].as_array().unwrap().len(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn audit_records_local_actor_when_auth_disabled() {
        let path = audit_file("local");
        let _ = std::fs::remove_file(&path);
        let router = app_audited(
            Arc::new(FakeMaster::default()),
            AuthConfig::default(),
            Arc::new(AuditLog::open(&path)),
        );
        // 关校验(本地):写操作放行,审计 actor = local。
        let req = Request::post("/api/maintenance")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":false}"#))
            .unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);
        let lines = audit_lines(&path);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0]["actor"], "local");
        assert_eq!(lines[0]["role"], "operator");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn audit_degraded_keeps_requests_working() {
        // 审计路径不可用(父目录不存在)→ 降级:写操作照常 200,查询回空表。
        let bad = std::env::temp_dir().join("antfs-admin-audit-none/t.jsonl");
        let router = app_audited(
            Arc::new(FakeMaster::default()),
            AuthConfig::default(),
            Arc::new(AuditLog::open(&bad)),
        );
        let req = Request::post("/api/maintenance")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"paused":true}"#))
            .unwrap();
        assert_eq!(status(&router, req).await, StatusCode::OK);
        let req = Request::get("/api/audit").body(Body::empty()).unwrap();
        assert_eq!(body_json(&router, req).await["entries"].as_array().unwrap().len(), 0);
        assert!(!bad.exists(), "降级实例不创建文件");
    }

    #[tokio::test]
    async fn balance_state_returns_plan_shape() {
        let router = app("");
        let req = Request::get("/api/balance").body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["present"], true);
        assert_eq!(v["plan"]["plan_id"], 7);
        assert_eq!(v["plan"]["status"], "PREVIEW");
        assert_eq!(v["plan"]["moves"][0]["partition_id"], 3);
        assert_eq!(v["plan"]["moves"][0]["to_node_id"], 104);
    }

    #[tokio::test]
    async fn balance_eval_forwards_max_moves_and_returns_plan() {
        let router = app("");
        let req = Request::post("/api/balance/eval")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"max_moves":42}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["plan"]["plan_id"], 7);
    }

    #[tokio::test]
    async fn balance_start_rejects_mismatched_plan_id() {
        let router = app("");
        let req = Request::post("/api/balance/start")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"plan_id":99}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "master ok=false 仍回 200,错误交前端");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["ok"], false);
        assert!(v["error"].as_str().unwrap().contains("plan 99 not found"));
    }

    #[tokio::test]
    async fn balance_actions_are_operator_only() {
        let router = app_auth(AuthConfig {
            users: Vec::new(),
            operator_token: "op".into(),
            viewer_token: "view".into(),
        });
        let read = Request::get("/api/balance")
            .header(header::AUTHORIZATION, "Bearer view")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(&router, read).await, StatusCode::OK, "viewer 可读");
        let write = Request::post("/api/balance/eval")
            .header(header::AUTHORIZATION, "Bearer view")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(status(&router, write).await, StatusCode::FORBIDDEN, "viewer 不能评估");
    }


    /// 假 store 的 scrub 三类素材:`/partitions/{pid}/scrub-report`、`/replica-metadata`、`/verify`。
    ///
    /// `keys` = `(key_hex, deleted, life_version, size)`;`bad_records` 用于 deep 校验场景。
    async fn spawn_scrub_store(
        live_records: u64,
        keys: Vec<(&'static str, bool, u16, u64)>,
        bad_records: u64,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let report = move || async move {
            Json(json!({
                "partition_id": 3,
                "live_records": live_records,
                "tombstones": 0,
                "live_bytes": live_records * 10,
                "sealed_segments": 1,
                "segments": [{"position": 0, "data_end": 100}],
                "token_seg": 0,
                "token_off": 100,
            }))
        };
        let meta = move || async move {
            let entries: Vec<serde_json::Value> = keys
                .iter()
                .map(|(k, deleted, lv, size)| {
                    json!({
                        "key": k,
                        "size": size,
                        "expiration_ms": -1,
                        "deleted": deleted,
                        "ttl_updated": false,
                        "undeleted": false,
                        "life_version": lv,
                        "operation_time_ms": 1,
                    })
                })
                .collect();
            Json(json!({
                "entries": entries,
                "token_seg": 0,
                "token_off": 100,
                "bytes_read": 128,
            }))
        };
        let verify = move || async move {
            Json(json!({
                "partition_id": 3,
                "records": 6,
                "bad_records": bad_records,
                "first_bad_offset": null,
                "index_mismatch": 0,
                "truncated": false,
                "duration_ms": 1,
            }))
        };
        let app = Router::new()
            .route("/partitions/{pid}/scrub-report", get(report))
            .route("/partitions/{pid}/replica-metadata", get(meta))
            .route("/partitions/{pid}/verify", get(verify));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn scrub_reports_consistent_replicas() {
        let a = spawn_scrub_store(2, vec![("aa", false, 0, 4)], 0).await;
        let b = spawn_scrub_store(2, vec![("aa", false, 0, 4)], 0).await;
        let master = Arc::new(FakeMaster::default().with_replica(101, &a).with_replica(102, &b));
        let router = app_with(master, AuthConfig::default());

        let v = body_json(
            &router,
            Request::post("/api/partitions/3/scrub").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(v["error"], "");
        assert_eq!(v["verdict"]["consistent"], true, "{v}");
        assert_eq!(v["verdict"]["divergences"].as_array().unwrap().len(), 0);
        assert_eq!(v["replicas"].as_array().unwrap().len(), 2);
        assert_eq!(v["replicas"][0]["node_id"], 101);
        assert_eq!(v["replicas"][0]["report"]["live_records"], 2);
        assert_eq!(v["replicas"][0]["key_count"], 1);
        assert_eq!(v["replicas"][0]["verify"], serde_json::Value::Null, "非 deep 不取 verify");
    }

    #[tokio::test]
    async fn scrub_detects_missing_key_and_summary_mismatch() {
        let a = spawn_scrub_store(2, vec![("aa", false, 0, 4), ("bb", false, 0, 4)], 0).await;
        let b = spawn_scrub_store(1, vec![("aa", false, 0, 4)], 0).await; // 少一个 key + 摘要不同
        let master = Arc::new(FakeMaster::default().with_replica(101, &a).with_replica(102, &b));
        let router = app_with(master, AuthConfig::default());

        let v = body_json(
            &router,
            Request::post("/api/partitions/3/scrub").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(v["verdict"]["consistent"], false, "{v}");
        let divs = v["verdict"]["divergences"].as_array().unwrap();
        assert!(divs.iter().any(|d| d["kind"] == "key" && d["key_hex"] == "bb"), "{v}");
        assert!(divs.iter().any(|d| d["kind"] == "summary"), "{v}");

        // 也进了最近列表(环形表在 AppState 内,必须用**同一个** router 读)
        let runs = body_json(&router, Request::get("/api/scrub").body(Body::empty()).unwrap()).await;
        assert_eq!(runs["runs"].as_array().unwrap().len(), 1, "{runs}");
        assert_eq!(runs["runs"][0]["partition_id"], 3);
        assert_eq!(runs["runs"][0]["consistent"], false);
    }

    #[tokio::test]
    async fn scrub_deep_flags_bad_records_and_inconclusive_on_dead_replica() {
        let a = spawn_scrub_store(2, vec![("aa", false, 0, 4)], 1).await; // 有坏记录
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let master = Arc::new(FakeMaster::default().with_replica(101, &a).with_replica(102, &dead));
        let router = app_with(master, AuthConfig::default());

        let v = body_json(
            &router,
            Request::post("/api/partitions/3/scrub?deep=1").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(v["deep"], true);
        assert_eq!(v["verdict"]["inconclusive"], true, "有副本取不到素材 → 无法判定: {v}");
        assert_eq!(v["replicas"][1]["ok"], false, "{v}");
        assert!(
            v["replicas"][0]["verify"]["bad_records"].as_u64().unwrap() >= 1,
            "deep 应带回本地校验: {v}"
        );
    }

    #[tokio::test]
    async fn scrub_is_operator_only_and_404s_unknown_partition() {
        let router = app_auth(AuthConfig {
            users: Vec::new(),
            operator_token: "op".into(),
            viewer_token: "view".into(),
        });
        let write = Request::post("/api/partitions/3/scrub")
            .header(header::AUTHORIZATION, "Bearer view")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(&router, write).await, StatusCode::FORBIDDEN, "viewer 不能触发 scrub");

        let router = app("");
        let req = Request::post("/api/partitions/999/scrub").body(Body::empty()).unwrap();
        assert_eq!(status(&router, req).await, StatusCode::NOT_FOUND);
    }

}
