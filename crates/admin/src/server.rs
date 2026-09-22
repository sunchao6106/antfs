//! admin HTTP 服务:JSON API(经 [`MasterApi`])+ operator 鉴权 + 内嵌 SPA(D-A/D-C)。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::error::Error;
use crate::master_api::{MasterApi, NodeDto};
use crate::metrics::AdminMetrics;

/// 内嵌的单页应用(vite `singlefile` 构建产物,自包含 HTML+JS+CSS)。
const SPA_INDEX: &str = include_str!("../ui/dist/index.html");

/// 抓取单个 store `/metrics` 端点的超时(挂死的节点不能拖慢整个 `/api/metrics`)。
const STORE_SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct AppState {
    pub master: Arc<dyn MasterApi>,
    pub metrics: Arc<AdminMetrics>,
    /// admin 自身指标注册表(`/metrics` 与 `/api/metrics` 的 admin 段用;与 `metrics` 接入同一个)。
    pub registry: antfs_metric::Registry,
    pub grafana_url: String,
}

/// operator(读+写)与 viewer(只读)双令牌 RBAC(D-C 升级)。两者皆空 = 关校验(仅本地)。
#[derive(Clone, Debug, Default)]
pub struct AuthConfig {
    pub operator_token: String,
    pub viewer_token: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Operator,
    Viewer,
    Anonymous,
}

impl AuthConfig {
    fn disabled(&self) -> bool {
        self.operator_token.is_empty() && self.viewer_token.is_empty()
    }

    /// 据 Authorization 头判角色;关校验时视作 operator(本地全权)。
    fn role_of(&self, header: Option<&str>) -> Role {
        if self.disabled() {
            return Role::Operator;
        }
        let tok = header.and_then(|h| h.strip_prefix("Bearer "));
        match tok {
            Some(t) if !self.operator_token.is_empty() && t == self.operator_token => Role::Operator,
            Some(t) if !self.viewer_token.is_empty() && t == self.viewer_token => Role::Viewer,
            _ => Role::Anonymous,
        }
    }

    fn role_name(&self, header: Option<&str>) -> &'static str {
        match self.role_of(header) {
            Role::Operator => "operator",
            Role::Viewer => "viewer",
            Role::Anonymous => "none",
        }
    }
}

/// 建路由:`/api/*` 经 RBAC(GET=viewer|operator,写=operator);`/metrics` 无鉴权(Prometheus 抓取惯例);其余回退 SPA。
pub fn build_router(state: AppState, auth: AuthConfig) -> Router {
    let api = Router::new()
        .route("/api/me", get(get_me))
        .route("/api/cluster", get(get_cluster))
        .route("/api/nodes", get(get_nodes))
        .route("/api/metrics", get(get_api_metrics))
        .route("/api/partitions", get(get_partitions).post(post_partitions))
        .route("/api/partitions/{id}", get(get_partition))
        .route("/api/partitions/{id}/state", post(post_partition_state))
        .route("/api/partitions/{id}/repair", post(post_repair))
        .route("/api/partitions/{id}/move", post(post_move))
        .route("/api/nodes/{id}/decommission", post(post_decommission))
        .layer(axum::Extension(auth.clone()))
        .layer(from_fn_with_state(auth, require_auth))
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

/// RBAC 中间件:关校验放行;否则按角色 + 方法门控(viewer 只读,写→403,未知→401)。
async fn require_auth(
    State(auth): State<AuthConfig>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    if auth.disabled() {
        return next.run(req).await;
    }
    let header = req.headers().get(header::AUTHORIZATION).and_then(|h| h.to_str().ok());
    let role = auth.role_of(header);
    let is_write = req.method() != axum::http::Method::GET;
    match role {
        Role::Anonymous => {
            (StatusCode::UNAUTHORIZED, "missing or invalid token").into_response()
        }
        Role::Viewer if is_write => {
            (StatusCode::FORBIDDEN, "viewer role is read-only").into_response()
        }
        _ => next.run(req).await,
    }
}

async fn get_me(
    axum::Extension(auth): axum::Extension<AuthConfig>,
    headers: axum::http::HeaderMap,
) -> Response {
    let h = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok());
    Json(json!({ "role": auth.role_name(h) })).into_response()
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
}

async fn get_partitions(
    State(st): State<AppState>,
    axum::extract::Query(page): axum::extract::Query<PageParams>,
) -> Response {
    st.metrics.api_requests.inc();
    match st.master.partitions(page.offset, page.limit).await {
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
/// 不可达不 5xx:对应段填 0 / scrape_ok=false,前端仍能渲染。
async fn get_api_metrics(State(st): State<AppState>) -> Response {
    st.metrics.api_requests.inc();

    let master = match st.master.master_metrics_text().await {
        Ok(text) => {
            let s = antfs_metric::parse(&text);
            json!({
                "is_leader": pick(&s, "master_is_leader"),
                "raft_term": pick(&s, "master_raft_term"),
                "heartbeats_total": pick(&s, "master_heartbeats_total"),
                "applied_commands_total": pick(&s, "master_applied_commands_total"),
                "transitions_issued_total": pick(&s, "master_transitions_issued_total"),
                "rereplications_total": pick(&s, "master_rereplications_total"),
                "replicas_removed_total": pick(&s, "master_replicas_removed_total"),
                "under_replicated_partitions": pick(&s, "master_under_replicated_partitions"),
                "registered_nodes": pick(&s, "master_registered_nodes"),
            })
        }
        Err(e) => {
            st.metrics.master_rpc_errors.inc();
            tracing::warn!(error = %e, "master metrics unavailable");
            master_json_zero()
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
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http();
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

    Json(json!({ "ts_ms": now_ms(), "master": master, "admin": admin, "stores": stores }))
        .into_response()
}

fn master_json_zero() -> serde_json::Value {
    json!({
        "is_leader": 0,
        "raft_term": 0,
        "heartbeats_total": 0,
        "applied_commands_total": 0,
        "transitions_issued_total": 0,
        "rereplications_total": 0,
        "replicas_removed_total": 0,
        "under_replicated_partitions": 0,
        "registered_nodes": 0,
    })
}

type HttpClient =
    hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, http_body_util::Empty<axum::body::Bytes>>;

/// 抓一个 store 节点的 `/metrics` 并聚合成契约字段;失败/超时 → scrape_ok=false + 计 `store_scrape_errors`。
/// `node.up=false` 也照抓(master 的心跳超时判定与 HTTP 活性可能不一致)。
async fn scrape_store(client: HttpClient, metrics: Arc<AdminMetrics>, node: NodeDto) -> serde_json::Value {
    let fetch = async {
        let url = format!("http://{}/metrics", node.addr);
        let req = axum::http::Request::get(&url)
            .body(http_body_util::Empty::<axum::body::Bytes>::new())
            .ok()?;
        let resp = client.request(req).await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let bytes = http_body_util::BodyExt::collect(resp.into_body()).await.ok()?.to_bytes();
        String::from_utf8(bytes.to_vec()).ok()
    };
    match tokio::time::timeout(STORE_SCRAPE_TIMEOUT, fetch).await {
        Ok(Some(text)) => {
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
            })
        }
        _ => {
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
            })
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_api::{ClusterDto, MoveOutcome, NodeDto, PartitionDto, RepairOutcome};
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeMaster {
        /// 记录运维调用("repair {pid}" / "move {pid} {from} {to}"),供测试断言参数透传。
        calls: Mutex<Vec<String>>,
    }

    impl FakeMaster {
        fn take_calls(&self) -> Vec<String> {
            std::mem::take(&mut self.calls.lock().unwrap_or_else(|p| p.into_inner()))
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
            })
        }
        async fn nodes(&self) -> crate::error::Result<Vec<NodeDto>> {
            Ok(vec![])
        }
        async fn partitions(&self, offset: u64, limit: u32) -> crate::error::Result<(Vec<PartitionDto>, u64)> {
            let all: Vec<PartitionDto> = (0..5u32)
                .map(|i| PartitionDto {
                    partition_id: i,
                    state: "RW".into(),
                    replication_factor: 3,
                    replicas: vec![],
                    healthy_replicas: 3,
                    healable: true,
                    under_replicated_reason: String::new(),
                    moving_out_nodes: vec![],
                })
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
        async fn master_metrics_text(&self) -> crate::error::Result<String> {
            Ok("master_is_leader 1\nmaster_raft_term 3\nmaster_heartbeats_total 7\n".to_string())
        }
        async fn partition_detail(&self, partition_id: u32) -> crate::error::Result<PartitionDto> {
            let (items, _) = self.partitions(0, 0).await?;
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
    }

    fn app_with(master: Arc<FakeMaster>, auth: AuthConfig) -> Router {
        let registry = antfs_metric::Registry::new();
        let state = AppState {
            master,
            metrics: Arc::new(AdminMetrics::new(&registry).unwrap()),
            registry,
            grafana_url: "http://grafana.local".into(),
        };
        build_router(state, auth)
    }

    fn app_auth(auth: AuthConfig) -> Router {
        app_with(Arc::new(FakeMaster::default()), auth)
    }

    fn app(token: &str) -> Router {
        app_auth(AuthConfig { operator_token: token.to_string(), viewer_token: String::new() })
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
        let router = app_auth(AuthConfig { operator_token: "op".into(), viewer_token: "view".into() });
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
}
