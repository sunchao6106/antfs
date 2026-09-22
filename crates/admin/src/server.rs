//! admin HTTP 服务:JSON API(经 [`MasterApi`])+ operator 鉴权 + 内嵌 SPA(D-A/D-C)。

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::error::Error;
use crate::master_api::MasterApi;
use crate::metrics::AdminMetrics;

/// 内嵌的单页应用(vite `singlefile` 构建产物,自包含 HTML+JS+CSS)。
const SPA_INDEX: &str = include_str!("../ui/dist/index.html");

#[derive(Clone)]
pub struct AppState {
    pub master: Arc<dyn MasterApi>,
    pub metrics: Arc<AdminMetrics>,
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

/// 建路由:`/api/*` 经 RBAC(GET=viewer|operator,写=operator);其余路径回退 SPA。
pub fn build_router(state: AppState, auth: AuthConfig) -> Router {
    let api = Router::new()
        .route("/api/me", get(get_me))
        .route("/api/cluster", get(get_cluster))
        .route("/api/nodes", get(get_nodes))
        .route("/api/partitions", get(get_partitions).post(post_partitions))
        .route("/api/partitions/{id}/state", post(post_partition_state))
        .route("/api/nodes/{id}/decommission", post(post_decommission))
        .layer(axum::Extension(auth.clone()))
        .layer(from_fn_with_state(auth, require_auth))
        .with_state(state);

    Router::new().merge(api).fallback(spa_index)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_api::{ClusterDto, NodeDto, PartitionDto};
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    struct FakeMaster;
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
    }

    fn app_auth(auth: AuthConfig) -> Router {
        let registry = metric::Registry::new();
        let state = AppState {
            master: Arc::new(FakeMaster),
            metrics: Arc::new(AdminMetrics::new(&registry).unwrap()),
            grafana_url: "http://grafana.local".into(),
        };
        build_router(state, auth)
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
}
