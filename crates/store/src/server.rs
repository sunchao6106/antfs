//! 数据节点装配 + HTTP 数据面(`axum`,D2)。
//!
//! store 引擎是**同步阻塞**(路线乙),故 async handler 里的 put/get/ensure 一律经
//! [`tokio::task::spawn_blocking`] offload,不阻塞 reactor(衔接 N4;专用每盘线程池留 S4/性能期)。

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use blob::StoreKey;

use crate::config::StoreServerConfig;
use crate::disk::DiskManager;
use crate::error::Result;
use crate::metrics::StoreServerMetrics;
use crate::replica::ReplicaManager;
use crate::replication::{token_from, MetadataResponse};
use crate::PartitionId;

#[derive(Clone)]
struct AppState {
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
}

/// 装配好的数据节点。
pub struct StoreServer {
    config: StoreServerConfig,
    state: AppState,
    disks: DiskManager,
    health: crate::health::HealthMap,
}

impl StoreServer {
    /// 挂盘、建 store 引擎依赖、装配 replica 管理器(用默认 [`blob::StoreConfig`])。
    pub fn start(config: StoreServerConfig, registry: &metric::Registry) -> Result<Self> {
        Self::start_with(config, blob::StoreConfig::default(), registry)
    }

    /// 同 [`start`](Self::start),但显式传入 store 引擎配置(测试/小部署可用小段大小)。
    pub fn start_with(
        config: StoreServerConfig,
        store_config: blob::StoreConfig,
        registry: &metric::Registry,
    ) -> Result<Self> {
        config::Validate::validate(&config)?;
        let disks = DiskManager::mount(&config.data_dirs)?;
        let store_metrics = blob::StoreMetrics::new(registry)?;
        let metrics = Arc::new(StoreServerMetrics::new(registry)?);
        let replicas = Arc::new(ReplicaManager::new(
            disks.clone(),
            store_config,
            store_metrics,
            config.partition_capacity_bytes,
        ));
        let health = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        Ok(StoreServer { config, state: AppState { replicas, metrics }, disks, health })
    }

    /// axum 路由(测试可直接拿去 `oneshot`;生产用 [`serve`](Self::serve))。
    pub fn router(&self) -> Router {
        Router::new()
            .route("/admin/partitions/{pid}", post(create_replica))
            .route(
                "/partitions/{pid}/blobs/{key}",
                put(put_blob).get(get_blob).delete(delete_blob),
            )
            .route("/partitions/{pid}/blobs/{key}/undelete", post(undelete_blob))
            .route("/partitions/{pid}/blobs/{key}/ttl", post(update_ttl_blob))
            .route("/partitions/{pid}/replica-metadata", get(replica_metadata))
            .route("/partitions/{pid}/segments", get(list_segments))
            .route("/partitions/{pid}/segments/{position}", get(get_segment))
            .with_state(self.state.clone())
    }

    /// 绑定 `http_addr` 后台运行数据面 + (若配置)接入 master 的注册/心跳 + (若开启)主动磁盘探测。
    pub async fn serve(&self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.config.http_addr).await?;
        let router = self.router();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        if self.config.disk_health_enabled {
            crate::health::spawn_prober(
                self.disks.disks().to_vec(),
                std::time::Duration::from_secs(self.config.disk_health_interval_secs),
                std::time::Duration::from_secs(self.config.disk_health_timeout_secs),
                self.health.clone(),
            );
        }
        if !self.config.master_addr.is_empty() {
            crate::heartbeat::spawn_cluster_client(
                self.config.clone(),
                self.disks.clone(),
                self.state.replicas.clone(),
                self.health.clone(),
                self.state.metrics.clone(),
            );
            if self.config.replication_enabled {
                crate::replication::spawn_replicator(
                    self.config.clone(),
                    self.state.replicas.clone(),
                    self.state.metrics.clone(),
                );
            }
        }
        if self.config.local_worker_enabled {
            crate::worker::spawn_local_worker(
                self.config.clone(),
                self.state.replicas.clone(),
                self.state.metrics.clone(),
            );
        }
        Ok(())
    }

    /// 本地 replica 管理器(测试/S2 心跳用)。
    pub fn replicas(&self) -> Arc<ReplicaManager> {
        self.state.replicas.clone()
    }
}

fn parse_key(key_hex: &str) -> Option<StoreKey> {
    let bytes = hex::decode(key_hex).ok()?;
    StoreKey::from_slice(&bytes)
}

/// store 引擎错误 → 变更端点 HTTP 状态码。
fn map_mut_err(e: &blob::Error) -> StatusCode {
    match e {
        blob::Error::NotFound => StatusCode::NOT_FOUND,
        blob::Error::Deleted
        | blob::Error::NotDeleted
        | blob::Error::AlreadyExists
        | blob::Error::AlreadyUpdated => StatusCode::CONFLICT,
        blob::Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// TTL 更新的查询参数。
#[derive(serde::Deserialize)]
struct TtlParams {
    expiration_ms: i64,
}

/// blob 读的查询参数:`replication=1` 时对已删除/过期对象也返回数据体(供复制忠实 apply)。
#[derive(serde::Deserialize, Default)]
struct GetParams {
    #[serde(default)]
    replication: Option<u8>,
}

/// 复制元数据请求参数:`seg`/`off` 为游标(均缺 = 从头);`max_bytes` 批上限。
#[derive(serde::Deserialize)]
struct MetaParams {
    seg: Option<u64>,
    off: Option<u64>,
    #[serde(default = "default_max_bytes")]
    max_bytes: u64,
}

fn default_max_bytes() -> u64 {
    4 * 1024 * 1024
}

async fn create_replica(
    State(st): State<AppState>,
    Path(pid): Path<PartitionId>,
) -> Response {
    let replicas = st.replicas.clone();
    match tokio::task::spawn_blocking(move || replicas.ensure_replica(pid)).await {
        Ok(Ok(_)) => {
            st.metrics.replicas.set(st.replicas.count() as i64);
            StatusCode::CREATED.into_response()
        }
        Ok(Err(e)) => {
            st.metrics.errors.inc();
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn put_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    body: Bytes,
) -> Response {
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let len = body.len() as u64;
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.put(key, &body)).await {
        Ok(Ok(())) => {
            st.metrics.put_total.inc();
            st.metrics.put_bytes.inc_by(len);
            StatusCode::CREATED.into_response()
        }
        Ok(Err(blob::Error::AlreadyExists)) => StatusCode::CONFLICT.into_response(),
        Ok(Err(e)) => {
            st.metrics.errors.inc();
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn get_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    Query(params): Query<GetParams>,
) -> Response {
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    // replication=1:复制读,连已删除/过期对象的数据体也返回(供接收方忠实 apply)。
    let for_repl = params.replication.unwrap_or(0) != 0;
    match tokio::task::spawn_blocking(move || store.get_with_options(&key, for_repl, for_repl)).await
    {
        Ok(Ok(data)) => {
            st.metrics.get_total.inc();
            st.metrics.get_bytes.inc_by(data.len() as u64);
            (StatusCode::OK, data).into_response()
        }
        Ok(Err(blob::Error::NotFound | blob::Error::Deleted | blob::Error::Expired)) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Ok(Err(e)) => {
            st.metrics.errors.inc();
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn replica_metadata(
    State(st): State<AppState>,
    Path(pid): Path<PartitionId>,
    Query(params): Query<MetaParams>,
) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let token = token_from(params.seg, params.off);
    let max_bytes = params.max_bytes;
    match tokio::task::spawn_blocking(move || store.find_entries_since(&token, max_bytes)).await {
        Ok(Ok(info)) => Json(MetadataResponse::from_find(info)).into_response(),
        Ok(Err(e)) => {
            st.metrics.errors.inc();
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn list_segments(State(st): State<AppState>, Path(pid): Path<PartitionId>) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let segs: Vec<crate::replication::SegmentInfo> = handle
        .store
        .sealed_segments()
        .into_iter()
        .map(|(position, data_end)| crate::replication::SegmentInfo { position, data_end })
        .collect();
    Json(segs).into_response()
}

async fn get_segment(
    State(st): State<AppState>,
    Path((pid, position)): Path<(PartitionId, u64)>,
) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.read_segment_prefix(position)).await {
        Ok(Ok(bytes)) => (StatusCode::OK, bytes).into_response(),
        Ok(Err(_)) => (StatusCode::NOT_FOUND, "no such segment").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn delete_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
) -> Response {
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.delete(key)).await {
        Ok(Ok(())) => {
            st.metrics.mutations.inc();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Err(e)) => {
            let code = map_mut_err(&e);
            if code == StatusCode::INTERNAL_SERVER_ERROR {
                st.metrics.errors.inc();
            }
            code.into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn undelete_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
) -> Response {
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.undelete(key)).await {
        Ok(Ok(life_version)) => {
            st.metrics.mutations.inc();
            (StatusCode::OK, life_version.to_string()).into_response()
        }
        Ok(Err(e)) => {
            let code = map_mut_err(&e);
            if code == StatusCode::INTERNAL_SERVER_ERROR {
                st.metrics.errors.inc();
            }
            code.into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

async fn update_ttl_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    Query(params): Query<TtlParams>,
) -> Response {
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let exp = params.expiration_ms;
    match tokio::task::spawn_blocking(move || store.update_ttl(key, exp)).await {
        Ok(Ok(())) => {
            st.metrics.mutations.inc();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Err(e)) => {
            let code = map_mut_err(&e);
            if code == StatusCode::INTERNAL_SERVER_ERROR {
                st.metrics.errors.inc();
            }
            code.into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}
