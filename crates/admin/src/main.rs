//! admin web 二进制入口:加载配置 → 连 master → 起 HTTP 服务。

use std::sync::Arc;

use antfs_admin::config::AdminConfig;
use antfs_admin::master_api::GrpcMasterApi;
use antfs_admin::metrics::AdminMetrics;
use antfs_admin::server::{build_router, AppState, AuthConfig};
use antfs_config::Validate;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let cfg: AdminConfig = antfs_config::Loader::new()
        .with_file("admin.toml", false)
        .with_env_prefix("AMBRY")
        .load()
        .unwrap_or_default();
    cfg.validate()?;

    let registry = antfs_metric::Registry::new();
    let metrics = Arc::new(AdminMetrics::new(&registry)?);
    let master = Arc::new(GrpcMasterApi::connect(&cfg.master_addr, cfg.cluster_secret.clone()).await?);

    let state = AppState { master, metrics, registry, grafana_url: cfg.grafana_url.clone() };
    let auth = AuthConfig {
        operator_token: cfg.operator_token.clone(),
        viewer_token: cfg.viewer_token.clone(),
    };
    let router = build_router(state, auth);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(addr = %cfg.listen_addr, master = %cfg.master_addr, "admin web listening");
    axum::serve(listener, router).await?;
    Ok(())
}
