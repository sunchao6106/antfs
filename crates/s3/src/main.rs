use std::sync::Arc;

use antfs_s3::config::S3GatewayToml;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    // 配置文件/环境变量都带 `s3` 表名(`antfs-s3.toml` 的 `[s3]` 表、`AMBRY_S3__LISTEN_ADDR`),
    // 与 frontend 二进制同型:顶层收进 `S3GatewayToml` 再取表。
    let toml: S3GatewayToml = antfs_config::Loader::new()
        .with_file("antfs-s3.toml", false)
        .with_env_prefix("AMBRY")
        .load()
        .expect("load config");
    let cfg = toml.s3;
    antfs_config::Validate::validate(&cfg).expect("validate config");

    let registry = antfs_metric::Registry::new();
    let metrics = Arc::new(antfs_s3::metrics::S3Metrics::new(&registry).expect("register metrics"));

    let metastore = antfs_s3::metastore::connect(&cfg.metastore)
        .await
        .expect("connect metastore");

    let router = Arc::new(
        antfs_router::Router::connect(cfg.router.clone(), &registry)
            .await
            .expect("connect router"),
    );

    let state = antfs_s3::server::AppState {
        router,
        metastore,
        metrics,
        registry: registry.clone(),
        config: Arc::new(cfg.clone()),
    };
    let app = antfs_s3::server::build_router(state);

    tracing::info!(
        listen = %cfg.listen_addr,
        driver = %cfg.metastore.driver,
        master = %cfg.router.master_addr,
        "antfs-s3 starting"
    );

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("bind");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("serve");
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("install ctrl-c handler");
    tracing::info!("shutting down");
}
