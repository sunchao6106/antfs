use antfs_s3::config::S3GatewayToml;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let toml: S3GatewayToml = antfs_config::Loader::new()
        .with_env_prefix("ANTFS_S3")
        .load()
        .expect("load config");
    let cfg = toml.s3;
    antfs_config::Validate::validate(&cfg).expect("validate config");

    let registry = antfs_metric::Registry::new();
    let _metrics = antfs_s3::metrics::S3Metrics::new(&registry).expect("register metrics");

    let _metastore = antfs_s3::metastore::connect(&cfg.metastore)
        .await
        .expect("connect metastore");

    let _router = antfs_router::Router::connect(cfg.router.clone(), &registry)
        .await
        .expect("connect router");

    tracing::info!(listen = %cfg.listen_addr, driver = %cfg.metastore.driver, "antfs-s3 starting");

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("bind");

    // TODO: build_router with real handlers (task 7.1)
    let app = axum::Router::new();

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
