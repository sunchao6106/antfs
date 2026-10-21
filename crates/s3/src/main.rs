use std::sync::Arc;

use axum::ServiceExt;

use antfs_s3::config::S3GatewayToml;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    // 配置文件/环境变量都带 `s3` 表名(`antfs-s3.toml` 的 `[s3]` 表、`AMBRY_S3__LISTEN_ADDR`),
    // 与 frontend 二进制同型:顶层收进 `S3GatewayToml` 再取表。
    // 加载链先读可选共享文件 `common.toml`(公共 `[upload]` 表 = 与 REST 网关共用的
    // 单文件上限,一处改两边生效),自家 `antfs-s3.toml` 的同键后读覆盖。
    let toml: S3GatewayToml = antfs_config::Loader::new()
        .with_file("common.toml", false)
        .with_file("antfs-s3.toml", false)
        .with_env_prefix("AMBRY")
        .load()
        .expect("load config");
    toml.validate_all().expect("validate config");
    let max_object_bytes = toml.upload.max_object_bytes;
    let cfg = toml.s3;

    let registry = antfs_metric::Registry::new();
    let metrics = Arc::new(antfs_s3::metrics::S3Metrics::new(&registry).expect("register metrics"));

    let metastore = antfs_s3::metastore::connect(&cfg.metastore)
        .await
        .expect("connect metastore");
    // 每请求双查缓存(`s3-meta-cache`):ttl = 0 时 wrap 原样归还,零行为零开销。
    let metastore = antfs_s3::metastore::cache::wrap(
        metastore,
        cfg.metastore.meta_cache_ttl_secs,
        Arc::clone(&metrics),
    );

    let router = Arc::new(
        antfs_router::Router::connect(cfg.router.clone(), &registry)
            .await
            .expect("connect router"),
    );

    // SSE 主钥:按配置装载(缺省空 = 能力不部署;装载失败 = 启动即败,不让坏钥上线)。
    let sse = if cfg.sse_master_key_file.is_empty() {
        None
    } else {
        match antfs_s3::sse::MasterKey::load(&cfg.sse_master_key_file) {
            Ok(master) => Some(Arc::new(master)),
            Err(e) => {
                eprintln!("fatal: failed to load s3 SSE master key: {e}");
                std::process::exit(1);
            }
        }
    };
    let state = antfs_s3::server::AppState {
        router,
        metastore,
        metrics,
        registry: registry.clone(),
        config: Arc::new(cfg.clone()),
        sse,
        max_object_bytes,
    };
    // `--rotate-sse-key <file>`:SSE 主钥轮换离线模式(不监听、不起 GC,跑完即退;
    // 网关须停机——语义见 sse_rotation 模块头)。
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--rotate-sse-key") {
        let new_key_file = args.get(pos + 1).cloned().unwrap_or_else(|| {
            eprintln!("fatal: --rotate-sse-key requires a <new-key-file> argument");
            std::process::exit(2);
        });
        antfs_s3::sse_rotation::run_tool(state, &new_key_file).await;
        return;
    }
    // 后台 GC(软删行保留期清理 + 孤儿 multipart TTL;受 antfs-s3.toml [s3.gc] 控制,默认开)。
    antfs_s3::gc::spawn(state.clone(), cfg.gc.clone());
    let app = antfs_s3::server::build_app(state);

    tracing::info!(
        listen = %cfg.listen_addr,
        driver = %cfg.metastore.driver,
        master = %cfg.router.master_addr,
        "antfs-s3 starting"
    );

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("bind");

    axum::serve(listener, app.into_make_service())
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
