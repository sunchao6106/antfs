//! frontend 二进制入口:加载配置 → 连 router SDK → 起 HTTP 服务。

use antfs_config::Validate;
use antfs_frontend::config::FrontendToml;
use antfs_frontend::server::{build_router, AppState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    // 配置文件/环境变量都带 `frontend` 表名(`frontend.toml` 的 `[frontend]` 表、
    // `AMBRY_FRONTEND__LISTEN_ADDR`),故顶层收进 [`FrontendToml`] 再取表。
    let loaded: FrontendToml = antfs_config::Loader::new()
        .with_file("frontend.toml", false)
        .with_env_prefix("AMBRY")
        .load()
        .unwrap_or_default();
    let cfg = loaded.frontend;
    cfg.validate()?;

    let registry = antfs_metric::Registry::new();
    let state = AppState::from_config(&cfg, &registry).await?;
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(
        addr = %cfg.listen_addr,
        master = %cfg.router.master_addr,
        "frontend listening"
    );
    // 收到 SIGINT/SIGTERM 后停止接收新请求并排空在途请求(上传/下载可能正在流式传输)。
    // SIGTERM 也要接:`docker stop`/systemd/K8s 都先发它,只接 ctrl_c 会让优雅退出形同虚设。
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        tracing::info!("shutdown signal received");
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
