//! store 数据节点二进制入口:加载配置 → 起 HTTP 数据面 + 集群接入 + 本地维护 worker → 常驻。
//!
//! 配置来源:`store.toml` + 环境变量前缀 `AMBRY`(同 master/admin 的 `antfs_config::Loader`)。
//! 引擎段大小等复用默认 [`antfs_blob::StoreConfig`];小部署/测试可经环境自定。

use antfs_store::{StoreServer, StoreServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let cfg: StoreServerConfig = antfs_config::Loader::new()
        .with_file("store.toml", false)
        .with_env_prefix("AMBRY")
        .load()
        .unwrap_or_default();

    let registry = antfs_metric::Registry::new();
    let server = StoreServer::start(cfg.clone(), &registry)?;
    server.serve().await?;

    tracing::info!(http = %cfg.http_addr, node_id = cfg.node_id, "store node listening; Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
