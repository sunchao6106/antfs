//! master 节点二进制入口:加载配置 → 起 Raft + gRPC → 单节点自举(如需)→ 常驻。
//!
//! 配置来源:`master.toml` + 环境变量前缀 `AMBRY`(见 admin/store 同款 `antfs_config::Loader`)。
//! 单节点自举:`initial_members == [node_id]` 时,若存储为空则 `initialize` 引导一个单成员集群;
//! 已初始化(重启)则跳过。多节点集群由运维显式在某节点调 `initialize`(此处仅覆盖本地/单节点)。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_master::{MasterConfig, MasterNode};
use openraft::BasicNode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let cfg: MasterConfig = antfs_config::Loader::new()
        .with_file("master.toml", false)
        .with_env_prefix("AMBRY")
        .load_table("master")
        .unwrap_or_default();
    antfs_config::Validate::validate(&cfg)?;

    let registry = antfs_metric::Registry::new();
    let master = MasterNode::start(cfg.clone(), &registry).await?;
    master.serve()?;

    // 单节点自举:仅当初始成员集正好是自己时才引导(已初始化会返回错误,降级为 info 忽略)。
    if cfg.initial_members == vec![cfg.node_id] {
        let members = BTreeMap::from([(cfg.node_id, BasicNode::new(cfg.raft_addr.clone()))]);
        match master.initialize(members).await {
            Ok(()) => tracing::info!(node_id = cfg.node_id, "bootstrapped single-node master cluster"),
            Err(e) => tracing::info!(error = %e, "initialize skipped (already initialized?)"),
        }
        let _ = master
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader == Some(cfg.node_id), "become leader")
            .await;
    }

    tracing::info!(grpc = %cfg.grpc_addr, raft = %cfg.raft_addr, "master listening; Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
