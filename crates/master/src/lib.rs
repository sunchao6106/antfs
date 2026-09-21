//! master:store 之上的集群大脑(内建 Raft,取代 Helix/ZK)。
//!
//! 设计见 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §7 与
//! `openspec/changes/master-node/`。本 crate 落地 **M1 骨架**:内建 Raft(`openraft`)+ 节点自注册
//! + 单调 partition id + 控制面 gRPC。
//!
//! - 一致性:Raft 只用于**集群元数据**(D4),不碰数据副本一致性。
//! - Raft 边界(Q2):仅 [`types::MasterState`] 的四块进 Raft;liveness/容量是心跳软状态。
//! - 持久化(Q3):Raft log + 状态机快照落 `redb`。

pub mod autogrow;
pub mod config;
pub mod metrics;
pub mod network;
pub mod placement;
pub mod reconcile;
pub mod server;
pub mod soft;
pub mod store;
pub mod types;

/// tonic 从 `proto/master.proto` 生成的类型(控制面 + RaftInternal)。
pub mod pb {
    tonic::include_proto!("master.v1");
}

pub use config::MasterConfig;
pub use metrics::MasterMetrics;
pub use server::MasterNode;
pub use types::{MasterCommand, MasterResponse, MasterState, NodeId, PartitionId, TypeConfig};
