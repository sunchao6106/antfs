//! admin web:控制面 dashboard + 运维 API(经 master admin gRPC)。
//!
//! 设计见 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §7.6 Q6 与
//! `openspec/changes/admin-web/`。独立服务:master gRPC 客户端 + axum HTTP + operator 鉴权 + 内嵌 SPA。

pub mod audit;
pub mod config;
pub mod error;
pub mod master_api;
pub mod metrics;
pub mod scrub;
pub mod server;
pub mod store_client;

pub use audit::AuditLog;
pub use config::AdminConfig;
pub use error::{Error, Result};
pub use metrics::AdminMetrics;
pub use server::{build_router, Actor, AppState, AuthConfig};
