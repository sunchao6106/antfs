//! antfs-frontend:HTTP REST 对象接口层(对标 Ambry `ambry-frontend`)。
//!
//! **定位**:把 HTTP 请求翻译成 [`antfs_router`](router SDK)的调用 —— 写 W(该分区多数派)、读 R=1、
//! 向 master 取 blobId 号段都在 SDK 里实现(见 `crates/common/router` 与 `cluster.md §10`),frontend
//! **不重新实现**任何一致性逻辑,也不落每对象元数据(D3),故本层无状态、可水平扩展。
//!
//! ```text
//! HTTP ─▶ frontend ─▶ antfs-router(router SDK)─▶ master(gRPC 发号/拓扑)
//!                                              └▶ store(HTTP 数据面,PUT/GET/变更)
//! ```
//!
//! 不做(见本次任务 Non-goals):account/container、ACL/配额、S3 协议、signed URL、签名 chunk id。

pub mod config;
pub mod metrics;
pub mod range;
pub mod server;
pub mod upload_wal;

pub use config::{FrontendConfig, FrontendToml};
pub use metrics::FrontendMetrics;
pub use server::{build_router, AppState, Error, FrontendError};
