//! store-server:数据节点(store 引擎的多副本编排 + HTTP 数据面外壳)。
//!
//! 设计见 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §9 与
//! `openspec/changes/store-server/`。**S1**:骨架 + 数据面(HTTP put/get)+ `statfs` 容量。
//! 不接 master、不复制、**不含 EC**(暂不解锁)。

pub mod compaction_jobs;
pub mod config;
pub mod disk;
pub mod disk_health;
pub mod disk_pool;
pub mod error;
pub mod health;
pub mod heartbeat;
pub mod metrics;
pub mod replica;
pub mod replication;
pub mod request_control;
pub mod server;
pub mod worker;

/// 分区 id(与 master 侧一致 u32;blobId 高 32bit,D3/D8)。
pub type PartitionId = u32;

/// 盘声明(集成测试/嵌入方构造配置时的别名;`Plain` = 只给目录,`Spec` = 带每盘参数)。
pub use config::DataDir as StoreDataDir;
pub use config::StoreServerConfig;
pub use disk_health::{DiskHealthMap, DiskStatus, Hysteresis, Transition};
pub use error::{Error, Result};
pub use metrics::StoreServerMetrics;
pub use request_control::{DataOp, RequestControl};
pub use server::StoreServer;
