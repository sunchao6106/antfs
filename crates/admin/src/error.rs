//! admin 服务错误类型(`thiserror` + `?` 传播,CLAUDE.md §2)。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// 配置错误。
    #[error("config: {0}")]
    Config(#[from] antfs_config::ConfigError),
    /// metric 注册错误。
    #[error("metric: {0}")]
    Metric(#[from] antfs_metric::Error),
    /// 连接/调用 master 失败。
    #[error("master rpc: {0}")]
    MasterRpc(String),
    /// master 拒绝操作(回传其错误文本)。
    #[error("master rejected: {0}")]
    MasterRejected(String),
    /// 分区不存在(master 无此分区;上层映射 404)。
    #[error("partition {0} not found")]
    PartitionNotFound(u32),
    /// IO / 监听错误。
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
