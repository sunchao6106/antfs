//! store-server 错误类型(`thiserror` + `?` 传播,CLAUDE.md §2)。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// 底层 store 引擎错误。
    #[error("store engine: {0}")]
    Store(#[from] antfs_blob::Error),
    /// IO 错误(建目录 / statfs 等)。
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// 配置错误。
    #[error("config: {0}")]
    Config(#[from] antfs_config::ConfigError),
    /// metric 注册错误。
    #[error("metric: {0}")]
    Metric(#[from] antfs_metric::Error),
    /// 无满足容量上界的可用盘。
    #[error("no disk with enough free space")]
    NoDiskSpace,
    /// 拉复制传输/解析错误。
    #[error("replication: {0}")]
    Replication(String),
}

pub type Result<T> = std::result::Result<T, Error>;
