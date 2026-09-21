//! 配置错误。

use thiserror::Error;

/// 加载或校验配置时的错误。
#[derive(Error, Debug)]
pub enum ConfigError {
    /// 底层加载/解析失败(文件缺失、TOML 语法错误、类型不匹配等)。
    #[error("failed to load config: {0}")]
    Load(#[from] config_rs::ConfigError),

    /// 语义校验失败:值能解析但不满足业务约束(区间/枚举等)。
    #[error("invalid config '{key}': {message}")]
    Invalid { key: String, message: String },
}

pub type Result<T> = std::result::Result<T, ConfigError>;

impl ConfigError {
    pub fn invalid(key: impl Into<String>, message: impl Into<String>) -> Self {
        ConfigError::Invalid {
            key: key.into(),
            message: message.into(),
        }
    }
}
