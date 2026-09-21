//! common/config:项目统一的**类型化分层配置**。
//!
//! 设计对齐 Ambry 语义、实现用 Rust 原生方案(`serde` + [`config`](https://docs.rs/config)):
//!
//! - Ambry 每个模块一个 `*Config` 类,字段用 `@Config("key")` + `@Default("v")` 声明。
//! - 这里每个模块定义一个 struct,`#[derive(Deserialize)]` + `#[serde(default)]` + `impl Default`
//!   ——`Default` 即等价 `@Default`,字段名即 key(TOML 里可嵌套分组)。
//! - 区间/枚举等语义校验实现 [`Validate`],用 [`in_range`] / [`at_least`] / [`one_of`] 辅助
//!   (等价 Ambry 的 `getIntInRange` / `getShortFromAllowedValues`)。
//!
//! 加载走 [`Loader`],来源按优先级(后覆盖前)合并:struct 默认 < TOML 文件 < 环境变量 < 显式 override。
//!
//! ```
//! use serde::Deserialize;
//! use config::{Loader, Validate, Result, in_range};
//!
//! #[derive(Debug, Deserialize)]
//! #[serde(default)]
//! struct StoreConfig {
//!     data_flush_interval_seconds: u64,
//!     index_max_memory_size_bytes: u64,
//! }
//! impl Default for StoreConfig {
//!     fn default() -> Self {
//!         Self { data_flush_interval_seconds: 60, index_max_memory_size_bytes: 20_971_520 }
//!     }
//! }
//! impl Validate for StoreConfig {
//!     fn validate(&self) -> Result<()> {
//!         in_range("data_flush_interval_seconds", self.data_flush_interval_seconds, 1, 86_400)?;
//!         Ok(())
//!     }
//! }
//!
//! let cfg: StoreConfig = Loader::new()
//!     .with_inline_toml("data_flush_interval_seconds = 30")
//!     .load_validated().unwrap();
//! assert_eq!(cfg.data_flush_interval_seconds, 30);
//! ```

mod error;

pub use error::{ConfigError, Result};

use config_rs::{Config, Environment, File, FileFormat};
use serde::de::DeserializeOwned;

/// 模块配置的语义校验(区间、枚举、互斥等)。反序列化只保证类型,业务约束在此把关。
pub trait Validate {
    fn validate(&self) -> Result<()>;
}

/// 分层配置加载器。来源优先级(后者覆盖前者):struct 默认值 < 内联/文件 TOML < 环境变量 < override。
#[derive(Debug, Clone, Default)]
pub struct Loader {
    inline_toml: Vec<String>,
    files: Vec<FileSpec>,
    env_prefix: Option<String>,
    env_separator: String,
    overrides: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct FileSpec {
    path: String,
    required: bool,
}

impl Loader {
    pub fn new() -> Self {
        Loader {
            env_separator: "__".to_string(),
            ..Default::default()
        }
    }

    /// 追加一段内联 TOML(测试或内置默认覆盖用)。
    pub fn with_inline_toml(mut self, toml: impl Into<String>) -> Self {
        self.inline_toml.push(toml.into());
        self
    }

    /// 追加一个 TOML 配置文件。`required=false` 时文件不存在也不报错。
    pub fn with_file(mut self, path: impl Into<String>, required: bool) -> Self {
        self.files.push(FileSpec {
            path: path.into(),
            required,
        });
        self
    }

    /// 启用环境变量覆盖。如前缀 `AMBRY`、分隔符 `__`,则 `AMBRY_STORE__FLUSH=1` → `store.flush`。
    pub fn with_env_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.env_prefix = Some(prefix.into());
        self
    }

    /// 自定义环境变量嵌套分隔符(默认 `__`)。
    pub fn with_env_separator(mut self, separator: impl Into<String>) -> Self {
        self.env_separator = separator.into();
        self
    }

    /// 最高优先级的显式覆盖(如命令行传入)。
    pub fn set_override(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.overrides.push((key.into(), value.into()));
        self
    }

    /// 合并所有来源并反序列化为 `T`(缺失字段回落到 `T` 的 serde 默认)。
    pub fn load<T: DeserializeOwned>(self) -> Result<T> {
        let mut builder = Config::builder();
        for toml in &self.inline_toml {
            builder = builder.add_source(File::from_str(toml, FileFormat::Toml));
        }
        for f in &self.files {
            builder = builder.add_source(File::with_name(&f.path).required(f.required));
        }
        if let Some(prefix) = &self.env_prefix {
            builder = builder.add_source(
                Environment::with_prefix(prefix)
                    .prefix_separator("_")
                    .separator(&self.env_separator)
                    .try_parsing(true),
            );
        }
        for (k, v) in &self.overrides {
            builder = builder.set_override(k.as_str(), v.as_str())?;
        }
        Ok(builder.build()?.try_deserialize()?)
    }

    /// 同 [`load`](Self::load),但反序列化后调用 [`Validate::validate`]。
    pub fn load_validated<T: DeserializeOwned + Validate>(self) -> Result<T> {
        let value: T = self.load()?;
        value.validate()?;
        Ok(value)
    }
}

/// 校验 `value ∈ [start, end]`,否则返回 [`ConfigError::Invalid`]。等价 Ambry `getIntInRange`。
pub fn in_range<T>(key: &str, value: T, start: T, end: T) -> Result<T>
where
    T: PartialOrd + std::fmt::Display,
{
    if value >= start && value <= end {
        Ok(value)
    } else {
        Err(ConfigError::invalid(
            key,
            format!("value {value} is not in range {start}-{end}"),
        ))
    }
}

/// 校验 `value >= min`,否则返回 [`ConfigError::Invalid`]。
pub fn at_least<T>(key: &str, value: T, min: T) -> Result<T>
where
    T: PartialOrd + std::fmt::Display,
{
    if value >= min {
        Ok(value)
    } else {
        Err(ConfigError::invalid(
            key,
            format!("value {value} is less than minimum {min}"),
        ))
    }
}

/// 校验 `value` 在允许集合内,否则返回 [`ConfigError::Invalid`]。等价 `getShortFromAllowedValues`。
pub fn one_of<T>(key: &str, value: T, allowed: &[T]) -> Result<T>
where
    T: PartialEq + std::fmt::Display,
{
    if allowed.contains(&value) {
        Ok(value)
    } else {
        let list = allowed
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Err(ConfigError::invalid(
            key,
            format!("value {value} is not among allowed values: [{list}]"),
        ))
    }
}
