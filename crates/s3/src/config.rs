use antfs_config::{at_least, Result, Validate};
use antfs_router::RouterConfig;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct S3GatewayConfig {
    pub listen_addr: String,
    pub max_body_bytes: u64,
    pub inline_body_max: u64,
    pub region: String,
    pub operator_token: String,
    pub metastore: MetaStoreConfig,
    pub router: RouterConfig,
    pub gc: GcConfig,
    /// 虚拟主机风格路由的 Host 匹配后缀(如 `s3.example.com`):`Host = {bucket}.{suffix}`
    /// 的请求按等价路径风格请求路由(路径与 query 前挂桶名;签名验证发生在改写之前)。
    /// **默认空 = 关**(行为零变化)。
    pub virtual_host_domain: String,
}

impl Default for S3GatewayConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:7500".to_string(),
            max_body_bytes: 4 * 1024 * 1024, // 4 MiB — 超过此大小走 S3 Multipart Upload
            inline_body_max: 1024 * 1024,    // 1 MiB
            region: "us-east-1".to_string(),
            operator_token: String::new(),
            metastore: MetaStoreConfig::default(),
            router: RouterConfig::default(),
            gc: GcConfig::default(),
            virtual_host_domain: String::new(),
        }
    }
}

impl Validate for S3GatewayConfig {
    fn validate(&self) -> Result<()> {
        if self.listen_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "s3.listen_addr",
                "must not be empty",
            ));
        }
        at_least("s3.max_body_bytes", self.max_body_bytes, 1)?;
        at_least("s3.inline_body_max", self.inline_body_max, 1)?;
        if self.region.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "s3.region",
                "must not be empty",
            ));
        }
        self.metastore.validate()?;
        self.router.validate()?;
        self.gc.validate()?;
        if !self.virtual_host_domain.is_empty() && !is_plain_domain(&self.virtual_host_domain) {
            return Err(antfs_config::ConfigError::invalid(
                "s3.virtual_host_domain",
                "must be a plain lowercase domain (letters, digits, '-', '.'; no scheme or port)",
            ));
        }
        Ok(())
    }
}

/// 朴素小写域名形态(`s3.virtual_host_domain` 断言用):无 scheme/端口、无前后导点,
/// 字符限于小写字母/数字/`-`/`.`。
fn is_plain_domain(domain: &str) -> bool {
    !domain.is_empty()
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && domain
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// S3 网关后台 GC(短删行保留期清理 + 孤儿 multipart TTL 回收)。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct GcConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    /// 软删/遮蔽行保留期(防误删缓冲;**最小 1s**,不为 0 —— 误配零保留立即清行不可接受)。
    pub deletion_retention_secs: u64,
    /// 未 Complete multipart session 的 TTL。
    pub upload_ttl_secs: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 300,
            deletion_retention_secs: 86_400,
            upload_ttl_secs: 259_200,
        }
    }
}

impl Validate for GcConfig {
    fn validate(&self) -> Result<()> {
        at_least("s3.gc.interval_secs", self.interval_secs, 1)?;
        at_least(
            "s3.gc.deletion_retention_secs",
            self.deletion_retention_secs,
            1,
        )?;
        at_least("s3.gc.upload_ttl_secs", self.upload_ttl_secs, 1)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct MetaStoreConfig {
    pub driver: String,
    pub url: String,
    pub max_connections: u32,
}

impl Default for MetaStoreConfig {
    fn default() -> Self {
        Self {
            driver: "postgres".to_string(),
            url: "postgres://localhost:5432/antfs_s3".to_string(),
            max_connections: 20,
        }
    }
}

impl Validate for MetaStoreConfig {
    fn validate(&self) -> Result<()> {
        match self.driver.as_str() {
            "postgres" | "mysql" => {}
            other => {
                return Err(antfs_config::ConfigError::invalid(
                    "s3.metastore.driver",
                    format!("unsupported driver `{other}`, expected `postgres` or `mysql`"),
                ));
            }
        }
        if self.url.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "s3.metastore.url",
                "must not be empty",
            ));
        }
        at_least(
            "s3.metastore.max_connections",
            self.max_connections as u64,
            1,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct S3GatewayToml {
    pub s3: S3GatewayConfig,
}
