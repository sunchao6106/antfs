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
