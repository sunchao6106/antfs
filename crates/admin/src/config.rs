//! `AdminConfig`:admin web 服务配置(TOML `[admin]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。

use antfs_config::{Result, Validate};
use serde::Deserialize;

/// admin web 服务配置。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    /// master 控制面 gRPC 地址(host:port)。
    pub master_addr: String,
    /// admin HTTP 监听地址(host:port)。
    pub listen_addr: String,
    /// operator 准入令牌(Bearer,读+写);**空 = 关校验**(仅本地开发)。
    pub operator_token: String,
    /// viewer 准入令牌(Bearer,只读);持此令牌的写操作返回 403。空 = 不启用只读角色。
    pub viewer_token: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致),用于对 master 发写操作(建分区/下线)。
    pub cluster_secret: String,
    /// Grafana 外链(dashboard 里"查看指标"按钮);空 = 不显示。
    pub grafana_url: String,
}

impl Default for AdminConfig {
    fn default() -> Self {
        AdminConfig {
            master_addr: "127.0.0.1:7101".to_string(),
            listen_addr: "127.0.0.1:7300".to_string(),
            operator_token: String::new(),
            viewer_token: String::new(),
            cluster_secret: String::new(),
            grafana_url: String::new(),
        }
    }
}

impl Validate for AdminConfig {
    fn validate(&self) -> Result<()> {
        if self.master_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid("admin.master_addr", "must not be empty"));
        }
        if self.listen_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid("admin.listen_addr", "must not be empty"));
        }
        Ok(())
    }
}
