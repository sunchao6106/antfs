//! `RouterConfig`:router(客户端 SDK)配置(TOML `[router]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。
//! **没有 W/R 配置项**:写成功副本数由该分区**实际副本数**推导(多数派),见
//! [`crate::Router::put`];读恒为「第一个成功即返回」——这两条是正确性约束,不做成可配。
//!
//! frontend 内嵌本配置时放在 `[frontend.router]` 子表下(结构完全相同)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// router(客户端 SDK)配置。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct RouterConfig {
    /// master 控制面 gRPC 地址(host:port)。
    ///
    /// **须指向 leader**(或前面挂一层转发):`GetBlobIds` 是写路径,非 leader 会回 `ok=false`
    /// 并给出 leader 地址(当前回填的是 raft 地址,不能直接当 gRPC 端用,详见 `docs/design/client.md`)。
    pub master_addr: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致);空 = 关校验(仅本地测试)。
    pub cluster_secret: String,
    /// 单次数据面 HTTP 请求超时(秒)。
    pub request_timeout_secs: u64,
    /// 一次向 master 申请的 blobId 个数(号段大小 N)。越大越省 RPC、client 崩溃留下的空洞越大
    /// (空洞无害,见 `blobid-master-assign` D3/D9)。
    pub id_segment_size: u32,
}

impl Default for RouterConfig {
    fn default() -> Self {
        RouterConfig {
            master_addr: "127.0.0.1:7101".to_string(),
            cluster_secret: String::new(),
            request_timeout_secs: 10,
            id_segment_size: 1024,
        }
    }
}

impl Validate for RouterConfig {
    fn validate(&self) -> Result<()> {
        if self.master_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "client.master_addr",
                "must not be empty",
            ));
        }
        at_least("router.request_timeout_secs", self.request_timeout_secs, 1)?;
        at_least("router.id_segment_size", self.id_segment_size, 1)?;
        Ok(())
    }
}
