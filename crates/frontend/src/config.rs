//! `FrontendConfig`:frontend(HTTP REST 对象接口层)配置(TOML `[frontend]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。
//!
//! router 的配置(写 W / 读 R 的实现)以 `[frontend.router]` 子表内嵌 [`RouterConfig`] —— **真源只此一处**:
//! W = 实际副本数/2+1、R = 1 是 SDK 的正确性约束,frontend 只是"HTTP ↔ SDK 调用"的翻译层,
//! 不重复表达、也不外露成配置(见 `crates/common/router/src/config.rs`)。

use antfs_config::{at_least, Result, Validate};
use antfs_router::RouterConfig;
use serde::Deserialize;

/// 自动分片单片大小上限(1 GiB):单片在写路径上是**驻留内存的读-写缓冲**(见 server.rs
/// `auto_chunk_store`),必须给它一个内存意义上的硬顶。4096 片 × 1 GiB = 总量上限 4 TiB。
pub const AUTO_CHUNK_MAX_ALLOWED_BYTES: u64 = 1 << 30;

/// frontend 配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct FrontendConfig {
    /// HTTP 监听地址(host:port)。
    pub listen_addr: String,
    /// 单请求 body 上限(字节)。
    ///
    /// **默认 4 MiB**:开启自动分片(默认)时,超过此大小的 body 自动切片多次上传(见 `auto_chunk_enabled`);
    /// 关闭时超过即 413,客户端应走分片上传(`POST /blobs` × N → `POST /stitch`)。
    pub max_body_bytes: u64,
    /// 开启服务端自动分片:`POST /blobs` 的 body 超过 `max_body_bytes` 时,服务端边收边按
    /// `auto_chunk_bytes` 切片逐片入库,最后写拼装描述符并返回其 blobId(客户端无感知)。
    /// 关闭则恢复旧契约:超过 `max_body_bytes` 一律 413。
    pub auto_chunk_enabled: bool,
    /// 自动分片的单片大小(字节)。总量上限 = 4096 片 × 该值(描述符的片数上限,见 server.rs `MAX_CHUNKS`)。
    ///
    /// 默认 4 MiB(与 `max_body_bytes` 默认同值 = "单 blob 的常态大小");部署时须保证
    /// store 侧 `store_server.max_body_bytes` ≥ 该值 —— 片是普通 blob,走同一条数据面 PUT,
    /// 与单 blob 上传的既有部署对齐要求一致。
    pub auto_chunk_bytes: u64,
    /// CORS 允许的来源列表;**默认空 = 不挂 CORS 层**(不是"放开所有跨域")。
    /// 需要浏览器直连时才逐个列出。
    pub cors_allowed_origins: Vec<String>,
    /// 内嵌的 router SDK 配置(`[frontend.router]` 子表)。
    pub router: RouterConfig,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        FrontendConfig {
            listen_addr: "127.0.0.1:7400".to_string(),
            // 4 MiB:超过此大小默认自动分片(POST /blobs 边收边切);关闭自动分片时客户端须手动分片。
            max_body_bytes: 4 * 1024 * 1024,
            auto_chunk_enabled: true,
            auto_chunk_bytes: 4 * 1024 * 1024,
            cors_allowed_origins: Vec::new(),
            router: RouterConfig::default(),
        }
    }
}

/// `frontend.toml` 的顶层:整块配置在 `[frontend]` 表下(与 [`docs/design/frontend.md`](../../../docs/design/frontend.md)
/// §8 的 `frontend.*` key 前缀一致),环境变量对应 `AMBRY_FRONTEND__*`(如 `AMBRY_FRONTEND__LISTEN_ADDR`)。
///
/// 为什么要包一层:配置文件/环境变量里的 key 带 `frontend` 表名,文档写下的 `frontend.router.master_addr`
/// 才是真能生效的 key(直接反序列化 [`FrontendConfig`] 只认根级字段,`[frontend]` 表会被静默忽略)。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct FrontendToml {
    /// `[frontend]` 表。
    pub frontend: FrontendConfig,
}

impl Validate for FrontendConfig {
    fn validate(&self) -> Result<()> {
        if self.listen_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "frontend.listen_addr",
                "must not be empty",
            ));
        }
        at_least("frontend.max_body_bytes", self.max_body_bytes, 1)?;
        if self.auto_chunk_bytes == 0 {
            return Err(antfs_config::ConfigError::invalid(
                "frontend.auto_chunk_bytes",
                "must be at least 1",
            ));
        }
        // 单片是写路径上驻留内存的读写缓冲,给一个内存意义上的硬顶(见 AUTO_CHUNK_MAX_ALLOWED_BYTES)。
        if self.auto_chunk_bytes > AUTO_CHUNK_MAX_ALLOWED_BYTES {
            return Err(antfs_config::ConfigError::invalid(
                "frontend.auto_chunk_bytes",
                format!("must be at most {AUTO_CHUNK_MAX_ALLOWED_BYTES}"),
            ));
        }
        for origin in &self.cors_allowed_origins {
            // `*` 既等于"放开所有跨域"(安全问题),又会让 `AllowOrigin::list` panic —— 在此边界直接拒掉。
            if origin == "*" {
                return Err(antfs_config::ConfigError::invalid(
                    "frontend.cors_allowed_origins",
                    "wildcard `*` is not allowed; list the explicit origins",
                ));
            }
            // 空来源没有意义(等于配错),也不让 `AllowOrigin::list` 拿到空值。
            if origin.trim().is_empty() || axum::http::HeaderValue::from_str(origin).is_err() {
                return Err(antfs_config::ConfigError::invalid(
                    "frontend.cors_allowed_origins",
                    format!("`{origin}` is not a valid origin"),
                ));
            }
        }
        // router 是写/读路径的真源,配置错误必须在这里就挡住(而不是等第一个请求才炸)。
        self.router.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let cfg = FrontendConfig::default();
        assert_eq!(cfg.listen_addr, "127.0.0.1:7400");
        assert_eq!(cfg.max_body_bytes, 4 * 1024 * 1024);
        assert!(cfg.auto_chunk_enabled, "默认开启服务端自动分片");
        assert_eq!(cfg.auto_chunk_bytes, 4 * 1024 * 1024);
        assert!(cfg.cors_allowed_origins.is_empty(), "默认不挂 CORS 层");
        cfg.validate().expect("默认配置必须自洽");
        // 内嵌 router 子表回落到 `RouterConfig` 默认值。
        assert_eq!(cfg.router, RouterConfig::default());
    }

    #[test]
    fn validate_rejects_bad_values() {
        let mut cfg = FrontendConfig {
            listen_addr: String::new(),
            ..Default::default()
        };
        assert!(cfg.validate().is_err(), "空监听地址必须拒");

        cfg.listen_addr = "127.0.0.1:7400".into();
        cfg.max_body_bytes = 0;
        assert!(cfg.validate().is_err(), "0 字节上限必须拒");

        cfg.max_body_bytes = 1024;
        cfg.auto_chunk_bytes = 0;
        assert!(cfg.validate().is_err(), "0 字节单片必须拒");
        cfg.auto_chunk_bytes = AUTO_CHUNK_MAX_ALLOWED_BYTES + 1;
        assert!(cfg.validate().is_err(), "单片超出硬顶必须拒");
        cfg.auto_chunk_bytes = AUTO_CHUNK_MAX_ALLOWED_BYTES;
        assert!(cfg.validate().is_ok(), "单片恰在硬顶合法");

        cfg.auto_chunk_bytes = 4096;
        cfg.cors_allowed_origins = vec!["*".into()];
        assert!(cfg.validate().is_err(), "通配来源必须拒(等于放开跨域)");

        cfg.cors_allowed_origins = vec!["https://a.example".into(), "a\r\nb".into()];
        assert!(cfg.validate().is_err(), "非法 header 值(CRLF)必须拒");

        cfg.cors_allowed_origins = vec![String::new()];
        assert!(cfg.validate().is_err(), "空来源必须拒");

        cfg.cors_allowed_origins = vec!["https://a.example".into()];
        cfg.validate().expect("显式来源合法");

        // router 子表的非法值经 frontend 的校验一并挡住。
        cfg.router.master_addr = String::new();
        assert!(cfg.validate().is_err(), "router.master_addr 为空必须拒");
    }

    #[test]
    fn parses_nested_tables() {
        let toml: FrontendToml = antfs_config::Loader::new()
            .with_inline_toml(
                "[frontend]\nlisten_addr = \"127.0.0.1:7500\"\nmax_body_bytes = 4096\n\
                 auto_chunk_enabled = false\nauto_chunk_bytes = 1024\n\n\
                 [frontend.router]\nmaster_addr = \"127.0.0.1:7999\"\nid_segment_size = 8\n",
            )
            .load()
            .expect("[frontend] / [frontend.router] 子表必须能反序列化");
        let cfg = toml.frontend;
        cfg.validate().expect("配置必须自洽");
        assert_eq!(cfg.listen_addr, "127.0.0.1:7500");
        assert_eq!(cfg.max_body_bytes, 4096);
        assert!(!cfg.auto_chunk_enabled);
        assert_eq!(cfg.auto_chunk_bytes, 1024);
        assert_eq!(cfg.router.master_addr, "127.0.0.1:7999");
        assert_eq!(cfg.router.id_segment_size, 8);
        // 未出现的字段回落到默认值。
        assert_eq!(
            cfg.router.request_timeout_secs,
            RouterConfig::default().request_timeout_secs
        );

        // 空配置 → 整表默认。
        let empty: FrontendToml = antfs_config::Loader::new()
            .load()
            .expect("空配置必须能加载");
        assert_eq!(empty.frontend, FrontendConfig::default());
    }
}
