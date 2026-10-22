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

/// 自动分片单片大小上限(1 GiB):单片在写路径上是**驻留内存的读写缓冲**(见 server.rs
/// `stream_upload` 的分片模式),必须给它一个内存意义上的硬顶。4096 片 × 1 GiB = 总量上限 4 TiB。
pub const AUTO_CHUNK_MAX_ALLOWED_BYTES: u64 = 1 << 30;

/// frontend 配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct FrontendConfig {
    /// HTTP 监听地址(host:port)。
    pub listen_addr: String,
    /// 单请求 body 上限(字节)。
    ///
    /// **默认 4 MiB**:这是**内部分档界**(≤ 它走"内存整收"的中档;超过转边收边分片),
    /// 对客户端不再是"被拒与被切"的分界 —— 契约分界只有一个:公共上限
    /// `[upload] max_object_bytes`(见 [`FrontendToml::upload`],REST 与 S3 网关共用一处)。
    pub max_body_bytes: u64,
    /// 边收边分片的**孤儿收账簿目录**(Ambry「MySQL 会话表 + 定期清扫」的本地等价物)。
    ///
    /// **空 = 不记账不清扫**(④ 已接受的崩溃窗孤儿无兜底);非空 = 启动建目录(失败即
    /// 拒启)、分片上传逐片记账、启动与每 10 分钟清扫超时孤儿会话(作废其片并删账)。
    /// 账簿语义与已知残余见 `docs/design/frontend.md` §5。
    pub upload_session_wal_dir: std::path::PathBuf,
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
            // 内部分档界(中档"内存整收"的上限;超过转边收边分片)。
            max_body_bytes: 4 * 1024 * 1024,
            upload_session_wal_dir: std::path::PathBuf::new(),
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
    /// `[upload]` 公共表(与 S3 网关共用一个值:单文件上传上限;来自 `common.toml` 或
    /// 自家文件,一处配置两边生效 —— 2026-10-21 所有者裁决)。默认 16 GiB。
    pub upload: antfs_config::UploadConfig,
}

impl FrontendToml {
    /// 整份装配校验:自家字段 + 公共上限 + **跨界检查**:
    /// ① 公共上限 ≤ 4096 × 片宽(物理天花板来自拼装描述符的片数容量;要更大单文件须先调大片宽);
    /// ② router 恒切片宽 ≤ 0(关)或 ≥ 网关切片宽 `auto_chunk_bytes` —— 否则网关片会在
    /// router 内被**二次切**成"子片 + 子描述符",而本校验保证它结构性不发生(design §12 共存矩阵)。
    pub fn validate_all(&self) -> Result<()> {
        self.frontend.validate()?;
        self.upload.validate()?;
        let cap = antfs_descriptor::MAX_CHUNKS as u64 * self.frontend.auto_chunk_bytes;
        if self.upload.max_object_bytes > cap {
            return Err(antfs_config::ConfigError::invalid(
                "common.max_object_bytes",
                format!(
                    "exceeds descriptor capacity: {} chunks x frontend.auto_chunk_bytes = {cap}; increase auto_chunk_bytes first",
                    antfs_descriptor::MAX_CHUNKS
                ),
            ));
        }
        let width = self.frontend.router.put_bytes_chunk_bytes;
        // 恒切禁 0(2026-10-22 裁决)+ 共存矩阵:width < 片宽即拒(0 天然小于任何正片宽)。
        if width < self.frontend.auto_chunk_bytes {
            return Err(antfs_config::ConfigError::invalid(
                "frontend.router.put_bytes_chunk_bytes",
                format!(
                    "must be >= frontend.auto_chunk_bytes ({}) and nonzero — chunking cannot be disabled — otherwise every gateway chunk is re-chunked into sub-chunks + a sub-descriptor",
                    self.frontend.auto_chunk_bytes
                ),
            ));
        }
        Ok(())
    }
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
        assert_eq!(
            cfg.upload_session_wal_dir,
            std::path::PathBuf::new(),
            "默认不记账不清扫"
        );
        assert_eq!(cfg.auto_chunk_bytes, 4 * 1024 * 1024);
        assert!(cfg.cors_allowed_origins.is_empty(), "默认不挂 CORS 层");
        cfg.validate().expect("默认配置必须自洽");
        // 内嵌 router 子表回落到 `RouterConfig` 默认值。
        assert_eq!(cfg.router, RouterConfig::default());
        // 公共上限层:FrontendToml 默认(顶层 [upload] 表缺省)与 S3 网关共用同一默认。
        let toml = FrontendToml::default();
        assert_eq!(toml.upload.max_object_bytes, 16 * 1024 * 1024 * 1024);
        toml.validate_all().expect("默认整份必须自洽");
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
        // 单片硬顶 1 GiB × 4096 片 = 4 TiB ≥ 默认上限,恒 OK。
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

    /// 公共上限与片宽的跨界检查(挂在 FrontendToml::validate_all,须两表在同一装配里)。
    #[test]
    fn shared_upload_cap_cross_checks_with_chunk_width() {
        let mut toml = FrontendToml::default();
        toml.frontend.auto_chunk_bytes = 4096; // 容量 = 4096 × 4 KiB = 16 MiB
        toml.upload.max_object_bytes = 16 * 1024 * 1024 + 1;
        assert!(
            toml.validate_all().is_err(),
            "公共上限超出 4096 × 片宽必须拒(先调大片宽)"
        );
        toml.upload.max_object_bytes = 16 * 1024 * 1024;
        toml.validate_all().expect("恰在容量上合法");

        // 上限为 0 由公共层自校验拒绝。
        toml.upload.max_object_bytes = 0;
        assert!(toml.validate_all().is_err(), "0 上限必须拒");
    }

    /// router 恒切片宽与网关切片宽的跨界检查(design §12 共存矩阵,结构性禁止"网关片被二次切")。
    #[test]
    fn router_chunk_width_cross_checks_with_gateway_chunk_bytes() {
        let mut toml = FrontendToml::default();
        toml.frontend.router.put_bytes_chunk_bytes = 0;
        assert!(
            toml.validate_all().is_err(),
            "恒切禁 0:片宽必须大于 0,0 必须被拒"
        );
        toml.frontend.router.put_bytes_chunk_bytes = 4 * 1024 * 1024;
        toml.validate_all()
            .expect("与默认网关片宽(4 MiB)相等合法(片不二次切)");
        toml.frontend.router.put_bytes_chunk_bytes = 8 * 1024 * 1024;
        toml.validate_all().expect("大于网关片宽合法(片走单片路径)");
        toml.frontend.router.put_bytes_chunk_bytes = 64 * 1024;
        assert!(
            toml.validate_all().is_err(),
            "router 片宽 < 网关片宽必须拒(每网关片将被二次切成子片 + 子描述符)"
        );
    }

    #[test]
    fn parses_nested_tables() {
        let toml: FrontendToml = antfs_config::Loader::new()
            .with_inline_toml(
                "[frontend]\nlisten_addr = \"127.0.0.1:7500\"\nmax_body_bytes = 4096\n\
                 auto_chunk_bytes = 1024\n\
                 upload_session_wal_dir = \"/var/lib/antfs/frontend-sessions\"\n\n\
                 [upload]\nmax_object_bytes = 1048576\n\n\
                 [frontend.router]\nmaster_addr = \"127.0.0.1:7999\"\nid_segment_size = 8\n",
            )
            .load()
            .expect("[frontend] / [frontend.router] 子表必须能反序列化");
        let cfg = toml.frontend.clone();
        let upload = toml.upload.clone();
        toml.validate_all().expect("整份配置必须自洽");
        assert_eq!(cfg.listen_addr, "127.0.0.1:7500");
        assert_eq!(cfg.max_body_bytes, 4096);
        assert_eq!(
            cfg.upload_session_wal_dir,
            std::path::PathBuf::from("/var/lib/antfs/frontend-sessions")
        );
        // 顶层公共表 [upload] 与 [frontend] 并存解析(公共上限挂 FrontendToml 层)。
        assert_eq!(upload.max_object_bytes, 1048576);
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
