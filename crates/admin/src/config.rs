//! `AdminConfig`:admin web 服务配置(TOML `[admin]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。

use antfs_config::{at_least, one_of, ConfigError, Result, Validate};
use serde::Deserialize;

/// 准入角色(配置 `[[admin.users]]` 的 `role` 取值真值;RBAC 与审计都以此为准)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// 读 + 写(运维操作)。
    Operator,
    /// 只读:写操作 → 403,`/api/audit` 也不开放。
    Viewer,
}

impl Role {
    /// 解析配置里的角色名;不在允许集合内 → `None`(由 [`AdminConfig::validate`] 报错)。
    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "operator" => Some(Role::Operator),
            "viewer" => Some(Role::Viewer),
            _ => None,
        }
    }

    /// 对外名称(`/api/me` 与审计记录的 `role` 字段)。
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Operator => "operator",
            Role::Viewer => "viewer",
        }
    }
}

/// 允许的角色取值(报错信息与校验共用一处真值)。
const ROLES: [&str; 2] = ["operator", "viewer"];

/// `[[admin.users]]` 表里的一个 admin 用户。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AdminUser {
    /// 操作者名(审计记录里的 `actor`)。
    pub name: String,
    /// 准入令牌(Bearer,精确匹配)。
    pub token: String,
    /// 角色:`operator` / `viewer`(非法值 [`.validate`](Validate::validate) 报错)。
    pub role: String,
}

/// admin 账号密码登录的 SQL 后端配置(`[admin.auth_db]` 表,形态对齐 S3 网关的
/// `[s3.metastore]`)。
///
/// **`url` 空 = 特性关**:不连接、无新端点行为(静态令牌面独存,零行为逃生口)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct AdminAuthDbConfig {
    /// SQL 驱动:`postgres` / `mysql`(**仅在 `url` 非空时校验**)。
    pub driver: String,
    /// 连接 URL;空 = 账号密码登录特性关。
    pub url: String,
    /// 连接池上限。
    pub max_connections: u32,
}

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
    /// 多用户令牌表(`[[admin.users]]`);**优先于**上面两个单令牌(同名角色各带自己的 actor 名)。
    pub users: Vec<AdminUser>,
    /// 审计日志(JSONL,append-only)路径;写失败只 warn,不影响请求。
    pub audit_path: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致),用于对 master 发写操作(建分区/下线)。
    pub cluster_secret: String,
    /// Grafana 外链(dashboard 里"查看指标"按钮);空 = 不显示。
    pub grafana_url: String,
    /// **周期性跨副本比对**(admin 级一致性巡检调度)总开关;默认关。开启后本进程定时
    /// 对健康分区逐个跑浅层比对(与手动 `POST /api/partitions/{id}/scrub` 同一条管线,deep 恒否),
    /// 结果进既有环形表与指标(带 `origin=scheduled` 标);**只报警不修复**。
    /// 运行期可用 `/api/scrub-schedule-control` 掐停(不动手动端点)。
    pub scrub_schedule_enabled: bool,
    /// 周期比对的节拍:两次比对之间的基础间隔(秒),每拍比对**一个**分区(蜗牛家风;
    /// 全量覆盖周期 = 分区数 × 本值)。默认 600s ⇒ 千分区 ≈ 7 天一轮。
    pub scrub_schedule_interval_secs: u64,
    /// 账号密码登录的 SQL 后端(`admin-auth-db`);`url` 空 = 特性关(静态令牌面独存)。
    pub auth_db: AdminAuthDbConfig,
}

impl Default for AdminConfig {
    fn default() -> Self {
        AdminConfig {
            master_addr: "127.0.0.1:7101".to_string(),
            listen_addr: "127.0.0.1:7300".to_string(),
            operator_token: String::new(),
            viewer_token: String::new(),
            users: Vec::new(),
            audit_path: "./admin-audit.jsonl".to_string(),
            cluster_secret: String::new(),
            grafana_url: String::new(),
            scrub_schedule_enabled: false,
            scrub_schedule_interval_secs: 600,
            auth_db: AdminAuthDbConfig::default(),
        }
    }
}

impl Validate for AdminConfig {
    fn validate(&self) -> Result<()> {
        if self.master_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "admin.master_addr",
                "must not be empty",
            ));
        }
        if self.listen_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "admin.listen_addr",
                "must not be empty",
            ));
        }
        for (i, u) in self.users.iter().enumerate() {
            if u.token.is_empty() {
                return Err(ConfigError::invalid(
                    format!("admin.users[{i}].token"),
                    "must not be empty",
                ));
            }
            one_of(&format!("admin.users[{i}].role"), u.role.as_str(), &ROLES)?;
        }
        // 间隔 0 = 忙转(每秒打满 master),与"周期比对"的意图背道而驰,当场拒。
        at_least(
            "admin.scrub_schedule_interval_secs",
            self.scrub_schedule_interval_secs,
            1,
        )?;
        self.auth_db.validate()?;
        Ok(())
    }
}

impl Validate for AdminAuthDbConfig {
    fn validate(&self) -> Result<()> {
        // url 空 = 特性关:其余字段不 constrain(与"空 = 行为不变"的逃生口一致)。
        if self.url.is_empty() {
            return Ok(());
        }
        one_of(
            "admin.auth_db.driver",
            self.driver.as_str(),
            &["postgres", "mysql"],
        )?;
        at_least(
            "admin.auth_db.max_connections",
            self.max_connections as u64,
            1,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str, role: &str) -> AdminUser {
        AdminUser {
            name: name.into(),
            token: format!("tok-{name}"),
            role: role.into(),
        }
    }

    #[test]
    fn default_has_no_users_and_default_audit_path() {
        let cfg = AdminConfig::default();
        assert!(cfg.users.is_empty());
        assert_eq!(cfg.audit_path, "./admin-audit.jsonl");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn users_accept_operator_and_viewer_only() {
        let ok = AdminConfig {
            users: vec![user("a", "operator"), user("b", "viewer")],
            ..Default::default()
        };
        assert!(ok.validate().is_ok(), "{ok:?}");

        let bad_role = AdminConfig {
            users: vec![user("a", "Operator")],
            ..Default::default()
        };
        let err = bad_role.validate().unwrap_err().to_string();
        assert!(err.contains("admin.users[0].role"), "{err}");
        assert!(err.contains("operator, viewer"), "{err}");

        let empty_token = AdminConfig {
            users: vec![AdminUser {
                name: "a".into(),
                token: String::new(),
                role: "viewer".into(),
            }],
            ..Default::default()
        };
        let err = empty_token.validate().unwrap_err().to_string();
        assert!(err.contains("admin.users[0].token"), "{err}");
    }

    #[test]
    fn role_round_trips_through_parse_and_name() {
        for name in ROLES {
            let role = Role::parse(name).unwrap();
            assert_eq!(role.as_str(), name);
        }
        assert!(Role::parse("root").is_none());
    }

    /// 周期比对默认:关 / 600s。改动默认值必须显式改本测试(CLAUDE.md §5.1)。
    #[test]
    fn scrub_schedule_defaults_off_and_slow() {
        let c = AdminConfig::default();
        assert!(!c.scrub_schedule_enabled, "默认必须关");
        assert_eq!(c.scrub_schedule_interval_secs, 600);
        assert!(c.validate().is_ok());
    }

    /// 间隔 0(忙转)必须当场拒。
    #[test]
    fn scrub_schedule_interval_zero_rejected() {
        let bad = AdminConfig {
            scrub_schedule_interval_secs: 0,
            ..Default::default()
        };
        let err = bad.validate().unwrap_err().to_string();
        assert!(err.contains("admin.scrub_schedule_interval_secs"), "{err}");
    }
}
