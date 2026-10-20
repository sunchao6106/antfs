//! admin web 二进制入口:加载配置 → 连 master → 起 HTTP 服务。

use std::sync::Arc;

use antfs_admin::audit::AuditLog;
use antfs_admin::config::AdminConfig;
use antfs_admin::master_api::GrpcMasterApi;
use antfs_admin::metrics::AdminMetrics;
use antfs_admin::server::{build_router, AppState, AuthConfig};
use antfs_config::Validate;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    // 离线引导:`antfs-admin --set-admin-password <username> [role]`(口令经 stdin 读入,
    // 不落配置文件);完成后退出,不启服务 —— 账号密码登录的冷启动路径。
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() >= 3 && argv[1] == "--set-admin-password" {
        let role = argv.get(3).map(String::as_str).unwrap_or("operator");
        return set_admin_password(&argv[2], role).await;
    }

    let cfg: AdminConfig = antfs_config::Loader::new()
        .with_file("admin.toml", false)
        .with_env_prefix("AMBRY")
        .load_table("admin")
        .unwrap_or_default();
    cfg.validate()?;

    // 账号密码登录后端(admin-auth-db):空 url = 特性关(None,登录端点回 501)。
    let auth_db = if cfg.auth_db.url.is_empty() {
        None
    } else {
        Some(antfs_admin::auth_db::connect(&cfg.auth_db).await?)
    };

    let registry = antfs_metric::Registry::new();
    let metrics = Arc::new(AdminMetrics::new(&registry)?);
    let master =
        Arc::new(GrpcMasterApi::connect(&cfg.master_addr, cfg.cluster_secret.clone()).await?);

    let state = AppState {
        master,
        metrics,
        registry,
        grafana_url: cfg.grafana_url.clone(),
        // 打不开也只 warn(审计降级),不阻断启动。
        audit: Arc::new(AuditLog::open(&cfg.audit_path)),
        scrub_runs: std::sync::Arc::new(antfs_admin::scrub::ScrubRuns::default()),
        scrub_schedule_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        auth_db,
        login_guard: Arc::new(antfs_admin::auth_db::LoginGuard::default()),
    };
    if cfg.scrub_schedule_enabled {
        antfs_admin::scrub_schedule::spawn_scrub_schedule(
            state.clone(),
            cfg.scrub_schedule_interval_secs,
        );
        tracing::info!(
            interval_secs = cfg.scrub_schedule_interval_secs,
            "scheduled replica-comparison scrub enabled (shallow, one partition per tick)"
        );
    }
    let auth = AuthConfig {
        users: cfg.users.clone(),
        operator_token: cfg.operator_token.clone(),
        viewer_token: cfg.viewer_token.clone(),
    };
    let router = build_router(state, auth);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(addr = %cfg.listen_addr, master = %cfg.master_addr, "admin web listening");
    // 带 connect info 以记录审计的 remote_addr。
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// `--set-admin-password`:引导首个/重置既有用户口令。存在即改密(角色不变);
/// 不存在则按给定角色建(默认 operator)。口令从 stdin 读(管道喂入,勿在交互终端
/// 明文回显);成功后仅 stdout 回执,口令本身绝不外泄。
async fn set_admin_password(
    username: &str,
    role: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg: AdminConfig = antfs_config::Loader::new()
        .with_file("admin.toml", false)
        .with_env_prefix("AMBRY")
        .load_table("admin")
        .unwrap_or_default();
    cfg.validate()?;
    if cfg.auth_db.url.is_empty() {
        return Err("admin.auth_db.url is empty; configure it before bootstrapping users".into());
    }
    let role = antfs_admin::config::Role::parse(role)
        .ok_or_else(|| format!("role must be `operator` or `viewer` (got `{role}`)"))?;
    if username.is_empty() || username.len() > antfs_admin::auth_db::USERNAME_MAX_LEN {
        return Err("username must be 1..=64 chars".into());
    }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let password = line.trim_end_matches(['\r', '\n']);
    antfs_admin::auth_db::validate_password(password)?;
    let db = antfs_admin::auth_db::connect(&cfg.auth_db).await?;
    let hash = antfs_admin::auth_db::hash_password(password).map_err(|e| format!("hash: {e}"))?;
    match db.find_user(username).await? {
        Some(_) => {
            db.set_password(username, &hash).await?;
            println!("password updated for user `{username}`");
        }
        None => {
            db.create_user(username, &hash, role).await?;
            println!("user `{}` created with role `{}`", username, role.as_str());
        }
    }
    Ok(())
}
