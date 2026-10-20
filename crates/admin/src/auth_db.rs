//! admin 账号密码登录的 SQL 后端(`admin-auth-db`):`[[admin.users]]` 静态令牌之外的
//! 第二代准入面(两者并存,会话优先、静态兜底)。用户/口令散列/会话存独立 SQL 库
//! (postgres / mysql,配置 `[admin.auth_db]`,驱动形态对齐 S3 网关的 `[s3.metastore]`)。
//!
//! 语义要点:
//! - **`url` 空 = 特性关**(不连接,登录端点回 501,静态令牌面不受任何影响);
//! - 口令 argon2id 散列存储(盐 = uuid v4);登录对未知用户也做一次等开销哈希
//!   (防用户名枚举时序差);
//! - 会话令牌 256-bit(uuid v4 ×2 的 hex);库内存 `sha256(token)` 的 hex ——
//!   库泄露不直接泄露可用令牌;查找走 **JOIN admin_users** ⇒ 用户删除即会话失活
//!   (已过期同样视为不存在);
//! - 登录防爆破闸(`LoginGuard`):同用户名 15min 窗内 ≥5 次失败 → 锁(429);
//!   进程内、不持久化、多实例各守各的;
//! - SQL 实现只在带活库的环境活测(`TEST_ADMIN_POSTGRES_URL` / `TEST_ADMIN_MYSQL_URL`,
//!   `#[ignore]`,同 S3 网关模式);逻辑面(登录/RBAC/守卫)走 server.rs 的 Mock。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::config::{AdminAuthDbConfig, Role};

/// 会话令牌 TTL:12h(一次登录换一个,过期重登;v1 无刷新语义,刻意保持最小)。
pub const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;
/// 防爆破闸:同用户名 15min 窗内达到此次数即锁。
const LOGIN_MAX_FAILURES: usize = 5;
const LOGIN_FAILURE_WINDOW: Duration = Duration::from_secs(15 * 60);
/// 口令长度域(下限防弱口令,上限防把库列撑爆/哈希面被长度攻击滥用)。
const PASSWORD_MIN_LEN: usize = 8;
const PASSWORD_MAX_LEN: usize = 128;
/// 用户名列宽(与迁移 `admin_users.username VARCHAR(64)` 对齐;前置校验防 SQL 端报 500)。
pub const USERNAME_MAX_LEN: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum AuthDbError {
    #[error("auth db connect: {0}")]
    Connect(String),
    #[error("auth db migrate: {0}")]
    Migrate(String),
    #[error("auth db query: {0}")]
    Query(String),
}

pub type AuthResult<T> = std::result::Result<T, AuthDbError>;

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
    pub created_at: i64,
}

/// 会话(用户名 + 角色为 JOIN 产物;`expires_at_ms` 已过 → 视为不存在)。
#[derive(Debug, Clone)]
pub struct AuthSession {
    pub username: String,
    pub role: Role,
    pub expires_at_ms: i64,
}

/// auth 后端原语(login/用户管理端点的全部存储面)。
#[async_trait]
pub trait AuthStore: Send + Sync + 'static {
    async fn find_user(&self, username: &str) -> AuthResult<Option<AuthUser>>;
    async fn create_user(&self, username: &str, password_hash: &str, role: Role) -> AuthResult<()>;
    /// 返回是否真的更新(调用方 404 判空)。
    async fn set_password(&self, username: &str, password_hash: &str) -> AuthResult<bool>;
    /// 返回是否真的删了(幂等:false 不报错)。删用户后其会话必须失活 —— 由
    /// [`AuthStore::find_session`] 的 JOIN 语义与 [`AuthStore::delete_sessions_of_user`] 共同保证。
    async fn delete_user(&self, username: &str) -> AuthResult<bool>;
    async fn list_users(&self) -> AuthResult<Vec<AuthUser>>;
    async fn insert_session(
        &self,
        token_hash: &str,
        username: &str,
        created_ms: i64,
        expires_ms: i64,
    ) -> AuthResult<()>;
    /// 用户不存在(JOIN 空)或已过期 → `None`。
    async fn find_session(&self, token_hash: &str) -> AuthResult<Option<AuthSession>>;
    async fn delete_session(&self, token_hash: &str) -> AuthResult<bool>;
    async fn delete_sessions_of_user(&self, username: &str) -> AuthResult<u64>;
    async fn purge_expired_sessions(&self, now_ms: i64) -> AuthResult<u64>;
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// argon2id 哈希(PHC 字符串;盐 = uuid v4)。
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    // 16 字节恒可 b64 编码 —— 确证不变量,expect 属例外项。
    let salt = SaltString::encode_b64(&uuid::Uuid::new_v4().into_bytes())
        .expect("16-byte salt is valid b64");
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

/// 库里散列形态坏(非 PHC)也按"口令不符"处理,不 panic。
pub fn verify_password(hash: &str, password: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// 口令长度域校验(引导 CLI 与用户管理端点共用)。
pub fn validate_password(pw: &str) -> Result<(), &'static str> {
    if (PASSWORD_MIN_LEN..=PASSWORD_MAX_LEN).contains(&pw.len()) {
        Ok(())
    } else {
        Err("password must be 8..=128 chars")
    }
}

/// 256-bit 会话令牌(uuid v4 ×2 的 hex,64 字符);只在登录响应里明文出现一次。
pub fn new_session_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// 令牌的库存形态:sha256 hex(`admin_sessions.token_hash` CHAR(64))。
pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// 登录防爆破闸(进程内):同用户名 15min 窗内失败 ≥5 次 → 锁(登录端点回 429)。
/// 不持久化(重启即清)、不跨实例;多 admin 实例各守各的一层。
#[derive(Default)]
pub struct LoginGuard {
    failures: Mutex<HashMap<String, Vec<Instant>>>,
}

impl LoginGuard {
    pub fn locked(&self, username: &str) -> bool {
        let now = Instant::now();
        self.failures
            .lock()
            .expect("login guard lock poisoned")
            .get(username)
            .is_some_and(|fs| {
                fs.iter()
                    .filter(|t| now.duration_since(**t) < LOGIN_FAILURE_WINDOW)
                    .count()
                    >= LOGIN_MAX_FAILURES
            })
    }

    pub fn record_failure(&self, username: &str) {
        let now = Instant::now();
        let mut guard = self.failures.lock().expect("login guard lock poisoned");
        let fs = guard.entry(username.to_string()).or_default();
        fs.retain(|t| now.duration_since(*t) < LOGIN_FAILURE_WINDOW);
        fs.push(Instant::now());
    }

    pub fn clear(&self, username: &str) {
        self.failures
            .lock()
            .expect("login guard lock poisoned")
            .remove(username);
    }
}

/// 连接 + 建表迁移(`migrations/{postgres,mysql}/001_admin_auth.sql`)。
pub async fn connect(cfg: &AdminAuthDbConfig) -> AuthResult<Arc<dyn AuthStore>> {
    match cfg.driver.as_str() {
        "postgres" => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(cfg.max_connections)
                .connect(&cfg.url)
                .await
                .map_err(|e| AuthDbError::Connect(e.to_string()))?;
            sqlx::migrate!("migrations/postgres")
                .run(&pool)
                .await
                .map_err(|e| AuthDbError::Migrate(e.to_string()))?;
            Ok(Arc::new(PgAuthStore { pool }))
        }
        "mysql" => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(cfg.max_connections)
                .connect(&cfg.url)
                .await
                .map_err(|e| AuthDbError::Connect(e.to_string()))?;
            sqlx::migrate!("migrations/mysql")
                .run(&pool)
                .await
                .map_err(|e| AuthDbError::Migrate(e.to_string()))?;
            Ok(Arc::new(MysqlAuthStore { pool }))
        }
        other => Err(AuthDbError::Connect(format!(
            "unsupported driver `{other}`"
        ))),
    }
}

/// 库存角色形态 → 枚举:脏数据按最小权限(viewer)处理(与配置面回落一致)。
fn role_of(raw: &str) -> Role {
    Role::parse(raw).unwrap_or(Role::Viewer)
}

pub struct PgAuthStore {
    pool: sqlx::postgres::PgPool,
}

fn pg_user(row: &sqlx::postgres::PgRow) -> AuthUser {
    AuthUser {
        username: row.get("username"),
        password_hash: row.get("password_hash"),
        role: role_of(&row.get::<String, _>("role")),
        created_at: row.get("created_at"),
    }
}

#[async_trait]
impl AuthStore for PgAuthStore {
    async fn find_user(&self, username: &str) -> AuthResult<Option<AuthUser>> {
        let row = sqlx::query(
            "SELECT username, password_hash, role, created_at FROM admin_users WHERE username = $1",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(row.as_ref().map(pg_user))
    }

    async fn create_user(&self, username: &str, password_hash: &str, role: Role) -> AuthResult<()> {
        sqlx::query(
            "INSERT INTO admin_users (username, password_hash, role, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(role.as_str())
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(())
    }

    async fn set_password(&self, username: &str, password_hash: &str) -> AuthResult<bool> {
        let r = sqlx::query("UPDATE admin_users SET password_hash = $2 WHERE username = $1")
            .bind(username)
            .bind(password_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn delete_user(&self, username: &str) -> AuthResult<bool> {
        let r = sqlx::query("DELETE FROM admin_users WHERE username = $1")
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn list_users(&self) -> AuthResult<Vec<AuthUser>> {
        let rows = sqlx::query(
            "SELECT username, password_hash, role, created_at FROM admin_users ORDER BY username",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(rows.iter().map(pg_user).collect())
    }

    async fn insert_session(
        &self,
        token_hash: &str,
        username: &str,
        created_ms: i64,
        expires_ms: i64,
    ) -> AuthResult<()> {
        sqlx::query(
            "INSERT INTO admin_sessions (token_hash, username, created_at, expires_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(token_hash)
        .bind(username)
        .bind(created_ms)
        .bind(expires_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(())
    }

    async fn find_session(&self, token_hash: &str) -> AuthResult<Option<AuthSession>> {
        let row = sqlx::query(
            "SELECT s.username, u.role, s.expires_at \
             FROM admin_sessions s JOIN admin_users u ON u.username = s.username \
             WHERE s.token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        let s = row.map(|r| AuthSession {
            username: r.get("username"),
            role: role_of(&r.get::<String, _>("role")),
            expires_at_ms: r.get("expires_at"),
        });
        Ok(s.filter(|sess| sess.expires_at_ms > now_ms()))
    }

    async fn delete_session(&self, token_hash: &str) -> AuthResult<bool> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE token_hash = $1")
            .bind(token_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn delete_sessions_of_user(&self, username: &str) -> AuthResult<u64> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE username = $1")
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected())
    }

    async fn purge_expired_sessions(&self, cutoff_ms: i64) -> AuthResult<u64> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE expires_at <= $1")
            .bind(cutoff_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected())
    }
}

pub struct MysqlAuthStore {
    pool: sqlx::mysql::MySqlPool,
}

fn mysql_user(row: &sqlx::mysql::MySqlRow) -> AuthUser {
    AuthUser {
        username: row.get("username"),
        password_hash: row.get("password_hash"),
        role: role_of(&row.get::<String, _>("role")),
        created_at: row.get("created_at"),
    }
}

#[async_trait]
impl AuthStore for MysqlAuthStore {
    async fn find_user(&self, username: &str) -> AuthResult<Option<AuthUser>> {
        let row = sqlx::query(
            "SELECT username, password_hash, role, created_at FROM admin_users WHERE username = ?",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(row.as_ref().map(mysql_user))
    }

    async fn create_user(&self, username: &str, password_hash: &str, role: Role) -> AuthResult<()> {
        sqlx::query(
            "INSERT INTO admin_users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(role.as_str())
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(())
    }

    async fn set_password(&self, username: &str, password_hash: &str) -> AuthResult<bool> {
        let r = sqlx::query("UPDATE admin_users SET password_hash = ? WHERE username = ?")
            .bind(password_hash)
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn delete_user(&self, username: &str) -> AuthResult<bool> {
        let r = sqlx::query("DELETE FROM admin_users WHERE username = ?")
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn list_users(&self) -> AuthResult<Vec<AuthUser>> {
        let rows = sqlx::query(
            "SELECT username, password_hash, role, created_at FROM admin_users ORDER BY username",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(rows.iter().map(mysql_user).collect())
    }

    async fn insert_session(
        &self,
        token_hash: &str,
        username: &str,
        created_ms: i64,
        expires_ms: i64,
    ) -> AuthResult<()> {
        sqlx::query(
            "INSERT INTO admin_sessions (token_hash, username, created_at, expires_at) VALUES (?, ?, ?, ?)",
        )
        .bind(token_hash)
        .bind(username)
        .bind(created_ms)
        .bind(expires_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(())
    }

    async fn find_session(&self, token_hash: &str) -> AuthResult<Option<AuthSession>> {
        let row = sqlx::query(
            "SELECT s.username, u.role, s.expires_at \
             FROM admin_sessions s JOIN admin_users u ON u.username = s.username \
             WHERE s.token_hash = ?",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthDbError::Query(e.to_string()))?;
        let s = row.map(|r| AuthSession {
            username: r.get("username"),
            role: role_of(&r.get::<String, _>("role")),
            expires_at_ms: r.get("expires_at"),
        });
        Ok(s.filter(|sess| sess.expires_at_ms > now_ms()))
    }

    async fn delete_session(&self, token_hash: &str) -> AuthResult<bool> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE token_hash = ?")
            .bind(token_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected() > 0)
    }

    async fn delete_sessions_of_user(&self, username: &str) -> AuthResult<u64> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE username = ?")
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected())
    }

    async fn purge_expired_sessions(&self, cutoff_ms: i64) -> AuthResult<u64> {
        let r = sqlx::query("DELETE FROM admin_sessions WHERE expires_at <= ?")
            .bind(cutoff_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthDbError::Query(e.to_string()))?;
        Ok(r.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_roundtrip_and_reject() {
        let hash = hash_password("correct-horse").expect("hash");
        assert!(verify_password(&hash, "correct-horse"));
        assert!(!verify_password(&hash, "wrong-horse"));
        // 非 PHC 形态(库存坏行)按"口令不符"处理,不 panic。
        assert!(!verify_password("not-a-phc-hash", "correct-horse"));
    }

    #[test]
    fn session_token_shape_unique_and_hashed() {
        let a = new_session_token();
        let b = new_session_token();
        assert_eq!(a.len(), 64, "uuid×2 的 hex");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
        // 摘要:确定性、定长、跨令牌不同。
        assert_eq!(token_hash(&a).len(), 64);
        assert_eq!(token_hash(&a), token_hash(&a));
        assert_ne!(token_hash(&a), token_hash(&b));
    }

    #[test]
    fn login_guard_lockout_and_window_pruning() {
        let g = LoginGuard::default();
        for _ in 0..LOGIN_MAX_FAILURES {
            assert!(!g.locked("u1"));
            g.record_failure("u1");
        }
        assert!(g.locked("u1"), "窗内满 5 次即锁");
        g.clear("u1");
        assert!(!g.locked("u1"), "成功登录清零");

        // 过窗失败不计数:直接倒拨既有失败时刻(同模块入私域;免 sleep,CLAUDE.md §5.6)。
        g.record_failure("u2");
        g.record_failure("u2");
        let old = Instant::now() - LOGIN_FAILURE_WINDOW - Duration::from_secs(1);
        if let Some(fs) = g.failures.lock().unwrap().get_mut("u2") {
            fs.iter_mut().for_each(|t| *t = old);
        }
        assert!(!g.locked("u2"), "过窗失败不再计数");
        g.record_failure("u2");
        assert!(!g.locked("u2"), "过窗失败被裁剪,单次新鲜失败不到阈值");
    }

    #[test]
    fn password_bounds_enforced() {
        assert!(validate_password("12345678").is_ok());
        assert_eq!(
            validate_password("1234567"),
            Err("password must be 8..=128 chars")
        );
        assert_eq!(
            validate_password(&"x".repeat(129)),
            Err("password must be 8..=128 chars")
        );
    }

    // ── 活库面(pg / mysql;#[ignore] + TEST_ADMIN_*_URL,同 S3 网关模式)──────

    fn env_cfg(var: &str, driver: &str) -> Option<AdminAuthDbConfig> {
        std::env::var(var).ok().map(|url| AdminAuthDbConfig {
            driver: driver.to_string(),
            url,
            max_connections: 2,
        })
    }

    /// 建号 → 查号 → 验密 → 会话(活/过期)→ 改密 → 删号后会话失活 的全生命周期。
    async fn exercise_live_store(store: &dyn AuthStore) {
        let name = format!("t-{}", uuid::Uuid::new_v4().simple());
        let hash = hash_password("live-pw-123456").expect("hash");
        store
            .create_user(&name, &hash, Role::Operator)
            .await
            .expect("create");
        let u = store.find_user(&name).await.expect("find").expect("user");
        assert!(verify_password(&u.password_hash, "live-pw-123456"));
        assert_eq!(u.role, Role::Operator);

        let token = new_session_token();
        let stash = token_hash(&token);
        let created = now_ms();
        store
            .insert_session(&stash, &name, created, created + SESSION_TTL_MS)
            .await
            .expect("insert session");
        let s = store
            .find_session(&stash)
            .await
            .expect("find session")
            .expect("session live");
        assert_eq!(s.username, name);
        assert_eq!(s.role, Role::Operator);

        // 过期会话查无(即便行还在)。
        let dead_token = token_hash(&new_session_token());
        store
            .insert_session(
                &dead_token,
                &name,
                now_ms() - 2 * SESSION_TTL_MS,
                now_ms() - 1000,
            )
            .await
            .expect("insert expired session");
        assert!(store
            .find_session(&dead_token)
            .await
            .expect("find expired")
            .is_none());
        assert!(store.purge_expired_sessions(now_ms()).await.expect("purge") >= 1);

        // 改密:旧口令失效、新口令过验。
        let second = hash_password("second-live-789").expect("hash");
        assert!(store
            .set_password(&name, &second)
            .await
            .expect("set password"));
        let u2 = store
            .find_user(&name)
            .await
            .expect("find2")
            .expect("present");
        assert!(verify_password(&u2.password_hash, "second-live-789"));
        assert!(!verify_password(&u2.password_hash, "live-pw-123456"));

        // 删号:会话行被连带清;即便残留行,JOIN 语义也使会话失效。
        assert!(store.delete_user(&name).await.expect("delete user"));
        assert!(store
            .find_session(&stash)
            .await
            .expect("post-delete session")
            .is_none());
        assert!(store
            .find_user(&name)
            .await
            .expect("post-delete user")
            .is_none());
    }

    #[tokio::test]
    #[ignore = "needs TEST_ADMIN_POSTGRES_URL with a live database"]
    async fn live_pg_user_session_lifecycle() {
        let cfg =
            env_cfg("TEST_ADMIN_POSTGRES_URL", "postgres").expect("TEST_ADMIN_POSTGRES_URL unset");
        let store = connect(&cfg).await.expect("connect + migrate");
        exercise_live_store(store.as_ref()).await;
    }

    #[tokio::test]
    #[ignore = "needs TEST_ADMIN_MYSQL_URL with a live database"]
    async fn live_mysql_user_session_lifecycle() {
        let cfg = env_cfg("TEST_ADMIN_MYSQL_URL", "mysql").expect("TEST_ADMIN_MYSQL_URL unset");
        let store = connect(&cfg).await.expect("connect + migrate");
        exercise_live_store(store.as_ref()).await;
    }
}
