CREATE TABLE IF NOT EXISTS admin_users (
    username VARCHAR(64) PRIMARY KEY,
    password_hash VARCHAR(512) NOT NULL,
    role VARCHAR(16) NOT NULL,
    created_at BIGINT NOT NULL
);

-- 会话令牌存 SHA-256 hex(库泄露不直接泄露可用令牌);username 无外键约束,
-- 查找路径走 JOIN —— 用户删则会话即失效(引用完整性由读侧保证,幂等友好)。
CREATE TABLE IF NOT EXISTS admin_sessions (
    token_hash CHAR(64) PRIMARY KEY,
    username VARCHAR(64) NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_username ON admin_sessions (username);
