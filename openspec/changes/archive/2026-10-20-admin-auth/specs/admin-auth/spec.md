# admin-auth 规约(admin-auth-db)

## ADDED Requirements

### Requirement: admin 账号密码登录与用户管理(独立 SQL 后端)
admin web SHALL 支持以独立 SQL 库(postgres / mysql,配置 `[admin.auth_db]`,形态对齐 `[s3.metastore]`)承载账号密码准入:`admin.auth_db.url` 为空 SHALL 视为特性关(`POST /api/login` 回 501,既有静态令牌面行为不变)。登录(`POST /api/login`,免 Bearer)SHALL 验证 argon2id 口令散列并对通过者签发 12 小时会话令牌(256-bit,库存仅存其 SHA-256 摘要,登录失败对未知用户 SHALL 做等开销哈希);同一用户名 15 分钟窗内 5 次失败 SHALL 被 429 加 Retry-After 拒绝且锁判定先于验密。会话解析 SHALL 经用户表 JOIN 取角色,用户删除 SHALL 使其全部会话立即失活;令牌解析 SHALL 会话库优先、DB 故障时静默回落静态令牌面。用户名册与用户增删改密端点 SHALL 仅 operator 可用;登录/登出/用户管理 SHALL 纳入既有 RBAC 与审计中间件链(失败登录记 anonymous)。冷启动 SHALL 经 `--set-admin-password` 离线引导(stdin 读口令,存在改密/不存在建号)。

#### Scenario: 登录签发会话并可访问受保护端点
- **WHEN** 有效用户名口令登录并以返回令牌访问 `/api/me`
- **THEN** 登录 200 返回 `{token, username, role, expires_at_ms}`,后续 Bearer 请求以会话身份通过 RBAC

#### Scenario: 错误口令与防爆破锁定
- **WHEN** 同一用户名连续五次提交错误口令后再次登录(即便口令正确)
- **THEN** 前五次 401;第六次 429 且带 Retry-After

#### Scenario: 删号即会话失活
- **WHEN** operator 删除某用户后,该用户既有会话令牌再次请求
- **THEN** 鉴权失败 401(JOIN 语义,无需等待 TTL)

#### Scenario: 特性关为零行为
- **WHEN** `admin.auth_db.url` 为空
- **THEN** 登录端点 501,静态令牌面与关校验语义与该特性引入前完全一致
