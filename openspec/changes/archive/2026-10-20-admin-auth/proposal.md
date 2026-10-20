# Proposal: admin-auth-db — admin 账号密码登录(独立 SQL 后端)

## Why

admin web 的准入此前只有**静态令牌面**(`[[admin.users]]` name/token/role +
operator/viewer_token,全空 = 关校验):令牌即口令、无轮换/吊销/可审计的账号语义,
多运维员共享长期令牌。所有者指示:账号密码 auth 接**独立 SQL 库**(地址单独配置,
与 `[s3.metastore]` 同形态,支持 postgres / mysql)。

## What Changes

| 位置 | 改动 |
|---|---|
| `auth_db.rs`(新) | `AuthStore` trait(用户/会话原语)+ Pg/Mysql 实现 + argon2id 哈希 + `LoginGuard`(15min 窗 5 败锁) |
| `migrations/{postgres,mysql}/001_admin_auth.sql` | `admin_users` + `admin_sessions`(token_hash = sha256 hex)|
| `config.rs` | `[admin.auth_db]`(driver/url/max_connections;**url 空 = 特性关**) |
| `server.rs` | `POST /api/login`(公开、防爆破、12h 会话)/ `POST /api/logout` / `GET\|POST /api/users` / `PUT .../password` / `DELETE .../{u}`;解析序 = 会话库优先 → DB 故障回落静态 → 皆空 = 关校验 |
| `main.rs` | 冷启动引导 `--set-admin-password <u> [role]`(stdin 口令;存在改密/不存在建号) |
| metrics | `admin_logins_total{result}`(success/invalid/locked) |
| 测试 | lib +12(逻辑面 MockAuthStore × 真 argon2;守卫窗免 sleep)/ 活库 lifecycle ×2 `#[ignore]` |

## Impact

- 与静态令牌面**并存**(会话优先;DB 故障静默回落静态面 —— 可用性不与 DB 耦合);
- 口令 argon2id;库存只存 sha256(会话令牌);**删用户即会话失活**(JOIN);改口令
  ≠ 吊销既有会话(TTL 自然到期;处置路径记档);
- 共享 SQL 库的多实例部署:会话**跨实例有效**(库存摘要,非进程态);防爆破闸进程
  内各守一层;
- devcluster 保持静态面不变(本地开发形态)。

## Out of Scope

- 登出后滑动刷新/续期(v1 刻意无);
- 改口令吊销既有会话(语义取舍:记档,处置 = 删号重建);
- 每实例登录.scope 之外的限流(进程内按用户名闸已覆盖)。
