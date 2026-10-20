# Tasks: admin-auth-db

- [x] 1. 迁移 001(pg/mysql):admin_users + admin_sessions(token_hash 索引)
- [x] 2. `auth_db.rs`:AuthStore trait + 双驱动 + argon2id + LoginGuard + 单测
- [x] 3. `[admin.auth_db]` 配置(url 空 = 特性关)+ 校验
- [x] 4. 端点六条(login 防爆破 + 12h 会话 / logout / users 四路由)+ RBAC/审计融入 + 路由测试
- [x] 5. 引导 CLI `--set-admin-password` + devcluster 无损(内嵌网关保持静态面)
- [x] 6. 指标 + 文档(cluster.md §11.1/§11.2/§11.3)+ DEVLOG 89 + spec/validate/archive + 单 commit + CI

## 落地记录(已回填)

- antfs-admin lib **83/0 + 2 ignored**(71 + 12 新:auth_db 4 + server 8;
  ignored = 活库 pg/mysql lifecycle,`TEST_ADMIN_*_URL` 同 S3 模式);
- **workspace 77 套 806/0;clippy 0;fmt 净**;
- 实现注记:锁判定先于验密(防"试对绕闸");改口令不吊销既有会话(处置 = 删号
  重建,记档);会话库存形态 = sha256 摘要(库泄露不直接泄露可用令牌)。
