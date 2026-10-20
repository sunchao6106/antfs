# Design: admin-auth-db

## D1 两面并存与逃生口

- 令牌解析序:**会话库**(JOIN 取角色)→ DB 故障**静默回落静态令牌面**(它独立于
  DB)→ 静态三闸原序;静态面与 auth_db 皆空且未配置 = 关校验(`local`),零变;
- `url` 空 = 登录端点 501(明告"未配置"),其余面行为零差。

## D2 口令与会话的持久形态

- 口令 argon2id(PHC 字符串,盐 = uuid v4);**未知用户也做一次等开销哈希**(防
  用户名枚举时序);库存非 PHC 坏行按"口令不符"处理(不 panic);
- 会话令牌 256-bit(uuid ×2 hex),只在登录响应出现一次;库存 `sha256(令牌)`
  hex(CHAR(64))—— 库泄露不直接泄露可用令牌;
- TTL 12h 常量;过期清理**搭登录路径顺带 DELETE**(登录低频,无周期任务);
- 会话查找 JOIN `admin_users` ⇒ **删号即失活**(改口令不吊销,proposal 记档)。

## D3 防爆破闸

`LoginGuard`:进程内 `Mutex<HashMap<username, Vec<Instant>>>`,15min 窗 5 次失败 →
锁(429 + Retry-After: 60);**锁判定先于验密**(正确口令在锁窗内同样被拒 ——
爆破者不能靠"试对"绕闸);记录时顺手裁剪过窗项(容量有界);成功登录清零。

## D4 端点与 RBAC/审计融入

- login = **唯一免 Bearer 端点**(`require_auth` 路径特判);logout = 已登录任意
  角色(viewer 写闸豁免 —— 吊销自己的会话不是写操作);
- `/api/users*` 全链 operator 专属(名册只读 + 建删改密写);
- 登录/登出/用户管理自动走既有审计中间件(写操作面);失败登录记 actor=anonymous。

## D5 冷启动引导

`--set-admin-password <u> [role]`:argv 分支(读同一 admin.toml),连库跑迁移,
stdin 读口令(管道形态;8..=128 校验),upsert(存在改密/不存在建号),退 0。
明文口令不落任何持久位置。

## D6 测试

- 单测:argon2 轮转/令牌形状(64 hex、摘要确定)/LoginGuard 窗(直接倒拨失败
  时刻,免 sleep §5.6)/口令域;
- 路由测试:MockAuthStore(内存 users/sessions,JOIN 语义等价)× **真 argon2 验密
  路径** —— 登录发会话、401、429+Retry-After、501(关校验面照旧)、logout 吊销、
  viewer 门控、建号-登录-删号失活、指标实例断言(§6);
- 活库:pq/mysql lifecycle `#[ignore]`(`TEST_ADMIN_*_URL`)。

## Risks / Rollback

- 回滚 = revert 单 commit;`url` 空 = 且用且退的逃生口。
