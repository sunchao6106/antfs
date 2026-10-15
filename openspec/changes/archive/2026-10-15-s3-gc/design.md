## Context

三面泄漏(软删行堆积 / 孤儿 multipart / overwrite 陈版遮蔽泄漏)见 proposal。网关无状态可多实例,GC 必须假设并发;blob 引擎 delete 幂等(墓碑恒胜)。现 schema:`named_objects.state`(1=READY,0 将用作已遮蔽/已删)、`deleted_at`;`multipart_uploads/​multipart_parts` 无 TTL 字段(created_at 可作判据)。retention 语义参照 AWS(用户不可见,防误删的运营缓冲)。

## Goals / Non-Goals

**Goals:** 零新账(overwrite 即刻回收)+ 清旧账(行 GC、孤儿 multipart TTL)三面关闭;多实例并发安全;§7 配置/监控/文档同步。
**Non-Goals:** 版本化保留;admin 触发端点;frontend stitch 孤儿片。

## Decisions

### D1. Overwrite 即刻回收,不走懒 GC

`put_object`/`complete_multipart` 落行时同语句序列里:
```sql
UPDATE named_objects SET state = 0, deleted_at = $now
WHERE bucket=$b AND key=$k AND state = 1 AND deleted_at IS NULL
  AND version < $new_version
RETURNING blob_id
```
返回的 blob_id 集交 handler 逐个 `router.delete`。理由:
- S3 无版本化语义 = overwrite 后老数据立即可弃(AWS 同);泄漏窗口最小化;
- 行级 UPDATE 命中 ≤ 数行(同 key 活行通常 1),热路径代价一次索引扫描;
- **否决懒回收**(GC 周期发现遮蔽行再删 blob):泄漏周期=GC 间隔,且需要引入"遮蔽但 blob 未删"的中间行状态与崩溃恢复协议,复杂度不值。

### D2. blob 死点前移,GC 只做行清理与孤儿回收

DELETE 与 overwrite 的 blob 都在**语义动作时**即刻 `router.delete`(现状 DELETE 已是如此);GC 循环只剩:
- `DELETE FROM named_objects WHERE state=0 AND deleted_at < $cutoff [LIMIT n] RETURNING (行——无需再碰 blob)`;
- 孤儿 multipart: `SELECT ... FROM multipart_uploads WHERE created_at < $cutoff` →
  逐 session 走既有 `abort_multipart`(其内含逐片 `router.delete` + 两表清理)。
修 live spec 措辞漂移(GC 不再"删孤儿 blob")。理由:blob 早死 = 早释放、语义单一、崩溃窗口最小。

### D3. 多实例安全 = 原子取件 + 引擎幂等兜底

- 行删除/GC:`DELETE ... LIMIT`+RETURNING 单语句原子(MySQL 侧同名次序);两实例并发扫同一行,只有一方拿 RETURNING;
- 孤儿 session 回收:取件用 `DELETE FROM multipart_uploads WHERE upload_id IN (候选) RETURNING ...`?multipart_uploads 主键单行 `DELETE ... RETURNING`,parts/blob 由**拿到 session 的实例**独占处理——没拿到的实例连行都看不到(已删);
- **blob 重复删**(极端交叠窗):引擎 delete 幂等(墓碑第二写恒胜),按既有伪重复安全设计兜底,不需分布式锁。
- **触发节拍**:每实例各自 `interval` 周期跑(默认 300s);不需要 leader 选举(取件原子性使多跑无害、只是空扫)。

### D4. 配置三键(§7 清单同步 s3.md)

| key | 型 | 默认 | 备注 |
|---|---|---|---|
| `s3.gc.enabled` | bool | `true` | 循环开关(关=纯手动运维形态) |
| `s3.gc.interval_secs` | u64 | 300 | 每实例扫描周期 |
| `s3.gc.deletion_retention_secs` | u64 | 86400(24h) | 软删行保留期(防误删缓冲;不可为 0) |
| `s3.gc.upload_ttl_secs` | u64 | 259200(72h) | 未 Complete multipart session TTL |

metrics:`s3_gc_runs_total`、`s3_gc_purged_rows_total`、`s3_gc_orphan_uploads_total`、
`s3_gc_orphan_parts_blobs_total`、`s3_gc_duration_seconds`(histogram)、错误入
`s3_gc_errors_total{kind}`。

### D5. 批量与背压

GC 每轮至多处理 `GC_BATCH_ROWS = 1000` 行 / `GC_BATCH_UPLOADS = 100` session(常量,防
长事务与一次海量 router.delete 打爆盘池); влаг不改配置,常数 + 注释。轮内失败(单行/单
session)记指标后继续,循环永不 panic。

## Risks / Trade-offs

- **[误删红面]** retention/双闸(24h/72h)+ 原子取件 + PG 集成测试覆盖(保留期内不回收、过期才回收);误配 retention=0 被 `Validate` 拒绝官方下限。
- **[覆盖率式漂移:blob 已死但行还在、GC 停摆]** 进度可视化靠 `s3_gc_*` 指标(admin 抓 `/metrics` 即可发现 GC 停摆——空转可见性)。
- **[overwrite 热路径增本]** 一次 UPDATE 索引扫描(命中=同 key 活行,通常 1);bench 主场景 4KiB×84MB/s 面积,量级无感;以 PG 集成测试保护正确性。
- **[MySQL 方言差异]** `DELETE ... LIMIT` MySQL 支持/PG 需 `ctid` 或子查询取件——按方言分支实现 + 各自 SQL 测试(验证边界沿现状:CI 仅 gate PG)。

## Migration Plan

零 schema 变更(复用 state/deleted_at/created_at 既有列)。部署顺序任意;回滚 = 关 `s3.gc.enabled`
(积压不清但不再误动作)。

## Open Questions

无(retention 默认值属常规运营参数,§7 表落定后可调)。
