## Why

live spec(`s3-metastore`「Soft-delete with cleanup」)承诺的后台 GC 从未实现,当前网关有三面真实泄漏:
1. **软删行堆积**:DeleteObject 把行标 `deleted_at` 后**永不清行**——`named_objects` 无限膨胀;
2. **孤儿 multipart**:create→upload parts 后无人 Complete/Abort —— parts blob(W 副本)+ 两张 session 表行**永久泄漏**;
3. **overwrite 陈版泄漏(最重,新发现)**:`put_object` 纯 INSERT,新版本靠 `ORDER BY version DESC` **遮蔽**老 READY 行;老行的 blob **没有任何路径会删**(spec 的"old blob becomes reclaimable by GC"从未落地)——越频繁 overwrite 泄漏越快。

生产可运营性(盘/库膨胀)与 spec 兑现双重欠账,必须收口。

## What Changes

- **Overwrite 即刻回收(对齐 S3 语义:无版本化 overwrite = 老数据立即可弃)**:`put_object`(含 multipart Complete 落行)在同事务内将被遮蔽的同 key 老 READY 行标 `state=0, deleted_at=now`,返回被遮蔽 blob_id 集给 handler,handler 逐个 `router.delete`(重复删幂等,墓碑恒胜)。泄漏面 3 关闭,GC 不欠新账。
- **后台 GC 循环(网关进程内,周期可配)**:
  - **行回收**:删 `state=0 AND deleted_at < now − retention`(retention 可配,默认 24h)的行——blob 在删除/遮蔽时**已**删,GC 只清行(修正 live spec 与实现的漂移措辞:GC 不再"删孤儿 blob",blob 的死点前移到语义动作时);
  - **孤儿 multipart 回收**:`created_at < now − upload_ttl`(默认 72h)的未 Complete session → 复用 `abort_multipart` 语义(逐片 `router.delete` + 删除 session 行)。
- **多实例安全**:无状态网关可水平扩,GC 并发 = 多实例同时扫——**原子取件**(`UPDATE ... RETURNING` / `DELETE ... RETURNING` 行级锁)保证行只被一实例处理一次;blob 重复 delete 靠引擎墓碑幂等兜底(按设计安全)。
- **契约**:新增配置 3 键(`s3.gc.*`)+ GC 指标族;零 REST/S3 API 面变化(纯后台行为)。
- 修 spec 漂移:`s3-metastore` 的「Soft-delete with cleanup」requirement 按实现现实改写(blob 死点前移)。

## Capabilities

### New Capabilities
- `s3-gc`:S3 网关后台垃圾回收 —— 软删行/孤儿 multipart/遮蔽陈版的成对回收语义、可配保留期与收集指标。

### Modified Capabilities
- `s3-metastore`:「Soft-delete with cleanup」requirement 修正 — GC 清行、blob 在删除/遮蔽时已即刻删除(实现与 spec 漂移回写);`put_object` 增加遮蔽行回标记与返回值。
- `s3-core`:PutObject overwrite 场景补「旧版本即刻进入回收」的语义(不再是"reclaimable by GC"的空话)。
- `s3-multipart`:Complete 后遗留遮蔽行同 put_object 语义;孤儿 session 有 TTL 回收。

## Impact

- **crate**:`crates/s3`(metastore trait +4 方法、pg/mysql 方言、handlers put/delete 接线、新 `gc.rs` 循环、config、metrics)。
- **契约**:无 API 变化;新增 `PutObjectMeta` 路径返回值(库内);配置表 +3 键、监控 +1 族(CLAUDE.md §7 同步 s3.md)。
- **风险面**:GC 误删 = 数据丢失红面 → 原子取件 + retention 双闸 + PG 真机集成测试(软删行保留期内不回收/过期回收/孤儿 upload TTL/多实例并发不双删/幂等重入)。
- **不涉**:blob 引擎、REST frontend、devcluster 之外的部分;MySQL 方言 SQL 与 PG 平行实现(验证边界沿现状:仅 PG 进 CI 门)。

## Non-goals

- 不做版本化保留(AWS S3 versioning 语义另立 change,届时 GC 语义会随版本化重定义)。
- 不做 GC 的 admin REST 触发端点(后台循环足矣,运维触发将来需要再加)。
- 不处理 frontend `/stitch` 孤儿片族(已登记 frontend.md 缺口 4,另一战场)。
