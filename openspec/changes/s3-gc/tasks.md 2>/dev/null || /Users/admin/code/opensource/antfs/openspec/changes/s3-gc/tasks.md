## 1. MetaStore 层(trait + 双方言)

- [ ] 1.1 `put_object` / `complete_multipart`:落行后**同事务序列**遮蔽旧活行(`state=0, deleted_at=now`,条件 `version < new_version`),返回被遮蔽 blob_id 列表(PG/MySQL 方言各一;`PutObjectMeta` 返回值升级)
- [ ] 1.2 `purge_expired_deletions(cutoff, limit) -> 行数`:原子 `DELETE ... RETURNING`/`DELETE LIMIT`(PG 子查询取件与 MySQL 方言分支,多实例取件安全)
- [ ] 1.3 `take_expired_uploads(cutoff, limit) -> Vec<upload_id>`:原子删 `multipart_uploads` 行取件(RETURNING),parts 处理由拿到者走既有 `abort_multipart`

## 2. Handler 接线 + GC 循环

- [ ] 2.1 put/delete handlers(含 multipart complete)对遮蔽/删除 blob 逐个 `router.delete`(幂等)
- [ ] 2.2 新 `gc.rs`:周期循环(interval 默认 300s;`GC_BATCH_ROWS=1000`/`GC_BATCH_UPLOADS=100` 常量+背压注释);行清理、孤儿 session 走 abort 语义;单行失败计指标续跑,循环不 panic
- [ ] 2.3 启动接线:devcluster 内嵌 + 独立二进制 main 各自 spawn(`enabled` 默认 true)

## 3. 配置/监控/文档(§7)

- [ ] 3.1 `S3GatewayConfig` 嵌入 `gc{enabled, interval_secs, deletion_retention_secs, upload_ttl_secs}` + `Validate`(retention ≥ 1、interval ≥ 1)
- [ ] 3.2 metrics:`s3_gc_runs_total`/`s3_gc_purged_rows_total`/`s3_gc_orphan_uploads_total`/`s3_gc_orphan_parts_blobs_total`/`s3_gc_duration_seconds`/`s3_gc_errors_total{kind}`
- [ ] 3.3 s3.md:配置项表 +4 行、监控项表 +1 族、操作映射补「后台 GC」一段(行为/默认/多实例安全)
- [ ] 3.4 live spec 漂移修正:s3-metastore「Soft-delete with cleanup」改写(GC 清行;blob 死点=删除/遮蔽时);完成 change 归档时走 MODIFIED

## 4. 测试(PG 真机,套 `#[ignore]` 仓例)

- [ ] 4.1 软删行:保留期内不动 / 过期清行且计数;孤儿 multipart:TTL 外整session回收(断言 parts blob 走墓碑)/ TTL 内不动
- [ ] 4.2 overwrite:put 两次同 key → 老行 state=0、老 blob 在集群被删(blobId GET 404);multipart complete 覆盖同径
- [ ] 4.3 并发取件:两个连接并发 `purge_expired_deletions` 同批行,总行数守恒(无双重 RETURNING)
- [ ] 4.4 既有全套(`cargo test -p antfs-s3`)+ workspace + fmt/clippy 全绿;推送后 CI 绿

## 5. 收口

- [ ] 5.1 DEVLOG 步骤 47(含三面泄漏的发现链);BASELINE 无涉;commit 分两笔(handlers+metastore / gc loop+docs)或单笔按 diff 聚拢
