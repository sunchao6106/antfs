
## 1. MetaStore 层(trait + 双方言)

- [x] 1.1 `put_object` / `complete_multipart`:落行后**同事务序列**遮蔽旧活行(`state=0, deleted_at=now`,`version < new_version`),返回被遮蔽 blob_id 列表(PG `UPDATE...RETURNING` / MySQL 先圈后改两步幂等)
- [x] 1.2 `purge_expired_deletions(cutoff, limit) -> 行数`:原子 `DELETE`(PG `ctid` 子查询 + **`FOR UPDATE SKIP LOCKED`**(修同快照同批取件竞态)、MySQL `DELETE ORDER BY LIMIT`)
- [x] 1.3 `take_expired_uploads(cutoff, limit) -> Vec<upload_id>`:原子删 `multipart_uploads` 取件(RETURNING / MySQL 两步)+ `delete_parts_of_session` 原语(take 后不能复用 `abort_multipart` 的存在性前置)

## 2. Handler 接线 + GC 循环

- [x] 2.1 put/delete handlers(含 multipart complete)对遮蔽/删除 blob 逐个 `router.delete`(幂等,失败记 warn 降级泄漏)
- [x] 2.2 新 `gc.rs`:周期循环(interval 默认 300s;`GC_BATCH_ROWS=1000`/`GC_BATCH_UPLOADS=100` 常量背压);行清理、孤儿 parts 清理 + 逐 blob 墓碑;`run_once_at` 时间注入(测试免 sleep);条目失败计指标续跑,循环不 panic
- [x] 2.3 启动接线:独立二进制 + devcluster 内嵌各 spawn(`enabled` 默认 true)

## 3. 配置/监控/文档(§7)

- [x] 3.1 `S3GatewayConfig` 嵌 `gc{enabled, interval_secs, deletion_retention_secs, upload_ttl_secs}` + `Validate`(全部 ≥1,retention 不许 0)
- [x] 3.2 metrics:runs/purged_rows/orphan_uploads/orphan_parts_blobs/errors{kind}/duration
- [x] 3.3 s3.md:配置表 +4 行、监控表 +6 项、「后台 GC」段(语义/默认/多实例安全/背压)
- [x] 3.4 live spec 漂移修正列入手册(blob 死点前移;归档时 MODIFIED 回写 s3-metastore)

## 4. 测试(PG 真机,`#[ignore]` 仓例,单线程跑)

- [x] 4.1 软删行保留期内不动/过期清行且计数;孤儿 multipart TTL 两沿(session 存活/回收含 part blob 墓碑有界轮询)
- [x] 4.2 overwrite:同 key put 两次 → 旧行 state=0、旧 blob 墓碑化收敛;multipart complete 覆盖同径
- [x] 4.3 并发取件:双连接 purge 恰好分账(和=行数;SKIP LOCKED 语义钉死)
- [x] 4.4 既有全套(`cargo test --workspace` + PG `--ignored --test-threads=1`)全绿;CI s3 job 同改单线程(理由注 ci.yml);推送 CI 绿

## 5. 收口

- [x] 5.1 DEVLOG 步骤 47(三面泄漏发现链 + 三坑根因);commit 8924dc7 + 事故修复笔(工件路径)