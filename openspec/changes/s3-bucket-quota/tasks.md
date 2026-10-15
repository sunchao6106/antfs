# Tasks

## 1. MetaStore 层(trait + 双方言)

- [ ] 1.1 migration:buckets 表 + `quota_bytes`/`used_bytes`(PG sqlx + MySQL 同构 DDL)
- [ ] 1.2 `S3Error::QuotaExceeded`(507,XML code+bucket/quota/used)+ pg/mysql 的 put_object/
     complete_multipart 守卫与扣减(单语句 UPDATE;shadow/delete 的 RETURNING/两步扣减)
- [ ] 1.3 quota 管理方法组:get/set_bucket_quota(admin 用)

## 2. Handler + admin + 观测

- [ ] 2.1 put/complete 的 QuotaExceeded 出口:同请求 router.delete 已写 blob(死点模型),
     指标 `s3_quota_rejected_total{bucket}` 计数
- [ ] 2.2 admin 端点:GET/PUT `/_admin/buckets/{name}/quota`(admin token 域)

## 3. 测试(PG 集成,真 Registry)

- [ ] 3.1 quota 拒/放/调大恢复/删释放/覆盖扣旧/multipart 计入 六场景(e2e)
- [ ] 3.2 并发守卫恰一次(两任务竞最后余量)
- [ ] 3.3 指标落真 Registry 断言

## 4. 文档与收口

- [ ] 4.1 s3.md:配额节(语义/竞态口径/热桶上界)+ 监控表 +1 行(§7)
- [ ] 4.2 净/脏双跑全绿 + workspace + fmt/clippy + push CI 绿 + 归档 + DEVLOG
