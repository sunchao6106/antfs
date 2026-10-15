# Tasks

## 1. 双方言事务化

- [x] 1.1 `put_part`(PG):事务 + uploads 行 `FOR UPDATE`(NoSuchUpload 语义保留)+
     parts 行 `FOR UPDATE` 锁读旧值;upsert 后 commit;旧值恰一次语义
- [x] 1.2 `put_part`(MySQL):同构(InnoDB 锁读同语义)
- [x] 1.3 `abort_multipart`(PG/MySQL):事务 + uploads 行锁;终版 parts 读取与两记 DELETE
     同事务
- [x] 1.4 `complete_multipart`(PG/MySQL):事务 + uploads 行锁;会话锁跨越 `put_object`
     (独立事务,加锁单向论证见 design);DELETE parts+uploads 同事务

## 2. discarded part 回收链

- [x] 2.1 `CompleteMultipartResult` 增 `discarded_part_blobs` 字段(契约文档注)
- [x] 2.2 complete 计算「已上传 − 已列举」= discarded(双方言)
- [x] 2.3 handler:`shadowed_blobs` 与 `discarded_part_blobs` 同路径逐一 `router.delete`
     (幂等,失败仅 warn)

## 3. 测试与文档

- [x] 3.1 内联单测(双方言):并发重传恰一次不变量 / unlisted 恰一片回收 / abort×put
     双向终局无孤儿行 —— PG 与 MySQL 真机跑绿
- [x] 3.2 e2e:`concurrent_retransmit_completes_and_reads`(8 路并发 → ListParts 权威
     etag → complete → GET)
- [x] 3.3 s3.md「multipart 会话原子性」段落 + 持久性红线 limitation 记档(commit 后
     回收窗 = 既有类,后续 change)
- [x] 3.4 净/脏双跑 + workspace + fmt/clippy + push CI 绿 + 归档 + DEVLOG 步骤 54
