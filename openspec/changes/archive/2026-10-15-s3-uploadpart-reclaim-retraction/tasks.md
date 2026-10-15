# Tasks

## 1. 复核与撤回

- [x] 1.1 实读 PG/MySQL `put_part` + handler:旧 blob 同请求墓碑成立(pg.rs 304-337 / mysql.rs 337-363 / multipart.rs upload_part)
- [x] 1.2 考古:de5f123 已含回收路径(`git show` 复核);误判源自对 ON CONFLICT 单行的扫读
- [x] 1.3 本地净库重跑 `put_part_overwrite`(PG 实跑 / MySQL 守卫跳过):2 passed
- [x] 1.4 撤回 DEVLOG 步骤 47 留观行;归档 `2026-10-15-s3-gc/proposal.md` 缺口段更正标注(原文划线保留)
- [x] 1.5 MODIFIED 回写 live spec(s3-multipart UploadPart 场景措辞)
- [x] 1.6 真留观入册:并发重传原子性(含修法方向)、MySQL DSN 跳过归 C1
- [x] 1.7 归档 + push + CI 绿
