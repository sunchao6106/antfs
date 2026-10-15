# s3-uploadpart-reclaim-retraction —— 撤回「UploadPart 重传泄漏」误判 + spec 措辞对齐

## Why

`s3-gc` 归档时在 proposal「已知缺口」与 DEVLOG 步骤 47 记录:UploadPart 同 part-number 重传覆盖旧行 blob_id,旧 part blob 无人回收(判为历史遗留)。**复核推翻该判断**:

- `put_part`(pg.rs / mysql.rs 双方言之)前置 `SELECT` 被替换行 blob_id,UPSERT 后返回 `Option<BlobId>`;
- handler `upload_part` 同请求内对返回的旧 blob 执行 `router.delete`(墓碑,幂等);
- 该结构自 de5f123(S3 网关初版,`git show` 复核)即存在;
- `put_part_overwrite` 单测 PG/MySQL 双份在册;本地净库重跑 2 passed;CI `s3-integration`(`--ignored`)实跑 PG 变体(MySQL 变体无 DSN 守卫跳过)。

误判成因:归档时只扫了 INSERT 的 `ON CONFLICT ... DO UPDATE` 单行,未读前置 SELECT 与返回值,望文生义「覆盖即丢引用」。失实记录留存于两处(归档 proposal、DEVLOG 47)必须撤回,否则后续维护者会按幽灵缺陷立项。

## What Changes

- 撤回两处失实记录:归档 `2026-10-15-s3-gc/proposal.md` 缺口段加更正标注(原文划线保留);DEVLOG 步骤 47 留观行重写为更正。
- `s3-multipart` live spec:MODIFIED「UploadPart writes a part as an independent blob」—— requirement 正文补「重传 UPSERT 返回被替换 blob_id,handler 同请求墓碑」;重传场景 THEN 由 "the old part blob becomes reclaimable"(与 s3-core 旧措辞同型的空话)改为与实现一致的即刻墓碑。
- 代码零改动:复核结论为行为已正确且已有单测。

## Capabilities

### Modified Capabilities
- `s3-multipart`:UploadPart 重传场景措辞对齐现实(即刻墓碑)。

## 真留观(已核实,不以误代误)

- **并发同 part-number 重传原子性**:`put_part` 的 SELECT→UPSERT 两步非原子;两个并发重传交错时,先提交者的新 blob 可被后提交者覆盖行而成为无引用孤儿(各自墓碑的是同一个旧 blob,幂等无害)。顺次重传不受影响。修法方向:PG 单语句 CTE(`WITH old AS (SELECT ... FOR UPDATE)` + upsert RETURNING);MySQL 事务内 `SELECT ... FOR UPDATE` 两步等价原子化。触发罕见(客户端并发重传同 part),留独立 change。
- **MySQL 变体 CI 覆盖**:本地/CI 无 MySQL DSN 时 `put_part_overwrite` 守卫跳过 —— 归入既列菜单 C1(MySQL dialect CI gate)统一处理。

## Impact

- 文档:`docs/DEVLOG.md`(步骤 47 更正)、归档 `2026-10-15-s3-gc/proposal.md`(更正标注)。
- spec:`openspec/specs/s3-multipart/spec.md`(UploadPart requirement 措辞)。
- 代码:无。

## Non-goals

- 不修并发重传原子性(真留观,独立 change);
- 不动 GC 循环与 put_object/complete 遮蔽路径(已正确);
- 不新增测试(既有 `put_part_overwrite` 双方言单测足够,本 change 无代码改动)。
