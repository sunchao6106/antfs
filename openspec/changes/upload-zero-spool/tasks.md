# Tasks: upload-zero-spool

> **未实施**——本 change 为方案设计档(待所有者三裁决点:①spill 语义 ②A 道型 ③顺序)。
> 点选后按下行执行;变体若改选(如 A-iii / 两遍),先改 proposal/design/spec 再动。

- [ ] 0. 所有者裁决确认(①原文直传 ②A-ii 分道 ③B 先 A 后)
- [ ] 1. B:router `compress_stream_to_mem` + `UploadSource` 枚举 + `put_file_to` Frame 臂
- [ ] 2. B:旋钮 `compression_frame_mem_max_bytes`(默认 8 MiB)+ Validate + 监控 `spill` 标签
- [ ] 3. B:测试(三闸对拍/边界/TMPDIR 零触/507 共用/指标实例)+ router.md 双表
- [ ] 4. A:`blob` Log lane 机制(`reserve_large`/commit/abort 回卷/seal 阈值)+ write_lock 拆短临界区
- [ ] 5. A:store handler 三段化(前查/reserve/转发环/commit)+ 空闲超时 + 内闸(`put_stream_direct` 默认 true,旧暂存路径保留灰度)
- [ ] 6. A:旋钮 ×2 + 监控 ×4 + `staging_dir` 退役文档 + store.md/router.md
- [ ] 7. A:测试面(I1-I6 逐条断言:崩溃 proptest / abort 回卷 / HOL 对照 / 503/507/409)+ colima 4 GiB 滚转复验
- [ ] 8. 两 PR 收官(各:全量门禁 → DEVLOG → spec 勾验 → archive → 单 commit → CI/Docker 双绿);稳定一版后删灰度旧路径(另开小批)

## 落地记录

(待实施后回填)
