# Tasks: upload-zero-spool

> **未实施**——① 已裁定(2026-10-21:任意体量必须压缩,混合形态定案);②A 道型 ③顺序仍待。
> 二者点选后按下行执行;变体若改选(如 A-iii),先改 proposal/design/spec 再动。

- [x] 0. 裁决①:大文件不压缩被否决;采纳 ≤上界内存帧 + >上界两遍流式(已于修订档落定,
      状态见 proposal「裁决记录」)
- [ ] 1. B:型一内存帧(`compress_stream_to_mem`,≤上界)+ 型二两遍流式(计数遍 + 逐副本
      边压边传 CL=精确值)+ `UploadSource` 三态 + `put-file/body` 臂扩展
- [ ] 2. B:旋钮 `compression_frame_mem_max_bytes`(默认 8 MiB,0=全走两遍)+ Validate +
      监控 `two_pass` 标签
- [ ] 3. B:测试(见 design §5 B 面:两遍确定性对拍/CL 恰等兜底/超界可压与熵大文件成对
      断言/上界边界/TMPDIR 零触/指标实例)+ router.md 双表
- [ ] 4. A:`blob` Log lane 机制(`reserve_large`/commit/abort 回卷/seal 阈值)+ write_lock 拆短临界区
- [ ] 5. A:store handler 三段化(前查/reserve/转发环/commit)+ 空闲超时 + 内闸(`put_stream_direct` 默认 true,旧暂存路径保留灰度)
- [ ] 6. A:旋钮 ×2 + 监控 ×4 + `staging_dir` 退役文档 + store.md/router.md
- [ ] 7. A:测试面(I1-I6 逐条断言:崩溃 proptest / abort 回卷 / HOL 对照 / 503/507/409)+ colima 4 GiB 滚转复验
- [ ] 8. 两 PR 收官(各:全量门禁 → DEVLOG → spec 勾验 → archive → 单 commit → CI/Docker 双绿);稳定一版后删灰度旧路径(另开小批)

## 落地记录

(待实施后回填)
