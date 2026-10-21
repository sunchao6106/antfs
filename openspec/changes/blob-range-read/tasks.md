# Tasks: blob-range-read

> 未开工。P0 闭环前不动代码。

## P0 — 裁决与提案定稿

- [x] 0.1 消解:压缩功能整体移除(⑬,同日),D1 不复存在;重开压缩时回访
- [x] 0.2 同上(D1 消解)
- [x] 0.3 D3 = HEAD 带 Range 回 206(Ambry 口径),已实施
- [x] 0.4 完成:specs/blob-range-read/spec.md(三 requirement + scenarios)
- [x] 0.5 已裁决并档:忠实对齐(Ambry REST 原生 Range),"该补"不依赖占比;优先级随本批全量落地

## P1 — store 数据面窗口读(超越 Ambry 的优化,最先落地、独立受益)

- [x] 1.1 完成:`GetParams{offset,length}`(serde 缺省;坏值 400 由 Query 层拒)
- [x] 1.2 完成:start > len → 416;length 截尾;零长窗 = 空 200
- [x] 1.3 保持:窗口参数不进复制分支(代码 + design 记档);e2e 压盖 router 套
- [x] 1.4 依托既有设计:classify 只认 `replication=`,未知 query 不误伤(design §4 验证);store 全量测试绿
      `path_op_names_every_data_plane_route` 门通过
- [x] 1.5 完成(窗口长计;store.md §12.15.1 记档)
- [x] 1.6 完成(§12.15.1 + duration 行注)

## P2 — router SDK 读口(对齐 `GetBlobOptions.range`)

- [x] 2.1 **未做(裁决⑭:读口只留 stream 式)**;窗口统一走 `get_object_range_stream`
- [x] 2.2 完成:`get_object_range_stream(key, start, len)`(plain = 数据面窗口流;
      composed = 覆盖片裁边)+ `open_replica_stream(window)`
- [x] 2.3 消解(⑬)
- [x] 2.4 e2e 落地(恒切套件的窗口断言 + 恒等对拍;R=1 failover 既有覆盖沿用,proptest 属性面后续补)
      NotFound/Deleted/Expired 映射不变
- [x] 2.5 完成(§3 读口表)

## P3 — frontend `/blobs` Range(忠实对齐还债)

- [x] 3.1 完成:frontend 自持 `src/range.rs`(同语义移植 + 单测矩阵;跨 crate 复用
      方向不正,记入 proposal 实施偏差),206/`Content-Range`/`Accept-Ranges`/416/忽略
      回 200 全量落地(REST e2e `range_reads_serve_206_and_slice_composed_objects`)。
- [x] 3.2 完成:覆盖片映射在 router(`get_object_range_stream` 的 covers 投影);
      e2e 断言窗口字节恒等 + 拼装读计数
- [x] 3.3 超预期:普通 GET 统一走 `get_object_stream` 直通,暂存机制整体删除(裁决⑫)
- [x] 3.4 完成(206 + Content-Range,REST e2e 断言)
- [x] 3.5 零新增:沿用 `download_total`/`download_bytes_total`(窗口长计入),router 侧拼装读计 `router_get_chunked_total`(见 router.md §6)
- [ ] 3.6 文档:`docs/design/frontend.md` API 表

## P4 — S3 网关收尾(小改,收益有界)

- [x] 4.1 完成:窗口走 `get_object_range_stream` 流式回体;整对象读保持 `get_blob`(尺寸对账)
- [x] 4.2 完成:每片经 `get_object_range_stream(key, skip, take)` 窗口取件,短读 = 响亮 500
- [ ] 4.3 活库矩阵 42 ignored(需 DB,惯例手动跑;纯函数矩阵绿)

## 收尾门(全阶段)

- [ ] `cargo test` 绿 + `cargo clippy` 零告警 + fmt 净
- [ ] devcluster 真面:大对象 Range 读字节恒等(整读对拍)
- [ ] `?replication=1` 复制读契约回归(整读 + include_expired + 整条 CRC)
- [ ] 四份设计文档(store/router/frontend/s3)同步 + §7 配置/监控清单逐项对账
- [ ] DEVLOG 记步 + validate/archive
