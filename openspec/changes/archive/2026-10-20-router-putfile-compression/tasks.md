# Tasks: router-putfile-compression

- [x] 1. `StreamedPut` + `compress_stream_to_temp`(流式三闸)+ `encode_file` 四算法臂(显式 finish)
- [x] 2. `put_file_with_ttl` 接线:一次判定、帧 RAII 跨重试/多副本共用、`io` 回落;tempfile 升依赖
- [x] 3. 监控 `io` 取值 + router.md(§4 写路径 bullet / §7 三闸收官 / 监控行)+ DEVLOG 步 87
- [x] 4. lib 单测(四算法 roundtrip / 熵 / 零长 / io)+ e2e(真 store 三闸走全 + 逻辑大小恒等)
- [x] 5. spec delta(MODIFIED ×2)+ validate + archive + 单 commit + CI/Docker 双绿

## 落地记录(已回填)

- router:**lib 26**(新增 `stream_compress_roundtrip_all_algos`、`stream_compress_io_failure_falls_back`)
  / **e2e 19**(新增 `put_file_compression_roundtrip_all_gates`:kept/too_small/low_ratio 三臂 +
  `get_file` 逻辑大小恒等断言)。
- **workspace 77 套 786/0;clippy 0;fmt 净**。
- 实现注记:zstd/gzip/lz4 `finish()` 显式收尾 + 末次 `flush`;snap 用 `into_inner()`(无 finish);
  比率阀按"帧文件(含 17B 头) vs 原文件",与 Bytes 口同义;`FinishedTempFile` 不引入 ——
  `tempfile::NamedTempFile` 本身即 RAII。
