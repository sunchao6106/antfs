# s3-range-streaming 任务

## 1. 纯函数层(无 DB,单测即全语义)

- [x] 1.1 `handlers/range.rs`:`parse_range`(Full/Partial/Unsatisfiable 三态,规则=design.md D1 表)+ `window_parts`(manifest 投影,非覆盖片零取件)+ `ByteWindow::content_range`
- [x] 1.2 单测(`handlers/range.rs::tests`):rule_matrix 28 例(截断/后缀/超界/`-0`/空对象/多区间/非 bytes 单位/大小写与空白)、window_projection 6 组(全量/整片/跨界切片/头尾/零尺寸片)、manifest_sum、content_range 文本

## 2. 错误面

- [x] 2.1 `error.rs` `InvalidRange { requested, actual_size }`:code=`InvalidRange`、status=416、message 带两者,`to_xml` 既有链路直出

## 3. 读路径(handler)

- [x] 3.1 `server.rs` `object_get_handler` 下探 `req.headers()` 的 `Range` 到 `get_object`;HEAD 不改签名
- [x] 3.2 `object.rs`:`get_object(st, bucket, key, range)` 三分支接窗(plain 切片/SSE 解密后切片/multipart 走 3.3);`object_read_builder` 统一 200/206 + `Accept-Ranges: bytes` + `Content-Range`;`apply_window` 先对账体长再切(Content-Length 不说谎)
- [x] 3.3 `get_multipart_object` 重写:响应前 `Σ manifest.size == row.size` 对账(失配 500);`PartStream`(`Body::from_stream`,逐片 `router.get` + 切片,至多一片驻留;短片 InternalError);无 Range = 全片窗口
- [x] 3.4 `head_object` 补 `Accept-Ranges: bytes`

## 4. 测试

- [x] 4.1 e2e `tests/object.rs` ×3:`range_get_matrix_small_object`(闭环/截断/开区/单字节/后缀/后缀超长/416×2/忽略×4/HEAD 的 Accept-Ranges)、`range_get_empty_object_is_416`、`range_get_sse_object`(206+`x-amz-server-side-encryption`+后缀+EOF 416)
- [x] 4.2 e2e `tests/multipart.rs` ×2:`multipart_streaming_read_60_parts`(60×64KiB 逐字节对拍 + Content-Length 精确)、`multipart_ranged_windows`(整片窗/跨界拼接/后缀/416);fixture 增 `get_object_with_headers`/`get_object_range`
- [x] 4.3 双言实跑(本机真 PG 5432 + MySQL 8.4 容器 13306,`--ignored --test-threads=1`):**两方言全量 e2e 各 68/68 通过**(auth 5 / chunked 7 / copy 11 / list 6 / multipart 11 / object 19 / sse 6 / vhost 3),零回归

## 5. 文档与收尾

- [x] 5.1 `docs/design/s3.md`:矩阵 GetObject 与 Multipart 行更新;操作映射 GET 行注 range;新增「Range 读与流式装配」节(D1 规则、单片内存上界、Σ 对账、窗口化、单 blob ≤ `max_body_bytes` 部署须知);监控表 `s3_requests_total` 行注明 206/416 观测面;README en/zh 组件行同步
- [x] 5.2 `cargo test --workspace` 退出码 0;`cargo clippy --workspace --all-targets` 0 告警;`cargo fmt -p antfs-s3` 干净;fmt 后新用例复跑仍绿
- [x] 5.3 三件套自检:spec delta 场景(206/截断/后缀/416/忽略/HEAD、60 片流式/窗口/对账)↔ e2e 用例 1:1 对得上;归档(archive + DEVLOG 步)待用户验收后由用户发起
