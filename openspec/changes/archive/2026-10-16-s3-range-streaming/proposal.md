# s3-range-streaming —— GET 路径硬化:Range 语义 + multipart 流式读(三个 P0 收口)

## Why

S3 网关 2026-10-13 审计出的三个 P0 级缺口(用户拍板「先完善三个 P0」):

1. **GET 无 Range 请求支持**——`get_object`/`head_object` 完全不解析 `Range` 头,断点续传/媒体拖动/并行分块下载全部退化为全量下载;
2. **multipart GET 整对象拼内存**——`get_multipart_object` 逐片 `router.get` 后 `extend_into` 单个 `Vec` 再响应,与设计文档声称的「流式拼接」不符,且违背 CLAUDE.md §5.3「不把整个 blob 缓内存」红线(总大小无上界,大对象直接 OOM);
3. **普通/SSE 对象 GET 全量缓冲**——须核实并收口缓冲上界成立条件。

## What Changes

- **Range 语义**(GET ;HEAD 按官方语义忽略 `Range` 只补 `Accept-Ranges: bytes`):
  - 单 range `bytes=start-end` / `bytes=start-` / `bytes=-suffix`(后缀);
  - `end ≥ size` 截到 `size-1`;后缀超长截成全对象;`start ≥ size` → **416 `InvalidRange`**;
  - 多 range / 语法坏 / 单位非 `bytes` / `start > end` → **忽略,回 200 全对象**(RFC 忽略语义,与 S3 一致);
  - 206 回 `Content-Range: bytes s-e/total` + `Content-Length = 窗宽`,ETag/Content-Type/用户元数据头照回;空对象任何 range → 416。
- **Multipart GET 流式化**:按 manifest 顺序逐片 `router.get`(每片 ≤ `max_body_bytes`),链式流产出,内存上界 = 单片;响应前校验 `Σ manifest.size == row.size`,不一致即 500 `InternalError`(响亮不吞)。
- **Range 落到 multipart**:由 manifest 尺寸纯计算覆盖窗口,只取覆盖片并对首尾片做切片——非覆盖片零网络读取。
- **SSE 对象**:解密(既有缓冲路径)后按 range 切片回 206;边界不变(单 blob ≤ `max_body_bytes`,见设计不变量)。
- **普通对象**:维持 `router.get` 缓冲 + Range 切片;GET/HEAD 全部补 `Accept-Ranges: bytes`。
- 文档同步:`docs/design/s3.md`(功能矩阵、操作映射、监控项说明 s3_requests_total{op,status} 已覆盖 206/416 可观测)。
- 无新配置、无新指标、无 schema 变更。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-core`:GetObject 要求增补 Range 语义(206/Content-Range/416/Accept-Ranges + 忽略规则)与内存上界;HeadObject 要求增补 `Accept-Ranges: bytes`。
- `s3-multipart`:装配(读回)要求——流式逐片、Σ 尺寸不变量、Range 覆盖窗口按 manifest 计算。

## Impact

- 代码:`crates/s3/src/handlers/object.rs`(get/head/multipart/SSE 读路径 + 纯函数 range 解析与窗口计算)、`crates/s3/src/server.rs`(GET handler 下探 headers)、`crates/s3/src/error.rs`(新增 `InvalidRange` 变体)。
- 契约:GET/HEAD 响应头集合新增 `Accept-Ranges`/条件性 `Content-Range`;不触碰写路径、MetaStore schema、SigV4、ACL。
- 测试:纯函数单测(不依赖库);e2e 走 `TEST_S3_DRIVER` 双言矩阵惯例(`#[ignore]` + DSN 守卫)。
- 不动 frontend(REST /blobs)层与 router SDK(零 SDK 改动,理由见 design.md)。
