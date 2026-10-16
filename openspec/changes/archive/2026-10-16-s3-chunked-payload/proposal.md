# Proposal: `aws-chunked` 流式分帧签名(STREAMING-AWS4-HMAC-SHA256-PAYLOAD 家族)

## Why

真实 AWS SDK/CLI 对带体写(`PutObject`、`UploadPart`)大规模默认使用
`Content-Encoding: aws-chunked` + `x-amz-content-sha256: STREAMING-*` 流式分帧签名。
当前网关对这类请求:**头验签会通过**(canonical 恰好收该字面量),但分帧字节
**原样入库**(读回 = 分帧噪声),且 put_object 的 Content-Length 一致性检查命中的是
分帧线上长度 → 500。真 SDK 客户端的带体写路径基本不可用。

## What Changes

- **新模块 `auth/chunked.rs`**:分帧申报一致性分类器(三个合法 `STREAMING-*` 字面量
  + `Content-Encoding`/`x-amz-decode-content-length` 配套检查)+ `AwsChunkedBody`
  (`http_body::Body` 实现:分帧解析、块签名链验证、解码预算、trailer 段解析)。
- **`authenticate` 返回 `AuthResolution`**:`{ context, chunked: Option<{declared, sig_ctx}> }`;
  块签名上下文(signing key/timestamp/scope/seed = 请求签名)在凭证验签后一次成型,
  不二次查库。
- **`GatewayPipeline` 验签成功后包体**:`req.map(body → AwsChunkedBody)`;申报解码长度
  超 `s3.max_body_bytes` 预拒(413,零流读)。
- **处理器共享 `read_bounded_body`**:解码错误精确映射(SignatureDoesNotMatch 403 /
  `IncompleteBody` 400 新变体 / InvalidArgument 400);分帧请求长度对账改
  `x-amz-decoded-content-length`(取代 Content-Length 线上长度)。
- **语义不变面**:无 chunked 申报的请求逐字节等价,默认行为零变化。

## Impact

- 到达路径:`put_object`/`upload_part` 是唯二消费对象的带体端点;XML 面(batch)照旧
  粗映射(真客户端不对 XML 送分帧,记档)。
- 信任面不变:非 STREAMING 十六进制 body 摘要**不重算**(信任等级 = UNSIGNED-PAYLOAD,
  checksum 家族明确不在本轮);trailer 内校验值解析后忽略。
- 监控:批次新增 `s3_chunked_requests_total` + `s3_chunked_rejected_total{reason}`。

## Non-goals

- trailer 校验值(`x-amz-checksum-*` / trailer 签名)的**验证**——解析、宽容放行、
  debug 日志;信任面等同 UNSIGNED-PAYLOAD。
- flexible checksums 家族(CRC32/CRC32C/SHA 家族请求头与 ETag 尾)。
- 非流式十六进制 body 摘要重算。
- SSE-C、part 级流式 PULL(解码后仍走 `to_bytes` 上限物化,限额即既有 4 MiB 面)。
