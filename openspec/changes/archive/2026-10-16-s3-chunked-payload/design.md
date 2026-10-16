# Design: `aws-chunked` 流式分帧签名

## 线格式(canonical 口径,严格接受 + 尾部宽容)

```
申报面(三字面量 → 三风味):
  STREAMING-AWS4-HMAC-SHA256-PAYLOAD           → 数据块带签名,无 trailer
  STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER   → 数据块带签名 + trailer 段
  STREAMING-UNSIGNED-PAYLOAD-TRAILER           → 数据块无签名 + trailer 段

块:
  chunk := hex-size [";chunk-signatory=" 64hex] CRLF  octets(hex-size)  CRLF
  终块 := "0" [";chunk-signature=" 64hex] CRLF
  (带签名风味:每个块含终块都必须携带 chunk-signature;无签名风味任何块都不得携带)
  trailer 段(仅 TRAILER 风味,紧随终块):== "Trailer-Header: value" CRLF ×N,空行 CRLF 终结
  终结后仅容忍 CR/LF 空白;其余尾字节 = 分帧格式错误。
  hex-size > 剩余申报解码长度、块头行 > 1 KiB、整数异常 → 分帧格式错误(400)。
```

## 块签名链

```
sig₀  = Authorization 头的 Signature(种子)
STS_k = "AWS4-HMAC-SHA256-PAYLOAD\n{ts}\n{scope}\n{sig_{k-1}}\n{sha256hex(data_k)}"
sig_k = hex(HMAC(signing_key, STS_k))     ← 常数时间比对(复用 sigv4::constant_time_eq_hex)
终块 data = ""(空哈希);signing_key 即请求头验签用的同一条 SigV4 密钥链。
```

## 状态机(`AwsChunkedBody`)

`http_body::Body` 实现,`type Error = S3Error`(新增 Display + std::error::Error impl,
错误即最终语义,`axum::Error::downcast` 精确取回),内嵌 `axum::Body` 原始流:

```
Header(找 CRLF 行 → size/sig 申报)→ Data(按 remaining 切片出帧,hasher 累计)
  → DataEnd(消 CRLF;带签名块此处验签,失配即 403)→ Header …
  终块(0):带验签 → Trailer(逐行至空行;校验值值记 debug 不验证)→ Done(仅容空白尾)
内层 EOF 且状态非 Done = 截断(IncompleteBody 语义);decoded 总长对账在 handler 侧。
```

## 申报一致性(分类器,验签前热路径完成)

| 条件 | 裁决 |
|---|---|
| 字面量 ∈ 三合法值 + `Content-Encoding: aws-chunked` + `x-amz-decode-content-length` 且方法 PUT/POST 且非 presigned | 进入分帧路径 |
| 字面量 ∈ 合法集但任一配套缺 | 400 InvalidArgument |
| 字面量非三值(含其它 STREAMING-* 变体) | 400 |
| `Content-Encoding: aws-chunked` 或存在 decoded 头但无字面量 | 400(申报撕裂) |
| presigned + 上述任一信号 | 400(presigned 不可分帧) |
| 申报解码长度 > `s3.max_body_bytes` | 413 EntityTooLarge(管线处预拒,零流读) |

## 集成形状

- `authenticate` 返回值升级:`Option<AuthResolution{context, chunked}>`;中间件既有
  测试同步机械迁移(字段访问)。分类在查凭证前完成(不合规 400 不触库)。
- `GatewayPipeline` 验签成功 → 包体(携 `SigCtx`/指标句柄)→ vhost 改写 → 分发;包体
  对被否决的请求零影响(先 auth 后包体,公共面早已直发,不达此处)。
- `read_bounded_body(st, req)`(put_object/upload_part 共用):to_bytes(inmemory limit
  即 `max_body_bytes`)→ 错误 `downcast::<S3Error>` 精确回放;分帧时对账
  `x-amz-decoded-content-length`(≠ → `IncompleteBody` 400),非分帧保持既有
  Content-Length 语义逐一不变。
- 指标:`s3_chunked_requests_total`(包体时)与 `s3_chunked_rejected_total{reason}`
  (framing/signature/length/oversize 构错点计数;解码器携带句柄克隆计数)。
- 解码限额:内存上界持续 = `max_body_bytes`(to_bytes 限额作用于解帧后数据),非分帧
  路径零新增常驻状态。

## Mechanism lesson(实装两课,探针/负例 e2e 实证)

- **axum 错误链三层**:`S3Error` 出 Body 后经 axum `to_bytes`(Limited + 装箱)成
  「包装 ×2 → 生源」三层;取回必须沿 `source()` 链 `downcast_ref::<S3Error>`——
  `into_inner().downcast` 只摸顶层摸不到,会把块签名失配误报成 413。负例 e2e 首
  捕获、探针单测定层后修复。
- **终块/trailer 宽容语法**(真 SDK 家族差异面):终块(0 尺寸)同样验签(对空数据);
  其头部 CRLF 后的空 CRLF **可选**(存在即消费);trailer 段以空行终结,流直接 EOF
  亦为终结(要求缓冲净);终态后仅容 CR/LF 至 EOF。数据块证据(每块尾 CRLF、
  存在性/长度一致性)保持严判。
- 包体在**服务级管线**完成(与 vhost 改写同层):auth 必须先行(块签种子依赖请求
  签名),层序 = auth → 包体 → vhost → dispatch;公共面早退不进此序。

## 安全/持久化边界

- 验签失败(含块链失配)= 403,对象**不落库**(e2e 硬断言 GET 404);无成功回报前移。
- 分帧解码为纯流上变换,零触及 blob 写路径语义(解出字节 = 普通请求体);块链验证
  属传输完整性,不改变账户/桶权界。
