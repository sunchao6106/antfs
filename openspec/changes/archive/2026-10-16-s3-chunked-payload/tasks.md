# Tasks: s3-chunked-payload

## 1. 机制

- [x] 1.1 `auth/chunked.rs`:申报分类器 + 分帧状态机 + 块签名链 + trailer 段解析
  (单测矩阵:三风味正/负、头部行、尾字节、截断、超申报)
- [x] 1.2 `error.rs`:`IncompleteBody` 变体(400)+ `S3Error` Display/std::error::Error impl
- [x] 1.3 `auth/middleware.rs`:`AuthResolution{context, chunked}` 返回值(带 SigCtx),
  既有测试机械迁移;主管线包体接线 + 预拒 413;指标双计数注册
- [x] 1.4 `read_bounded_body` helper(错误精确映射 + decoded 长度对账);put_object /
  upload_part 接线;`Cargo.toml` + `http-body`

## 2. 验证

- [x] 2.1 夹具:块签名编码器(镜像服务器口径)+ 新二进制 `tests/chunked.rs`
  (正:String 三肉/Multipart 块化 part/UNSIGNED-TRAILER;负:块签名篡改 403 不落库、
  解码长度舞弊 400、超限 413 预拒;双跑)
- [x] 2.2 既有全套 e2e fresh/dirty 零变化(默认无申报即旁路);lib/单测绿

## 3. 收尾

- [x] 3.1 docs:矩阵行翻转、监控双指标、后续方向重排、「流式分帧」节;
  DEVLOG;归档 + push CI 硬验
