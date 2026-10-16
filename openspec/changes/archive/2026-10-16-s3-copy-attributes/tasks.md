# Tasks: s3-copy-attributes

## 1. 机制

- [x] 1.1 `error.rs`:`PreconditionFailed`(412)+ `InvalidRequest`(400)双变体三段
- [x] 1.2 `xml.rs`:`serialize_copy_object_result` + `serialize_object_attributes`
  (含单测:引号/转义/选集)
- [x] 1.3 `handlers/object.rs`:`copy_object`(源解析/条件评估/指令/自拷贝/单块弧/
  multipart 内部重写/配额弧)+ `get_object_attributes`(选集/501/头)
- [x] 1.4 `server.rs`:`S3QueryParams.attributes` + PUT/GET 两分叉;GET handler 收 req

## 2. 验证

- [x] 2.1 e2e `tests/copy.rs`(净/脏双跑):
  单块拷贝回环+XML 断言(ETag/LastModified)、multipart 源拷贝(装配等价)、
  COPY/REPLACE 元数据、四条件头正负(+412)、自拷贝 COPY 400 / REPLACE 200、
  源 404/403(public-read 放行)、配额 507、`?attributes` 三属 + 头 + 501/400
- [x] 2.2 全套回归零变化;fmt/clippy;lib 绿

## 3. 收尾

- [x] 3.1 docs(s3.md 矩阵行翻转 + 「拷贝与属性」节;README 双语);DEVLOG 61;
  归档 + push CI 硬验
