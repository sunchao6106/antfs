## 1. 错误形态与常量

- [x] 1.1 `crates/s3/src/error.rs`:`S3Error` 增 `MetadataTooLarge`/`InvalidArgument(原因)` → 400 XML(`error.rs` → HTTP 映射与 xml 序列化同补)
- [x] 1.2 `crates/s3/src/handlers/object.rs`:具名常量 `MAX_USER_META_BYTES = 2048`(注释口径:Σ key+value UTF-8 字节,S3 语义官方值)、`MAX_CONTENT_TYPE_BYTES = 256`(对齐 VARCHAR(256) 列宽)

## 2. put 边界与默认

- [x] 2.1 `object.rs put_object`:采集后校验——meta 超 2048 → `MetadataTooLarge`;Content-Type 超 256 → `InvalidArgument`;采集 `Content-Type` 为 None 时存默认 `binary/octet-stream`
- [x] 2.2 `multipart.rs` create_multipart:同一套校验与默认(复用 1.2 常量;create 时校验,complete 不再校验)

## 3. 测试(PG-gated,既有 `#[ignore]` 套路)

- [x] 3.1 `tests/object.rs`:meta 往返(PUT 带 Content-Type+x-amz-meta-* → GET/HEAD 回吐)、无 Content-Type → GET 得 `binary/octet-stream`、meta >2048 → 400 `MetadataTooLarge`、Content-Type >256 → 400 `InvalidArgument`
- [x] 3.2 `tests/multipart.rs`:create 带 meta(+默认型)→ part → complete → GET 回吐 meta

## 4. 文档(CLAUDE.md §6/§7)

- [x] 4.1 `docs/design/s3.md`:「S3 操作映射」补「对象元数据」段(采集/回吐/2 KiB 上限/列宽上限/存储默认/旧行 NULL 差异);无新配置项与监控项,相应表不动
- [x] 4.2 `docs/DEVLOG.md`:步骤 43(S3 对象元数据补证据+边界化;含「实现早已存在、审计复核纠错」的来龙去脉)

## 5. 收口验证

- [x] 5.1 `cargo fmt --all --check` + `cargo clippy -p antfs-s3 --all-targets -- -D warnings` + `cargo test -p antfs-s3`(单测);本地 PG 跑 `--ignored` 集成全绿;推送后 CI 全绿
