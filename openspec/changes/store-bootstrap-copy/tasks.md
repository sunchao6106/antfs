# tasks — store-bootstrap-copy (S3b)

## 1. 变更文档
- [x] 1.1 proposal / spec delta 写就,`openspec validate store-bootstrap-copy --strict` 通过。

## 2. blob 引擎:只读拷贝原语 + DI 重建
- [x] 2.1 `log_segment.rs`:`LogSegment::raw_prefix()`(读 header + data_end 字节)。
- [x] 2.2 `log.rs`:`Log::sealed_positions()` / `Log::read_segment_prefix(pos)`。
- [x] 2.3 `blob.rs`:`BlobStore::sealed_segments()` / `read_segment_prefix(pos)` / `open_rebuild_index_with(...)`。
- [x] 2.4 单测:多段库封存段枚举(不含活跃);`open_rebuild_index_with` 装载后可读。

## 3. store 数据节点:发送方端点 + 接收方装载
- [x] 3.1 `server.rs`:`GET /partitions/{pid}/segments`(JSON [{position,data_end}])+ `GET /partitions/{pid}/segments/{position}`(原始字节)。
- [x] 3.2 `replica.rs`:`ReplicaManager::install_bootstrap(pid, segments)`(选盘 + tmp→fsync→rename + open_rebuild_index_with + 注册)。
- [x] 3.3 `replication.rs`:`SegmentInfo` DTO + `PeerSource::segments()`/`fetch_segment()`(HTTP adapter 实现)+ `bootstrap_from_peer` 返回续拉 token。

## 4. 复制器集成
- [x] 4.1 复制器:空本地副本首次先物理 bootstrap → 设续拉 token → 逻辑补;失败/无段回落纯逻辑。
- [x] 4.2 config `bootstrap_copy_enabled`(默认 true)+ metrics `store_server_bootstrap_segments_total`/`_bytes_total`。

## 5. 测试
- [x] 5.1 集成测试:peer 造多封存段 + 活跃段新写 → 空副本混合 bootstrap → 全部存活对象可读;无封存段回落逻辑。
- [x] 5.2 `cargo test` 绿 + `cargo clippy` 无告警 + 全 workspace build。

## 6. 文档
- [x] 6.1 `docs/design/cluster.md` §9.1/§10:S3b 落地(物理拷贝 + 逻辑尾)+ 配置/监控清单补项。

## 后续(不在本 change)
- [ ] disk-aware 每盘并发限流(N4);增量段 diff/断点续传;**EC 仍锁定**。
