## Why

S3a 落了正确收敛的**逻辑**拉复制,但 bootstrap 一个空副本(新建/失联再复制)仍逐记录 HTTP GET + apply,大分区 MTTR 慢。N3(cluster.md §9)锁定 bootstrap = **混合**:封存段(不可变)走**物理流式拷贝**(快、顺序 IO、免逐记录),活跃段尾巴走 token 逻辑补。S3a design.md D-A 已把物理拷贝记为 S3b。本 change 落 S3b。

关键可复用性:`blob` 引擎的段文件(`{pos}_log.seg`,64B header + 数据区)中**除最高位(活跃)外全是封存段**——不可变,可整文件安全拷贝;引擎已有 `open_rebuild_index`(丢索引、扫 log 重建),故接收方装好段文件后直接重建索引即可,**无需新增引擎重建逻辑**。

## What Changes

- **blob 引擎**:
  - `LogSegment::raw_prefix()`(读 `[0, HEADER + data_end)`,即 header + 有效数据,省去预分配尾部空洞)。
  - `Log::sealed_positions()` / `Log::read_segment_prefix(pos)`;`BlobStore::sealed_segments() -> [(pos, data_end)]` 与 `BlobStore::read_segment_prefix(pos)`(发送方读封存段字节)。
  - `BlobStore::open_rebuild_index_with(dir, cap, config, metrics, store_id)`(DI 变体,接收方装好段后重建打开)。
- **store 数据节点(发送方 HTTP)**:
  - `GET /partitions/{pid}/segments` → JSON `[{position, data_end}]`(仅封存段,活跃段不可拷)。
  - `GET /partitions/{pid}/segments/{position}` → 该封存段原始字节(header + 数据)。
- **store 数据节点(接收方)**:`ReplicaManager::install_bootstrap(pid, segments)`——选盘 → 段文件写入(crash-safe `tmp→fsync→rename`)→ `open_rebuild_index_with` → 注册句柄;返回续拉 token = `Offset(max_sealed_pos, data_end)`。
  - `replication.rs`:`bootstrap_from_peer(peer, pid)` 拉封存段清单 + 逐段下载 + `install_bootstrap`,返回续拉 token;复制器对**空**本地副本首次先物理 bootstrap,再从续拉 token 逻辑补活跃尾;无 peer/无封存段则回落 S3a 纯逻辑。
- **配置**:`bootstrap_copy_enabled`(默认 true,随 `replication_enabled` 生效)。
- **监控**:`store_server_bootstrap_segments_total`、`store_server_bootstrap_bytes_total`。

## Capabilities

### New Capabilities
- `store-bootstrap-copy`:封存段物理流式拷贝的 bootstrap(MTTR 优化),叠加在 S3a 逻辑拉复制之上。

### Modified Capabilities
<!-- blob 引擎新增只读段拷贝原语 + DI 重建构造(不改既有写/读语义);store 复制器 bootstrap 路径。 -->

## Impact

- **crate**:`blob`(`log_segment.rs`/`log.rs`/`blob.rs` 加只读拷贝原语 + `open_rebuild_index_with`);`store`(`replica.rs` 加 install_bootstrap;`replication.rs` 加 bootstrap_from_peer + endpoints;`server.rs` 加 2 段端点;config/metrics)。
- **持久性(§3.1/§3.4)**:只拷**封存(不可变)**段;接收方 `tmp→fsync→rename` 逐段落地,全部就位后才 `open_rebuild_index`;崩溃则 staging 未 rename,下次重来(幂等)。物理拷贝**只增**(空副本装载),不删既有数据。段格式带 version(§3.4),拷贝的是同格式字节。

## Non-goals

- 不做增量段级 diff / 断点续传(段级全量拷贝;失败整段重来)。
- 不拷活跃段(可变;交逻辑补)。不做 disk-aware 每盘并发限流(N4,后续);MVP 顺序拷。
- 不动 S3a 逻辑路径语义;**EC 仍锁定**。
