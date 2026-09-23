# Tasks

> **状态:已实施(2026-09-23)**。本 change 原为"先记决策再改代码"的档案([CLAUDE.md §9](../../../CLAUDE.md)),
> 现按**实现后的真实形态**回填勾选,并在文末列出[与原提案的偏差](#与原提案的偏差实现后回填)。
> 代码/测试层面的详细记录见 [`docs/DEVLOG.md`](../../../docs/DEVLOG.md) 2026-09-23 条与 `docs/design/store.md` §3/§5。

## 前置(拍板项,见 design.md「Open Questions」)

- [x] `delete` 的幂等口径(2026-09-23 改判):删**不存在**对象 → `NotFound`(404);删**已墓碑**对象 → **幂等成功(204,不写第二条墓碑)**。
      已落地:`blob.rs::delete_inner` 墓碑分支改 `Ok(())`;store/router/frontend 零改动自然贯通;测试(`blob` 单测 + 模型对拍、
      `store/tests/data_plane.rs`、`frontend/tests/rest.rs`、`router/tests/e2e.rs`)同步改断言。
- [x] `key_prefix` 取值 = blobId **高 32bit**(该副本的 partitionId),由调用方(数据节点 `replica.rs`)给出;
      `open` 时与既有段读回的前缀核对,不一致 → `Corrupt`;写路径对 key 的 partition 与前缀不符者 → `BadArgument`。
- [x] 索引 flags **不设记录版本位** —— 只存在一种记录格式(v4),没有版本判别需求。
- [x] 段头 v2 的 CRC **仍在 36..40**(覆盖 `[0,36)`),`key_prefix` 占 **32..36**(与原提案推测的"CRC 后移到 40..44"不同)。
- [x] `crates/admin` 连带已处理;`store_server.hard_delete_interval_secs` 已移除。

## blob:段头 v2(每段 4B `key_prefix`)

- [x] `crates/blob/src/log_segment.rs`:物理 64 B 头,**40 B 有意义**;`magic(0..4) / version=2(4..6) / reserved(6..8) / capacity(8..16) / data_end(16..24) / created_ns(24..32) / key_prefix(32..36) / crc(36..40,覆盖 `[0,36)`)`;段容量硬上界 `MAX_CAPACITY = 4 GiB`。
- [x] `LogSegment::open` 读回并校验 `version` 与 CRC;**没有 v1 兼容分支**(无已部署数据):版本不符 / CRC 不符 → `Corrupt`。
- [x] `LogSegment::{create,write_header}` 收 `key_prefix`;新增 `key_prefix()` 读回。
- [x] `Log::{create,open,roll_over}` 传递同一前缀;`open` 校验"调用方给的前缀 == 各段读回的前缀"(所有段必须一致)。
- [x] 测试:前缀 round-trip、前缀/CRC 被篡改报 `Corrupt`、未知版本报 `Corrupt`、`data_capacity = capacity − 64` 与物理 64 B 头不变。

## blob:记录 v4

- [x] `crates/blob/src/blob.rs`:常量改 v4(`REC_VERSION = 4`、`rec::{KEY_SUFFIX,EXPIRATION,DATA_LEN,DATA}` 改偏移),`RECORD_OVERHEAD = 24`(记录头 20 + 尾 crc 4)。
- [x] `encode_record` / `encode_header_into` 去掉 `operation_time_ms` / `life_version` 参数,改收 4B key 后缀(与段前缀拼装)。
- [x] `decode_record` 只解码 v4;未知版本 → `Corrupt`(**不做 v3 分支**:无已部署数据)。
- [x] `DecodedRecord` 去掉 `operation_time_ms` / `life_version`。
- [x] 偏移算术全部改为"记录总长 = `RECORD_OVERHEAD + data_len`"(含尾 crc)。
- [x] 写路径校验 key 的 partition 与该副本 `key_prefix` 一致,不一致 → `BadArgument`(不静默拼出错 key)。
- [x] `record_data_len` 注释更新:`segment_size_bytes ≤ 4 GiB` 后它是第二道保险。

## blob:索引 v2 + 配置约束

- [x] `crates/blob/src/index/value.rs`:`VALUE_SIZE = 21` —— `offset(8,打包) | size(u32,4) | flags(1) | expiration_ms(i64,8)`。
- [x] `IndexValue`:`offset` 编解码为 `(段号 u32 << 32) | 段内偏移 u32`(内存 `LogOffset` 表示不变);`size` 为 **u32 = 整条记录的字节数**;删 `operation_time_ms` / `life_version` / `original_offset`;**保留** `expiration_ms`。
- [x] `flags` 只留 `FLAG_DELETED`(**没有**任何"记录是 v4 布局"位 —— 只有一种记录格式)。
- [x] `crates/blob/src/index/segment.rs`:`ENTRY_SIZE = 8 + 21 = 29`;`IDX_VERSION = 2`;`key_size`/`value_size` 指纹比对保持(旧值 → `Corrupt` → 从 log 全量重建)。
- [x] **索引 key 仍是完整 8B `StoreKey`**(不是 4B 后缀);entry = 8 + 21 = 29 B。
- [x] `crates/blob/src/config.rs`:`StoreConfig::validate` 增 `segment_size_bytes ≤ 4 GiB`(`MAX_SEGMENT_SIZE_BYTES`,默认值不动)。
- [x] 测试:索引 v2 round-trip、offset 打包边界。

## blob:删除语义与变更类 API 移除

- [x] `delete` 改为**不可逆**:只写墓碑;`put` 命中已存在 key(**存活或墓碑**)→ `AlreadyExists`。
- [x] 删 `undelete` / `update_ttl` / `hard_delete`(含整段 COW 重写与 `FLAG_HARD_DELETED` 幂等逻辑);就地改写原语 `overwrite_at` 从 `Log`/`LogSegment` 删除。
- [x] 删 `FLAG_TTL_UPDATED` / `FLAG_UNDELETED` / `FLAG_HARD_DELETED`,flags 只剩 `FLAG_DELETED`。
- [x] `apply_replicated_inner` 改为**墓碑恒胜**:本地墓碑 → `Skipped`;peer 墓碑 → 覆盖;peer 存活 + 本地存活 → `Skipped`;peer 存活 + 本地缺失 → 应用(数据体来自 peer)。删掉 `life_version` 比较与 `tomb_orig` / 数据体倒读。
- [x] `ReplicatedRecord` / `apply_replicated` 去掉 `operation_time_ms` / `life_version`;墓碑不再需要 `data`。
- [x] `index_value_from_record` 去掉 `life_version` / `original_offset` 继承;`rebuild_index` / `replay_from` 同步。
- [x] `BlobMeta` 收缩(`deleted` / `expired` / `expiration_ms` / `data_len`);`MessageInfo` 去掉 `undeleted` / `ttl_updated` / `operation_time_ms` **并连同 `life_version`**;`get_with_options` 只保留 `include_expired`。
- [x] `crates/blob/src/error.rs` 删 `NotDeleted` / `AlreadyUpdated`;**保留 `Deleted`**。
- [x] `crates/blob/src/metrics.rs` 删 `store_hard_delete_total` / `store_hard_delete_duration_seconds` 与其 handle 字段。
- [x] `verify` 的 `index_mismatch` 口径 = 比对 `size` / `flags`(不再有 `life_version`)。
- [x] 测试改造:被移除能力的用例随能力一起删;补"DELETE 后 `get` → `Deleted`、无复活路径"与墓碑恒胜的单调性/幂等性。

## store:端点 / 复制 / 静默 / 指标 / worker

- [x] `crates/store/src/server.rs`:删 `undelete_blob` / `update_ttl_blob` 与 `POST .../undelete`、`POST .../ttl` 两条路由;`map_mut_err` 删 `NotDeleted` / `AlreadyUpdated` 分支。
- [x] `crates/store/src/server.rs`:`MetadataDto` 删 `undeleted` / `ttl_updated` / `life_version`。
- [x] `crates/store/src/server.rs`:blob 读 `?replication=1` 只保留 `include_expired`。
- [x] `crates/store/src/replication.rs`:`MetadataEntry` 删 `ttl_updated` / `undeleted` / `life_version` / `operation_time_ms`;flags 只剩 `DELETED`。
- [x] `crates/store/src/replication.rs`:`need_data` 简化为"peer 非墓碑且本地缺失";墓碑路径不再取数据体。
- [x] `crates/store/src/request_control.rs`:`DataOp` 删 `Undelete` / `TtlUpdate`(`ALL` 5 → 3),`as_str` / `parse` / `classify` 与单测同步。
- [x] `crates/store/src/metrics.rs`:`mutations` 文案改"delete 变更";删 `hard_deleted`。
- [x] `crates/store/src/worker.rs`:删 `hard_delete_all` 与第三条 loop。
- [x] `crates/store/src/config.rs`:删 `hard_delete_interval_secs`(含 `Default` / `Validate`)。
- [x] `crates/store/src/replica.rs`:`ensure_replica` / `install_from_segments` 把 4B key 前缀传给 `BlobStore::{create_with,open_with}` 与 `open_rebuild_index_with`(前缀 = `pid` 的高 32bit)。
- [x] `crates/store/tests/`:涉及 undelete/ttl/hard_delete 的用例与假 store JSON 同步。

## common/router

- [x] `crates/common/router/src/lib.rs`:删 `Router::undelete` / `update_ttl`;`mutate_one` 只留 `delete`(去掉 `"undelete"`/`"ttl"` URL 分支与 `expiration_ms` 参数)。
- [x] `BlobStat` 删 `undeleted` / `life_version`(对齐 store 的 `MetadataDto`)。
- [x] **新增 `Router::put_with_ttl(key, data, expiration_ms)` / `Router::put_file_with_ttl(path, expiration_ms)`**;`put` / `put_file` 保持为 `NO_EXPIRATION` 包装(`NO_EXPIRATION` 由 `antfs_router` 再导出)。
- [x] `crates/common/router/src/metrics.rs`:`router_mutation_total{op}` 取值集合收窄为 `delete` / `stat`。
- [x] `crates/common/router/src/error.rs`:`AlreadyExists` 的文档注释不再只说"号被复用"(重复 DELETE 命中墓碑同样回 409)。
- [x] `crates/common/router/tests/e2e.rs`:删"复活"用例,补"DELETE 后 GET 404 且不可复活"。

## frontend

- [x] `crates/frontend/src/server.rs`:删 `/blobs/{blob_id}/undelete`、`/blobs/{blob_id}/ttl` 两条路由与 `undelete_blob` / `update_ttl` handler。
- [x] 删 `x-ambry-blob-undeleted` / `x-ambry-blob-ttl-updated` / `x-ambry-blob-life-version` 三个响应头常量、HEAD 响应里的三处与 CORS 暴露头里的三项(`CORS_EXPOSED_HEADERS` 现为 **8 项**);`x-ambry-ttl-ms`(`HEADER_TTL_MS`)保留。
- [x] 上传路径的"先 `put` 再 `update_ttl`"改为**写入时一次带 TTL**(调 `Router::put_with_ttl` / `put_file_with_ttl`,数据面 `PUT ?expiration_ms=`)。
- [x] `crates/frontend/src/metrics.rs`:删 `OP_UNDELETE` / `OP_TTL`;`crates/frontend/tests/rest.rs` 同步。

## admin(连带)

- [x] `crates/admin/src/scrub.rs`:`KeyState` 删 `life_version`,比对文案去掉 lv。
- [x] `crates/admin/src/server.rs`:去掉对 `replica-metadata` 的 `life_version` / `undeleted` / `operation_time_ms` 反序列化;假 store 测试夹具(`spawn_scrub_store`)的 JSON 字段同步。

## 测试夹具迁移(4B 前缀一致)

- [x] `key(n)` 夹具统一改 `StoreKey::from_parts(0, n)`(`blob/src`、`blob/tests`、`store/src`、`store/tests`)。
- [x] 负例测试:夹具 key 的高 4B 与该副本 `key_prefix` 不一致时,读写显式报错(而不是拼出另一个 key 静默错读)。

## 文档

- [x] `docs/design/store.md`:§3 段头 v2 + 记录 v4、§5 索引条目(29 B / value 21 B)、§8(删 hard_delete 的 COW 通道)、§9 路线图、§10.1(`segment_size_bytes ≤ 4 GiB`)、§10.4(移除)、§11.5(HardDeleter 表移除)、§12.3(静默 op 枚举)、§12.10(`index_mismatch` 口径)。
- [x] `docs/design/cluster.md`:§1 角色表、§7.2 门控表、§9.1 数据面端点 / 配置 / 监控清单、§10 的 W 适用范围(put/delete)与变更段。
- [x] `docs/design/router.md`:方法清单、`*_with_ttl`、错误表、`router_mutation_total`。
- [x] `docs/design/frontend.md`:端点表、HEAD 响应头、CORS 暴露头(8)、409 段落、监控项表、`x-ambry-ttl-ms` 单次写说明。
- [x] `docs/design/replication.md`:复制元数据字段与"墓碑恒胜"。
- [x] 受影响 change 档案(`store-local-worker` / `store-server` / `store-server-mutations` / `store-server-replication` / `store-request-control` / `blob-io-end-to-end` / `balance-and-scrub`)加"后续变更"注记。
- [x] `docs/DEVLOG.md` 追加 2026-09-23 条目。

## 验证

- [x] `crates/blob` 测试全绿(新格式 round-trip、墓碑恒胜单调性/幂等性、前缀负例)。
- [x] `antfs-store` 集成测试全绿(数据面端点表、静默枚举、复制收敛、worker 只剩 flush/compaction)。
- [x] `crates/common/router/tests/e2e.rs` 与 `crates/frontend/tests/rest.rs` 全绿(真 master + 真 store)。
- [x] 旧索引文件(`value_size = 59`)→ 判 `Corrupt` → 从 log 全量重建的成功路径仍绿,并断言新指纹(29 / 21)。
- [x] 崩溃一致性:`crates/blob/tests/crash_consistency.rs` 全绿;`flush` 仍是唯一持久化边界。
- [x] 真集群手测(devcluster):PUT → GET(字节一致)→ HEAD → DELETE 204 → GET 404 → **`POST .../undelete` 不存在**(404/405 而非 200);`x-ambry-ttl-ms` 上传即带 TTL 且到期读 404。
- [x] 跨副本收敛手测:在一个副本上删对象 → 另一副本经拉复制**只写墓碑就收敛**(不拉数据体);两侧重启后墓碑仍在、无复活路径。

## 与原提案的偏差(实现后回填)

1. **没有版本化兼容层**。本 change 实施时**尚无任何已部署数据**(stage 为 design-only),故原提案 D5/D8 设想的兼容机制**全部不做**:
   不 dual-read 旧记录(v3)与旧段头(v1),没有"v1 活跃段只读、升级后滚新段"。段头版本 / CRC 一旦不符即 `Corrupt`(唯一"迁移故事"是索引旧布局 → 判 `Corrupt` → 从 log 重建)。
2. **段头 CRC 位置未变**:仍在 **36..40**,覆盖 `[0,36)`;`key_prefix` 占 **32..36**。原提案 D5 与 Open Question 4 推测 CRC 会因"不能覆盖自身"而后移到 40..44 —— 未发生。
3. **索引 key 保持完整 8B blobId**:条目 **29 B = 8 + 21**,不是"4B 后缀"。原提案把"索引 key 是否也缩到 4B 后缀"记为待办,本次未做。
4. **索引 `flags` 没有"记录版本位"**:原提案 D7 设想用一位标记"该记录是 v4 布局"。实际只存在一种记录格式,该位不需要;`flags` 目前只有 `FLAG_DELETED`。
5. **`MessageInfo.life_version` 一并移除**:原提案 D9 的表述是"去掉 `undeleted` / `ttl_updated` / `operation_time_ms`(并连同 `life_version`)",已照此落地 —— `life_version` 在 `MessageInfo` 上也不存在(它已无生产者与消费者)。
6. **admin 连带已落地**:`crates/admin` 读取的 store 元数据 JSON / `KeyState` 里的 `life_version` 等字段已删除,假 store 夹具同步。
7. **TTL 由 `put_with_ttl` 承接**:frontend 的 `x-ambry-ttl-ms` 从"先 `put` 再 `update_ttl`(两次多数派写)"改为**写入时一次带 TTL**(`Router::put_with_ttl` / `put_file_with_ttl` + 数据面 `PUT ?expiration_ms=`),即单次多数派写。
8. **索引的 `size` 字段语义**(按最终布局):`u32` = **整条记录的字节数**(记录头 + 数据体 + crc),而不是提案里写的"数据体长度 `data_len`"。这使 `seg_bytes` / `valid_data_size` / `verify` 的总长计算无需再按版本查开销。

## 待办(不在本次范围)

- [ ] `expiration_ms` 由 `i64 毫秒` 降为 `u32 秒`(再省 4B/记录 + 4B/索引;Ambry `IndexValue` V1 先例),需与 API 语义一起单独拍板(design D6)。
- [ ] 隐私擦除的段级实现:compaction 时把死记录(含被 DELETE 取代的 PUT 数据体)置零再落盘(design D3 的可逆降级路径)。
- [ ] 索引 key 是否也缩到 4B 后缀(需按 offset 的段号解析所在段前缀,索引查找多一次前缀映射)。
- [ ] 拼装对象的显式类型位(空出的 flags 位;`blob-io-end-to-end` 待议项)。
- [ ] DELETE 重试精确返回 409:router 需要暴露单副本状态,别把单副本冲突收敛成 503(见 `docs/design/frontend.md` §2/§11)。
