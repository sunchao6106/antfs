> **状态:已实施(2026-09-23)。本档的决策 D1–D10 方向全部落地,但落地形态与提案有若干偏差,已在正文内就地标注为"(实现后校正)"。**
> 要点:① **没有任何版本化兼容层**(实施时无已部署数据,不 dual-read v1 段头/v3 记录,也没有"v1 活跃段只读、滚新段升级");
> ② 段头 CRC **仍在 36..40**(覆盖 `[0,36)`),`key_prefix` 占 **32..36**(未按 D5 推测后移到 40..44);
> ③ 索引条目 = **8B 完整 key + 21B value**,不是 4B 后缀;④ 索引 `flags` **无记录版本位**(只有 `FLAG_DELETED`);
> ⑤ `MessageInfo.life_version` 一并移除;⑥ admin 侧连带的字段删除已落地;⑦ TTL 由 `Router::put_with_ttl` 承接(写入时一次带 TTL)。
> 逐项见 [`tasks.md` 的「与原提案的偏差」](./tasks.md#与原提案的偏差实现后回填)。

## Context

`blob` 引擎当前承载三个变更类操作(`undelete` / `update_ttl` / `hard_delete`),它们是"忠实翻译 Ambry"的直接产物,并各自把成本摊进了磁盘格式与外围契约:

- **记录**(`crates/blob/src/blob.rs` 的 `rec` 模块):固定开销 38B = `magic(2) + version(1) + flags(1) + key(8) + expiration(8) + op_time(8) + life_version(2) + data_len(4) + crc(4)`。`op_time` 与 `life_version` 只为"后写覆盖先写的定序"存在。
- **索引**(`crates/blob/src/index/value.rs`,`VALUE_SIZE = 59`):`offset(16) + size(8) + flags(1) + expiration(8) + op_time(8) + life_version(2) + original_offset(16)`;索引条目 = 8B key + 59B value = **67B**。`original_offset` 专门为"墓碑指向被删前的 PUT"存在,消费者有三:拉复制要给已删对象取数据体、`undelete` 要取回原数据、`hard_delete` 要定位原数据体。
- **段头**(`crates/blob/src/log_segment.rs`):64B,`hdr::HEADER_LEN = 36`,CRC 在 32..36 覆盖 `[0,32)`;`HEADER_VERSION` **写了但 open 时完全不校验**。
- **复制合并**(`BlobStore::apply_replicated_inner`):`life_version` 更高者恒赢;同版本下 peer 引入新 flag(删 / TTL)才赢。

用户已拍定**整体简化**:`blob` 不可变 + **删除不可逆**,上述三个操作全部去掉。由此产生的连带是"删掉一条语义 → 一整层机制失去用途":删/复活竞态消失 ⇒ 不再需要 `life_version`;TTL 只在写入时定 ⇒ `operation_time_ms` **彻底失去用途**(已核实:它在当前实现里不参与任何裁决,只写盘、写进复制元数据,`BlobMeta` 都不暴露);对象级擦除消失 ⇒ `original_offset` 的三个消费者全部消失。

### 已核实的关键事实(写决策前逐条查过源码)

- `operation_time_ms` 当前**没有任何裁决作用**:`decode_record` 读出后只被 `IndexValue` 存下、被 `MessageInfo`/复制元数据透传;`BlobMeta` 不含它。故删掉它**不改行为**。
- `apply_replicated` 里"同 `life_version` 下 flag 才赢"是 `life_version` 之外的第二排序键;`undelete` 靠 `life_version + 1` 压制已有墓碑(`undelete_inner`)。两者一起构成本次可整体移除的定序机制。
- `compact` 把**存活记录原样字节拷贝**进新段(`new_log.append(raw)`),不为记录重新编码。(**实现后校正**:原据此推出"段头版本与记录版本必须各自独立解析、v3 记录可与 v2 段共存"—— 因不存在 v1/v3 分支,该要求已不适用;compaction 只涉及 v4 记录。)
- 索引文件头带 `key_size/value_size`(布局指纹),`read_segment_file` 发现不匹配即报 `Corrupt`;`open` 侧有"索引损坏 → 从 log 全量重建"的安全网与测试 `open_falls_back_to_rebuild_on_corrupt_index`。

## Goals / Non-Goals

**Goals:**

- 固化"不可变 blob + 删除不可逆"的语义:删除幂等收敛、复制合并**墓碑恒胜**(单调、幂等、无需定序字段)。
- 固化精简后的三层磁盘格式(段头 v2 / 记录 v4 / 索引 v2)与随之而来的配置约束(`segment_size_bytes ≤ 4 GiB`)。
- 逐项列清"同时去掉的能力"——引擎 API、数据面端点、响应头、router 方法、静默枚举、错误类型、指标、配置项、worker loop —— 使实现与文档可以逐条核对。
- **(实现后校正)** 明确兼容策略:**无版本化兼容层** —— 不做数据迁移、不做 dual-read;唯一兼容面是索引旧布局走"判 `Corrupt` → 从 log 全量重建"。守 [CLAUDE.md §3.4](../../../CLAUDE.md)。

**Non-Goals:**

- **不做数据迁移,也不做 dual-read**(实施时无已部署数据,见 D8 校正)。
- **不做**隐私擦除的段级实现(compaction 置零)——只记录为**可逆的降级路径**(D3)。
- **不做** `expiration_ms` 由 `i64 毫秒` 降为 `u32 秒` 的进一步压缩(记入待办,D6)。
- **不改任何既有配置项的默认值**(尤其 `store.segment_size_bytes` 已是 4 GiB,只加校验)。
- **不动** `segment_size_bytes ≤ 4 GiB` 之外的其他容量规则(残段规则、`total < segment` 退化单段等既有语义不变)。

## Decisions

### D1:删除不可逆(去掉 `undelete`),复制合并退化为**墓碑恒胜**

- 去掉 `BlobStore::undelete` / `undelete_inner` 与 `FLAG_UNDELETED`。删除成为**终态**:没有"从墓碑回到存活"的路径。
- 删/复活竞态在语义上消失 ⇒ `life_version` 失去存在理由(它只为"比较谁更新"而存在),记录与索引里一并删除。
- 复制合并规则(拉复制的 apply 侧)从"比 `life_version` + flags"退化为**墓碑恒胜**,判据只有一条:

  | 本地现状 | peer 记录 | 结果 |
  |---|---|---|
  | 无该 key | 墓碑 | 写墓碑(`Applied`) |
  | 无该 key | 存活 | 写整记录(需 peer 数据体,`Applied`) |
  | 墓碑 | 任意(墓碑/存活) | `Skipped`(**删除不可逆**) |
  | 存活 | 墓碑 | 写墓碑(`Applied`) |
  | 存活 | 存活 | `Skipped`(blob 不可变 ⇒ 同 key 不可能有两次 PUT) |

  该规则**单调**(状态沿 `无 → 存活 → 墓碑` 单向推进)、**幂等**(重复应用同一记录恒 `Skipped`)、**天然收敛**(与 apply 顺序无关)。删掉 `tomb_orig`/`original_offset` 组装、`read_full_record` 倒读原 PUT 的逻辑。
- **业务代价(用户明确接受)**:误删不可恢复,无回收站、无宽限期。数据面/文档须写明。
- **`Error::Deleted` 保留**(读路径据此判"已删不返回数据",store 仍映射 404);`Error::NotDeleted` / `AlreadyUpdated` 随能力移除。

### D2:TTL 只在写入时定(去掉 `update_ttl`)

- 去掉 `BlobStore::update_ttl` / `update_ttl_inner` 与 `FLAG_TTL_UPDATED`。TTL 与数据体一样**不可变**:写完即定,之后无处可改。
- **业务代价(用户明确接受)**:上传后不能改过期时间(不能续期、不能提前作废、不能永久化)。
- **连带一:`operation_time_ms` 彻底失去用途** —— 已核实它不参与任何裁决(只写盘 / 进复制元数据 / `BlobMeta` 不暴露),故**记录与索引都删掉该字段**(各 -8B)。
- **连带二:记录里必须保留 `expiration_ms(i64)`** —— 读路径要能**零 IO 判过期**(`stat` 不读数据体即回 `expired`),且重建索引后不丢 TTL(`ttl_survives_rebuild` 那条测试改为"写入时定的 TTL 重建后仍在")。
- **连带三(frontend 侧)**:`x-ambry-ttl-ms` **保留**(它就是"写入时定 TTL",不受本 change 影响),但前端当前的实现是"先 `put` 再 `update_ttl`"(两轮多数派写)—— 该路径必须改为**写入时一次带上 TTL**,即给 `Router::put` / `Router::put_file` 增加 `expiration_ms` 参数(数据面 `PUT ...?expiration_ms=` 已支持,见 `blob-io-end-to-end` 待办清单),否则 `x-ambry-ttl-ms` 无人承接。

### D3:去掉 `hard_delete`(隐私擦除降级为**段级周期擦除**)

- 去掉 `BlobStore::hard_delete`(整段 COW 重写 + 数据体清零 + `FLAG_HARD_DELETED` 幂等跳过)、`FLAG_HARD_DELETED`、以及本地的 hard_delete worker loop 与它的配置项/指标。
- **降级后的隐私口径**:删除只写墓碑;被删对象的原 PUT 数据体成为该段的**死数据**,由既有 compaction(死占比阈值触发)**回收文件**。文件被 unlink 后,其字节在物理块被复用前**可能仍可取证**。
- **这是一条可逆的降级,不是堵死的路**:将来若需要真正的擦除,落法是"**compaction 时把死记录(含被 DELETE 取代的 PUT 数据体)置零再落盘**" —— 段内其余记录位置不变或整体重排,都**不需要每对象元数据**(不破 [cluster.md D3](../../../docs/design/cluster.md) 的"无中心每对象元数据")。记为待办,不在本次实施。
- 附带收益:`hard_delete` 是当前唯一"就地/COW 改已提交字节 + 原子换 `Inner`"的写路径(并发与崩溃风险最高的一条),移除后"已提交字节永不改写"这条不变式**再无例外**(只剩 compaction 的"造新换旧")。

### D4:去掉索引里的 `original_offset`

`original_offset` 的作用是"墓碑指向被删前的 PUT,以便取回数据体"。它的三个消费者**全部消失**:

| 原消费者 | 本次变化 |
|---|---|
| 拉复制(要给已删对象取数据体,`replicate_from_peer` 的 `need_data` + `?replication=1` 的 `include_deleted`) | 墓碑**只需写状态**:接收方无数据也能收敛 ⇒ 索引里不必记数据体位置;**顺带省带宽**(复制不再拉已删对象) |
| `undelete`(取回原数据复活) | 操作没了(D1) |
| `hard_delete`(定位原数据体去清零) | 实现方式改了(D3) |

故 `IndexValue` 删除 `original_offset`,墓碑条目不再指向任何数据体;`?replication=1` 的复制读**只保留** `include_expired`(过期但未回收的活记录仍需传数据体)。

### D5:段头 v2 —— 每段存 4B `key_prefix`,记录只存 4B 后缀

动机:blobId 是 8B,**同一个副本(同一分区)内所有 key 的高 4B 相同**。把这一份信息**每段存一次**,记录里的 key 就能从 8B 缩到 4B。

- **v2 布局**(`LogSegment`,物理固定 64B 头):

  | 偏移 | 字段 | 说明 |
  |---|---|---|
  | 0..4 | `magic("AMBS")` | |
  | 4..6 | `version` | `= 2` |
  | 6..8 | `reserved` | |
  | 8..16 | `capacity` | |
  | 16..24 | `data_end` | |
  | 24..32 | `created_ns` | |
  | 32..36 | `key_prefix` | **4B 前缀**(blobId 高 32bit) |
  | 36..40 | `crc` | **覆盖 `[0,36)`** |

  > **(实现后校正)** 提案曾推测"CRC 必须后移到 40..44 以覆盖含前缀的 `[0,40)`";落地时 `key_prefix` 占 **32..36**、CRC **留在 36..40** 并覆盖 `[0,36)` —— 同样满足"CRC 不覆盖自身"。头物理仍是 64B(`HEADER_SIZE` 不变),故 `data_capacity = capacity − 64` 与"offset 0 映射到物理 64"全部不变。**没有 v1 分支**:实施时无已部署数据,版本 / CRC 不符即 `Corrupt`。

- **`HEADER_VERSION` 从"写了不校验"变成 open 时校验(实现后校正)**:版本不符 → `Corrupt`(带版本值,便于定位);解析**只有 v2 一条路径**(读回 4B 前缀),**没有 v1 分支**。
- **不破"store 只把 key 当不透明值"这条不变量**([cluster.md §8/D3](../../../docs/design/cluster.md)):store/引擎**只做字节前缀拼接**(`完整 key = prefix ‖ suffix`)与相等比较,**不解析 key 的任何语义**。前缀由**调用方(数据节点)给出**,引擎只负责"读回自己段头里的那一份",本质是"**每副本恒定的 4B key 前缀**"。
  - 最自然的取值是 blobId 的高 32bit(`partitionId` 的大端字节):一个副本恒服务一个分区,blobId 的字节序就是大端 `partitionId ‖ objectId`。**取何值仍由调用方决定**(引擎不关心),具体落法与校验口径见待定项。
- **(实现后校正)只有一种记录格式(v4)**:没有 v3 记录,也不存在"记录版本决定 key 宽度"这回事 —— 后缀总是靠"段头 `key_prefix` + 4B `key_suffix`"拼回完整 key。`compact` 仍**原样拷贝记录字节**。
- **派生约束(实现时必须落实,否则会读出错误的 key)**:
  1. **同一副本的所有段必须携带同一前缀**:`roll_over` / compaction 建新段 / 物理拷贝 bootstrap 都必须传递同一个前缀(compaction 现为原样拷贝记录,若新段前缀不同,v4 记录的 key 就变了);
  2. **open 时校验**"调用方给的前缀"与"既有 v2 段里读出的前缀"一致,不一致直接报错(不静默);
  3. **(实现后校正)写路径仍校验 key 的 partition**:key 的高 32bit 与该副本前缀不符时 → `BadArgument`(绝不静默拼出错 key)。原"v1 活跃段只读、升级后滚新段"与"v4 记录落在 v1 段属非法组合"两条约束**已随"无 v1 分支"而作废**。

### D6:记录 v4 布局(固定开销 38B → **24B**)

```
magic(2) | version(1) | flags(1) | key_suffix(4) | expiration_ms(i64,8) | data_len(u32,4) | data | crc32(4)
   0..2       2           3            4..8                8..16                 16..20        20..      尾 4B
```

- **去掉**:`operation_time_ms(8)`(D2:失去用途)、`life_version(2)`(D1:竞态消失);**key 由 8B 全 key 改为 4B 后缀**(D5)。
- **CRC 覆盖面不变**:仍覆盖"整条 header + data"(不含尾部 CRC 自身),故"坏记录即 `Corrupt`"的检出口径不动。
- **(实现后校正)** `RECORD_OVERHEAD = 24` 是**唯一**值(没有"按版本取值");记录物理总长 = `RECORD_OVERHEAD + data_len`(含尾 crc)。
- **`expiration_ms` 保留 i64 毫秒**(与 API/`x-ambry-ttl-ms` 的精度一致)。
- **显式记录而不做的进一步优化**:`expiration_ms` 降为 `u32 秒`(Ambry `IndexValue` V1 就是这么改的)可再省 **4B/记录 + 4B/索引**,但它改 TTL 精度与对外 API 语义,记入待办(不在本次)。

### D7:索引 v2(条目 67B → **29B**)

`IndexValue`(59B → **21B**)+ 8B 完整 key ⇒ 索引条目 **29B**。索引**仍以完整 8B `StoreKey` 为查找键**(它是全局查找键,跨越多个段;段内前缀只有 log 段才知道),只精简 key 之外的部分(该口径由"67B → 29B"的差额确定:value 恰好减 38B;若要让索引也只存 4B 后缀,需按 offset 的段号去解析所在段的前缀 → 已列入待办):

```
value(21B): offset(8, 打包) | size(u32,4) | flags(u8,1) | expiration_ms(i64,8)
entry(29B): key(8) | value(21)
```

- **`offset` 打包成一个 u64**:`(段号 u32 << 32) | 段内偏移 u32`,替代原"段号 u64 + 段内偏移 u64"(16B → 8B)。内存里的 `LogOffset { segment, offset }` 表示不变,只在索引编解码处打包/解包。
- **(实现后校正)`size(u64)` → `size(u32)`**:索引存的是**整条记录的字节数**(记录头 + 数据体 + crc),读路径据此定位下一条记录 —— 不是提案里写的"数据体长度 `data_len`"。
- **(实现后校正)`flags` 没有"记录版本位"**:只有一种记录格式、开销恒为 24 B,不存在"按记录版本算总长"的需求。
  - 位分配:`bit0 = FLAG_DELETED`(**唯一**保留的标志);其余位空闲 —— 留给 `blob-io-end-to-end` 待办里那条待议的"**拼装对象类型位**"。
- **删掉**:`original_offset(16)`(D4)、`life_version(2)`(D1)、`operation_time_ms(8)`(D2)。
- **保留 `expiration_ms(i64)`**:读路径零 IO 判过期;重建索引后 TTL 不丢。
- `.idx` 段头里的 `key_size/value_size` 就是布局指纹,`IDX_VERSION` 1 → 2(v2 写出的 `value_size = 21` 与旧值 59 不符,旧文件被判 `Corrupt` → 走重建,见 D8)。
- **由此推出新的配置约束:`segment_size_bytes ≤ 4 GiB`**(在 `StoreConfig::validate` 里**显式校验**;默认值已是 4 GiB,**不动**)。它一条同时保证三件事:
  (a) **段内偏移 < 2³²** ⇒ 偏移放得进打包后的 u32 半区;
  (b) **单条记录 ≤ 4 GiB** ⇒ `data_len`(u32)**与"记录总长"**都放得进 u32;
  (c) **单条记录数据体 < `u32::MAX`** ⇒ 与刚落地的 `record_data_len` 校验一致(后者从"兜底"变成"结构性不可能"+第二道保险)。

### D8:格式兼容策略 —— **实现后校正:没有版本化兼容层**

实施时**无任何已部署数据**(stage 为 design-only),故原提案"读旧写新 + 不迁移"的整套机制**不做**:

| 层 | 读路径(落地) | 写路径 |
|---|---|---|
| 段头 | **只认 v2**:版本 / CRC 不符 ⇒ `Corrupt`,**没有 v1 分支** | 只写 **v2** |
| 记录 | **只认 v4**:未知版本 ⇒ `Corrupt`,**没有 v3 分支** | 只写 **v4** |
| 索引 | **不解析旧布局**:`.idx` 的 `key_size`/`value_size` 指纹不符 ⇒ `Corrupt` ⇒ **从 log 全量重建**(既有安全网与测试 `open_falls_back_to_rebuild_on_corrupt_index`)。效果上等价于"旧索引照样可用",走的是重建而非旧读路径 | 只写 **v2** |

- **不做数据迁移,也不做 dual-read**;"v1 活跃段只读、升级后滚新段"这条机制**不存在**(无 v1 分支)。
- 唯一保留的兼容面是**索引**那条安全网:`value_size` 59 → 21(entry 29、`IDX_VERSION` 2),旧文件判 `Corrupt` → 从 log 重建。
- 持久化边界不变:仍以 `flush` 推进 header 的 `data_end` 为唯一持久边界,段头新增字段不改变"恢复严格截断到上次 flush 的边界"。

### D9:同时去掉的能力清单(影响面,逐条核对用)

| 位置 | 移除内容 |
|---|---|
| `blob::index::value` flags | `FLAG_TTL_UPDATED` / `FLAG_UNDELETED` / `FLAG_HARD_DELETED` 全删,**flags 只剩 `FLAG_DELETED`**(无记录版本位,见 D7 校正);空出的位留给 `blob-io-end-to-end` 待议的"拼装对象类型位" |
| `blob::BlobMeta` | 去掉 `undeleted` / `ttl_updated` / `life_version`(保留 `deleted` / `expired` / `expiration_ms` / `data_len`) |
| `blob::MessageInfo` | 去掉 `undeleted` / `ttl_updated` / `operation_time_ms`(**并连同 `life_version`** —— 它已无生产者与消费者) |
| `blob::Error` | 去掉 `NotDeleted` / `AlreadyUpdated`;**保留 `Deleted`**(读路径判"已删、不返回数据";store 仍映射 404) |
| `blob` 指标 | `store_hard_delete_total` / `store_hard_delete_duration_seconds` |
| `blob` API | `undelete` / `update_ttl` / `hard_delete`(`put_ttl` / `put_from_reader` 的"写入时定 TTL"能力保留) |
| store 数据面 | `POST .../blobs/{key}/undelete`、`POST .../blobs/{key}/ttl?expiration_ms=` 两条端点及其 handler;`?replication=1` 的 `include_deleted` 语义 |
| store 契约/DTO | `MetadataDto` 的 `undeleted` / `ttl_updated` / `life_version`;`MetadataEntry` 的 `ttl_updated` / `undeleted` / `life_version` / `operation_time_ms`;`map_mut_err` 的 `NotDeleted`/`AlreadyUpdated` 分支 |
| store 静默 | `DataOp::Undelete` / `DataOp::TtlUpdate`(`ALL` 5 → 3;`classify` / `as_str` / `parse` 的分支)。变更类静默**只剩 `delete`** |
| store worker/配置/指标 | `hard_delete_all` 与第三条 loop;`store_server.hard_delete_interval_secs`(含 `Default` / `Validate`);`store_server_hard_deleted_total`;`store_server_mutations_total` 文案收窄为"delete 变更" |
| router | `Router::undelete` / `update_ttl`;`mutate_one` 的 `"undelete"`/`"ttl"` 分支与 `expiration_ms` 参数;`BlobStat` 的 `undeleted` / `life_version`;`router_mutation_total{op}` 的取值集合收窄;**变更类只剩 `delete`** |
| frontend | `POST /blobs/{blobId}/undelete`、`POST /blobs/{blobId}/ttl` 两条路由与 `undelete_blob` / `update_ttl` handler;响应头 `x-ambry-blob-undeleted` / `x-ambry-blob-ttl-updated` / `x-ambry-blob-life-version`(及其 CORS 暴露项、HEAD 响应);`FrontendMetrics` 的 `OP_UNDELETE` / `OP_TTL`。**`x-ambry-ttl-ms` 保留**(写入时定 TTL),但上传路径改为写入时一次带上(见 D2 连带三) |
| admin(连带) | `crates/admin/src/scrub.rs::KeyState.life_version`、`server.rs` 对 `replica-metadata` 的 `life_version`/`undeleted`/`operation_time_ms` 反序列化与假 store 测试夹具 |

### D10:被否决方案(记录在案)

| 方案 | 否决理由 |
|---|---|
| **只删 `operation_time_ms`,保留 `undelete`/`update_ttl`/`hard_delete`** | 收益最小(省 8B/记录 + 8B/索引)却**仍要动全部三层格式**(记录、索引、段头都对不上),等于把一次格式演进的成本花在最小的目标上;而且删/复活竞态、每对象擦除这两个**真正的复杂度来源**原封不动 |
| **保留 `original_offset`,只去掉 `undelete`** | 去掉 `undelete` 后它的消费者只剩 `hard_delete` 一个 —— 花 16B/条目的索引空间养一个消费者,不如让"对象级擦除"整体降级为段级(D3),三个消费者一起消失 |
| **把 `expiration` 也从记录与索引里去掉** | 读路径就必须**读数据体 / 扫段头**才能判过期,`stat` 与 HEAD 的"零 IO 判过期"立刻失效;**重建索引后 TTL 丢失**(原本有效的 TTL 变成永不过期)—— 既是功能倒退,也踩隐私/合规预期(该过期的数据留了下来)。故 D2/D7 都**保留** `expiration_ms` |
| **`expiration_ms` 改为 `u32 秒`**(D6 提到的进一步压缩) | 省 4B/记录 + 4B/索引,但改 TTL 精度(秒级)与对外 API 语义(`x-ambry-ttl-ms` 是毫秒),宜与 API 一起单独拍板 → 记入待办,不在本次 |

## Open Questions(实现后:已全部拍板)

1. **`delete` 的幂等口径(2026-09-23 改判)**:删**不存在**对象 → `NotFound`(404);删**已墓碑**对象 → **幂等成功(204,不写第二条墓碑)**。
   原口径"墓碑再删 → `Deleted`(409)"已废弃:blob 引擎 `delete` 对墓碑 key 直接 `Ok(())`,store→router→frontend 全链路 204。
   从未存在的 key 经多数派聚合仍表现为 503(既有口径,未动)。
2. **`key_prefix` 的取值与来源(已定)**:取 blobId **高 32bit**(该副本的 partitionId,由调用方即数据节点作为显式参数给出);`open` 时与既有段读回的前缀核对,不一致 → `Corrupt`。
3. **索引 flags 的 v4 布局位(已取消)**:只存在一种记录格式,该位不需要;`flags` 仅 `FLAG_DELETED`。见 D7 校正。
4. **段头 v2 的 CRC 位置(已定,与原推测不同)**:`key_prefix` 占 **32..36**,CRC **留在 36..40**(覆盖 `[0,36)`);原推测的 40..44 未采用。见 D5 校正。
5. **"v1 活跃段只读、升级后滚新段"(已取消)**:无 v1 分支,该机制不存在。见 D8 校正。
6. **admin 侧连带(已含在范围)**:`life_version` 等字段的读取与假 store 测试夹具已同步删除。
7. **`store_server.hard_delete_interval_secs` 移除(已定)**:直接删除,不留"配置兼容期"(无已部署数据)。

已记录、但不属本次范围的后续项(清单见 [`tasks.md`](./tasks.md) 末节):`expiration_ms` 的 `u32 秒`化(D6)、隐私擦除的段级实现(D3 的可逆降级路径)、索引 key 是否也缩到 4B 后缀、拼装对象的显式类型位。
