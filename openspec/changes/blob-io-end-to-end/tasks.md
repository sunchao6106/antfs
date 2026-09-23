# Tasks

> **后续变更(2026-09-23,`simplify-delete-model`)**:本清单里「复活」/「改 TTL」(REST 端点、router 方法、HEAD 响应头、
> 真集群手测里的"复活/TTL"步骤)已随"对象不可变 + 删除不可逆"删除;`x-ambry-ttl-ms` 改为**写入时一次带 TTL**
> (`Router::put_with_ttl`)。清单保留为实现当时的记录。

## master:发号(写路径入口)
- [x] `proto`:新增 `rpc GetBlobIds` + `GetBlobIdsRequest/Response`(version 字段守 §3.4)
- [x] `idalloc.rs`:leader 内存号段池(`Pool`)+ 抬水位(`AllocateObjectId`)+ 饱和批次 + 耗尽回退 + 空洞计数;单测(池算术/批次饱和/错误文案)
- [x] `config.rs`:`master.id_alloc_segment_size`(1024)/ `master.id_alloc_raft_batch_multiple`(2)+ 校验
- [x] `metrics.rs`:5 个发号指标(requests/allocated/failures/raft_commits/holes)
- [x] `server.rs`:handler(leader-only + 准入密钥)+ `pick_writable_partition` 按**剩余容量加权**(优先有存活副本的分区,空则退回全部 RW);`AssignPartition` 行为不动
- [x] 集成测试 `tests/blob_ids.rs`(6 个):号段连续/无重叠/摊薄提交、空洞计数、容量加权、**空池从已提交水位续发绝不重用**、耗尽不回绕、follower 拒绝并回填 leader

## store:大文件与可观测
- [x] `PUT ...?expiration_ms=`(建对象即带 TTL)
- [x] `GET .../blobs/{key}/metadata`(只读元数据不读数据体;墓碑/过期仍 200)
- [x] 流式 PUT:带 `Content-Length` 时边收边落 `<disk>/tmp/` 暂存文件 → 引擎 `put_from_reader`;无 CL 回落缓冲路径;暂存文件成功/失败都删 + `DiskManager::mount` 清崩溃遗留
- [x] `Error::StoreFull` → **507**
- [x] 测试:3 MiB 流式往返 + 暂存目录无残留、超限 413、短读 400 不产生对象、`?expiration_ms=` 生效、metadata 端点(含墓碑)、写满 507

## blob:流式写原语
- [x] `LogSegment::{reserve,write_reserved,commit_reserved}`(预留期不发布 `data_end`)
- [x] `Log::{reserve,Reserved}`(与 `append` 同样的跨段/容量判定)
- [x] `BlobStore::put_from_reader`(按已知长度顺序写 + CRC 增量 + `publish_put` 收口);`encode_header_into` 抽取 + `NO_EXPIRATION` 导出
- [x] 测试 `tests/stream_put.rs`(7 个):与整块写逐字节等价、跨段滚动、读流出错不发布、非法长度、AlreadyExists、TTL、重开可读

## router(客户端 SDK / 通用组件)
- [x] **纯库、无二进制**,落在 `crates/common/router`(package `antfs-router`,主类型 `Router`)—— 它是客户端侧通用组件,
      被 `crates/frontend` 与 `benchmark/router` 依赖;由原 `crates/client` 重构而来
- [x] `RouterConfig`(`[router]` 表:master_addr/cluster_secret/request_timeout_secs/id_segment_size;**无 W/R 配置**)
- [x] `Router`:号段缓存(单飞取段)、`replicas_for`(拓扑缓存 + `up` 优先排序)、`put`(W=副本数/2+1,收满即返回)、
      `get`(R=1 首个成功)、`delete`(多数派)、`stat`、`put_file/get_file`(流式)
- [x] 错误分类(`Error::kind()` 稳定契约)+ `RouterMetrics`(9 项)
- [x] 集成测试 `tests/e2e.rs`(5 个,真 master + 真 store):往返(W 实算)、**一个副本挂掉仍能写(acks=2)且读换副本**、
      RF=1 只需 1 ack、号段唯一且都落在可写分区、缺失对象 NotFound / 未知分区不静默

## frontend:HTTP REST 入口
- [x] `FrontendConfig`(`[frontend]` + `[frontend.router]` 子表复用 `RouterConfig`)+ CORS 白名单(空 = 不挂;拒 `*`)
- [x] REST 端点:`POST/GET/HEAD/DELETE /blobs[/{blobId}]`、`POST /stitch`、`GET /metrics`
- [x] 流式上传(落暂存文件再 `put_file`,内存不随文件大小涨)+ `max_body_bytes` 校验(413)
- [x] 分片拼装(无状态描述符当 blob 存 + 读时流式拼接 + 502 语义)
- [x] `FrontendMetrics` 9 项
- [x] 集成测试 `tests/rest.rs`(9 个):3 MiB 往返、HEAD、删除、TTL(写入时)、**stitch 三片拼装一致**、400、413、副本挂一个仍 201/200、`/metrics`

## benchmark:压测二进制(独立目录,不在库里)
- [x] `benchmark/common`(`antfs-bench-common`):负载生成、逐操作延迟收集、报表、极简 flag 解析
- [x] `benchmark/router`(`antfs-bench-router`):**直连模式**端到端压测(取号段 + W 写 + R=1 读 + `--verify`)
- [x] `benchmark/frontend`(`antfs-bench-frontend`):**REST 模式**端到端压测(只打 HTTP,不内嵌 SDK)
- [x] 每个压测 crate = **lib(压测逻辑)+ 薄 main(解析 flag)**:devcluster 的 `--run-bench` 复用同一份逻辑,不重复实现
- [x] `benchmark/blob`(既有,`antfs-bench-blob`)保持:单机引擎层压测

## devcluster / 联调
- [x] devcluster 一并起 frontend(`--frontend-port`,默认 7400)并在 summary 打印 curl 与两条 bench 命令
- [x] devcluster `--run-bench router|frontend`(+ `--bench-*` 参数):起全栈 → 等就绪 → 压一轮(带 verify)→ 打印 → 退出
      (**同进程**压测,数字只作冒烟;正式压测另起进程)
- [x] 建分区后等各 store 真把副本建出来(`ReplicaManager::count()`)再报"就绪"(否则首个 PUT 会 404)
- [x] dev 环境显式把 `max_body_bytes` 放到 16 MiB(不改默认值)
- [x] summary 修正为完整的直连/REST 两条路径(并修掉过期的 key 示例)

## 文档
- [x] 新增 `docs/design/router.md`(角色、写读路径、错误契约、配置项清单、监控项清单、与 Ambry 差异、待办)
- [x] 新增 `docs/design/frontend.md`(REST 契约表、分片设计、配置项清单、监控项清单、与 Ambry 对照、被否决方案)
- [x] `docs/design/common.md`:公共库索引补 `router` 行(它是 `crates/common/` 下的公共能力)
- [x] `docs/design/cluster.md`:**删掉「client SDK 取代 frontend」「去掉 frontend 的连带」**,改为 router + frontend 并存;
      §7.4 加 `GetBlobIds`;§7.5 时序;§7.7 配置/监控各补;§9.1 store 端点与配置(含 507、metadata、流式 PUT);§10 W=副本数/2+1
- [x] `docs/design/README.md` + 根 `README.md` 模块表:frontend 从"待添加/规划中"改为已实现;`client` 行改为 `router`
- [x] `docs/design/store.md`:`max_body_bytes` 语义更新(流式路径按 `Content-Length` 前置校验)+ 压测入口指向 bench
- [x] `benchmark/blob/{README.md,src/main.rs}` 过期表述 → 指向新的 bench 二进制
- [x] `openspec/changes/blobid-master-assign/{design.md,proposal.md}`:D9 配置 key 扁平化 + 标注发号已落地
- [x] 本 change 档案(`proposal.md` / `design.md` / `tasks.md`)

## 验证
- [x] 单 crate:`antfs-blob`(7)、`antfs-master`(6 集成 + 5 单测)、`antfs-store`(15 + 4)、`antfs-router`(5)、`antfs-frontend`(9 单测 + 12 集成)全绿
- [x] `cargo test --workspace` 全绿;`cargo clippy --workspace --all-targets` 无本 change 引入的告警(仅剩 `crates/blob` 3 条既有告警)
- [x] 真集群手测(devcluster,release,`/tmp/antfs-e2e`,6 分区 RF=3):
      REST 全流程 — 3 MiB 上传(201 + `Location`/`x-ambry-blob-id`/`x-ambry-blob-size`)→ 下载 `cmp` **逐字节一致** →
      HEAD 元数据 → DELETE 204 → GET 404 → HEAD `deleted:true` → 缺参/非法 id 400 → `/metrics` 有 `frontend_*{outcome}`;
      **stitch**:3 MiB 切 32 片逐片上传 → `POST /stitch` → 取回**逐字节一致**,缺 session / 空片列表均 400
- [x] `devcluster --run-bench router|frontend`:一条命令起全栈 → 等就绪 → 压一轮(带 `--verify`)→ 打印 → 退出
- [x] 压测数字(4 KiB 对象,并发 8,release,单机 5 进程共抢一张盘 ⇒ **只作冒烟**,正式压测须压测端与服务端分机):
      `antfs-bench-router` PUT 14.6k ops/s / 57 MB/s(p50 0.37ms)、GET 61k ops/s(**err 0**);
      `antfs-bench-frontend` PUT 11.7k ops/s / 46 MB/s(p50 0.43ms)、GET 27.7k ops/s(**err 0**)
- [x] 调优证据(自适应大小分档 + 去掉暂存文件的多余 fsync):并发 32 下 PUT **1.77k → 15.7k ops/s**、p50 **13.4ms → 1.46ms**
- [x] 反证:RF=1 分区写成功且 `required=1`(W 实算,`crates/common/router/tests/e2e.rs`);
      一个副本挂掉仍能写(RF=3 下 `acks=2`)且读自动换副本(router + frontend 两侧测试);
      写满分区 → **507**(`crates/store/tests/data_plane.rs`);被静默 → 503 + `Retry-After`(frontend 测试)
- [x] 大文件内存不随大小涨:store 与 frontend 的 >1 MiB 路径均为"落盘暂存 + 引擎按长度顺序写",单测断言暂存目录无残留
- [ ] 分机压测(压测端独立于服务端)/ 更多并发档位:留给后续(单机上并发 32 会出现连接层失败,
      是"5 进程 + 压测端共抢 CPU"的资源现象,服务端侧错误计数为 0、在途请求归零)

## 本轮 bug 排查(fix + 回归测试)

> 背景:用户要求"全面测试、找 bug"。以下每条都是**先复现/先取证再修**,并留下能抓住它的测试。

| # | 缺陷 | 取证 | 修复 | 回归测试 |
|---|---|---|---|---|
| 1 | **取消的上传漏暂存文件**:客户端断开/上游取消会 drop 掉 handler future,`.await` 之后手写的 `remove_file` 永不执行 | 2 MiB × 3000 次压测后暂存目录残留 **1624** 个文件 | store 侧改 **RAII `StagedFile`(Drop 删)** | `cancelled_upload_leaves_no_staging_file`(禁用 Drop 会红);集群级复测残留 **0** |
| 2 | **小对象也落暂存文件**:每个 PUT 多 ~5 次文件系统元数据操作 | 并发 32 下 PUT 仅 1.77k ops/s、p50 13.4ms | 自适应分档(≤1 MiB 走内存,>1 MiB 才落盘流式) | `put_with_content_length_streams_body_...` + 压测复测 **15.7k ops/s / p50 1.46ms** |
| 3 | **暂存文件上多余 fsync** | PUT p50 248ms(单次串行仅 0.56ms) | 去掉 `sync_all`(中转文件不是持久化边界) | 同上压测复测 |
| 4 | **短读被当成功写入**:走内存路径后 `to_bytes` 不校验 `Content-Length` | 声明 1000 字节、实发 10 字节 → 201 | 两侧都校验实际字节数,不符即 400 | `put_with_mismatched_content_length_creates_nothing`(store)+ 同类 frontend 测试 |
| 5 | **拼装对象的 `HEAD` 报描述符大小**(785B)而非逻辑大小(3 MiB),与 `GET` 错配 | 真集群 curl `HEAD` vs `GET` | `HEAD` 也嗅探 MAGIC,报 `Σ chunk_size` + `x-ambry-blob-stitched`/`-stitch-chunks` | `stitch_concatenates_chunks_in_order` 增断言(含普通对象不受影响) |
| 6 | **"记录放不进一个段"返回 500** | body 2 MiB / 段 1 MiB → 500 | `Error::BadArgument` → **400** | `record_too_big_for_segment_is_400_not_500` |
| 7 | **`partition_id=0` 既表示第 0 号分区又表示"自动挑"** | 点名第 0 号分区被当成自动挑(测试直接撞上) | 改 proto3 **`optional uint32 partition_id`**,缺省才是自动 | `partition_without_replica_is_never_offered` + 既有各测试改传 `Option` |
| 8 | **无副本的分区被当作可写候选**(迁移/下线中可能出现瞬时 0 副本) | 移除唯一副本后自动挑仍会挑它 → 发一段必然写不进去的号 | 自动挑选与显式点名都要求 **≥1 副本** | `partition_without_replica_is_never_offered` |
| 9 | **副本的 400/413 被 frontend 吞成 500** | 代码审查 + #6 的链路推演 | `router_status` 按语义透传 400/413/503/507 | frontend 状态码映射单测 |
| 10 | **每次成功上传打一条 INFO 日志**(2 万次上传 = 2 万行,日志成热点) | `--run-bench` 输出里逐条 `blob stored` | 改 `debug!` | — |
| 11 | **整块写路径缺 4 GiB(u32)上限校验**:`encode_record` 里 `data.len() as u32` **静默截断**,而流式路径有检查 → 段配 >4 GiB 时写一条 4 GiB+ 记录会落盘 `data_len` 回绕的记录,之后读取一律 `Corrupt`(持久性红线) | 代码审查(`blob.rs` 两侧不对称) | 记录长度策略抽成 `record_data_len(len) -> Result<u32>`,`encode_record` 改返回 `Result<Vec<u8>>`(**6 个调用点**都要处理,单一咽喉点不可绕过),流式路径共用同一策略;删掉两处 `as u32` 截断 | `record_len_tests::{record_data_len_accepts_up_to_u32_max_and_rejects_more, bulk_put_path_rejects_oversized_slice_via_policy}`(只测长度策略,无需真分配 4 GiB) |

> **配套的默认值改动(用户指示)**:`StoreConfig::segment_size_bytes` 默认 **8 GiB → 4 GiB**。理由:
> 段 ≤ 4 GiB 时单条记录必然 ≤ `段 − 102 < u32::MAX`,"长度回绕写坏记录"在**结构上**不可能(上面的显式校验退化为第二道保险);
> 4 GiB 也是满足该不变量的**最大值**(2/1 GiB 虽也满足,但滚动更频繁、索引文件对数翻倍),且容量粒度比 8 GiB 更细。
> 文档同步:`docs/design/store.md` §10.1 配置表(原先还写着"待定")。
>
> **另一处容量规则放宽(用户指示)**:`Log` 不再要求"总容量是段容量的整数倍" —— 末段为**残段**,
> 装不下记录时按写满处理(`SegmentFull` → `StoreFull`);`total < segment` 退化为单段(对齐 Ambry
> `segmentCapacity = min(capacity, segment)`)。这样段大小与分区容量两个配置项**解耦**
> (100 GiB + 8 GiB = 12 整段 + 1 个 4 GiB 残段,过去是直接 `BadArgument`)。
> 实现:`crates/blob/src/log.rs`(`validate_capacities` 放宽 + `div_ceil` 段数 + `roll_over` 算残段容量 +
> `max_segment_capacity` 区分"参数错"与"写满");测试 `crates/blob/tests/segment_tail.rs`(5 条:残段几何、
> 残段过小→写满且不建空段、`total < segment` 单段、重开几何不变、引擎层报 `StoreFull` 而非 `BadArgument`)。

**审查后确认"不是 bug"的两处**(避免误修):
- store 不校验 `blobId 的高 32 位 == URL 里的 partition`:按 `cluster.md §8`,**store 只把 8B key 当不透明值**,
  解析字段是 router/master 的事;加这道校验会让"store 当不透明存储"的约定破裂(且会波及全部测试夹具)。
- 2 MiB 对象写几十个就 `store_full`:dev 分区 64 MiB ÷ 段 8 MiB = **最多 8 段**,是"定容 + 写满即拒"的**正确**行为
  (盘本身还剩 169 GB,不是 ENOSPC)。

## 待办(不在本次范围)

- [ ] **【待议,已有完整上下文】拼装对象的类型判定:靠"嗅探数据体前 4 字节"而非显式标志位**
      - **问题**:描述符本身是普通 blob,"它是清单还是数据"没有地方标 → 读路径只能看 `MAGIC "ASTC"` 猜。
        风险窗口 = **任何 ≤1 MiB 且前 4 字节为 `ASTC` 的普通 blob**(描述符按构造 ≤1 MiB,故更大的对象不嗅探)。
      - **两个已复现的后果**(devcluster + curl,2026-09-22):
        ① 100 字节普通文件恰好以 `ASTC` 开头 → 经 frontend 读回 **502**(`corrupt stitch descriptor`),
        但直连数据面仍 200 且字节一致 —— **数据没丢,是网关拒绝服务**;
        ② 把一个**恰好合法**的描述符(63 字节)当普通文件上传 → 读回 **200 但内容是它引用的两片数据**
        (100×`A` + 50×`B`)—— **静默返回错内容**(confused deputy)。
      - **选项**:
        A. **记录头加类型位**(推荐):`crates/blob/src/index/value.rs` 的 `flags` 只用 `1<<0..1<<3`,**bits 4–7 空闲**;
           加 `FLAG_COMPOSED = 1<<4` → `PUT ...?composed=true` 置位(对齐 SeaweedFS `?cm=true`)→
           `.../metadata` 回 `composed: bool` → frontend 的 HEAD/GET **不再嗅探**。
           布局不变(§3.4 安全:旧读者忽略未知位)。代价:数据面多一个"应用语义位"(对比 delete/TTL 属存储语义)。
        B. 网关侧存 `id → 片列表`(filer 式):一次也不用描述符,但引入**中心化每对象元数据**,与 D3 相悖。
        C. 不做分片:对象大小 = 单条记录上限(`segment_size_bytes`/`max_body_bytes`),歧义自然消失,但大对象传不了。
      - **先例(已核实本地源码)**:SeaweedFS **从不嗅探**,类型由两层显式标志承载 ——
        volume 层 needle 头 `Flags` bit `0x80 = FlagIsChunkManifest`(`weed/storage/needle/needle_read.go:22`,
        上传 `?cm=true` 置位于 `weed/storage/needle/needle.go:97-99`,读时判定于
        `weed/server/volume_server_handlers_read.go:315-318`);filer 层 `FileChunk.is_chunk_manifest`
        (`weed/pb/filer.proto:163`)。**说明"记录自带一位语义"是被业界接受的落法,不违反 D3**
        (D3 反的是中心化每对象元数据服务)。
      - **待拍的点**:① 数据面是否接受这个"应用语义位";② 存量拼装对象没有该位 → 选"flag 优先 + 无位兜底嗅探"
        (平滑,存量仍有歧义)还是"不做兼容/重写"(需数据迁移)。
- [ ] 分机压测与更高并发档位(单机上并发 32 会撞连接层失败,服务端错误计数为 0)
- [ ] leader 重定向:`leader_addr` 回填的是 raft 地址(既有写 RPC 的共性问题),router 目前不自动改拨
- [ ] 号段后台预取(D2 双 buffer)、读路由缓存 TTL / 失败后强制刷新
- [ ] in-log 预留槽零拷贝写(见 design.md D5 被否决方案)
- [ ] 真流式 GET(store 的 `GET` 仍把数据体整个读进内存)
- [x] ~~`Router::put*` 增加 `expiration_ms`~~ —— **已落地为 `Router::put_with_ttl` / `put_file_with_ttl`**(写入时定 TTL,`x-ambry-ttl-ms` 已是单次原子写;见 `simplify-delete-model`)
- [ ] 分片:签名片 id、服务端会话、递归删片;S3 兼容层;多租户 ACL/配额
