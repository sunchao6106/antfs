# frontend(HTTP REST 对象接口层)

> 对标:Ambry `ambry-frontend`(REST 请求 → router → 数据节点)。**实现**:`crates/frontend`;
> **它不自己实现 W/R**——内嵌 [`client`](./router.md)(router SDK),REST 层只做协议翻译与状态码映射。

## 1. 定位

```
curl / 任意 HTTP 客户端 ──▶ frontend ──内嵌 Client(router SDK)──▶ master(GetBlobIds/LookupPartition)
                                    └──────────────────────────▶ store ×N(HTTP 数据面,W 多数派写 / R=1 读)
```

存在的意义(frontend 与 router SDK **并存**,不是二选一,见 `cluster.md §1`):

1. **协议翻译**:REST(将来 S3)↔ router 操作;调用方不必内嵌 Rust SDK。
2. **稳定入口 + 拓扑隔离**:客户端只认一个 URL;选分区、取号段、副本故障切换、重试全在服务端,master 的准入密钥
   与集群拓扑**不下发给客户端**。
3. **将来鉴权/配额/多租户的唯一落点**:antfs 的 blobId 是 64 位、无 account/container 位(D7 布局永久冻结),
   租户信息只能挂网关层(Ambry 用 `AccountService` + `AmbrySecurityService` 做的正是这件事)。
4. **运维面**:CORS、限流、按租户的日志/指标。
5. **联调/压测**:`curl` 就能验收,`wrk`/`hey`/`vegeta` 等通用 HTTP 压测工具可直接打。

## 2. REST 契约

| 方法 | 路径 | 请求 | 成功 | 失败 |
|---|---|---|---|---|
| `POST` | `/blobs` | body = blob 裸字节;可选 `x-antfs-ttl-ms: <i64>`。**上传用 POST**(`curl -T` 发的是 PUT,会 405)。body ≤ `max_body_bytes` 走单 blob;**超过由网关恒切**(2026-10-21 裁决,无开关),边收边分片(见 §5,201 的 `x-antfs-blob-size` = 逻辑总量) | **201** + `Location: /blobs/{blobId}` + `x-antfs-blob-id: {16 hex}` + `x-antfs-blob-size: {n}` | 400(非法 session 头——分片路径校验;单 blob 直写路径无 id/会话参数)/ 413(超**公共单文件上限 `[upload] max_object_bytes`**,REST 与 S3 同一处:有声明读 body 前拒;无声明边收边超,已入库片逐片墓碑回收)/ 409(keyed 写全副本冲突,`router-conflict-surfacing` 2026-10-20 透传;现 REST 路径未部署 keyed 写,契约预留)/ 507(分区写满)/ 503(未达多数派) |
| `GET` | `/blobs/{blobId}` | — | **200** + 原始字节 | 404(所有副本都没有)/ 400(非法 id)/ 502(分片描述符损坏或片缺失)/ 503(副本不可达) |
| `HEAD` | `/blobs/{blobId}` | — | **200** + 无 body;头 `x-antfs-blob-size`/`-deleted`/`-expired`/`-expiration-ms`。**拼装对象额外带 `x-antfs-blob-stitched: true` + `x-antfs-blob-stitch-chunks: N`,且 `size` 是各片之和(逻辑大小,与 `GET` 一致)** | 404 / 400 / 503 |
| `DELETE` | `/blobs/{blobId}` | — | **204**(多数派写墓碑;**删除不可逆**;重复删**幂等**,仍 204) | 404 / 503 / 400 |
| `POST` | `/stitch` | body `{"signedChunkIds":["<16 hex>",…]}`;头 `x-antfs-session: <opaque>` 必填 | **201** + 同 `POST /blobs` 三个头 | 400(session 缺失/空列表/片数 >4096/非法 id/片不可读) |
| `GET` | `/metrics` | — | **200** + Prometheus 文本 | — |
| `GET` | `/healthCheck` | — | **200** + `text/plain` 正文 `GOOD` | — |

> `/healthCheck` 是**进程级探活**(免鉴权、无副作用、不看集群):口径对齐 Ambry rest 的同名端点 —— 只代表 frontend 进程活着,后端(master/store)全挂时也回 200,**不要**当 readiness 用(那会「集群越病越被重启」);K8s readiness 语义将来另立端点。

- **blobId 路径段**:必须能 `hex::decode` 成 **8 字节**,否则 400(文案含 `blob id must be 8 bytes (16 hex)`)。
- **状态码映射**(统一一处):`NotFound → 404`;`StoreFull → 507`;`TooLarge → 413`;
  `Silenced/Quorum/Http/NoReplica/Master/Rejected/NoWritablePartition/IdExhausted → 503`;非法输入 → 400;
  上游数据不一致(描述符损坏/缺片) → 502;其余 → 500。其中 `Silenced` **透传** `Retry-After` 头。
- **副本状态码按语义透传**(不再一律 500):副本回的 `400/413/503/507` 分别映射成同名状态码 —— 这些是
  调用方可控的 payload/容量问题(如"对象太大放不进一个段"是 400、"分区写满"是 507),混进 500 会让
  调用方与压测误判成服务端故障。
- **409(冲突)透传**(`router-conflict-surfacing` 2026-10-20):副本对「已存在 key(存活或已墓碑)的
  再次 `put`」回 409;router 仲裁环对「多数派未达且已收集失败**全为** 409」透传为
  `AlreadyExists` → 本层映射 409(与 store 数据面同码)。混合失败(409 + 网络/其它)仍按
  `Quorum` → 503(判定不了就按暂时性失败,routers 保守)。409 **不计**多数派 ack。
  **`DELETE` 已幂等(2026-09-23 拍板)**:墓碑 key 的重复删由副本直接回 204(不写第二条墓碑)→ 多数派达成 → 本层 204;
  从未存在的 key 仍 404(经多数派聚合为 503,既有口径)。
- **`x-antfs-*` 头命名**:前身沿用 Ambry 的 `x-ambry-*` 头名,2026-10 以 antfs 自有前缀**整体改名**——
  本项目不是 Ambry,头是自己的契约面。改名为不兼容变更(客户端需一次性迁移;开发期项目,不保留旧名别名)。
  成员:`x-antfs-blob-id` / `x-antfs-blob-size` / `x-antfs-session` / `x-antfs-ttl-ms` /`-blob-deleted` /
  `-blob-expired` / `-blob-expiration-ms` / `-blob-stitched` / `-blob-stitch-chunks`。
- **不带 account/container**:blobId 64 位无租户位(D7/D8),路径里不放一个"存不下来又不管事"的段;多租户将来落在
  网关层(那时再加)。

## 3. 写 / 读路径(全在 SDK 里)

| 端点 | 内部动作 |
|---|---|
| `POST /blobs` | 小 body(≤ 1 MiB):`put_auto_with_ttl(bytes, expiration_ms)`(**内部发号 + 分区满 507 自动换分区重试**,最多 2 次);中 body(声明 ≤ `max_body_bytes`):有界内存整收 + `put_auto_with_ttl`(核验一致才写);**超过(自动分片开)或无声明长度**:`stream_upload` 决定窗 + 边收边分片(§5,④,零本地文件)。三条路径写行为对称(**W = 副本数/2+1**,收满即返回,其余靠拉复制补) |
| `GET /blobs/{id}` | router **逻辑流式读** `get_object_stream(&key)`(R=1;拼装自动拼回真数据,内存 = 传输块级,**全程零本地文件**;回 `Accept-Ranges: bytes`) |
| `GET /blobs/{id}` + `Range` | `stat_object`(逻辑大小)→ `get_object_range_stream(&key, start, len)`:单区间 → **206** + `Content-Range`(拼装对象只取**覆盖片**,边界片裁边);越界/空对象/`bytes=-0` → **416** + `Content-Range: bytes */size`;无头/语法坏/多区间 → 200 全对象(对齐 s3 语义矩阵;blob-range-read 2026-10-21) |
| `HEAD /blobs/{id}` | `stat_object(&key)`(逻辑元数据:拼装对象 size = 各片之和、`x-antfs-blob-stitch-chunks`);带 Range 也回 **206** 无 body(Ambry 口径) |
| `DELETE` | `delete`(多数派写墓碑,**不可逆**) |

- `x-antfs-ttl-ms` 的落地方式:**同一次写** —— frontend 把该头解析成绝对 `expiration_ms` 交给 SDK(`Router::put_with_ttl` /
  `put_file_with_ttl`),数据面 `PUT ...?expiration_ms=` 原子承接,故 TTL 是**单次多数派写**。
  **写入之后没有任何改 TTL 的接口**(不能续期、不能作废、不能永久化)。

## 4. 分片拼装(`POST /stitch`)——**无状态**设计

Ambry 把会话信息编进**签名片 id**;antfs 无签名体系(无 ACL/密钥),故:

1. **分片上传** = 普通 `POST /blobs`(带同一个 `x-antfs-session`);片列表由**客户端自己记**,服务端不存 session。
2. **`POST /stitch`**:
   - 校验 `x-antfs-session` 非空、片列表非空且 ≤ 4096、每个 id 是 16 hex;
   - 逐片 `stat` 拿尺寸(拿不到 → 400);
   - 构造**自描述描述符**(小端,经组合写口落位 —— `composed` 类型位随写置上):
     ```
     version: u8 = 1
     chunk_count: u32
     session_len: u16
     session: [u8; session_len]
     × chunk_count: chunk_id(8B) | chunk_size(u64) | expiration_ms(i64)
     ```
     描述符上限 1 MiB(超 → 400);片列表**当 blob 存**是刻意的:中心存一份"片→对象"映射就等于引入
     **每对象元数据**,与 D3 冲突。
   - 因此 `POST /stitch` 是**幂等性无关**的普通写:201 + `Location: /blobs/{stitchedId}`。
3. **读 stitched blob**(2026-10-21 读侧下沉后):**网关不再识描述符** —— 走 router 逻辑口
   (`stat_object` 逻辑大小 / `get_object_stream` 流式拼装 / `get_object_range_stream` 窗口裁片)。
   类型判定(`composed` 位)、描述符解析、覆盖片映射与片完整性核对全在 router(裁决⑩);
   网关只把 router 的错误按 §错误映射表翻译(描述符/片损坏 = `bad_composed` → **502**;
   预检遇到暂时性传输失败 = **503** 可重试 —— 与旧"一律 502"相比,此漂移记录在案:
   片不可读属暂时态,503 语义更准)。
4. **`HEAD` 与 `GET` 对齐(经 `stat_object`)**:置位行逻辑大小 = 各片之和、片数头两只
   (`x-antfs-blob-stitched: true` / `x-antfs-blob-stitch-chunks: N`;router 内对描述符做
   ≤1 MiB 有界读,尽力口径 —— 已删/过期退描述符自身 stat);无位行不读数据体、不带两头;
   带 `Range` 时 HEAD 回 **206** 无 body(blob-range-read D3,Ambry 口径)。
4. **`DELETE` 只删描述符本身,不递归删片**:一次删 N 个对象是危险动作(且片可能被多个 stitched blob 共享);
    孤儿片靠 `x-antfs-ttl-ms` + store 本地 worker 回收。自动分片(§5)产生的对象**删描述符后同样留下孤儿片**
    —— 服务端知悉但故意不递归(无法从描述符区分"片是否被共享"是原因之一;描述符现已带 composed 类型位(`blob-composed-flag`),
    但递归删片仍是危险动作,维持不递归)。

**相对 Ambry 的偏离(登记)**:无签名片 id、无服务端会话(不做"同 session 才可拼"的服务端强制)、不强制分片大小、
不递归删片。

## 5. 流式、自动分片与 body 上限

- 上传**分档**(④ 边收边分片,upload-zero-spool 2026-10-21;阈值 `INLINE_BODY_MAX` = 1 MiB,常量):≤ 1 MiB 走
  **内存路径**(`Router::put_auto_with_ttl(Bytes, ...)`,内部发号 + 507 换分区重试,内存上界 = 在途请求数 × 1 MiB);
  1 MiB < 声明 ≤ `max_body_bytes` 走**有界内存整收**(声明核验一致才写,内存上界 = 在途数 × `max_body_bytes`,与
  S3 网关同口径);超过或无声明长度走 [`stream_upload`] 的**决定窗 + 流式分片(恒切,2026-10-21 裁决废除开关)**(内存 = 窗口 +
  单片,不随对象大小涨)。三条路径的 507 重试行为**完全对称**(最多 2 次换分区)。
  分档原因:每次落系统临时目录文件多出 `create/write/close/unlink` 的元数据开销,实测把 PUT p50 从 ~1ms 拖到
  ~9ms(release,4 KiB × 并发 32)—— **上传已全档零本地临时文件**(旧"上传落暂存文件"机制退役);
  **下载方向同样零盘(2026-10-21 读侧下沉,裁决⑫)**:大对象 GET 不再落系统临时目录中转,
  router `get_object_stream` 直通(窗口读 `get_object_range_stream` 同口径,§3 表);旧
  `TempFile` 中转机制删除。
- `frontend.max_body_bytes`:**内部分档界 + 无声明接收的决定窗**(恒切后不再是 413 契约线)。**契约分界
  只有一条**:公共单文件上限 `[upload] max_object_bytes`(与 S3 网关同一处)—— 声明超限读 body 前 413;
  chunked(无 `Content-Length`)边收边计数,越线即停 → 413 且已入库片逐片墓碑回收(孤儿残余语义同断流,
  收账簿兜底见下条)。**部署对齐要求**:单 blob 与片走同一条数据面 PUT,故
  `store_server.max_body_bytes` 必须 ≥ `frontend.max_body_bytes`(单 blob 不被数据面 413)且
  ≥ `frontend.auto_chunk_bytes`(片同理;devcluster 两侧同为 16 MiB)。
- **服务端自动分片(恒切)**(2026-10-21 裁决:`auto_chunk_enabled` 整字段删除,无兼容包袱;`frontend.auto_chunk_bytes` 定片宽,公共 `[upload] max_object_bytes` 定单文件上限):
  `POST /blobs` 的 body(声明或实收)超过 `max_body_bytes` 时:
  1. **决定窗** = `max_body_bytes`:无声明路径 EOF 在窗内 ⇒ 单 blob(核验后写);越窗即转分片模式
     (buf 自头切片,片边界语义同前)。有声明的大档直接进分片模式且 session 头**先校验**(400 早于
     任何片入库);声明超公共单文件上限在预检处 413,不读 body(配置校验保证上限 ≤ 4096 × 片宽 = 描述符容量);
  2. 攒满 `auto_chunk_bytes` 即 `put_auto_with_ttl` **一片**(顺序写:并发扇出会把"单片内存"乘以片数
     在途,先不做);片与描述符**同 TTL**,整个逻辑对象一体过期(HEAD 可见 `expired` / `expiration-ms`);
  3. **EOF 对账**(实收 == 声明)后编**同 `/stitch` 的描述符**(session = 请求带的 `x-antfs-session`,缺省空)
     也当普通 blob 写入;
  4. 201 返回**描述符的 blobId** + `x-antfs-blob-size: 逻辑总量`;`GET`/`HEAD` 走 §4 的既有透明拼装,
     客户端不感知分片。
  **中途失败**(断流/超发/欠发/片或描述符写失败):对已 ack 的片逐个写墓碑**尽力回收**(best-effort:删失败
  只记日志 + 指标,不影响回报调用方的原错误;片若已落少数副本,墓碑多数派 + 拉复制"墓碑恒胜"会盖掉)。
  **重试安全**:重试拿到一套全新 blobId,与失败轮互不相扰。
  **内存**:决定窗 + 单片(= `auto_chunk_bytes`,校验限 1..=1 GiB),不随对象大小涨;**本地落盘**:零。
  **与旧"先暂存后切片"的净语义差(所有者裁决 ④ 已接受)**:客户端断流时可能已有若干片入库(旧路径整包
  校验后才写,零孤儿)。**收账簿(已于同日裁决「已入库垃圾要删」后实施)**:配 `frontend.upload_session_wal_dir`
  (空 = 不记账、维持 ④ 无兜底)后,start / 逐片 chunk / commit / abort 全程落账(JSONL,逐行 fsync,写线程
  独占);超时孤儿会话(最后记账 6h 无终态)由清扫任务逐片墓碑并核销(启动即一拍 + 每 10 分钟;四类会话判定
  与残余风险[断电线毫秒窗 / 账簿卷蒸发,对照 Ambry 25h TTL 取舍]见 `upload_wal.rs` 模块注释;行业取证见
  upload-zero-spool design §10)。观测:`frontend_upload_chunk_cleanups_total`(断流清理)+
  `frontend_upload_sweeper_runs/tombstones_total`(清扫)。
  **单文件上限**(公共表默认 16 GiB)与描述符容量(4096 × 片宽)由 `FrontendToml::validate_all` 在配置层合闸。
- **读方向**(2026-10-21 更新):router 逻辑流直通(`get_object_stream`),内存 = 传输块级,
  **零临时文件**;小对象与拼装对象走同一条逻辑流(router 决定分岔)
  —— 故 **frontend 侧内存不随对象大小涨**。**但 store 侧的 `GET` 仍把数据体整个读进内存**(真流式 GET 是 store 的待办,
  见 §11),所以端到端内存上界仍由 store 决定(自动分片的 `GET` 同样受此限:逐片 `get`、内存 = 单片)。
- 一次上传的落盘账(2026-10-21 读侧下沉后):frontend **读/写双向零本地文件**(上传 ④ 恒切+收账;
  下载 router 逻辑流直通)+ store 侧暂存 1 份(A 段未做)+ log 1 份 —— A 段落地后收敛为
  "log 一份 = 数据真身"(写放大 2× → 1×)。
  分片形态 = × N 片 + 描述符 1 份(每片普通 blob;A2 压缩帧临时文件已从网关路径消失)。

## 6. CORS

`frontend.cors_allowed_origins` 给出**白名单**才挂 `CorsLayer`(非空时才挂);默认 `[]` = 不挂。
**不用 `permissive()`**:默认放开跨域的 API 是安全问题。放开的方法 `GET/POST/HEAD/DELETE`,
允许的请求头 `x-antfs-session` / `x-antfs-ttl-ms` / `content-type`,暴露响应头 `x-antfs-*` 与 `location`,共 **8 项**(`CORS_EXPOSED_HEADERS`)。

## 7. 与 Ambry frontend 的对照

| 维度 | Ambry `ambry-frontend` | antfs frontend |
|---|---|---|
| 上传路径 | `POST /`(account/container 在 **header**) | `POST /blobs`(**不带** account/container) |
| blobId 谁生成 | frontend 调 router,router **本地随机铸**(partition 从本地 clustermap 随机挑) | **向 master 申请号段**(`GetBlobIds`) |
| 上传结果 | `Location: <blobId>` | `Location: /blobs/{blobId}` + `x-antfs-blob-id`/`-blob-size` |
| 鉴权/配额 | `AccountService` + `AmbrySecurityService` + quota | **无**(多租户推后到网关期) |
| 大对象 | chunked upload + `/stitch`(**签名片 id**;分片在 router 做) | `/stitch`(无签名片 id,描述符自描述)+ **服务端自动分片**(§5,分片在 frontend 做) |
| 元数据/HEAD | `GetBlobInfo`(blob 元数据在 id 内编码 account/container/ttl) | `HEAD /blobs/{id}` → store 的 `.../metadata`(无每对象元数据) |
| CORS | `frontend.options.allow.methods` 等配置 | `frontend.cors_allowed_origins` 白名单 |
| 指标 | Dropwizard `FrontendMetrics` | `FrontendMetrics`(Prometheus,见 §9) |

## 8. 配置项清单

`FrontendConfig`,TOML `[frontend]` 表(CLAUDE.md §7 必须项;与 `crates/frontend/src/config.rs` 的 `impl Default` +
`Validate` 逐项对齐)。

| key(TOML) | 类型 | 默认值 | 取值范围/校验 | 含义/影响 | 对齐的 Ambry 原配置 |
|---|---|---|---|---|---|
| `frontend.listen_addr` | String | `127.0.0.1:7400` | 非空 | HTTP 监听地址 | `frontend.http.port` 等 |
| `frontend.max_body_bytes` | u64 | `4194304`(4 MiB) | ≥1 | **内部分档界**(恒切,2026-10-21:不再是 413 契约线):有声明且 ≤ 此值走"内存整收"单 blob;超过转边收边分片。部署须与 `store_server.max_body_bytes` 对齐 | `frontend.max.json.request.size.bytes` 等(语义不同) |
| `frontend.auto_chunk_bytes` | u64 | `4194304`(4 MiB) | 1..=1073741824(1 GiB) | **恒切的单片大小**(片 = 普通 blob 粒度,也是单片驻留内存的读写缓冲);**描述符容量 = 4096 片 × 此值**(公共单文件上限在配置层须 ≤ 它) | 无(近义的 Ambry `router.max.put.chunk.size.bytes` 在 router 层;router 下沉开工单 = upload-zero-spool design §12) |
| `[upload] max_object_bytes`(顶层公共表) | u64 | `17179869184`(16 GiB) | ≥ 1 且 ≤ 4096 × `frontend.auto_chunk_bytes` | **单文件上传上限**:REST 与 S3 网关共用的契约分界线(`common.toml` 或各网关 TOML 顶层,一处改两边生效);有声明超限读 body 前 413,无声明边收边超、已入库片墓碑回收后 413 | 无 |
| `frontend.upload_session_wal_dir` | String(路径) | `""` | 空 = 不记账;非空 = 目录可建可写(启动校验,失败拒启) | 边收边分片的**孤儿收账簿目录**(Ambry 会话表 + 定期清扫的本地等价物):begin / 逐片 chunk / commit / abort 全程落 JSONL(逐行 fsync,写线程独占);超时孤儿会话(最后记账 6h 无终态)由清扫任务逐片墓碑 + 核销 | 无 |
| `frontend.cors_allowed_origins` | Vec\<String\> | `[]` | 各项为非空且非 `*` 的合法 header 值(校验期就拒) | CORS 来源白名单(**空 = 不挂 CORS 层**;不接受 `*`) | `frontend.options.allow.methods` 等 |
| `frontend.router.master_addr` | String | `127.0.0.1:7101` | 非空 | 内嵌 SDK 的 master 地址(见 [`router.md` §5](./router.md)) | 无 |
| `frontend.router.cluster_secret` | String | `""` | — | 集群准入密钥 | 无 |
| `frontend.router.request_timeout_secs` | u64 | `10` | ≥1 | 单请求超时 | `router.routerRequestTimeoutMs` |
| `frontend.router.id_segment_size` | u32 | `1024` | ≥1 | **均分时**每个可写分区一次申请的 blobId 个数(总预留 = 分区数 × 该值,**实际按写权重配比**;见 [`router.md` §2/§5](./router.md)) | 无 |
| `frontend.router.partition_refresh_secs` | u64 | `30` | ≥0(`0` = 关闭) | 后台刷新可写分区列表的周期(秒;见 [`router.md` §2](./router.md)) | 无 |
| `frontend.router.latency_aware_routing` | bool | `true` | — | 读路径副本排序开关(`up` → 观测延迟 EWMA 升序 → master 原序;只改尝试序不改候选集;见 [`router.md` §5](./router.md)) | 无 |
| `frontend.router.put_bytes_chunk_bytes` | u64 | `4194304`(4 MiB) | `0`(关)或 64 KiB ..= 1 GiB,且**交叉校验:0 或 ≥ `frontend.auto_chunk_bytes`**(违反拒启) | 内嵌 router 的 SDK 恒切片宽(直连语义,见 [`router.md` §5](./router.md))。**交叉校验的存在理由**:网关切片宽 ≤ router 片宽才能保证"网关片在 router 里恒为单笔普通 blob",一旦小于,每片网关片会被 router 二次切成"子片 + 子描述符"而网关读不回原文 —— 配置期结构性禁止(design §12 共存矩阵) | 无 |

> **`[frontend.router]` 是内嵌 router 的配置子表**:W/R、号段大小等只有**一处真源**,frontend 不重复定义
> (W 甚至不可配,见 `router.md §2`)。
>
> **加载要包一层表名**:`frontend.toml` 的 `[frontend]` 表与 `AMBRY_FRONTEND__*` 环境变量在代码里经
> `FrontendToml { frontend: FrontendConfig }` 顶层包装后才生效(直接反序列化 `FrontendConfig` 只认根级字段,
> `[frontend]` 表会被静默忽略)。
>
> 注:admin/master/store 三个二进制原先有同样的静默忽略问题(master/store/admin 的 TOML 表写了不生效、
> 值被 serde 默认掩盖),现已统一改用 `Loader::load_table("<表名>")`(表存在取表、不存在回落 flat),见 common.md。

## 9. 监控项清单

`FrontendMetrics`,接入 frontend 进程的 `Registry`。

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐的 Ambry 原指标 |
|---|---|---|---|---|---|
| `frontend_upload_total` | counter | `outcome` | 次 | 上传请求数(`ok` / 错误 `kind`) | `FrontendMetrics.postBlobRequestMetrics` |
| `frontend_upload_bytes_total` | counter | — | 字节 | 上传字节数(成功;自动分片按逻辑总量) | 无 |
| `frontend_upload_auto_chunk_total` | counter | `outcome` | 次 | 走服务端自动分片路径的上传请求数(成功在 handler 记、失败在 upload 的分片出口记),`upload_total` 的子集 | 无(语义上 Ambry 把 chunk 计数放在 router 层) |
| `frontend_upload_auto_chunk_chunks_total` | counter | — | 片 | **成功**的自动分片上传写入的数据片数(不含描述符) | 无 |
| `frontend_upload_auto_chunk_orphan_deletes_total` | counter | — | 次 | 自动分片中途失败后发起的孤儿片回收删除**尝试**数(best-effort,可能未删成) | 无 |
| `frontend_upload_chunk_cleanups_total` | counter | — | 次 | 边收边分片上传失败后**触发**孤儿片回收的事件数(逐片删除尝试数看上行;观测断流后的孤儿压力面) | 无 |
| `frontend_upload_sweeper_runs_total` | counter | — | 次 | 收账清扫拍数(启用 `upload_session_wal_dir` 后:启动即一拍 + 每 10 分钟;常零命中) | 无 |
| `frontend_upload_sweeper_tombstones_total` | counter | — | 次 | 清扫器成功发出的孤儿片**墓碑数**(删除失败留待下一拍重试) | 无 |
| `frontend_download_total` | counter | `outcome` | 次 | 下载请求数 | `FrontendMetrics.getBlobRequestMetrics` |
| `frontend_download_bytes_total` | counter | — | 字节 | 下载字节数(成功;拼装对象按描述符里各片长度求和) | 无 |
| `frontend_delete_total` | counter | `outcome` | 次 | 删除请求数 | `FrontendMetrics.deleteBlobRequestMetrics` |
| `frontend_mutation_total` | counter | `op`, `outcome` | 次 | `head`/`stitch` 请求数 | `FrontendMetrics.*RequestMetrics` |
| `frontend_request_duration_seconds` | histogram | `op` | 秒 | 请求延迟(含取号段与副本扇出) | `FrontendMetrics.*RequestTimeInMs` |
| `frontend_in_flight_requests` | gauge | `op` | 个 | 在途请求数 | 无 |
| `frontend_replica_errors_total` | counter | `kind` | 次 | 副本侧失败分类(kind 同 SDK 错误表) | 无 |

## 10. 被否决方案

- **给 frontend 加 account/container 路径段**:存不下来(blobId 无租户位)也不管事(无 ACL/配额),
  纯装饰(D8)。
- **`CorsLayer::permissive()`**:默认全开的跨域是安全问题;用显式白名单。
- **服务端 session 表**(分片上传):引入每对象/会话元数据,与 D3 冲突且让 frontend 变有状态;描述符自描述已够。
- **frontend 直接读写 store 字节、不走 SDK**:会把 W/R 语义复制成两份实现(必然漂移)。
- **自动分片放在 router SDK 层**(Ambry `NonBlockingRouter` 的 composite-blob 路线):切片与描述符/拼装
  读回强耦合,而 antfs 的拼装语义(透明拼装、位判定)判定在 frontend —— router 保持纯"恒等翻译
  层"(W/R/发号)。描述符**编解码**在公共 crate `antfs-descriptor`;将来 S3 multipart 等网关需要
  同款能力时直接复用。

## 11. 待办

- [x] ~~拼装对象的类型判定~~ —— **`FLAG_COMPOSED` 位是唯一判定**(`blob-composed-flag` 2026-10-16
      落位:`?composed=true` 组合写口 + `metadata` 回 `composed`;`descriptor-drop-magic` 2026-10-19
      收口:无位行恒按普通对象原字节返回,内容探测退役)。三选项推演与先例出处见
      `openspec/changes/blob-io-end-to-end/tasks.md` 待办首条。
- [x] ~~`Router::put*` 增加 `expiration_ms` 参数~~ —— **已落地为 `Router::put_with_ttl` / `put_file_with_ttl`**,`x-antfs-ttl-ms` 现为单次多数派写。
- [x] ~~读路径端到端流式~~ —— **已完结**(`store-read-streaming` 2026-10-20:引擎
      `read_range` 切片原语 + 数据面 GET 分块流(512 KiB 过每盘有界池,单请求内存 =
      单块,与对象大小无关);`?replication=1` 复制读保持整读原路含 CRC;细节见
      ``store.md`` §12.15)。
- [x] ~~单副本 409 透传~~ —— **已完结**(`router-conflict-surfacing` 2026-10-20:仲裁环
      失败终判新增 `all_already_exists` 特判(与 507 特判同构),已收集失败全为冲突 →
      透传 `Error::AlreadyExists`;混合失败保守 Quorum;409 不计 ack;`router_status`
      增 `AlreadyExists → 409`(现 REST 路径无 keyed 写,契约面预留)。**勾销注记
      (2026-10-20 补):40e8f0a 落地时只改了 §2/§4,本条勾销漏改 —— 文档落后于代码
      一小时,本次补正。**(重复 `DELETE` 已幂等 204,不在此列)。
