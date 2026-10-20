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
| `POST` | `/blobs` | body = blob 裸字节;可选 `x-antfs-ttl-ms: <i64>`。**上传用 POST**(`curl -T` 发的是 PUT,会 405)。body ≤ `max_body_bytes` 走单 blob;**超过且开启 `auto_chunk_enabled`(默认)时服务端自动分片**(见 §5,201 的 `x-antfs-blob-size` = 逻辑总量) | **201** + `Location: /blobs/{blobId}` + `x-antfs-blob-id: {16 hex}` + `x-antfs-blob-size: {n}` | 400(非法 session 头——分片路径校验;单 blob 直写路径无 id/会话参数)/ 413(超该路径上限:关自动分片 = `max_body_bytes`,开 = 片数 4096 × `auto_chunk_bytes`)/ 409(keyed 写全副本冲突,`router-conflict-surfacing` 2026-10-20 透传;现 REST 路径未部署 keyed 写,契约预留)/ 507(分区写满)/ 503(未达多数派) |
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
| `POST /blobs` | 小 body(≤ 1 MiB):`put_auto_with_ttl(bytes, expiration_ms)`(**内部发号 + 分区满 507 自动换分区重试**,最多 2 次);大 body(> 1 MiB 或无 Content-Length):`put_file_with_ttl(path, expiration_ms)`(同样内部发号 + 507 重试)。**body > `max_body_bytes` 且开启自动分片**:`auto_chunk_store`(§5)。三条路径写行为对称(**W = 副本数/2+1**,收满即返回,其余靠拉复制补) |
| `GET /blobs/{id}` | `get(&key)`(**R=1**,副本按 `up` 排序,首个 200 即返回) |
| `HEAD /blobs/{id}` | `stat(&key)`(元数据不读数据体) |
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
3. **读 stitched blob**:先 `stat`;**位是唯一类型判定**(`composed` 置位 ⇔ 描述符):置位行整体读回
   解析拼装(超上限/结构坏 = 上游损坏响亮 502,信任位不降级),逐片 `get` 并**流式拼接**进响应
   (内存 O(单片));无位行 = 普通对象原字节返回,任何前缀都不触发内容探测
   (避免把大对象整个缓进内存,CLAUDE.md §5.3);
   片缺失/描述符损坏 → **502**(上游数据不一致,不是客户端的问题)。
4. **`HEAD` 与 `GET` 对齐**:置位行做一次小对象读回换算**逻辑大小**(各片之和)+
   `x-antfs-blob-stitched: true` / `x-antfs-blob-stitch-chunks: N`;无位行不读数据体,报自身
   `stat` 大小、不带这两个头。代价可控:描述符按构造 ≤ 1 MiB(`DESCRIPTOR_MAX_BYTES`)。
4. **`DELETE` 只删描述符本身,不递归删片**:一次删 N 个对象是危险动作(且片可能被多个 stitched blob 共享);
    孤儿片靠 `x-antfs-ttl-ms` + store 本地 worker 回收。自动分片(§5)产生的对象**删描述符后同样留下孤儿片**
    —— 服务端知悉但故意不递归(无法从描述符区分"片是否被共享"是原因之一;描述符现已带 composed 类型位(`blob-composed-flag`),
    但递归删片仍是危险动作,维持不递归)。

**相对 Ambry 的偏离(登记)**:无签名片 id、无服务端会话(不做"同 session 才可拼"的服务端强制)、不强制分片大小、
不递归删片。

## 5. 流式、自动分片与 body 上限

- 上传**分档**(阈值 `INLINE_BODY_MAX` = 1 MiB,常量):≤ 1 MiB 走**内存路径**(`Router::put_auto_with_ttl(Bytes, ...)`,内部发号 + 507 换分区重试,内存上界 =
  在途请求数 × 1 MiB);> 1 MiB(或未声明长度)才**落暂存文件 + `Router::put_file_with_ttl` 流式**,**内存不随文件大小涨**。
  两条路径的 507 重试行为**完全对称**(最多 2 次换分区),不存在"小 body 不重试"的不一致。
  分档原因:每个小对象都落一个暂存文件会多出 `create/write/close/unlink` 的元数据开销,实测把 PUT p50 从 ~1ms
  拖到 ~9ms(release,4 KiB × 并发 32)。暂存文件放系统临时目录,成功/失败都删。
- `frontend.max_body_bytes`:**单 blob 路径的上限**。有 `Content-Length` 时先校验;chunked(无 `Content-Length`)
  时边收边计数,超了 → 413。未开自动分片时它就是 `/blobs` 的硬上限(超 → 413,不读 body);
  开启后超过它的 body 转入自动分片(见下条)。**部署对齐要求**:单 blob 与片走同一条数据面 PUT,故
  `store_server.max_body_bytes` 必须 ≥ `frontend.max_body_bytes`(单 blob 不被数据面 413)且
  ≥ `frontend.auto_chunk_bytes`(片同理;devcluster 两侧同为 16 MiB)。
- **服务端自动分片**(2026-10 起,默认开,`frontend.auto_chunk_enabled` / `auto_chunk_bytes`):
  `POST /blobs` 的 body(声明或实收)超过 `max_body_bytes` 时:
  1. **先整体落暂存并校验**(复用既有暂存路径,上限 = 片数 4096 × 单片,超 → 413)—— 客户端中途断流/谎报
     长度时**一片都不写**就失败,不产生孤儿;
  2. 从暂存文件**按 `auto_chunk_bytes` 顺序切片,逐片 `put_auto_with_ttl`**(顺序写:并发扇出会把
     "单片内存"乘以片数在途,先不做);片与描述符**同 TTL**,整个逻辑对象一体过期(HEAD 可见
     `expired` / `expiration-ms`);
  3. 编**同 `/stitch` 的描述符**(session = 请求带的 `x-antfs-session`,缺省空)也当普通 blob 写入;
  4. 201 返回**描述符的 blobId** + `x-antfs-blob-size: 逻辑总量`;`GET`/`HEAD` 走 §4 的既有透明拼装,
     客户端不感知分片。
  **中途失败**:对已 ack 的片逐个写墓碑**尽力回收**(best-effort:删失败只记日志 + 指标,不影响回报调用方的
  原错误;片若已落少数副本,墓碑多数派 + 拉复制"墓碑恒胜"会盖掉)。**重试安全**:重试拿到一套全新 blobId,
  与失败轮互不相扰。**内存**:单片(= `auto_chunk_bytes`,校验限 1..=1 GiB,顺序写同一块缓冲)+ 暂存期
  一个 64 KiB 块,不随对象大小涨。**落盘**:暂存 1 份(先写满总量再切片读回)+ 每片各 1 次数据面写
  (上一条"内存换磁盘"的代价,自动分片按片数放大)。
  **已知取舍**:超大 body 会先整体占掉体量等大的系统临时目录空间,总量上限(默认 4096 × 4 MiB = 16 GiB)
  同时也是暂存空间的界 —— 大 body 场景建议给临时目录单独的卷;另见 §4 末条(删描述符留孤儿片)。
- **读方向**:大对象(> 1 MiB)先经 SDK 落到暂存文件再**流式**回给客户端,小对象才整体读回
  —— 故 **frontend 侧内存不随对象大小涨**。**但 store 侧的 `GET` 仍把数据体整个读进内存**(真流式 GET 是 store 的待办,
  见 §11),所以端到端内存上界仍由 store 决定(自动分片的 `GET` 同样受此限:逐片 `get`、内存 = 单片)。
- 一次上传的落盘量:frontend 暂存 1 份 + store 侧暂存 1 份 + log 1 份 —— 这是"内存换磁盘"的代价,记在 D5
  (自动分片是"× N 片 + 描述符 1 份",且暂存文件例外地先写满总量再切片)。

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
| `frontend.max_body_bytes` | u64 | `4194304`(4 MiB) | ≥1 | 单请求 body 上限(body ≤ 此值的仍是**单 blob**;关闭自动分片时即 413 硬上限,开启后是"转分片"的门槛,见 §5)。部署须与 `store_server.max_body_bytes` 对齐 | `frontend.max.json.request.size.bytes` 等(语义不同) |
| `frontend.auto_chunk_enabled` | bool | `true` | — | 服务端自动分片开关:开 = body 超过 `max_body_bytes` 时自动切片多次上传并返回描述符的 blobId;关 = 恢复旧行为(超 `max_body_bytes` 一律 413,客户端手动走 `/stitch`) | 无(router 侧 chunking 在 Ambry 是另一层的事) |
| `frontend.auto_chunk_bytes` | u64 | `4194304`(4 MiB) | 1..=1073741824(1 GiB) | 自动分片单片大小:切片逐片入库的粒度,也是写路径单片驻留内存的读写缓冲大小;总量上限 = 4096 片 × 此值(默认 16 GiB) | 无(近义的 Ambry `router.max.put.chunk.size.bytes` 在 router 层) |
| `frontend.cors_allowed_origins` | Vec\<String\> | `[]` | 各项为非空且非 `*` 的合法 header 值(校验期就拒) | CORS 来源白名单(**空 = 不挂 CORS 层**;不接受 `*`) | `frontend.options.allow.methods` 等 |
| `frontend.router.master_addr` | String | `127.0.0.1:7101` | 非空 | 内嵌 SDK 的 master 地址(见 [`router.md` §5](./router.md)) | 无 |
| `frontend.router.cluster_secret` | String | `""` | — | 集群准入密钥 | 无 |
| `frontend.router.request_timeout_secs` | u64 | `10` | ≥1 | 单请求超时 | `router.routerRequestTimeoutMs` |
| `frontend.router.id_segment_size` | u32 | `1024` | ≥1 | **均分时**每个可写分区一次申请的 blobId 个数(总预留 = 分区数 × 该值,**实际按写权重配比**;见 [`router.md` §2/§5](./router.md)) | 无 |
| `frontend.router.partition_refresh_secs` | u64 | `30` | ≥0(`0` = 关闭) | 后台刷新可写分区列表的周期(秒;见 [`router.md` §2](./router.md)) | 无 |
| `frontend.router.latency_aware_routing` | bool | `true` | — | 读路径副本排序开关(`up` → 观测延迟 EWMA 升序 → master 原序;只改尝试序不改候选集;见 [`router.md` §5](./router.md)) | 无 |

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
