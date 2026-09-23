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
| `POST` | `/blobs` | body = blob 裸字节;可选 `x-ambry-ttl-ms: <i64>`。**上传用 POST**(`curl -T` 发的是 PUT,会 405) | **201** + `Location: /blobs/{blobId}` + `x-ambry-blob-id: {16 hex}` + `x-ambry-blob-size: {n}` | 400(非法 id)/ 413(超 `max_body_bytes`)/ 507(分区写满)/ 503(未达多数派) |
| `GET` | `/blobs/{blobId}` | — | **200** + 原始字节 | 404(所有副本都没有)/ 400(非法 id)/ 502(分片描述符损坏或片缺失)/ 503(副本不可达) |
| `HEAD` | `/blobs/{blobId}` | — | **200** + 无 body;头 `x-ambry-blob-size`/`-deleted`/`-expired`/`-undeleted`/`-ttl-updated`/`-life-version`/`-expiration-ms`。**拼装对象额外带 `x-ambry-blob-stitched: true` + `x-ambry-blob-stitch-chunks: N`,且 `size` 是各片之和(逻辑大小,与 `GET` 一致)** | 404 / 400 / 503 |
| `DELETE` | `/blobs/{blobId}` | — | **204**(多数派写墓碑) | 404 / 503 / 400 |
| `POST` | `/blobs/{blobId}/undelete` | — | **200** + `x-ambry-blob-life-version: {n}` | 404 / 503(未处于已删态也会收敛成 503,见下) |
| `POST` | `/blobs/{blobId}/ttl?expiration_ms=<i64>` | 查询参数必填 | **204** | 400(缺参数)/ 404 / 503(已更新过同样收敛成 503) |
| `POST` | `/stitch` | body `{"signedChunkIds":["<16 hex>",…]}`;头 `x-ambry-session: <opaque>` 必填 | **201** + 同 `POST /blobs` 三个头 | 400(session 缺失/空列表/片数 >4096/非法 id/片不可读) |
| `GET` | `/metrics` | — | **200** + Prometheus 文本 | — |

- **blobId 路径段**:必须能 `hex::decode` 成 **8 字节**,否则 400(文案含 `blob id must be 8 bytes (16 hex)`)。
- **状态码映射**(统一一处):`NotFound → 404`;`StoreFull → 507`;`TooLarge → 413`;
  `Silenced/Quorum/Http/NoReplica/Master/Rejected/NoWritablePartition/IdExhausted → 503`;非法输入 → 400;
  上游数据不一致(描述符损坏/缺片) → 502;其余 → 500。其中 `Silenced` **透传** `Retry-After` 头。
- **副本状态码按语义透传**(不再一律 500):副本回的 `400/413/503/507` 分别映射成同名状态码 —— 这些是
  调用方可控的 payload/容量问题(如"对象太大放不进一个段"是 400、"分区写满"是 507),混进 500 会让
  调用方与压测误判成服务端故障。
- ⚠ **409 在当前分层下不可达**:store 对「undelete 目标未处于已删态」「ttl 已更新过」回 409,但 router 的多副本变更把
  单副本失败收敛成 `Quorum` → 本层只能给 503。要精确出 409 需扩 router 暴露单副本状态(记入待办)。
- **`x-ambry-*` 头命名**:沿用 Ambry 的既有头名(`x-ambry-blob-id` / `x-ambry-blob-size` / `x-ambry-session` …),
  便于熟悉 Ambry 的人上手。
- **不带 account/container**:blobId 64 位无租户位(D7/D8),路径里不放一个"存不下来又不管事"的段;多租户将来落在
  网关层(那时再加)。

## 3. 写 / 读路径(全在 SDK 里)

| 端点 | 内部动作 |
|---|---|
| `POST /blobs` | `allocate_one()` 取 blobId → `put(key, bytes)`(**W = 副本数/2+1**,收满即返回,其余靠拉复制补) |
| `GET /blobs/{id}` | `get(&key)`(**R=1**,副本按 `up` 排序,首个 200 即返回) |
| `HEAD /blobs/{id}` | `stat(&key)`(元数据不读数据体) |
| `DELETE` / `ttl` | `delete` / `update_ttl`(多数派) |
| `POST /undelete` | `undelete`(多数派,回 life version) |

- `x-ambry-ttl-ms` 的落地方式:**先 `put` 再 `update_ttl`**(两次多数派写)。取舍:SDK 的 `put` 没有 TTL 参数,
  本次不扩 SDK 的写签名(避免为一个头改公共 API);代价是"非原子"(极端情况下对象已写成功但 TTL 没设上)。
  数据面 `PUT ...?expiration_ms=` 已支持原子写,后续把 `Router::put*` 加一个 `expiration_ms` 参数即可收口(待办)。

## 4. 分片拼装(`POST /stitch`)——**无状态**设计

Ambry 把会话信息编进**签名片 id**;antfs 无签名体系(无 ACL/密钥),故:

1. **分片上传** = 普通 `POST /blobs`(带同一个 `x-ambry-session`);片列表由**客户端自己记**,服务端不存 session。
2. **`POST /stitch`**:
   - 校验 `x-ambry-session` 非空、片列表非空且 ≤ 4096、每个 id 是 16 hex;
   - 逐片 `stat` 拿尺寸(拿不到 → 400);
   - 构造**自描述描述符**(小端,当**普通 blob** 写入):
     ```
     MAGIC = b"ASTC"   (4B)
     version: u8 = 1
     chunk_count: u32
     session_len: u16
     session: [u8; session_len]
     × chunk_count: chunk_id(8B) | chunk_size(u64) | expiration_ms(i64)
     ```
     描述符上限 1 MiB(超 → 400);片列表**当 blob 存**是刻意的:中心存一份"片→对象"映射就等于引入
     **每对象元数据**,与 D3 冲突。
   - 因此 `POST /stitch` 是**幂等性无关**的普通写:201 + `Location: /blobs/{stitchedId}`。
3. **读 stitched blob**:先 `stat` 拿 `size`;`size` 小到可能是描述符时才整体读出来看 MAGIC(避免把大对象整个
   缓进内存,CLAUDE.md §5.3),命中则解析描述符、逐片 `get` 并**流式拼接**进响应(内存 O(单片));
   片缺失/描述符损坏 → **502**(上游数据不一致,不是客户端的问题)。
4. **`HEAD` 与 `GET` 对齐**:`HEAD` 也做同样的 MAGIC 嗅探,拼装对象报**逻辑大小**(各片之和)+
   `x-ambry-blob-stitched: true` / `x-ambry-blob-stitch-chunks: N`;普通对象报自身大小、不带这两个头。
   代价可控:描述符按构造 ≤ 1 MiB(`DESCRIPTOR_MAX_BYTES`),故**只有小对象**需要多读一次数据体,
   更大的对象不可能是描述符、直接按 `stat` 的 size 回(不读数据体)。
4. **`DELETE` 只删描述符本身,不递归删片**:一次删 N 个对象是危险动作(且片可能被多个 stitched blob 共享);
   孤儿片靠 `x-ambry-ttl-ms` + store 本地 worker 回收。

**相对 Ambry 的偏离(登记)**:无签名片 id、无服务端会话(不做"同 session 才可拼"的服务端强制)、不强制分片大小、
不递归删片。

## 5. 流式与 body 上限

- 上传**分档**(阈值 `INLINE_BODY_MAX` = 1 MiB,常量):≤ 1 MiB 走**内存路径**(`Router::put(Bytes)`,内存上界 =
  在途请求数 × 1 MiB);> 1 MiB(或未声明长度)才**落暂存文件 + `Router::put_file` 流式**,**内存不随文件大小涨**。
  分档原因:每个小对象都落一个暂存文件会多出 `create/write/close/unlink` 的元数据开销,实测把 PUT p50 从 ~1ms
  拖到 ~9ms(release,4 KiB × 并发 32)。暂存文件放系统临时目录,成功/失败都删。
- `frontend.max_body_bytes`:有 `Content-Length` 时先校验(超 → 413,不读 body);chunked(无 `Content-Length`)时边收边计数,
  超了 → 413。**默认与 `store_server.max_body_bytes` 同值同因**(axum 默认 2 MiB;不擅自改默认,要收大文件显式调大)。
- **读方向**:大对象(> 1 MiB)先经 SDK 落到暂存文件再**流式**回给客户端,小对象才整体读以嗅探分片描述符的 MAGIC
  —— 故 **frontend 侧内存不随对象大小涨**。**但 store 侧的 `GET` 仍把数据体整个读进内存**(真流式 GET 是 store 的待办,
  见 §11),所以端到端内存上界仍由 store 决定。
- 一次上传的落盘量:frontend 暂存 1 份 + store 侧暂存 1 份 + log 1 份 —— 这是"内存换磁盘"的代价,记在 D5。

## 6. CORS

`frontend.cors_allowed_origins` 给出**白名单**才挂 `CorsLayer`(非空时才挂);默认 `[]` = 不挂。
**不用 `permissive()`**:默认放开跨域的 API 是安全问题。放开的方法 `GET/POST/HEAD/DELETE`,
允许的请求头 `x-ambry-session` / `x-ambry-ttl-ms` / `content-type`,暴露响应头 `x-ambry-*` 与 `location`。

## 7. 与 Ambry frontend 的对照

| 维度 | Ambry `ambry-frontend` | antfs frontend |
|---|---|---|
| 上传路径 | `POST /`(account/container 在 **header**) | `POST /blobs`(**不带** account/container) |
| blobId 谁生成 | frontend 调 router,router **本地随机铸**(partition 从本地 clustermap 随机挑) | **向 master 申请号段**(`GetBlobIds`) |
| 上传结果 | `Location: <blobId>` | `Location: /blobs/{blobId}` + `x-ambry-blob-id`/`-blob-size` |
| 鉴权/配额 | `AccountService` + `AmbrySecurityService` + quota | **无**(多租户推后到网关期) |
| 大对象 | chunked upload + `/stitch`(**签名片 id**) | `/stitch`(无签名片 id,描述符自描述) |
| 元数据/HEAD | `GetBlobInfo`(blob 元数据在 id 内编码 account/container/ttl) | `HEAD /blobs/{id}` → store 的 `.../metadata`(无每对象元数据) |
| CORS | `frontend.options.allow.methods` 等配置 | `frontend.cors_allowed_origins` 白名单 |
| 指标 | Dropwizard `FrontendMetrics` | `FrontendMetrics`(Prometheus,见 §9) |

## 8. 配置项清单

`FrontendConfig`,TOML `[frontend]` 表(CLAUDE.md §7 必须项;与 `crates/frontend/src/config.rs` 的 `impl Default` +
`Validate` 逐项对齐)。

| key(TOML) | 类型 | 默认值 | 取值范围/校验 | 含义/影响 | 对齐的 Ambry 原配置 |
|---|---|---|---|---|---|
| `frontend.listen_addr` | String | `127.0.0.1:7400` | 非空 | HTTP 监听地址 | `frontend.http.port` 等 |
| `frontend.max_body_bytes` | u64 | `2097152`(2 MiB) | ≥1 | 单请求 body 上限(超 → 413)。默认与 `store_server.max_body_bytes` 同因同值 | `frontend.max.json.request.size.bytes` 等(语义不同) |
| `frontend.cors_allowed_origins` | Vec\<String\> | `[]` | 各项为非空且非 `*` 的合法 header 值(校验期就拒) | CORS 来源白名单(**空 = 不挂 CORS 层**;不接受 `*`) | `frontend.options.allow.methods` 等 |
| `frontend.router.master_addr` | String | `127.0.0.1:7101` | 非空 | 内嵌 SDK 的 master 地址(见 [`router.md` §5](./router.md)) | 无 |
| `frontend.router.cluster_secret` | String | `""` | — | 集群准入密钥 | 无 |
| `frontend.router.request_timeout_secs` | u64 | `10` | ≥1 | 单请求超时 | `router.routerRequestTimeoutMs` |
| `frontend.router.id_segment_size` | u32 | `1024` | ≥1 | 一次申请的 blobId 个数 | 无 |

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
| `frontend_upload_bytes_total` | counter | — | 字节 | 上传字节数(成功) | 无 |
| `frontend_download_total` | counter | `outcome` | 次 | 下载请求数 | `FrontendMetrics.getBlobRequestMetrics` |
| `frontend_download_bytes_total` | counter | — | 字节 | 下载字节数(成功) | 无 |
| `frontend_delete_total` | counter | `outcome` | 次 | 删除请求数 | `FrontendMetrics.deleteBlobRequestMetrics` |
| `frontend_mutation_total` | counter | `op`, `outcome` | 次 | `head`/`undelete`/`ttl`/`stitch` 请求数 | `FrontendMetrics.*RequestMetrics` |
| `frontend_request_duration_seconds` | histogram | `op` | 秒 | 请求延迟(含取号段与副本扇出) | `FrontendMetrics.*RequestTimeInMs` |
| `frontend_in_flight_requests` | gauge | `op` | 个 | 在途请求数 | 无 |
| `frontend_replica_errors_total` | counter | `kind` | 次 | 副本侧失败分类(kind 同 SDK 错误表) | 无 |

## 10. 被否决方案

- **给 frontend 加 account/container 路径段**:存不下来(blobId 无租户位)也不管事(无 ACL/配额),
  纯装饰(D8)。
- **`CorsLayer::permissive()`**:默认全开的跨域是安全问题;用显式白名单。
- **服务端 session 表**(分片上传):引入每对象/会话元数据,与 D3 冲突且让 frontend 变有状态;描述符自描述已够。
- **frontend 直接读写 store 字节、不走 SDK**:会把 W/R 语义复制成两份实现(必然漂移)。

## 11. 待办

- [ ] **【待议】拼装对象的类型判定靠"嗅探数据体前 4 字节"**:≤1 MiB 且以 `ASTC` 开头的普通 blob 会被误判
      (要么 502 拒绝服务、要么被当清单展开后**静默返回别人的内容**)。本层是实现该嗅探的地方,但**改法要落在
      数据面**:推荐在 store 记录头的 `flags` 里加一个"composed"位(SeaweedFS needle `FlagIsChunkManifest` 同款),
      之后 HEAD/GET 只看标志、不再嗅探(顺带省掉 HEAD 对小对象的那次数据体读取)。
      完整复现、三个选项与先例出处见 `openspec/changes/blob-io-end-to-end/tasks.md` 的待办首条。
- [ ] `Router::put*` 增加 `expiration_ms` 参数,让 `x-ambry-ttl-ms` 变成**原子**写(现在先 put 再 update_ttl)。
- [ ] **store 侧真流式 GET**(把数据体整个读进内存的是 store;frontend 侧已落暂存再流式回)。
- [ ] router 暴露单副本状态,让 `undelete`/`ttl` 的 409 语义能透传到 REST 层(现在收敛成 503)。
- [ ] S3 兼容层(与 frontend 同层)。
- [ ] 多租户:account/container + ACL + 配额(网关层)。
