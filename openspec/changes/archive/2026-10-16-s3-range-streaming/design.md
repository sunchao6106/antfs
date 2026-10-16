# s3-range-streaming 设计

## Context

GET 读路径现状(`crates/s3/src/handlers/object.rs`):

- `get_object` → `router.get(blob_id)` 整体 `Bytes` → 响应;
- multipart 对象 → `get_multipart_object` 逐片 `router.get` 后拼接**整个对象**进 `Vec`;
- SSE 对象 → `router.get(frame)` 整帧解密 → 明文整体缓存;
- 全路径无 `Range` 解析,无 206/416,无 `Accept-Ranges`。

关键事实(决定设计形态):

1. **store 数据面 `get_blob` 本身一次返回整条 blob**(引擎同步读+page cache,现状设计;`server.rs:1249` `(StatusCode::OK, data)`),router `get_file`/私有 `get_stream` 只是把同一条整 blob **流式传走**。网关内存问题的根源只在"拼总量",不在传输方式;
2. **网关写入的每条单 blob ≤ `max_body_bytes`**(不变量,见 D4):PUT / UploadPart 同走 `read_bounded_body`(cap = `max_body_bytes`),CopyObject 单块源同界、multipart 源按片重建。故**无上界的只有 multipart 对象总量**;
3. `router.get` 在 body 传输完成前有**全程 R=1 failover**;任何流式方案在首字节发出后即失去 mid-body failover(frontend `get_file` 同语义,先例在册);
4. manifest 自带各片 `size`(`ManifestPart{blob_id, size}`),`named_objects.size` 由 complete 事务写入 = Σ parts——GET 可以纯内存元数据推算 range 覆盖窗口,不触任何 blob。

## Goals / Non-Goals

**Goals:**

- GET 支持 S3 Range 语义(单 range/后缀/截断/416/忽略规则),覆盖普通、SSE、multipart 三种对象;
- multipart GET 内存上界 = 单片(≤ `max_body_bytes`),`Content-Length` 恒准且响应前对账;
- Range 请求只触达覆盖片(非覆盖片零网络读);
- GET/HEAD 回 `Accept-Ranges: bytes`。

**Non-Goals:**

- 多 range(逗号多区间)按字节返回(RFC 合法但 S3 不做,忽略回 200,与官方一致);
- `If-Range`/GET 条件请求族(V1 审计 P1 档,独立立项);
- `?partNumber=` 单片查询(AWS「GetObject partNumber」另一功能面,不混入本 change);
- SSE 流式逐块解密(SSE 对象受 D4 不变量约束 ≤ `max_body_bytes`,解密后切片已受界;见 D5);
- router SDK 改动(不改 `antfs-router`,理由见 D4/D6);
- 数据面 offset 读(store 引擎按段定位读,属 blob 引擎能力,改动面大,收益仅剩非覆盖片传输——已被 D3 窗口化覆盖)。

## Decisions

### D1:Range 解析 = 纯函数,忽略语义优先

`parse_range(header: Option<&str>, size: u64) -> RangeOutcome`:
`Full(sig=无效忽略) | Partial{start, end_incl} | Unsatisfiable{spec}`。

规则表(取齐 AWS 行为 + RFC 7233 忽略语义):

| 输入(size=N) | 输出 |
|---|---|
| 无头 / 非 `bytes=` / `start>end` / 多 range / `bytes=-0` 之外语法坏 | `Full`(回 200 全对象) |
| `bytes=s-` | s ≤ N-1 → `Partial{s, N-1}`;s ≥ N → `Unsatisfiable` |
| `bytes=s-e` | e ≥ N 截到 N-1;s > e → `Full`;s ≥ N → `Unsatisfiable` |
| `bytes=-k`(k ≥ 1) | k < N → `Partial{N-k, N-1}`;k ≥ N → `Partial{0, N-1}`;N=0 → `Unsatisfiable` |
| `bytes=-0` | `Unsatisfiable`(RFC 不可满足;不静默 200) |
| 任意 range,N=0 | `Unsatisfiable` |

`Unsatisfiable` → 新增 `S3Error::InvalidRange{requested, actual}`(416,XML Code=InvalidRange,Message 带 requested/actual)。选择:坏语法回 200 是 RFC「MUST ignore」;语义不可满足回 416 是 S3 行为——两者边界即上表。

替代方案(全按 416 处理语法坏)被否:RFC 明文要求忽略,且 curl/浏览器会发探索性 range,416 会放大故障面。

### D2:multipart GET = 链式部分流(Body::from_stream),串行逐片

新类型 `PartStream`:持有 `AppState` + `Vec<ManifestPart>`(已按窗口裁剪为 `Vec<WindowPart{store_key, skip, take}>`)+ 迭代游标;`poll_next` 里**逐片** `router.get(part_key)`(整片 ≤ `max_body_bytes`,缓冲后可能切片)产出。

- 串行取片;每片一次 `router.get` 保有**全程 R=1 failover**(对单条 part blob 而言失败可换副本——比首字节流式更稳);
- 内存 = 当前片(single part),整对象永不落单个 `Vec`;
- `Content-Length = row.size`,响应前校验 `Σ manifest.size == row.size`,失配 → 500 `InternalError`(响亮;挡住"元数据行与 manifest 不一致却静默回短/长体"的协议级谎言);
- 部分 SSE 请求?不可能存在(multipart+SSE 写入即 501),读侧无须组合。

吞吐取舍:串行逐片 = 每片一个 RTT(+读盘)。冷缓存下 store 端本就要逐片读盘,流水线并行收益 <2×,复杂度(并发预取+背压)不成比例——遵循「简单优先」,串行落地,记档观察。

### D3:multipart Range = manifest 窗口纯计算

`window_parts(parts, start, end) -> Vec<WindowPart>`:按累积偏移扫描覆盖片,首片算 `skip`,尾片算 `take`(mid 片全量)。纯函数(单测即全语义)。非覆盖片**零网络读取**——这等于把"range 只拖想要的部分"的价值最大头拿到手(不依赖 store offset 读)。

### D4:普通/SSE 对象 GET 维持缓冲——不变量收口而非流式改造

**不变量:网关写入的每条单 blob ≤ `max_body_bytes`**。三路汇总:PUT(`read_bounded_body`)、UploadPart(同)、CopyObject(单块源 ≤ `max_body_bytes`;multipart 源逐片 ≤4MiB/步)。MetaStore 行只能由网关写入 ⇒ 行指向的 blob 满足不变量 ⇒ `router.get` 缓冲上界 = 配置值(默认 4MiB,operator 调大 = 显式选择内存开销,与 frontend 小对象路径同性质)。

故普通对象 GET 改造收益仅剩"去双缓冲",却要付出:mid-body failover 丢失(首字节后断流不可换副本)+ SDK 暴露流式口。**决策:不改**。真正的无界内存(multipart 总量)由 D2 收口;缓冲上界在 s3.md 文档化为部署约束(调大 `max_body_bytes` 的 operator 须知)。

SSE 帧 = 明文 + 封装开销(<200B),同受界;Range = 既有解密路径后切片,无新增帧几何依赖。

### D5:SSE + Range = 解密后切片

`get_encrypted_object` 增加窗口参数,解密(缓冲 ≤ `max_body_bytes` 界)→ `slice(start..=end)` → 206。选择性块解密(帧内 64KiB 块独立 nonce,可按块解)被否:对象受 D4 不变量约束,选择解收益为零,复杂度为负价值。

### D6:SSE/普通对象不引入 router SDK 流式口

显式否决(对应审计建议「router staging 形态未用上」):`get_file` 落暂存盘 = 每请求双倍盘 IO;`get_stream` 公开 = 失去 mid-body failover。普通对象场景两者皆负收益(受 D4 界),multipart 场景 D2 已解。记档:若未来放开单 PUT 尺寸(>max_body_bytes 路径),须重开此裁决。

### D7:HEAD 忽略 Range,GET/HEAD 补 `Accept-ranges: bytes`

AWS HeadObject 不处理 Range;唯一新增响应头。空对象上也回(官方行为)。

### 可观测性

无新指标。206/416 经 `s3_requests_total{op,status}` 既有的 status 维度直接可观测(记录点在响应包装层,以回给客户端的真实 status 记)。§6 文档表格增补说明性注记,不加行——没有注册新 handle,加表行反而违反「表与注册处逐项对得上」。

## Risks / Trade-offs

- [片级网络错 → 流中断(部分响应已发出)] → 客户端重试同一 range,窗口计算确定性 ⇒ 幂等;语义与 HTTP 206 部分响应天然一致,不引入服务端状态。
- [Σ 校验在存在历史脏行时报 500] → 响亮失败是有意选择:短/长体 + Content-Length 恒谎是更坏的失败面;发生条件(行不由事务维护一致)在 complete 记账事务设计下不可达。
- [串行取片吞吐] → 见 D2;观察对象:`s3_request_duration_seconds{op="GET"}` 直方图,若大对象读成为瓶颈再立项预取。
- [`bytes=-0` 回 416 与「测试者预期 200」] → RFC 语义正确;记档在错误测试里固化,防回归。
- [Range 头的大小写/空白容忍] → 解析按 `trim` + ASCII 大小写不敏感(与既有 SigV4 头处理同风格)。

## Migration Plan

纯读路径增强,无格式/配置/schema 变更:部署即生效,回滚 = 回退二进制。旧客户端(无 Range)行为零变化(200 全对象,唯一新增响应头 `Accept-Ranges`)。

## Open Questions

(无——三 P0 均有唯一合理解;以上 D1–D7 即裁决记录。)
