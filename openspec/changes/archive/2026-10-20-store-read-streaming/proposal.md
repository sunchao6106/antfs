# Proposal: store-read-streaming — 读路径端到端流式(发送端组包瓶颈根除)

## Why(实测证据链)

- 引擎层:`BlobStore::get/get_with_options` 内部 `read_full_record` → `log.read(at, total)` 一次分配整条 `Vec<u8>`(`log_segment.rs:335` 每段 `vec![0; len]`)—— **读整条 = 内存整条**。
- store HTTP 层:`get_blob` 把该 Vec 一次性挂上响应 —— 发送端组包。
- 中间层早已流式:router `get_file → drain_to_file` 走 `bytes_stream()` 逐块落盘;frontend 大对象走 `get_file` + 暂存文件流式回(小对象 ≤1 MiB 有意直读,见 INLINE 设计注)。
- **端到端内存上界因此卡在 store 组包侧**(`docs/design/frontend.md` §11「读路径端到端流式」待办;`benchmark/BASELINE.md` 亦记)。

## What Changes

| 位置 | 改动 |
|---|---|
| `crates/blob/src/blob.rs` | 新公开原语 `read_range(key, offset, len)`:复用 index/日志既有"任意位置读"(`Log::read(at, len)` → 段 `read_exact_at`),只读 24B 记录头做同口径校验(magic/version/key/deleted/expired),数据体只搬 `[offset, offset+len)` 切片。**不重验 CRC**(切片不构成 CRC 覆盖单元;bit-rot 防线 = 后台 scrub,记档) |
| `crates/store/src/server.rs` | `get_blob` 主体改流式:先经盘池 `stat` 决定 404/长度,再 `Body::from_stream` 逐块(512 KiB)经**每盘有界池**调 `read_range` —— 背压天然(下游消费慢 → 不拉下一块 → 盘池无积压)。`?replication=1` 分支**保留原整读**(`get_with_options` 含过期体语义是复制拉取的契约,本批不动);`OpTimer` 移入 stream 状态持有(Drop 打点,传输全程计入时长与字节) |
| 测试 | blob 单测:read_range 矩阵 + **分段拼接 ≡ 整读** 对拍;store data_plane:大小件 GET 字节恒等 + 真实 registry 的 `get_bytes_total` 断言;devcluster 真面:64 MiB PUT/GET 字节恒等 |
| 文档 | `frontend.md` §11 勾销「读路径端到端流式」;`store.md` 数据面 GET 流式语义(分块、CRC 裁剪取舍、replication=1 保留整读);DEVLOG 步 81 |

## Impact

- **磁盘格式零改**;读路径行为:普通 GET 响应体从"一次性组装"变"分块流"(客户端可见差异仅 chunked 传输形态,router/reqwest/curl 均聚合语义不变);无新配置无新监控(复用 get_total/get_bytes/duration 三件,口径见 design)。
- 删除/到期对象的 404 时机:从"读完才发现"提前到"流首 stat"(更新鲜,语义更对);流中途对象被删 → 连接中断(与现状"开始前 404"差异记档,RSK 段)。
- CRC 全量验证由整读路径(复制拉取 `get_with_options` 仍验)与 scrub 承担;面向客户端的普通 GET 每块无 CRC 重验 —— 取舍见 design D3。

## Out of Scope

- `?replication=1` 复制读流式化(复制语义与幂等另案)。
- mmap sealed 段 zero-copy 响应(段生命周期重构,收益另案)。
- frontend 小对象(≤1 MiB)直读路径(有意保留,见 INLINE_BODY_MAX 注)。
