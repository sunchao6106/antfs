# Tasks: store-read-streaming

## 1. Engine Primitive

- [x] 1.1 `blob.rs` 私有帮手 `read_record_header`(24B 头校验:magic/version/key/flags → data_len)
- [x] 1.2 `pub fn read_range(key, offset, len)`:同口径校验 + 界检查 + 数据体切片读;单测矩阵 + 分段拼接 ≡ 整读对拍

## 2. Store Data Plane

- [x] 2.1 `get_blob` 主体流式化:首 stat(池内)→ 404/长度 → `Body::from_stream` 逐块(512 KiB)经盘池 `read_range`;OpTimer 移入流状态(传输端到端计时)
- [x] 2.2 `?replication=1` 分支保持整读原路径(复制契约:include_expired + CRC 整验)
- [x] 2.3 data_plane 集成:大件字节恒等 + `get_bytes_total` 真实 registry 断言 + 404 路径不破

## 3. Spec & Docs & Closure

- [x] 3.1 spec delta:ADDED capability requirements(切片原语语义 / 流式响应与池纪律 / 复制读保留整读)
- [x] 3.2 `frontend.md` §11「读路径端到端流式」勾销;`store.md` 数据面 GET 流式段(分块、CRC 裁剪、replication=1 口径);DEVLOG 步 81
- [x] 3.3 全套门禁(workspace/clippy/fmt)+ devcluster 真面 64 MiB 闭环 + validate/archive + 单 commit push + CI 盯绿

## 落地记录(已回填)

- blob **125** 个(新增 `read_range_matrix_and_chunked_equivalence`:全量/中段/尾块/单字节/
  越界 Corrupt×2/NotFound/Deleted + 四种切分【1B、7B、BLOCK、1/3+非对齐】拼段恒等);
  store **221** 个 17 套(新增 `streaming_get_roundtrips_large_object_across_chunks`:
  2 MiB 经 4 块流式 GET 字节恒等 + 盘维度 `disk_bytes_total{op=get}` 落账);
  **workspace 77 套 769/0;clippy 0;fmt 净**。
- 流式实现面:首 stat(池内)定 404/长度 → `async_stream::stream!` 逐块(512 KiB)经
  每盘池 `read_range`;`?replication=1` 保留整读原路(含过期体 + 整条 CRC);
  `OpTimer` 移入流状态持有(Drop 落盘维度带宽),duration 直方图流首按总长 observe。
