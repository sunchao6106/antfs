# Design: store-read-streaming

## D1 引擎原语:`read_range`(只动读,格式零改)

`Log::read(at, len)` 与段 `read(offset, len)`(底层 `read_exact_at`)本就是参数化位置读 —— 引擎侧缺的只是"数据体切片 + 同口径校验"。新公开 API:

```rust
pub fn read_range(&self, key: &StoreKey, offset: u64, len: u64) -> Result<Vec<u8>>
```

流程(index 快照与 `get_with_options` 完全同构):
1. `is_available` → `inner()` 快照 → `index.find` → Not Found/Deleted/Expired 同 get 语义(`include_expired` 恒 false —— 复制不走此口);
2. 头校验(读 `RECORD_HEADER`=24B,新私有帮手 `read_record_header`):magic/version(key_prefix 取 `log.key_prefix()`)、key 全等、FLAG_DELETED(活索引指墓碑 = Corrupt,同 get);
3. 界检查:`offset + len <= data_len`,越界 = `Corrupt`(调用方按自身 stat 循环,正常不越;防御数据/索引分歧);
4. `log.read(at + RECORD_HEADER + offset, len)` 搬切片。

**并发正确性**:段表为追加式;compaction 换 `Inner` 的瞬间,后续块走新快照 `index.find` —— 若记录已被搬走,find 命中新位置照读(对);若嫌太老被回收 → NotFound → 流断(同"中途删除"语义)。**没有"读旧段悬垂"**:旧段在 `Inner` 被 drop 前仍由该 Inner 的 `Log` 快照持有(Arc);index.find 每块重新定位,不缓存旧 offset 跨 compaction 复用。

## D2 store 数据面:分块流(512 KiB,过每盘有界池)

- 首 stat(池内):NotFound/Deleted/Expired → 404(时机**提前**——原为读后);拿到 `data_len`;
- 流:每块 `min(READ_CHUNK_BYTES, remain)` 经 `pool.run(disk, "get", …)`(同步块在盘池线程,排队/背压/`503` 语义与其他盘操作同轨)→ `Bytes` 块;`Body::from_stream`;
- `OpTimer` 移入 stream 状态:`set_bytes(total)` 在响应头组装时定值;Drop(发完/断连)统一落点 —— `disk_request_duration` 口径从"整读组装时长"变"含传输的端到端时长"(语义更贴标签名,如实记档);
- **`?replication=1` 分支不动**:`get_with_options` 整读 + `include_expired`(复制契约,含 CRC 整验);
- 单请求内存 = **一块(512 KiB)+ 头 24B**,与对象大小无关。

## D3 CRC 裁剪取舍(读路径唯一的语义弱化,显式记录)

- 记录 CRC 覆盖"头+数据体"整条;切片读**无法**逐块重验(不是"懒得验"是数学上不构成覆盖单元);
- 承担防线:①`?replication=1` 复制读仍走整读 + CRC(`read_full_record` decode 全验);②后台 scrub 巡检(store-scrub-background)周期比对 CRC;③头 24B 每块重验(magic/version/key);
- 与 Ambry 语义对齐:Ambry 流式读同样不做逐块 CRC(整条 CRC 在复制/校验路径)。
- 风险窗口 = 客户端 GET 收到 bit-rot 字节而 store 无感,scrub 周期内自愈纠偏;**接受**(持久性红线针对"写/丢",读出坏块可重试副本 —— R=1 首副本坏 bytes → 客户端可重试;上层 gateway 已有重试语义)。

## D4 指标口径(零新增,如实记录漂移)

`get_total`(每 GET 1 次,stat 成功即计——与"成功路径才计"对齐);`get_bytes_total`(总 data_len);`blob_request_duration{op=get}` 标签含义 = stat(不含指标面)移入流完成时点,含传输端到端;`disk_*` 系列按每块 observe?**否** —— OpTimer 单实例仍按请求记(首 stat 建立,流尾 drop),`disk_request_duration` = 端到端;盘维 `disk_bytes_total` 照 set_bytes(total)。分界:per 块不再单独 observe(避免 duration histogram 被块数污染 —— 保持"每请求一点")。

## D5 测试证据链

1. blob 单测矩阵:offset=0 全量 ≡ get / 中段 / 尾块 / 越界 offset / 越界 len / deleted / expired / 未知 key / 恶造坏头(magic)→ Corrupt;**分段拼接 ≡ 整读**随机对拍(尺寸 0..N);
2. store data_plane:大件(8 MiB)真 HTTP GET 字节恒等 + `get_bytes_total` 按真实接入的 registry 断言(CLAUDE §6 口径);404/断流不因流式破坏;\n3. devcluster 真面:64 MiB PUT → GET 字节恒等;release 手感顺带。
