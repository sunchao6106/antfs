# Design: upload-zero-spool

> 前置阅读:proposal.md(两堵墙定义 + 裁决点)。本文给机制级细节与验证面。

## 0. 现状事实(行号级,方案的地基)

- **客户端**:`compression.rs::compress_stream_to_temp`(`NamedTempFile`:17B 头 + 编码流;
  比率闸 = 帧文件 vs 原文件)→ `lib.rs::prepare_file_upload`(三闸一次判定,`_frame` RAII
  跨全副本)→ `put_file_with_ttl` 扇出 `put_file_to`:每副本 `tokio::fs::File::open(path)`
  → `reqwest::Body::from(file)`(CL 恒已知)。**`put_file` 的源本来就是调用方的磁盘文件
  —— 临时盘全部来自"额外再压一份"的帧文件。**
- **服务端**:`server.rs::put_blob` 三分:CL ≤ 1 MiB 内存直进引擎(`put_ttl`);CL > 1 MiB
  → `stage_body` 流式落 `(各盘或 staging_dir)/tmp/put_*.tmp` → 池任务 `BufReader` →
  `put_from_reader`;无 CL → 有界缓冲回落。`StagedFile` RAII(正常出口显式删、取消出口
  Drop 兜底、启动清扫)。
- **引擎**:`LogSegment::reserve(total)` **只检查不推进**(`start = data_end`;返回即弃无
  自愈需求);`write_reserved(start, off, buf)` = 定位 `write_all_at`(pwrite,与引擎锁无关);
  `commit_reserved(start, total)` 一次性 release-store `data_end`(头 header 内存里的磁盘真值
  由 flush 写)。**单写者不变量**:`write_lock` 全程持有,段内 `debug_assert`("committed
  before any other append")——并发预留今日结构性不存在。弃单语义:未 commit 区间对读者与
  恢复扫描不可见,后续追加自然覆盖(`Log::reserve` 文档明文)。
- **网关面(2026-10-21 勘误补入)**:`frontend/server.rs` `POST /blobs` body > 1MiB(或无
  CL)→ 自制 `TempFile`(TMPDIR,RAII)全量中转 → `router.put_file(path)` / 分片复读切片;
  GET 大对象同名中转(`get_file`)。根因同源:SDK 无"流式 body"写口。S3 网关零盘(全
  `Bytes`,`read_bounded_body` 上限 `max_body_bytes` = 4 MiB/对象;另发现死旋钮
  `s3.inline_body_max` 有声明无使用点)。**全场景临时文件清单见附录 §8。**
- **削峰设计为什么存在**(暂存的辩护词,必须尊重):写锁串行整个引擎写;若引擎直读网络流,
  网络时长进锁 ⇒ 同分区所有写 p99 挂起。实测记录(小对象走暂存的那条注释):每 PUT 多 5 次
  fs 元数据操作,p50 ~1ms → ~9ms —— 这就是小对象走内存、大对象走暂存+每盘池的由来。

## 1. 目标与不变量

**目标**:全链路(客户端 + 服务端)上传路径除数据自身的落盘外,零中转文件;写放大(大
对象)2× → 1×;小对象时延形态与今日逐点一致。

**不变量(红线,B/A 两段共同的验收底线)**:

| # | 不变量 |
|---|---|
| I1 | 磁盘格式零变(段 header / 记录布局 / `.idx`/`.fuse` / COMPACT 标记,一字不动) |
| I2 | 可见性 = 提交语义:`data_end` 仍是唯一前缀游标;commit 前任何字节对读者/恢复不可见 |
| I3 | 恢复只认上一次 flush 的 `data_end`;未 commit 预留 = 崩溃后自然消失(今日语义原样) |
| I4 | 409 / 507 / 413 / 400 状态码与触发条件语义不变 |
| I5 | 小对象(≤1 MiB)路径行为与时延零回归 |
| I6 | 比率闸诚实性(B 段):kept 的判定仍基于**实际**压缩后尺寸,非估计 |

## 2. B 段设计(router 客户端:内存帧 + 两遍流式;① 裁定后的混合形态)

> 所有者裁决(2026-10-21):超上界**不许回落原文**——任意体量必须保持压缩。混合形态:
> 小文件吃内存红利,大文件吃 CPU 税,零盘、零密度回归。

### 2.1 机制(按体量两型)

**型一:内存帧(≤ 上界)** —— `spawn_blocking` 内从原文件逐块(256 KiB)读,四算法族
流式编码写入 `BytesMut`:
- 编码完成:比率闸(`(17B 头 + 压缩长) vs 原长`,语义逐字对齐今日 Bytes 口)过 →
  `Frame { bytes, stored }`;不过 → `Original { why }`(同今日)。
- **累计写入超上界** → 放弃内存态、转型二(已编码字节丢弃;或实施时做"无缝换 sink 续数"
  优化 —— 同一编码器把 sink 从内存换计数器接着跑,语义等价)。
- 帧上送:`http.put(...).body(bytes.clone())`,跨副本并传与 507 重试 = Arc clone,
  **零文件 opener、零重读**。

**型二:两遍流式(> 上界)**
- **第一遍(计数,前置)**:同一流式编码器全文跑一遍,sink = 只计数的伪 Writer(丢弃输出),
  产出压缩后真实尺寸 `n` → 比率判定;不过 → `Original{low_ratio}` 原文直传("确实压不动"
  的语义,与"压得动但超内存上界"是两件事)。
- **第二遍(逐副本边压边传)**:每个副本一条流式编码管道直连 HTTP body:块读 → 编码 →
  喂 socket;`Content-Length` = `n`。507 换分区重试 = 该副本重跑第二遍。
- **确定性论证**:两遍由**同一块读循环 + 同一编码器参数**驱动;四算法族流式编码在同库、
  同参数、同输入分块下输出逐字节确定(块边界由喂法决定,共用 helper 即锁定)。即使任何
  事先未料的漂移出现,唯一后果 = 声明 CL ≠ 实际字节 ⇒ store"恰等 CL"校验 400 拒收 ⇒
  该次写入失败**可见**,**不可能落半条记录**(I2 保护)—— 兜底是硬的。
- `UploadSource` 三态:`File(Path)`(原文)/ `Frame(Bytes)`(内存帧)/ `TwoPass { frame_len }`
  (每 body 即时编码)。`_frame` RAII 语义退役;TMPDIR 依赖清零。

### 2.2 配置与监控

- 新旋钮 `router.compression_frame_mem_max_bytes`:**默认 8 MiB**;**语义 = 内存帧/两遍的
  分界(内存 × CPU 取舍拨盘),不是压缩截断点**;`0` = 永不走内存帧(全部两遍);上界
  64 MiB(Validate)。上界 × 并发 put_file = 内存上界(同 `INLINE_BODY_MAX` 精神)。
- 监控:`router_compression_attempts{outcome}` 标签集 += `two_pass`(标识走了型二);
  `router_compression_saved_bytes` 两型同语义(orig − stored)。

### 2.3 取舍(如实)

- **CPU**:>上界文件编码次数 1 → **1 + N**(N = 副本数;3 副本 ≈ 4× 编码 CPU)。纯墙钟
  增量 ≈ 第一遍计数的前置时长(zstd-3 ≈ 300 MB/s/核,1 GiB 文件 ≈ 3-4 秒);N 次编码与
  网络发送流水线重叠,通常仍被网络时间掩盖。小文件主场景(≤上界)零增量。
- 多副本不共享编码产物(今日共享帧文件的带宽收益让位于零盘;确定性保证各副本字节一致)。
- 型一超上界的半途编码字节被丢弃(最坏多 ~0.5 遍计数 CPU;"无缝续数"优化可消,留实施)。
- **B 段不覆盖网关墙③**(frontend 自制 `TempFile` 上传中转):其根因(SDK 无流式 body 写口)
  与 B 段在 router 内消除压缩帧是两件事;frontend 去法两案(frontend 整包内存上限 / 边收边
  分片)见 proposal 裁决点 4,获批后另补细案。

## 3. A 段设计(store 服务端:log 预留区直写 + 大小分道)

### 3.1 为什么必须分道(否决 A-i 的核心论证)

-prefix 可见性 + 段中慢写者 = HOL:大对象若写进小对象的活跃段,其 commit 前该段
`data_end` 不动,期间落在其后的**所有**小对象(数据已写、索引未发布)对读者不可见、
ack 挂起 —— 这正是暂存机制用双写换掉的后果(§0 实测教训的放大版)。
**分道破局:大对象独占专属 lane 段,该段 `data_end` 只被它自己的 commit 推动 ——
段内前缀纯度保真,小对象在直行道完全无感。**

### 3.2 Log 层新机制

- `Log` 增 lane 位:`lane: Option<Arc<LogSegment>>` + lane 段上有"活动预留登记"。
- `reserve_large(total) -> LargeReserved`(在 write_lock 短临界区内):
  - 选段:无 lane 或 lane.remaining < total → 滚新段为 lane(旧 lane 若无活动预留且
    remaining < SEAL_LAZY_BYTES(1 MiB,见 3.5)→ 走既有 `pending_seals` 封存);
  - 段登记 start(uint 池内唯一);**每 lane 同时至多 1 个活动预留**(K=1;lane 数
    `store_server.put_lanes` 默认 2,即并行大对象数,对齐今日暂存的天然并行度)。
- `LargeReserved::write(off, buf)` = 定位 pwrite(`write_reserved` 复用,**不需要引擎锁**:
  专属段 + 区间独占);调用方为 async 转发任务(见 3.3)。
- `LargeReserved::commit()` = 短池任务(write_lock 瞬段):CRC 尾补 `write_reserved` →
  `commit_reserved` → 索引 `put` → 统计/dirty 递增 —— 与今日 `put_from_reader` 尾段
  完全同序,只是数据早已就位。
- **abort 回卷**:drop/显式取消 → 若该 lane 无更晚活动预留(K=1 ⇒ 恒成立)→ lane 的
  逻辑起点回卷到弃单 start,**零空洞零浪费**;若有(未来 K>1 再议),尾回卷到最小存活
  start,中间空洞 ≤ 弃单长度,由 compaction 收(记档,初版不做 K>1)。
- 小对象 `append`/`put_from_reader` 路径**一行不改**(同锁同段同序)。

### 3.3 store server handler 三段化(CL > 1 MiB 分支替换)

```
1) 前置闸(不动):CL vs max_body_bytes → 413;dup 前查 → 409(今日在引擎锁内查,
   长时序等价:提前查省整次传输;-commit 兜底复检,竞态最坏 = 409 + 弃单回收,同今日)。
2) reserve_large(池任务,短)→ 507 映射(StoreFull 同今)。
3) 转发环(reactor):body 逐块 → crc32fast 随算 → chunk 经 spawn_blocking 散量子任务
   pwrite 进预留区(不钉任何池线程;每 chunk 一次短阻塞任务,长度 256 KiB 级);
   逐块校验"不多不少恰 = CL"(同 stage_body 契约)。空闲超时(put_stream_idle_timeout,
   默认 60s)或客户端断开 → abort 路径(400/取消,回卷)。
4) commit(池任务,短)→ finish_put(201/409/500,指标口径不变)。
```

- **池占用形态变化**:今日 = 一次长池任务(读暂存进引擎);新 = 每上传 2 次短池任务 +
   转发环的散集阻塞写。每盘池 (N4) 不再被长任务占座;`Saturated` → 503 语义不变。
- 取消安全:未来 drop → channel 关闭 → 转发环终止 → abort 回卷(双出口:
  显式/取消,均落池;对齐今日 `StagedFile` 的双出口纪律,**响应发出 ⇒ 预留已收** --
  硬契约同款)。

### 3.4 崩溃/持久性红线走查(CLAUDE.md §3.3 逐项)

| 检查项 | 结论 |
|---|---|
| 写路径触及 | ✅ 集中评估:数据落点 = 预留区内 pwrite,与今日"暂存文件→引擎写 log"殊途同归,且少一跳 |
| fsync 边界 | **零变**:flush(段 header + sync_all)仍是唯一持久化边界;中转不再存在,反而少一个"以为落了实则没 fsync"的中间物(今日暂存就是 flush-only,不 fsync,注释明文) |
| 成功回报时机 | 不变:201 = 数据在 log 内 + 索引发布(进度:容忍到下一次 flush 的 at-risk 窗,口径与今日全同) |
| 中途失败一致性 | 未 commit 预留:读者/恢复不可见(`data_end` 未动);崩溃后自然消失;zero 残留物(今日还依赖启动清扫 *.tmp,新法免) |
| 重删幂等 | 409 复检在 commit 时点,与今日同一时序位 |
| 资源清理 | 预留即资源(lane 登记),abort/commit 双出口;响应发出 ⇒ 收口(硬契约) |

### 3.5 密度论证(seal 阈值)

lane 段无活动预留且 `remaining < SEAL_LAZY_BYTES`(1 MiB = 一个大对象都容不下;段容量
4 GiB)→ 封存:损耗 ≤ 1 MiB/段 ≈ **0.02%**,且发生频率 = 段尾自然现象(与直行道 seal
同型)。不引入"lane 回收给小对象"的复杂化(否决矩阵 #6)。max_segments 预算:摊销段数
≈ 今日(大对象字节照常填满段才封)。

### 3.6 配置与监控

- 配置 +2:`store_server.put_lanes`(默认 2,1..=8)、`store_server.put_stream_idle_timeout_secs`
  (默认 60,5..=600);`staging_dir` **落地后退役**(knob 保留解析不报错,文档明示
  deprecated —— 配置兼容红线)。
- 监控 +4:`store_server_put_lane_segments`(gauge,活跃 lane 段数)、`store_server_put_lane_reserved_bytes`
  (gauge,在途预留字节)、`store_server_put_lane_aborts_total{reason=timeout|short_body|long_body|client_gone}`
  (counter)、`store_server_put_prewait_lanes_total`(counter,抵达时全 lane 占用的排队数)。

## 4. 否决矩阵(带理由)

| # | 方案 | 否决理由 |
|---|---|---|
| 1 | A-i 单活性段流式直写(大对象进小对象活跃段) | HOL:大对象传输期间该分区所有后续 put 的可见性/ack 挂起 = 暂存机制实测教训放大版;p99 小对象直接坏死 |
| 2 | **全量**两遍确定性(小文件也两遍) | ① 裁定前本行曾整案否决;**勘误并改立场(2026-10-21)**:① 两遍并不"需服务端无 CL 路径"—— 计数遍给出了精确 CL,服务端零改;② 真正的否决理由只剩"全量 ×(1+N) 编码在小文件主流下 CPU 过载"—— 故两遍**限 >上界选型**(① 裁定采纳);小文件必须留在内存帧广播 |
| 3 | SeaweedFS 式采样启发(128 KiB 样本估比率) | 比率失真(判错认栽:帧可能反而更大/可压文件被弃);违背 I6 诚实比率;同样依赖无 CL 路径 |
| 4 | 全内存缓冲大对象 | CLAUDE.md §5.3 红线(不把整个 blob 缓进内存) |
| 5 | 暂存指 tmpfs/内存盘 | store.md 明文警告(内存压力直接顶死) |
| 6 | lane 段回收给小对象复用 | 密度已由 0.02% seal 损耗收口;换来的状态机复杂度(lane↔active 迁移竞态)不值 |
| 7 | 无 CL(chunked)大 body 流式升级 | 无内部生产者(router 全链 CL 恒已知);引擎记录长度 u32 形态; услуги面维持缓冲回落即兼容 |

## 5. 测试计划(验收 = 不变量逐条有断言)

**B 段**:三闸矩阵(型一 vs Bytes 口逐语义对拍);上界边界(恰等/恰超);超界可压文件转
型二(读回逻辑等长 + TMPDIR 零触 + outcome=two_pass);**两遍确定性对拍**(同文件两轮完整
传输的帧字节哈希相等;507 重试后仍一致);**CL 恰等兜底**(构造"声明≠实际"错 → 服务端
400,库中无半记录);>上界的熵大文件比率不过 → `Original{low_ratio}`(压缩不动的原文
—— 与超上界可压文件走型二成对断言);型一半途超界的"续数"与"重数"两口径一致(若实现
续跑优化);帧路径全程**不触 TMPDIR**(tempdir 基座夹具 + 计数断言);指标挂实例断言
(§6);集群混存(旧 tempfile 帧时代写的对象照常读 —— 帧格式零变,免回归)。

**A 段**:e2e = 100 MiB PUT 无暂存文件断言(各盘 tmp 目录差集)+ 读回逐字节;中途断开
(abort 粒度:超发/少发/客户端断),`data_end` 不动 + 回卷后空间复用(下一条 PUT 落同
start);大对象 30s 慢速传输期间并发 1 KiB PUT ack 延迟对照组(I5 的量化断言,无 sleep —
恒定源/限速器确定性);409 前查与 commit 复检双路径;507(lane 满且全 lane 占用 + 段满
双重注入);崩溃一致性 proptest:传输中 kill 点采样 → 恢复数 = 上次 flush 版本、无残留
预留告警(gauge 归零);每盘池饱和注入 → 503(短任务化后的池行为不回退)。

## 6. 实施切分与门禁

- **PR-1(B 段)**:router 修改 + 旋钮/指标 + 测试 + router.md(common.md 若指标公共
  化)双表 → 全量门禁 → 本 change 的 B 部分 spec 勾验。
- **PR-2(A 段)**:blob(lane 机制)+ store(handler 三段化)+ 旋钮/指标 + 测试 +
  store.md/router.md 退役说明 → 全量门禁 + colima 大数据量复验(4 GiB 滚转口径不回退)。
- 两 PR 各自独立可回退(revert 单提交;A 段退役 = 回落暂存路径代码未删,首版共存
  `store_server.put_stream_direct` 内闸默认 true;显式 false = 旧暂存路径 —— 灰度逃生口,
  稳定一个版本后删)。**§3.4 磁盘格式零变 ⇒ 升级顺序无约束(读写天然前后兼容)。**

## 7. 风险与残余

- A 段 = 引擎写路径手术(红线区):最大风险 = lane 状态机竞态(并发 reserve/abort/seal);
  缓解 = 全状态变更收进 write_lock 短临界区 + 上述测试面;灰度闸(内闸保留旧路径)。
- B 段残余 = cap 以上可压文件密度回归(裁决点 ① 已列回退选项)。
- `write_all_at` 转发环在 reactor-adjacent blocking 散集:每 chunk 无序完成不伤正确性
  (定位写),但极端块乱序落盘不构成任何可见性问题(commit 前不可见)。

## 9. 网关墙③方案(④ 已裁定 2026-10-21:边收边分片)

> 所有者裁决:网关上传不再"先整包中转再切片",改为**边接收边分片直接写进集群**;
> 断流孤儿由既有清理机制兜底(残余风险见 9.3,已明码接受)。

### 9.1 分档(配置零新增;对象语义对既有契约零变化)

| 档 | 判据(有 CL) | 行为 | 内存 |
|---|---|---|---|
| 小 | ≤ 1 MiB(INLINE) | `to_bytes` → 单 blob `put_auto_with_ttl`(现状不动) | 1 MiB × 在途 |
| 中 | 1 MiB < CL ≤ `max_body_bytes`(默认 4 MiB) | `to_bytes(max)` → 等长核验 → 单 blob(**核验通过才写 ⇒ 该档零孤儿**) | 4 MiB × 在途(与 S3 网关同界) |
| 大 | CL > `max_body_bytes`(auto_chunk 开,默认开) | **流式分片**:攒满 `auto_chunk_bytes`(默认 4 MiB)即 `put` 一片;EOF 对账(Σ==CL,片数 ≤ 4096)→ 描述符 | 单片缓冲 × 在途 |
| 无 CL | 自适应 | 攒到 `max_body_bytes`:EOF 在内 = 单 blob;溢出 = 已攒部分作首片、转入流式分片 | 同上按档 |

- `auto_chunk` 关:大档一律 413(旧契约不动);无 CL 超 cap 同 413(转档前零写 ⇒ 零孤儿)。
- 对象形态不变:**≤ max_body = 普通 blob;> max_body = 拼装(描述符 + 片,同 TTL)** —— GET/HEAD/拼装/配置零改。
- 每片仍走 `put_auto_with_ttl(Bytes)` + `compress_for_put`(同今日分片路径)⇒ **网关从此不调
  `put_file` —— A2(router 压缩帧临时文件)从网关路径彻底消失**,只余 SDK 直用户。

### 9.2 失败与清理

断流/字节不符/片数/超发/任一片失败耗尽内部重试 → `cleanup_acked_chunks`(既有函数,逐片
`delete` 墓碑,幂等)→ 400 + 清理指标;单片清理失败记 warn 留孤儿(无对账面,趟 9.3)。
201 的对外语义 = 描述符落库(片先于描述符;描述符是可见性原子门,既有语义原样)。

### 9.3 孤儿风险(净变化,已拍板接受)

- 今日"先整包中转"= 任意失败**零孤儿**;④ 后:**断流且进程活着** ⇒ 墓碑清理兜(个别墓碑
  失败留孤儿);**进程崩溃窗** ⇒ 已入库片无人认领(frontend 无 metastore/无 GC)。
  窗口 = 在途上传 × 已写片量;频率 = 网关崩溃于上传中(低)。
- 留观缓解(不在本批,档此):描述符先行 / 断流 WAL(类比 s3-reclaim-wal)/ 低频孤儿清扫。

### 9.4 监控

新增 counter `frontend_upload_chunk_cleanups_total`(断流清理触发次数;成功/失败语义靠
该读数 + 既有 `upload_total{outcome}`/`upload_auto_chunk_chunks_total` 合成)。frontend.md
双表随批更新(CLauDE.md §7)。

### 9.5 测试面

大档 e2e(100 MiB:TMPDIR 差集零文件 + 读回逐字节 + CL 对账);中小档边界恰等/恰超(4 MiB);
无 CL 自适应三分支(EOF 内单 blob / 溢出转分片 / 超 4096 片 413+全清);断流注入(片界
前/后 + 客户端 abort)→ 400 + 逐片墓碑断言 + 指标递增;超发(声明 5 MiB 实发 6 MiB)→
已写片全清;auto_chunk 关(413 且零写);cleanup 幂等(重复清理属性断言);崩溃窗孤儿
仅记档不做恢复断言(无 GC 面,诚实标界)。

## 8. 附录:全场景临时文件清单(2026-10-21 全量清点,行号级)

> 取证方法:大小写不敏感全库扫描(`tempfile|temp_dir|tempdir|NamedTempFile` + `.tmp|/"tmp"|
> staging|unique_suffix|自制 RAII 结构`),逐处人工分类运行时 vs 测试内联;教训 = 自制
> `TempFile`(非 tempfile crate)曾致漏检。测试/基准夹具的 `tempdir()` 不入列(非运行时)。

| # | 场景 | 触发条件 | 落盘位置 | 生命周期 | 与压缩关系 |
|---|---|---|---|---|---|
| **A 上传路径(墙)—— 本 change 的作战面** ||||||
| A1 | frontend `POST /blobs` 上传中转(`server.rs:426` `TempFile`) | body > 1 MiB 或无 CL | **TMPDIR**(`antfs-frontend-{pid}-{ns}.tmp`) | Drop 同步删(async 线程直删) | 无关 |
| A2 | router `put_file` 压缩帧(`compression.rs` `NamedTempFile`) | 压缩开 + 过前置闸 | **TMPDIR** | RAII 守护跨全副本/重试 | **独有**(此墙之本源) |
| A3 | store 数据面 PUT 暂存(`server.rs` `StagedFile`) | CL > 1 MiB | **数据盘** `(盘)/tmp/` 或 `staging_dir` | 正常池删 + Drop 兜底 + 启动清扫 | 无关(数据不透明) |
| **B 下载路径(呈报,不在当前订单)** ||||||
| B1 | frontend `GET /blobs` 大对象中转(`server.rs:626` `get_file`) | 返回体为非拼装大对象 | **TMPDIR** | 随响应流 drop(发完/断连即删) | 无关 |
| **C 引擎/集群机制性 tmp(数据盘上;原子发布/探测,非中转语义)** ||||||
| C1 | 拉复制 bootstrap 物理拷段(`replica.rs:336`) | 空副本从对端拉段 | 副本 `log/{段名}.tmp` → fsync → rename | 失败清整个半成品目录 | 无关 |
| C2 | 索引 / Binary Fuse filter 构建(`index/segment.rs:225`) | 段封存构建期 | 数据目录 `.tmp` → rename | 原子发布 | 无关 |
| C3 | COMPACT 状态原子写(`compact_state.rs:39`) | 压实政策状态更新 | `{STATE_FILE}.tmp` → rename | 原子发布 | 无关 |
| C4 | 磁盘健康探测短线文件(`health.rs`,§12.12 记档) | 探测开启时周期 | 数据盘 `tmp/`(即建即删) | 单次探测内 | 无关 |
| **D 语义 = 数据/零盘佐证** ||||||
| D1 | devcluster 测试内 tempdir(非运行时数据位置;生产 devcluster 数据走显式目录) | 仅测试 | — | — | — |
| D2 | **S3 网关全链零临时文件**:PutObject/UploadPart/Copy/读回/拼装全部 `Bytes` 内存(`read_bounded_body` 上限 `max_body_bytes` = 4 MiB/对象,过限拒收;更大对象走 multipart 语义)。附发现:死旋钮 `s3.inline_body_max`(声明 + 校验,无任何使用点)—— 留观,不擅动 | ||||

**上行叠加账(如实)**:经 frontend 的开压缩 10 MiB 上传最多踩 A1+A2+A3(三份临时拷贝);
关压缩仍有 A1+A3(两份)。经 S3 的 ≤ 4 MiB 对象只有 A3 一份;> 4 MiB 走 multipart,每片
≤ 4 MiB,内存分片 + 每片过 A3。**"零临时盘"在同一对象上的真实含义 = 把所在路径上的 A* 全消。**
