# Phase 2 基线压测(BASELINE)

> **用途**:Phase 2「优化」的对照零点。任何性能改动前后都应在本口径下重跑对比。
> 数字属 `colima` VM 级参考(见环境),**不是**生产硬件数字;目的一是分层瓶颈定位,二是回归对照。

## 环境

| 项 | 值 |
|---|---|
| 日期 / commit | 2026-10-14 / `62fadb1`(Phase 1 Exit 后) |
| 平台 | colima default(aarch64,Apple Virtualization.Framework),4 vCPU / 8 GiB / 40 GiB disk |
| 文件系统 | VM 原生 ext4(`/`,数据目录 `/var/tmp`);源码 virtiofs 只读参与编译,`target/` 与数据落 ext4 |
| 构建 | `cargo build --release`(VM 内,`./vm-cargo.sh`) |
| 拓扑(集群层) | 进程内 1 master(单节点 Raft)+ 3 store(各 1 盘)+ frontend;压测客户端**另起进程** |
| 默认引擎参数 | segment 8 MiB(devcluster)/ 64 MiB(bench-blob);index 100k;W=2/3 quorum |

## 分层结果

### L1 引擎直压(`antfs-bench-blob`,单机 BlobStore,4 线程)

| 对象 | 写吞吐 | 写延迟(p50/p99) | 热读吞吐 | 热读延迟(p50/p99) |
|---|---|---|---|---|
| 4 KiB × 50k | **140 389 ops/s / 548 MB/s** | 3.3 µs / 220 µs | **1 046 346 ops/s / 4 087 MB/s** | 3.7 µs / 5.3 µs |
| 64 KiB × 10k | 22 783 ops/s / 1 424 MB/s | 23.2 µs / 91.8 µs(另见 max 236 ms,段滚转) | 257 706 ops/s | 14.8 µs / 22.2 µs |
| 1 MiB × 2k | 1 752 ops/s / 1 752 MB/s | 247 µs / 20 ms(滚转+flush 窗口) | 22 756 ops/s | 152.8 µs / 354.7 µs |

> 读为**全热 page-cache**口径(压完立读);冷读需另行测。put 尾部 max 台阶来自段滚转与 fsync 窗口,属设计内行为。

### L2 router 直连 e2e(W=2/3 quorum,HTTP 数据面,另起进程压)

| 场景 | PUT | PUT p50/p99 | GET | 备注 |
|---|---|---|---|---|
| 4 KiB × 20k,c=32,读 20% | **21 961 ops/s / 85.8 MB/s** | 1.33 ms / 4.6 ms | 72 192 ops/s | verify 64 blob 全对;号段分配三分区 33.3% 全平 |

### L3 frontend REST(加 HTTP 网关翻译与并发协调)

| 场景 | PUT | PUT p50/p99 | GET | 备注 |
|---|---|---|---|---|
| 4 KiB × 20k,c=32,读 20% | **12 428 ops/s / 48.5 MB/s** | 2.45 ms / 5.45 ms | 25 406 ops/s | err 1/20 000(0.005%,单次 quorum 未达→503) |
| 5 MiB × 200,c=8(auto-chunk:2 片+拼装描述符) | **31 ops/s / 152.8 MB/s** | 257 ms / 323 ms | 132 ops/s(660 MB/s) | **需 `--partition-capacity-bytes` 调大容量**(见下),verify 全对 |

## 4 GiB 段(生产口径)复测 —— 滚转问题的真实形态

> 本节修正前版"生产 4 GiB 段下台阶不显形"的**外推误判**:实测后结论反转,滚转问题在生产口径下
> **真实存在,而且更凶** —— 只是形态从"高频小台阶"变成"低频秒级停摆"。

| 场景(1 MiB 记录,4 线程) | p99 | **max** | 备注 |
|---|---|---|---|
| 64 MiB 段 × 2000(零滚转…几乎每 62 条滚一次) | 11.8 ms | ~0.8 s | 高频小台阶(上一版口径) |
| **4 GiB 段 × 2000(零滚转,2 GiB)** | 616 µs | 317 ms(VM 毛刺) | 无滚转,写路径干净 |
| **4 GiB 段 × 5000(1 次真实滚转,裸)** | 1.09 ms | **1.16 s** | 滚转瞬间的 fsync 4 GiB ≈ 秒级 |
| **4 GiB 段 × 5000 + flush 2s(生产近似)** | 870 µs | **1.53 s** | 周期 flush **救不了**,甚至更糟 |
| 4 GiB 段 × 5000 + flush 500ms | 711 µs | **1.19 s** | 同上 |

- **4 KiB × 50k @ 4 GiB**:165 702 ops/s / 647 MB/s,p99 251 µs,max 4.2 ms(无滚转,基线主场景干净)。
- **归因(多次微基准交叉验证)**:fallocate 4 GiB ≈ 1.4 ms(罪名彻底洗清);fsync 是吞吐墙
  —— VM virtio-blk 上 fsync 大脏段的速率 ≈ 本盘写吞吐(~3 GB/s),即 **fsync N GiB ≈ N/3 秒**,
  任何调度节奏分摊不掉总量(周期 flush 只是把同样的字节挪时间)。
- **结论**:4 GiB 段下,每次滚转 ≈ 一次 1-1.5 s 的**写锁全线停摆**(发起滚转的请求 + 写锁后排队
  的全部并发写;本测量 3 分多钟才滚一次,p99 被稀释到看不见,**风险全藏在 max / p99.9+**)。
  生产形态(flush worker)不减轻。低频(每 4 GiB/分区一次)但 SLA 长尾、复制追赶、同盘心跳
  全被这一个动作劫持。**滚转异步化(移出写锁)升级为 Phase 2 正经靶**,已立案:
  `openspec/changes/log-rollover-off-critical/`。

## 过程发现

1. **devcluster 默认每分区 64 MiB 是刻意小值**(便于本地验证写满 507/转 RO 路径):4 KiB 场景(≈80 MiB)不触界;**5 MiB×n 大对象场景在 ~120 MiB 后打穿容量 → store_full×2 → quorum 0/2 → 大面积 503**(表象是"全挂",实际是容量语义按设计工作)。本轮为此给 devcluster 加了 `--partition-capacity-bytes` 参数(默认 64 MiB 不变),大对象压测按 4 GiB/分区跑通。**教训入档:压大对象前先看容量口径。**
2. 容量打穿时的对外形态:frontend 503(`write quorum not reached`),bench 记为 `unavailable`;**不是**错误注入,是容量红线按设计服务降级。
3. **滚转/fsync 尾部归因(两轮修正,勿复抄第一版结论)**:第一轮(64 MiB 段口径)结论"周期 flush
   追不上、段大小是旋钮、生产 4 GiB 段不显形"——其中**"不显形"是外推,已被 4 GiB 复测推翻**:
   konkludo 见上一节(生产口径下滚转=低频秒级停摆,周期 flush 无效)。归因部分仍然成立:
   fallocate 无罪、fsync 全责、fsync 大段 ≈ 吞吐墙。
4. 各组均出现一次 max ~600-750 ms 的孤立长停顿(与滚转无关、跨形态同现):colima 虚拟盘(APFS 底层)写回收/首次大分配类环境毛刺,标注为 VM 环境噪声,不归引擎。
5. bench-blob 新增 `--flush-interval-ms`(默认 0 关):把"生产 flush worker 形态"带进裸引擎压测的能力(A/B 证明本速率下它救不了滚转,但保留了形态对齐的实验口)。

## 修复后决算(`log-rollover-off-critical` 落地)

> 判别链:零滚转口径(1 MiB×2400@4 GiB)max 330 ms 干净 ⇒「秒级尾只在含滚转场景」;
> 第二病灶揪出并修序 —— `LogSegment::create` 原为 preallocate→sync_all(满载下 4 GiB 段的
> fallocate+fsync 秒级),现改「header 先 sync(64 B 级,快)→ fallocate 后置不强落盘」,
> `Log::open` 对 0 字节半建段残骸(建段瞬窗)宽容跳过。
> **决算口径取 64 MiB 段**(高频滚转、代码效应占比大、环境毛刺占比小):

| 1 MiB × 2000 @ 64 MiB 段(4 线程,裸) | p99 | 吞吐 | 备注 |
|---|---|---|---|
| 修复前(= sync 档) | 11.8 ~ 17.5 ms | 1958 ops/s | 基线 / S64 两轮同量级 |
| **修复默认(async,`defer_seal_flush=true`)** | **0.92 ms** | **2333 ops/s** | **p99 ×19、吞吐 +19%** |
| `--sync-seal-flush`(`store.defer_seal_flush=false`) | 17.5 ms | 1958 ops/s | 旧行为一键回退保留 |

- 4 KiB × 50k @ 4 GiB:172 269 ops/s / 673 MB/s,p99 223 µs(主场景无回归;较基线 140k→172k)。
- **4 GiB 大段口径在 colima 不可判**(async/sync 的 max 同现 ~1.1-1.2 s):VM 虚拟盘容量级
  毛刺(第二段 4 GiB 预分配触发盘映像扩容)主导;同族病灶移除的工程推断成立,但**标注
  「真机复验」**。1 MiB × 2400 零滚转 max 330 ms 作参照。
- 切换语义字节级钉子(引擎测试):`rollover_defers_seal_fsync_until_flush`(flush 前封存段
  header data_end 陈旧 → flush 后追平)/`rollover_sync_seal_flush_keeps_legacy_behavior`
  (滚点即落盘)/`close_drains_pending_seals`;持久性契约「flush 是边界」由既有 crash-consistency
  属性面继续守护。

## frontend → router 45% 折损耗解剖(并发扫描 + CPU 预算,归因完成)

| 并发(c) | router 直连 | frontend REST | 折损 | p50 差(router→frontend) |
|---|---|---|---|---|
| 8 | 18 949 ops/s | 12 666 ops/s | 33% | 377 µs → 589 µs(**固定税 ≈200 µs/req**) |
| 16 | 20 769 | 12 332 | 41% | 684 µs → 1.20 ms |
| 32 | 20 051 | 13 243 | 34% | 1.39 ms → 2.34 ms |
| 64 | 20 657 | 11 837 | 43% | 2.84 ms → 5.11 ms(斜率放大 ≈7×) |

- **CPU 峰值采样**(80k ops / c=64 重载):客户端进程 31%,**devcluster 全家桶单进程 228%**,整机 idle ≈47%(未跑满)。
- **归因**:折损 = ①「每请求固定税 ~200µs」(c=8 即 33%,HTTP 网关两侧栈:axum 解析/body copy/reqwest 客户端/响应组装)+ ②「并发放大项」(斜率 7×,单进程全家桶在前端 HTTP 层与 3×store/master 之间挤占 4 核预算导致的协调开销)。
- **判定**:VM 4 vCPU 的「frontend 12.4k vs router 21.9k」**不是**纯网关税形状——多核真机按 ①外推折损应 ≤33%;前端的工程性减税(轴向:减 body 拷贝层、批量头组装)列为后续可选-task,单 vCPU 配置失真列为**真机复验项**。两 bench 吞吐在 c=8→64 全平 = 达到层次容量(非 CPU 瓶颈)。

## 冷读口径(`--evict-before-read`,冷数据·暖索引,guest 级)

| GET(4 线程) | 吞吐 | p50 / p99 | 带宽 |
|---|---|---|---|
| 热读对照(4 KiB × 50k) | 1 083 552 ops/s | 3.6 µs / 4.8 µs | 4233 MB/s |
| **冷读 4 KiB × 50k** | **192 497 ops/s** | **3.9 µs / 152.6 µs** | **752 MB/s** |
| **冷读 64 KiB × 10k** | **57 510 ops/s** | 33.4 µs / 234.3 µs | 3594 MB/s |

- 口径:`fadvise(DONTNEED)` 驱逐**数据段**页(guest 层);索引 mmap 页驻留(暖索引——如实记录,常态生产形状)。
- **双缓存层警示**:colima 的"冷"只冷 guest——virtio-blk 底下的 macOS(APFS 页缓存)仍热,p50 µs 级证实大部分读被宿主立即服务;峰值带宽(顺序重填 3.6 GB/s)≈ 虚拟盘顺序吞吐墙。**真掉电冷读只能真机测**;本口径的价值是「guest 填页成本 + 引擎读路径开销上界」:引擎读代码本身在冷口径下不显形(p99 微秒级增长),读路径优化空间不在引擎层而在页/预读层次。
- 4 KiB(752 MB/s)vs 64 KiB(3594 MB/s):顺序预读把小粒度随机读吸收成大块传输——小文件冷读的吞吐敏感度取决于预读窗与局部性。

## 下一步真正值得打的靶(修正后)

- ~~滚转异步化~~ ✅ 已落地(64 MiB 段 p99 ×19)
- ~~frontend→router 折损解剖~~ ✅ 已归因(固定税 200 µs + 单进程 CPU 争挤;真机复验 + 可选网关减税工项)
- ~~冷读口径~~ ✅ 已建立(双缓存层警示 + 真机复验)
- 真机复验清单:4 GiB 滚转(虚拟盘毛刺遮蔽)/ frontend 折损形状 / 掉电冷读 —— 三项合一上 Linux 真铁

## 真机复验(2026-10-15,原生 macOS,非 VM)

**口径**:Mac17,9 / 48 GiB RAM / 15 核(P5+E10)/ 926 Gi APFS / Apple SSD(顺序写实测 7.4 GB/s)。release 构建,数据在 `${TMPDIR}`(同卷)。结论按「±本机真铁」记;Linux/ext4 裸机另测。

### ① 4 GiB 滚转(1 MiB × 5000,恰 1 次滚转)——真机判读反转

| 模式 | ops/s | p50 | p99 | max |
|---|---|---|---|---|
| 异步 seal + flush 2s | 1561-1807 | ~265 µs | **182-185 ms** | **1.19-1.54 s** |
| 同步 seal + flush 2s | 2816-2954 | ~216 µs | **10-12 ms** | **1.18-1.30 s** |
| 裸异步(无 flush) | 2929 | 211 µs | **4.1 ms** | 1.11 s |
| 裸同步 | 1655 | 265 µs | **210 ms** | 1.55 s |

- **max ≈ 1.1-1.6 s 四模式皆在** → colima「VM 虚影盘毛刺」理论**修正**:不是虚拟盘虚构,是**多 GB 脏积压一次 fsync 的真实代价**;VM 遮住的是模式间差异、量级本来就真实。生产含义:4 GiB 段的滚转尾随一次秒级 fsync,**defer seam 不能缩量只能挪位**。
- **裸 A/B 是发版修复的铁证**:异步 p99 4.1 ms vs 同步 210 ms(**≈50×**,colima 64 MiB 口径为 19×)——`defer_seal_flush` 在真铁成立。
- **flush worker 在 GB/s 级写入下污染 p99**(~100-200 ms 摆动,异步带 flush 两轮 p99 全劣化):APFS 上周期全量 fsync 与并发写互相限流。生产单位写入低(≈百 MB/s 级)不受此形状影响;**P2 已试并反转(2026-10-15 同日)**:机制落地 `flush-bounded-dirty-window`(引擎脏计数 + worker 水位早醒,`store_server.flush_dirty_threshold_bytes`),**默认 0 = 关** —— 20 GB/4 线程口径真机 A/B:周期臂 max 1.14-2.07 s(本病理如实复现);水位 256 MiB 臂吞吐 −60%(1347-1403 vs 3229-3521 ops/s)、max 反至 1.65-5.5 s —— APFS 上「fsync×并发写互踩」随频数放大,小拆窗把巨停摆换成一串中停摆,净亏;生产百 MB/s 级两态等价;ext4 未测。详见 change 档「真机证据」。
- 64 MiB 段(1 MiB × 6000,80 滚转)带 flush:async p99 105 ms / sync 28 ms——同为 flush 窗口噪声,单模式下难分优劣(与 colima 19× 判读差异 = I/O 栈不同,**横移差以裸 A/B 为准**)。
- 64 KiB × 50k @ 4 GiB(零滚转锚):28.4k ops/s,p50 21.8 µs,p99 4.8 ms,GET 暖 10.1 GB/s——无回归。

### ② frontend 折损形状(4 KiB PUT,稳态,同进程 devcluster 单机)

| c | frontend REST | router SDK 直连 | 折损 |
|---|---|---|---|
| 1 | 4.1-4.7k ops/s | 5.2-7.0k | ~25%(低并发句柄税)|
| 4 | 10.7-11.3k | 14.3k | **~23%** |
| 8 | 12.7-13.9k | 14.6-15.1k | **~12%** |
| 16 | 12.6-12.8k | 14.0-15.6k | **~12%** |
| 32 | 13.2-13.6k | — | — |
| 64 | 12.7-14.5k | — | — |

- **colima 45% 折损在真机缩水到 ~12-23%**(中并发口径)**,VM 4 vCPU 的「单进程全家桶挤核」失真因子被证实**(解剖节预判「多核应 ≤33%」兑现并更好)。固定税依旧在(c=1 时 25%),工程性减税(减 body 拷贝/批量头)仍是可选方向但收益上限已见。
- **err 窗口现象(诚实记录)**:连扫矩阵期间 router 直连偶发 27-85% err(p99 43-100 ms、max 0.9-1.1 s 停顿),单发型复测始终全绿;时间轴与**分区 auto-grow/bootstrap、复制重启、64 MiB 容量耗尽转 RO**事件共窗。同store日志无任何 503/静默记录。归因(高置信、未逐字点名错误 label):**引导/变更窗口写失败快** 族(与测试夹具 503 同族)+ 小段(8 MiB)高频滚转×周期 flush 家族的 I/O 限流对齐(bare-run 无此窗)。**这是客户端上线/扩容窗口的运维提醒:首写需 5xx 重试语义**,非稳态缺陷。错误类别直方图因瞬态未能捕获,列留观。

### ③ 冷读(掉电口径)——macOS 无 root 不可达,方法学记录

- `purge` 需 root(免密 sudo 不通);**100 GiB 垃圾写回不产生 LRU 驱逐**(冷读 14.2 GB/s ≈ 暖 17.6 GB/s,数据页全在缓存——macOS 写回不等价驱逐,此法作废,后勿重试)。
- `--reopen`(进程重启 + 索引自盘重载)vs 暖:p50 38.4 → 41.3 µs、p99 118 → 388 µs——**重启/恢复窗的读代价极小**,引擎 mmap 索引重载不显形。
- **真掉电冷读只能 Linux 裸机**(`--evict-before-read` fadvise 口径已备)或真断电;本机结论保留 colima 口径的判断:引擎读路径自身在冷口径无显形代价,敏感度在页/预读层。

### ④ 复测工具沉淀

`antfs-bench-blob` 新增:`--reopen`(走 `BlobStore::open` 重开既有库,冷/重启复测)、`--total-mib`(显式容量对齐建库口径)、`--ops 0`(纯读模式;GET 键空间 = read-ops,调用方对齐上轮 PUT 数)。

- 判定更新:真机复验清单 ①② native 完成;③ 方法受限留 Linux 裸机一期(colima guest 口径仍为当前引用)。

## 复现

```bash
# 引擎层(VM 内)
./vm-cargo.sh build --release -p antfs-bench-blob -p antfs-bench-router -p antfs-bench-frontend -p antfs-devcluster
colima ssh -- /var/tmp/antfs-target/release/antfs-bench-blob --ops 50000 --value-size 4096 --threads 4 \
  --read-ops 50000 --read-threads 4 --dir /var/tmp/bench/blob-4k

# 集群层(VM 内,另起进程压)
colima ssh -- sh -c '
nohup /var/tmp/antfs-target/release/antfs-devcluster --dir /var/tmp/bench-cluster --host 127.0.0.1 \
  --partition-capacity-bytes 4294967296 > /var/tmp/bench-cluster.log 2>&1 &   # 等 "frontend listening"
/var/tmp/antfs-target/release/antfs-bench-router  --master 127.0.0.1:7101 --secret local-dev-secret \
  --ops 20000 --concurrency 32 --size 4096 --read-ratio 20 --verify
/var/tmp/antfs-target/release/antfs-bench-frontend --frontend http://127.0.0.1:7400 \
  --ops 20000 --concurrency 32 --size 4096 --read-ratio 20 --verify
/var/tmp/antfs-target/release/antfs-bench-frontend --frontend http://127.0.0.1:7400 \
  --ops 200 --concurrency 8 --size 5242880   # auto-chunk 路径
'
```

> 同进程压测(开发期参考):`antfs-devcluster --run-bench router|frontend`——客户端与被测共享 CPU,数字只为冒烟,不作基线。
