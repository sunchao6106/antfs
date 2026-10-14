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

## 下一步真正值得打的靶(基于 4 GiB 复测修正)

- **滚转异步化(移出写锁)**——4 GiB 段实测出低频秒级停摆,周期 flush 无效;立案
  `openspec/changes/log-rollover-off-critical/`
- **frontend → router ~45% 吞吐折损**(HTTP 翻译 + 并发协调;先 flame 定位)
- **冷读口径未测**(当前读数字全是热 page cache)

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
