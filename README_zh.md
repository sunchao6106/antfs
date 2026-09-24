<p align="center">
  <img alt="antfs logo" src="docs/assets/logo.svg" width="440">
</p>

<p align="center">
  <strong>用 Rust 实现的分布式 blob 存储系统,为海量小文件而生。</strong>
</p>

<p align="center">
  <a href="https://github.com/sunchao6106/antfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/sunchao6106/antfs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <a href="https://github.com/sunchao6106/antfs/pkgs/container/antfs"><img alt="容器镜像" src="https://img.shields.io/badge/ghcr.io-antfs-2496ED?logo=docker&logoColor=white"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-stable-orange?logo=rust">
  <a href="docs/design/README.md"><img alt="Docs" src="https://img.shields.io/badge/docs-design%20docs-green"></a>
  <img alt="PRs Welcome" src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg">
</p>

<p align="center">
  <a href="README.md">English</a> | 简体中文
</p>

---

## antfs 是什么?

**antfs 取意「蚂蚁」:像蚂蚁一样,个体微小、数量惊人、协作可靠 —— 为海量小文件存储而生。**

设计参考了 [LinkedIn Ambry](https://github.com/linkedin/ambry)、[SeaweedFS](https://github.com/seaweedfs/seaweedfs) 与 [CubeFS](https://github.com/cubefs/cubefs):小文件追加进 log、索引定位,O(1) 次磁盘寻道完成读写;集群元数据由内建 Raft 的 master 管理,不依赖外部协调组件(ZooKeeper / etcd)。分阶段推进:

- **现阶段:忠实的 Rust 翻译** —— 对齐 Ambry 的架构与语义,先把功能 1:1 搬到 Rust(仅在等价前提下用现代实现,如 Binary Fuse 过滤器替 Bloom)。
- **后续:在 Rust 版本之上做优化** —— 待翻译完成、正确性锁定后,再针对性能 / 并发 / 存储密度做改进。

## 特性

- **O(1) 小文件读写** —— 追加式 log + 索引定位;无逐对象文件系统元数据、无随机寻道。
- **内建 Raft 控制面** —— 放置、节点状态机、调谐、下线、发号(blobId 号段)全部在 master 内完成,无需额外运维外部组件。
- **自研存储引擎** —— `Log` / `LogSegment` / `PersistentIndex` / `BlobStore`:对象**不可变**、删除**不可逆**(墓碑),支持 compaction、崩溃恢复,复制合并退化为单调幂等的「墓碑恒胜」。
- **封存段快速索引** —— mmap 封存段 + 持久化 **Binary Fuse 过滤器**,快速排除不存在的 key。
- **多数派 router SDK** —— 向 master 租号段、W 多数派写、R=1 读、流式大文件;frontend 与压测共用。
- **REST 接口** —— HTTP frontend 暴露 `/blobs`(读 / 写 / 删 / TTL / 分片拼装)与 `/metrics`。
- **可观测性优先** —— 全链路 Prometheus 指标,另有 admin web 控制台(拓扑 / 节点 / 盘 / 分区 / 指标)。
- **一条命令起本地集群** —— `cargo run` 即可拉起完整 dev cluster(Raft master + 3 个 store + admin web)。

## 架构

```text
                          ┌────────────┐
                          │ Admin Web  │  拓扑 · 节点 · 盘 · 分区 · 指标
                          └─────┬──────┘
                                │
   客户端 ── REST /blobs ──▶ Frontend ── 内嵌 ──▶ Router SDK ── 租 blobId 号段 ──▶ ┌────────────────┐
                                  │               (W 多数派写,R=1 读)               │ Master (Raft)  │
                                  │                            │                    │ 放置           │
                                  │                            ▼                    │ 调谐           │
                                  │                ┌───────────────────────┐ 心跳   │ 下线           │
                                  │                │ Store 数据节点 (× N)  │ ◀───── │ blobId 号段    │
                                  └── 复制 ──────▶ │  └─ blob 引擎:        │        └────────────────┘
                                                   │    Log / LogSegment / │
                                                   │    PersistentIndex    │
                                                   └───────────────────────┘
```

## 模块

| 模块 | 说明 | 状态 |
|---|---|---|
| common | 公共库:`config`(`serde`+`config` 分层配置)、`metric`(`prometheus` 统计打点);对标 Ambry `config` 包 + Dropwizard `MetricRegistry` | 🚧 进行中 |
| blob | 单机存储引擎:`Log` / `LogSegment` / `PersistentIndex` / `BlobStore`(put/get/delete/compaction/恢复 + 复制 apply;对象不可变、删除不可逆);对标 Ambry `ambry-store` | 🚧 进行中 |
| master | 集群大脑:内建 Raft(`openraft`)管放置/节点状态机/调谐/下线/**发号(blobId 号段)**;对标 Helix+ZK 的替代 | 🚧 进行中 |
| store | 数据节点(store server):多副本编排 + HTTP 数据面 + 心跳/磁盘监控 + 拉复制;承载 `blob` 引擎 | 🚧 进行中 |
| router | 客户端 SDK(通用组件,`crates/common/router`):向 master 取号段、W 多数派写、R=1 读、流式大文件;frontend 与压测共用;对标 Ambry `NonBlockingRouter` | 🚧 进行中 |
| frontend | HTTP REST 对象接口层(`/blobs` 读写/删/TTL/分片拼装 + `/metrics`);内嵌同一个 router SDK;对标 Ambry `ambry-frontend` | 🚧 进行中 |
| admin | admin web:运维操作 + 控制面 dashboard(拓扑/节点/盘/分区/指标) | 🚧 进行中 |
| S3 网关 / 多租户 ACL、配额 | 网关层远景(与 frontend 同层) | 📋 规划中 |

> 命名:`blob` = 单机 blob 存储引擎;`store` = 数据节点(store server)。

blob 引擎当前要点:

- IO:**同步阻塞 `std::fs` + buffered(OS page cache)**,对齐 Ambry 原版
  —— **IO 原语没变**(没有引入 async/AIO);变的是"这些阻塞调用在哪个线程上跑":每块盘拥有**自己的有界池**
  (专属 worker 线程 + 有界队列),一块盘挂死不再吃光共享阻塞池、不再拖住健康盘;池满则快速失败
  (`503` + `Retry-After`)。见 [`docs/design/store.md` §12.11](docs/design/store.md)(内标"地基性变更")
  与 `docs/design/cluster.md` §6 **N4**
- **单盘故障隔离(单进程多盘)**:把"一块盘坏"从"整台机不可用"降级为"那块盘被隔离、其上副本被替换"。四层,
  默认配置下都不会误伤:

  | 层 | 做什么 | 状态 |
  |---|---|---|
  | **D1** 每盘有界池 | 一块盘挂死只吃**自己**的 worker 线程;池满 ⇒ `503 + Retry-After` | ✅ 已落地(store) |
  | **D2** 可服务性闸门 | 盘被判 `UNAVAILABLE` ⇒ 该盘上所有 `/partitions/**` 请求快失败(含拉复制读 / `replica-metadata` / `segments`);`/metrics` 与任何 `/admin/**` **永不被短路**(节点必须可远程恢复) | ✅ 已落地(store) |
  | **D3** IO 看门狗 | 盯每盘**最久未归的那一次 syscall**(不含池排队);超期记 `IO_TIMEOUT` ⇒ 同一套迟滞 ⇒ `UNAVAILABLE`。**不受** `disk_health_enabled`(默认关)约束 | ✅ 已落地(store) |
  | **D4** 坏盘撤离 | 把副本从坏盘搬走:**先补后删**,继任者**追平后**才删源(绝不丢最后一份) | ✅ 已落地(master) |
  | **D5** 判活迟滞 | 连续 K 拍观察不到心跳才判节点死;一次抖动不再等于"节点死 ⇒ 全量再复制" | ✅ 已落地(master) |
  | **D6** 再复制预算 | 每 tick 限制新发起搬迁的分区数;被限速的余量暴露成 gauge,让"正在限速"可见 | ✅ 已落地(master) |

  健康判定带**迟滞**(连续 N 轮坏 / 持续 T 秒,取先到;复位更严),故**偶尔一次抽风不会摘掉一块好盘**。
  "从未观测过"一律报 `UNKNOWN` —— 不报健康、也不报空串。完整设计、配置项、监控项、**测试证据表**与残留风险:
  [`docs/design/store.md` §12.12](docs/design/store.md)。
- **慢盘(与"盘坏"是两回事)**:**慢 ≠ 坏**,故慢盘**不摘**,而是三条更便宜的处理:
  1. **按 op 分档的看门狗门槛**(`disk_watchdog_ms_by_op`)—— compaction 拷整段比 4KiB 读慢几个数量级,
     共用一个门槛只能二选一:「真死的盘半天不被隔离」或「健康盘的 compaction 天天误报」。
  2. **`DEGRADED` 中间档**(`disk_degraded_io_ms`,**默认关**)—— **闭包内**慢 IO 占比持续超阈值即报
     `DEGRADED`:该盘**仍然服务(绝不 503)**、master **不再往它放新副本**、且**不触发撤离**
     (撤离只认 `UNAVAILABLE`)。
  3. **延迟感知读路由**(客户端侧,`router.latency_aware_routing`,默认开)—— 读是 R=1(第一个成功即返回),
     故"先试哪个副本"直接决定读延迟;顺序为 `up` → 观测延迟(EWMA)→ master 原序。它**只改试的顺序、
     不改候选集** ⇒ W/R 与冲突解决语义不变,而没有测量数据时它就是恒等变换。
     见 [`docs/design/router.md`](docs/design/router.md) 与 `docs/design/cluster.md` §9.3。

  以上全部阈值的**标定步骤**(12 个数字,目前都还没压测过)连查询与判据都写好了:
  `docs/design/cluster.md` §9.4。
- 索引:并发活跃段 + **mmap 封存段 + 持久化 Binary Fuse 过滤器**

详细设计见 [`docs/design/`](docs/design/)(总览 + 各模块);决策档案见 [`openspec/changes/`](openspec/changes/)。

## 快速开始

需要 [rustup](https://rustup.rs/) 工具链(stable)。工程**跨平台**(Linux / macOS,均 Unix):

```bash
cargo build
cargo test
cargo run -p antfs-smoke        # 存储引擎冒烟测试
```

一条命令拉起完整本地集群 —— 1 个内建 Raft master + 3 个 store 数据节点 + admin web,全部进程内运行,启动时打印各服务地址:

```bash
cargo run -p antfs-devcluster
```

### 预编译二进制

打 `v*` tag 会在 [Releases 页面](https://github.com/sunchao6106/antfs/releases)产出 Linux(x86_64 / aarch64)与 macOS(arm64 / x86_64)四个平台的包,每个包里含 `antfs-master`、`antfs-store`、`antfs-frontend`、`antfs-admin`、`antfs-devcluster`:

```bash
target=x86_64-unknown-linux-gnu    # 或 aarch64-unknown-linux-gnu / aarch64-apple-darwin / x86_64-apple-darwin
base=https://github.com/sunchao6106/antfs/releases/latest/download
curl -LO "$base/antfs-$target.tar.gz" && curl -LO "$base/SHA256SUMS"
sha256sum "antfs-$target.tar.gz"   # macOS 用 shasum -a 256,与 SHA256SUMS 对应行比对
tar xzf "antfs-$target.tar.gz" && cd "antfs-$target" && ./antfs-devcluster
```

### 用 Docker 跑

容器镜像发布在 GitHub Container Registry:每次推 `main` 更新 `latest`,打 `v*` tag 出语义化版本 tag。

```bash
docker run --rm -it \
  -p 7300:7300 -p 7400:7400 \
  -v antfs-data:/data \
  ghcr.io/sunchao6106/antfs:latest
```

起的还是那条「一键本地集群」(1 个 Raft master + 3 个 store + admin web + REST frontend)。启动日志会打印各端点与 admin token,宿主机上可直接访问:

- admin web 控制台 —— <http://127.0.0.1:7300>
- REST 接口 —— <http://127.0.0.1:7400/blobs>,Prometheus 指标在 `/metrics`

也可以用 Compose:

```bash
docker compose up -d
```

镜像以非 root 用户(UID 10001)运行,所以若改用挂载宿主机目录而不是命名卷,先把目录属主调好:

```bash
mkdir -p data && sudo chown -R 10001:10001 data
docker run --rm -p 7300:7300 -p 7400:7400 -v "$PWD/data:/data" ghcr.io/sunchao6106/antfs:latest
```

镜像里同时带了各独立组件(`antfs-master`、`antfs-store`、`antfs-frontend`、`antfs-admin`),可以只起其中一个:

```bash
docker run --rm --entrypoint antfs-master -p 7101:7101 ghcr.io/sunchao6106/antfs:latest
```

想在容器启动时顺手压一轮:

```bash
docker run --rm ghcr.io/sunchao6106/antfs:latest --run-bench router --bench-ops 50000
```

本地构建镜像:`docker build -t antfs:dev .`

### 可选:Linux 目标保真(colima VM)

生产目标是 Linux ext4。需在真实目标 OS 上做保真 / 大数据量 / 性能测试时,用 colima Linux VM:

```bash
./vm-cargo.sh build   # 自动转进 VM 跑 cargo(源码 virtiofs 挂载,target/数据落 VM 原生 ext4)
./vm-cargo.sh test
./vm-cargo.sh run -p antfs-smoke
```

## 文档

- 📐 [设计文档](docs/design/) —— 架构总览与各模块设计
- 📓 [开发日志 DEVLOG](docs/DEVLOG.md) —— 按实现步骤记录:做了什么、关键决策、验证证据
- 🗂 [OpenSpec 决策档案](openspec/changes/) —— 设计决策存档
- 🤝 [贡献指南](CONTRIBUTING.md) —— 如何构建、测试与提交改动

## 路线图

- [x] 单机 blob 引擎(log + 索引 + compaction + 恢复)
- [x] 内建 Raft 的 master(放置 / 调谐 / 下线 / blobId 号段)
- [x] store 数据节点(复制、心跳、磁盘监控)
- [x] router SDK + REST frontend
- [x] admin web 控制台
- [ ] 完成 Ambry 忠实翻译、锁定正确性(第一阶段收官)
- [ ] 性能 / 并发 / 存储密度优化(第二阶段)
- [ ] S3 网关、多租户 ACL 与配额

## 参与贡献

欢迎贡献!请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md),也欢迎直接提 [issue](https://github.com/sunchao6106/antfs/issues) 或 pull request。

## 许可证

antfs 基于 [Apache License 2.0](LICENSE) 开源。

## 致谢

- 架构设计参考 [LinkedIn Ambry](https://github.com/linkedin/ambry)、[SeaweedFS](https://github.com/seaweedfs/seaweedfs) 与 [CubeFS](https://github.com/cubefs/cubefs)。
- antfs logo 为项目自制 SVG —— 小小的蚂蚁,大大的梦想。
