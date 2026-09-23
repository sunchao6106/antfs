<p align="center">
  <img alt="antfs logo" src="docs/assets/logo.svg" width="440">
</p>

<p align="center">
  <strong>用 Rust 实现的分布式 blob 存储系统,为海量小文件而生。</strong>
</p>

<p align="center">
  <a href="https://github.com/sunchao6106/ambry-rs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/sunchao6106/ambry-rs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
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

欢迎贡献!请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md),也欢迎直接提 [issue](https://github.com/sunchao6106/ambry-rs/issues) 或 pull request。

## 许可证

antfs 基于 [Apache License 2.0](LICENSE) 开源。

## 致谢

- 架构设计参考 [LinkedIn Ambry](https://github.com/linkedin/ambry)、[SeaweedFS](https://github.com/seaweedfs/seaweedfs) 与 [CubeFS](https://github.com/cubefs/cubefs)。
- antfs logo 为项目自制 SVG —— 小小的蚂蚁,大大的梦想。
