# 蚂蚁文件系统 (antfs) 设计总览

用 Rust 重写**整个 Ambry**(不止存储引擎)。分阶段:**现阶段是忠实的 Rust 翻译**(对齐 Ambry 架构与语义,仅在等价前提下用现代实现),**后续在 Rust 版本之上做优化**。大原则:**整体照搬 Ambry 架构,局部替换为业界最新最佳实现**。

本目录按模块拆分设计文档,本文只放**跨模块的战略方向、系统层级与开发环境**;各模块细节见下方索引。

## 模块设计索引

| 模块 | 文档 | 状态 |
|---|---|---|
| common(公共库:`config` 分层配置 + `metric` 统计打点) | [common.md](./common.md) | 进行中 |
| store(单机存储引擎:Log / LogSegment / PersistentIndex / BlobStore;对标 Ambry `ambry-store`) | [store.md](./store.md) | 进行中 |
| cluster(集群/服务层:master / server / frontend 角色 + 传输选型;锁定架构目标) | [cluster.md](./cluster.md) | 选型已定,实现规划中 |
| frontend / S3 网关(对象接口层) | *(待添加)* | 规划中 |

> 决策档案(含被否决方案与理由)见 `openspec/changes/`。

## 战略方向(重要)

目标:一套**集群承载千亿(10¹¹)小文件**,每节点 36×16T(裸盘 576TB),访问模式「新数据热、老数据偶尔归档读」,高效首指性能(吞吐/延迟)同时要扩展性/可用性。

**曾反转一次**:初版采用 Seastar 式路线甲(`monoio` thread-per-core + io_uring + O_DIRECT + 自管块缓存),经评估放弃,退回更贴近 Ambry 原版的**路线乙**——**同步阻塞 IO + buffered(page cache)+ 共享锁并发 + mmap 索引**。完整决策见 `openspec/changes/io-index-design/`。核心结论:
- ambry-store 的 log 本就是**同步阻塞** `FileChannel`(非异步),路线乙是对齐原版而非退化。
- 「伪异步/盘 hang 泄漏线程」是**可用性/容错**层面问题,由集群层的磁盘健康检测 + 多副本兜底,不在引擎层硬换异步。
- 每 Log **定容上界 + 写满自动滚动新 partition** 是设计锚点(本轮暂缓实现)。

## 系统层级(与 Ambry 一致)

```
Partition(逻辑分区,句柄里编码,永不变)
  → Replica(某机某盘上的一份副本)
    → Store(一个 replica 的本地引擎)         ← 见 store.md
      → Log → LogSegment(数据)   +   PersistentIndex(索引)
        → Disk / DataNode(物理)
```

## 开发环境

工程**跨平台**(Linux / macOS,均 Unix;非 Unix 不支持)。本机 macOS 装 rustup 后可直接 `cargo build/test`——已无任何"非 Linux 不可"的硬依赖(段预分配按平台分派:Linux `posix_fallocate` / macOS `F_PREALLOCATE` / 回退 `set_len`)。

生产目标是 Linux ext4,故保留 **colima Linux VM** 作为**可选**的目标 OS 保真 / 大数据量 / 性能测试路径:`./vm-cargo.sh <cmd>` 转进 VM 跑 cargo(源码 virtiofs 挂载,target/数据落 VM 原生 ext4)。
