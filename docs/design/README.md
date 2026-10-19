# 蚂蚁文件系统 (antfs) 设计总览

用 Rust 重写**整个 Ambry**(不止存储引擎)。分阶段:**阶段一「忠实的 Rust 翻译」已于 v0.0.4 收官**(对齐 Ambry 架构与语义,仅在等价前提下用现代实现;取舍裁决见 `openspec/changes/`),**现阶段(阶段二)在 Rust 版本之上做优化**。大原则:**整体照搬 Ambry 架构,局部替换为业界最新最佳实现**。

本目录按模块拆分设计文档,本文只放**跨模块的战略方向、系统层级与开发环境**;各模块细节见下方索引。

## 模块设计索引

| 模块 | 文档 | 状态 |
|---|---|---|
| common(公共库:`config` 分层配置 + `metric` 统计打点) | [common.md](./common.md) | 已实现 |
| store(单机存储引擎:Log / LogSegment / PersistentIndex / BlobStore;对标 Ambry `ambry-store`) | [store.md](./store.md) | 已实现 |
| cluster(集群/服务层:master / store server / router SDK / frontend 角色 + 传输选型;锁定架构目标) | [cluster.md](./cluster.md) | 已实现 |
| router(客户端 SDK / 通用组件:`crates/common/router`,package `antfs-router`;号段缓存、W 多数派写、R=1 读;对标 Ambry `NonBlockingRouter`) | [router.md](./router.md) | 已实现 |
| frontend(HTTP REST 对象接口层;内嵌 router;对标 Ambry `ambry-frontend`) | [frontend.md](./frontend.md) | 已实现 |
| replication(拉复制设计:墓碑恒胜合并 + 游标重校验 + 段身份 + 副本 incarnation) | [replication.md](./replication.md) | 已实现 |
| S3 网关(S3 协议翻译 + SigV4 鉴权 + MetaStore 名字映射;外接 PG/MySQL,网关无状态) | [s3.md](./s3.md) | 已实现 |
| S3 网关增强(多租户 ACL + canned ACL、桶级配额、multipart 生命周期原子化、虚拟主机寻址、CopyObject/Attributes、aws-chunked 流式分帧、SSE、Range 读与流式装配) | [s3.md](./s3.md) | 已实现 |

> 决策档案(含被否决方案与理由)见 `openspec/changes/`。

## 战略方向(重要)

目标:一套**集群承载千亿(10¹¹)小文件**,每节点 36×16T(裸盘 576TB),访问模式「新数据热、老数据偶尔归档读」,高效首指性能(吞吐/延迟)同时要扩展性/可用性。

**曾反转一次**:初版采用 Seastar 式路线甲(`monoio` thread-per-core + io_uring + O_DIRECT + 自管块缓存),经评估放弃,退回更贴近 Ambry 原版的**路线乙**——**同步阻塞 IO + buffered(page cache)+ 共享锁并发 + mmap 索引**。完整决策见 `openspec/changes/io-index-design/`。核心结论:
- ambry-store 的 log 本就是**同步阻塞** `FileChannel`(非异步),路线乙是对齐原版而非退化。
- 「伪异步/盘 hang 泄漏线程」是**可用性/容错**层面问题,由集群层的磁盘健康检测 + 多副本兜底,不在引擎层硬换异步。
- 每 Log **定容上界 + 写满自动滚动新 partition** 是设计锚点(本轮暂缓实现)。

## 阶段二优化路线(2026-10-16 快照:GetObject 硬化收官后)

> 每项带证据源(档/日志/审计),防路线自己烂掉;**B2 EC 为用户明令暂缓项,重开须显式下令**;
> versioning 计费 undelete 外部凭证源为永久拒绝族,永不在此列。排序 = 建议优先级。

| # | 方向 | 内容 | 证据源 |
|---|---|---|---|
| 1 | S3 协议面补齐(小步快) | ~~条件 GET(`If-Match`/`If-None-Match`/`If-*` 族)+ `If-Range`;`?partNumber` 单片查询;ListObjectsV1 / GetBucketLocation 兼容形态(旧 SDK/迁移工具面)~~ ✅ 完成(`s3-read-compat`,2026-10-16:s3.md「条件读与读兼容面」节) | S3 审计 P1 菜单(`s3.md` 后续方向) |
| 2 | flexible checksums | ~~CRC32/CRC32C/SHA 家族:PUT `x-amz-checksum-*` 验证 + aws-chunked trailer 已解析未验证的校验值承接(GET `?attributes` 的 `Checksum` 501 面随之开)~~ ✅ 完成(`s3-checksums`,2026-10-16:s3.md「灵活校验」节;multipart complete 级复合校验 ✅ 2026-10-19 收口(`s3-multipart-complete-checksum`:composite 推导/FULL_OBJECT 全验/per-part 持久化)) | `s3.md` 后续方向、`2026-10-16-s3-chunked-payload` 明示不验证面 |
| 3 | 整型正确性补强 | ~~multipart 并发重传原子化~~ ✅ 已由 `s3-multipart-lifecycle`(commit fe2fc82)提前落地(retraction 留观行曾过时,本行更正);~~GC 死点崩溃窗兜底~~ ✅ 完成(`s3-reclaim-wal`,2026-10-16:s3.md「回收 WAL」节);~~frontend 描述符 composed 位置换嗅探~~ ✅ 完结(`blob-composed-flag` 2026-10-16:`FLAG_COMPOSED` 类型位 + 组合写口;`descriptor-drop-magic` 2026-10-19:位成为唯一类型判定,内容探测与回填旋钮全部退役,见 frontend.md §4/§11) | retraction 档「真留观」、`blob-io-end-to-end` 待办首条、DEVLOG 54 留观 |
| 4 | 性能主轴(Phase 2 正题) | 真机复验三件(4 GiB 滚转 / frontend 折损形状 / 掉电冷读);frontend→router ~200µs 固定税工程减税(可选,多核真机外推折损 12-23%);flush 有界脏窗口 P2(APFS 限流耦合);S3 multipart GET 串行取片吞吐(每片一 RTT,观察 `s3_request_duration_seconds{op}`,瓶颈显现再预取) | `benchmark/BASELINE.md` 真机复验段、DEVLOG 46/49、`s3-range-streaming` 记档 |
| 5 | 可运维性 | ✅ SSE 主钥轮换(2026-10-16:`--rotate-sse-key` 离线工具,s3.md「主钥轮换」节);✅ 桶策略 JSON 数据面授权(附于本批 S3 完结,用户 2026-10-19 指令;`s3-bucket-policy`:见 s3.md「桶策略」节);每请求 credentials/head_bucket 双查缓存(亚毫秒池化,升吞吐前不动);阈值标定 12 数真机落地(§9.4 流程已落,数据未采) | `s3.md` SSE 局限、DEVLOG 53 留观、`cluster.md` §9.4 |
| 6 | 密度主轴(档位项) | B2 EC 冷层(D5,P1-A 路径已解锁:master 下发 + 段 owner 后台 worker + 校验分片走数据面)——**用户裁定暂缓** | `s3.md` 后续方向、cluster.md D5 |

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
