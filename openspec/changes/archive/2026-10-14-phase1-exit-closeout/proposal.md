## Why

Phase 1 的定义是「忠实翻译 Ambry + 锁定正确性」,但它一直缺一份**出口判据**。本 change 用三方对表(antfs 实现面 @ `bdbfa64` ↔ 本地 Ambry `v0.5.195` 源码 ↔ cluster.md §2-§6 锁死偏差清单)产出分类结论,并把结论**落成档案 + 一批小收尾**:哪些 Ambry 特性确认砍掉(理由入档)、哪些是有意偏差永不补(防未来误判成"缺口"再搬回来)、哪些归后续阶段;同时补上唯一拍板要做的语义缺口(frontend 探活端点)。收官卫生的头一块(S3 独立二进制接线 + 文档去 stale)已先行落地并随 `v0.0.3` 发布,记入本档案成为已完成部分。

## What Changes

- **归档对表矩阵(A/B/C 三分类)**,作为 Phase 1 是否可收官的**常设判据文档**:
  - **B(有意偏差,永不补)**:Raft master 取代 Helix+ZK(D1)/ gRPC+HTTP 双面取代自研二进制协议(D2,明令禁止再发明)/ Binary Fuse 取代 Bloom / 每盘有界池 + D1-D6 盘故障抑制(antfs 自有增强)/ 复合对象切片放 frontend 层而非 router 层(frontend.md §10)/ S3 网关 + 外置 MetaStore 取代 Ambry named-blob / 现代基建栈(serde-config / prometheus / tracing)。
  - **C(已登记的后续,本收官不动)**:EC 冷层(D5)/ scheduler 独立角色(P1)/ 跨 DC 复制与 VCR 云备份 / S3 侧 versioning·SSE·quota·GC(s3.md「后续方向」)/ 真机阈值标定与故障注入验收(cluster.md §9.4-§9.5,按决策移交真机)。
  - **砍掉的 Ambry 特性(拍板入档)**:`undelete` 与 TTL 后置更新(对象不可变模型 + DEVLOG 步骤 19 简化删除决策的延续);`multipart/form-data` 浏览器表单直传(A6,过时场景)。理由与 Ambry 侧对应物一并记录,防止后续"补课"式返工。
- **新增 frontend `GET /healthCheck`**(A5 窄档):免鉴权、无副作用、恒 `200` + 文本 `GOOD`(对齐 Ambry rest 层同名端点语义);给 LB / K8s 探针 / 运维 curl 用,不再靠拉 `/metrics` 当心跳。
- **A1「对象元数据族」已裁决(2026-10-14,用户拍板):仅 S3 侧、基于 MetaStore,与底层 blob 引擎隔离**(三路线对比见 design.md;磁盘格式 v5 路线的 §3.4 红线因此**不触发**)。裁决后复核发现**该路线的实现主体已存在**(schema 列 + 双方言 SQL + put 采集/get·head 回吐 + multipart 传递全在),真实缺口是**零测试覆盖**(`crates/s3/tests/` 无任何 metadata 断言)与**边界行为缺失**(无 2 KiB 上限 → `MetadataTooLarge` 400 缺失;`content_type` 超 `VARCHAR(256)` → 500 而非 400)。收口工作另立 change `s3-object-metadata`(补证据 + 边界化,不重写实现);本 change 归档裁决与路线对比。
- **已完成(先行收口,随 v0.0.3 发布,入档备查)**:`antfs-s3` 独立二进制接线修复(此前 tarball 中为空壳)+ 四份设计文档六处去 stale + DEVLOG 步骤 39/40/41。
- **留观项入档**:`cluster_join` 15s 放置窗口偶发(环境性,DEVLOG 步骤 39),再触发按既有线索排查。

## Capabilities

### New Capabilities
- `frontend-liveness-endpoint`:frontend 的进程级探活端点(`GET /healthCheck`),免鉴权、无副作用、恒 200,供负载均衡与编排系统健康检查。

### Modified Capabilities
<!-- 无:frontend 此前没有 spec 能力;既有 s3-* specs 不涉改动。 -->

## Impact

- **crate**:`crates/frontend`(server.rs 增一条路由 + 集成测试)、`crates/devcluster`(启动 banner 增 healthCheck 端点一行,如既有输出格式保留)。
- **契约**:新增端点 `GET /healthCheck`(响应 `200 text/plain "GOOD"`);零新配置项、零新指标(探活不进业务统计)。
- **文档**:frontend.md 契约小节补行;DEVLOG 步骤 42;README features 的 REST API 一句话提及(可选,轻量)。
- **风险评估**:无(读路径独立于 router/master 健康——探活语义是"frontend 进程活着",不是"全集群健康",与 Ambry 同口径;文档写清防误读)。

## Non-goals

- **不做 A1 对象元数据族实现**(决策点;选定路线后另立 change)。
- **不照搬 Ambry 多租户 account/container 模型**(A4):租户与所有权归网关层(S3 MetaStore 已承担),blobId 布局 D7 冻结不动。
- **不做 hard delete 物理擦除**(A3):合规级特性,归 Phase 2。
- **不做 **`/statsReport`/`/clusterMapSnapshot` 等观测端点**(A5 宽档):admin web 已有同等观测面,REST 侧不重复建设;如将来需要 K8s readiness 语义再加。
- **不动 C 类**任何已登记项;不开 EC、不引 scheduler。
