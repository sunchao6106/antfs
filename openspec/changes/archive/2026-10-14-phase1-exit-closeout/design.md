## Context

Phase 1 的章程是「忠实翻译 Ambry + 锁定正确性」,但没有出口判据文档。三方对表(antfs 实现面 @ `bdbfa64` ↔ 本地 Ambry `v0.5.195` 源码逐模块 ↔ cluster.md §2-§6 锁死偏差清单)已完成,分类矩阵(有意偏差 B / 已登记后续 C / 语义差距 A)见 proposal.md。对表过程带出的 P0(antfs-s3 二进制空壳)已先行修复并随 `v0.0.3` 发布,六处文档失真同时去 stale(DEVLOG 步骤 41)。本 change 只剩两类实施物:`frontend` 探活端点(A5 窄档,已拍板)+ A1 裁决归档(A1/A5/A6 中 A6 已砍、A1 已裁为乙路线、实施另立 change)。

## Goals / Non-Goals

**Goals:**
- Phase 1 收官判据落档:矩阵成为常设参考(不再散落在对话/日志里)。
- 补 `frontend GET /healthCheck`(A5 窄档):LB / K8s / 运维 curl 的标准探活姿势。
- A1「对象元数据族」三路线的技术对比与迁移代价入档,用户选定后另立 change 实施。

**Non-Goals:**
- A1 的任何实现(已裁决乙路线;实施在独立 change `s3-object-metadata`)。
- A3 hard-delete(Phase 2)、A4 Ambry account/container(路线裁决,见 Decisions 4)。
- A5 宽档(`/statsReport`、`/clusterMapSnapshot` 等;admin web 已有同等观测面)。
- C 类任何项(EC / scheduler / 跨 DC / S3 quota·SSE·versioning / 真机标定)。

## Decisions

### 1. healthCheck 形态:进程级探活,不是集群健康

- **契约**:`GET /healthCheck` → 恒 `200` + `text/plain` 正文 `GOOD`;无鉴权、无副作用、不依赖 router/master/store 连通性。
- **理由**:对齐 Ambry rest 层同名端点口径(其 `healthCheck` 同样只回 `GOOD`,代表 frontend 进程活着,不代表集群健康)。K8s `livenessProbe` / LB 健康检查要求的正是这一层;把集群纠葛掺进探活会在后端抖动时造成**探针雪崩重启**(集群越病越被重启,方向反了)。
- **替代方案(否决)**:①复用 `/metrics` 当心跳 —— payload 重、要抓取管线、语义混;②readiness 语义(检查 master 连通)—— 是另一种探针的目标,K8s readiness 将来需要再立独立端点,不与 liveness 混码。
- **指标**:该端点**不进**任何业务 counter/histogram(探活QPS 会稀释真实业务速率;devcluster/本地也无观测需求)。

### 2. A1 对象元数据族:三路线对比(决策记录,非实现)

背景:Ambry 每条 blob 记录自带 BlobProperties(content-type / owner / service-id / **用户自定义 KV**),GET/HEAD 原样吐回;antfs 的**引擎层**对象只有「字节 + 大小 + TTL」(正确事实,record 无元数据字段)。**但 S3 网关层的乙路线(元数据放 MetaStore)在裁决复核时发现已实现**:`named_objects`/`multipart_uploads` 已有 `content_type`/`user_meta` 列,put 侧已采集 `Content-Type`/`x-amz-meta-*`,get/head 已回吐,multipart create→complete 已传递 —— 与 Ambry 的差异是**位置**(Ambry 放 record,antfs 放 MetaStore),功能面对齐 S3 语义。残留缺口:测试零覆盖、2 KiB 上限/`MetadataTooLarge` 400、`content_type` 长度边界(列宽 256,超限现会 500)、S3 语义的 `binary/octet-stream` 存储默认。

| 路线 | 内容 | 优点 | 代价 / 风险 |
|---|---|---|---|
| **甲:磁盘 record 格式演进 v5** | record 加 `content_type` + `user_meta` KV(可选 owner/service-id),S3 与 REST 双面全量语义,blob 自描述 | 真·Ambry BlobProperties 翻译;与 D3「id 自带定位」同精神;两网关共享同一真源 | 触发 CLAUDE.md §3.4 磁盘格式红线:版本字段 + **前向/后向兼容测试**(新写旧读/旧写新读)、升级顺序「先全员可读后写」;record 头变长,小对象密度略降;一次性大 PR |
| **乙:仅 MetaStore 存** | `named_objects` 加 `content_type`/`meta` 列;S3 GetObject 从 MetaStore 取,put 时存 | 不动磁盘格式;工作量小;S3 语义几周内可用 | REST 直连层完全不可见;engine 不自描述;S3 与(将来的)REST 元数据真源割裂,乙→甲 迁移时要做数据搬家 |
| **丙:不做** | 声明 antfs blob 层元数据无关 | 零成本 | S3 语义残缺(GetObject 无 Content-Type);客户端靠嗅探;每多一天用户面越广,补课越贵 |

**裁决(2026-10-14,用户拍板):乙** —— 只做 S3、基于 MetaStore、**与底层 blob 引擎隔离**;REST/frontend 与引擎零改动,磁盘格式 §3.4 红线不触发。**实现主体已存在(见上述背景复核)**,收口 change 为 `s3-object-metadata`:补测试证据、`MetadataTooLarge`/长度边界 400、`binary/octet-stream` 存储默认、s3.md 行为文档 —— 不新增特性面。将来若升甲:乙→甲 需 MetaStore 列内容回填进 record(S3 读侧切换真源),列保留为过渡期兼容 —— 列为该方向的已知代价。

### 3. 砍掉特性归档(undelete / TTL 后置更新 / form 直传)

- `undelete`、`x-ambry-ttl` 后置更新:与「对象不可变、删除不可逆」模型相抵(DEVLOG 步骤 19 简化删除模型的既有决策延续);TTL 写入时指定的语义已完备(过期即删除,GET/HEAD 表现一致)。Ambry 侧对应物(Undelete/TtlUpdate record + TtlUpdateHandler 等)记入 B 类备注:将来若做对象生命周期管理,按**新特性提案**走,不按「翻译缺课」补。
- `multipart/form-data` 浏览器表单直传(A6):现代客户端均走 raw body/S3;S3 网关已覆盖 web 直传场景。砍。

### 4. A4 多租户:裁决为「有意偏差」

Ambry 的 account/container + ACL + router 鉴权模型**不照搬**。租户/所有权/凭证归网关层:S3 网关 MetaStore(buckets ownership + s3_credentials)已承担;cluster.md §1 的既有裁定「frontend 是将来 ACL/配额/多租户的唯一落点(blobId 布局 D7 冻结,租户信息只能挂网关层)」正是同一结论。归 B 类,永不补。

## Risks / Trade-offs

- **[healthCheck 被误读为集群健康]** → 契约文档与 spec 场景明写「后端全挂仍 200」;将来 readiness 需求另立端点,不在此扩语义。
- **[A1 悬置期 S3 语义残缺]** → 决策点列 proposal 首屏;乙路线可随时以小 PR 补,不会烂尾。
- **[矩阵时点性]** → 本档案对表基线 = `bdbfa64` + Ambry `v0.5.195`;Phase 2 入口重审一次,避免 stale 判据误导后续阶段。

## Migration Plan

- `healthCheck`:纯增量路由,无迁移;回滚 = 删路由。
- A1(若选甲):disk 格式演进按 CLAUDE.md §3.4 升级顺序(先让全体节点具备读 v5 的能力,再开写 v5);compat 测试矩阵(旧盘新读 / 新盘旧读)是该 change 的必要交付物;此处只记原则,实施 change 再细化。

## Open Questions

<!-- 无:唯一未闭包项 A1 已于 2026-10-14 裁定为乙路线(仅 S3、MetaStore、与 blob 隔离),实施见 change `s3-object-metadata`。 -->
