# Proposal: store-composed-backfill —— 存量描述符回填,置信读可翻转

## Why

`blob-composed-flag`(步 67)给了新写描述符置信读;**无位存量面仍靠 MAGIC 嗅探**,
冻结两失真(blob-io-end-to-end tasks 已复现,本档设计文档 D5 明记"另立项"):

- ① 普通 blob 恰以 `ASTC` 开头 → 读回 **502**(数据没丢,网关拒服务);
- ② 恰好合法的描述符字节当普通文件上传 → **静默返回它引用的片内容**(confused deputy)。

根因消除条件 = 集群内全部描述符带位。本档补齐两件:**store 侧回填机制**(存量描述符落位)+
**frontend 置信翻转开关**(`composed_flag_only`,默认 false,回填验收后运维可翻)。

## What Changes

- **机制裁决(实码验证,非猜测)**:blob id 永久占用(含墓碑后,`AlreadyExists`)⇒
  回填**不能**重写/新写同 id;改为**引擎内置置换写** `BlobStore::mark_composed(key)`:
  `write_lock` 下重读当前赢家记录 → 追加**同 key 新记录**(flags |= FLAG_COMPOSED、
  原样字节与 expiration)→ 索引 newest-wins 替换周口(seg_bytes 旧段 live→dead、
  valid_stats 换值——逐字对齐 `apply_replicated` 的置换纪律)。三条已验机制背书:
  - 恢复重建 = 顺序扫 log + `index.add` upsert ⇒ 同 key 新者自然胜(崩溃一致性同 put);
  - 压实以 `find(key).offset == off` 选赢家 ⇒ 旧记录自然判死回收,新带位记录被搬运;
  - 并发删:两者同 `write_lock` ⇒ check-then-publish 原子,**无复活窗口**;复制 apply
    对"本地 PUT + 对端非墓碑"= Skip ⇒ 未回填副本各自回填收敛,无双向环路。
- **`antfs-descriptor` 新小 crate**(common 家族,零依赖纯字节):
  描述符格式单点(MAGIC/version/MAX_CHUNKS/MAX_BYTES/编解码)。frontend 的编解码换用
  (既有 stitch 用例零回归 = 字节级等价证明);store 回填以**同一 parse**做甄别,
  杜绝两处格式逻辑漂移(§3.4 格式即契约)。
- **store 端点 `POST /admin/backfill-composed`**(与 scrub-control 等 admin 面并列,
  D2 豁免闸):同步一次性本地扫(单飞闸防踩踏)—— 全副本 `for_each_entry_winner`
  串行过:尺寸 ≤ MAX、非删、非过牌、无位 → 读字节 → 严格 parse 成功 → `mark_composed`。
  报告 JSON(scanned/candidates/migrated/already/invalid/expired/failed)。
  **幂等可续跑由构造保证**(已有位 → already;中断即重扫,无单独进度状态)。
- **甄别口径 = 行为奇偶校验**(刻意):不校验 chunk 引用存在性——回填只把
  "parse 严格通过"的落位。后果:存量真描述符 → 搬到置信路径(行为同今);
  存量伪描述符(parse 过) → 带位 → 照旧拼装(行为同今,不劣化);ASTC 前缀
  非 descriptor → **不落位** → 翻转后读原始字节 = **失真①被修复**;② 的存量伪件保持
  现状(新写自带位,新②不再发生)。残余记录在 design。
- **frontend `composed_flag_only`(默认 false)**:翻开后无位行**不再嗅探**——GET 返回
  原始字节(小对象路径不变,只是不做 MAGIC 判定);HEAD 无位行免读数据体
  (D5"HEAD 数据体读节省"自然兑现)。**誓约语义**:只可在集群各 store 回填验收
  后翻开,否则存量真描述符会被当普通对象返回描述符本体(文档大字写明)。
  有位行的置信路径/超限 502 语义逐字不动。

## Impact

- `crates/common/descriptor`(新)+ workspace 成员行;
  `crates/blob/src/{blob.rs,index/mod.rs}`(mark_composed + 赢家迭代 + 若干单测);
  `crates/store/src/server.rs`(端点 + 单飞闸 + 报告)+ 测试;
  `crates/frontend/src/{config.rs,server.rs}`(旋钮 + codec 换共享)+ rest.rs 新负例;
- 默认行为逐字节等价(旋钮默认关;端点属显式运维动作);
- 文档:frontend.md(§8 配置行 + §4 读序/誓约)、store.md(端点语义/报告/移交清单)、
  DEVLOG 步 76;
- 严格不动:记录/索引字节布局(只启用既有保留位)、S3(manifest 在 MetaStore)、
  复制/压实/恢复语义(全部复用既有不变量)。
