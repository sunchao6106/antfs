# s3-sse-key-rotation —— SSE 主钥轮换离线工具(解帧→重帧→换 blob,可续跑)

## Why

`s3-sse` 单主钥(`MasterKey` 全网关单把,s3.md「局限」原文:「单主钥(轮换 = 离线重加密
工具,后续方向)」)。**轮换操作面为空**:主钥疑似泄漏或合规周期到期时,没有任何官方
工具能把存量密文行换到新钥下——只能手工改库自伤。

- 阶段二优化路线档 5 第一行:「SSE 主钥轮换(现为离线重加密工具,**工具未建**)」;
- s3.md SSE 节「局限(透明记档)」记档的既定方向。

## What Changes

- **零 schema / 零磁盘格式变化**:扫描走既有 `named_objects.sse` 可空列(迁移 004);
  帧与描述符几何一字不动(`ANTFSSE1 | key_id | …` 自述帧,读侧零改动)。
- **metastore 新 API** `scan_sse_objects(after, limit)`(双方言):分页扫描
  `state=1 且 sse 非空` 行,`ORDER BY bucket, key` 排他游标 + LIMIT;直接返回整
  `ObjectMeta`(免逐行二次 `get_object`)。在线重写的行落在游标左侧,无幻读环。
- **新模块 `sse_rotation.rs`**:`rotate(st, old, new, batch)` 逐行轮换——
  描述符守卫(alg≠AES256 / manifest+SSE 乱序态 → **中止**;key_id≠旧钥 → **跳过保原**)→
  读旧帧 → 旧钥解明文 → 新钥重帧 → 落新 blob(既有 put 弧:blob 先落,孤儿由配额拒
  同款同请求墓碑兜底)→ `put_object` 全字段原样、blob_id 与 sse 描述符换新 →
  遮蔽墓碑 + WAL 消账(既有 reclaim 弧)。失败返回 `RotationFailure`(**已轮换计数 +
  定位行 + 原因**):重跑即续——已轮换行描述符已携新 key_id,天然归「跳过」。
- **CLI 子命令** `antfs-s3 --rotate-sse-key <new-key-file>`:照常载 config 建状态
  (不 spawn GC、不监听、跑完即退);前置校验(旧钥须已配 / 新钥可载 / key_id 不得
  同旧);完成打印 `scanned/rotated/skipped-other-key` 报告 + 换钥复产提示;中止
  退出码 1 且带进度定位。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-core`:新增(ADDED)「SSE 主钥轮换」要求(离线语义、可续跑、他代行不动、
  坏行响亮中止、事后工序)。
- `s3-metastore`:新增(ADDED)「SSE 行扫描」要求(分页契约、排他游标、state=1
  且 sse 非空过滤、双方言同构)。

## Impact

- 代码:`metastore/{mod,pg,mysql}.rs`(+1 trait 方法/两实现),`sse_rotation.rs`(新),
  `main.rs`(~20 行子命令分支),`tests/sse.rs`(+4 用例 + `poll_tombstoned` 抄
  object.rs 样式)。
- 契约:网关 HTTP 面零变化(轮换绕 handler,不走服务路径);router/engine 零改动
  (新帧 = 普通 blob 用既有写口);无新配置、无新监控项(CLI 报告 = stdout + 退出码,
  不进 Registry)。
- 持久性风险分析(§3.3 清单):只碰读+既有写弧——blob 先落/行后写不变、
  行写失败同请求墓碑(配额拒弧同款)、重跑幂等(可安全重试)、无新 fsync 边界。
  **并发窗口须离线**:轮换期间网关并发 PUT 同 key 会造成丢更新(读-改-写竞态)——
  工具起手即打印停机警告,s3.md 记档为硬前提。
- 测试:e2e 四件(换钥读回·含分页·幂等重跑/他代 key 行跳过·原样保 version/
  坏 alg 中止·带进度/manifest+SSE 乱序态中止)+ 墓碑收敛与 WAL 清空断言;双言
  全量零回归。
