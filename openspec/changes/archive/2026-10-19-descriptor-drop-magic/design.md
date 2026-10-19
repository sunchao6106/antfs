# Design — descriptor-drop-magic

## Context(现役职责盘点,实码)

- `MAGIC` 出现点:`antfs-descriptor`(常量+encode 首写+parse 首检)、frontend
  `download` 嗅探(`!composed_flag_only && body.starts_with(MAGIC)`)、frontend `HEAD`
  嗅探(`meta.composed || body.starts_with(MAGIC)`)、store 回填 sweep 漏斗
  (`data.starts_with(MAGIC)`)。三张引用图互不重叠地各自为政,正是"路由(嗅探)/
  甄别(回填)/校验(parse)"三职责。
- 无关血缘(刻意不动):log 记录头 `REC_MAGIC`(u16 记录帧)、S3 SSE 帧格局——
  与描述符格式毫无关系,盘点先行避免误伤。

## Decisions

### D1:单一格式,无 MAGIC("从未存在过"态)

`version(1,恒 1) | chunk_count(u32, 1..=4096) | session_len(u16) | session |
× chunk_count: id(8) | size(8) | expiration_ms(8)`(头 **7 B**,条目 24 B 不变;
`MAX_BYTES` 兜底与严格"flavor 校验"不变——尾随字节、计数界、定长、越界全保留)。
`BadMagic` 错误型退役;"恰好过时的答案"行为:旧前缀字节版本位读到 0x41(65)→
`UnsupportedVersion(65)` **响亮失败**,不静默、不误拼。

### D2:parse 的输入域收敛为"位字节"(可行性核心)

去掉 MAGIC 后判别面弱了 4 字节熵——这个弱点**只有在 parse 面对不可信字节时才成立**。
因此本 change 的硬性不变量:**全部"对无位字节判型"的路径一并删除**:

- 嗅探(download/HEAD)无位分支 → 删(位独占路由:有位 → 拼装;无位 → 普通对象);
- 回填 sweep → 整建制删(漏斗、端点、报告、引擎置换 API 与迭代 API、全部测试、
  store 对 antfs-descriptor 的依赖);

剩唯一 parse 调用域 = **置位行**(置位者只有 frontend 组合写口)。于此,version/计数/
定长/尾随的校验强度是纯冗余防御(自写自读),4 字节熵的缺失不构成任何业务风险。
有毒/冲突输入面 = **不存在**。

### D3:frontend 行为静态 flag-only(旋钮退役)

`composed_flag_only` 的存在前提是"有嗅探可关";嗅探没了,旋钮失去语义 → 删除
(config 行、AppState 字段、validate、测试、誓约文档)。行为:

- GET:位 → 置信拼装(超限 502、坏描述符 502,语义逐字不动);无位 → 小对象整体
  读返回原字节 / 大对象暂存流式(与既有非描述符路径合流);
- HEAD:位 → 读回小描述符换算逻辑大小 + stitched 头;无位 → 不读数据体、stat 原值。

此前冻结的两失真(ASTC 前缀 502 / 伪描述符静默展开)**随嗅探消失而消失**——
不是被修,是疟疾蚊子被抽干。旋钮关时的 502 钉住用例改写为"任意前缀普通件 200 原字节"。

### D4:spec 增量.MODIFIED 与 .REMOVED)

- `blob-composed-flag` 第 4 条(`Frontend trusts the flag first and keeps legacy sniff
  fallback`)MODIFIED:flag-first **且 flag-only**,嗅探句删除;对应场景改写
  (无位行原字节,置位行权威拼装)。
- `store-composed-backfill` 三条 Requirement REMOVED(引擎置换/端点/旋钮旋钮誓约
  ——能力撤销,归档后 openspec/specs 下该目录随 delta 清空)。

### D5:兼容性承载 = Owner Rulings(见 proposal)

§3.4 两半各自被所有者裁决置空:前向(旧二进制)无部署;后向(旧盘数据)无存量要保;
残留个人盘上的旧前缀描述符如遇 → 响亮 502。**本条即兼容性测试义务的替代物**:
新增用例覆盖"旧前缀字节 → 502 不误拼"(以 0x41 版本位报 UnsupportedVersion)。

### D6:文档口径

living 文档(frontend.md/store.md/design README/crate 注释)清零 ASTC 论述、按终态
改写;DEVLOG 与 openspec 历版归档保留轨迹(改记不改史),步 78 记录本轮裁决链
(D7 复议 → 所有者终裁)。

## Risks / Trade-offs

- **若现实出现违反r:2 的盘**(部署 fleet / 个人旧盘要求可读的存量前缀描述符):该批
  会以 502 暴露它们——**响亮失败、可由 owner 推翻重启**(重新引入 = 恢复 D7 复议里
  备好的双 parse 判别式方案,通道仍开)。
- 格式熵降低的唯一暴露面已随 D2 收敛为零;随机字节偶然撞上"合法无魔格式"的概率仍
  为 version×计数×定长×尾随四道闸(仅对自写位字节作冗余防御,实际不参与任何判定)。
- 删除量大于新增量(移交面净缩);`compose 相关 e2e`(stitch/auto-chunk)全量回归
  守护格式重写的字节级正确性。

## 测试策略

- descriptor:roundtrip(无前缀)/版本 65(旧前缀字节的如实报错)/截断矩阵/尾随字节/
  计数上下界/尺寸兜底;
- frontend rest:①任意前缀普通件 200 原字节(原 502 钉住用例改写)/ ②**无伪**旧前缀
  描述符概念已灭 → "位判断即一切"用例:无位伪描述符字节(合法无魔格式)→ 200 原字节;
  ③正常 stitch(置位)拼装照旧 / ④HEAD:置位逻辑大小、无位原始大小;
- blob/store:回填族用例随特性删除;`composed_flag_survives_reopen_and_rebuild`
  (位置写路径)保留全绿;旧前缀字节遇新 parse → UnsupportedVersion 用例进 descriptor;
- 全量:workspace + clippy + fmt + devcluster(stitch 闭环真面)。

## 附录:撤销记录(store-composed-backfill 能力整撤,2026-10-19)

## REMOVED Requirements

> 归档说明:该能力全部条目随 `descriptor-drop-magic` 撤销;`openspec` CLI 不支持
> "REMOVED 后剩余 0 条"的空能力,故归档时相应能力目录(`openspec/specs/
> store-composed-backfill/`)由实现侧一并移除 —— 此 delta 即撤销记录本身。

### Requirement: Engine-level composed marking by record replacement
The blob engine SHALL expose `mark_composed(key)`, which under the existing write lock re-appends the current live winner record for that key with `FLAG_COMPOSED` set (identical bytes, size, and expiration) and republishes the index entry newest-wins, mirroring the existing replacement discipline of `apply_replicated` (old segment live→dead, live-stats swap). It SHALL NOT modify bytes in place, SHALL NOT resurrect deleted keys (`Deleted` outcome), SHALL be idempotent (`Already` when the bit is present), and the marking SHALL survive both index reopen and full log-based index rebuild. The engine SHALL also expose a winner-only index iteration (`for_each_entry_winner`) that yields only entries whose `find(key).offset` equals the entry's offset (the same winner criterion compaction uses).

#### Scenario: Marking survives crash-recovery paths
#### Scenario: Delete after marking stays deleted

### Requirement: Store admin backfill endpoint
The store server SHALL expose `POST /admin/backfill-composed` (admin route family, never gated by the serviceability rule): a single-flight synchronous sweep over all local replicas that, for each current winner entry that is not deleted, not expired, carries no composed bit, and is within the descriptor size bound, reads the record bytes and — only if the bytes strictly parse as a stitch descriptor via the shared descriptor codec — marks it composed. The response SHALL carry a report of scanned/candidates/migrated/already/deleted/invalid/expired/failed counts. Concurrent invocations SHALL be rejected as busy; re-running SHALL be idempotent by construction (backfilled keys surface as `already`). Ordinary blobs whose bytes merely start with the descriptor magic but fail strict parsing SHALL NOT be marked.

#### Scenario: Legacy descriptor becomes trusted, magic-collision stays plain

#### Scenario: No resurrection of previously deleted keys

### Requirement: Frontend flag-only read policy (operator covenant, default off)
The frontend SHALL gain config `frontend.composed_flag_only` (default `false`). When enabled, blobs without the composed bit SHALL be served as-is (GET returns original bytes without consulting the descriptor magic; HEAD skips the descriptor-body read and reports the raw size), while blobs with the bit SHALL keep the existing authoritative assembly semantics unchanged (including the loud 502 on over-limit or corrupt descriptors). The docs SHALL state the covenant in terms the operator cannot miss: the switch may only be enabled after the backfill has run and been verified on every store, otherwise pre-existing legacy descriptors would be served as their raw descriptor bytes.

#### Scenario: Magic-collision distortion is repaired under flag-only

#### Scenario: Bit-set stitch keeps authoritative assembly under both policies
