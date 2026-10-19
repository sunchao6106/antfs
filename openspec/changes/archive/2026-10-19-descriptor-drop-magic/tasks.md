# Tasks — descriptor-drop-magic

## A. 格式(descriptor crate)

- [x] `antfs-descriptor`:删 `MAGIC`/`BadMagic`;encode/parse 改 `version|chunk_count|
      session_len|session|entries`(头 7B);VERSION 恒 1;
      单测重写:roundtrip / **旧前缀字节 → UnsupportedVersion(65) 响亮失败** / 截断矩阵 /
      尾随 / 计数界 / 尺寸兜底不变
- [x] 公共面清洁:crate 文档不再提前缀概念

## B. frontend

- [x] `server.rs`:download/HEAD 嗅探分支删除(无位 ⇒ 普通对象路径);`DESCRIPTOR_MAGIC`
      删除;HEAD 只在 `meta.composed` 时读回换算;
      `composed_flag_only` 旋钮全链删除(config 字段 + AppState + from_config + 誓约注释)
- [x] `rest.rs` 重写:任意前缀普通件 200 原字节(原 502 钉住用例反向收口)/
      无位合法无魔描述符字节 → 200 原字节(纯位判定的正向翻转)/ 复合用例删
      (回填弧 + 旋钮弧随特性退役);既有 stitch 用例零改零绿 = 格式字节级回归
- [x] `config.rs`:删字段与文档行;validate 不涉新面

## C. store + blob(回填子系统删除)

- [x] store:`/admin/backfill-composed` 路由/handler/sweep/报告/单飞闸删除;
      `tests/backfill_composed.rs` 删除;Cargo 撤 antfs-descriptor 依赖
- [x] blob:`mark_composed` / `MarkComposedOutcome` / `for_each_entry_winner` /
      `PersistentIndex::for_each_entry` + lib.rs 再导出 + 有关单测删除
      (`put_composed_*` 与 `composed_flag_survives_*` 保留)

## D. 文档

- [x] frontend.md:§4 读序段重写(flag-first 且 flag-only,收口两失真叙述)/ §8 删
      `composed_flag_only` 行 / §11 待办条目收口为终态
- [x] store.md:§12.14 整节删除;§11/数据面提及的 `?composed=true` 保持
- [x] docs/design/README.md 路线档行:终态改写(无回填/无旋钮字样)
- [x] DEVLOG 步 78(裁决链:D7 复议 → 所有者终裁 → 本批落地与删除账)

## E. spec 增量与门禁

- [x] specs delta:`blob-composed-flag` 第 4 条 MODIFIED(flag-only 化);
      `store-composed-backfill` 3 Requirement REMOVED;validate 过
- [x] 门禁:workspace 全绿 + clippy 0 + fmt 净 + devcluster stitch 闭环真面 +
      archive(或 REMOVED 应用后 specs 清帐)+ 单 commit push + handoff

## 落地记录(已回填)

- descriptor **6/6**(roundtrip / 遗留前缀字节→UnsupportedVersion 响亮失败 / 计数界 /
  尺寸兜底 / 截断×尾随×结构矩阵);blob **95/95**;store **147/147**;frontend lib 11 +
  rest **18/18**(任意前缀 200 原字节三前缀循环钉住 / 无位结构字节原字节返回 /
  有位照常拼装 / HEAD 语义);**workspace 77 套 765/0**;clippy 0;fmt 净;
  devcluster 真面:历史前缀字节普通件 200 原字节(无探测)、stitch 201 拼装恒等。
- 删除账:blob 回填 API 族(mark_composed/for_each_entry_winner/index for_each + 4 测试)
  124 行;store 回填子系统(sweep/enum/报告/闸)~257 行;store tests/backfill_composed.rs
  整文件;frontend 旋钮 + 嗅探两分支;STITCH 量级净缩(新增 < 删除)。
