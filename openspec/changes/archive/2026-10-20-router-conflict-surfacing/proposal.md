# Proposal: router-conflict-surfacing — put 仲裁失败中的确定性 409 透传

## Why

显式 key 的 `Router::put*`(及 stitch 描述符写口)在命中已存在(或已删除墓碑)的 key 时,store 数据面回 409(RestApiConflict),router 错误归一层也早已把它归一成 `Error::AlreadyExists`;但仲裁环的失败终判把**一切非 StoreFull 失败统一折叠**进 `Error::Quorum`,网关层将 Quorum 一律映射 503。结果:唯一一类"调用方确定性不可重试"的写失败被伪装成"可以重试的暂时性失败"——上层(未来任何 keyed-put 调用者、观测面、重试策略)拿不到冲突信号,409 信号在中间层被吞。

`docs/design/frontend.md` §11 与 `docs/design/router.md` 错误表(`already_exists` 行)长期挂着这条待办;本 change 按调研结论(缺口仅在仲裁环终判一处)以最小手术落地。

## What Changes

| 位置 | 改动 |
|---|---|
| `crates/common/router/src/lib.rs` | 新增自由函数 `all_already_exists`(与 `all_store_full` 同镜像结构、同"全部失败"口径);在 `put_keyed` / `put_auto_keyed` / `put_file*(环体)` 三处 quorum 失败终判中、StoreFull 特判之后追加:全部失败为 `AlreadyExists` → 直接返回 `Error::AlreadyExists` |
| `crates/common/router/src/lib.rs`(单测) | `all_already_exists` 判定矩阵(全 409 / 混合失败 / 1 成功 + 其余 409),镜像既有 `all_store_full` 用例 |
| `crates/common/router/tests/e2e.rs` | 真 store 集成用例:同 key 二次 keyed put → `Error::AlreadyExists`(天然 409 复现面:第一次 W=2/3 成功后,第二次两副本 409 + 一副本得 ok ⇒ 多数派未达且失败全为 409);**顺带补课**:该文件是上一批零竞态迁移的漏网文件(三层深路径不在 glob 内),`free_port` 探测模式同批退役 |
| `crates/frontend/src/server.rs` | `router_status` 增 `AlreadyExists → 409 CONFLICT`(与 store 数据面同码) |
| 文档 | `frontend.md` §11 勾销条目 + §2 错误表 POST 行增 409;`router.md` 错误表 `already_exists` 行补透传语义;DEVLOG 步 80 |

## Impact

- **不改**:quorum 算法本身(409 **不**计为 ack——"key 已被占"不等于"本次数据已落",W/R 语义不动);`Error::AlreadyExists` 枚举与 `kind()` 标签原样复用;S3 网关不加映射(S3 覆盖写合法,metastore 主导重复面,keyed-put 冲突在 S3 语义无对应错误码,维持 InternalError 兜底)。
- **改**:REST 契约面 — keyed 写全失败且皆为 409 的场景由 503 变 409(可重试 → 不可重试,语义修正);现网路径无一抵达(REST/S3 皆走 put_auto 新号),风险面为零。
- 无新配置、无新监控(`put_replica_errors{kind=already_exists}` 已有)、无磁盘格式面。

## Out of Scope

- `put` 409 计入多数派(语义危险,key 上的旧数据 ≠ 本次写入)。
- S3 网关 409 映射(S3 无此错误族)。
- delete/Get 面 409 透传(删除已幂等 204;读面无冲突概念)。
