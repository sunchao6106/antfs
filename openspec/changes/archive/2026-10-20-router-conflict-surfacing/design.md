# Design: router-conflict-surfacing

## Context

`put_keyed`(显式 key)与 `put_auto_keyed`/`put_file` 环(auto 发号)的仲裁流程一致:全副本并行 `put_body` → `collect_majority` → 失败终判。既有终判只区分两态:达到多数派 → Ok;全失败为 507 → `StoreFull`(auto 路径触发换分区重试);**其余一切** → `Error::Quorum`。

store 对"撞已存在 key 或撞墓碑"回 409(`server.rs`: `Deleted | AlreadyExists → CONFLICT`);router `classify_err_status` 把 409 归一为 `Error::AlreadyExists`——错误**归一**已存在,缺的是**终判出口**。

## D1 契约:失败终判的"全 409"特判(与 all_store_full 同构)

新自由函数:

```rust
fn all_already_exists<T>(f: &Fanout<T>) -> Option<String>
```

- 判定口径与 `all_store_full` 逐字对齐:**已收集失败非空且全部为 `Error::AlreadyExists`** 时返回第一个失败副本地址;
- 严谨性依据同源:`collect_majority` 在多数派**未达**时必然等全部 future settle(否则不返回),故 `failures + ok = total`,`all` = "除 ok 外全部副本都报 409";多数派已达的分支在函数之前已短路返回 Ok,不存在"提前返回吞视野"的歧义;
- 部分成功语义设计(镜像 507 用例):若有 1 副本成功、其余全 409(如 W=2 得 1 ok + 2 409 → 1 < 2 未达)仍判"全 409 成立"——因为判定只在 `reached()==false` 之后调用,该场景就是确定性冲突(数据已在多数位)。

## D2 三处插入点(环体统一,不引入行为分叉)

- `put_keyed`:`StoreFull` 判定后、`Err(Quorum)` 前插 `if let Some(addr) = all_already_exists(&f) { return Err(Error::AlreadyExists { addr }) }`;
- `put_auto_keyed` / `put_file` 重试环:同位置插入,**且不做 507 式换 key 重试**——auto 发的号撞旧号即号被复用(error.rs `AlreadyExists` 变体注释钉为"严重问题,必须查"),换号重试会把严重信号吞进循环;透传让上层尖叫才是正解。
- 409 **不计入多数派 ack**:冲突 ≠ 本次数据落位。W/R 语义零改动。

## D3 网关映射(frontend 一层)

`router_status` 增 arm `AlreadyExists → 409 CONFLICT`,与 store 数据面同码(对齐"副本状态码按语义透传"的既有契约段);S3 网关不加映射(S3 覆盖写合法、无此错误族,见 proposal Impact)。

## D4 测试证据链

1. 判定矩阵单测(`lib.rs` 内联,镜像 `all_store_full_detects_partition_full`):全 409 → Some(addr);混合(409 + silenced)→ None;无失败 → None;1 ok + 1 409(W 未达)→ Some。
2. 真 store e2e(`common/router/tests/e2e.rs`):RF=2 三副本,keyed `put_with_ttl(key, …)` 第一次成功 → 第二次同 key:两副本已有 → 409,第三副本重写成功得 ok(1 < 2 多数派未达)→ 断言 `Error::AlreadyExists`。混合失败(409 + 网络错)场景由判定矩阵单测覆盖(真网络故障注入超出最小面)。
3. REST 面:不加前端集成用例(现网 REST 无 keyed 路径抵达;映射 arm 为纯查表,系 `router_status` 直查,由既有风格一致性守护)。 — 折衷理由记档,不扩超出目的的测试面。

## Risks / Rollback

- 行为面变更仅"keyed 写全 409 失败"的返回错误类型;现网无路径抵达,回滚 = revert 单 commit。
- 已知非目标:409 计 ack / S3 映射(见 Out of Scope)。
