# Tasks: router-conflict-surfacing

## 1. Router Arbitration

- [x] 1.1 `lib.rs` 新增 `all_already_exists` 自由函数 + 判定矩阵单测(镜像 `all_store_full` 用例结构)
- [x] 1.2 `put_keyed` 失败终判插入 409 特判(StoreFull 判定后)
- [x] 1.3 `put_auto_keyed` / `put_file*` 环内同插;**不做换号重试**(号复用 = 严重信号须透传)
- [x] 1.4 e2e.rs:二次 keyed put → `AlreadyExists` 集成用例;同文件 `free_port` 探测模式退役(上批漏网,迁 0 端口 + bound 取回)

## 2. Gateway Surface

- [x] 2.1 frontend `router_status` 增 `AlreadyExists → CONFLICT`(自 500 兜底组合臂摘除,避免不可达模式)

## 3. Spec & Docs

- [x] 3.1 spec delta:ADDED capability requirements(SDK 终判语义 / 网关 409 映射 / 混合失败保守 Quorum)
- [x] 3.2 `frontend.md` §11 勾销 + §2 POST 行错误面补 409 + §4 409 警示段改透传语义;`router.md` 错误表 `already_exists` 行补透传语义
- [x] 3.3 DEVLOG 步 80;落地记录回填;validate + archive

## 落地记录(已回填)

- router lib 单测 **17** 个(判定矩阵 4 例:全 409 → Some(addr)/混合 → None/无失败 →
  None/1 ok + 其余 409 → Some);e2e **13** 个(新增 `duplicate_keyed_put_surfaces_conflict`:
  RF=2 双副本首写 2/2 ack → 同 key 重发 → 断言 `Error::AlreadyExists` 且 `get` 仍回
  首写内容);**workspace 77 套 767/0;clippy 0;fmt 净**。
- e2e.rs 根治补课:4 处 free_port 全退役(master cfg :0 → serve 后 bound 取回;store
  同;宕机副本模拟地址改 "127.0.0.1:103" 字面量,零 probe)。
