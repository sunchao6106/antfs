# Proposal: master-leader-address — leader 以 gRPC 地址自报,router 自愈重定向

## Why

`RouterConfig.master_addr` **须指向 leader** 是文档长期挂着的运维痛点(router.md §7「leader 重定向」):
master 发号(`GetBlobIds`)是写路径,非 leader 回 `ok=false` + `leader_addr` —— 但该字段一直回 **raft 内部地址**
(openraft membership 只存 `BasicNode::new(raft_addr)`),router 拿到也用不了,结果是 **每次切主都要人手改全部
router 配置**。三节点 master 集群的可用性因此绑在"配置文件跟着 leader 跑"上。

## What Changes

| 位置 | 改动 |
|---|---|
| `crates/master/src/types.rs` | 新命令 `RegisterMasterEndpoint { node_id, grpc_addr }`(Raft apply 幂等 upsert);状态机新字段 `master_endpoints: BTreeMap<NodeId, String>`(`serde(default)` —— §3.4 后向兼容,旧快照读空表) |
| `crates/master/src/server.rs` | `MasterNode::serve()` 后 spawn 自注册循环(退避重试直到 apply 成功 —— Raft 幂等使重试安全);`leader_addr()` **优先查 `master_endpoints`**,查不到回退旧 membership raft 地址(兼容自报未收敛的窗口) |
| `crates/master/proto/master.proto` | `leader_addr` 字段注释改语义:gRPC 地址(自报未收敛时为 raft 内部地址,fallback 兼容) |
| `crates/common/router` | 新错误变体 `Error::NotLeader { detail, leader_addr: Option<String> }`(取代 `"not leader"` 消息折叠进 `Rejected`);`Router` 内建自愈:发号遇 `NotLeader` 且 hint 可用 → **单飞切换 master channel**(防抖 1s,`Mutex<MasterClient>` 整体换新 channel)+ 重试发号一次 |
| 测试 | master:注册幂等 upsert / leader_addr 优先查表 + raft fallback;router e2e:3 节点 master、client 故意连 follower → 首写触发自愈切 leader → 写成功且后续写直达 |
| 文档 | `router.md` §7「leader 重定向」勾销 + 错误表 NotLeader 行;`cluster.md` master 注册机制段;DEVLOG 步 84 |

## Impact

- **磁盘格式**:所有者裁定(2026-10-20)—— **系统无部署态历史包袱,不做旧快照兼容**(`master_endpoints` 无 `serde(default)` 缺字段容错;与 descriptor-drop-magic 同一精神)。注册数据**可丢可重建**(`serve()` 后自报循环秒级收敛),无持久性依赖;自报未收敛的窗口期 `leader_addr` 为空(调用方无提示时重试即可)。
- 行为:切主后 router 自愈(1 次重试 + 通道切换);`Error::Rejected("not leader…")` 归一为 `NotLeader`(错误面 **Breaking** —— `kind()` 稳定标签:`not_leader`,新增枚举变体,外部 match 需补 arm;仓内调用面已全改)。配置零新增(自报用既有 `grpc_addr`;advertise host 为监听地址 —— 文档注记)。
- 不做:多 master 种子列表(`master_addr` 仍单点;自愈只解决"指到 follower/旧 leader"这一族)、refresher 通道热切(ListPartitions 只读、follower 可服务,无需)。

## Out of Scope

- router 的 master 种子列表/故障转移全集(另案)。
- gRPC 层 LB / DNS 发现。
