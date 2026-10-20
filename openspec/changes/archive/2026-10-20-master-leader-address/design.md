# Design: master-leader-address

## D1 注册面(数据可重建 ⇒ 持久性零焦虑)

- 命令 `RegisterMasterEndpoint { node_id, grpc_addr }`:apply = `master_endpoints.insert`(幂等 upsert,重试/重放安全);
- **自报循环**(`serve()` 后 spawn):`client_write` 直到 ok —— Raft 未收敛(无 leader)时退避重试(500ms 起、上限 5s),收敛后一次即可;
  循环退出条件 = 命令 apply 成功;注册数据**可丢可重建**(`redb` 里被清也由下一轮循环补)—— 即使某实现细节产生旧快照,正确性自愈。
- 注册内容 = 本节点 `config.grpc_addr` 字符串原样;**advertiseaddr 语义由配置方保证**(须监听可对外地址;`0.0.0.0:7101` 形态在多机部署会自坑,文档注记)。

## D2 leader_addr 解析(唯一真值,无 fallback —— 所有者裁定 2026-10-20)

```
current_leader(NodeId) → master_endpoints.get(id)   ← 唯一来源(自报收敛后常态)
                      └→ None(注册窗口期:无提示,调用方重试)
```

- **不再回退 membership raft 地址**:系统无部署态历史包袱,"raft 地址提示"这一旧行为随本
  change 直接废弃(同 descriptor-drop-magic 精神);窗口期为空提示 · 1-2s 自报收敛;
- 全部 `leader_addr` 回填点共用 `ServerContext::leader_addr()` 单函数,proto 注释统一改语义。

## D3 Router 自愈(单飞换通道)

- 新 `Error::NotLeader { detail, leader_addr: Option<String> }`:替换 `"not leader"` → `Rejected` 的字符串折叠
  (`map_master_error` 返回值族);`kind() = "not_leader"` 稳定标签;
- `Router::retarget_master(hint)`:
  - 构造新 channel(与 `connect` 同形:`Endpoint::from_shared("http://hint")` + request_timeout);
  - `*self.master.lock().await = MasterClient::new(channel)` —— master client 在 `Mutex` 内整体替换,
    并发发号方天然串行化;
  - **防抖**:AtomicU64 `last_retarget_ms`,1s 窗口内重复 not-leader 不再反复换(hint 抖动/选主中期不放大);
  - 仅写通道换:`spawn_refresher` 持独立 channel,其 RPC(ListPartitions)是只读、follower 可服务,无需热切。
- 触发/重试:`sync_segments → segment_from_response → NotLeader{leader_addr: Some}` 冒泡;`allocate_one`
  顶层捕**一次 in-flight 重试**(换通道后立刻重发号;头一次同步路径里 GetBlobIds 携带的 hint 多为同一轮选主结果)。
- 不无限自愈:重试后仍 NotLeader(1s 防抖内)照原样上抛 —— 上层可见 `Error::NotLeader`(含 hint),运维可程序化处置。

## D4 兼容/测试/文档

- 兼容性(所有者裁定 2026-10-20):无部署态历史包袱 ⇒ **无 §3.4 双向义务**(无 `serde(default)`;
  raft fallback 弃用)。注册数据可重建 ⇒ 快照语义零脆弱面。
- 测试:master 单测(Register 幂等 + leader_addr 唯一真值解析);router e2e(3 节点真 master,
  client 连 follower,首写自愈 → 成功;二次写无 not-leader);错误分类单测。
- 文档:router.md §7 勾销 + 错误表;cluster.md master 注册段;DEVLOG 84。

## Risks / Rollback

- 错误面新增变体 = 仓内全调用面补 arm(编译器强制,已全改);回滚 = revert 单 commit。
- 自报循环退避不产生 thundering herd(单节点常数)。
