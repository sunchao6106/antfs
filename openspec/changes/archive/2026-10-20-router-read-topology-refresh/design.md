# Design: router-read-topology-refresh

## D1 条目与 TTL

`topology: RwLock<HashMap<PartitionId, TopoEntry>>`,`TopoEntry { replicas: Vec<ReplicaLoc>, at: Instant }`。

- TTL 期 = `cfg.partition_refresh_secs`(**与后台可写刷新共用旋钮**,0 = 关 TTL);
- `replicas_for(pid)`:命中且未过期 → 照旧;**过期 → `LookupPartition` 重拉**(成功:替换条目;
  失败:**保留过期表继续返回** —— 读可用性优先,陈旧好过没有)+ `refreshes{reason="ttl"}` +1;
- 填充点(发号回填/lookup miss 填)统一带 `Instant::now()`。

## D2 全败驱逐 + 一次重拉重试

`get`(R=1 循环)试完全部副本后:

- 全 `NotFound` → 维持纯正 miss 语义(对象不存在/已被删,**不**触发驱逐 —— 陈旧拓扑与 miss 无从判别,保守不动);
- 其余(有不可达类:Http/Timeout/Silenced/NoReplica)→ `refreshes{reason="evict"}` +1、**驱逐 pid 条目**、
  `LookupPartition` 一次:成功且新列表与旧不同 → 用新表**重试本请求一次**(单请求有界);
  新表相同/拉失败 → 按原最终错误上抛(不自造重试风暴);
- 驱逐条件不含"单个副本失败"—— 只有**全员失败**才是"拓扑疑似陈旧"的信号(单副本失败由 R=1 自然换路)。

## D3 观测

`router_topology_refreshes_total{reason}`:`ttl`(TTL 到期重拉)/ `evict`(全败驱逐重拉)。
失败成功都不隐 —— reason 只记触发面,成败看 get_total 的 outcome。

## D4 测试

1. 纯语义单测(lib):TTL 未到期用缓存/到期触发;全 NotFound 不驱逐;全败含不可达驱逐;(借 topology
   的可见性 —— lib tests 同 crate 可达 `#[cfg(test)]` 内部字段直断言);
2. e2e:分区副本含 dead 模拟(注册无监听地址)→ TTL 短旋钮(partition_refresh_secs=1)轮询
   断言 `reason="ttl"` 实增;全员不可达形态注册全 dead 分区 → 断言 `reason="evict"`(读仍失败,
   但证据拓扑驱逐发生)。

## Risks / Rollback

- 一次重拉有界(不叠加退避);TTL=默认 30s 对 master 的额外 Lookup 量 = 读多写少场景下
  每 pid 每周期至多一次;回滚 = revert 单 commit。
