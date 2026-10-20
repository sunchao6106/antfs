# router-read-topology-refresh Specification

## Purpose
TBD - created by archiving change router-read-topology-refresh. Update Purpose after archive.
## Requirements
### Requirement: 读拓扑条目按 TTL 刷新且全败时驱逐重拉
Router 的分区副本缓存条目 SHALL 携带时间戳并在 TTL(与 `partition_refresh_secs` 同值,0=关闭)到期时经 master 重拉;重拉失败 SHALL 保留过期条目继续服务(读可用性优先)。R=1 读试完全部副本均失败且失败含不可达类时 SHALL 驱逐该分区条目并重拉一次;若新副本列表与旧列表不同 SHALL 以新列表重试本次读取一次(单请求至多一次);全部副本返回 NotFound 的纯正 miss SHALL NOT 触发驱逐。观测 SHALL 以 `router_topology_refreshes_total{reason=ttl|evict}` 计数。

#### Scenario: TTL 到期后读拿到新拓扑
- **WHEN** 某分区副本集在 master 侧变更,且 router 缓存条目超过 TTL 后再次读取
- **THEN** 读取先经 master 重拉副本列表(计 reason=ttl),新列表生效

#### Scenario: 全员不可达触发驱逐与一次重试
- **WHEN** R=1 读的全部副本都不可达(含网络类失败)且缓存已陈旧
- **THEN** 条目被驱逐(计 reason=evict),router 重拉拓扑;列表变化时该次读取用新副本重试一次,仍败则按最终错误上抛

#### Scenario: 纯 NotFound 不驱逐
- **WHEN** 所有副本对读取回 NotFound
- **THEN** 读按对象不存在语义返回,不驱逐、不重拉

