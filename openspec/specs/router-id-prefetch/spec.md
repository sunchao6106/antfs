# router-id-prefetch Specification

## Purpose
TBD - created by archiving change router-id-prefetch. Update Purpose after archive.
## Requirements
### Requirement: 号段低水位触发后台单飞预取
Router 发号命中本地号段后,若该分区段剩余量低于四分之一段长(或不超过 2 号),SHALL 触发该分区的一次后台预取(同分区同时至多一个在飞);预取取回一段(容量 = `id_segment_size`),若与本地段连续(master 发号连续性)SHALL 无缝续段(不产生空洞),不连续 SHALL 保守丢弃;本地无段 SHALL 直接插入。预取失败 SHALL 静默计数(`router_id_prefetch_errors_total`)且不影响发号正确性 —— 号段耗尽的同步补段路径 SHALL 维持原状(预取仅为平滑)。

#### Scenario: 极小段连续高频发号流畅
- **WHEN** 配置极小号段(如 4 号/段)并连续发号数十次
- **THEN** 预取与同步补段任一路径保证发号连续成功,预取零失败计数

#### Scenario: 预取回填无缝续段
- **WHEN** 预取段起点恰为本地段段尾(master 连号)
- **THEN** 本地段直接续尾,零新空洞

