# router

router 发号(blobId 分配)按 master 下发的 partition 写权重加权分配,取代此前的"可写分区均权轮转"。
承 [`router-partition-round-robin`](../../router-partition-round-robin/proposal.md) 自己记下的那处偏离
(D10 的"按剩余容量加权"被弱化为均匀轮转),本 capability 把它收口。

## ADDED Requirements

### Requirement: 写权重字段的语义与前后兼容

`PartitionEntry` SHALL 携带 `write_weight`(千分比,`0..=1000`):`1000` = 该分区完全空,越接近 `1` 越满,
**计算值的下限 SHALL 为 1**。`0` SHALL 表示"未下发 / 无意见",客户端 SHALL 按**均权**处理。故 `0` SHALL NOT
被解释为"零分配"。客户端 SHALL 只使用**相对**权重(段大小比 = 取号比),SHALL NOT 解释容量口径。

#### Scenario: master 未下发权重时按均权

- **WHEN** client 从旧 master(或未配 `partition_capacity_bytes`)拿到 `write_weight == 0`
- **THEN** 该分区 SHALL 按权重 `1` 参与分配,整体行为 SHALL 与"均权轮转"逐位一致

#### Scenario: 满或超卖的分区不被饿死

- **WHEN** 某可写分区上报的 `used_bytes` ≥ 其容量上限
- **THEN** 其 `write_weight` SHALL 为 `1`(不是 `0`),该分区 SHALL 仍参与发号

#### Scenario: 未配容量上限即无意见

- **WHEN** master 未配置分区容量上限(`partition_capacity_bytes == 0`)
- **THEN** 其下发的 `write_weight` SHALL 为 `1000`(无意见 ⇒ 客户端按均权)

### Requirement: master 与 client 共用同一个权重口径

master SHALL 用一个纯函数(输入 `cap`/`used`,输出千分比)计算写权重,并 SHALL 在**两处共用同一个定义**:
① `partition_id` 缺省时按权重加权挑分区;② `ListPartitions` 下发 `PartitionEntry.write_weight`。
`used` SHALL 取"每分区各副本 `used_bytes` 的最大值"。

测试:`crates/master/src/server.rs::capacity_weight_is_remaining_ratio_with_floor_one`。

#### Scenario: 两处口径一致

- **WHEN** 同一个分区的容量与占用经心跳进入软状态
- **THEN** 它在 `ListPartitions` 里下发的 `write_weight` SHALL 与"`partition_id` 缺省发号"所用的权重相同

#### Scenario: u64 上界不溢出

- **WHEN** 以 `cap = u64::MAX` 调用权重计算
- **THEN** SHALL 返回 `1000`(空)/ `500`(半满)等正确比例,SHALL NOT 因乘法溢出而回绕

### Requirement: 取号按写权重分配(平滑加权轮转)

router SHALL 在**取号环节**按写权重挑选分区(平滑加权轮转:每拍给在册分区各加 `weight` 信用,取最大者,
再减去在册权重总和),SHALL NOT 只按权重切分号段大小而取号仍顺序轮转。手上没有号段的分区 SHALL NOT 参与
分配、SHALL NOT 累积信用。同分时 SHALL 取 partition id 较小者(结果确定、可单测)。

测试:`crates/common/router/src/lib.rs::weights_steer_allocation_within_a_short_window`、
`crates/common/router/src/lib.rs::uniform_weights_rotate_across_partitions`、
`crates/common/router/src/lib.rs::zero_weight_means_uniform_not_zero`。

#### Scenario: 短窗口内即按权重分配

- **WHEN** 三个可写分区的权重为 1000:500:500,连续取 20 个号
- **THEN** 落在三个分区的个数 SHALL 精确为 10:5:5(而不是"各分区相等")

#### Scenario: 均权退化为顺序轮转

- **WHEN** 所有在册分区权重相等
- **THEN** 取号顺序 SHALL 逐位等价于原来的顺序轮转(相邻写落在不同分区)

#### Scenario: 无号段的分区不参与也不攒信用

- **WHEN** 某分区当前的号段已取完
- **THEN** 它 SHALL NOT 被发号、SHALL NOT 累积信用(补段后从 0 起,不吐出一串突发)

### Requirement: 号段份额按权重配比且总预留不变

号段总预留 `pool` SHALL 为 `id_segment_size × 可写分区数`(与均权时**不变**);各分区份额 SHALL 为
`max(1, pool × w_i / Σw)`,余数 SHALL 按「小数部分降序,pid 升序」分配。均权时每个分区的份额 SHALL 等于
`id_segment_size`。份额 SHALL NOT 为 0(0 长度的段等于"该分区不发号",那应由 master 把它转 RO 表达)。

测试:`crates/common/router/src/lib.rs::segment_shares_are_proportional_with_a_floor_of_one`。

#### Scenario: 按比例切分

- **WHEN** `pool = 128`、权重 1000:500
- **THEN** 两份份额 SHALL 为 `[85, 43]`

#### Scenario: 极端偏斜不产生 0 长段

- **WHEN** `pool = 2048`、权重 1000:1
- **THEN** 份额 SHALL 为 `[2046, 2]`,SHALL NOT 出现 0 长度的段

#### Scenario: 均权时逐位一致

- **WHEN** 有 N 个可写分区且权重全相等
- **THEN** 每个分区的份额 SHALL 等于 `id_segment_size`(总预留 = `N × id_segment_size`,与改动前相同)

### Requirement: 后台刷新按新份额收敛已有号段

拉取可写分区列表(带权重)后,router SHALL 丢掉已不可写分区的号段,并 SHALL 把**已有号段的段尾**按本次份额
收敛为 `end = min(end, next + share)`;SHALL NOT 增大段尾(增大等于替 master 多要号)。被收敛掉的余号 SHALL
成为**空洞**(无害)。收敛到无号可发的段 SHALL 被丢弃。信用 SHALL 只保留给手上仍有号段的分区。

测试:`crates/common/router/src/lib.rs::trim_segment_only_shrinks_and_reports_exhaustion`(收敛规则本身)、
`crates/common/router/tests/e2e.rs::background_refresh_tracks_writable_partitions`(转 RO 即丢段、不再发号)、
`crates/common/router/tests/e2e.rs::partition_write_weight_steers_allocation`(权重变化经补段端到端生效)。

#### Scenario: 权重变化在一个刷新周期内生效

- **WHEN** 某分区的占用上升(权重下降)而它手上仍持有号段
- **THEN** 该号段的段尾 SHALL 在本次刷新时被收紧,权重变化 SHALL NOT 等整池号段轮完才生效

#### Scenario: 收敛掉的余号成空洞

- **WHEN** 某个已持有号段被收敛掉一部分
- **THEN** 那部分 blobId SHALL 不再被发放(成为空洞),SHALL NOT 被发放给别的分区

#### Scenario: 分区不可写即丢段

- **WHEN** 某分区从可写(RW)列表中消失
- **THEN** 它未用完的号段 SHALL 被丢弃,后续发号 SHALL NOT 再落到该分区

### Requirement: 权重可只读观测

router SHALL 提供只读方法 `write_weights()` 返回当前生效的 `(partition_id, weight)` 快照(pid 升序,未下发
已归一),SHALL NOT 借它改变任何分配行为;压测报告 SHALL 把"权重 → 期望 %"与写成功的实际分布并列打印。

#### Scenario: 快照跟随 master

- **WHEN** master 侧某分区占用变化、router 完成一次刷新
- **THEN** `write_weights()` 返回的该分区权重 SHALL 跟随更新

## MODIFIED Requirements

### Requirement: 一次补段的范围

`id_segment_size` 的语义 SHALL 为"**均分时**每个可写分区一次申请的 blobId 个数";总预留 SHALL 仍为
"可写分区数 × `id_segment_size`"(实际按写权重配比),SHALL NOT 因按权重分配而改变总预留口径。

#### Scenario: 总预留口径不变

- **WHEN** 有 N 个可写分区、权重各不相同
- **THEN** 本轮申请的号总数 SHALL 仍在"N × `id_segment_size`"量级(按份额配比,允许下界 `1` 带来的极小超出)
