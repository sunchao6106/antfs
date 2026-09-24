## Why

上一轮 change [`router-partition-round-robin`](../router-partition-round-robin/proposal.md) 把写路径从"单分区号段
连续耗尽"改成"每个可写分区各持一段 + 轮转",并在同一份 proposal 里**自己记下了一处偏离**:`blobid-master-assign`
D10 定的是"partition 由 master **按剩余容量加权**挑",落地形态却只是"可写分区上的**均匀轮转**" —— 因为那一轮
还没有一条把"容量"送进客户端路由决策的路径。本次把这处偏离**收口**:router 发号按 master 下发的 partition
写权重加权分配。

- **为什么值得补**:均匀轮转只在"分区之间容量本来就一样"时才是对的分配。集群里分区是先后建出来的、盘是先后
  填满的,同一个可写集合里"半满的分区"与"空分区"拿到一样多的号 ⇒ 先撞 507 的一定是前者。按写权重发号让
  分配跟着"还能装多少"走:越空的分区拿到越多号。
- **为什么补的方式是"下发权重"而不是"改 master 挑分区"**:`partition_id` 缺省(由 master 挑一个)这条路径
  只有**别的调用方**走。本次落地的形态是 client **显式点名**每一个可写分区(它必须自己持有多个号段才能把写 IOPS
  摊到多块盘上,见上一轮 change),容量口径因此**必须下发到 client**,否则"按容量加权"根本轮不到路由决策。
- **为什么只能做到粗粒度(不吹)**:权重来自 store 心跳上报的 `used_bytes`(报告周期
  `store_server.heartbeat_interval_secs` 默认 3s),且只在"补段 / 后台刷新"那一拍生效(后台周期
  `router.partition_refresh_secs` 默认 30s)。它是**心跳级的粗粒度容量反馈,不是精确配额** —— 本 change 不声称
  能把各分区用到毫厘不差,也不引入任何精确的容量预留。

## What Changes

- **协议:加字段,不动既有字段**。`crates/master/proto/master.proto` 的 `message PartitionEntry` 新增
  `uint32 write_weight = 10;` —— **千分比 `0..=1000`**:`1000` = 该分区完全空,越接近 `1` 越满。
  `0` = **未下发 / 无意见 → 客户端按均权处理**;计算值的**下限固定为 1**,所以 `0` 永远不会表示"零分配",
  可以安全地让给"未下发"这个含义。客户端只用**相对**权重(段大小比 = 取号比),**容量口径(policy)只活在
  master 侧** —— router 不解释它。
- **master 侧:一个定义,两处复用**。`crates/master/src/server.rs` 新增纯函数
  `capacity_weight_permille(cap, used) -> u32`(内部 `u128` 做乘法防 `cap = u64::MAX` 溢出;下限 1;
  `cap == 0` 即未配容量上限 → 返回 `1000` = 无意见)与常量 `WEIGHT_PERMILLE_MAX = 1000`。**两处共用同一个
  定义**(否则集群里会出现两套自相矛盾的"容量权重"):
  - ① 发号时 `partition_id` 缺省 → `pick_writable_partition` 按它**加权随机**挑分区(改动前是"字节余量"口径,
    现在统一成千分比);
  - ② `list_partitions` 把它下发给 client(`PartitionEntry.write_weight`)。
  `used` 口径 = `SoftState::used_bytes_by_partition()`(**每分区取各副本 `used_bytes` 的最大值**,RF 不重复
  计数)。权重是**软状态派生的只读视图,不进 Raft / apply**(切主后由心跳重建);`pick_writable_partition` 的
  随机只在 leader 侧发生,而 `list_partitions` 是**只读查询 —— 任何节点(含 follower)都可答,各自按自己那份
  软状态现算**(软状态由心跳重建,故各节点读数可能略有差异,见 [`cluster.md` §7.1](../../../docs/design/cluster.md))。
- **router 侧:按权重取号 + 按权重配号段**。`crates/common/router/src/lib.rs`:
  - `struct PartitionWeight { pid, weight }`;`IdCache { segments, order: Vec<PartitionWeight>,
    credit: HashMap<PartitionId, i64> }` —— 早期的 `cursor` 由 `credit` 取代。
  - `take_weighted()` = **平滑加权轮转(SWRR)**:每拍给在册分区各加 `weight` 信用,取最大者,再把它减去在册
    权重总和。手上没号段的分区**不参与、也不攒信用**;同分取 pid 小的 ⇒ 结果**确定、可单测**;**均权时逐位
    退化为原来的顺序轮转**(相邻写仍落在不同分区)。
  - `segment_shares(pool, weights)`:总预留 `pool = id_segment_size × 可写分区数`(**与改动前不变**),
    `share_i = max(1, pool × w_i / Σw)`,余数按「小数部分降序,pid 升序」分配;均权时 `share_i == id_segment_size`。
  - `sync_segments()`:拉列表(带权重) → 丢掉已不可写分区的号段 → 按份额切 → **已有段按新份额收敛段尾**
    `end = min(end, next + share)`(收敛掉的余号成**空洞**,D3 无害;收敛到没号的段直接丢掉) → 重建 `order` →
    `credit` 只留给手上还有号段的分区(新分区从 0 起)。
  - `list_writable()` 返回 `Vec<PartitionWeight>`,`write_weight.max(1)` 在此归一;新增公开只读方法
    `Router::write_weights() -> Vec<(PartitionId, u32)>`(排障 / 压测对照"期望比例 vs 实际分布"用)。
  - **`RouterConfig` 不新增配置项**;`router.id_segment_size` 的语义改为"**均分时**每个可写分区一次申请的
    blobId 个数(总预留仍是 分区数 × 该值)"。
- **压测**:`benchmark/router/src/lib.rs` 的分配分布报告新增「权重 → 期望 %」一列(经 `Router::write_weights()`),
  把期望比例与实际分布并列打印。
- **文档**:`docs/design/router.md` §2 写路径与流程片段、§5 配置表 + 表下"请求哪个 partition"、§6 表后的观测办法、
  §7 差异与待办;`docs/design/cluster.md` §7.4(`ListPartitions` / `PartitionEntry` 说明)与 §7.5 第 ④ 条写路径
  时序;`docs/design/frontend.md` 内嵌配置表;`docs/DEVLOG.md` 新增步骤 29。

### 为什么必须 SWRR:只按权重切"段大小"不成立

这是本 change 最该写清的一步论证。把"每个分区的号段大小"按权重切分、**取号仍按顺序轮转**,在直觉上像是
"按权重分配",实际不成立:

- 顺序轮转每轮给每个分区**各 1 个号**,于是**任何中途观察窗口内各分区的计数都相等**;
- 权重只能决定"最满的那个分区的号段何时先耗尽、之后什么时候停止发号",**它不改发号环节的相对比例**;
- 更糟的是,短窗口内的实际偏差方向甚至是**反的** —— 最满分区的段先耗尽后,剩下的号**全部**给别的分区。

所以**取号本身必须按权重挑**;段大小再按权重配比,只是为了让各分区的号段**同步耗尽**(否则每个分区的段长
不同,补段时机不同,配额又会漂移)。两者配合,才在**任意窗口**内都按权重分配 —— 这正是 `take_weighted()` 单测
里"20 个号按 1000:500:500 精确 10:5:5"钉住的语义。

### 权重什么时候生效(反馈滞后,如实写)

- 权重来源:store 心跳里的 `used_bytes`(`store_server.heartbeat_interval_secs` 默认 3s 上报一次);
- 生效时机:只在 router **补段或后台刷新**那一拍拉取(`router.partition_refresh_secs` 默认 30s 一拍);
- **刷新时"收敛段尾"这一步是关键**:它把"权重何时生效"的窗口从"整池轮完"压到**一个刷新周期**
  (刷新时按新份额把已持有号段的段尾收紧,不必等它自然耗尽);
- `partition_refresh_secs = 0`(关后台刷新)时退化为"**整池轮完才更新权重**" —— 可接受:号段亏空的写路径
  自己会同步一次,只是滞后变大。

### 手动权重的插口(为 admin 预留,客户端零改动)

本 change **只做两个东西**:`PartitionEntry.write_weight` 字段 + master 侧 `capacity_weight_permille` 这个
**唯一插口**。后期若要做 admin 手动设权重(例如把某个分区人为压到很低),路径是
**admin → master(把人工值写进同一处计算 / 替换该函数的结果)→ 同一个 `write_weight` 字段下发**,
**客户端零改动** —— router 本来就只当相对权重用。本 change **不实现** `SetPartitionWeight` 与 admin 入口。

## Capabilities

### Modified Capabilities

- `router`(见 [`docs/design/router.md` §2](../../../docs/design/router.md)):发号由"可写分区**均权轮转**"改为
  "按 master 下发的 partition **写权重**加权分配";号段份额按权重配比、后台刷新时按新份额收敛段尾。
- `master`(见 [`docs/design/cluster.md` §7.4](../../../docs/design/cluster.md)):`ListPartitions` 的
  `PartitionEntry` 新增 `write_weight`(千分比),与"`partition_id` 缺省时的加权挑分区"**共用同一口径与同一函数**。

## Impact

- **crate**:`antfs-router`(lib:发号路径 + 只读方法 + 单测/e2e)、`antfs-master`(`server.rs` 纯函数 + 下发
  字段 + 单测)、`antfs-bench-router`(报告一列)。`RouterConfig` / store 侧**无改动**。
- **协议**:`PartitionEntry` **加字段**(编号 10,既有字段与编号一律不动)。**前后兼容(CLAUDE.md §3.4)**:旧
  client 拿到新字段会忽略它(行为不变);新 client 对接到**旧 master** 时字段回默认值 `0` → 归一成 `1` →
  **按均权处理,行为与改动前逐位一致**(`list_writable` 里 `write_weight.max(1)` 就是这个兼容落点)。
- **监控**:**不新增指标**(本次没有新增任何指标名)。"权重是否生效"的观测办法:`Router::write_weights()` 只读
  快照 + 压测打印的「权重 → 期望 %」列,与写成功的 blobId 在各分区上的实际分布并列对照。
- **代价 / 取舍(如实记)**:
  - `take_weighted()` 每次发号 **O(n)**(n = 本地持段分区数,即"可写分区数";它与 `id_segment_size` 相乘构成
    总预留,实际规模有限)。十万级分区需换 O(log n) 结构 —— 已记入待办,不在本 change 做。
  - 极端偏斜时(如 1000:1)最满的分区每轮只拿 1 个号(≈0.1%)。它仍是"可写分区",写进去仍可能 507,错误照旧
    上报 —— 该分区应由**运维转 RO / 封存**,本 change **不静默吞掉**它,也不做"本地按 507/silenced 降权"。
  - **仍不做盘级摊开 / 放置**:分区级按权重分配只决定"号落在哪个分区",副本落在哪块盘仍由 master 的放置决定
    (既有判定:杠杆在 `crates/master/src/placement.rs`,见 [`router.md` §7](../../../docs/design/router.md))。

## 明确不做(边界)

- **不动盘级摊开 / 副本放置**(与上一轮同一判定:杠杆在 `crates/master/src/placement.rs`)。
- **不做 admin 手动权重入口,不做 `SetPartitionWeight`** —— 本轮只留 `write_weight` 字段 + master 侧
  `capacity_weight_permille` 这个唯一插口(见上"手动权重的插口")。
- **不做按权重的号段后台预取**(号段仍是"用完了才补";预取是另一件事,D2 双 buffer)。
- **不做 507 / silenced 驱动的本地降权**(客户端不自行判容量 / 判健康,只消费 master 的相对权重)。
- **不新增指标、不新增配置项**(`RouterConfig` / `MasterConfig` 均不改)。
