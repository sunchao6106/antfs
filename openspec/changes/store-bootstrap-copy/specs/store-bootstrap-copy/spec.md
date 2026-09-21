# store-bootstrap-copy

封存段物理流式拷贝的 bootstrap(N3 混合的物理路径,MTTR 优化),叠加在 S3a 逻辑拉复制之上。

## ADDED Requirements

### Requirement: 封存段只读拷贝原语(blob 引擎)

`BlobStore` SHALL 提供枚举**封存段**(除活跃段外)`(position, data_end)` 及读取某封存段原始字节(header + 有效数据)的能力;活跃段 SHALL NOT 通过该接口暴露(可变、不可安全拷贝)。

#### Scenario: 只列封存段

- **WHEN** 一个库有多个段且最高位为活跃段
- **THEN** 封存段枚举 SHALL 返回除活跃段外的所有段,且不含活跃段

### Requirement: 装载并重建 bootstrap 副本

store 数据节点 SHALL 支持把一组(来自 peer 的)封存段字节装载为本地某分区的副本:段文件以崩溃安全方式落盘(`tmp→fsync→rename`),全部就位后重建索引打开;装载 SHALL 幂等(部分失败后重试安全,不破坏既有数据)。

#### Scenario: 物理装载后可读

- **WHEN** 把 peer 的封存段字节装载为本地某分区副本
- **THEN** 该副本 SHALL 能读出这些封存段中的存活对象

### Requirement: 混合 bootstrap(物理段拷贝 + 逻辑尾补)

启用复制且启用 bootstrap 拷贝时,一个**空**本地副本首次向 peer 同步 SHALL 先物理拷贝 peer 的封存段,再从"最后封存段末尾"这一续拉游标做逻辑追平(活跃段尾);无 peer 或 peer 无封存段时 SHALL 回落到纯逻辑拉复制(S3a)。

#### Scenario: 物理拷贝后逻辑补齐活跃尾

- **WHEN** peer 有若干封存段 + 活跃段的新写,空副本对其做混合 bootstrap
- **THEN** 追平后该副本 SHALL 含 peer 封存段与活跃段的全部存活对象

#### Scenario: 无封存段回落逻辑

- **WHEN** peer 只有一个(活跃)段、无封存段
- **THEN** bootstrap SHALL 回落纯逻辑拉复制并正确收敛
