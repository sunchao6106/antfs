# store-offset-encoding

`LogSegment` 记录编排与 `IndexValue` offset 编码的选型(承 store.md §3.1)。三方权衡:寻址范围 ↔ 数据文件浪费 ↔ 索引条目大小;决定性因素为索引是否全量常驻 RAM。

## ADDED Requirements

### Requirement: 记录紧密打包，无对齐 padding

`LogSegment` 数据区的变长记录 SHALL 背靠背顺序打包，SHALL NOT 为满足任何 N 字节对齐在记录间或记录尾部补 padding。系统 SHALL NOT 依赖"记录起点为 N 的整数倍"这一不变量。

#### Scenario: 相邻记录物理相接

- **WHEN** 向一个 `LogSegment` 连续追加两条记录 A、B
- **THEN** B 的起始物理 offset SHALL 等于 A 的起始 offset 加 A 的完整落盘长度（无对齐填充间隙）

#### Scenario: 任意长度记录不触发对齐补齐

- **WHEN** 追加一条落盘长度不是任何 2 的幂对齐的记录
- **THEN** 下一条记录 SHALL 紧接其后写入，段的 `data_end` 推进量 SHALL 恰好等于该记录的落盘长度

### Requirement: IndexValue offset 全宽存储，不做 ÷N 压缩

`IndexValue` 的 offset SHALL 以全宽字节偏移存储（`LogOffset { segment, offset }`，段内 offset 为真实字节号），SHALL NOT 存储 `字节地址 / N` 的压缩商。读取 SHALL 直接以该 offset 定位，不经乘除还原。

#### Scenario: offset 往返为真实字节号

- **WHEN** 一条记录写在段内物理字节位置 P（P 可为任意非对齐值）
- **THEN** 其 `IndexValue` 记录的段内 offset SHALL 等于 P，据此 `read` SHALL 精确定位到该记录，无需 ×N 还原

#### Scenario: 单段寻址不被 offset 位宽卡死

- **WHEN** 评估单段/partition 的容量上界
- **THEN** 该上界 SHALL 由容量策略决定，而非由 offset 字段位宽（全宽 u64 覆盖 16EiB，实际不成为约束）

### Requirement: 被否决与保留方案存档

本 capability SHALL 存档被否决方案（÷N 对齐压缩）与其理由，以及保留的二段优化（余数塞索引），供后续实现与优化参照，SHALL NOT 在 base 设计中引入余数塞索引的复杂度。

#### Scenario: ÷N 压缩被明确否决

- **WHEN** 后续实现需要在 offset 编码上做选择
- **THEN** 决策档案 SHALL 表明 ÷N 对齐压缩被否决（理由：索引不全量常驻 RAM，其省 RAM 收益不存在，却引入 padding 浪费与对齐不变量的崩溃风险）

#### Scenario: 余数塞索引仅作二段优化

- **WHEN** 未来出现"热索引子集常驻 RAM 且条目大小敏感"的需求
- **THEN** 余数塞索引方案 SHALL 可作为二段优化启用；在此需求出现前 base 设计 SHALL NOT 采用它
