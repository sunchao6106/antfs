## Why

LogSegment 数据区如何编排记录、`IndexValue` 如何存 offset,直接决定三件事的取舍:**单段寻址范围 ↔ 数据文件空间浪费 ↔ 索引条目大小**。SeaweedFS 等在这里做了 "记录强制 N 字节对齐 + 索引存 `offset/N`" 的压缩(以 32GiB 卷上限换 4 字节 offset + 每对象 padding 浪费),是一个**被生产系统采用但前提特定**的方案。本 change 固化 ambry-rs 在此点的选型与被否决方案,避免后续实现时凭直觉误抄 ÷N 压缩。

本 change 只**固化设计决策**,不改实现代码(现状 `LogSegment` 顺序打包、`IndexValue` 全宽 `LogOffset` 已符合本决策)。

## What Changes

- **固化**:数据区变长记录**紧密顺序打包,不做 N 字节对齐 padding**;`IndexValue` 的 offset 用**全宽字节偏移**(`LogOffset { segment, offset }`,段内 offset 存真实字节号),**不做 ÷N 压缩**。
- **明确分水岭**:offset 是否压缩的决定因素是**「索引是否全量常驻 RAM」,不是 key 长度**。ambry-rs 索引为磁盘 mmap 封存段(见 io-index-design),offset 位宽是磁盘账,故选全宽。
- **否决**:÷N 对齐压缩方案(SeaweedFS 式)。**保留**:余数位塞索引的混合方案,仅作未来"热索引子集缓进 RAM"时的二段优化。

## Capabilities

### New Capabilities
- `store-offset-encoding`: LogSegment 记录编排与 IndexValue offset 编码的选型——全宽 offset + 零对齐 padding;三方权衡(寻址/浪费/索引大小);被否决的 ÷N 压缩及理由;保留的余数塞索引二段优化。

### Modified Capabilities
<!-- 无既有 spec 冲突;与 io-index-design 的 index-engine 前提(索引不全量常驻 RAM)互补。 -->

## Impact

- **代码**:无变更——现状已符合(`crates/store/src/log_segment.rs` 顺序打包、`crates/store/src/index/` 的 `IndexValue` 全宽 `LogOffset`)。本 change 仅补决策档案。
- **文档**:`docs/design/store.md` §3.1 已记该决策与权衡表,本 change 与之对齐。
- **保留契约**:log 零 padding、无"记录必须 N 对齐"不变量(少一类崩溃一致性风险)、单段寻址上限由容量策略而非 offset 位宽决定。
- **对齐 Ambry**:`IndexValue.OFFSET_SIZE_IN_BYTES = 8`,段内 offset 为原始 `long`,无对齐无压缩(源码实证)。
