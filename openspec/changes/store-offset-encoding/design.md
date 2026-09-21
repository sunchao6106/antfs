## Context

工程目标是单机高密度、聚合**千亿(10¹¹)小文件**。在 `LogSegment` 里如何编排一条条变长记录、在 `IndexValue` 里如何存"记录在 log 的字节位置(offset)",是一个绕不开的磁盘格式决策,牵动三方权衡:

- **寻址范围**:一个 offset 字段能定位多大的段/log。
- **数据文件浪费**:为满足某种编码约束(如对齐)在数据区补的 padding。
- **索引条目大小**:offset 字段占几字节,× 条目数,算在 RAM 还是磁盘。

参照系统的两种典型做法:
- **SeaweedFS(÷N 压缩)**:needle 强制按 `NeedlePaddingSize=8` 字节对齐,索引 offset 存 `字节地址 / 8`(4 字节),读时 ×8 还原。效果:4 字节 offset 覆盖 `2³² × 8 = 32GiB`(源码 `MaxPossibleVolumeSize`);代价:每对象尾部补 1~8 字节 padding。其动机是 **needle map 全量常驻 RAM**,每字节 ×十亿条都要命,故牺牲磁盘 padding 换 RAM。
- **Ambry(全宽 offset)**:`IndexValue.OFFSET_SIZE_IN_BYTES = 8`,段内 offset 是原始 `long`,记录紧密打包,无对齐、无压缩(源码 `ambry-store/.../IndexValue.java` 实证)。其可行前提是 **索引主要驻磁盘**(mmap 封存段 + 过滤器,见 io-index-design change),offset 多占字节是磁盘账。

**关键洞察**:选 ÷N 压缩还是全宽,分水岭是 **「索引是否全量常驻 RAM」,不是 key 长度**。早期讨论一度以"key 本来就几十字节,offset 多几字节可忽略"为由,该论据不成立(key 长度/形式未定,仅确定含 partitionId),已弃用。

## Goals / Non-Goals

**Goals:**
- 固化 ambry-rs 在"记录编排 + offset 编码"上的选型,给出三方权衡与决定性因素,供实现与后续优化参照。
- 记录被否决方案(÷N 压缩)与保留的二段优化(余数塞索引),对齐 CLAUDE.md 决策档案要求(含被否决方案)。

**Non-Goals:**
- 不改实现代码(现状已符合)。
- 不定单段/partition 的具体容量上限值(属容量策略,见 io-index-design 的定容锚点)。
- 不涉及小对象聚合/chunking 的具体设计(仅指出它才是压存储放大的高杠杆)。

## Decisions

### D1:记录紧密顺序打包,不做 N 字节对齐 padding
- 数据区变长记录背靠背写,不为对齐在记录间/尾部补字节。
- **收益**:log 零 padding 浪费;无"记录起点必须 N 对齐"这一持久化不变量 → 少一类崩溃一致性 footgun(对齐 CLAUDE.md §3.1 持久性红线)。
- **对齐 Ambry**:log 记录顺序打包,不对齐。

### D2:IndexValue offset 全宽存储,不做 ÷N 压缩
- offset 存全宽字节偏移(`LogOffset { segment, offset }`,段内 offset 为真实字节号);读时直接 seek,无乘除。
- **寻址范围**:段内 offset 全宽(u64)→ 近乎无限(16EiB),单段/partition 上限由**容量策略**而非 offset 位宽决定。
- **代价**:索引每条 offset 占更多字节;因 ambry-rs 索引驻磁盘 mmap 段,该代价是磁盘账,不吃 RAM。

### D3:三方权衡与选型表

| 方案 | 数据浪费 | 索引 offset 占用 | 寻址范围 | 复杂度 | 代表 |
|---|---|---|---|---|---|
| ① ÷N 对齐(存 `offset/N`,强制记录 N 对齐) | 平均 ~N/2 B/对象 | 最小(如 4B) | `2^(8·宽) · N` | 中(维护对齐不变量) | SeaweedFS(N=8→32GiB) |
| **② 全宽 offset(直接存字节号,不对齐)** | **0** | 大(8/16B) | 近乎无限(u64=16EiB) | **最简单** | **Ambry / 本决策** |
| ③ 余数塞索引(存 `offset/N` + 低位余数进空闲 bit) | **0** | 小(4B + ~3bit) | `2^(8·宽) · N` | 高(需空闲位 + 破坏扫描步进) | — |

**选 ②**,因 ambry-rs 索引驻磁盘,①省的 RAM 收益不存在。

### D4(否决):÷N 对齐压缩(方案①)
- **否决理由**:我们索引不全量常驻 RAM,①以"每对象 padding 浪费 + 对齐不变量维护/崩溃风险"换取的 RAM 节省在本架构下无收益。÷N 压缩是 SeaweedFS "索引全内存"约束下的正解,不是通用最优。

### D5(保留):余数塞索引的混合方案(方案③)
- 存 `offset/N` + 把低位余数(N=8 时 3 bit)塞进 `IndexValue` 空闲位,既零 padding 又保持小 offset 字段。
- **保留而非采用**:仅在未来"把热索引子集缓进 RAM 做小对象吞吐"、RAM 索引条目大小变敏感时启用;base 设计不引入其复杂度(需可靠腾空闲位,且破坏"扫 log 按定长步进重建"的便利)。

### D6:存储放大的高杠杆不在此处
- 小对象存储放大主要来自**每对象固定头开销**(header/crc/时间戳等),对齐 padding 只是其中最小一项。真要压放大,高杠杆是**上层对象聚合/chunking**(对齐 Ambry composite blob),而非抠 offset 位编码。此处决策只求"不引入不必要的浪费与不变量",不追求极致省字节。
