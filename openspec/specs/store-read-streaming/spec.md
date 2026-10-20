# store-read-streaming Specification

## Purpose
TBD - created by archiving change store-read-streaming. Update Purpose after archive.
## Requirements
### Requirement: 引擎提供数据体切片读原语
`BlobStore` SHALL 提供 `read_range(key, offset, len)` 原语:校验口径与整读一致(key 命中、非删除、未过期、记录头 magic/version/key 匹配;活索引指向墓碑即 Corrupt),返回数据体 `[offset, offset+len)` 的字节切片;`offset + len` 越过数据体长度 SHALL 报 Corrupt。切片读不得重验整条 CRC(CRC 覆盖单元是整条记录;bit-rot 防线由复制整读的 CRC 校验与后台 scrub 承担),但每块 SHALL 重验记录头。

#### Scenario: 分段拼接与整读恒等
- **WHEN** 同一 key 先整读、再按任意切分逐段 `read_range` 并拼接
- **THEN** 两者的字节完全一致

#### Scenario: 越界切片被拒
- **WHEN** `read_range` 的 offset 或 len 使区间越过该记录数据体
- **THEN** 返回 Corrupt 错误,不返回部分字节

### Requirement: store 数据面普通 GET 为分块流式响应
数据面 `GET /partitions/{pid}/blobs/{key}`(非复制读)SHALL 在响应开始前经盘池 stat 决定 404 与总长度,然后以有界分块流式响应输出数据体;每块 SHALL 经该盘的有界线程池执行(单请求内存 = 单块缓冲,与对象大小无关);分块读取 SHALL 在每块重新经索引定位(不跨 compaction 缓存旧 offset)。

`?replication=1` 复制读 SHALL 维持原整读路径(含过期数据体返回与整条 CRC 校验)。

#### Scenario: 大对象 GET 字节恒等
- **WHEN** 客户端 PUT 一个远大于分块上限的对象后 GET
- **THEN** 响应字节与写入逐字节一致,`get_bytes_total` 增量等于对象数据长度

#### Scenario: 不可读对象在流首被拒
- **WHEN** GET 一个不存在/已删除/已过期的 key(非复制读)
- **THEN** 流首 stat 即回 404,不产生部分响应

