# blob-range-read — spec delta(2026-10-21 实施刊定)

## ADDED Requirements

### Requirement: store 数据面窗口读
store 数据面 `GET /partitions/{pid}/blobs/{key}` SHALL 支持 `?offset=&length=` 查询参数:offset 缺省 0、length 缺省读到尾并截到对象尾;start 超过对象长 SHALL 回 416;零长窗口 SHALL 回空 200;`?replication=1` 复制读分支 SHALL 不消费窗口参数且整读契约(含过期数据体 + 整条 CRC)零触碰;窗口流 SHALL 复用既有分块 `read_range` 管线(内存 = 单块)且窗口长计入指标。

#### Scenario: 数据面窗口
- **WHEN** 对 300 KiB blob 请求 `?offset=1000&length=2048`
- **THEN** 200 恰回 2048 字节(= 整读切片同款);`?offset=999999` → 416;`?replication=1&offset=1` → 整读原文


### Requirement: router 逻辑窗口流读
router SHALL 提供 `get_object_range_stream(key, start, len)`:普通对象走数据面窗口参数(R=1);拼装对象 SHALL 仅取覆盖片并对边界片裁边(片完整性核对入流,短读响亮);已删/已过期 SHALL 回 NotFound;调用方 SHALL 保证窗口落在逻辑对象内(网关以 `stat_object` 逻辑尺寸为 416 基准)。

#### Scenario: 拼装对象窗口只取覆盖片
- **WHEN** 17 MiB / 5 片拼装对象请求窗口 [5 MiB, 7 MiB)
- **THEN** 只取第 2、3 片(边界片裁边),返回字节 == 原文切片;拼装读计入指标


### Requirement: REST `/blobs` Range 语义(Ambry 对齐)
frontend `GET /blobs/{id}` SHALL 支持 `Range` 头单区间语义:可满足 → 206 + `Content-Range` + `Accept-Ranges: bytes`;首 byte 越界/空对象/`bytes=-0` → 416 + `Content-Range: bytes */size`;无头/语法坏/多区间/非 bytes 单位 → 忽略回 200 全对象;拼装对象的窗口 SHALL 只触达覆盖片;`HEAD` 带 Range SHALL 同样回 206(无 body)。

#### Scenario: REST Range 矩阵
- **WHEN** GET 带 `Range: bytes=100000-120000` / `bytes=-1000` / `bytes=999999-` / `bytes=0-1,3-4`
- **THEN** 分别 206(字节恒等)/ 206(尾窗)/ 416(`bytes */size`)/ 200(忽略多区间回全对象);HEAD 带 Range 回 206 无 body

