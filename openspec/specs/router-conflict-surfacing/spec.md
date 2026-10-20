# router-conflict-surfacing Specification

## Purpose
TBD - created by archiving change router-conflict-surfacing. Update Purpose after archive.
## Requirements
### Requirement: Put 冲突在仲裁失败时确定性透传
Router SDK 的写路径(keyed put 与 auto-keyed/file 形态)在多数派未达、且**已收集的全部副本失败均为 already-exists 类(409)** 时,SHALL 返回 `Error::AlreadyExists`(携带首个报告冲突的副本地址),而不是把该确定性失败折叠进 `Error::Quorum`。部分失败为 409、部分为其它错误(网络/静默/507)时,SHALL 维持 `Error::Quorum`(保守语义:判定不了就按暂时性失败处理)。409 SHALL NOT 计入多数派 ack(冲突 ≠ 本次数据落位)。

auto-keyed 路径对 `AlreadyExists` SHALL NOT 做 507 式换号重试 —— 新号撞旧号意味着号被复用,属必须暴露的严重信号。

#### Scenario: 同 key 二次 keyed put
- **WHEN** 一个 key 的首次写入在 RF=2 三副本上达到多数派后,对同 key 再次写入
- **THEN** 已有该 key 的副本报 409、未达多数派,SDK 返回 `Error::AlreadyExists`,不返回 Quorum

#### Scenario: 混合失败仍按 Quorum
- **WHEN** 多数派未达且失败集中 409 与网络错误并存
- **THEN** SDK 返回 `Error::Quorum`(重试安全的暂时性失败语义)

#### Scenario: 冲突不算 ack
- **WHEN** 某副本对写回 409 而其他副本成功
- **THEN** 409 不计入多数派计数;多数派是否达成只数成功 ack

### Requirement: Frontend 网关将写冲突映射为 409
HTTP 网关的 REST 错误映射 SHALL 将 router 的 `Error::AlreadyExists` 映射为 `409 CONFLICT`,与 store 数据面对 key 冲突的状态码一致;不得沿用暂时性失败的 503。

#### Scenario: 冲突写透传成 409 到调用方
- **WHEN** 网关收到 keyed 写路径回传的 `Error::AlreadyExists`
- **THEN** REST 响应状态码为 409(CONFLICT)

