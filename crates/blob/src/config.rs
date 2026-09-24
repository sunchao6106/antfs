//! `StoreConfig`:store 模块的类型化配置(对齐 Ambry `StoreConfig` 的单机引擎子集)。
//!
//! 范式见 [`docs/design/common.md` §1];完整清单见 [`docs/design/store.md` §10]。
//! 只收录**当前引擎真实消费**的项:段大小、活跃索引内存条目上限。总容量是 per-replica 属性
//! (对齐 Ambry:`capacityInBytes` 由 replica 传入、不在 `StoreConfig`),故不在此,由构造参数传入。

use antfs_config::{at_least, in_range, Result, Validate};
use serde::Deserialize;

/// 单个 log 段容量的硬上界:4 GiB(唯一真源在 [`crate::log_segment::MAX_CAPACITY`])。
///
/// 这不是"调优建议"而是**格式不变量**:段内偏移必须放进索引条目的 u32 字段
///([`LogOffset::pack`](crate::types::LogOffset::pack)),且"单条记录 ≤ 段 − 64 < `u32::MAX`"
/// 让记录内的 u32 `data_len` 结构性地不可能回绕。
pub const MAX_SEGMENT_SIZE_BYTES: u64 = crate::log_segment::MAX_CAPACITY;

/// store 引擎配置。字段名即 TOML key(在 `[store]` 表下)。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    /// 单个 log 段大小(字节)。对齐 Ambry `store.segment.size.in.bytes`。
    ///
    /// **默认 4 GiB**(即格式硬上界 [`MAX_SEGMENT_SIZE_BYTES`]):记录内 `data_len` 是 u32,段 ≤ 4 GiB 时
    /// 单条记录必然 ≤ `段 − 102 < u32::MAX`,"长度回绕写坏记录"这件事在**结构上**不可能发生
    /// (`record_data_len` 的显式校验是第二道保险,供把段配大时兜底)。取到上界的代价:
    /// `is_full()`(剩余 < 一个段,见 `BlobStore::is_full`)最多把 4 GiB 判为不可写。
    pub segment_size_bytes: u64,
    /// 活跃索引常驻内存的最大条目数,达阈值即封存。对齐 Ambry `store.index.max.number.of.inmem.elements`。
    pub index_max_inmem_elements: usize,
    /// 连续 IO 错误达此值则 store 自关(被动磁盘检测)。**0 = 禁用**(对齐 Ambry 默认
    /// `store.io.error.count.to.trigger.shutdown = MAX_VALUE` 的"默认不触发")。
    pub io_error_count_to_trigger_shutdown: u32,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            // 4 GiB:使"单条记录 ≤ 段 − 102 < u32::MAX"成为结构性不变量(见字段注释)。
            segment_size_bytes: 4 * 1024 * 1024 * 1024,
            index_max_inmem_elements: 10_000,
            io_error_count_to_trigger_shutdown: 0,
        }
    }
}

impl Validate for StoreConfig {
    fn validate(&self) -> Result<()> {
        in_range(
            "store.segment_size_bytes",
            self.segment_size_bytes,
            1,
            MAX_SEGMENT_SIZE_BYTES,
        )?;
        at_least(
            "store.index_max_inmem_elements",
            self.index_max_inmem_elements,
            1,
        )?;
        Ok(())
    }
}
