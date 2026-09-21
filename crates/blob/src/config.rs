//! `StoreConfig`:store 模块的类型化配置(对齐 Ambry `StoreConfig` 的单机引擎子集)。
//!
//! 范式见 [`docs/design/common.md` §1];完整清单见 [`docs/design/store.md` §10]。
//! 只收录**当前引擎真实消费**的项:段大小、活跃索引内存条目上限。总容量是 per-replica 属性
//! (对齐 Ambry:`capacityInBytes` 由 replica 传入、不在 `StoreConfig`),故不在此,由构造参数传入。

use config::{at_least, Result, Validate};
use serde::Deserialize;

/// store 引擎配置。字段名即 TOML key(在 `[store]` 表下)。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    /// 单个 log 段大小(字节)。对齐 Ambry `store.segment.size.in.bytes`。
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
            segment_size_bytes: 8 * 1024 * 1024 * 1024,
            index_max_inmem_elements: 10_000,
            io_error_count_to_trigger_shutdown: 0,
        }
    }
}

impl Validate for StoreConfig {
    fn validate(&self) -> Result<()> {
        at_least("store.segment_size_bytes", self.segment_size_bytes, 1)?;
        at_least(
            "store.index_max_inmem_elements",
            self.index_max_inmem_elements,
            1,
        )?;
        Ok(())
    }
}
