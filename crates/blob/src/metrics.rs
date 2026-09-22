//! `StoreMetrics`:store 模块的统计打点(对齐 Ambry `StoreMetrics` 的 BlobStore 层子集)。
//!
//! DI 与多实例区分策略见 [`docs/design/common.md` §2.2];完整清单见 [`docs/design/store.md` §11]。
//!
//! - [`StoreMetrics`] 持有注册在 `Registry` 上的 `*Vec`(每个 `Registry` 只建一次),对齐 Ambry
//!   `StorageManager` 里 `new StoreMetrics(registry)`。
//! - [`StoreMetrics::for_store`] 用 label `store="<id>"` 取某个 store 的子 handle 集
//!   [`StoreMetricHandles`],存进该 `BlobStore`;多 store 共享同一 `Registry`、靠 label 区分。
//!
//! 本轮只接 BlobStore 层指标;Log/Index/过滤器/stats 内部打点待这些模块各自接入(见 store.md §11)。

use antfs_metric::{
    counter_vec, gauge_vec, latency_seconds_vec, Histogram, HistogramTimer, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, Result,
};

/// 注册在某个 `Registry` 上的 store 指标(带 `store` label 的 `*Vec`)。每 `Registry` 建一次,
/// 各 store 用 [`for_store`](Self::for_store) 取自己的子 handle。
#[derive(Clone)]
pub struct StoreMetrics {
    request_duration: HistogramVec,
    already_exists: IntCounterVec,
    compaction_reclaimed_bytes: IntCounterVec,
    compaction_duration: HistogramVec,
    hard_delete_total: IntCounterVec,
    hard_delete_duration: HistogramVec,
    used_capacity_bytes: IntGaugeVec,
    invalid_data_bytes: IntGaugeVec,
    segment_count: IntGaugeVec,
}

impl StoreMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(StoreMetrics {
            request_duration: latency_seconds_vec(
                registry,
                "store_request_duration_seconds",
                "per-operation request latency",
                &["store", "op"],
            )?,
            already_exists: counter_vec(
                registry,
                "store_already_exists_total",
                "put hit an existing key (put/delete tombstone)",
                &["store"],
            )?,
            compaction_reclaimed_bytes: counter_vec(
                registry,
                "store_compaction_reclaimed_bytes_total",
                "bytes reclaimed by compaction",
                &["store"],
            )?,
            compaction_duration: latency_seconds_vec(
                registry,
                "store_compaction_duration_seconds",
                "single compaction run duration",
                &["store"],
            )?,
            hard_delete_total: counter_vec(
                registry,
                "store_hard_delete_total",
                "objects scrubbed by hard delete",
                &["store"],
            )?,
            hard_delete_duration: latency_seconds_vec(
                registry,
                "store_hard_delete_duration_seconds",
                "hard delete run duration",
                &["store"],
            )?,
            used_capacity_bytes: gauge_vec(
                registry,
                "store_log_used_capacity_bytes",
                "log bytes used (live + dead)",
                &["store"],
            )?,
            invalid_data_bytes: gauge_vec(
                registry,
                "store_log_invalid_data_bytes",
                "dead (reclaimable) log bytes",
                &["store"],
            )?,
            segment_count: gauge_vec(
                registry,
                "store_log_segment_count",
                "number of log segments",
                &["store"],
            )?,
        })
    }

    /// 取某个 store 的子 handle 集(按 `store="<id>"` label 绑定)。廉价:`with_label_values` 返回克隆的子 handle。
    pub fn for_store(&self, store_id: &str) -> StoreMetricHandles {
        StoreMetricHandles {
            store_id: store_id.to_string(),
            request_duration: self.request_duration.clone(),
            already_exists: self.already_exists.with_label_values(&[store_id]),
            compaction_reclaimed_bytes: self
                .compaction_reclaimed_bytes
                .with_label_values(&[store_id]),
            compaction_duration: self.compaction_duration.with_label_values(&[store_id]),
            hard_delete_total: self.hard_delete_total.with_label_values(&[store_id]),
            hard_delete_duration: self.hard_delete_duration.with_label_values(&[store_id]),
            used_capacity_bytes: self.used_capacity_bytes.with_label_values(&[store_id]),
            invalid_data_bytes: self.invalid_data_bytes.with_label_values(&[store_id]),
            segment_count: self.segment_count.with_label_values(&[store_id]),
        }
    }
}

/// 某个 `BlobStore` 实例持有的、已绑定 `store` label 的子 handle 集。
#[derive(Clone)]
pub struct StoreMetricHandles {
    store_id: String,
    request_duration: HistogramVec,
    pub(crate) already_exists: IntCounter,
    pub(crate) compaction_reclaimed_bytes: IntCounter,
    pub(crate) compaction_duration: Histogram,
    pub(crate) hard_delete_total: IntCounter,
    pub(crate) hard_delete_duration: Histogram,
    pub(crate) used_capacity_bytes: IntGauge,
    pub(crate) invalid_data_bytes: IntGauge,
    pub(crate) segment_count: IntGauge,
}

impl StoreMetricHandles {
    /// 起一个操作计时器(drop 时记录耗时到 `store_request_duration_seconds{op=...}`)。
    pub(crate) fn op_timer(&self, op: &str) -> HistogramTimer {
        self.request_duration
            .with_label_values(&[&self.store_id, op])
            .start_timer()
    }

    /// 未接入外部 `Registry` 时的兜底(裸构造器/测试用):挂在一个丢弃的私有 `Registry` 上,
    /// 打点仍可自增,只是不被导出。
    pub(crate) fn disabled() -> Self {
        let registry = Registry::new();
        StoreMetrics::new(&registry)
            .expect("fresh registry never has duplicate registrations")
            .for_store("default")
    }
}
