//! `RouterMetrics`:router(客户端 SDK)的统计打点(接 common/metric 的 `Registry`)。
//!
//! DI 见 [`docs/design/common.md` §2.2];清单见 [`docs/design/router.md`];改名字即改文档(CLAUDE.md §7)。

use antfs_metric::{
    counter, counter_vec, gauge, histogram_vec, IntCounter, IntCounterVec, IntGauge, Registry,
    Result,
};

/// router 指标集,构造时注册进传入的 `Registry`。
#[derive(Clone)]
pub struct RouterMetrics {
    /// 向 master 取号段的次数(counter)。
    pub id_segments: IntCounter,
    /// 取到的 blobId 总数(counter)。
    pub id_segment_ids: IntCounter,
    /// blob 写请求数,label `outcome`(ok / 各种 [`crate::Error::kind`])。
    pub put_total: IntCounterVec,
    /// 写入的字节数(counter,只计成功)。
    pub put_bytes: IntCounter,
    /// 单次写(`put`)耗时直方图(秒),label `outcome`。
    pub put_seconds: antfs_metric::HistogramVec,
    /// 单个副本写失败数,label `kind`(分类同 [`crate::Error::kind`])。
    pub put_replica_errors: IntCounterVec,
    /// 因分区写满(全副本 507)而换分区重试的次数(counter)。
    pub router_put_store_full_retries: IntCounter,
    /// 压缩三闸判定计数,label `outcome`(kept / too_small / low_ratio / disabled)。
    pub compression_attempts: IntCounterVec,
    /// 压缩节省的字节数(orig − stored;只计 kept)。
    pub compression_saved_bytes: IntCounter,
    /// 读拓扑重拉次数,label `reason`(ttl = 周期过期 / evict = 全副本不可达驱逐)。
    pub topology_refreshes: IntCounterVec,
    /// 号段后台预取失败次数(成功不打:成功的预取自然体现在耗尽次数下降)。
    pub prefetch_errors: IntCounter,
    /// blob 读请求数,label `outcome`。
    pub get_total: IntCounterVec,
    /// 读出的字节数(counter,只计成功)。
    pub get_bytes: IntCounter,
    /// 其它数据面操作数,label `op`(delete/stat)+ `outcome`。
    pub mutation_total: IntCounterVec,
    /// 后台刷新可写分区列表的次数,label `outcome`(ok / error)。
    pub partition_refresh_total: IntCounterVec,
    /// 本地持有号段的**分区数**(= 当前可写分区数;gauge)。
    pub writable_partitions: IntGauge,
}

impl RouterMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(RouterMetrics {
            id_segments: counter(
                registry,
                "router_id_segments_total",
                "blob id segments fetched from master",
            )?,
            id_segment_ids: counter(
                registry,
                "router_id_segment_ids_total",
                "blob ids received from master (segment reservations included)",
            )?,
            put_total: counter_vec(
                registry,
                "router_put_total",
                "blob put requests by outcome",
                &["outcome"],
            )?,
            put_bytes: counter(
                registry,
                "router_put_bytes_total",
                "bytes written (successful puts)",
            )?,
            put_seconds: histogram_vec(
                registry,
                "router_put_seconds",
                "put latency (seconds) by outcome",
                &["outcome"],
                vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0],
            )?,
            put_replica_errors: counter_vec(
                registry,
                "router_put_replica_errors_total",
                "per-replica put failures by kind",
                &["kind"],
            )?,
            router_put_store_full_retries: counter(
                registry,
                "router_put_store_full_retries_total",
                "put retries after all replicas reported partition full (507)",
            )?,
            compression_attempts: counter_vec(
                registry,
                "router_compression_attempts_total",
                "SDK compression gate decisions per put",
                &["outcome"],
            )?,
            compression_saved_bytes: counter(
                registry,
                "router_compression_saved_bytes_total",
                "bytes saved by SDK compression (orig minus stored, kept only)",
            )?,
            topology_refreshes: counter_vec(
                registry,
                "router_topology_refreshes_total",
                "read-topology refreshes (ttl expiry or all-replica-unreachable eviction)",
                &["reason"],
            )?,
            prefetch_errors: counter(
                registry,
                "router_id_prefetch_errors_total",
                "background id-segment prefetch failures (spike-smoothing enhancement, not correctness)",
            )?,
            get_total: counter_vec(
                registry,
                "router_get_total",
                "blob get requests by outcome",
                &["outcome"],
            )?,
            get_bytes: counter(
                registry,
                "router_get_bytes_total",
                "bytes read (successful gets)",
            )?,
            mutation_total: counter_vec(
                registry,
                "router_mutation_total",
                "delete/stat requests by op and outcome",
                &["op", "outcome"],
            )?,
            partition_refresh_total: counter_vec(
                registry,
                "router_partition_refresh_total",
                "writable-partition list refreshes by outcome",
                &["outcome"],
            )?,
            writable_partitions: gauge(
                registry,
                "router_writable_partitions",
                "partitions currently holding a cached id segment",
            )?,
        })
    }

    /// 未接入外部 `Registry` 时的兜底(测试/裸构造):挂在丢弃的私有 `Registry` 上。
    pub fn disabled() -> Self {
        let registry = Registry::new();
        RouterMetrics::new(&registry).expect("fresh registry never has duplicate registrations")
    }
}
