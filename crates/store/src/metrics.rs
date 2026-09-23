//! `StoreServerMetrics`:数据节点打点(接 common/metric 的 `Registry`)。

use antfs_metric::{
    counter, counter_vec, gauge, gauge_vec, latency_seconds, latency_seconds_vec, Histogram,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, Result,
};

use crate::replica::{ReplicaState, STATE_BOOTSTRAP};

/// 小 blob 上界(50 KiB):`len < 50 KiB` 记 `small`。
/// 对齐 Ambry `ServerMetrics` 的 blob 大小分类边界(`smallBlob` = 50 KB)。
pub const SMALL_BLOB_MAX_BYTES: u64 = 50 * 1024;
/// 中 blob 上界(1 MiB):`len < 1 MiB` 记 `medium`,否则 `large`。
/// 对齐 Ambry `ServerMetrics` 的 blob 大小分类边界(`mediumBlob` = 1 MB)。
pub const MEDIUM_BLOB_MAX_BYTES: u64 = 1024 * 1024;

/// blob 请求按对象大小分桶(对齐 Ambry 的大小三分类):`small` / `medium` / `large`。
/// 边界取「上界开区间」:50 KiB 本身算 `medium`,1 MiB 本身算 `large`。
pub fn size_bucket(len: u64) -> &'static str {
    if len < SMALL_BLOB_MAX_BYTES {
        "small"
    } else if len < MEDIUM_BLOB_MAX_BYTES {
        "medium"
    } else {
        "large"
    }
}

/// 数据节点指标集,构造时注册进传入 `Registry`。
#[derive(Clone)]
pub struct StoreServerMetrics {
    pub put_total: IntCounter,
    pub get_total: IntCounter,
    pub put_bytes: IntCounter,
    pub get_bytes: IntCounter,
    pub replicas: IntGauge,
    /// 其中**尚未追平**的本地副本数(`current_state == "BOOTSTRAP"`,正在同步中)。
    /// 与 `replicas` 同步刷新(见 [`StoreServerMetrics::set_replica_gauges`])。
    pub replicas_bootstrapping: IntGauge,
    /// 数据面错误,按 `kind` 分类(io / unavailable / join_error)。
    pub errors: IntCounterVec,
    /// 数据面请求延迟(handler 入口 → spawn_blocking 完成,含排队),按 `op` 分。
    pub request_duration: HistogramVec,
    /// 成功 blob 请求的延迟,按 `op`(put/get)× `size`(small/medium/large)分桶
    /// (对齐 Ambry 只在 `NoError` 时按对象大小计数;早退/错误路径不 observe)。
    pub blob_request_duration: HistogramVec,
    /// 在途数据面请求数,按 `op` 分。
    pub in_flight: IntGaugeVec,
    /// 请求静默(RequestControl)拒掉的请求数,按 `op` × `reason`(目前仅 `disabled`)分。
    pub rejected_requests: IntCounterVec,
    /// 手动 compact 端点调用次数。
    pub manual_compactions: IntCounter,
    /// 经 `DROPPED` 指令**实际删除**的本地副本数(重复指令被幂等忽略,不计数)。
    pub replicas_dropped: IntCounter,
    /// 拉复制是否被运维暂停(1 = 暂停本节点主动拉,0 = 正常);见 `/admin/replication-control`。
    pub replication_paused: IntGauge,
    /// 手动 compaction 的异步任务数,按 `state`(running/done/failed)各计一次。
    pub compaction_jobs: IntCounterVec,
    /// 本地完整性校验(`GET /partitions/{pid}/verify`)调用次数。
    pub verify_runs: IntCounter,
    /// 校验中发现的坏记录总数(CRC/解码失败)。
    pub verify_bad_records: IntCounter,
    /// 校验单次耗时(秒)。
    pub verify_duration: Histogram,
    /// scrub 摘要(`GET /partitions/{pid}/scrub-report`)调用次数。
    pub scrub_reports: IntCounter,
    pub heartbeats: IntCounter,
    pub mutations: IntCounter,
    pub replication_rounds: IntCounter,
    pub replication_applied: IntCounter,
    pub replication_bytes: IntCounter,
    pub replication_errors: IntCounter,
    pub bootstrap_segments: IntCounter,
    pub bootstrap_bytes: IntCounter,
    pub flushes: IntCounter,
    pub compactions: IntCounter,
}

impl StoreServerMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(StoreServerMetrics {
            put_total: counter(registry, "store_server_put_total", "blob put requests")?,
            get_total: counter(registry, "store_server_get_total", "blob get requests")?,
            put_bytes: counter(registry, "store_server_put_bytes_total", "bytes written")?,
            get_bytes: counter(registry, "store_server_get_bytes_total", "bytes read")?,
            replicas: gauge(
                registry,
                "store_server_replicas",
                "local partition replicas",
            )?,
            replicas_bootstrapping: gauge(
                registry,
                "store_server_replicas_bootstrapping",
                "local replicas not yet caught up (current_state=BOOTSTRAP)",
            )?,
            errors: counter_vec(
                registry,
                "store_server_errors_total",
                "data-plane errors",
                &["kind"],
            )?,
            request_duration: latency_seconds_vec(
                registry,
                "store_server_request_duration_seconds",
                "data-plane request latency (handler entry to engine completion, incl. queueing)",
                &["op"],
            )?,
            in_flight: gauge_vec(
                registry,
                "store_server_in_flight_requests",
                "in-flight data-plane requests",
                &["op"],
            )?,
            blob_request_duration: latency_seconds_vec(
                registry,
                "store_server_blob_request_duration_seconds",
                "successful blob request latency by object size class (put/get)",
                &["op", "size"],
            )?,
            rejected_requests: counter_vec(
                registry,
                "store_server_rejected_requests_total",
                "data-plane requests rejected before reaching the engine",
                &["op", "reason"],
            )?,
            manual_compactions: counter(
                registry,
                "store_server_manual_compactions_total",
                "manual compaction endpoint calls",
            )?,
            replicas_dropped: counter(
                registry,
                "store_server_replicas_dropped_total",
                "local replicas whose data was dropped on master DROPPED directive",
            )?,
            replication_paused: gauge(
                registry,
                "store_server_replication_paused",
                "pull-replication paused by operator (1 = paused)",
            )?,
            compaction_jobs: counter_vec(
                registry,
                "store_server_compaction_jobs_total",
                "async manual compaction jobs by state",
                &["state"],
            )?,
            verify_runs: counter(
                registry,
                "store_server_verify_runs_total",
                "local integrity verify calls",
            )?,
            verify_bad_records: counter(
                registry,
                "store_server_verify_bad_records_total",
                "bad records found by local integrity verify",
            )?,
            verify_duration: latency_seconds(
                registry,
                "store_server_verify_duration_seconds",
                "single local integrity verify duration",
            )?,
            scrub_reports: counter(
                registry,
                "store_server_scrub_reports_total",
                "scrub report endpoint calls",
            )?,
            heartbeats: counter(
                registry,
                "store_server_heartbeats_total",
                "heartbeat directives received",
            )?,
            mutations: counter(registry, "store_server_mutations_total", "delete mutations")?,
            replication_rounds: counter(
                registry,
                "store_server_replication_rounds_total",
                "pull-replication rounds run",
            )?,
            replication_applied: counter(
                registry,
                "store_server_replication_applied_total",
                "records applied from peers",
            )?,
            replication_bytes: counter(
                registry,
                "store_server_replication_bytes_total",
                "bytes applied from peers",
            )?,
            replication_errors: counter(
                registry,
                "store_server_replication_errors_total",
                "pull-replication errors",
            )?,
            bootstrap_segments: counter(
                registry,
                "store_server_bootstrap_segments_total",
                "sealed segments physically copied on bootstrap",
            )?,
            bootstrap_bytes: counter(
                registry,
                "store_server_bootstrap_bytes_total",
                "bytes physically copied on bootstrap",
            )?,
            flushes: counter(
                registry,
                "store_server_flushes_total",
                "local-worker flushes run",
            )?,
            compactions: counter(
                registry,
                "store_server_compactions_total",
                "local-worker segment compactions run",
            )?,
        })
    }

    /// 刷新副本相关的两个 gauge:`store_server_replicas`(本地副本数)与
    /// `store_server_replicas_bootstrapping`(其中尚未追平的数量)。
    ///
    /// 入参是调用方**已经取好**的副本实况(心跳每拍本来就要取一次,见 `heartbeat.rs`),避免重复取。
    /// 刷新点:心跳每拍 + 心跳指令处理后(副本集/实然态可能刚变)+ 复制 loop 每 tick(刚转 `ACTIVE`
    /// 立刻反映在指标上,不必等下一拍心跳)。
    pub fn set_replica_gauges(&self, states: &[ReplicaState]) {
        let bootstrapping = states
            .iter()
            .filter(|s| s.current_state == STATE_BOOTSTRAP)
            .count();
        self.replicas.set(states.len() as i64);
        self.replicas_bootstrapping.set(bootstrapping as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 分桶边界:50 KiB / 1 MiB 均为**上界开区间**(等于边界值落到更大的桶)。
    #[test]
    fn size_bucket_boundaries() {
        let small_max = SMALL_BLOB_MAX_BYTES;
        let medium_max = MEDIUM_BLOB_MAX_BYTES;
        assert_eq!(size_bucket(0), "small");
        assert_eq!(size_bucket(small_max - 1), "small");
        assert_eq!(size_bucket(small_max), "medium");
        assert_eq!(size_bucket(small_max + 1), "medium");
        assert_eq!(size_bucket(medium_max - 1), "medium");
        assert_eq!(size_bucket(medium_max), "large");
        assert_eq!(size_bucket(medium_max + 1), "large");
        assert_eq!(size_bucket(u64::MAX), "large");
    }

    /// 边界常量本身对齐 Ambry 的 50 KB / 1 MB 分类。
    #[test]
    fn size_class_thresholds_match_ambry() {
        assert_eq!(SMALL_BLOB_MAX_BYTES, 51_200);
        assert_eq!(MEDIUM_BLOB_MAX_BYTES, 1_048_576);
    }
}
