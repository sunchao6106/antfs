//! `StoreServerMetrics`:数据节点打点(接 common/metric 的 `Registry`)。

use antfs_metric::{
    counter, counter_vec, gauge, gauge_vec, histogram_vec, latency_seconds, latency_seconds_vec,
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, Result,
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
    /// **每盘**在途数据面请求数,按 `disk_id` × `op` 分(与全局 `in_flight` 同口径,只是多了盘维度)。
    ///
    /// 引擎 IO 全是同步阻塞且共用同一个进程级阻塞池:一块盘开始堆积(或盘 hang 住),在它上面就
    /// 只增不减 —— 这是"该盘出事了"最直接、也最不依赖任何超时判定的现网信号。
    ///
    /// **UI 上这一列就是"排队深度"的代偿**:真正的池排队深度要 `--cfg tokio_unstable`,而每盘有界池
    /// (N4)落地后队列归我们自己管,届时换真值(见 `server.rs` 里那段取舍记录)。
    pub disk_in_flight: IntGaugeVec,
    /// 落在该盘上的**数据面请求数**,按 `disk_id` × `op` 分(`rate()` 即该盘 IOPS)。
    ///
    /// 口径 = 请求(不是磁盘扇区 IO),且只覆盖数据面;引擎内部 IO(flush / compaction / 入向复制
    /// apply)不在此列 —— 它们随 N4 每盘池归口到同一条路径时再计入,见 `docs/design/store.md` §12.11。
    pub disk_requests: IntCounterVec,
    /// 该盘上**成功**请求的字节数,按 `disk_id` × `op` 分(`rate()` 即该盘带宽)。
    ///
    /// 与 `put_bytes_total` / `get_bytes_total` 同口径:只在成功路径计,失败/早退不计。
    pub disk_bytes: IntCounterVec,
    /// 落在该盘上的请求**端到端**延迟,按 `disk_id` × `op` 分(口径与 `request_duration` 一致,
    /// 含阻塞池排队 —— 它是"用户看到的这块盘的 RT",不是纯 syscall 耗时)。
    pub disk_request_duration: HistogramVec,
    /// 该盘是否有一发探测**卡在盘里未归**(1 = 卡住)。
    ///
    /// 只为 `disk_health_enabled` 的部署服务(未开探测时恒 0)。为 1 说明探测线程收不回来,后续轮次
    /// 已停止叠加(见 `health.rs` 模块注释)。
    pub disk_probe_stuck: IntGaugeVec,
    /// 因上一发探测未归而**跳过**探测的累计轮次数(× 探测周期 ≈ 该盘卡住的时长)。
    pub disk_probe_skipped: IntCounterVec,
    /// 端到端耗时超过 `disk_health_timeout_secs` 的数据面请求数,按 `disk_id` × `op` 分。
    ///
    /// 口径是**端到端**(handler 入口 → 引擎完成,**含阻塞池排队**),不是"盘自己慢":它回答"哪块盘
    /// 在拖长尾、多久拖一次",用于标定每盘隔离/看门狗的强度。真正的"盘慢"(syscall 耗时)要看每盘
    /// 看门狗落地时单独立的那组指标。
    pub disk_slow_requests: IntCounterVec,
    /// 每盘池的 **worker 线程数**(建池时定死;= `pool_size_by_dir` 或 `pool_shared_size` 均分的结果)。
    pub disk_pool_workers: IntGaugeVec,
    /// 每盘池**已准入未完成**的任务数(含排队中)。逼近 `workers + pool_queue_size` = 该盘正在被压满。
    pub disk_pool_in_flight: IntGaugeVec,
    /// 每盘池**已准入但还没被 worker 取走**的任务数 —— 这才是**真正的每盘排队深度**
    /// (与 `disk_in_flight_requests` 那个代偿口径不同,后者含正在执行的)。
    pub disk_pool_queued: IntGaugeVec,
    /// 每盘池拒绝掉的提交数,按 `disk_id` × `op` × `reason`(`saturated` 池满超时 / `unknown_disk` /
    /// `closed`)。**非零且持续 = 这块盘已经供不上,该加池或查盘**。
    pub disk_pool_rejected: IntCounterVec,
    /// 每盘**闭包内**测得的单次 IO 时长(syscall,**不含**池排队),按 `disk_id` × `op` 分。
    ///
    /// 与 `disk_request_duration`(端到端,含排队)**刻意分开**:后者回答"用户看到的这块盘多慢",
    /// 前者回答"这盘自己快不快"。**只有后者能做隔离判据** —— 用端到端口径会把"池忙"判成"盘坏"
    /// (design §5.1)。
    pub disk_io_duration: HistogramVec,
    /// 看门狗判定的单次 IO 超期次数,按 `disk_id` 分(超期只算**一次坏观测**,是否摘盘由迟滞决定)。
    pub disk_io_watchdog_trips: IntCounterVec,
    /// 该盘当前状态,按 `disk_id` × `state` 分(`state` ∈ [`crate::disk_health::REPORTED_STATES`])。
    ///
    /// **整族每次摆平**(当前值 1、其余 0):只置 1 不置 0 会让状态迁移后旧值永远留 1,看板上出现
    /// "一块盘同时健康又坏掉"。
    pub disk_health_state: IntGaugeVec,
    /// 该盘可服务性的状态迁移次数,**按 `from` × `to` 分**。
    ///
    /// 非零是**正常**的(坏盘被隔离、恢复后复位);**频繁迁移**说明迟滞阈值定得太紧。
    pub disk_health_transitions: IntCounterVec,
    /// 因盘不可服务(或池满)而被跳过的后台工作,按 `disk_id` × `task`(`flush`/`compaction`/
    /// `replication`)分。
    ///
    /// 它让"坏盘被跳过"这件事**可见** —— 否则表现为"这块盘上的副本悄悄不推进了"。
    pub disk_worker_skipped: IntCounterVec,
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
    /// 拉复制游标被**重定位**的次数,按 `reason`(incarnation_mismatch/segment_generation_mismatch/
    /// segment_missing/offset_out_of_range/no_progress)各计一次。对齐 Ambry `revalidateStoreFindToken`
    /// +resetKey:非零是**正常**的(说明发生过 compaction/重建后的续拉),持续增长才值得看。
    pub replication_cursor_resets: IntCounterVec,
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
    pub compaction_policy_used: IntCounterVec,
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
            disk_in_flight: gauge_vec(
                registry,
                "store_server_disk_in_flight_requests",
                "in-flight data-plane requests per disk (engine IO shares one process-wide blocking pool)",
                &["disk_id", "op"],
            )?,
            disk_requests: counter_vec(
                registry,
                "store_server_disk_requests_total",
                "data-plane requests landed on each disk (rate = per-disk IOPS)",
                &["disk_id", "op"],
            )?,
            disk_bytes: counter_vec(
                registry,
                "store_server_disk_bytes_total",
                "bytes of successful requests per disk (rate = per-disk bandwidth)",
                &["disk_id", "op"],
            )?,
            disk_request_duration: latency_seconds_vec(
                registry,
                "store_server_disk_request_duration_seconds",
                "end-to-end latency of requests per disk (incl. blocking-pool queueing)",
                &["disk_id", "op"],
            )?,
            disk_probe_stuck: gauge_vec(
                registry,
                "store_server_disk_probe_stuck",
                "disk has a probe thread stuck inside the device (1 = stuck)",
                &["disk_id"],
            )?,
            disk_probe_skipped: counter_vec(
                registry,
                "store_server_disk_probe_skipped_total",
                "probe rounds skipped because the previous probe has not returned",
                &["disk_id"],
            )?,
            disk_slow_requests: counter_vec(
                registry,
                "store_server_disk_slow_requests_total",
                "data-plane requests slower than disk_health_timeout_secs (end-to-end, incl. blocking-pool queueing)",
                &["disk_id", "op"],
            )?,
            disk_pool_workers: gauge_vec(
                registry,
                "store_server_disk_pool_workers",
                "per-disk pool worker threads (fixed at startup)",
                &["disk_id"],
            )?,
            disk_pool_in_flight: gauge_vec(
                registry,
                "store_server_disk_pool_in_flight",
                "per-disk pool admitted-but-unfinished tasks (running + queued)",
                &["disk_id"],
            )?,
            disk_pool_queued: gauge_vec(
                registry,
                "store_server_disk_pool_queued",
                "per-disk pool tasks admitted but not yet picked up by a worker (true queue depth)",
                &["disk_id"],
            )?,
            disk_pool_rejected: counter_vec(
                registry,
                "store_server_disk_pool_rejected_total",
                "submissions rejected by a per-disk pool (reason: saturated/unknown_disk/closed)",
                &["disk_id", "op", "reason"],
            )?,
            // 桶刻意铺到 120s:看门狗 deadline(默认 30s)就是要看的长尾,而 Prometheus 默认桶在 10s
            // 就封顶了 —— 那样"盘慢到什么程度"正好看不见。
            disk_io_duration: histogram_vec(
                registry,
                "store_server_disk_io_duration_seconds",
                "single engine IO duration measured INSIDE the blocking closure (syscall only, excl. pool queueing)",
                &["disk_id", "op"],
                vec![
                    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
                ],
            )?,
            disk_io_watchdog_trips: counter_vec(
                registry,
                "store_server_disk_io_watchdog_trips_total",
                "per-disk IO watchdog deadline breaches (one breach = one bad observation, hysteresis decides)",
                &["disk_id"],
            )?,
            disk_health_state: gauge_vec(
                registry,
                "store_server_disk_health_state",
                "per-disk serviceability state (1 = current, 0 = others; states: HEALTHY/UNAVAILABLE/UNKNOWN/failure)",
                &["disk_id", "state"],
            )?,
            disk_health_transitions: counter_vec(
                registry,
                "store_server_disk_health_transitions_total",
                "per-disk availability transitions by from/to (frequent transitions = hysteresis thresholds too tight)",
                &["disk_id", "from", "to"],
            )?,
            disk_worker_skipped: counter_vec(
                registry,
                "store_server_disk_worker_skipped_total",
                "background work skipped because the disk is unavailable or its pool is full",
                &["disk_id", "task"],
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
            replication_cursor_resets: counter_vec(
                registry,
                "store_server_replication_cursor_resets_total",
                "pull-replication cursor revalidations (reasons: incarnation_mismatch/segment_generation_mismatch/segment_missing/offset_out_of_range/no_progress)",
                &["reason"],
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
            compaction_policy_used: counter_vec(
                registry,
                "store_server_compaction_policy_used_total",
                "compaction executions by policy actually used",
                &["policy"],
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
