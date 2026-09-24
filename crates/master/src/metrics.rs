//! `MasterMetrics`:master 节点的统计打点(接 common/metric 的 `Registry`)。
//!
//! DI 见 [`docs/design/common.md` §2.2];清单见 [`docs/design/cluster.md`](../../../docs/design/cluster.md)「监控项清单」。
//! master 是单例进程(非多 store 那种多实例),故用**无 label** 的裸 handle;唯一例外是
//! `replicas_by_disk`(需按 node_id × disk_id 分盘观测,用 label vec)。指标断言必须打在实际接入
//! 的这个 `Registry` 上(CLAUDE.md §6)。

use antfs_metric::{
    counter, counter_vec, gauge, gauge_vec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry, Result,
};

/// master 指标集,构造时注册进传入的 `Registry`。
#[derive(Clone)]
pub struct MasterMetrics {
    /// 当前 Raft term(gauge)。
    pub raft_term: IntGauge,
    /// 本节点是否为 leader(1/0)。
    pub is_leader: IntGauge,
    /// 状态机 apply 的命令条数(counter)。
    pub applied_commands: IntCounter,
    /// 已注册节点数(gauge)。
    pub registered_nodes: IntGauge,
    /// 收到的心跳数(counter)。
    pub heartbeats: IntCounter,
    /// 被准入校验拒绝的请求数(counter)。
    pub admission_rejected: IntCounter,
    /// 成功创建的分区数(counter)。
    pub partitions_created: IntCounter,
    /// 建分区失败数(counter)。
    pub partition_create_failed: IntCounter,
    /// 下发的副本状态转移指令数(counter,M3)。
    pub transitions_issued: IntCounter,
    /// 触发的再复制次数(counter,M3)。
    pub rereplications: IntCounter,
    /// 移除的副本数(counter,M4 decommission evacuation)。
    pub replicas_removed: IntCounter,
    /// 手动 RepairPartition 触发数(counter)。
    pub repairs_triggered: IntCounter,
    /// 迁移完成后移除的迁出源副本数(counter,MoveReplica 的"后删")。
    pub replicas_moved: IntCounter,
    /// 当前欠副本(健康副本数 < RF)的分区数(gauge,反映集群健康)。
    pub under_replicated_partitions: IntGauge,
    /// AssignPartition 请求总数(counter,含失败)。
    pub assign_requests: IntCounter,
    /// AssignPartition 失败数(counter,无可写分区等 ok=false 或错误返回)。
    pub assign_failures: IntCounter,
    /// LookupPartition 读路由查询数(counter)。
    pub lookup_requests: IntCounter,
    /// GetBlobIds 发号请求数(counter,含失败)。
    pub blob_ids_requests: IntCounter,
    /// 已发出的 blobId 个数(counter,= 各次实发 count 之和)。
    pub blob_ids_allocated: IntCounter,
    /// GetBlobIds 失败数(counter:非 leader/无可写分区/objectId 耗尽/raft 提交失败)。
    pub blob_ids_failures: IntCounter,
    /// 为抬水位而做的 Raft 提交次数(counter;摊薄效果 = 发放数 / 该值 ≈ 号段 × 倍数)。
    pub id_alloc_raft_commits: IntCounter,
    /// 换号段时未发完而被丢弃的号数(counter,即"空洞";按 D3 无害,仅作观测)。
    pub id_alloc_holes: IntCounter,
    /// RW 态分区数(gauge,reconciler 周期刷新,仅 leader 更新)。
    pub partitions_rw: IntGauge,
    /// RO 态分区数(gauge,同上)。
    pub partitions_ro: IntGauge,
    /// SEALED 态分区数(gauge,同上)。
    pub partitions_sealed: IntGauge,
    /// 可写容量上界 = RW 分区数 × partition_capacity_bytes(gauge)。
    pub writable_capacity_bytes: IntGauge,
    /// 心跳判活的节点数(gauge)。
    pub nodes_up: IntGauge,
    /// 集群磁盘余量合计(gauge,心跳上报软状态汇总)。
    pub cluster_free_bytes: IntGauge,
    /// 集群磁盘已用 = Σcapacity − Σfree(gauge)。
    pub cluster_used_bytes: IntGauge,
    /// 所有副本上报 lag_bytes 的最大值(gauge)。
    pub max_replica_lag_bytes: IntGauge,
    /// 健康副本数 ≤ RF/2 的分区数(gauge,对齐 Ambry isMajorityReplicasDown)。
    pub majority_replicas_down_partitions: IntGauge,
    /// 是否处于维护模式(1/0;reconciler 周期 set,切主自然跟随)。
    pub maintenance: IntGauge,
    /// 可写(RW)分区实际占用汇总(gauge;口径见 `soft::SoftState::sum_used_bytes`)。
    pub writable_used_bytes: IntGauge,
    /// 可写空间余量 = `writable_capacity_bytes` − `writable_used_bytes`(饱和减)。
    pub writable_free_bytes: IntGauge,
    /// 每块盘承载的副本数(gauge vec;labels: node_id / disk_id)。
    pub replicas_by_disk: IntGaugeVec,
    /// 每盘副本数的 max − min(gauge;无盘 = 0),均衡度信号。
    pub disk_skew: IntGauge,
    /// 主动探测报出不健康的盘数(gauge;`health` 非空且 ≠ `HEALTHY`;探测未启用的盘不计)。
    pub unhealthy_disks: IntGauge,
    /// 因"副本所在盘将满"被自动置为 RO 的分区数(counter;只置不回,恢复靠运维。
    /// 盘满止损,对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage`)。
    pub partitions_forced_ro: IntCounter,
    /// 待删源副本对数(gauge,`pending_drop`:placement 已移除、等源节点确认后下发 `DROPPED`)。
    pub pending_drops: IntGauge,
    /// 下发的 `DROPPED` 指令数(counter,迁移/下线完成后删源目录)。
    pub dropped_directives: IntCounter,
    /// 当前均衡计划内 pending 的搬迁数(gauge,reconciler 周期刷新,仅 leader;无计划 = 0)。
    pub balance_pending: IntGauge,
    /// 当前均衡计划内在途(已受理待摘源)的搬迁数(gauge,同上)。
    pub balance_in_flight: IntGauge,
    /// 均衡计划内已完成的搬迁数(counter;以"观测到进入 done"计,切主后对已在终态的条目不重复计)。
    pub balance_moves: IntCounter,
    /// 均衡计划内判败的搬迁数(counter,口径同上)。
    pub balance_failed: IntCounter,
    /// 判定节点 down 的次数(counter;**迟滞后**才计 —— 便于确认判活没被一次心跳抖动污染)。
    ///
    /// `master.node_down_debounce_ticks = 0`(关迟滞)时它就是"每拍超时即计一次",会持续增长;
    /// 开了迟滞后只在**连击达标那一刻**计一次(持续 down 不会每拍重复计)。
    pub node_down: IntCounter,
    /// 因每 tick 预算(`master.rereplication_max_per_tick`)被推后到下轮的欠副本分区数(gauge)。
    ///
    /// 它让"我们正在限速"**可见** —— 否则运维看到的现象是"系统不动了",而不是"在按预算补齐"。
    pub rereplication_deferred: IntGauge,
    /// 坏盘撤离计划生成/无法满足的次数,按 `result`(`planned` / `unsatisfiable`)分(D4)。
    ///
    /// `unsatisfiable` 持续增长 = "有坏盘要搬,但没地方搬"(需加节点/盘),这是必须让运维看见的状态。
    pub disk_evac_planned: IntCounterVec,
    /// 在途撤离分区数(gauge;受 `master.disk_evac_max_in_flight` 约束)(D4)。
    pub disk_evacuating: IntGauge,
}

impl MasterMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(MasterMetrics {
            raft_term: gauge(registry, "master_raft_term", "current raft term")?,
            is_leader: gauge(registry, "master_is_leader", "1 if this node is raft leader else 0")?,
            applied_commands: counter(
                registry,
                "master_applied_commands_total",
                "commands applied to the state machine",
            )?,
            registered_nodes: gauge(
                registry,
                "master_registered_nodes",
                "number of nodes registered in hard state",
            )?,
            heartbeats: counter(registry, "master_heartbeats_total", "heartbeats received")?,
            admission_rejected: counter(
                registry,
                "master_admission_rejected_total",
                "requests rejected by admission check",
            )?,
            partitions_created: counter(
                registry,
                "master_partitions_created_total",
                "partitions successfully created",
            )?,
            partition_create_failed: counter(
                registry,
                "master_partition_create_failed_total",
                "partition creation failures",
            )?,
            transitions_issued: counter(
                registry,
                "master_transitions_issued_total",
                "replica state transition directives issued",
            )?,
            rereplications: counter(
                registry,
                "master_rereplications_total",
                "re-replication actions triggered",
            )?,
            replicas_removed: counter(
                registry,
                "master_replicas_removed_total",
                "replicas removed during decommission evacuation",
            )?,
            repairs_triggered: counter(
                registry,
                "master_repairs_triggered_total",
                "manual repair-partition requests triggered",
            )?,
            replicas_moved: counter(
                registry,
                "master_replicas_moved_total",
                "replicas removed after move completed",
            )?,
            under_replicated_partitions: gauge(
                registry,
                "master_under_replicated_partitions",
                "partitions whose healthy replica count is below RF",
            )?,
            assign_requests: counter(
                registry,
                "master_assign_requests_total",
                "AssignPartition requests received (including failures)",
            )?,
            assign_failures: counter(
                registry,
                "master_assign_failures_total",
                "AssignPartition failures (no writable partition etc.)",
            )?,
            lookup_requests: counter(
                registry,
                "master_lookup_requests_total",
                "LookupPartition read-routing queries received",
            )?,
            blob_ids_requests: counter(
                registry,
                "master_blob_ids_requests_total",
                "GetBlobIds allocation requests received (including failures)",
            )?,
            blob_ids_allocated: counter(
                registry,
                "master_blob_ids_allocated_total",
                "blob ids handed out to clients (segment reservations included)",
            )?,
            blob_ids_failures: counter(
                registry,
                "master_blob_ids_failures_total",
                "GetBlobIds failures (not leader / no writable partition / objectId exhausted)",
            )?,
            id_alloc_raft_commits: counter(
                registry,
                "master_id_alloc_raft_commits_total",
                "Raft commits made to raise the objectId watermark (two-layer batching)",
            )?,
            id_alloc_holes: counter(
                registry,
                "master_id_alloc_holes_total",
                "reserved object ids dropped when switching segments (holes; harmless by design)",
            )?,
            partitions_rw: gauge(
                registry,
                "master_partitions_rw",
                "partitions in ReadWrite state (reconciler refreshed, leader only)",
            )?,
            partitions_ro: gauge(
                registry,
                "master_partitions_ro",
                "partitions in ReadOnly state (reconciler refreshed, leader only)",
            )?,
            partitions_sealed: gauge(
                registry,
                "master_partitions_sealed",
                "partitions in Sealed state (reconciler refreshed, leader only)",
            )?,
            writable_capacity_bytes: gauge(
                registry,
                "master_writable_capacity_bytes",
                "writable capacity ceiling = RW partitions * partition_capacity_bytes",
            )?,
            nodes_up: gauge(
                registry,
                "master_nodes_up",
                "nodes considered alive by heartbeat soft state",
            )?,
            cluster_free_bytes: gauge(
                registry,
                "master_cluster_free_bytes",
                "sum of free bytes over all heartbeat-reported disks",
            )?,
            cluster_used_bytes: gauge(
                registry,
                "master_cluster_used_bytes",
                "sum(capacity) - sum(free) over all heartbeat-reported disks",
            )?,
            max_replica_lag_bytes: gauge(
                registry,
                "master_max_replica_lag_bytes",
                "max lag_bytes reported by any replica",
            )?,
            majority_replicas_down_partitions: gauge(
                registry,
                "master_majority_replicas_down_partitions",
                "partitions whose healthy replica count is at most RF/2",
            )?,
            maintenance: gauge(
                registry,
                "master_maintenance",
                "1 if cluster is in maintenance mode (auto reconcile paused)",
            )?,
            writable_used_bytes: gauge(
                registry,
                "master_writable_used_bytes",
                "sum over RW partitions of max replica used_bytes (no RF double count)",
            )?,
            writable_free_bytes: gauge(
                registry,
                "master_writable_free_bytes",
                "writable_capacity_bytes - writable_used_bytes (saturating)",
            )?,
            replicas_by_disk: gauge_vec(
                registry,
                "master_replicas_by_disk",
                "replicas placed per disk (hard placement)",
                &["node_id", "disk_id"],
            )?,
            disk_skew: gauge(
                registry,
                "master_disk_skew",
                "max - min replica count per disk (0 when no disk)",
            )?,
            unhealthy_disks: gauge(
                registry,
                "master_unhealthy_disks",
                "disks whose active probe reported a non-HEALTHY state (probe disabled = not counted)",
            )?,
            partitions_forced_ro: counter(
                registry,
                "master_partitions_forced_ro_total",
                "partitions forced to ReadOnly because a replica disk is near full (never auto-reverted)",
            )?,
            pending_drops: gauge(
                registry,
                "master_pending_drops",
                "removed replicas still awaiting a DROPPED directive to the source node",
            )?,
            dropped_directives: counter(
                registry,
                "master_dropped_directives_total",
                "DROPPED directives issued to source nodes (directories to delete)",
            )?,
            balance_pending: gauge(
                registry,
                "master_balance_pending",
                "pending moves in the current balance plan (leader refreshed)",
            )?,
            balance_in_flight: gauge(
                registry,
                "master_balance_in_flight",
                "in-flight moves in the current balance plan (leader refreshed)",
            )?,
            balance_moves: counter(
                registry,
                "master_balance_moves_total",
                "balance plan moves observed reaching done",
            )?,
            balance_failed: counter(
                registry,
                "master_balance_failed_total",
                "balance plan moves observed reaching failed",
            )?,
            node_down: counter(
                registry,
                "master_node_down_total",
                "nodes declared down (after liveness debounce; a node staying down is counted once)",
            )?,
            rereplication_deferred: gauge(
                registry,
                "master_rereplication_deferred",
                "under-replicated partitions deferred to the next tick by rereplication_max_per_tick",
            )?,
            disk_evac_planned: counter_vec(
                registry,
                "master_disk_evac_planned_total",
                "bad-disk evacuation plans generated (result: planned/unsatisfiable)",
                &["result"],
            )?,
            disk_evacuating: gauge(
                registry,
                "master_disk_evacuating",
                "partitions currently being evacuated off a bad disk (bounded by disk_evac_max_in_flight)",
            )?,
        })
    }

    /// 未接入外部 `Registry` 时的兜底(测试/裸构造):挂在丢弃的私有 `Registry` 上。
    pub fn disabled() -> Self {
        let registry = Registry::new();
        MasterMetrics::new(&registry).expect("fresh registry never has duplicate registrations")
    }
}
