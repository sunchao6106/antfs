//! `MasterMetrics`:master 节点的统计打点(接 common/metric 的 `Registry`)。
//!
//! DI 见 [`docs/design/common.md` §2.2];清单见 [`docs/design/cluster.md`](../../../docs/design/cluster.md)「监控项清单」。
//! master 是单例进程(非多 store 那种多实例),故用**无 label** 的裸 handle。指标断言必须打在
//! 实际接入的这个 `Registry` 上(CLAUDE.md §6)。

use metric::{counter, gauge, IntCounter, IntGauge, Registry, Result};

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
        })
    }

    /// 未接入外部 `Registry` 时的兜底(测试/裸构造):挂在丢弃的私有 `Registry` 上。
    pub fn disabled() -> Self {
        let registry = Registry::new();
        MasterMetrics::new(&registry).expect("fresh registry never has duplicate registrations")
    }
}
