//! M3 调谐的**纯决策函数**(承 §7.2/§7.3):转移下发 + 再复制判定。无 IO/时钟/随机。
//!
//! master 只决策(D1):这里算出"该给节点下发哪些转移"和"哪些分区需补副本",执行/搬数据在
//! store server(后续)。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use crate::types::{NodeId, PartitionId, PartitionInfo};

/// 调谐稳定期判定(承 §7.3):master 启动或切主后,软状态(节点存活/副本实然态)靠 store 心跳
/// **从零重建**;重建前"暂无心跳"不等于"失联",据此触发再复制会引发切主/重启后的再复制风暴。
/// 返回 `true` 表示自成为 leader 起已 ≥ `grace`,可执行失败驱动的调谐。
pub fn reconcile_ready(leader_elapsed: Duration, grace: Duration) -> bool {
    leader_elapsed >= grace
}

/// 欠副本原因码(前端映射为中英文案)。
pub const REASON_NODE_DOWN: &str = "node_down";
pub const REASON_REPLICA_ERROR: &str = "replica_error";
pub const REASON_DECOMMISSIONING: &str = "decommissioning";
pub const REASON_BOOTSTRAPPING: &str = "bootstrapping";

/// 一个副本的健康输入(硬放置 + 软状态合成)。
pub struct ReplicaHealth {
    pub up: bool,
    /// 心跳上报的实然态:ACTIVE/BOOTSTRAP/ERROR/OFFLINE/""(未上报)。
    pub current_state: String,
    pub decommissioning: bool,
}

/// 分区健康分类:返回(健康副本数, 欠副本主因)。
///
/// 健康 = 节点存活 且 `current_state==ACTIVE` 且 非下线迁移中。`healthy >= rf` 时原因为 `None`。
/// 欠副本主因优先级:节点失联 > 副本错误 > 下线迁移中 > 同步中。
pub fn classify_partition(replicas: &[ReplicaHealth], rf: u8) -> (u32, Option<&'static str>) {
    let healthy = replicas
        .iter()
        .filter(|r| r.up && r.current_state == "ACTIVE" && !r.decommissioning)
        .count() as u32;
    if healthy >= rf as u32 {
        return (healthy, None);
    }
    let reason = if replicas.iter().any(|r| !r.up) {
        REASON_NODE_DOWN
    } else if replicas.iter().any(|r| r.current_state == "ERROR") {
        REASON_REPLICA_ERROR
    } else if replicas.iter().any(|r| r.decommissioning) {
        REASON_DECOMMISSIONING
    } else {
        REASON_BOOTSTRAPPING
    };
    (healthy, Some(reason))
}


/// 一条状态转移指令:把某分区在某盘上的副本推向 `target_state`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transition {
    pub partition_id: PartitionId,
    pub disk_id: String,
    pub target_state: String,
}

/// 为某节点算出待下发的转移:该节点承载的副本里,上报 current 与 placement target 不一致的。
///
/// `reported`:该节点上报的 `partition_id -> current_state`;缺失(未上报)视为需要转移。
pub fn plan_transitions(
    node_id: NodeId,
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    reported: &BTreeMap<PartitionId, String>,
) -> Vec<Transition> {
    let mut out = Vec::new();
    for (pid, info) in partitions {
        for r in &info.replicas {
            if r.node_id != node_id {
                continue;
            }
            let current = reported.get(pid);
            if current != Some(&r.target_state) {
                out.push(Transition {
                    partition_id: *pid,
                    disk_id: r.disk_id.clone(),
                    target_state: r.target_state.clone(),
                });
            }
        }
    }
    out
}

/// 算出需要再复制(补副本恢复冗余)的分区:健康副本数 < RF **且**存在不健康副本。
///
/// 健康 = 节点存活(`up_nodes`)**且**未上报 `ERROR`(`errored`,来自软状态 `ReplicaCurrent`,S2 被动检测)。
/// 不健康 = 节点失联 **或** replica 上报 ERROR。
pub fn plan_rereplication(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    up_nodes: &HashSet<NodeId>,
    errored: &HashSet<(PartitionId, NodeId)>,
) -> Vec<PartitionId> {
    let mut out = Vec::new();
    for (pid, info) in partitions {
        let is_healthy = |n: &NodeId| up_nodes.contains(n) && !errored.contains(&(*pid, *n));
        let healthy = info.replicas.iter().filter(|r| is_healthy(&r.node_id)).count();
        let any_unhealthy = info.replicas.iter().any(|r| !is_healthy(&r.node_id));
        if any_unhealthy && healthy < info.replication_factor as usize {
            out.push(*pid);
        }
    }
    out
}

/// 节点下线的一步动作(M4 evacuation)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvacAction {
    /// 该分区健康副本不足 RF,需在健康节点补一个(先补)。
    Add(PartitionId),
    /// 该分区健康副本已达 RF,移除下线节点上的旧副本(后删)。
    Remove(PartitionId, NodeId),
}

/// 为下线中节点承载的分区算 evacuation 动作(先补后删,守 §3.1)。
///
/// 对每个"含下线副本"的分区:健康副本(存活且非下线)`< rf` → `Add`(先补);`>= rf` → 对每个
/// 下线副本产 `Remove`(后删)。
pub fn plan_evacuation(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    decommissioning: &BTreeSet<NodeId>,
    up_nodes: &HashSet<NodeId>,
) -> Vec<EvacAction> {
    let mut out = Vec::new();
    for (pid, info) in partitions {
        let has_decom = info.replicas.iter().any(|r| decommissioning.contains(&r.node_id));
        if !has_decom {
            continue;
        }
        let healthy = info
            .replicas
            .iter()
            .filter(|r| up_nodes.contains(&r.node_id) && !decommissioning.contains(&r.node_id))
            .count();
        if healthy < info.replication_factor as usize {
            out.push(EvacAction::Add(*pid));
        } else {
            for r in &info.replicas {
                if decommissioning.contains(&r.node_id) {
                    out.push(EvacAction::Remove(*pid, r.node_id));
                }
            }
        }
    }
    out
}

/// 对迁移中的副本(MoveReplica)算"后删":新副本已补上、存活且非迁出源的副本数 ≥ RF 时,
/// 可移除迁出源 (pid, node)(add-before-remove,守 §3.1)。
///
/// `moving`:`(pid, node)` = 该节点上该分区副本是待迁出源。迁出源自身即使存活也不计入健康数。
pub fn plan_move(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    moving: &BTreeSet<(PartitionId, NodeId)>,
    up_nodes: &HashSet<NodeId>,
) -> Vec<(PartitionId, NodeId)> {
    let mut out = Vec::new();
    for (pid, node) in moving {
        let Some(info) = partitions.get(pid) else { continue };
        let healthy = info
            .replicas
            .iter()
            .filter(|r| up_nodes.contains(&r.node_id) && !moving.contains(&(*pid, r.node_id)))
            .count();
        if healthy >= info.replication_factor as usize {
            out.push((*pid, *node));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PartitionState, ReplicaPlacement};

    fn part(rf: u8, replicas: &[(NodeId, &str, &str)]) -> PartitionInfo {
        PartitionInfo {
            replication_factor: rf,
            state: PartitionState::ReadWrite,
            replicas: replicas
                .iter()
                .map(|(n, d, t)| ReplicaPlacement {
                    node_id: *n,
                    disk_id: (*d).to_string(),
                    target_state: (*t).to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn transition_when_current_differs_or_missing() {
        let parts = BTreeMap::from([(0, part(2, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE")]))]);
        // node1 上报 BOOTSTRAP(≠ACTIVE)→ 需转移;node1 无 pid1 上报(缺失)→ 也需转移(但无此分区)。
        let reported = BTreeMap::from([(0u32, "BOOTSTRAP".to_string())]);
        let t = plan_transitions(1, &parts, &reported);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].target_state, "ACTIVE");
        assert_eq!(t[0].partition_id, 0);
    }

    #[test]
    fn no_transition_when_current_matches() {
        let parts = BTreeMap::from([(0, part(1, &[(1, "d0", "ACTIVE")]))]);
        let reported = BTreeMap::from([(0u32, "ACTIVE".to_string())]);
        assert!(plan_transitions(1, &parts, &reported).is_empty());
    }

    #[test]
    fn missing_report_needs_transition() {
        let parts = BTreeMap::from([(0, part(1, &[(1, "d0", "ACTIVE")]))]);
        let reported = BTreeMap::new();
        assert_eq!(plan_transitions(1, &parts, &reported).len(), 1);
    }

    #[test]
    fn rereplicate_when_replica_down_and_below_rf() {
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE")]),
        )]);
        let up = HashSet::from([1, 2]); // node3 down → live=2 < 3
        assert_eq!(plan_rereplication(&parts, &up, &HashSet::new()), vec![0]);
    }

    #[test]
    fn no_rereplicate_when_all_up() {
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE")]),
        )]);
        let up = HashSet::from([1, 2, 3]);
        assert!(plan_rereplication(&parts, &up, &HashSet::new()).is_empty());
    }

    #[test]
    fn rereplicate_when_replica_errored_though_node_up() {
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE")]),
        )]);
        let up = HashSet::from([1, 2, 3]); // 全存活
        let errored = HashSet::from([(0u32, 3u64)]); // 但 node3 的 replica 上报 ERROR
        assert_eq!(
            plan_rereplication(&parts, &up, &errored),
            vec![0],
            "node up but replica ERROR must still trigger re-replication"
        );
    }

    #[test]
    fn evacuation_adds_before_removing() {
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE")]),
        )]);
        let up = HashSet::from([1, 2, 3]);
        // node3 下线:healthy(非下线且存活)=2 < 3 → 先 Add,不 Remove。
        let decom = BTreeSet::from([3]);
        assert_eq!(plan_evacuation(&parts, &decom, &up), vec![EvacAction::Add(0)]);
    }

    #[test]
    fn evacuation_removes_after_healthy_reaches_rf() {
        // 已补上 node4:replicas=101..? 用 1..4;node3 下线,healthy={1,2,4}=3≥rf → Remove(0,3)。
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE"), (4, "d0", "ACTIVE")],
            ),
        )]);
        let up = HashSet::from([1, 2, 3, 4]);
        let decom = BTreeSet::from([3]);
        assert_eq!(plan_evacuation(&parts, &decom, &up), vec![EvacAction::Remove(0, 3)]);
    }

    #[test]
    fn no_evacuation_without_decommission() {
        let parts = BTreeMap::from([(0, part(2, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE")]))]);
        let up = HashSet::from([1, 2]);
        assert!(plan_evacuation(&parts, &BTreeSet::new(), &up).is_empty());
    }

    #[test]
    fn move_removes_source_after_replacement_healthy() {
        // RF=3,副本在 1/2/3 + 新补 4;moving={(0,3)}:非迁出源存活 = {1,2,4} = 3 ≥ RF → 移除 3。
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE"), (4, "d0", "ACTIVE")]),
        )]);
        let moving = BTreeSet::from([(0u32, 3u64)]);
        let up = HashSet::from([1, 2, 3, 4]); // 迁出源 3 自己也 up,也要被移除
        assert_eq!(plan_move(&parts, &moving, &up), vec![(0, 3)]);
    }

    #[test]
    fn move_holds_source_until_replacement_up() {
        // 新副本节点 4 尚未 up:非迁出源存活 = {1,2} = 2 < RF=3 → 不移除。
        let parts = BTreeMap::from([(
            0,
            part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE"), (3, "d0", "ACTIVE"), (4, "d0", "ACTIVE")]),
        )]);
        let moving = BTreeSet::from([(0u32, 3u64)]);
        let up = HashSet::from([1, 2, 3]);
        assert!(plan_move(&parts, &moving, &up).is_empty(), "replacement not up: must hold the source");
    }

    #[test]
    fn move_ignores_unknown_partition() {
        let moving = BTreeSet::from([(9u32, 1u64)]);
        assert!(plan_move(&BTreeMap::new(), &moving, &HashSet::from([1])).is_empty());
    }

    #[test]
    fn reconcile_gated_until_grace_elapsed() {
        let grace = Duration::from_secs(15);
        assert!(!reconcile_ready(Duration::from_secs(0), grace), "just became leader: hold off");
        assert!(!reconcile_ready(Duration::from_secs(14), grace), "still within grace");
        assert!(reconcile_ready(Duration::from_secs(15), grace), "grace elapsed: reconcile");
        assert!(reconcile_ready(Duration::from_secs(60), grace));
    }

    fn rh(up: bool, state: &str, decom: bool) -> ReplicaHealth {
        ReplicaHealth { up, current_state: state.to_string(), decommissioning: decom }
    }

    #[test]
    fn classify_partition_reasons() {
        let all_active = [rh(true, "ACTIVE", false), rh(true, "ACTIVE", false), rh(true, "ACTIVE", false)];
        assert_eq!(classify_partition(&all_active, 3), (3, None), "fully healthy");

        let one_down = [rh(true, "ACTIVE", false), rh(true, "ACTIVE", false), rh(false, "ACTIVE", false)];
        assert_eq!(classify_partition(&one_down, 3), (2, Some(REASON_NODE_DOWN)));

        let one_err = [rh(true, "ACTIVE", false), rh(true, "ACTIVE", false), rh(true, "ERROR", false)];
        assert_eq!(classify_partition(&one_err, 3), (2, Some(REASON_REPLICA_ERROR)));

        let one_decom = [rh(true, "ACTIVE", false), rh(true, "ACTIVE", false), rh(true, "ACTIVE", true)];
        assert_eq!(classify_partition(&one_decom, 3), (2, Some(REASON_DECOMMISSIONING)));

        let one_boot = [rh(true, "ACTIVE", false), rh(true, "ACTIVE", false), rh(true, "BOOTSTRAP", false)];
        assert_eq!(classify_partition(&one_boot, 3), (2, Some(REASON_BOOTSTRAPPING)));
    }
}
