//! 计划式均衡(对标 Ceph balancer 的 eval → optimize → execute)的 **planner 与进度派生**。
//!
//! - [`plan_balance`]:纯函数(无 IO/时钟/随机),从 hard placement + 软状态快照算出一份
//!   均衡计划(一组 `BalanceMoveRecord`)。只在 leader 侧调用;结果经
//!   `MasterCommand::SetBalancePlan` 进 Raft(apply 只存不算,确定性铁律见 `types.rs` 头)。
//! - [`move_phase`] / [`plan_view`]:从 `partitions` + `moving` + 计划的 `failed` **派生**
//!   每条 move 的实时状态与汇总进度(计划持久层只存数据,不存进度)。
//!
//! **判据(两个维度找倾斜)**:对每块"合格盘"(所在节点存活、未下线、有容量上报)统计
//! ① 承载副本数、② 用量比 `used/capacity`,取全盘均值 `avg_count` / `avg_ratio`:
//! - **源盘**:`replicas > avg_count × skew_threshold` **或** `ratio > avg_ratio × skew_threshold`
//!   (默认 threshold=1.0,即严格高于均值);
//! - **目标盘**:低于均值(同上两式取 `<`,任一成立)且通过 [`select_disks`] 落地校验
//!   (盘 HEALTHY、free ≥ partition 上界、未将满;节点存活、未下线、未持有该分区副本);
//! - **改善护栏(防抖/防振荡)**:仅当 `源.replicas ≥ 目标.replicas + 2` **或**
//!   `源.ratio > 目标.ratio × skew_threshold` 时才产出该 move —— 副本数不可整除均分时
//!   (如 9 副本 4 盘)不会来回搬同一份数据。
//!
//! **硬约束**:同一分区在一份计划里至多 1 条 move(避免临时 RF+1 叠加放大);已在 `moving`
//! 中的分区跳过(另有迁移在途);找不到合格落点的分区**不进计划**(plan 只含可执行 move);
//! 计划条数超过 `max_moves` 截断。挑选顺序:源盘按 (副本数 desc, 用量比 desc, node, disk)
//! 定序,源盘上按分区实际占用(软状态 `used_bytes`,缺失按 0)从大到小挑要搬的分区;
//! 每产出一条即在内存里**模拟**副本数/占用的转移,再算下一条(避免一份计划把目标盘打满)。

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::pb::{BalanceMove, BalancePlan};
use crate::placement::{select_disks, DiskFree};
use crate::soft::DiskUsage;
use crate::types::{
    BalanceMoveRecord, BalancePlanRecord, BalanceStatus, DiskId, NodeId, NodeInfo, PartitionId,
    PartitionInfo,
};

/// 均衡 planner 的全部输入(hard placement + 软状态快照 + 配置阈值)。
pub struct BalanceInputs<'a> {
    /// 分区放置(应然,hard state)。
    pub partitions: &'a BTreeMap<PartitionId, PartitionInfo>,
    /// 已注册节点(盘的固化容量来自这里)。
    pub nodes: &'a BTreeMap<NodeId, NodeInfo>,
    /// 各盘实时状态(余量/健康;未上报的盘不参与均衡)。
    pub usage: &'a BTreeMap<(NodeId, DiskId), DiskUsage>,
    /// 每分区实际占用(软状态口径:各副本上报最大值;缺失按 0)。
    pub used_bytes: &'a BTreeMap<PartitionId, u64>,
    /// 迁移中的 (pid, 迁出源):这些分区本轮不再搬(已有迁移在途)。
    pub moving: &'a BTreeSet<(PartitionId, NodeId)>,
    /// 下线中节点:既不作源也不作目标(其副本由 evacuation 负责)。
    pub decommissioning: &'a BTreeSet<NodeId>,
    /// 存活节点集合。
    pub up: &'a HashSet<NodeId>,
    /// 计划最多包含多少条搬迁(> 0)。
    pub max_moves: u32,
    /// 倾斜阈值:每盘副本数/用量比超过均值 × 该值才认为倾斜(1.0 = 严格高于均值)。
    pub skew_threshold: f64,
    /// 分区容量上界(落地校验:目标盘 free 须 ≥ 它)。
    pub partition_capacity_bytes: u64,
    /// 盘将满阈值(落地校验,同 [`select_disks`])。
    pub near_full_ratio: f64,
}

/// 一块合格盘的均衡视图(计划演算中被就地模拟更新)。
#[derive(Clone, Debug)]
struct DiskStat {
    node_id: NodeId,
    disk_id: DiskId,
    /// 承载的副本数(hard placement 口径)。
    replicas: u64,
    capacity_bytes: u64,
    /// 已用字节(初值 = capacity − free;产出 move 后就地模拟增减)。
    used_bytes: u64,
}

impl DiskStat {
    fn key(&self) -> (NodeId, DiskId) {
        (self.node_id, self.disk_id.clone())
    }
    /// 用量比(capacity=0 视为 0:这种盘本就被 select_disks 排除,不会成为目标)。
    fn ratio(&self) -> f64 {
        if self.capacity_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 / self.capacity_bytes as f64
        }
    }
}

/// 从 hard placement + 软状态快照算出一份均衡计划(语义见模块头)。
pub fn plan_balance(inputs: &BalanceInputs) -> Vec<BalanceMoveRecord> {
    let BalanceInputs {
        partitions,
        nodes,
        usage,
        used_bytes,
        moving,
        decommissioning,
        up,
        max_moves,
        skew_threshold,
        partition_capacity_bytes,
        near_full_ratio,
    } = *inputs;
    if max_moves == 0 || skew_threshold <= 0.0 {
        return Vec::new();
    }

    // 合格盘 = 节点存活且未下线、且有实时余量上报的盘。源/目标都从这里面挑:
    // down/下线节点上的副本数不计入均值,也不参与搬迁(失联迁出是再复制/evacuation 的职责)。
    let mut stats: BTreeMap<(NodeId, DiskId), DiskStat> = BTreeMap::new();
    for (node_id, info) in nodes {
        if !up.contains(node_id) || decommissioning.contains(node_id) {
            continue;
        }
        for (disk_id, spec) in &info.disks {
            let Some(u) = usage.get(&(*node_id, disk_id.clone())) else {
                continue;
            };
            stats.insert(
                (*node_id, disk_id.clone()),
                DiskStat {
                    node_id: *node_id,
                    disk_id: disk_id.clone(),
                    replicas: 0,
                    capacity_bytes: spec.capacity_bytes,
                    used_bytes: spec.capacity_bytes.saturating_sub(u.free_bytes),
                },
            );
        }
    }
    if stats.len() < 2 {
        return Vec::new(); // 不足两块合格盘,谈不上倾斜
    }
    for info in partitions.values() {
        for r in &info.replicas {
            if let Some(d) = stats.get_mut(&(r.node_id, r.disk_id.clone())) {
                d.replicas += 1;
            }
        }
    }

    let avg_count = |stats: &BTreeMap<(NodeId, DiskId), DiskStat>| {
        stats.values().map(|d| d.replicas).sum::<u64>() as f64 / stats.len() as f64
    };
    let avg_ratio = |stats: &BTreeMap<(NodeId, DiskId), DiskStat>| {
        stats.values().map(DiskStat::ratio).sum::<f64>() / stats.len() as f64
    };

    let disk_free: BTreeMap<(NodeId, DiskId), DiskFree> = usage
        .iter()
        .map(|(k, u)| {
            (
                k.clone(),
                DiskFree {
                    free_bytes: u.free_bytes,
                    health: u.health.clone(),
                },
            )
        })
        .collect();

    let mut out: Vec<BalanceMoveRecord> = Vec::new();
    let mut planned: HashSet<PartitionId> = HashSet::new();
    let mut moving_pids: HashSet<PartitionId> = moving.iter().map(|(pid, _)| *pid).collect();

    // 每轮全量重扫(均值随模拟转移变化),直到一轮下来一无所获或达到 max_moves。
    while (out.len() as u32) < max_moves {
        let ac = avg_count(&stats);
        let ar = avg_ratio(&stats);
        let heavy = |d: &DiskStat| {
            d.replicas as f64 > ac * skew_threshold || d.ratio() > ar * skew_threshold
        };
        let light = |d: &DiskStat| {
            (d.replicas as f64) < ac * skew_threshold || d.ratio() < ar * skew_threshold
        };
        // 改善护栏:副本数差 ≥ 2(不可整除均分时防抖)或用量比显著更好。
        let improves = |src: &DiskStat, dst: &DiskStat| {
            src.replicas >= dst.replicas + 2 || src.ratio() > dst.ratio() * skew_threshold
        };

        // 源盘定序:副本数 desc → 用量比 desc → (node, disk) asc(确定性)。
        let mut sources: Vec<&DiskStat> = stats.values().filter(|d| heavy(d)).collect();
        sources.sort_by(|a, b| {
            b.replicas
                .cmp(&a.replicas)
                .then_with(|| b.ratio().total_cmp(&a.ratio()))
                .then_with(|| a.key().cmp(&b.key()))
        });
        let source_keys: Vec<(NodeId, DiskId)> = sources.iter().map(|d| d.key()).collect();

        let mut progressed = false;
        'sources: for src_key in source_keys {
            if (out.len() as u32) >= max_moves {
                break;
            }
            // 该源盘上可搬的分区:排除已入计划的、已有迁移在途的;按占用从大到小(缺失按 0)。
            let mut cands: Vec<(u64, PartitionId)> = partitions
                .iter()
                .filter(|(pid, info)| {
                    !planned.contains(pid)
                        && !moving_pids.contains(pid)
                        && info
                            .replicas
                            .iter()
                            .any(|r| r.node_id == src_key.0 && r.disk_id == src_key.1)
                })
                .map(|(pid, _)| (used_bytes.get(pid).copied().unwrap_or(0), *pid))
                .collect();
            cands.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

            for (pused, pid) in cands {
                let part = &partitions[&pid];
                // 目标节点定序:其最空盘的 (副本数 asc, 用量比 asc, node asc)。
                let mut target_nodes: Vec<(u64, f64, NodeId)> = stats
                    .values()
                    .filter(|d| light(d))
                    .filter(|d| !part.replicas.iter().any(|r| r.node_id == d.node_id))
                    .map(|d| (d.replicas, d.ratio(), d.node_id))
                    .collect();
                target_nodes.sort_by(|a, b| {
                    a.0.cmp(&b.0)
                        .then_with(|| a.1.total_cmp(&b.1))
                        .then_with(|| a.2.cmp(&b.2))
                });
                target_nodes.dedup_by_key(|t| t.2);

                let mut accepted: Option<(NodeId, DiskId)> = None;
                for (_, _, tnode) in target_nodes {
                    let Some(info) = nodes.get(&tnode) else {
                        continue;
                    };
                    // 落地判定复用 select_disks(盘 HEALTHY / free ≥ 上界 / 未将满),候选限定该节点。
                    let candidates = BTreeMap::from([(tnode, info.clone())]);
                    let Ok(mut reps) = select_disks(
                        &candidates,
                        &disk_free,
                        1,
                        partition_capacity_bytes,
                        near_full_ratio,
                    ) else {
                        continue;
                    };
                    let chosen = reps.remove(0);
                    let (src, dst) = {
                        let src = &stats[&src_key];
                        let Some(dst) = stats.get(&(chosen.node_id, chosen.disk_id.clone())) else {
                            continue;
                        };
                        (src.clone(), dst.clone())
                    };
                    if !light(&dst) || !improves(&src, &dst) {
                        continue;
                    }
                    accepted = Some((chosen.node_id, chosen.disk_id));
                    break;
                }
                if let Some((tnode, tdisk)) = accepted {
                    let from_disk = src_key.1.clone();
                    // 模拟转移:下一条 move 看到更新后的副本数/占用。
                    if let Some(s) = stats.get_mut(&src_key) {
                        s.replicas -= 1;
                        s.used_bytes = s.used_bytes.saturating_sub(pused);
                    }
                    if let Some(d) = stats.get_mut(&(tnode, tdisk.clone())) {
                        d.replicas += 1;
                        d.used_bytes = d.used_bytes.saturating_add(pused);
                    }
                    out.push(BalanceMoveRecord {
                        partition_id: pid,
                        from_node_id: src_key.0,
                        from_disk_id: from_disk,
                        to_node_id: tnode,
                        to_disk_id: tdisk,
                    });
                    planned.insert(pid);
                    moving_pids.insert(pid); // 同分区至多一条(本计划内)
                    progressed = true;
                    continue 'sources; // 该源盘每条 move 后重估(统计已变),先处理下一个源
                }
            }
        }
        if !progressed {
            break;
        }
    }
    out
}

/// 一条 move 的派生状态(由 placement + `moving` + 计划 `failed` 算出,不落盘)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MovePhase {
    /// 尚未受理(目标还没有该副本)。
    Pending,
    /// 已受理:目标已有副本(或已记 moving),等 reconciler `plan_move` 摘源(add-before-remove)。
    InFlight,
    /// 完成:源已不在 placement(目标在)。
    Done,
    /// 判败:执行器记了 `failed`,或源已消失但目标没落下(异常态,不重试)。
    Failed,
}

/// 派生一条 move 的实时状态与错误串(非 Failed 时错误串为空)。
///
/// 幂等要点:`MoveReplica` 的 apply 只在"目标副本尚不存在"时追加副本并记 `moving`
/// (见 `types.rs`);故"目标已在 placement"即视为**已受理**(InFlight),绝不重复投递。
pub fn move_phase(
    plan: &BalancePlanRecord,
    mv: &BalanceMoveRecord,
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    moving: &BTreeSet<(PartitionId, NodeId)>,
) -> (MovePhase, String) {
    if let Some((_, _, err)) = plan
        .failed
        .iter()
        .find(|(pid, from, _)| *pid == mv.partition_id && *from == mv.from_node_id)
    {
        return (MovePhase::Failed, err.clone());
    }
    let part = partitions.get(&mv.partition_id);
    let from_holds = part.is_some_and(|p| p.replicas.iter().any(|r| r.node_id == mv.from_node_id));
    let to_holds = part.is_some_and(|p| p.replicas.iter().any(|r| r.node_id == mv.to_node_id));
    if !from_holds {
        if to_holds {
            (MovePhase::Done, String::new())
        } else {
            (
                MovePhase::Failed,
                "source replica gone before target landed".to_string(),
            )
        }
    } else if to_holds || moving.contains(&(mv.partition_id, mv.from_node_id)) {
        (MovePhase::InFlight, String::new())
    } else {
        (MovePhase::Pending, String::new())
    }
}

/// 计划状态的 proto 串(与 proto 注释一致)。
pub fn status_str(s: BalanceStatus) -> &'static str {
    match s {
        BalanceStatus::Preview => "PREVIEW",
        BalanceStatus::Running => "RUNNING",
        BalanceStatus::Cancelled => "CANCELLED",
        BalanceStatus::Done => "DONE",
    }
}

/// move 状态的 proto 串(与 proto 注释一致)。
pub fn phase_str(p: MovePhase) -> &'static str {
    match p {
        MovePhase::Pending => "pending",
        MovePhase::InFlight => "in_flight",
        MovePhase::Done => "done",
        MovePhase::Failed => "failed",
    }
}

/// 计划的对外视图(RPC 返回):持久记录 + 派生进度(total/pending/in_flight/done/failed)。
pub fn plan_view(
    plan: &BalancePlanRecord,
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    moving: &BTreeSet<(PartitionId, NodeId)>,
) -> BalancePlan {
    let mut counts = [0u32; 4]; // pending / in_flight / done / failed
    let moves: Vec<BalanceMove> = plan
        .moves
        .iter()
        .map(|m| {
            let (phase, error) = move_phase(plan, m, partitions, moving);
            counts[match phase {
                MovePhase::Pending => 0,
                MovePhase::InFlight => 1,
                MovePhase::Done => 2,
                MovePhase::Failed => 3,
            }] += 1;
            BalanceMove {
                partition_id: m.partition_id,
                from_node_id: m.from_node_id,
                from_disk_id: m.from_disk_id.clone(),
                to_node_id: m.to_node_id,
                to_disk_id: m.to_disk_id.clone(),
                status: phase_str(phase).to_string(),
                error,
            }
        })
        .collect();
    BalancePlan {
        plan_id: plan.plan_id,
        status: status_str(plan.status).to_string(),
        created_at_ms: plan.created_at_ms,
        max_in_flight: plan.max_in_flight,
        total: plan.moves.len() as u32,
        pending: counts[0],
        in_flight: counts[1],
        done: counts[2],
        failed: counts[3],
        moves,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DiskSpec, PartitionState, ReplicaPlacement};

    const CAP: u64 = 1_000_000_000_000; // 1 TB
    const PART_CAP: u64 = 100_000_000_000; // 100 GB

    fn node(id: NodeId, rack: &str) -> (NodeId, NodeInfo) {
        (
            id,
            NodeInfo {
                dc: "dc1".into(),
                rack: rack.into(),
                addr: format!("127.0.0.1:{id}"),
                disks: BTreeMap::from([(
                    "d0".to_string(),
                    DiskSpec {
                        capacity_bytes: CAP,
                    },
                )]),
                registered_at_ms: 0,
            },
        )
    }

    fn usage_for(
        nodes: &BTreeMap<NodeId, NodeInfo>,
        free: u64,
        health: &str,
    ) -> BTreeMap<(NodeId, DiskId), DiskUsage> {
        nodes
            .keys()
            .map(|id| {
                (
                    (*id, "d0".to_string()),
                    DiskUsage {
                        capacity_bytes: CAP,
                        free_bytes: free,
                        health: health.to_string(),
                    },
                )
            })
            .collect()
    }

    fn part(rf: u8, nodes_on: &[NodeId]) -> PartitionInfo {
        PartitionInfo {
            replication_factor: rf,
            state: PartitionState::ReadWrite,
            replicas: nodes_on
                .iter()
                .map(|n| ReplicaPlacement {
                    node_id: *n,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
                })
                .collect(),
        }
    }

    struct Fx {
        nodes: BTreeMap<NodeId, NodeInfo>,
        partitions: BTreeMap<PartitionId, PartitionInfo>,
        usage: BTreeMap<(NodeId, DiskId), DiskUsage>,
        used: BTreeMap<PartitionId, u64>,
        moving: BTreeSet<(PartitionId, NodeId)>,
        decom: BTreeSet<NodeId>,
        up: HashSet<NodeId>,
    }

    impl Fx {
        /// 4 节点(1..4),3 个 RF=3 分区全落 1/2/3 → 4 为空,明显倾斜。
        fn skewed() -> Self {
            let nodes =
                BTreeMap::from([node(1, "rA"), node(2, "rB"), node(3, "rC"), node(4, "rD")]);
            let partitions = BTreeMap::from([
                (0, part(3, &[1, 2, 3])),
                (1, part(3, &[1, 2, 3])),
                (2, part(3, &[1, 2, 3])),
            ]);
            let usage = usage_for(&nodes, 900_000_000_000, "");
            let mut used = BTreeMap::new();
            used.insert(0, 10_000);
            used.insert(1, 20_000);
            used.insert(2, 30_000);
            Fx {
                up: nodes.keys().copied().collect(),
                nodes,
                partitions,
                usage,
                used,
                moving: BTreeSet::new(),
                decom: BTreeSet::new(),
            }
        }

        fn inputs(&self, max_moves: u32) -> BalanceInputs<'_> {
            BalanceInputs {
                partitions: &self.partitions,
                nodes: &self.nodes,
                usage: &self.usage,
                used_bytes: &self.used,
                moving: &self.moving,
                decommissioning: &self.decom,
                up: &self.up,
                max_moves,
                skew_threshold: 1.0,
                partition_capacity_bytes: PART_CAP,
                near_full_ratio: 0.95,
            }
        }
    }

    #[test]
    fn uniform_distribution_yields_empty_plan() {
        // 4 节点 4 个 RF=3 分区轮转放置 → 每盘恰 3 副本,无人倾斜。
        let mut fx = Fx::skewed();
        fx.partitions = BTreeMap::from([
            (0, part(3, &[1, 2, 3])),
            (1, part(3, &[2, 3, 4])),
            (2, part(3, &[3, 4, 1])),
            (3, part(3, &[4, 1, 2])),
        ]);
        assert!(
            plan_balance(&fx.inputs(100)).is_empty(),
            "uniform cluster must yield no moves"
        );
    }

    #[test]
    fn skewed_cluster_yields_expected_moves() {
        // 3,3,3,0 → 模拟演算后产出 2 条:源 1/2(并列按 node asc),目标唯一候选 4。
        // (9 副本 4 盘不可整除均分,2,2,3,2 即收敛;第 3 条被改善护栏拦下:3 ≥ 2+2 不成立。)
        let fx = Fx::skewed();
        let moves = plan_balance(&fx.inputs(100));
        assert_eq!(moves.len(), 2, "got {moves:?}");
        // 源盘上按占用从大到小:pid2(30000)先于 pid1(20000)先于 pid0(10000)。
        assert_eq!(
            (
                moves[0].partition_id,
                moves[0].from_node_id,
                moves[0].to_node_id
            ),
            (2, 1, 4)
        );
        assert_eq!(
            (
                moves[1].partition_id,
                moves[1].from_node_id,
                moves[1].to_node_id
            ),
            (1, 2, 4)
        );
        assert!(moves
            .iter()
            .all(|m| m.from_disk_id == "d0" && m.to_disk_id == "d0"));
    }

    #[test]
    fn one_move_per_partition_and_max_moves_truncation() {
        let fx = Fx::skewed();
        let moves = plan_balance(&fx.inputs(1));
        assert_eq!(moves.len(), 1, "max_moves must truncate the plan");
        let pids: HashSet<_> = moves.iter().map(|m| m.partition_id).collect();
        assert_eq!(
            pids.len(),
            moves.len(),
            "same partition must not repeat in one plan"
        );
    }

    #[test]
    fn down_target_node_yields_no_move() {
        // 空节点 4 失联 → 不是合格目标;倾斜存在但无落点 → 空计划。
        let mut fx = Fx::skewed();
        fx.up.remove(&4);
        assert!(plan_balance(&fx.inputs(100)).is_empty());
    }

    #[test]
    fn unhealthy_target_disk_yields_no_move() {
        // 空节点 4 的盘探测不健康 → select_disks 排除 → 无落点 → 空计划。
        let mut fx = Fx::skewed();
        fx.usage.insert(
            (4, "d0".to_string()),
            DiskUsage {
                capacity_bytes: CAP,
                free_bytes: 900_000_000_000,
                health: "WRITE_TIMEOUT".into(),
            },
        );
        assert!(plan_balance(&fx.inputs(100)).is_empty());
    }

    #[test]
    fn decommissioning_node_is_neither_source_nor_target() {
        // 空节点 4 在下线 → 不作目标;同时重源 1 下线 → 也不作源(其副本归 evacuation 管)。
        let mut fx = Fx::skewed();
        fx.decom.insert(4);
        assert!(plan_balance(&fx.inputs(100)).is_empty());
        fx.decom.clear();
        fx.decom.insert(1);
        let moves = plan_balance(&fx.inputs(100));
        assert!(moves
            .iter()
            .all(|m| m.from_node_id != 1 && m.to_node_id != 1));
    }

    #[test]
    fn partition_already_moving_is_skipped() {
        let mut fx = Fx::skewed();
        fx.moving.insert((2, 1));
        let moves = plan_balance(&fx.inputs(100));
        assert!(
            moves.iter().all(|m| m.partition_id != 2),
            "in-flight partition must be skipped"
        );
    }

    #[test]
    fn move_phase_derivation() {
        let plan = BalancePlanRecord {
            plan_id: 1,
            status: BalanceStatus::Running,
            created_at_ms: 0,
            max_in_flight: 1,
            moves: vec![],
            failed: vec![(7, 1, "stuck".to_string())],
        };
        let mv = BalanceMoveRecord {
            partition_id: 7,
            from_node_id: 1,
            from_disk_id: "d0".into(),
            to_node_id: 4,
            to_disk_id: "d0".into(),
        };
        let moving: BTreeSet<(PartitionId, NodeId)> = BTreeSet::new();

        // failed 记账优先。
        let parts = BTreeMap::from([(7, part(3, &[1, 2, 3]))]);
        assert_eq!(move_phase(&plan, &mv, &parts, &moving).0, MovePhase::Failed);

        let clean = BalancePlanRecord {
            failed: vec![],
            ..plan.clone()
        };
        // 源在、目标不在 → pending。
        assert_eq!(
            move_phase(&clean, &mv, &parts, &moving).0,
            MovePhase::Pending
        );
        // 目标已补上 → in_flight(等摘源)。
        let parts = BTreeMap::from([(7, part(3, &[1, 2, 3, 4]))]);
        assert_eq!(
            move_phase(&clean, &mv, &parts, &moving).0,
            MovePhase::InFlight
        );
        // 目标未补上但已记 moving → in_flight(防御性口径)。
        let parts = BTreeMap::from([(7, part(3, &[1, 2, 3]))]);
        let moving = BTreeSet::from([(7u32, 1u64)]);
        assert_eq!(
            move_phase(&clean, &mv, &parts, &moving).0,
            MovePhase::InFlight
        );
        // 源已摘、目标在 → done。
        let parts = BTreeMap::from([(7, part(3, &[2, 3, 4]))]);
        assert_eq!(
            move_phase(&clean, &mv, &parts, &BTreeSet::new()).0,
            MovePhase::Done
        );
        // 源已摘、目标也不在 → failed(异常)。
        let parts = BTreeMap::from([(7, part(3, &[2, 3, 5]))]);
        let (phase, err) = move_phase(&clean, &mv, &parts, &BTreeSet::new());
        assert_eq!(phase, MovePhase::Failed);
        assert!(!err.is_empty());
    }
}
