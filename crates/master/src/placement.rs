//! 容量感知 + 故障域感知的选盘(§7.3)。**纯函数**:无 IO / 时钟 / 随机。
//!
//! 选盘依赖软状态(各盘实时 `free_bytes`),故只在 leader 提案前调用;结果随
//! [`MasterCommand::CreatePartition`](crate::types::MasterCommand) 进 Raft,`apply` 不再读软状态
//! (保确定性,见 `openspec/changes/master-placement/design.md` §1)。

use std::collections::BTreeMap;

use thiserror::Error;

use crate::types::{DiskId, NodeId, NodeInfo, ReplicaPlacement};

/// M1/M2 副本统一目标态(Q4:不区分 leader/standby)。
const ACTIVE: &str = "ACTIVE";

/// 盘主动探测的健康值(store `DiskHealth::Healthy`)。
const HEALTHY: &str = "HEALTHY";
/// store 侧"**从未观测过**该盘"(刚启动 / 主动探测未启用 —— 后者是**默认**)。
///
/// 与空串一样算**可用**:没有证据说它坏,就不能拦(见 [`select_disks`])。
const UNKNOWN: &str = "UNKNOWN";
/// store 侧迟滞判定后的**隔离态**(闸门据此 503)。
#[allow(dead_code)] // 同上:记录值域语义,单测引用
const UNAVAILABLE: &str = "UNAVAILABLE";
/// store 侧**延迟劣化**:盘还在服务(不 503),但持续的慢 IO 占比超阈值。
///
/// **不放新副本**(它 != `HEALTHY`/`UNKNOWN`/空串 ⇒ 天然被 [`select_disks`] 排除),
/// 但**不触发撤离** —— 撤离只认 `UNAVAILABLE`(design §6:有点慢就搬 GB 数据,代价不对称)。
/// 两条都由单测钉住(见 `degraded_disk_takes_no_new_replicas_but_is_not_evicted`)。
#[allow(dead_code)] // 只在单测里被引用:它记录的是"值域语义",`select_disks` 靠排除法天然覆盖
const DEGRADED: &str = "DEGRADED";

/// 选盘失败。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlacementError {
    /// 满足条件的候选盘不足以放下 `needed` 个副本。
    #[error("insufficient disks: need {needed}, only {available} eligible")]
    InsufficientDisks { needed: usize, available: usize },
    /// 复制因子为 0(无意义)。
    #[error("replication_factor must be >= 1")]
    ZeroReplicationFactor,
}

/// 某块盘的实时快照(软状态,心跳上报)。
#[derive(Clone, Debug)]
pub struct DiskFree {
    pub free_bytes: u64,
    /// 该盘主动探测的健康结果(空串 = **未启用主动探测**,视为可用;见 [`select_disks`])。
    pub health: String,
}

/// 一块候选盘(展平后带定位与排序键)。
struct Candidate {
    node_id: NodeId,
    disk_id: DiskId,
    rack: String,
    free_bytes: u64,
}

/// 从注册节点 + 各盘实时余量中,为一个 RF=`rf` 的分区选出放置目标。
///
/// - `usage`:`(node_id, disk_id) -> DiskFree`;**未上报余量的盘视为不可用**(不放)。
/// - `partition_cap`:分区容量上界,盘可用容量须 ≥ 它。
/// - `near_full_ratio`:盘用量超此比例视为将满,跳过(用量 = 1 - free/capacity)。
///
/// 规则:候选 = 余量 ≥ 上界、未将满、**健康**;按余量降序贪心,**每个副本落不同节点**、优先不同 rack;
/// 不足则 `InsufficientDisks`(不降 rf)。确定性:同输入恒产同输出(按 (node,disk) 稳定定序)。
///
/// **盘健康避让**(S2):`health` **明确报出不健康**的盘不再放新副本。可放集合 =
/// `""`(旧节点,没上报健康值)| `HEALTHY` | **`UNKNOWN`**(store 侧"从未观测过")。
///
/// **`DEGRADED`(慢但可用)不在可放集合里** —— 这是这一档存在的意义:只有"可用/不可用"两档时,
/// 一块 p99 从 1ms 涨到 2s 的盘**既不会被隔离、也没人停止往它放副本**,就那么一直烂着。
/// 排除它是**免费的**(不放新副本几乎零代价),而搬走已有副本很贵 —— 故只做前者(见 `DEGRADED` 的常量说明)。
///
/// 为什么 `UNKNOWN` 必须算可用:store 侧自 D2 起把"没探过"从**空串**改成了显式的 `UNKNOWN`
/// (`disk_health_enabled` 默认就是 **false**,即默认部署里每块盘都是这个值)。若这里把 `UNKNOWN`
/// 当坏,默认部署会**一块盘都放不了** —— 那是"实现了坏盘隔离、顺手打死默认部署"的回归。
///
/// 换句话说:避让只对**明确的证据**生效,不把"没有证据"当"坏"。
pub fn select_disks(
    nodes: &BTreeMap<NodeId, NodeInfo>,
    usage: &BTreeMap<(NodeId, DiskId), DiskFree>,
    rf: u8,
    partition_cap: u64,
    near_full_ratio: f64,
) -> Result<Vec<ReplicaPlacement>, PlacementError> {
    if rf == 0 {
        return Err(PlacementError::ZeroReplicationFactor);
    }
    let rf = rf as usize;

    let mut candidates: Vec<Candidate> = Vec::new();
    for (node_id, info) in nodes {
        for (disk_id, spec) in &info.disks {
            let Some(free) = usage.get(&(*node_id, disk_id.clone())) else {
                continue; // 未上报余量 → 不可用
            };
            if !free.health.is_empty() && free.health != HEALTHY && free.health != UNKNOWN {
                continue; // 明确探测为不健康 → 不放新副本(空串/UNKNOWN = 没有证据,视为可用)
            }
            if free.free_bytes < partition_cap {
                continue; // 容量不足
            }
            if spec.capacity_bytes == 0 {
                continue;
            }
            let used_ratio = 1.0 - (free.free_bytes as f64 / spec.capacity_bytes as f64);
            if used_ratio > near_full_ratio {
                continue; // 盘将满
            }
            candidates.push(Candidate {
                node_id: *node_id,
                disk_id: disk_id.clone(),
                rack: info.rack.clone(),
                free_bytes: free.free_bytes,
            });
        }
    }

    // 稳定定序:余量降序;并列按 (node_id, disk_id) 保确定性。
    candidates.sort_by(|a, b| {
        b.free_bytes
            .cmp(&a.free_bytes)
            .then_with(|| a.node_id.cmp(&b.node_id))
            .then_with(|| a.disk_id.cmp(&b.disk_id))
    });

    // 贪心:每副本换一个节点,优先未用过的 rack;两轮——先严格不同 rack,再放宽到仅不同 node。
    let mut chosen: Vec<&Candidate> = Vec::with_capacity(rf);
    let mut used_nodes: Vec<NodeId> = Vec::new();
    let mut used_racks: Vec<String> = Vec::new();

    for c in &candidates {
        if chosen.len() == rf {
            break;
        }
        if used_nodes.contains(&c.node_id) || used_racks.contains(&c.rack) {
            continue;
        }
        chosen.push(c);
        used_nodes.push(c.node_id);
        used_racks.push(c.rack.clone());
    }
    // 第二轮:rack 不够多时,放宽到仅要求不同 node。
    if chosen.len() < rf {
        for c in &candidates {
            if chosen.len() == rf {
                break;
            }
            if used_nodes.contains(&c.node_id) {
                continue;
            }
            chosen.push(c);
            used_nodes.push(c.node_id);
        }
    }

    if chosen.len() < rf {
        return Err(PlacementError::InsufficientDisks {
            needed: rf,
            available: chosen.len(),
        });
    }

    Ok(chosen
        .into_iter()
        .map(|c| ReplicaPlacement {
            node_id: c.node_id,
            disk_id: c.disk_id.clone(),
            target_state: ACTIVE.to_string(),
            // incarnation 占位由 apply 覆盖(见 types.rs ReplicaPlacement::incarnation)。
            incarnation: 0,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DiskSpec;
    use proptest::prelude::*;

    fn node(rack: &str, disks: &[(&str, u64)]) -> NodeInfo {
        NodeInfo {
            dc: "dc1".into(),
            rack: rack.into(),
            addr: "127.0.0.1:1".into(),
            disks: disks
                .iter()
                .map(|(id, cap)| {
                    (
                        id.to_string(),
                        DiskSpec {
                            capacity_bytes: *cap,
                        },
                    )
                })
                .collect(),
            registered_at_ms: 0,
        }
    }

    const CAP: u64 = 100;

    /// 盘余量快照(health 空串 = 该盘未启用主动探测)。
    fn free(free_bytes: u64) -> DiskFree {
        DiskFree {
            free_bytes,
            health: String::new(),
        }
    }

    /// 带健康上报的盘余量快照。
    fn free_health(free_bytes: u64, health: &str) -> DiskFree {
        DiskFree {
            free_bytes,
            health: health.to_string(),
        }
    }

    #[test]
    fn spreads_across_nodes_and_racks() {
        let nodes = BTreeMap::from([
            (1, node("rA", &[("d0", 1000)])),
            (2, node("rB", &[("d0", 1000)])),
            (3, node("rC", &[("d0", 1000)])),
        ]);
        let usage = BTreeMap::from([
            ((1, "d0".to_string()), free(1000)),
            ((2, "d0".to_string()), free(1000)),
            ((3, "d0".to_string()), free(1000)),
        ]);
        let out = select_disks(&nodes, &usage, 3, CAP, 0.95).unwrap();
        assert_eq!(out.len(), 3);
        let mut node_ids: Vec<_> = out.iter().map(|r| r.node_id).collect();
        node_ids.sort();
        assert_eq!(node_ids, vec![1, 2, 3], "one replica per distinct node");
        assert!(out.iter().all(|r| r.target_state == "ACTIVE"));
    }

    #[test]
    fn skips_near_full_disk() {
        let nodes = BTreeMap::from([
            (1, node("rA", &[("d0", 1000)])),
            (2, node("rB", &[("d0", 1000)])),
        ]);
        // node2 盘用量 99% > 95% → 跳过 → 候选不足。
        let usage = BTreeMap::from([
            ((1, "d0".to_string()), free(900)),
            ((2, "d0".to_string()), free(10)),
        ]);
        let err = select_disks(&nodes, &usage, 2, CAP, 0.95).unwrap_err();
        assert!(matches!(
            err,
            PlacementError::InsufficientDisks {
                needed: 2,
                available: 1
            }
        ));
    }

    #[test]
    fn rejects_disk_below_partition_cap() {
        let nodes = BTreeMap::from([(1, node("rA", &[("d0", 1000)]))]);
        let usage = BTreeMap::from([((1, "d0".to_string()), free(50))]);
        let err = select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap_err();
        assert!(matches!(err, PlacementError::InsufficientDisks { .. }));
    }

    #[test]
    fn unreported_disk_is_unavailable() {
        let nodes = BTreeMap::from([(1, node("rA", &[("d0", 1000)]))]);
        let usage = BTreeMap::new(); // 无心跳余量
        let err = select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap_err();
        assert!(matches!(
            err,
            PlacementError::InsufficientDisks {
                needed: 1,
                available: 0
            }
        ));
    }

    #[test]
    fn unhealthy_disk_is_excluded() {
        // node2 的盘主动探测报 WRITE_TIMEOUT(容量充裕、也未将满)→ 仍不得放新副本。
        let nodes = BTreeMap::from([
            (1, node("rA", &[("d0", 1000)])),
            (2, node("rB", &[("d0", 1000)])),
        ]);
        let usage = BTreeMap::from([
            ((1, "d0".to_string()), free(1000)),
            ((2, "d0".to_string()), free_health(1000, "WRITE_TIMEOUT")),
        ]);
        // RF=2 只剩 1 块可用盘 → 不足(绝不放到坏盘上);RF=1 则只选健康的 node1。
        let err = select_disks(&nodes, &usage, 2, CAP, 0.95).unwrap_err();
        assert!(matches!(
            err,
            PlacementError::InsufficientDisks {
                needed: 2,
                available: 1
            }
        ));
        let one = select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(
            one[0].node_id, 1,
            "unhealthy disk must not host a new replica"
        );
    }

    #[test]
    fn empty_health_means_probe_disabled_and_disk_stays_usable() {
        // 未启用主动探测的集群:health 恒为空串 → 照常可选(否则会全盘不可用)。
        let nodes = BTreeMap::from([(1, node("rA", &[("d0", 1000)]))]);
        let usage = BTreeMap::from([((1, "d0".to_string()), free_health(1000, ""))]);
        let out = select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap();
        assert_eq!(out[0].node_id, 1, "empty health = probe disabled = usable");
    }

    /// **`DEGRADED` 不放新副本**(慢盘的"止损"就靠这一条),但它**不是**不可用 ——
    /// 盘上的既有副本照常被读,也不会被撤离(撤离只认 `UNAVAILABLE`)。
    #[test]
    fn degraded_disk_takes_no_new_replicas_but_is_not_evicted() {
        let nodes = BTreeMap::from([
            (1, node("rA", &[("d0", 1000)])),
            (2, node("rB", &[("d0", 1000)])),
        ]);
        let usage = BTreeMap::from([
            ((1, "d0".to_string()), free_health(1000, DEGRADED)),
            ((2, "d0".to_string()), free_health(1000, "HEALTHY")),
        ]);
        // RF=2 只剩 1 块可用盘 ⇒ 不足(绝不放慢盘)。
        let err = select_disks(&nodes, &usage, 2, CAP, 0.95).unwrap_err();
        assert!(matches!(
            err,
            PlacementError::InsufficientDisks {
                needed: 2,
                available: 1
            }
        ));
        // RF=1 ⇒ 只选健康的那块。
        let one = select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap();
        assert_eq!(one[0].node_id, 2, "慢盘不接新副本");

        // 但 `DEGRADED` ≠ `UNAVAILABLE`:撤离只认后者(D4 的判据在 reconcile 侧,这里守住值域语义)。
        assert_ne!(
            DEGRADED, UNAVAILABLE,
            "两个值必须不同 —— 撤离判据只匹配 UNAVAILABLE"
        );
    }

    /// **`UNKNOWN` 必须算可用**(D2 起 store 侧的"从未观测过"就是这个值)。
    ///
    /// 这条是"别用坏盘隔离顺手打死默认部署"的守门人:store 的 `disk_health_enabled` 默认 **false**,
    /// 于是**每块盘**都报 `UNKNOWN`。若这里把它当坏,默认部署会一块盘都放不了 —— 集群建不起来。
    #[test]
    fn unknown_health_stays_usable_so_default_deployments_keep_working() {
        let nodes = BTreeMap::from([
            (1, node("rA", &[("d0", 1000)])),
            (2, node("rB", &[("d0", 1000)])),
        ]);
        let usage = BTreeMap::from([
            ((1, "d0".to_string()), free_health(1000, "UNKNOWN")),
            ((2, "d0".to_string()), free_health(1000, "UNKNOWN")),
        ]);
        let out = select_disks(&nodes, &usage, 2, CAP, 0.95).expect("UNKNOWN ⇒ 可放");
        assert_eq!(out.len(), 2, "两块 UNKNOWN 盘都该可选");
    }

    /// 隔离态(`UNAVAILABLE`)与具体故障值一样**不可放新副本**;单轮故障值同样不放(先避开,等隔离判据)。
    #[test]
    fn unavailable_and_failure_values_are_both_excluded() {
        for health in ["UNAVAILABLE", "IO_TIMEOUT", "READ_MISMATCH"] {
            let nodes = BTreeMap::from([(1, node("rA", &[("d0", 1000)]))]);
            let usage = BTreeMap::from([((1, "d0".to_string()), free_health(1000, health))]);
            assert!(
                select_disks(&nodes, &usage, 1, CAP, 0.95).is_err(),
                "{health} 的盘不该接新副本"
            );
        }
    }

    #[test]
    fn healthy_report_keeps_disk_usable() {
        let nodes = BTreeMap::from([(1, node("rA", &[("d0", 1000)]))]);
        let usage = BTreeMap::from([((1, "d0".to_string()), free_health(1000, "HEALTHY"))]);
        assert_eq!(select_disks(&nodes, &usage, 1, CAP, 0.95).unwrap().len(), 1);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        // 无论输入如何,选出的副本恒:数量=rf、跨不同节点、盘余量≥上界、未将满、非不健康;且确定。
        #[test]
        fn placement_invariants(
            n_nodes in 1usize..8,
            rf in 1u8..6,
            frees in prop::collection::vec(0u64..2000, 1..8),
            unhealthy in prop::collection::hash_set(0usize..8, 0..3),
        ) {
            let mut nodes = BTreeMap::new();
            let mut usage = BTreeMap::new();
            let mut bad: std::collections::HashSet<u64> = std::collections::HashSet::new();
            for i in 0..n_nodes {
                let id = i as u64 + 1;
                let cap = 1000u64;
                nodes.insert(id, node(&format!("r{i}"), &[("d0", cap)]));
                let free_b = frees[i % frees.len()].min(cap);
                if unhealthy.contains(&i) {
                    bad.insert(id);
                    usage.insert((id, "d0".to_string()), free_health(free_b, "READ_ERROR"));
                } else {
                    usage.insert((id, "d0".to_string()), free(free_b));
                }
            }
            let near_full = 0.95;
            let r1 = select_disks(&nodes, &usage, rf, CAP, near_full);
            let r2 = select_disks(&nodes, &usage, rf, CAP, near_full);
            prop_assert_eq!(&r1, &r2, "deterministic");

            if let Ok(placed) = r1 {
                prop_assert_eq!(placed.len(), rf as usize);
                let mut seen = std::collections::HashSet::new();
                for p in &placed {
                    prop_assert!(seen.insert(p.node_id), "distinct node per replica");
                    prop_assert!(!bad.contains(&p.node_id), "never place on an unhealthy disk");
                    let cap = nodes[&p.node_id].disks[&p.disk_id].capacity_bytes;
                    let free_b = usage[&(p.node_id, p.disk_id.clone())].free_bytes;
                    prop_assert!(free_b >= CAP, "free >= partition cap");
                    let used = 1.0 - (free_b as f64 / cap as f64);
                    prop_assert!(used <= near_full, "not near-full");
                }
            }
        }
    }
}
