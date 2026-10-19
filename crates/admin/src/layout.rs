//! 布局反查与放置合规审计(纯函数,admin 侧 join 后的计算核)。
//!
//! 设计(openspec `admin-layout-compliance`,cluster.md §11):
//! - **反查**:节点 → 它承载哪些副本(正查是 `GET /api/partitions` 的分区 → 副本;这里是
//!   排水/搬机前"这台机器上躺着什么"的清点视图),零副本节点也列出(空是有效布局事实)。
//! - **违规集 = placement 真实强制的约束 + join 完整性**:`same_node`(反亲和违例)、
//!   `unknown_node`/`unknown_disk`(注册面与放置面漂移)。慢盘/下线/BOOTSTRAP 是**状态事实**
//!   不是违规 —— 把"进行中的正常过程"计成红灯会训练运维无视红灯。rack 级反亲和在 placement
//!   强制它之前不入审计集(约束跟随 enforcement)。
//! - 只报警不修复:处置走运维弧 `move`/`repair`/重 eval。

use std::collections::BTreeMap;

use crate::master_api::{NodeDto, PartitionDto};

/// 机器视角的一条副本(紧凑形态:哪个分区、哪块盘、目标态)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEntry {
    pub partition_id: u32,
    pub disk_id: String,
    pub target_state: String,
}

/// 一个节点的布局清单(反查视图)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInventory {
    pub node_id: u64,
    pub addr: String,
    pub up: bool,
    pub replica_count: usize,
    /// 该节点承载的全部副本,按 `partition_id` 升序。
    pub replicas: Vec<ReplicaEntry>,
}

/// 一处放置违规(结构字段供 UI 渲染;`detail` 是英文兜底串)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub kind: &'static str,
    pub partition_id: Option<u32>,
    pub node_id: Option<u64>,
    pub disk_id: Option<String>,
    pub detail: String,
}

/// 布局报表(全部派生,无 IO/时钟 —— 时间戳由 server 层补)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutReport {
    pub total_partitions: usize,
    pub total_replicas: usize,
    /// 按 `node_id` 升序;含零副本节点。
    pub nodes: Vec<NodeInventory>,
    pub violations: Vec<Violation>,
    pub compliant: bool,
}

/// 违规 kind 真值表(admin 测试与 UI i18n key 的共同上游)。
pub const VIOLATION_KINDS: [&str; 3] = ["same_node", "unknown_node", "unknown_disk"];

/// 从节点表 + 全量分区表算布局报表(纯函数)。
pub fn compute(nodes: &[NodeDto], partitions: &[PartitionDto]) -> LayoutReport {
    let registry: BTreeMap<u64, &NodeDto> = nodes.iter().map(|n| (n.node_id, n)).collect();
    let disks_of: BTreeMap<u64, BTreeMap<&str, ()>> = nodes
        .iter()
        .map(|n| {
            (
                n.node_id,
                n.disks.iter().map(|d| (d.disk_id.as_str(), ())).collect(),
            )
        })
        .collect();

    // 反查聚合:node_id → 副本条目(逐分区 push;入清单前统一按 pid 排序兜底)。
    let mut per_node: BTreeMap<u64, Vec<ReplicaEntry>> = BTreeMap::new();
    let mut total_replicas = 0usize;
    for p in partitions {
        for r in &p.replicas {
            total_replicas += 1;
            per_node.entry(r.node_id).or_default().push(ReplicaEntry {
                partition_id: p.partition_id,
                disk_id: r.disk_id.clone(),
                target_state: r.target_state.clone(),
            });
        }
    }
    // 清点视图只覆盖注册节点;引用未注册节点的副本不进任何清单(也无处可进),由下方
    // unknown_node 违例如实呈现,但计入合计(total_replicas 不丢观测)。
    let inventory: Vec<NodeInventory> = nodes
        .iter()
        .map(|n| {
            let mut replicas = per_node.remove(&n.node_id).unwrap_or_default();
            replicas.sort_by_key(|e| e.partition_id);
            NodeInventory {
                node_id: n.node_id,
                addr: n.addr.clone(),
                up: n.up,
                replica_count: replicas.len(),
                replicas,
            }
        })
        .collect();

    let mut violations: Vec<Violation> = Vec::new();

    for p in partitions {
        let mut by_node: BTreeMap<u64, usize> = BTreeMap::new();
        for r in &p.replicas {
            *by_node.entry(r.node_id).or_default() += 1;
        }
        for (node_id, count) in by_node {
            if count > 1 {
                violations.push(Violation {
                    kind: "same_node",
                    partition_id: Some(p.partition_id),
                    node_id: Some(node_id),
                    disk_id: None,
                    detail: format!(
                        "partition {} holds {} replicas on node {}",
                        p.partition_id, count, node_id
                    ),
                });
            }
        }
    }
    // unknown_node / unknown_disk:逐副本对注册表核对。
    for p in partitions {
        for r in &p.replicas {
            let Some(node) = registry.get(&r.node_id) else {
                violations.push(Violation {
                    kind: "unknown_node",
                    partition_id: Some(p.partition_id),
                    node_id: Some(r.node_id),
                    disk_id: Some(r.disk_id.clone()),
                    detail: format!(
                        "partition {} references unregistered node {}",
                        p.partition_id, r.node_id
                    ),
                });
                continue;
            };
            if !disks_of[&node.node_id].contains_key(r.disk_id.as_str()) {
                violations.push(Violation {
                    kind: "unknown_disk",
                    partition_id: Some(p.partition_id),
                    node_id: Some(r.node_id),
                    disk_id: Some(r.disk_id.clone()),
                    detail: format!(
                        "partition {} references disk {} not registered on node {}",
                        p.partition_id, r.disk_id, r.node_id
                    ),
                });
            }
        }
    }
    let compliant = violations.is_empty();
    LayoutReport {
        total_partitions: partitions.len(),
        total_replicas,
        nodes: inventory,
        violations,
        compliant,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::master_api::{DiskDto, ReplicaDto};

    /// server 端点测试共用的 DTO 构造(server.rs tests 经 `crate::layout::tests::*` 复用)。
    pub(crate) fn node(id: u64, up: bool, disks: &[&str]) -> NodeDto {
        NodeDto {
            node_id: id,
            dc: "dc1".into(),
            rack: format!("r{id}"),
            addr: format!("127.0.0.1:{id}"),
            up,
            disks: disks
                .iter()
                .map(|d| DiskDto {
                    disk_id: (*d).to_string(),
                    capacity_bytes: 1000,
                    free_bytes: 500,
                    health: "HEALTHY".into(),
                })
                .collect(),
        }
    }

    /// 便捷构造:replica(node, disk) 一行一个;target 态一律 ACTIVE(判定与态无关)。
    pub(crate) fn partition(pid: u32, reps: &[(u64, &str)]) -> PartitionDto {
        PartitionDto {
            partition_id: pid,
            state: "RW".into(),
            replication_factor: reps.len() as u32,
            replicas: reps
                .iter()
                .map(|(n, d)| ReplicaDto {
                    node_id: *n,
                    addr: format!("127.0.0.1:{n}"),
                    disk_id: (*d).to_string(),
                    target_state: "ACTIVE".into(),
                    up: true,
                    current_state: Some("ACTIVE".into()),
                    lag_bytes: Some(0),
                    used_bytes: 0,
                })
                .collect(),
            used_bytes: 0,
            healthy_replicas: reps.len() as u32,
            healable: true,
            under_replicated_reason: String::new(),
            moving_out_nodes: vec![],
        }
    }

    #[test]
    fn reverse_lookup_aggregates_and_lists_empty_nodes() {
        let nodes = vec![
            node(1, true, &["d1", "d2"]),
            node(2, false, &["d3"]),
            node(3, true, &["d9"]),
        ];
        let partitions = vec![
            partition(7, &[(1, "d1"), (2, "d3")]),
            partition(8, &[(1, "d2")]),
        ];
        let r = compute(&nodes, &partitions);
        assert_eq!(r.total_partitions, 2);
        assert_eq!(r.total_replicas, 3);
        assert_eq!(r.nodes.len(), 3, "零副本节点也必须出现");
        let n1 = &r.nodes[0];
        assert_eq!(n1.node_id, 1);
        assert_eq!(n1.replica_count, 2);
        assert_eq!(n1.replicas[0].partition_id, 7);
        assert_eq!(n1.replicas[1].partition_id, 8);
        assert!(!r.nodes[1].up, "up 态照抄节点表");
        assert_eq!(r.nodes[2].replica_count, 0, "节点 3 零副本");
        assert!(r.compliant, "这个布局本身没有违反任何 enforced 约束");
    }

    #[test]
    fn same_node_replicas_is_flagged() {
        let nodes = vec![node(1, true, &["d1", "d2"]), node(2, true, &["d3"])];
        let partitions = vec![partition(9, &[(1, "d1"), (1, "d2"), (2, "d3")])];
        let r = compute(&nodes, &partitions);
        assert!(!r.compliant);
        let v: Vec<&Violation> = r
            .violations
            .iter()
            .filter(|v| v.kind == "same_node")
            .collect();
        assert_eq!(v.len(), 1, "同分区同节点聚成一条: {r:?}");
        assert_eq!(v[0].partition_id, Some(9));
        assert_eq!(v[0].node_id, Some(1));
    }

    #[test]
    fn unknown_node_and_unknown_disk_are_flagged() {
        let nodes = vec![node(1, true, &["d1"])];
        let partitions = vec![partition(4, &[(99, "dx")]), partition(5, &[(1, "ghost")])];
        let r = compute(&nodes, &partitions);
        let kinds: Vec<&str> = r.violations.iter().map(|v| v.kind).collect();
        assert!(kinds.contains(&"unknown_node"), "{r:?}");
        assert!(
            r.violations.iter().any(|v| v.kind == "unknown_node"
                && v.partition_id == Some(4)
                && v.node_id == Some(99)),
            "{r:?}"
        );
        assert!(
            r.violations.iter().any(|v| v.kind == "unknown_disk"
                && v.partition_id == Some(5)
                && v.disk_id.as_deref() == Some("ghost")),
            "{r:?}"
        );
        // 幽灵副本有处可去:violation 如实呈现,合计也不丢。
        assert_eq!(r.total_replicas, 2);
        assert_eq!(r.nodes[0].replica_count, 1, "只有 p5 那条落在注册节点上");
    }

    /// 状态事实不是违规:down 节点、BOOTSTRAP 副本(此处用 target/current 差表达)不亮红灯。
    #[test]
    fn operational_states_are_not_violations() {
        let mut p = partition(6, &[(1, "d1"), (2, "d1")]);
        p.replicas[1].target_state = "BOOTSTRAP".into();
        p.replicas[1].up = false;
        let nodes = vec![node(1, true, &["d1"]), node(2, false, &["d1"])];
        let r = compute(&nodes, &[p]);
        assert!(r.compliant, "状态事实不入违规集: {r:?}");
    }

    #[test]
    fn empty_inputs_are_compliant() {
        let r = compute(&[], &[]);
        assert!(r.compliant);
        assert_eq!(r.nodes, vec![]);
        assert_eq!((r.total_partitions, r.total_replicas), (0, 0));
    }
}
