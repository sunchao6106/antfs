//! master 的 Raft 类型:应用命令 / 响应、hard-state 数据模型、`RaftTypeConfig`。
//!
//! 对齐 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §7.1(Q2 边界)与
//! `openspec/changes/master-node/design.md` §2。**只有"必须一致 + 需 failover 存活"的进 Raft。**
//!
//! 确定性铁律(§design 2):状态机 `apply` 必须确定 —— 同一 log 序列在任意节点产出同一状态,
//! 不依赖本地时钟 / 随机。故 `registered_at_ms` 由 leader 在提案里带入,不在 `apply` 里取 `now()`。

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use serde::{Deserialize, Serialize};

/// 集群节点 id(openraft `NodeId`,取默认 `u64`)。
pub type NodeId = u64;
/// 分区 id(自带定位,D3;单调分配,永不复用)。
pub type PartitionId = u64;
/// 磁盘 id(节点内唯一,通常是盘的挂载标识)。
pub type DiskId = String;

/// 一块盘的固化规格(自注册时上报,进 Raft `Nodes`)。
///
/// `capacity_bytes` 是 `statfs` 探到的**真实总容量**(Q1),不是写死配置。实时余量(free)是
/// 心跳软状态,不进这里。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskSpec {
    /// 盘总容量(字节),节点 `statfs` 探得。
    pub capacity_bytes: u64,
}

/// 节点身份(自注册后固化进 Raft `Nodes`)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// 数据中心。
    pub dc: String,
    /// 机架(故障域,放置打散用)。
    pub rack: String,
    /// 数据面 + 控制面地址(host:port)。
    pub addr: String,
    /// 该节点挂载的盘。
    pub disks: BTreeMap<DiskId, DiskSpec>,
    /// 首次注册时间(**leader 提案时带入**,保 apply 确定性)。
    pub registered_at_ms: u64,
}

/// 分区级写入闸门(承 Ambry)。M1 不写入 `Partitions`,此处为 M2 预留。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    /// 可读可写。
    ReadWrite,
    /// 只读(盘将满 / 手动封存)。
    ReadOnly,
    /// 已封存(不再接受新写,可转 EC)。
    Sealed,
}

/// 一个副本的应然放置(ideal placement,M2 起写入)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    pub node_id: NodeId,
    pub disk_id: DiskId,
    /// master 决定的目标副本态(§7.2;M1 折叠为 `ACTIVE`,Q4)。
    pub target_state: String,
}

/// 分区放置(应然)。M1 空,M2 起写入。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub replication_factor: u8,
    pub state: PartitionState,
    pub replicas: Vec<ReplicaPlacement>,
}

/// 集群放置策略。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub default_replication_factor: u8,
}

impl Default for Policy {
    fn default() -> Self {
        // 默认 3 副本(对齐 cluster.md §10 W=2/R=1 建立在 RF=3 之上)。
        Self { default_replication_factor: 3 }
    }
}

/// Raft 状态机的**全部** hard state(Q2 边界:只这四块进 Raft)。
///
/// 规模估算(§7.1):~22 万副本 × ~100B ≈ ~20MB,内存放得下、快照几十 MB → `redb` 足够。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MasterState {
    /// 单调 partition id 分配器(类 SeaweedFS `MaxVolumeId`)。
    pub next_partition_id: PartitionId,
    /// 自注册固化的节点身份 + 容量。
    pub nodes: BTreeMap<NodeId, NodeInfo>,
    /// 分区放置(应然)。M1 空。
    pub partitions: BTreeMap<PartitionId, PartitionInfo>,
    /// 集群策略。
    pub policy: Policy,
    /// 下线中的节点(M4):不再作为放置/补副本候选,其副本被逐步迁走。
    pub decommissioning: BTreeSet<NodeId>,
}

impl MasterState {
    /// 确定性地应用一条命令,返回响应。**不得**引入时钟 / 随机 / 迭代顺序不确定性。
    ///
    /// 幂等(§design 2):`RegisterNode` 以 `node_id` 覆盖(重放安全);`AllocatePartitionId`
    /// 经 Raft 线性化后单调自增(不重不漏)。
    pub fn apply_command(&mut self, cmd: &MasterCommand) -> MasterResponse {
        match cmd {
            MasterCommand::AllocatePartitionId => {
                let id = self.next_partition_id;
                self.next_partition_id += 1;
                MasterResponse::PartitionId(id)
            }
            MasterCommand::RegisterNode { node_id, info } => {
                // 幂等覆盖:同 node_id 再注册以最新上报为准,不产生重复记录。
                self.nodes.insert(*node_id, info.clone());
                MasterResponse::Registered
            }
            MasterCommand::CreatePartition { replication_factor, replicas } => {
                // 选盘在 leader 侧已算好(读软状态,非确定);apply 只做确定动作:分配 id + 写 placement。
                let id = self.next_partition_id;
                self.next_partition_id += 1;
                self.partitions.insert(
                    id,
                    PartitionInfo {
                        replication_factor: *replication_factor,
                        state: PartitionState::ReadWrite,
                        replicas: replicas.clone(),
                    },
                );
                MasterResponse::PartitionId(id)
            }
            MasterCommand::AddReplica { partition_id, replica } => {
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    // 幂等:同 (node, disk) 已在该分区则不重复追加。
                    let exists = p
                        .replicas
                        .iter()
                        .any(|r| r.node_id == replica.node_id && r.disk_id == replica.disk_id);
                    if !exists {
                        p.replicas.push(replica.clone());
                    }
                }
                MasterResponse::Registered
            }
            MasterCommand::DecommissionNode { node_id } => {
                self.decommissioning.insert(*node_id);
                MasterResponse::Registered
            }
            MasterCommand::RemoveReplica { partition_id, node_id } => {
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    p.replicas.retain(|r| r.node_id != *node_id);
                }
                MasterResponse::Registered
            }
            MasterCommand::SetPartitionState { partition_id, state } => {
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    p.state = *state;
                }
                MasterResponse::Registered
            }
        }
    }
}

/// 经 `client_write` 提案的应用命令(M1 仅两个;M2+ 扩 CreatePartition 等)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasterCommand {
    /// 分配一个单调 partition id。
    AllocatePartitionId,
    /// 固化(或覆盖)一个节点的身份 + 容量。
    RegisterNode { node_id: NodeId, info: NodeInfo },
    /// 建分区:分配 id + 写入已选定的副本放置(选盘在 leader 侧先算,见 `placement`)。
    CreatePartition { replication_factor: u8, replicas: Vec<ReplicaPlacement> },
    /// 再复制:向已有分区追加一个副本(恢复冗余,M3;选盘在 leader 侧先算)。
    AddReplica { partition_id: PartitionId, replica: ReplicaPlacement },
    /// 标记节点下线(M4):加入 `decommissioning`,不再作为放置候选。
    DecommissionNode { node_id: NodeId },
    /// 移除分区的一个副本(M4 evacuation 的"删",幂等)。
    RemoveReplica { partition_id: PartitionId, node_id: NodeId },
    /// 设置分区级写闸门状态(RW/RO/SEALED;admin 运维,对齐 §7.3 盘满转 RO)。
    SetPartitionState { partition_id: PartitionId, state: PartitionState },
}

/// 状态机对每条已提交 log 的响应。
///
/// `Empty` 对应 membership / blank 等非应用 entry —— openraft 的 `apply` 要求对**每条** entry
/// 返回一个 `C::R`(含 membership 与空 entry)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasterResponse {
    /// `AllocatePartitionId` 分得的 id。
    PartitionId(PartitionId),
    /// `RegisterNode` 成功。
    Registered,
    /// 非应用 entry(membership / blank)。
    Empty,
}

openraft::declare_raft_types!(
    /// master 的 openraft 类型配置。其余关联类型取宏默认:
    /// `NodeId = u64`、`Node = BasicNode`、`Entry = Entry<Self>`、
    /// `SnapshotData = Cursor<Vec<u8>>`、`AsyncRuntime = TokioRuntime`。
    pub TypeConfig:
        D = MasterCommand,
        R = MasterResponse,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_partition_id_is_monotonic() {
        let mut s = MasterState::default();
        assert_eq!(s.apply_command(&MasterCommand::AllocatePartitionId), MasterResponse::PartitionId(0));
        assert_eq!(s.apply_command(&MasterCommand::AllocatePartitionId), MasterResponse::PartitionId(1));
        assert_eq!(s.apply_command(&MasterCommand::AllocatePartitionId), MasterResponse::PartitionId(2));
        assert_eq!(s.next_partition_id, 3);
    }

    #[test]
    fn register_node_is_idempotent_overwrite() {
        let mut s = MasterState::default();
        let mk = |cap: u64| NodeInfo {
            dc: "dc1".into(),
            rack: "r1".into(),
            addr: "127.0.0.1:7000".into(),
            disks: BTreeMap::from([("d0".to_string(), DiskSpec { capacity_bytes: cap })]),
            registered_at_ms: 100,
        };
        s.apply_command(&MasterCommand::RegisterNode { node_id: 7, info: mk(10) });
        s.apply_command(&MasterCommand::RegisterNode { node_id: 7, info: mk(20) });
        assert_eq!(s.nodes.len(), 1, "same node_id must not duplicate");
        assert_eq!(s.nodes[&7].disks["d0"].capacity_bytes, 20, "latest report wins");
    }

    #[test]
    fn create_partition_allocates_id_and_writes_placement() {
        let mut s = MasterState::default();
        let replicas = vec![
            ReplicaPlacement { node_id: 1, disk_id: "d0".into(), target_state: "ACTIVE".into() },
            ReplicaPlacement { node_id: 2, disk_id: "d0".into(), target_state: "ACTIVE".into() },
        ];
        let r = s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: replicas.clone(),
        });
        assert_eq!(r, MasterResponse::PartitionId(0));
        assert_eq!(s.next_partition_id, 1);
        let p = &s.partitions[&0];
        assert_eq!(p.replication_factor, 2);
        assert_eq!(p.state, PartitionState::ReadWrite);
        assert_eq!(p.replicas, replicas);
    }

    #[test]
    fn add_replica_appends_and_is_idempotent() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![ReplicaPlacement {
                node_id: 1,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            }],
        });
        let add = MasterCommand::AddReplica {
            partition_id: 0,
            replica: ReplicaPlacement { node_id: 2, disk_id: "d0".into(), target_state: "ACTIVE".into() },
        };
        s.apply_command(&add);
        assert_eq!(s.partitions[&0].replicas.len(), 2);
        s.apply_command(&add);
        assert_eq!(s.partitions[&0].replicas.len(), 2, "AddReplica must be idempotent");
    }

    #[test]
    fn decommission_and_remove_replica() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![
                ReplicaPlacement { node_id: 1, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 2, disk_id: "d0".into(), target_state: "ACTIVE".into() },
            ],
        });
        s.apply_command(&MasterCommand::DecommissionNode { node_id: 2 });
        assert!(s.decommissioning.contains(&2));
        let rm = MasterCommand::RemoveReplica { partition_id: 0, node_id: 2 };
        s.apply_command(&rm);
        assert_eq!(s.partitions[&0].replicas.len(), 1);
        assert_eq!(s.partitions[&0].replicas[0].node_id, 1);
        s.apply_command(&rm);
        assert_eq!(s.partitions[&0].replicas.len(), 1, "RemoveReplica must be idempotent");
    }

    #[test]
    fn set_partition_state_changes_gate() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 1,
            replicas: vec![ReplicaPlacement {
                node_id: 1,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            }],
        });
        assert_eq!(s.partitions[&0].state, PartitionState::ReadWrite);
        s.apply_command(&MasterCommand::SetPartitionState {
            partition_id: 0,
            state: PartitionState::ReadOnly,
        });
        assert_eq!(s.partitions[&0].state, PartitionState::ReadOnly, "gate updated to RO");
    }

    #[test]
    fn apply_is_deterministic_across_replay() {
        let cmds = vec![
            MasterCommand::AllocatePartitionId,
            MasterCommand::RegisterNode {
                node_id: 1,
                info: NodeInfo {
                    dc: "d".into(), rack: "r".into(), addr: "a:1".into(),
                    disks: BTreeMap::new(), registered_at_ms: 5,
                },
            },
            MasterCommand::AllocatePartitionId,
        ];
        let mut a = MasterState::default();
        let mut b = MasterState::default();
        for c in &cmds {
            a.apply_command(c);
        }
        for c in &cmds {
            b.apply_command(c);
        }
        assert_eq!(a, b, "same command sequence must yield identical state");
    }
}
