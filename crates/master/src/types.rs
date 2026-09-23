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
///
/// **u32**:blobId 高 32bit 即此 id(低 32bit 为 objectId),见 blobid-master-assign D8。
pub type PartitionId = u32;
/// 对象 id(blobId 低 32bit;master 按 partition 单调发号,永不复用,见 blobid-master-assign)。
pub type ObjectId = u32;
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
        Self {
            default_replication_factor: 3,
        }
    }
}

/// Raft 状态机的**全部** hard state(Q2 边界:只这四块进 Raft)。
///
/// 规模估算(§7.1):~22 万副本 × ~100B ≈ ~20MB,内存放得下、快照几十 MB → `redb` 足够。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MasterState {
    /// 单调 partition id 分配器(类 SeaweedFS `MaxVolumeId`)。
    pub next_partition_id: PartitionId,
    /// per-partition objectId 高水位:下一个可发放的 objectId(号段发放的已提交上界)。
    ///
    /// blobid-master-assign 正确性骨架:抬高水位 SHALL 先提交进 Raft 再据此发号;切主后新主从
    /// 已提交水位续发,永不复用越过水位的号;水位与实际发放间未用的号允许成为空洞。缺省(不在表中)
    /// 即该 partition 尚未发过号,从 0 起。
    pub object_next: BTreeMap<PartitionId, ObjectId>,
    /// 自注册固化的节点身份 + 容量。
    pub nodes: BTreeMap<NodeId, NodeInfo>,
    /// 分区放置(应然)。M1 空。
    pub partitions: BTreeMap<PartitionId, PartitionInfo>,
    /// 集群策略。
    pub policy: Policy,
    /// 下线中的节点(M4):不再作为放置/补副本候选,其副本被逐步迁走。
    pub decommissioning: BTreeSet<NodeId>,
    /// 迁移中的"待迁出"源(MoveReplica,add-before-remove):(pid, node) 表示该节点上该分区的
    /// 副本是迁出源,等新副本补上且健康副本(不含迁出源)≥ RF 后由 reconciler 移除(守 §3.1)。
    /// 新增字段:`serde(default)` 使旧快照/旧持久态(无此字段)可读为空集(§3.4)。
    #[serde(default)]
    pub moving: BTreeSet<(PartitionId, NodeId)>,
    /// 迁移的**继任者**记账:`partition -> (迁出源 -> 目标节点)`(A6)。
    ///
    /// `MoveReplica` 的 apply 顺带写入;`RemoveReplica` 的 apply **不清**(DROPPED 的确认判据要用它),
    /// 等源节点不再上报该副本时由 `ConfirmDrop` 清。语义等价于 `(pid, from) -> to` 的扁平 map;
    /// 用嵌套 map 是持久化格式所限:状态机经 **JSON** 落 `redb`(`store.rs`),而 serde_json 不支持
    /// 元组作 map key(会报 "key must be a string")。
    /// 新增字段:`serde(default)` 使旧快照(无此字段)可读为空表(§3.4)。
    #[serde(default)]
    pub move_targets: BTreeMap<PartitionId, BTreeMap<NodeId, NodeId>>,
    /// 待删源副本(A6):placement 已移除、但源节点盘上数据还在的 (pid, node)。
    ///
    /// `RemoveReplica` 的 apply 顺带插入(与"移除 placement"同一命令,原子);源节点的心跳被确认
    /// 已不再持有该副本后,由 `ConfirmDrop` 摘除。新增字段:`serde(default)` 使旧快照可读为空集(§3.4)。
    #[serde(default)]
    pub pending_drop: BTreeSet<(PartitionId, NodeId)>,
    /// 维护模式(对标 Ceph `osd set noout/norecover`):true 时 reconciler 暂停**自动动作**
    /// (再复制 / 迁移后删 / 下线 evacuation / 自动扩容);观测(心跳、指令下发、指标)与**手动 ops**
    /// (`RepairPartition` / `MoveReplica`)不受影响。
    /// 新增字段:`serde(default)` 使旧快照(无此字段)可读为 false(§3.4)。
    #[serde(default)]
    pub maintenance: bool,
    /// 维护模式是否**连带**暂停自动扩容(默认 false = 维护不连带停扩容)。
    /// 新增字段:`serde(default)` 使旧快照(无此字段)可读为 false(§3.4)。
    #[serde(default)]
    pub pause_autogrow: bool,
    /// 当前均衡计划(计划式均衡,对标 Ceph balancer):`None` = 无计划。
    ///
    /// 计划**只存数据**(谁迁到哪、失败记录),进度(pending/in_flight/done)由 master 从
    /// `partitions` + `moving` + `failed` 派生,不冗余持久化。新增字段:`serde(default)`
    /// 使旧快照(无此字段)可读为 `None`(§3.4)。
    #[serde(default)]
    pub balance: Option<BalancePlanRecord>,
}

/// 均衡计划状态(语义与 proto `BalancePlan.status` 一一对应)。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BalanceStatus {
    /// 待运维确认(EvalBalance 产出,StartBalance 前)。
    Preview,
    /// 执行中(StartBalance 之后,reconciler 按并发上限推进)。
    Running,
    /// 已取消(预留;当前 CancelBalance 直接清计划,不落此态)。
    Cancelled,
    /// 全部 move 进入 done/failed。
    Done,
}

/// 一条均衡搬迁记录(计划内;字段与 proto `BalanceMove` 一一对应)。
///
/// 只存"从哪到哪";该 move 的实时状态(pending/in_flight/done/failed)由
/// [`crate::balance::move_phase`] 从 placement + `moving` + 计划的 `failed` 派生。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceMoveRecord {
    pub partition_id: PartitionId,
    /// 迁出源节点。
    pub from_node_id: NodeId,
    /// 源节点上持有该副本的盘(UI 预览用)。
    pub from_disk_id: DiskId,
    /// 迁入目标节点。
    pub to_node_id: NodeId,
    /// 评估时选定的目标盘(执行期若已不可用,执行器会在目标节点上重选)。
    pub to_disk_id: DiskId,
}

/// 一次均衡计划(进 Raft,可跨切主存活;apply 只存不算,保确定性)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalancePlanRecord {
    /// 计划 id(单调:新计划 = 旧 plan_id + 1,leader 提案时带入)。
    pub plan_id: u64,
    pub status: BalanceStatus,
    /// 创建时间(**leader 提案时带入**,审计/展示用;apply 不取 now)。
    pub created_at_ms: u64,
    /// 执行期同时最多几个在途搬迁(评估时的配置快照)。
    pub max_in_flight: u32,
    /// 计划内的搬迁(每分区至多一条,见 `balance::plan_balance`)。
    pub moves: Vec<BalanceMoveRecord>,
    /// 执行期判败的 move:`(partition, 迁出源, error)`;判败后不再重试该条。
    pub failed: Vec<(PartitionId, NodeId, String)>,
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
            MasterCommand::AllocateObjectId {
                partition_id,
                count,
            } => {
                let start = self.object_next.get(partition_id).copied().unwrap_or(0);
                // checked_add 兜住 u32 空间耗尽:宁可少发最顶端不足一段的号,绝不回绕复用(持久性红线)。
                match start.checked_add(*count) {
                    Some(end) => {
                        self.object_next.insert(*partition_id, end);
                        MasterResponse::ObjectIdRange {
                            partition_id: *partition_id,
                            start,
                            count: *count,
                        }
                    }
                    None => MasterResponse::ObjectIdExhausted {
                        partition_id: *partition_id,
                    },
                }
            }
            MasterCommand::RegisterNode { node_id, info } => {
                // 幂等覆盖:同 node_id 再注册以最新上报为准,不产生重复记录。
                self.nodes.insert(*node_id, info.clone());
                MasterResponse::Registered
            }
            MasterCommand::CreatePartition {
                replication_factor,
                replicas,
            } => {
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
            MasterCommand::AddReplica {
                partition_id,
                replica,
            } => {
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
            MasterCommand::MoveReplica {
                partition_id,
                from_node_id,
                replica,
            } => {
                // add-before-remove:先补新副本(同 AddReplica 幂等),from 源记入 moving,
                // 由 reconciler 在新副本健康后 RemoveReplica(守 §3.1)。
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    let exists = p
                        .replicas
                        .iter()
                        .any(|r| r.node_id == replica.node_id && r.disk_id == replica.disk_id);
                    if !exists {
                        p.replicas.push(replica.clone());
                        self.moving.insert((*partition_id, *from_node_id));
                        // 继任者记账(与 moving 同源):"后删"闸门与 DROPPED 下发都要问它目标是谁、
                        // 目标是否已 ACTIVE(追平完成)。
                        self.move_targets
                            .entry(*partition_id)
                            .or_default()
                            .insert(*from_node_id, replica.node_id);
                    }
                }
                MasterResponse::Registered
            }
            MasterCommand::DecommissionNode { node_id } => {
                self.decommissioning.insert(*node_id);
                MasterResponse::Registered
            }
            MasterCommand::RemoveReplica {
                partition_id,
                node_id,
            } => {
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    p.replicas.retain(|r| r.node_id != *node_id);
                }
                // 顺带清迁移标记:迁出源被移除(或本就不在 moving)都幂等。
                self.moving.remove(&(*partition_id, *node_id));
                // 顺带记"待删源副本":placement 已不含它,但源节点盘上数据还在 → 由心跳下发 DROPPED。
                // 与上面的"移除 placement"同一命令 = 一条 log 原子完成(不会出现"移除了 placement 却不删数据"
                // 的中间态)。**不清 `move_targets`**:DROPPED 的确认判据还要用它问继任者是谁。
                self.pending_drop.insert((*partition_id, *node_id));
                MasterResponse::Registered
            }
            MasterCommand::ConfirmDrop {
                partition_id,
                node_id,
            } => {
                // 源节点已不再上报该副本(= 盘上已删/已不再持有)→ 清两条记账。纯记账清理,不碰 placement。
                self.pending_drop.remove(&(*partition_id, *node_id));
                if let Some(m) = self.move_targets.get_mut(partition_id) {
                    m.remove(node_id);
                    if m.is_empty() {
                        self.move_targets.remove(partition_id);
                    }
                }
                MasterResponse::Registered
            }
            MasterCommand::SetPartitionState {
                partition_id,
                state,
            } => {
                if let Some(p) = self.partitions.get_mut(partition_id) {
                    p.state = *state;
                }
                MasterResponse::Registered
            }
            MasterCommand::SetMaintenance {
                paused,
                pause_autogrow,
                ..
            } => {
                // 纯赋值、幂等(重放同一 log 序列产出同一状态)。`set_at_ms` 只是 leader 的提案
                // 时间戳(审计),apply 不取 `now()`——确定性铁律见模块头。
                self.maintenance = *paused;
                self.pause_autogrow = *pause_autogrow;
                MasterResponse::Registered
            }
            MasterCommand::SetBalancePlan { plan } => {
                // 写/清当前均衡计划:纯赋值,新计划整体替换旧计划(EvalBalance/CancelBalance)。
                self.balance = plan.clone();
                MasterResponse::Registered
            }
            MasterCommand::SetBalanceStatus { plan_id, status } => {
                // 只认当前计划:plan_id 不匹配 = 陈旧提案(计划已被替换/清掉),直接忽略。
                if let Some(p) = self.balance.as_mut() {
                    if p.plan_id == *plan_id {
                        p.status = *status;
                    }
                }
                MasterResponse::Registered
            }
            MasterCommand::FailBalanceMove {
                plan_id,
                partition_id,
                from_node_id,
                error,
            } => {
                // 判败一条 move(执行器:在途超时/落点失效)。幂等:同 (pid, from) 已记败则覆盖
                // error(重放同命令产出同状态);判败后执行器不再重试该条。
                if let Some(p) = self.balance.as_mut() {
                    if p.plan_id == *plan_id {
                        match p
                            .failed
                            .iter_mut()
                            .find(|(pid, from, _)| pid == partition_id && from == from_node_id)
                        {
                            Some(e) => e.2 = error.clone(),
                            None => p.failed.push((*partition_id, *from_node_id, error.clone())),
                        }
                    }
                }
                MasterResponse::Registered
            }
        }
    }

    /// 某 `(partition, 迁出源)` 的迁移目标节点(继任者);无记账 = `None`(A6)。
    ///
    /// 无记账意味着这条删除不是 `MoveReplica` 的"后删"(典型是 evacuation):此时 DROPPED 的确认
    /// 退化为"其余副本数 ≥ RF 且无 ERROR"的冗余判据,见
    /// [`reconcile::drop_confirmed`](crate::reconcile::drop_confirmed)。
    pub fn move_target_of(&self, pid: PartitionId, from: NodeId) -> Option<NodeId> {
        self.move_targets
            .get(&pid)
            .and_then(|m| m.get(&from))
            .copied()
    }
}

/// 经 `client_write` 提案的应用命令(M1 仅两个;M2+ 扩 CreatePartition 等)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasterCommand {
    /// 分配一个单调 partition id。
    AllocatePartitionId,
    /// 为某 partition 预留一段 objectId `[start, start+count)`:先经 Raft 提交抬高水位,再由 leader
    /// 切成号段发给 client(两层批处理见 blobid-master-assign)。
    AllocateObjectId {
        partition_id: PartitionId,
        count: u32,
    },
    /// 固化(或覆盖)一个节点的身份 + 容量。
    RegisterNode { node_id: NodeId, info: NodeInfo },
    /// 建分区:分配 id + 写入已选定的副本放置(选盘在 leader 侧先算,见 `placement`)。
    CreatePartition {
        replication_factor: u8,
        replicas: Vec<ReplicaPlacement>,
    },
    /// 再复制:向已有分区追加一个副本(恢复冗余,M3;选盘在 leader 侧先算)。
    AddReplica {
        partition_id: PartitionId,
        replica: ReplicaPlacement,
    },
    /// 标记节点下线(M4):加入 `decommissioning`,不再作为放置候选。
    DecommissionNode { node_id: NodeId },
    /// 移除分区的一个副本(M4 evacuation 的"删",幂等)。
    RemoveReplica {
        partition_id: PartitionId,
        node_id: NodeId,
    },
    /// 副本迁移(MoveReplica):追加新副本(replica)+ 把 from 源记入 `moving`(add-before-remove;
    /// 选盘在 leader 侧先算)。
    MoveReplica {
        partition_id: PartitionId,
        from_node_id: NodeId,
        replica: ReplicaPlacement,
    },
    /// 设置分区级写闸门状态(RW/RO/SEALED;admin 运维,对齐 §7.3 盘满转 RO)。
    SetPartitionState {
        partition_id: PartitionId,
        state: PartitionState,
    },
    /// 确认某源副本已删(A6):源节点心跳不再上报该 `(pid, node)` → 清 `pending_drop` 与
    /// `move_targets` 记账(纯记账清理,不改 placement;幂等)。
    ConfirmDrop {
        partition_id: PartitionId,
        node_id: NodeId,
    },
    /// 开关维护模式(对标 Ceph `osd set/unset noout`):`paused` 暂停自动调谐动作,
    /// `pause_autogrow` 是否连带暂停自动扩容。`set_at_ms` 由 **leader 提案时带入**
    /// (审计用;apply 内不取时钟,见模块头确定性铁律)。
    SetMaintenance {
        paused: bool,
        pause_autogrow: bool,
        set_at_ms: u64,
    },
    /// 写/清当前均衡计划(计划式均衡):`Some` = 写入(整体替换旧计划),`None` = 清除
    /// (CancelBalance;**不回滚**已受理的 move,在途迁移由 reconciler 既有"后删"自然收敛)。
    SetBalancePlan { plan: Option<BalancePlanRecord> },
    /// 推进当前计划状态(PREVIEW→RUNNING→DONE);`plan_id` 不匹配当前计划则忽略(防陈旧提案)。
    SetBalanceStatus { plan_id: u64, status: BalanceStatus },
    /// 判败计划内一条 move(记录 error;判败后不再重试)。`plan_id` 不匹配则忽略。
    FailBalanceMove {
        plan_id: u64,
        partition_id: PartitionId,
        from_node_id: NodeId,
        error: String,
    },
}

/// 状态机对每条已提交 log 的响应。
///
/// `Empty` 对应 membership / blank 等非应用 entry —— openraft 的 `apply` 要求对**每条** entry
/// 返回一个 `C::R`(含 membership 与空 entry)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasterResponse {
    /// `AllocatePartitionId` 分得的 id。
    PartitionId(PartitionId),
    /// `AllocateObjectId` 预留的 objectId 号段 `[start, start+count)`(已提交进水位)。
    ObjectIdRange {
        partition_id: PartitionId,
        start: ObjectId,
        count: u32,
    },
    /// `AllocateObjectId` 时该 partition 的 u32 objectId 空间已耗尽(应改为封存该 partition,不复用)。
    ObjectIdExhausted { partition_id: PartitionId },
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
        assert_eq!(
            s.apply_command(&MasterCommand::AllocatePartitionId),
            MasterResponse::PartitionId(0)
        );
        assert_eq!(
            s.apply_command(&MasterCommand::AllocatePartitionId),
            MasterResponse::PartitionId(1)
        );
        assert_eq!(
            s.apply_command(&MasterCommand::AllocatePartitionId),
            MasterResponse::PartitionId(2)
        );
        assert_eq!(s.next_partition_id, 3);
    }

    #[test]
    fn allocate_object_id_is_monotonic_and_partition_scoped() {
        let mut s = MasterState::default();
        let alloc = |s: &mut MasterState, pid, n| {
            s.apply_command(&MasterCommand::AllocateObjectId {
                partition_id: pid,
                count: n,
            })
        };
        assert_eq!(
            alloc(&mut s, 7, 1024),
            MasterResponse::ObjectIdRange {
                partition_id: 7,
                start: 0,
                count: 1024
            }
        );
        assert_eq!(
            alloc(&mut s, 7, 512),
            MasterResponse::ObjectIdRange {
                partition_id: 7,
                start: 1024,
                count: 512
            }
        );
        // 另一 partition 独立从 0 起,互不干扰。
        assert_eq!(
            alloc(&mut s, 9, 100),
            MasterResponse::ObjectIdRange {
                partition_id: 9,
                start: 0,
                count: 100
            }
        );
        assert_eq!(s.object_next[&7], 1536);
        assert_eq!(s.object_next[&9], 100);
    }

    #[test]
    fn allocate_object_id_guards_u32_overflow_without_wrapping() {
        let mut s = MasterState::default();
        s.object_next.insert(3, u32::MAX - 10);
        // 越界申请 → 报耗尽,水位不动(绝不回绕复用)。
        assert_eq!(
            s.apply_command(&MasterCommand::AllocateObjectId {
                partition_id: 3,
                count: 100
            }),
            MasterResponse::ObjectIdExhausted { partition_id: 3 }
        );
        assert_eq!(
            s.object_next[&3],
            u32::MAX - 10,
            "exhausted alloc must not advance watermark"
        );
    }

    #[test]
    fn register_node_is_idempotent_overwrite() {
        let mut s = MasterState::default();
        let mk = |cap: u64| NodeInfo {
            dc: "dc1".into(),
            rack: "r1".into(),
            addr: "127.0.0.1:7000".into(),
            disks: BTreeMap::from([(
                "d0".to_string(),
                DiskSpec {
                    capacity_bytes: cap,
                },
            )]),
            registered_at_ms: 100,
        };
        s.apply_command(&MasterCommand::RegisterNode {
            node_id: 7,
            info: mk(10),
        });
        s.apply_command(&MasterCommand::RegisterNode {
            node_id: 7,
            info: mk(20),
        });
        assert_eq!(s.nodes.len(), 1, "same node_id must not duplicate");
        assert_eq!(
            s.nodes[&7].disks["d0"].capacity_bytes, 20,
            "latest report wins"
        );
    }

    #[test]
    fn create_partition_allocates_id_and_writes_placement() {
        let mut s = MasterState::default();
        let replicas = vec![
            ReplicaPlacement {
                node_id: 1,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            },
            ReplicaPlacement {
                node_id: 2,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            },
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
            replica: ReplicaPlacement {
                node_id: 2,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            },
        };
        s.apply_command(&add);
        assert_eq!(s.partitions[&0].replicas.len(), 2);
        s.apply_command(&add);
        assert_eq!(
            s.partitions[&0].replicas.len(),
            2,
            "AddReplica must be idempotent"
        );
    }

    #[test]
    fn decommission_and_remove_replica() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 1,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 2,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        });
        s.apply_command(&MasterCommand::DecommissionNode { node_id: 2 });
        assert!(s.decommissioning.contains(&2));
        let rm = MasterCommand::RemoveReplica {
            partition_id: 0,
            node_id: 2,
        };
        s.apply_command(&rm);
        assert_eq!(s.partitions[&0].replicas.len(), 1);
        assert_eq!(s.partitions[&0].replicas[0].node_id, 1);
        s.apply_command(&rm);
        assert_eq!(
            s.partitions[&0].replicas.len(),
            1,
            "RemoveReplica must be idempotent"
        );
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
        assert_eq!(
            s.partitions[&0].state,
            PartitionState::ReadOnly,
            "gate updated to RO"
        );
    }

    #[test]
    fn set_maintenance_toggles_flags_and_is_idempotent() {
        let mut s = MasterState::default();
        assert!(
            !s.maintenance && !s.pause_autogrow,
            "fresh state is not in maintenance"
        );

        let on = MasterCommand::SetMaintenance {
            paused: true,
            pause_autogrow: false,
            set_at_ms: 7,
        };
        s.apply_command(&on);
        assert!(s.maintenance, "maintenance must be on");
        assert!(
            !s.pause_autogrow,
            "auto-grow stays enabled unless requested"
        );

        // 幂等:重放同一命令不改变结果。
        s.apply_command(&on);
        assert!(s.maintenance && !s.pause_autogrow);

        // 同一命令可连带暂停扩容。
        s.apply_command(&MasterCommand::SetMaintenance {
            paused: true,
            pause_autogrow: true,
            set_at_ms: 9,
        });
        assert!(s.maintenance && s.pause_autogrow);

        // 关闭维护:两个标志一并清零(不留 pause_autogrow 残值)。
        s.apply_command(&MasterCommand::SetMaintenance {
            paused: false,
            pause_autogrow: false,
            set_at_ms: 11,
        });
        assert!(!s.maintenance && !s.pause_autogrow);
    }

    #[test]
    fn move_replica_adds_new_and_marks_source_moving() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 1,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 2,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        });
        let mv = MasterCommand::MoveReplica {
            partition_id: 0,
            from_node_id: 2,
            replica: ReplicaPlacement {
                node_id: 3,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            },
        };
        s.apply_command(&mv);
        assert_eq!(
            s.partitions[&0].replicas.len(),
            3,
            "new replica added before remove"
        );
        assert!(
            s.moving.contains(&(0, 2)),
            "source must be marked moving-out"
        );
        assert_eq!(
            s.move_target_of(0, 2),
            Some(3),
            "successor must be recorded for the later drop"
        );
        // 幂等:重放同命令不重复追加、不重复标记。
        s.apply_command(&mv);
        assert_eq!(s.partitions[&0].replicas.len(), 3);
        assert_eq!(s.moving.len(), 1);
        // RemoveReplica 顺带清 moving,并记一条待删源副本(等源节点不再上报后 ConfirmDrop 摘除)。
        s.apply_command(&MasterCommand::RemoveReplica {
            partition_id: 0,
            node_id: 2,
        });
        assert_eq!(s.partitions[&0].replicas.len(), 2);
        assert!(
            s.moving.is_empty(),
            "removing the source must clear its moving mark"
        );
        assert!(
            s.pending_drop.contains(&(0, 2)),
            "removal must record a pending drop for the source"
        );
        assert_eq!(
            s.move_target_of(0, 2),
            Some(3),
            "successor bookkeeping must survive until the drop is confirmed"
        );
    }

    #[test]
    fn remove_replica_records_pending_drop_and_confirm_drop_clears_it() {
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 1,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 2,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        });
        // evacuation 式移除(无 move 记账):也要记待删,靠冗余判据确认。
        s.apply_command(&MasterCommand::RemoveReplica {
            partition_id: 0,
            node_id: 2,
        });
        assert_eq!(
            s.pending_drop.iter().copied().collect::<Vec<_>>(),
            vec![(0, 2)]
        );
        assert_eq!(
            s.move_target_of(0, 2),
            None,
            "no move bookkeeping for an evacuation removal"
        );

        let confirm = MasterCommand::ConfirmDrop {
            partition_id: 0,
            node_id: 2,
        };
        s.apply_command(&confirm);
        assert!(
            s.pending_drop.is_empty(),
            "confirmed drop must clear pending_drop"
        );
        // 幂等:重放确认命令不改变状态。
        s.apply_command(&confirm);
        assert!(s.pending_drop.is_empty());
        assert!(
            s.move_targets.is_empty(),
            "empty successor bookkeeping must be pruned"
        );
    }

    #[test]
    fn stored_state_without_drop_bookkeeping_fields_deserializes() {
        // §3.4 后向兼容:旧格式 JSON(含 moving/maintenance,但无 move_targets/pending_drop)必须能读。
        let old_json = serde_json::json!({
            "next_partition_id": 3,
            "object_next": {},
            "nodes": {},
            "partitions": {},
            "policy": { "default_replication_factor": 3 },
            "decommissioning": [],
            "moving": [[0, 101]],
            "maintenance": true,
            "pause_autogrow": false
        });
        let s: MasterState = serde_json::from_value(old_json).expect("old format must deserialize");
        assert!(
            s.move_targets.is_empty(),
            "missing move_targets must default to empty"
        );
        assert!(
            s.pending_drop.is_empty(),
            "missing pending_drop must default to empty"
        );
        assert!(s.moving.contains(&(0, 101)));
        assert!(s.maintenance);
    }

    #[test]
    fn drop_bookkeeping_round_trips_through_json() {
        // 状态机经 JSON 落 redb:新字段必须能序列化再读回(元组 key 在 serde_json 里不可用,
        // 故 move_targets 用嵌套 map —— 这条测试就是该约束的回归防线)。
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::CreatePartition {
            replication_factor: 1,
            replicas: vec![ReplicaPlacement {
                node_id: 1,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            }],
        });
        s.apply_command(&MasterCommand::MoveReplica {
            partition_id: 0,
            from_node_id: 1,
            replica: ReplicaPlacement {
                node_id: 2,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
            },
        });
        s.apply_command(&MasterCommand::RemoveReplica {
            partition_id: 0,
            node_id: 1,
        });

        let bytes =
            serde_json::to_vec(&s).expect("state must serialize (nested map, no tuple keys)");
        let back: MasterState = serde_json::from_slice(&bytes).expect("must deserialize");
        assert_eq!(back, s, "drop bookkeeping must survive a JSON round trip");
        assert_eq!(back.move_target_of(0, 1), Some(2));
        assert!(back.pending_drop.contains(&(0, 1)));
    }

    #[test]
    fn stored_state_without_moving_field_deserializes() {
        // §3.4 后向兼容:旧格式 JSON(无 `moving` 字段)必须能读,默认空集。
        let old_json = serde_json::json!({
            "next_partition_id": 1,
            "object_next": {},
            "nodes": {},
            "partitions": {},
            "policy": { "default_replication_factor": 3 },
            "decommissioning": []
        });
        let s: MasterState = serde_json::from_value(old_json).expect("old format must deserialize");
        assert!(s.moving.is_empty());
        assert_eq!(s.next_partition_id, 1);
    }

    #[test]
    fn stored_state_without_maintenance_fields_deserializes() {
        // §3.4 后向兼容:旧格式 JSON(含 `moving`,但无 `maintenance`/`pause_autogrow`)必须能读,
        // 两标志默认 false(维护模式视作关闭)。
        let old_json = serde_json::json!({
            "next_partition_id": 2,
            "object_next": {},
            "nodes": {},
            "partitions": {},
            "policy": { "default_replication_factor": 3 },
            "decommissioning": [],
            "moving": [[0, 101]]
        });
        let s: MasterState = serde_json::from_value(old_json).expect("old format must deserialize");
        assert!(!s.maintenance, "missing maintenance must default to false");
        assert!(
            !s.pause_autogrow,
            "missing pause_autogrow must default to false"
        );
        assert!(s.moving.contains(&(0, 101)));
    }

    #[test]
    fn stored_state_without_balance_field_deserializes() {
        // §3.4 后向兼容:旧格式 JSON(无 `balance` 字段)必须能读,默认 None(无计划)。
        let old_json = serde_json::json!({
            "next_partition_id": 2,
            "object_next": {},
            "nodes": {},
            "partitions": {},
            "policy": { "default_replication_factor": 3 },
            "decommissioning": [],
            "moving": [],
            "maintenance": false,
            "pause_autogrow": false
        });
        let s: MasterState = serde_json::from_value(old_json).expect("old format must deserialize");
        assert_eq!(s.balance, None, "missing balance must default to None");
    }

    fn preview_plan(plan_id: u64) -> BalancePlanRecord {
        BalancePlanRecord {
            plan_id,
            status: BalanceStatus::Preview,
            created_at_ms: 42,
            max_in_flight: 1,
            moves: vec![BalanceMoveRecord {
                partition_id: 7,
                from_node_id: 101,
                from_disk_id: "d0".into(),
                to_node_id: 104,
                to_disk_id: "d0".into(),
            }],
            failed: vec![],
        }
    }

    #[test]
    fn balance_plan_commands_are_deterministic_and_plan_id_scoped() {
        let mut s = MasterState::default();
        // 写入计划。
        s.apply_command(&MasterCommand::SetBalancePlan {
            plan: Some(preview_plan(1)),
        });
        assert_eq!(
            s.balance.as_ref().map(|p| p.status),
            Some(BalanceStatus::Preview)
        );
        // 陈旧 plan_id 的状态推进被忽略。
        s.apply_command(&MasterCommand::SetBalanceStatus {
            plan_id: 99,
            status: BalanceStatus::Running,
        });
        assert_eq!(
            s.balance.as_ref().map(|p| p.status),
            Some(BalanceStatus::Preview)
        );
        // 匹配 plan_id → 推进。
        s.apply_command(&MasterCommand::SetBalanceStatus {
            plan_id: 1,
            status: BalanceStatus::Running,
        });
        assert_eq!(
            s.balance.as_ref().map(|p| p.status),
            Some(BalanceStatus::Running)
        );
        // 判败一条 move;重放幂等(不重复追加)。
        let fail = MasterCommand::FailBalanceMove {
            plan_id: 1,
            partition_id: 7,
            from_node_id: 101,
            error: "move stuck".into(),
        };
        s.apply_command(&fail);
        s.apply_command(&fail);
        let p = s.balance.as_ref().expect("plan present");
        assert_eq!(
            p.failed.len(),
            1,
            "replay must not duplicate the failure record"
        );
        assert_eq!(p.failed[0], (7, 101, "move stuck".to_string()));
        // 陈旧 plan_id 的判败被忽略。
        s.apply_command(&MasterCommand::FailBalanceMove {
            plan_id: 99,
            partition_id: 8,
            from_node_id: 102,
            error: "stale".into(),
        });
        assert_eq!(s.balance.as_ref().expect("plan").failed.len(), 1);
        // 清计划。
        s.apply_command(&MasterCommand::SetBalancePlan { plan: None });
        assert_eq!(s.balance, None);
        // 计划已清:状态推进/判败都打不到任何计划。
        s.apply_command(&MasterCommand::SetBalanceStatus {
            plan_id: 1,
            status: BalanceStatus::Done,
        });
        assert_eq!(s.balance, None);
    }

    #[test]
    fn balance_plan_round_trips_through_json() {
        // 状态机经 JSON 落 redb:计划必须能序列化再读回。
        let mut s = MasterState::default();
        s.apply_command(&MasterCommand::SetBalancePlan {
            plan: Some(preview_plan(3)),
        });
        s.apply_command(&MasterCommand::FailBalanceMove {
            plan_id: 3,
            partition_id: 7,
            from_node_id: 101,
            error: "timeout".into(),
        });
        let bytes = serde_json::to_vec(&s).expect("state must serialize");
        let back: MasterState = serde_json::from_slice(&bytes).expect("must deserialize");
        assert_eq!(back, s, "balance plan must survive a JSON round trip");
    }

    #[test]
    fn apply_is_deterministic_across_replay() {
        let cmds = vec![
            MasterCommand::AllocatePartitionId,
            MasterCommand::RegisterNode {
                node_id: 1,
                info: NodeInfo {
                    dc: "d".into(),
                    rack: "r".into(),
                    addr: "a:1".into(),
                    disks: BTreeMap::new(),
                    registered_at_ms: 5,
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
