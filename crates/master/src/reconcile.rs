//! M3 调谐的**纯决策函数**(承 §7.2/§7.3):转移下发 + 再复制判定 + 墓碑回收闸门。无 IO/时钟/随机。
//!
//! master 只决策(D1):这里算出"该给节点下发哪些转移"、"哪些分区需补副本"和"某分区此刻能否回收
//! 墓碑",执行/搬数据/回收在 store server。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::types::{DiskId, ReplicaPlacement};

use crate::types::{NodeId, PartitionId, PartitionInfo, PartitionState};

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

/// 为某节点算出待下发的转移:该节点在 placement 中承载的副本里,上报 current 与 target 不一致的。
///
/// `node_pids`:placement 侧该节点涉及的分区集(派生索引 —— 心跳每拍 O(本节点副本数) 的关键,
/// 不扫全集群 partitions;索引的重建与保鲜见 server 侧 `PlacementIndex`)。
/// `reported`:该节点软状态视图 `partition_id -> current_state`;缺失(未上报)视为需要转移 ——
/// "placement 有、节点没上报"(如本地数据丢失)必须仍下发重建指令。
pub fn plan_transitions(
    node_id: NodeId,
    node_pids: &BTreeSet<PartitionId>,
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    reported: &BTreeMap<PartitionId, String>,
) -> Vec<Transition> {
    let mut out = Vec::new();
    for pid in node_pids {
        let Some(info) = partitions.get(pid) else {
            continue;
        };
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

/// 为某节点算出其承载副本的**分区状态**(RW/RO/SEALED),随心跳下行给 store 端挑 compaction 候选集。
///
/// 只报 `reported` 里出现过的分区(= 该节点确实承载副本、本拍有上报),缺失的不臆造;
/// 状态取 master 状态机的权威值(store 侧不自行推断可写性)。
///
/// 同一份 `PartitionStateInfo` 里的墓碑回收闸门由 [`tombstones_reclaim_allowed`] 单独算
/// (它要的是"该分区全副本是否追平",与本节点上报与否无关)。
pub fn plan_partition_states(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    reported: &BTreeMap<PartitionId, String>,
) -> Vec<(PartitionId, PartitionState)> {
    reported
        .keys()
        .filter_map(|pid| partitions.get(pid).map(|info| (*pid, info.state)))
        .collect()
}

/// 墓碑回收闸门:该分区**此刻可否回收墓碑**(compaction 会回收删除标记,故必须先确认删除已传播到所有副本)。
///
/// **为什么是闸门而不是时间水位**:master 不知道"每个副本看到了哪个删除时刻",但知道"是否还有副本没追平",
/// 所以下发的是一道 bool。`true` ⟺ placement 里的**每个副本**都"已追平"。
///
/// `replica_caught_up`:单个副本是否已追平,由调用方用软状态给出(节点存活 **且** 上报
/// `current_state == "ACTIVE"` 且 `lag_bytes == 0`;缺上报 / 节点 down 都算未追平)。
///
/// **保守性**:只要有一个副本没追平 → 整体不放行。墓碑一旦被回收,滞后副本就永远学不到那条删除,
/// 已删对象会以"活数据"永久留在它上面(僵尸数据)。无副本(空 placement)也返回 `false` ——
/// 没有任何副本能证明删除已传播出去,"看不见"不等于"已传播";宁可让墓碑多留一拍(追平后自然放行)。
pub fn tombstones_reclaim_allowed(
    part: &PartitionInfo,
    replica_caught_up: impl Fn(NodeId) -> bool,
) -> bool {
    // 无副本 → 没有任何副本能证明删除已传播出去:保守不放行(见上文)。
    if part.replicas.is_empty() {
        return false;
    }
    part.replicas.iter().all(|r| replica_caught_up(r.node_id))
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
        let healthy = info
            .replicas
            .iter()
            .filter(|r| is_healthy(&r.node_id))
            .count();
        let any_unhealthy = info.replicas.iter().any(|r| !is_healthy(&r.node_id));
        if any_unhealthy && healthy < info.replication_factor as usize {
            out.push(*pid);
        }
    }
    out
}

/// 判活迟滞的**纯状态机**(D5,**每一 reconcile tick 调用一次**):
/// 节点被判 down 需要**连续 `debounce_ticks` 拍**都观察到心跳过期。
///
/// ## 为什么需要迟滞
///
/// 判活原先只看单窗口(`last_beat.elapsed() <= heartbeat_timeout_secs`,默认 15s),而 store 每 3s 一拍
/// —— 一次 GC/调度停顿、一次 `statvfs` 卡顿就能跨过阈值。而判死会触发**全节点再复制**:一次抖动 =
/// 一次几万个分区的数据搬迁。3s 心跳在这段窗口里本应有 10 拍,**少一两拍不该等于死亡**。
///
/// ## 语义
///
/// - `debounce_ticks == 0` ⇒ **保持改造前行为**(本拍过期即判 down),便于回退对比;
/// - 否则:某节点本拍过期 ⇒ 连击 +1,`< debounce_ticks` 时**仍算 up**;达到 `debounce_ticks` 才判 down;
///   本拍有心跳 ⇒ 连击**立刻清零**(抖一下不该累积成"半个 down")。
/// - 返回值 = 本拍应视为 **up** 的节点集;`misses` 是调用方持有的连击状态(逐 tick 传入传出)。
///
/// 有效判死窗口 ≈ `heartbeat_timeout_secs + debounce_ticks × reconcile_interval_secs`。
pub fn up_nodes_debounced(
    known: impl IntoIterator<Item = NodeId>,
    expired: &HashSet<NodeId>,
    debounce_ticks: u64,
    misses: &mut HashMap<NodeId, u64>,
) -> HashSet<NodeId> {
    let known: Vec<NodeId> = known.into_iter().collect();
    let mut up = HashSet::new();
    for node in known.iter().copied() {
        if !expired.contains(&node) {
            misses.remove(&node); // 有心跳 ⇒ 连击清零(不累积)
            up.insert(node);
            continue;
        }
        if debounce_ticks == 0 {
            // 关迟滞:本拍过期即判 down(与改造前完全一致)。
            continue;
        }
        let streak = misses.entry(node).or_insert(0);
        *streak += 1;
        if *streak < debounce_ticks {
            up.insert(node); // 还没到门槛:先当它活着(宁可迟,不可早)
        }
    }
    // **只清"已不在册"的节点**,绝不清"仍在册但已判 down"的 —— 否则它的连击会归零,于是
    // 下一拍又变成"第 1 拍未达标 ⇒ 先当活着",节点就会在 up/up/down 之间**周期性翻转**,
    // 每 K 拍把全节点再复制又来一遍(这正是迟滞要防的动荡)。见单测
    // `a_long_down_node_never_flips_back_to_up`。
    misses.retain(|n, _| known.contains(n));
    up
}

/// 从欠副本集合里按**每 tick 预算**取本拍要发起的那一批(D6)。
///
/// - 输入已按 `PartitionId` 排序(`BTreeSet` 迭代序)**⇒ 选出的是确定的前 K 个**,同一输入恒产同一输出,
///   便于复现与排查(不随机、不与 HashMap 迭代序耦合);
/// - 返回 `(本拍发起, 推到下轮的个数)`;`max_per_tick == 0` 视为"不限"在这里**不会**出现
///   (配置校验强制 ≥1),但为健壮性按"全发"处理并记录在文档里。
pub fn take_rereplication_budget(
    targets: &BTreeSet<PartitionId>,
    max_per_tick: u64,
) -> (Vec<PartitionId>, u64) {
    if max_per_tick == 0 {
        return (targets.iter().copied().collect(), 0);
    }
    let limit = max_per_tick as usize;
    let take = targets.iter().take(limit).copied().collect();
    let deferred = targets.len().saturating_sub(limit) as u64;
    (take, deferred)
}

/// 节点下线 / **坏盘撤离**的一步动作(reconcile 层的 evacuation,先补后删,守 §3.1)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvacAction {
    /// 该分区健康副本不足 RF,需在健康节点补一个(先补)。
    Add(PartitionId),
    /// 该分区健康副本已达 RF,移除被抖出节点上的旧副本(后删)。
    Remove(PartitionId, NodeId),
}

/// 为"被抖出的 (节点) / (节点,盘)"承载的分区算 evacuation 动作(先补后删,守 §3.1)。
///
/// 这是把已有正确闸门的**作用域放大**:`decommission`(运维主动下线整台机)是"该节点所有盘都进抖出集"
/// 的特例,**不是**另写一个平行函数。
///
/// - `decommissioning`:被下线节点(其**所有盘**上的副本都算被抖出);
/// - `evicted_disks`:**坏盘**抖动集(D4),`(node, disk)` 粒度 —— 同一台机的健康盘不受牵连;
/// - `up_nodes`:存活节点(判活**已含迟滞**,见 [`up_nodes_debounced`]);
/// - `caught_up`:已上报 `ACTIVE`(= 已追平)的副本集,`(pid, node)`。
///
/// 对每个"含被抖出副本"的分区:健康副本(存活 ∧ 未被抖出)`< rf` → `Add`(先补);`>= rf` → 对每个
/// 被抖出副本产 `Remove`(后删)。
///
/// ## 删源闸门:**两条路径都要"已追平(ACTIVE)"**
///
/// 内部算**两个**健康计数,分别管两件事:
///
/// - `healthy`(只要"存活且未被抖出")= 决定**要不要补**。补上来的那份一进 placement 就算数,
///   所以不会每个 tick 重复补(重复补 = 同一个分区被加两份副本,RF 静默膨胀);
/// - `caught_up_healthy`(额外要求"已 `ACTIVE`")= 决定**能不能删源**。
///
/// 为什么删源必须等 `ACTIVE`:新补的副本可能还在 `BOOTSTRAP`(追平中)。此时若只按"存活副本数 ≥ RF"
/// 就删源,等于把"还在追平的那份"算成已有数据 ⇒ 可能删掉**盘上唯一一份可用数据** ⇒ 丢数据
/// (CLAUDE.md §3.1)。这道闸门对**节点下线**同样成立 —— 早先只有坏盘撤离那条路径检查它,是一处
/// **已知的不一致**,现已统一(宁可迟,不可早)。
///
/// 两数之间的窄缝 = "替代已就位、还在追平" ⇒ 该分区本拍**什么都不做**,等它变 `ACTIVE`。
/// 见 `openspec/changes/store-disk-isolation/tasks.md` §5。
pub fn plan_evacuation(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    decommissioning: &BTreeSet<NodeId>,
    evicted_disks: &BTreeSet<(NodeId, DiskId)>,
    up_nodes: &HashSet<NodeId>,
    caught_up: &HashSet<(PartitionId, NodeId)>,
) -> Vec<EvacAction> {
    let mut out = Vec::new();
    for (pid, info) in partitions {
        let is_evicted = |r: &ReplicaPlacement| {
            decommissioning.contains(&r.node_id)
                || evicted_disks.contains(&(r.node_id, r.disk_id.clone()))
        };
        if !info.replicas.iter().any(&is_evicted) {
            continue;
        }
        // **两个计数刻意分开**(否则会踩两个坑):
        //
        // - `healthy`(只要"存活且未被抖出"):决定**要不要补**。补上来的那份一进 placement 就算数,
        //   所以不会每个 tick 重复补(重复补 = 同一个分区被加两份副本,RF 静默膨胀);
        // - `caught_up_healthy`(额外要求"已 ACTIVE"):决定**能不能删源**(两条路径一视同仁)。
        let healthy = info
            .replicas
            .iter()
            .filter(|r| !is_evicted(r) && up_nodes.contains(&r.node_id))
            .count();
        let caught_up_healthy = info
            .replicas
            .iter()
            .filter(|r| {
                !is_evicted(r)
                    && up_nodes.contains(&r.node_id)
                    && caught_up.contains(&(*pid, r.node_id))
            })
            .count();
        if healthy < info.replication_factor as usize {
            out.push(EvacAction::Add(*pid));
        } else if caught_up_healthy >= info.replication_factor as usize {
            for r in &info.replicas {
                if is_evicted(r) {
                    out.push(EvacAction::Remove(*pid, r.node_id));
                }
            }
        }
        // 两数之间的窄缝 = "替代副本已就位、但还在追平":本拍**什么都不做**,等它 ACTIVE
        // (宁可迟,不可早 —— 此时删源就是丢数据)。
    }
    out
}

/// 从软状态下**持续**报 `UNAVAILABLE` 的盘里,挑出"已经可以动手撤离"的(D4)。
///
/// ## 为什么必须"持续证据"
///
/// 单轮故障枚举(`WRITE_TIMEOUT` …)**不足**以触发撤离:一次抖动就搬 GB 级数据,代价不对称
/// (design §8:多留一份副本几乎免费,一次不必要的迁移是真金白银)。故只认 `UNAVAILABLE`
/// (store 侧迟滞判定后的隔离态),且要求它**已持续** ≥ `min_bad_secs`。
///
/// `status` = `(node, disk) -> (最近健康值, 该值自何时起未变)`;由调用方逐 tick 维护 ——
/// 它**只认"变了才重置"**,故一个 status 反复在 `UNAVAILABLE` 与其他值之间跳动的盘**永远攒不满**
/// 持续时间(这是刻意的:抖动中的盘不该被搬)。
pub fn pick_evicted_disks(
    status: &BTreeMap<(NodeId, DiskId), (String, Instant)>,
    min_bad_secs: u64,
    now: Instant,
) -> BTreeSet<(NodeId, DiskId)> {
    status
        .iter()
        .filter(|(_, (health, since))| {
            health == "UNAVAILABLE"
                && now.saturating_duration_since(*since) >= Duration::from_secs(min_bad_secs)
        })
        .map(|(k, _)| k.clone())
        .collect()
}

/// 对迁移中的副本(MoveReplica)算"后删":新副本已补上、存活且非迁出源的副本数 ≥ RF **且继任者
/// 已 ACTIVE** 时,可移除迁出源 (pid, node)(add-before-remove,守 §3.1)。
///
/// `moving`:`(pid, node)` = 该节点上该分区副本是待迁出源。迁出源自身即使存活也不计入健康数。
/// `successor_active`:`(pid, 迁出源)` 中**继任者(= MoveReplica 的目标节点)已上报 ACTIVE** 的子集。
///
/// **闸门收紧**(§3.1"先补后删"要真的等到新副本可用):只数"存活副本 ≥ RF"不够——新副本刚补上就
/// 可能还在 BOOTSTRAP(追平中),此时删源会退化成"只有 RF−1 份可用数据"。故额外要求继任者已 ACTIVE。
pub fn plan_move(
    partitions: &BTreeMap<PartitionId, PartitionInfo>,
    moving: &BTreeSet<(PartitionId, NodeId)>,
    up_nodes: &HashSet<NodeId>,
    successor_active: &HashSet<(PartitionId, NodeId)>,
) -> Vec<(PartitionId, NodeId)> {
    let mut out = Vec::new();
    for (pid, node) in moving {
        let Some(info) = partitions.get(pid) else {
            continue;
        };
        if !successor_active.contains(&(*pid, *node)) {
            continue; // 继任者尚未 ACTIVE(追平中/未上报)→ 绝不删源
        }
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

/// 待删源副本(`pending_drop`)的继任者确认输入(A6;喂 [`drop_confirmed`])。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuccessorConfirm {
    /// 迁移删除:`move_targets` 有记账,且继任者已上报 `ACTIVE`(= 追平完成)。
    Active,
    /// 迁移删除:有记账但继任者尚未 `ACTIVE` → 还不能删。
    NotYetActive,
    /// 无迁移记账(evacuation 的"后删")→ 走冗余判据。
    Unrecorded,
}

/// (A6) 待删源副本是否已可下发 `DROPPED`:hard placement 已移除该副本 + 继任者确认。
///
/// 三条确认里的前两条在此判定(第三条"源仍持有"由调用方用软状态判):
/// 1. **placement 已不含** `(pid, source)`——否则还没"移除",谈不上删;
/// 2. **继任者确认**:
///    - 有迁移记账([`SuccessorConfirm::Active`] / [`SuccessorConfirm::NotYetActive`]):**只看继任者是否
///      已 ACTIVE**(= 新副本追平,转移成功)。其余副本是否临时失联**不拖延删除**——转移已由 hard
///      placement 记账,源上的那份数据不再是唯一来源;
///    - 无迁移记账([`SuccessorConfirm::Unrecorded`],evacuation 的"后删"):退化为**冗余判据**
///      "其余副本(排除源)数量 ≥ RF 且无一上报 `ERROR`"。
///
/// **保守性**:任何一条拿不准就返回 false —— 宁可让源盘上多留一份数据(后续心跳会重试),绝不少删。
pub fn drop_confirmed(
    pid: PartitionId,
    info: &PartitionInfo,
    source: NodeId,
    successor: SuccessorConfirm,
    errored: &HashSet<(PartitionId, NodeId)>,
) -> bool {
    if info.replicas.iter().any(|r| r.node_id == source) {
        return false;
    }
    let others: Vec<_> = info
        .replicas
        .iter()
        .filter(|r| r.node_id != source)
        .collect();
    match successor {
        SuccessorConfirm::Active => true,
        SuccessorConfirm::NotYetActive => false,
        SuccessorConfirm::Unrecorded => {
            others.len() >= info.replication_factor as usize
                && !others.iter().any(|r| errored.contains(&(pid, r.node_id)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PartitionState, ReplicaPlacement};

    /// D5:**一次心跳抖动不该判节点死**。节点过期 1 ~ K−1 拍仍算 up,第 K 拍才判 down。
    ///
    /// 这条守的是"判死的代价"—— 判死会触发该节点承载的**全部**分区再复制(可能是几万个);
    /// 而 3s 心跳在 30s 窗口里本应有 10 拍,少一两拍不该等于死亡。
    #[test]
    fn liveness_debounce_needs_k_consecutive_missed_ticks() {
        let known = [1u64, 2];
        let expired: HashSet<NodeId> = [2].into_iter().collect();
        let mut misses = HashMap::new();

        // K = 3:前两拍仍然"算活着"。
        for tick in 1..=2 {
            let up = up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
            assert!(up.contains(&1), "有心跳的节点永远 up(第 {tick} 拍)");
            assert!(up.contains(&2), "第 {tick} 拍还没到门槛 ⇒ 先当它活着");
        }
        // 第 3 拍:达标 ⇒ 判死。
        let up = up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
        assert!(!up.contains(&2), "连续 3 拍过期 ⇒ 才判 down");
        assert_eq!(misses.get(&2).copied(), Some(3), "连击数如实累积");

        // 持续 down 不重置计数(继续长),但也不影响结论。
        let up = up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
        assert!(!up.contains(&2));
        assert_eq!(misses.get(&2).copied(), Some(4));
    }

    /// D5:抖一下**不累积**成"半个 down" —— 中途收到心跳即连击清零,后面要重新数满 K 拍。
    #[test]
    fn a_heartbeat_resets_the_miss_streak() {
        let known = [2u64];
        let expired: HashSet<NodeId> = [2].into_iter().collect();
        let fresh: HashSet<NodeId> = HashSet::new();
        let mut misses = HashMap::new();

        // 过期两拍(还没到 K=3)…
        up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
        up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
        assert_eq!(misses.get(&2).copied(), Some(2));
        // …来了一拍心跳:清零。
        let up = up_nodes_debounced(known.iter().copied(), &fresh, 3, &mut misses);
        assert!(up.contains(&2));
        assert_eq!(misses.get(&2), None, "有心跳 ⇒ 连击清零,不留'半个 down'");

        // 再过期两拍:仍不该判死(重新数)。
        for _ in 0..2 {
            let up = up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
            assert!(up.contains(&2), "清零后重新数,两拍还不够");
        }
    }

    /// D5:`debounce_ticks = 0` ⇒ **保持改造前行为**(本拍过期即判 down),可回退对比。
    #[test]
    fn debounce_zero_keeps_the_previous_behaviour() {
        let known = [1u64, 2];
        let expired: HashSet<NodeId> = [2].into_iter().collect();
        let mut misses = HashMap::new();
        let up = up_nodes_debounced(known.iter().copied(), &expired, 0, &mut misses);
        assert!(up.contains(&1));
        assert!(!up.contains(&2), "0 = 无迟滞,过期即判 down");
        assert!(misses.is_empty(), "关迟滞时不维护连击状态");
    }

    /// D5:不在册的节点不留残留连击(节点被删/重注册后不继承别人的"半个 down")。
    #[test]
    fn miss_state_does_not_leak_for_unregistered_nodes() {
        let expired: HashSet<NodeId> = [7].into_iter().collect();
        let mut misses = HashMap::new();
        up_nodes_debounced([7u64], &expired, 3, &mut misses);
        assert_eq!(misses.get(&7).copied(), Some(1));
        // 该节点从在册集合里消失 ⇒ 状态被清掉。
        up_nodes_debounced([], &expired, 3, &mut misses);
        assert!(misses.is_empty(), "不在册的节点不该留下连击残留");
    }

    /// D5(**差点写进去的坑,单测钉死**):长期 down 的节点**绝不能周期性翻回 up**。
    ///
    /// 第一版实现里清理连击用的是"保留 `up` 集合里的节点",而判 down 的节点**不在** `up` 里 ⇒
    /// 它的连击被清掉 ⇒ 下一拍变成"第 1 拍未达标 ⇒ 先当它活着" ⇒ 节点在 up/up/down 之间**每 K 拍翻转一次**,
    /// 每次翻转都把"全节点再复制"又触发一遍。这正是迟滞要防的动荡,只是换了个形式。
    ///
    /// 正确语义:连击**单调累积**到判 down 之后**继续保持** down,直到真的收到心跳。
    #[test]
    fn a_long_down_node_never_flips_back_to_up() {
        let known = [2u64];
        let expired: HashSet<NodeId> = [2].into_iter().collect();
        let mut misses = HashMap::new();
        // 连跑 20 拍全过期:第 3 拍之后必须**永远**是 down(不许在第 4/6/9… 拍翻回 up)。
        for tick in 1..=20 {
            let up = up_nodes_debounced(known.iter().copied(), &expired, 3, &mut misses);
            if tick < 3 {
                assert!(up.contains(&2), "第 {tick} 拍还没到门槛");
            } else {
                assert!(!up.contains(&2), "第 {tick} 拍必须一直是 down(不许翻回 up)");
            }
        }
        assert_eq!(misses.get(&2).copied(), Some(20), "连击单调累积,不清零");
    }

    /// D6:每 tick 只取前 K 个,**按 `PartitionId` 确定性排序**;余量如实报为 deferred。
    ///
    /// 确定性很重要:同一输入恒产同一输出,排查"为什么这个分区还没补上"时才能复现。
    #[test]
    fn rereplication_budget_takes_a_deterministic_prefix() {
        let targets: BTreeSet<PartitionId> = [5u32, 1, 9, 3, 7].into_iter().collect();
        let (taken, deferred) = take_rereplication_budget(&targets, 2);
        assert_eq!(taken, vec![1, 3], "取最小的两个(确定性前缀)");
        assert_eq!(deferred, 3, "其余 3 个顺延到下一拍");

        // 预算 ≥ 需求量 ⇒ 全部发起、零顺延。
        let (taken, deferred) = take_rereplication_budget(&targets, 99);
        assert_eq!(taken, vec![1, 3, 5, 7, 9]);
        assert_eq!(deferred, 0);

        // 空集合 ⇒ 什么都不做(不 panic)。
        let (taken, deferred) = take_rereplication_budget(&BTreeSet::new(), 4);
        assert!(taken.is_empty());
        assert_eq!(deferred, 0);

        // 极端:0 视为"不限"(配置校验会拦下 0,这里只是不 panic 的兜底)。
        let (taken, deferred) = take_rereplication_budget(&targets, 0);
        assert_eq!(taken.len(), 5);
        assert_eq!(deferred, 0);
    }

    /// D6 + D5 的**组合性质**(本改动真正要防的场景):一个节点判死后,它的 N 个分区
    /// **不该在同一拍全部开始搬数据** —— 每拍只放行 K 个,直到补齐。
    #[test]
    fn a_node_down_does_not_drain_all_partitions_in_one_tick() {
        // 3 个分区都放在节点 2 上(RF=1 便于把"欠副本"算清楚)。
        let parts: BTreeMap<PartitionId, PartitionInfo> = (0..3)
            .map(|pid| (pid, part(1, &[(2, "d0", "ACTIVE")])))
            .collect();
        // 迟滞判定后节点 2 已 down,节点 1 up。
        let up: HashSet<NodeId> = [1].into_iter().collect();
        let wanted: BTreeSet<PartitionId> = plan_rereplication(&parts, &up, &HashSet::new())
            .into_iter()
            .collect();
        assert_eq!(wanted.len(), 3, "三个分区都欠副本");

        // 预算 = 2 ⇒ 本拍只发起 2 个,第三个顺延。
        let (first, deferred) = take_rereplication_budget(&wanted, 2);
        assert_eq!(first, vec![0, 1]);
        assert_eq!(deferred, 1);
        // 下一拍:剩下的那个仍在集合里(幂等重算 ⇒ 不会漏),并且能一次发完。
        let (second, deferred) = take_rereplication_budget(&wanted, 2);
        assert_eq!(second, vec![0, 1], "集合每拍重算,顺序稳定");
        assert_eq!(deferred, 1);
        // 计划上,第三个分区在"前两个补完、不再欠副本"之后的某一拍自然轮到。
        let remaining: BTreeSet<PartitionId> = [2u32].into_iter().collect();
        let (last, deferred) = take_rereplication_budget(&remaining, 2);
        assert_eq!(last, vec![2]);
        assert_eq!(deferred, 0);
    }

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
                    incarnation: 0,
                })
                .collect(),
        }
    }

    /// placement 派生索引的测试版:从 parts 现算 node -> pids(server 侧是 epoch 懒重建的缓存)。
    fn pids_of(
        parts: &BTreeMap<PartitionId, PartitionInfo>,
        node: NodeId,
    ) -> BTreeSet<PartitionId> {
        parts
            .iter()
            .filter(|(_, info)| info.replicas.iter().any(|r| r.node_id == node))
            .map(|(pid, _)| *pid)
            .collect()
    }

    #[test]
    fn transition_when_current_differs_or_missing() {
        let parts = BTreeMap::from([(0, part(2, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE")]))]);
        // node1 上报 BOOTSTRAP(≠ACTIVE)→ 需转移;node1 无 pid1 上报(缺失)→ 也需转移(但无此分区)。
        let reported = BTreeMap::from([(0u32, "BOOTSTRAP".to_string())]);
        let t = plan_transitions(1, &pids_of(&parts, 1), &parts, &reported);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].target_state, "ACTIVE");
        assert_eq!(t[0].partition_id, 0);
    }

    #[test]
    fn no_transition_when_current_matches() {
        let parts = BTreeMap::from([(0, part(1, &[(1, "d0", "ACTIVE")]))]);
        let reported = BTreeMap::from([(0u32, "ACTIVE".to_string())]);
        assert!(plan_transitions(1, &pids_of(&parts, 1), &parts, &reported).is_empty());
    }

    /// 分区状态下发:只报本节点上报过的分区,状态取状态机权威值(`SEALED`/`RO` 不得被吞掉)。
    #[test]
    fn partition_states_only_for_reported_partitions() {
        let mut parts = BTreeMap::from([
            (0, part(1, &[(1, "d0", "ACTIVE")])),
            (1, part(1, &[(1, "d0", "ACTIVE")])),
            (2, part(1, &[(9, "d0", "ACTIVE")])), // 别的节点承载
        ]);
        parts.get_mut(&1).unwrap().state = PartitionState::ReadOnly;
        parts.get_mut(&2).unwrap().state = PartitionState::Sealed;

        let reported = BTreeMap::from([(0u32, "ACTIVE".to_string()), (1u32, "ACTIVE".to_string())]);
        let mut got = plan_partition_states(&parts, &reported);
        got.sort_by_key(|(pid, _)| *pid);
        assert_eq!(
            got,
            vec![
                (0, PartitionState::ReadWrite),
                (1, PartitionState::ReadOnly)
            ],
            "本节点承载的 RW/RO 都要下发,未承载的分区不下发"
        );

        // 未上报(如副本刚被摘掉)= 不下发,不使用陈旧认知。
        assert!(plan_partition_states(&parts, &BTreeMap::new()).is_empty());
    }

    /// 墓碑回收闸门:全副本已追平才放行;任一副本没追平(追平中 / 报错 / 仍有滞后 / 未上报)一律不放行。
    #[test]
    fn tombstones_reclaim_only_when_every_replica_caught_up() {
        let p = part(
            3,
            &[
                (1, "d0", "ACTIVE"),
                (2, "d0", "ACTIVE"),
                (3, "d0", "ACTIVE"),
            ],
        );

        // 闭包 = 软状态口径(节点存活 且 上报 ACTIVE 且 lag==0);表里没有该节点 = 未上报 / 节点 down = 未追平。
        let reports: BTreeMap<NodeId, (&str, u64)> =
            BTreeMap::from([(1, ("ACTIVE", 0)), (2, ("ACTIVE", 0)), (3, ("ACTIVE", 0))]);
        let allowed = |reports: &BTreeMap<NodeId, (&str, u64)>| {
            tombstones_reclaim_allowed(&p, |n| matches!(reports.get(&n), Some(("ACTIVE", 0))))
        };

        assert!(allowed(&reports), "全副本 ACTIVE 且 lag=0 → 允许回收墓碑");

        let with = |node: NodeId, st: (&'static str, u64)| {
            let mut r = reports.clone();
            r.insert(node, st);
            r
        };
        assert!(!allowed(&with(3, ("BOOTSTRAP", 0))), "有副本还在追平");
        assert!(!allowed(&with(3, ("ERROR", 0))), "有副本上报错误");
        assert!(!allowed(&with(3, ("ACTIVE", 1))), "有副本还有滞后字节");
        let mut missing = reports.clone();
        missing.remove(&3);
        assert!(!allowed(&missing), "有副本未上报(等价于节点 down)");
    }

    /// 无副本(空 placement):没法证明删除已传播 → 保守拒绝,而不是"没人没追平"就放行。
    #[test]
    fn tombstones_reclaim_denied_without_replicas() {
        let empty = part(3, &[]);
        assert!(
            !tombstones_reclaim_allowed(&empty, |_| true),
            "空副本表必须拒绝:看不见 ≠ 已传播"
        );
    }

    #[test]
    fn missing_report_needs_transition() {
        let parts = BTreeMap::from([(0, part(1, &[(1, "d0", "ACTIVE")]))]);
        let reported = BTreeMap::new();
        assert_eq!(
            plan_transitions(1, &pids_of(&parts, 1), &parts, &reported).len(),
            1
        );
    }

    #[test]
    fn rereplicate_when_replica_down_and_below_rf() {
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([1, 2]); // node3 down → live=2 < 3
        assert_eq!(plan_rereplication(&parts, &up, &HashSet::new()), vec![0]);
    }

    #[test]
    fn no_rereplicate_when_all_up() {
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([1, 2, 3]);
        assert!(plan_rereplication(&parts, &up, &HashSet::new()).is_empty());
    }

    #[test]
    fn rereplicate_when_replica_errored_though_node_up() {
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                ],
            ),
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
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([1, 2, 3]);
        // node3 下线:healthy(非下线且存活)=2 < 3 → 先 Add,不 Remove。
        let decom = BTreeSet::from([3]);
        assert_eq!(
            plan_evacuation(&parts, &decom, &BTreeSet::new(), &up, &HashSet::new()),
            vec![EvacAction::Add(0)]
        );
    }

    #[test]
    fn evacuation_removes_after_healthy_reaches_rf() {
        // 已补上 node4:replicas=101..? 用 1..4;node3 下线,healthy={1,2,4}=3≥rf → Remove(0,3)。
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                    (4, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([1, 2, 3, 4]);
        let decom = BTreeSet::from([3]);
        // 删源闸门已统一:**节点下线也要等替代副本 ACTIVE**。
        let caught_up = HashSet::from([(0u32, 1u64), (0, 2), (0, 4)]);
        assert_eq!(
            plan_evacuation(&parts, &decom, &BTreeSet::new(), &up, &caught_up),
            vec![EvacAction::Remove(0, 3)]
        );
        // 反证:替代副本还没追平 ⇒ **不删**(哪怕是运维主动下线)。
        assert_eq!(
            plan_evacuation(&parts, &decom, &BTreeSet::new(), &up, &HashSet::new()),
            Vec::<EvacAction>::new(),
            "替代副本未 ACTIVE ⇒ 该分区本拍什么都不做,绝不删源"
        );
    }

    #[test]
    fn no_evacuation_without_decommission() {
        let parts = BTreeMap::from([(0, part(2, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE")]))]);
        let up = HashSet::from([1, 2]);
        assert!(plan_evacuation(
            &parts,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &up,
            &HashSet::new()
        )
        .is_empty());
    }

    // ── D4 坏盘撤离 ─────────────────────────────────────────────────────────────────────

    /// 坏盘上的副本先补:健康副本不足 RF 时只产 `Add`,**绝不**先删(守 §3.1)。
    #[test]
    fn disk_eviction_adds_before_removing() {
        // RF=2,副本在 (101,d0) 与 (102,d0);(101,d0) 判坏。
        let parts = BTreeMap::from([(0, part(2, &[(101, "d0", "ACTIVE"), (102, "d0", "ACTIVE")]))]);
        let up = HashSet::from([101, 102]);
        let evicted = BTreeSet::from([(101u64, "d0".to_string())]);
        assert_eq!(
            plan_evacuation(&parts, &BTreeSet::new(), &evicted, &up, &HashSet::new()),
            vec![EvacAction::Add(0)],
            "坏盘上的那份不算健康 ⇒ 只剩 1 份 < RF=2 ⇒ 先补,不许先删"
        );
    }

    /// 补够 RF **且继任者已 ACTIVE** ⇒ 才移除坏盘上的旧副本(先补后删走完)。
    #[test]
    fn disk_eviction_removes_only_after_replacements_are_active() {
        // 已补上 (103,d0);RF=2。
        let parts = BTreeMap::from([(
            0,
            part(
                2,
                &[
                    (101, "d0", "ACTIVE"),
                    (102, "d0", "ACTIVE"),
                    (103, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([101, 102, 103]);
        let evicted = BTreeSet::from([(101u64, "d0".to_string())]);
        let caught_up = HashSet::from([(0u32, 102u64), (0, 103)]);
        assert_eq!(
            plan_evacuation(&parts, &BTreeSet::new(), &evicted, &up, &caught_up),
            vec![EvacAction::Remove(0, 101)]
        );
    }

    /// **坏盘撤离的硬闸门**:新副本还在 `BOOTSTRAP`(未 ACTIVE)时**绝不删源** ——
    /// 否则会把"还在追平的那份"算成已有数据,删掉源盘上唯一一份可用数据 ⇒ **丢数据**。
    ///
    /// 这条与"节点下线"口径刻意不同(那里只看 `healthy >= rf`),理由见 [`plan_evacuation`]。
    #[test]
    fn disk_eviction_never_removes_while_the_replacement_is_still_bootstrapping() {
        let parts = BTreeMap::from([(
            0,
            part(
                2,
                &[
                    (101, "d0", "ACTIVE"),
                    (102, "d0", "ACTIVE"),
                    (103, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let up = HashSet::from([101, 102, 103]);
        let evicted = BTreeSet::from([(101u64, "d0".to_string())]);
        // 103 还没上报 ACTIVE(追平中)⇒ 数得上的只有 102 ⇒ 1 < 2 ⇒ 不删源。
        let caught_up = HashSet::from([(0u32, 102u64)]);
        assert_eq!(
            plan_evacuation(&parts, &BTreeSet::new(), &evicted, &up, &caught_up),
            Vec::<EvacAction>::new(),
            "替代副本已就位但还在追平 ⇒ **本拍什么都不做**(既不重复补、也不删源),等它 ACTIVE"
        );

        // 补充:替代副本**还不存在**时(只有 101/102,103 还没补)才该继续补 —— 两种情形必须分清,
        // 否则"每 tick 重补一发"会让 RF 静默膨胀(同一个分区被加两份副本)。
        let before_add =
            BTreeMap::from([(0, part(2, &[(101, "d0", "ACTIVE"), (102, "d0", "ACTIVE")]))]);
        assert_eq!(
            plan_evacuation(
                &before_add,
                &BTreeSet::new(),
                &evicted,
                &up,
                &HashSet::new()
            ),
            vec![EvacAction::Add(0)],
            "替代副本还没补上 ⇒ 该补"
        );
    }

    /// **同一台机的健康盘不受牵连**:抖出集是 `(node, disk)` 粒度,不是 node 粒度。
    #[test]
    fn evicting_one_disk_does_not_take_out_the_same_nodes_other_disk() {
        // 坏的是 (101,d1);该分区的副本在 (101,d0) —— 不是被抖出的那块盘。
        let parts = BTreeMap::from([(0, part(1, &[(101, "d0", "ACTIVE")]))]);
        let up = HashSet::from([101]);
        let evicted = BTreeSet::from([(101u64, "d1".to_string())]);
        assert!(
            plan_evacuation(&parts, &BTreeSet::new(), &evicted, &up, &HashSet::new()).is_empty(),
            "别把'一台机上有块盘坏了'升级成'这台机所有副本都要搬'"
        );
    }

    /// `rf = 1` ⇒ **永不删**(单副本分区只会补、不会删)。
    #[test]
    fn disk_eviction_never_removes_the_last_copy() {
        // RF=1,唯一副本在坏盘上:健康副本 0 < 1 ⇒ 只会 Add(哪怕一直补不上,也绝不先删)。
        let parts = BTreeMap::from([(0, part(1, &[(101, "d0", "ACTIVE")]))]);
        let up = HashSet::from([101, 102]);
        let evicted = BTreeSet::from([(101u64, "d0".to_string())]);
        let acts = plan_evacuation(&parts, &BTreeSet::new(), &evicted, &up, &HashSet::new());
        assert_eq!(acts, vec![EvacAction::Add(0)]);
        assert!(
            !acts.iter().any(|a| matches!(a, EvacAction::Remove(..))),
            "唯一一份数据绝不能被自动搬走"
        );
    }

    /// D4 的**持续证据**:只有"一直是 `UNAVAILABLE` 且已满 min_bad_secs"的盘才进抖动集。
    #[test]
    fn pick_evicted_disks_needs_persistent_unavailable() {
        let now = Instant::now();
        let key = (101u64, "d0".to_string());
        let mut st = BTreeMap::new();

        // 刚变成 UNAVAILABLE:证据不足。
        st.insert(key.clone(), ("UNAVAILABLE".to_string(), now));
        assert!(pick_evicted_disks(&st, 300, now).is_empty(), "刚坏不算持续");

        // 满了 300s:进抖动集。
        assert_eq!(
            pick_evicted_disks(&st, 300, now + Duration::from_secs(301)),
            BTreeSet::from([key.clone()])
        );

        // 单轮故障枚举(**不是**迟滞后的隔离态)永远不够。
        st.insert(key.clone(), ("WRITE_TIMEOUT".to_string(), now));
        assert!(
            pick_evicted_disks(&st, 300, now + Duration::from_secs(9_999)).is_empty(),
            "只认 UNAVAILABLE(store 迟滞的产物);单轮故障枚举不足以触发搬数据"
        );

        // 值一变就重置计时 ⇒ 反复跳动的盘永远攒不满(这是刻意的)。
        st.insert(key.clone(), ("UNAVAILABLE".to_string(), now));
        assert!(pick_evicted_disks(&st, 300, now + Duration::from_secs(299)).is_empty());
    }

    #[test]
    fn move_removes_source_after_replacement_active() {
        // RF=3,副本在 1/2/3 + 新补 4;moving={(0,3)}:非迁出源存活 = {1,2,4} = 3 ≥ RF,
        // 且继任者 4 已 ACTIVE → 移除 3。
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                    (4, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let moving = BTreeSet::from([(0u32, 3u64)]);
        let up = HashSet::from([1, 2, 3, 4]); // 迁出源 3 自己也 up,也要被移除
        let successor_active = HashSet::from([(0u32, 3u64)]);
        assert_eq!(
            plan_move(&parts, &moving, &up, &successor_active),
            vec![(0, 3)]
        );
    }

    #[test]
    fn move_holds_source_until_replacement_up() {
        // 新副本节点 4 尚未 up:非迁出源存活 = {1,2} = 2 < RF=3 → 不移除。
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                    (4, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let moving = BTreeSet::from([(0u32, 3u64)]);
        let up = HashSet::from([1, 2, 3]);
        let successor_active = HashSet::from([(0u32, 3u64)]); // 即便"已 ACTIVE"也要节点存活 ≥ RF
        assert!(
            plan_move(&parts, &moving, &up, &successor_active).is_empty(),
            "replacement not up: must hold the source"
        );
    }

    #[test]
    fn move_holds_source_until_successor_active() {
        // 新副本 4 存活但仍在 BOOTSTRAP(未 ACTIVE):up 计数已够(3),闸门必须再拦住。
        let parts = BTreeMap::from([(
            0,
            part(
                3,
                &[
                    (1, "d0", "ACTIVE"),
                    (2, "d0", "ACTIVE"),
                    (3, "d0", "ACTIVE"),
                    (4, "d0", "ACTIVE"),
                ],
            ),
        )]);
        let moving = BTreeSet::from([(0u32, 3u64)]);
        let up = HashSet::from([1, 2, 3, 4]);
        assert!(
            plan_move(&parts, &moving, &up, &HashSet::new()).is_empty(),
            "successor not ACTIVE yet: must hold the source (true add-before-remove)"
        );
    }

    #[test]
    fn move_ignores_unknown_partition() {
        let moving = BTreeSet::from([(9u32, 1u64)]);
        let successor_active = HashSet::from([(9u32, 1u64)]);
        assert!(plan_move(
            &BTreeMap::new(),
            &moving,
            &HashSet::from([1]),
            &successor_active
        )
        .is_empty());
    }

    #[test]
    fn reconcile_gated_until_grace_elapsed() {
        let grace = Duration::from_secs(15);
        assert!(
            !reconcile_ready(Duration::from_secs(0), grace),
            "just became leader: hold off"
        );
        assert!(
            !reconcile_ready(Duration::from_secs(14), grace),
            "still within grace"
        );
        assert!(
            reconcile_ready(Duration::from_secs(15), grace),
            "grace elapsed: reconcile"
        );
        assert!(reconcile_ready(Duration::from_secs(60), grace));
    }

    fn rh(up: bool, state: &str, decom: bool) -> ReplicaHealth {
        ReplicaHealth {
            up,
            current_state: state.to_string(),
            decommissioning: decom,
        }
    }

    #[test]
    fn drop_confirmed_requires_source_removed() {
        // 源 3 仍在 placement(还没"移除")→ 任何确认都不得放行。
        let p = part(
            3,
            &[
                (1, "d0", "ACTIVE"),
                (2, "d0", "ACTIVE"),
                (3, "d0", "ACTIVE"),
            ],
        );
        assert!(!drop_confirmed(
            0,
            &p,
            3,
            SuccessorConfirm::Active,
            &HashSet::new()
        ));
    }

    #[test]
    fn drop_confirmed_for_move_only_looks_at_successor() {
        // 源 3 已被移除;继任者 4 已 ACTIVE → 放行(其余副本是否失联不影响:不拖延删除)。
        let p = part(
            3,
            &[
                (1, "d0", "ACTIVE"),
                (2, "d0", "ACTIVE"),
                (4, "d0", "ACTIVE"),
            ],
        );
        assert!(drop_confirmed(
            0,
            &p,
            3,
            SuccessorConfirm::Active,
            &HashSet::new()
        ));
        // 继任者尚未 ACTIVE → 继续等(哪怕冗余数已够)。
        assert!(!drop_confirmed(
            0,
            &p,
            3,
            SuccessorConfirm::NotYetActive,
            &HashSet::new()
        ));
    }

    #[test]
    fn drop_confirmed_without_move_bookkeeping_uses_redundancy() {
        // evacuation 的"后删":无 move 记账 → 其余副本(排除源)≥ RF 且无 ERROR 才放行。
        let enough = part(
            3,
            &[
                (1, "d0", "ACTIVE"),
                (2, "d0", "ACTIVE"),
                (4, "d0", "ACTIVE"),
            ],
        );
        assert!(drop_confirmed(
            0,
            &enough,
            3,
            SuccessorConfirm::Unrecorded,
            &HashSet::new()
        ));
        // 只有 2 份(< RF=3)→ 不放行。
        let short = part(3, &[(1, "d0", "ACTIVE"), (2, "d0", "ACTIVE")]);
        assert!(!drop_confirmed(
            0,
            &short,
            3,
            SuccessorConfirm::Unrecorded,
            &HashSet::new()
        ));
        // 其余的有一份上报 ERROR → 不放行(那份数据不算数);ERROR 只在**别的分区**上 → 不影响。
        assert!(!drop_confirmed(
            0,
            &enough,
            3,
            SuccessorConfirm::Unrecorded,
            &HashSet::from([(0u32, 2u64)])
        ));
        assert!(drop_confirmed(
            0,
            &enough,
            3,
            SuccessorConfirm::Unrecorded,
            &HashSet::from([(9u32, 2u64)])
        ));
    }

    #[test]
    fn classify_partition_reasons() {
        let all_active = [
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
        ];
        assert_eq!(
            classify_partition(&all_active, 3),
            (3, None),
            "fully healthy"
        );

        let one_down = [
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
            rh(false, "ACTIVE", false),
        ];
        assert_eq!(
            classify_partition(&one_down, 3),
            (2, Some(REASON_NODE_DOWN))
        );

        let one_err = [
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
            rh(true, "ERROR", false),
        ];
        assert_eq!(
            classify_partition(&one_err, 3),
            (2, Some(REASON_REPLICA_ERROR))
        );

        let one_decom = [
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", true),
        ];
        assert_eq!(
            classify_partition(&one_decom, 3),
            (2, Some(REASON_DECOMMISSIONING))
        );

        let one_boot = [
            rh(true, "ACTIVE", false),
            rh(true, "ACTIVE", false),
            rh(true, "BOOTSTRAP", false),
        ];
        assert_eq!(
            classify_partition(&one_boot, 3),
            (2, Some(REASON_BOOTSTRAPPING))
        );
    }
}
