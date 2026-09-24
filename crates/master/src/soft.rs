//! 软状态(§7.1):liveness / 磁盘余量。**不进 Raft**,由心跳重建,新 leader 上任后一个心跳周期恢复。
//!
//! 用 `RwLock`(读多写少:心跳周期写,查询读)。共享可变状态必须同步(CLAUDE.md §2 并发)。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::types::{DiskId, NodeId, PartitionId};

/// 一块盘的实时状态(心跳上报,软状态)。
#[derive(Clone, Debug)]
pub struct DiskUsage {
    pub capacity_bytes: u64,
    pub free_bytes: u64,
    /// 该盘主动探测的健康结果(`HEALTHY` / `WRITE_TIMEOUT` / …,见 store `DiskHealth`)。
    ///
    /// **空串 = 该盘未启用主动探测**(store 侧默认关)→ 视为可用;语义见
    /// [`placement::select_disks`](crate::placement::select_disks)。
    pub health: String,
}

#[derive(Debug)]
struct Liveness {
    last_beat: Instant,
    disks: HashMap<DiskId, DiskUsage>,
}

/// 一个副本的实然态(心跳上报,M3)。
#[derive(Clone, Debug)]
pub struct ReplicaState {
    /// 该副本所在的盘(DROPPED 指令要按它定位"删哪块盘上的目录")。
    pub disk_id: DiskId,
    pub current_state: String,
    pub lag_bytes: u64,
    /// 该分区在此副本上的**实际占用**字节(blob 活数据,心跳上报)。
    pub used_bytes: u64,
}

#[derive(Default)]
struct Inner {
    nodes: HashMap<NodeId, Liveness>,
    replicas: HashMap<(PartitionId, NodeId), ReplicaState>,
    /// 节点 → 持有副本 反查索引(M7):与 `replicas` 严格同步维护。
    ///
    /// 为什么需要它:心跳每拍都要按节点增删/枚举副本,没有索引时"全量替换"只能 `retain` 扫
    /// **全集群**副本表(O(集群副本数)/拍),节点数上去后心跳处理平方增长。有索引后全量替换、
    /// 增量合并、摘除都只做 O(本节点副本数) 的工作。
    node_index: HashMap<NodeId, HashSet<PartitionId>>,
}

/// 集群软状态句柄(可克隆,内部共享)。
#[derive(Clone, Default)]
pub struct SoftState {
    inner: Arc<RwLock<Inner>>,
}

impl SoftState {
    /// 记录一次心跳:刷新 `last_beat` 与各盘实时状态(余量 + 健康)。
    pub fn record_beat(&self, node_id: NodeId, disks: HashMap<DiskId, DiskUsage>) {
        let mut g = self.inner.write().expect("soft state lock poisoned");
        g.nodes.insert(
            node_id,
            Liveness {
                last_beat: Instant::now(),
                disks,
            },
        );
    }

    /// 节点是否在 `timeout` 内有心跳(据此在 `ListNodes` 标 up/down)。
    pub fn is_up(&self, node_id: NodeId, timeout: Duration) -> bool {
        let g = self.inner.read().expect("soft state lock poisoned");
        match g.nodes.get(&node_id) {
            Some(l) => l.last_beat.elapsed() <= timeout,
            None => false,
        }
    }

    /// 某节点各盘的实时状态快照(余量 + 健康;`ListNodes` 回填用)。
    pub fn disks_of(&self, node_id: NodeId) -> HashMap<DiskId, DiskUsage> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.nodes
            .get(&node_id)
            .map(|l| l.disks.clone())
            .unwrap_or_default()
    }

    /// 全集群 `(node, disk) -> 实时盘状态`(余量 + 健康)快照,喂给选盘(§7.3)与盘将满判定。
    pub fn usage_snapshot(&self) -> std::collections::BTreeMap<(NodeId, DiskId), DiskUsage> {
        let g = self.inner.read().expect("soft state lock poisoned");
        let mut out = std::collections::BTreeMap::new();
        for (node_id, l) in &g.nodes {
            for (disk_id, u) in &l.disks {
                out.insert((*node_id, disk_id.clone()), u.clone());
            }
        }
        out
    }

    /// 记录某节点上报的各副本实然态(M3/M7)。
    ///
    /// `full_report` = 心跳声明的上报口径(proto `NodeBeat.full_report`):**全量**上报时按节点整体替换
    /// —— 本轮没列出的 `(pid, node)` 表示该节点已**不再持有**该副本(如收到 DROPPED 后删除),必须一并
    /// 清掉,否则软状态永久残留;**增量**上报时只做插入合并(缺项不代表"没有"),删除走显式的
    /// `removed`(proto `NodeBeat.removed_replicas`)。
    ///
    /// 复杂度:三种路径都只做 O(本节点副本数) 的工作 —— 全量替换经 `node_index` 反查删旧,
    /// 不做"扫全集群副本表 retain"(那是 O(集群副本数)/拍,规模上去后心跳处理平方增长)。
    pub fn record_replicas(
        &self,
        node_id: NodeId,
        replicas: impl IntoIterator<Item = (PartitionId, ReplicaState)>,
        removed: &[PartitionId],
        full_report: bool,
    ) {
        let mut g = self.inner.write().expect("soft state lock poisoned");
        let Inner {
            replicas: table,
            node_index,
            ..
        } = &mut *g;
        if full_report {
            // 经反查索引删该节点旧条目:O(节点旧副本数)。
            if let Some(old) = node_index.remove(&node_id) {
                for pid in old {
                    table.remove(&(pid, node_id));
                }
            }
        } else {
            for pid in removed {
                table.remove(&(*pid, node_id));
                if let Some(set) = node_index.get_mut(&node_id) {
                    set.remove(pid);
                }
            }
        }
        let index = node_index.entry(node_id).or_default();
        for (pid, st) in replicas {
            table.insert((pid, node_id), st);
            index.insert(pid);
        }
    }

    /// 某节点当前持有(最近上报口径)的副本视图:O(本节点副本数),心跳每拍规划的数据源。
    pub fn node_replicas(&self, node_id: NodeId) -> Vec<(PartitionId, ReplicaState)> {
        let g = self.inner.read().expect("soft state lock poisoned");
        let Some(index) = g.node_index.get(&node_id) else {
            return Vec::new();
        };
        index
            .iter()
            .filter_map(|pid| {
                g.replicas
                    .get(&(*pid, node_id))
                    .cloned()
                    .map(|st| (*pid, st))
            })
            .collect()
    }

    /// 按本节点软状态全集重算上报摘要(与 store 侧同一口径,见 [`crate::digest`]):
    /// 与心跳携带的 `replica_digest` 比对,不符 → 要求下一拍全量重发。
    pub fn node_digest(&self, node_id: NodeId) -> u64 {
        let g = self.inner.read().expect("soft state lock poisoned");
        let mut acc = 0u64;
        if let Some(index) = g.node_index.get(&node_id) {
            for pid in index {
                if let Some(st) = g.replicas.get(&(*pid, node_id)) {
                    acc = crate::digest::combine_digest(
                        acc,
                        crate::digest::replica_beat_hash(
                            *pid,
                            &st.disk_id,
                            &st.current_state,
                            st.lag_bytes,
                            st.used_bytes,
                        ),
                    );
                }
            }
        }
        acc
    }

    /// 查某 (partition, node) 副本的实然态。
    pub fn replica_state(&self, pid: PartitionId, node_id: NodeId) -> Option<ReplicaState> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas.get(&(pid, node_id)).cloned()
    }

    /// 该节点最近一次心跳是否仍持有某分区副本(A6 的判据之一:源仍持有才需要下发删除)。
    pub fn holds(&self, pid: PartitionId, node_id: NodeId) -> bool {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas.contains_key(&(pid, node_id))
    }

    /// 上报 `ERROR` 的副本集合(S2 被动检测 → 喂再复制判定)。
    pub fn errored_replicas(&self) -> std::collections::HashSet<(PartitionId, NodeId)> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas
            .iter()
            .filter(|(_, st)| st.current_state == "ERROR")
            .map(|(k, _)| *k)
            .collect()
    }

    /// 上报 `ACTIVE` 的副本集合(A6:move 的"后删"与 DROPPED 下发都要求**继任者已 ACTIVE** = 追平完成)。
    pub fn active_replicas(&self) -> std::collections::HashSet<(PartitionId, NodeId)> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas
            .iter()
            .filter(|(_, st)| st.current_state == "ACTIVE")
            .map(|(k, _)| *k)
            .collect()
    }

    /// 判活集合:超时未心跳的节点视为 down;返回当前 up 的节点集。
    pub fn up_nodes(&self, timeout: Duration) -> std::collections::HashSet<NodeId> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.nodes
            .iter()
            .filter(|(_, l)| l.last_beat.elapsed() <= timeout)
            .map(|(id, _)| *id)
            .collect()
    }

    /// **在册**节点集(不管心跳是否超时)。
    ///
    /// 判活迟滞需要它:迟滞的语义是"**在册**节点的连击",而不是"过期集合的连击" ——
    /// 一个从没上报过的节点不该被算成"连续 N 拍过期",一个刚注册的节点也不该继承别人的连击。
    pub fn known_nodes(&self) -> std::collections::HashSet<NodeId> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.nodes.keys().copied().collect()
    }

    /// 全集群磁盘 (Σcapacity_bytes, Σfree_bytes) 汇总(心跳上报,reconciler 观测用)。
    pub fn capacity_totals(&self) -> (u64, u64) {
        let g = self.inner.read().expect("soft state lock poisoned");
        let mut capacity = 0u64;
        let mut free = 0u64;
        for l in g.nodes.values() {
            for u in l.disks.values() {
                capacity = capacity.saturating_add(u.capacity_bytes);
                free = free.saturating_add(u.free_bytes);
            }
        }
        (capacity, free)
    }

    /// 所有副本上报 `lag_bytes` 的最大值(无上报 = 0)。
    pub fn max_replica_lag_bytes(&self) -> u64 {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas
            .values()
            .map(|st| st.lag_bytes)
            .max()
            .unwrap_or(0)
    }

    /// 各分区上报的 `used_bytes` 最大值(`partition -> bytes`;无任何上报的分区不在表中)。
    ///
    /// **口径**:一个分区的多个副本各报各自占用(同一份数据,量级相同),取其**最大值**代表该分区
    /// 的实际占用,避免按副本数(RF 倍)重复计数放大用量。
    pub fn used_bytes_by_partition(
        &self,
    ) -> std::collections::BTreeMap<crate::types::PartitionId, u64> {
        let g = self.inner.read().expect("soft state lock poisoned");
        let mut out = std::collections::BTreeMap::new();
        for ((pid, _), st) in &g.replicas {
            let e = out.entry(*pid).or_insert(0u64);
            *e = (*e).max(st.used_bytes);
        }
        out
    }

    /// 给定分区集合的占用合计(口径同 [`Self::used_bytes_by_partition`]:每分区先取各副本最大值,
    /// 再求和——RF 副本不重复计)。调用方传 RW 分区集即得"可写已用字节"。
    pub fn sum_used_bytes(&self, pids: impl IntoIterator<Item = crate::types::PartitionId>) -> u64 {
        let by_pid = self.used_bytes_by_partition();
        pids.into_iter().fold(0u64, |acc, pid| {
            acc.saturating_add(by_pid.get(&pid).copied().unwrap_or(0))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(state: &str, lag: u64, used: u64) -> ReplicaState {
        ReplicaState {
            disk_id: "d1".to_string(),
            current_state: state.to_string(),
            lag_bytes: lag,
            used_bytes: used,
        }
    }

    /// 全量替换:经索引删旧,不再持有的条目被清掉(原 retain 全表扫的等价语义)。
    #[test]
    fn full_report_replaces_node_entries_via_index() {
        let soft = SoftState::default();
        soft.record_replicas(
            1,
            [(1, st("ACTIVE", 0, 100)), (2, st("ACTIVE", 0, 200))],
            &[],
            true,
        );
        soft.record_replicas(2, [(1, st("ACTIVE", 0, 100))], &[], true);

        // 节点 1 全量重报:pid 2 不再持有 → 必须清掉;节点 2 的条目不受影响。
        soft.record_replicas(1, [(1, st("ACTIVE", 0, 150))], &[], true);

        assert!(soft.holds(1, 1));
        assert!(!soft.holds(2, 1), "全量替换后不再上报的副本必须摘除");
        assert!(soft.holds(1, 2), "别的节点的条目不受波及");
        let view = soft.node_replicas(1);
        assert_eq!(view.len(), 1);
        assert_eq!(view[0].1.used_bytes, 150);
    }

    /// 增量合并:缺项不代表没有;显式 removed 才删除;索引与主表同步。
    #[test]
    fn delta_report_merges_and_removes_via_index() {
        let soft = SoftState::default();
        soft.record_replicas(
            1,
            [(1, st("ACTIVE", 0, 100)), (2, st("ACTIVE", 0, 200))],
            &[],
            true,
        );

        // 增量:pid 1 值变化,pid 2 未提 → 保留。
        soft.record_replicas(1, [(1, st("ACTIVE", 0, 150))], &[], false);
        assert!(soft.holds(1, 1) && soft.holds(2, 1), "增量缺项不得误删");
        assert_eq!(soft.replica_state(1, 1).unwrap().used_bytes, 150);

        // 显式摘除 pid 2。
        soft.record_replicas(1, [], &[2], false);
        assert!(!soft.holds(2, 1), "removed 必须生效");
        assert!(soft.holds(1, 1));
        assert_eq!(soft.node_replicas(1).len(), 1);

        // 索引不得残留:再全量重报只含 pid 1 时,删旧不该碰别的节点。
        soft.record_replicas(1, [(1, st("ACTIVE", 0, 160))], &[], true);
        assert_eq!(soft.node_replicas(1).len(), 1);
    }

    /// `node_replicas` 视图只含该节点、且与主表内容一致。
    #[test]
    fn node_replicas_scoped_to_node() {
        let soft = SoftState::default();
        soft.record_replicas(1, [(1, st("ACTIVE", 0, 100))], &[], true);
        soft.record_replicas(
            2,
            [(1, st("BOOTSTRAP", 5, 100)), (3, st("ACTIVE", 0, 1))],
            &[],
            true,
        );

        let n1 = soft.node_replicas(1);
        assert_eq!(n1.len(), 1);
        assert_eq!(n1[0].0, 1);
        let mut n2: Vec<_> = soft.node_replicas(2);
        n2.sort_by_key(|(pid, _)| *pid);
        assert_eq!(n2.len(), 2);
        assert_eq!(n2[0].1.current_state, "BOOTSTRAP");
        assert!(soft.node_replicas(99).is_empty(), "未知节点空视图");
    }

    /// `node_digest` 与 `crate::digest::digest_of` 同口径;值变化/摘除都反映到摘要。
    #[test]
    fn node_digest_matches_shared_digest_over_full_set() {
        let soft = SoftState::default();
        soft.record_replicas(
            1,
            [(1, st("ACTIVE", 0, 100)), (2, st("ACTIVE", 0, 200))],
            &[],
            true,
        );
        let expect = crate::digest::digest_of([
            (1u32, "d1", "ACTIVE", 0u64, 100u64),
            (2u32, "d1", "ACTIVE", 0u64, 200u64),
        ]);
        assert_eq!(soft.node_digest(1), expect);

        soft.record_replicas(1, [(1, st("ACTIVE", 0, 101))], &[], false);
        assert_ne!(soft.node_digest(1), expect, "值变化必须改摘要");

        soft.record_replicas(1, [], &[2], false);
        let after_remove = crate::digest::digest_of([(1u32, "d1", "ACTIVE", 0u64, 101u64)]);
        assert_eq!(soft.node_digest(1), after_remove);
        assert_eq!(soft.node_digest(99), 0, "未知节点摘要为 0");
    }
}
