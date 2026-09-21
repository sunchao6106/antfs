//! 软状态(§7.1):liveness / 磁盘余量。**不进 Raft**,由心跳重建,新 leader 上任后一个心跳周期恢复。
//!
//! 用 `RwLock`(读多写少:心跳周期写,查询读)。共享可变状态必须同步(CLAUDE.md §2 并发)。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::types::{DiskId, NodeId};

/// 一块盘的实时余量(心跳上报,软状态)。
#[derive(Clone, Copy, Debug)]
pub struct DiskUsage {
    pub capacity_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Debug)]
struct Liveness {
    last_beat: Instant,
    disks: HashMap<DiskId, DiskUsage>,
}

/// 一个副本的实然态(心跳上报,M3)。
#[derive(Clone, Debug)]
pub struct ReplicaState {
    pub current_state: String,
    pub lag_bytes: u64,
}

#[derive(Default)]
struct Inner {
    nodes: HashMap<NodeId, Liveness>,
    replicas: HashMap<(crate::types::PartitionId, NodeId), ReplicaState>,
}

/// 集群软状态句柄(可克隆,内部共享)。
#[derive(Clone, Default)]
pub struct SoftState {
    inner: Arc<RwLock<Inner>>,
}

impl SoftState {
    /// 记录一次心跳:刷新 `last_beat` 与各盘余量。
    pub fn record_beat(&self, node_id: NodeId, disks: HashMap<DiskId, DiskUsage>) {
        let mut g = self.inner.write().expect("soft state lock poisoned");
        g.nodes.insert(node_id, Liveness { last_beat: Instant::now(), disks });
    }

    /// 节点是否在 `timeout` 内有心跳(据此在 `ListNodes` 标 up/down)。
    pub fn is_up(&self, node_id: NodeId, timeout: Duration) -> bool {
        let g = self.inner.read().expect("soft state lock poisoned");
        match g.nodes.get(&node_id) {
            Some(l) => l.last_beat.elapsed() <= timeout,
            None => false,
        }
    }

    /// 某节点各盘的实时余量快照。
    pub fn disks_of(&self, node_id: NodeId) -> HashMap<DiskId, DiskUsage> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.nodes.get(&node_id).map(|l| l.disks.clone()).unwrap_or_default()
    }

    /// 全集群 `(node, disk) -> free_bytes` 快照,喂给选盘(§7.3)。
    pub fn free_snapshot(&self) -> std::collections::BTreeMap<(NodeId, DiskId), u64> {
        let g = self.inner.read().expect("soft state lock poisoned");
        let mut out = std::collections::BTreeMap::new();
        for (node_id, l) in &g.nodes {
            for (disk_id, u) in &l.disks {
                out.insert((*node_id, disk_id.clone()), u.free_bytes);
            }
        }
        out
    }

    /// 记录某节点上报的各副本实然态(M3)。
    pub fn record_replicas(
        &self,
        node_id: NodeId,
        replicas: impl IntoIterator<Item = (crate::types::PartitionId, ReplicaState)>,
    ) {
        let mut g = self.inner.write().expect("soft state lock poisoned");
        for (pid, st) in replicas {
            g.replicas.insert((pid, node_id), st);
        }
    }

    /// 查某 (partition, node) 副本的实然态。
    pub fn replica_state(
        &self,
        pid: crate::types::PartitionId,
        node_id: NodeId,
    ) -> Option<ReplicaState> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas.get(&(pid, node_id)).cloned()
    }

    /// 上报 `ERROR` 的副本集合(S2 被动检测 → 喂再复制判定)。
    pub fn errored_replicas(&self) -> std::collections::HashSet<(crate::types::PartitionId, NodeId)> {
        let g = self.inner.read().expect("soft state lock poisoned");
        g.replicas
            .iter()
            .filter(|(_, st)| st.current_state == "ERROR")
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
}
