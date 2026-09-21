//! `ReplicaManager`:按 partition 管理本地 `BlobStore`(每个 partition-replica 一个)。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blob::{BlobStore, StoreConfig, StoreMetrics};

use crate::disk::DiskManager;
use crate::error::{Error, Result};
use crate::PartitionId;

/// 一个本地 partition-replica 句柄(`BlobStore` 用 `Arc` 共享给并发 put/get,方法均 `&self`)。
#[derive(Clone)]
pub struct ReplicaHandle {
    pub disk_id: String,
    pub store: Arc<BlobStore>,
    /// 该 replica 的实然态(心跳上报);IO 自关 hook 触发时被置 `ERROR`(S2 被动检测)。
    pub current_state: Arc<RwLock<String>>,
}

/// 本节点所有 partition-replica 的管理器。
pub struct ReplicaManager {
    disks: DiskManager,
    store_config: StoreConfig,
    store_metrics: StoreMetrics,
    partition_capacity: u64,
    replicas: RwLock<HashMap<PartitionId, ReplicaHandle>>,
}

impl ReplicaManager {
    pub fn new(
        disks: DiskManager,
        store_config: StoreConfig,
        store_metrics: StoreMetrics,
        partition_capacity: u64,
    ) -> Self {
        ReplicaManager {
            disks,
            store_config,
            store_metrics,
            partition_capacity,
            replicas: RwLock::new(HashMap::new()),
        }
    }

    /// 幂等地确保某分区在本地有 replica:已存在则返回,否则选盘 create/open 一个 `BlobStore`。
    pub fn ensure_replica(&self, pid: PartitionId) -> Result<ReplicaHandle> {
        if let Some(h) = self.replicas.read().expect("replicas lock poisoned").get(&pid) {
            return Ok(h.clone());
        }
        let mut guard = self.replicas.write().expect("replicas lock poisoned");
        if let Some(h) = guard.get(&pid) {
            return Ok(h.clone());
        }
        let disk = self.disks.pick(self.partition_capacity).ok_or(Error::NoDiskSpace)?;
        let dir = disk.dir.join(format!("partition_{pid}"));
        let store_id = format!("p{pid}");
        let store = if dir.exists() {
            BlobStore::open_with(
                &dir,
                self.partition_capacity,
                &self.store_config,
                &self.store_metrics,
                &store_id,
            )?
        } else {
            BlobStore::create_with(
                &dir,
                self.partition_capacity,
                &self.store_config,
                &self.store_metrics,
                &store_id,
            )?
        };
        let handle = Self::make_handle(disk.id.clone(), store);
        guard.insert(pid, handle.clone());
        Ok(handle)
    }

    /// 取某分区的本地 replica(数据面路由用);未建则 `None`。
    pub fn get(&self, pid: PartitionId) -> Option<ReplicaHandle> {
        self.replicas.read().expect("replicas lock poisoned").get(&pid).cloned()
    }

    /// 用 `BlobStore` 建句柄并接被动 IO-error hook(置 ERROR)。
    fn make_handle(disk_id: String, store: BlobStore) -> ReplicaHandle {
        let handle = ReplicaHandle {
            disk_id,
            store: Arc::new(store),
            current_state: Arc::new(RwLock::new("ACTIVE".to_string())),
        };
        let cs = handle.current_state.clone();
        handle.store.set_io_error_hook(Arc::new(move || {
            *cs.write().expect("current_state lock poisoned") = "ERROR".to_string();
        }));
        handle
    }

    /// S3b:把来自 peer 的封存段字节装载为本地某分区副本(物理 bootstrap)。
    ///
    /// 崩溃安全:段文件 `tmp→fsync→rename` 逐个落地,全部就位后 `open_rebuild_index_with` 重建索引打开。
    /// 覆盖式(先清 partition 目录):仅用于**空/新** replica 的 bootstrap,幂等(失败重试重来)。
    pub fn install_bootstrap(
        &self,
        pid: PartitionId,
        segments: &[(u64, Vec<u8>)],
    ) -> Result<ReplicaHandle> {
        use std::io::Write;
        self.remove_replica(pid);
        let disk = self.disks.pick(self.partition_capacity).ok_or(Error::NoDiskSpace)?;
        let dir = disk.dir.join(format!("partition_{pid}"));
        let _ = std::fs::remove_dir_all(&dir);
        let log_dir = dir.join("log");
        std::fs::create_dir_all(&log_dir)?;
        for (pos, bytes) in segments {
            let final_path = log_dir.join(blob::LogSegmentName(*pos).filename());
            let tmp_path = log_dir.join(format!("{pos}_log.seg.tmp"));
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(bytes)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp_path, &final_path)?;
        }
        let store_id = format!("p{pid}");
        let store = blob::BlobStore::open_rebuild_index_with(
            &dir,
            self.partition_capacity,
            &self.store_config,
            &self.store_metrics,
            &store_id,
        )?;
        let handle = Self::make_handle(disk.id.clone(), store);
        self.replicas.write().expect("replicas lock poisoned").insert(pid, handle.clone());
        Ok(handle)
    }

    /// 关闭并移除某分区的本地 replica(OFFLINE/DROPPED;删目录回收留后续)。
    pub fn remove_replica(&self, pid: PartitionId) {
        self.replicas.write().expect("replicas lock poisoned").remove(&pid);
    }

    /// 各 replica 的 `(partition_id, disk_id, current_state)`(心跳上报用)。
    pub fn replica_states(&self) -> Vec<(PartitionId, String, String)> {
        self.replicas
            .read()
            .expect("replicas lock poisoned")
            .iter()
            .map(|(pid, h)| {
                let st = h.current_state.read().expect("current_state lock poisoned").clone();
                (*pid, h.disk_id.clone(), st)
            })
            .collect()
    }

    /// 当前本地 replica 数(指标用)。
    pub fn count(&self) -> usize {
        self.replicas.read().expect("replicas lock poisoned").len()
    }

    /// 快照全部 `(partition_id, ReplicaHandle)`(本地 worker 遍历维护用)。
    pub fn handles(&self) -> Vec<(PartitionId, ReplicaHandle)> {
        self.replicas
            .read()
            .expect("replicas lock poisoned")
            .iter()
            .map(|(pid, h)| (*pid, h.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_replica_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned()]).unwrap();
        let registry = metric::Registry::new();
        let sc = StoreConfig { segment_size_bytes: 1 << 20, index_max_inmem_elements: 1000, io_error_count_to_trigger_shutdown: 0 };
        let sm = StoreMetrics::new(&registry).unwrap();
        let rm = ReplicaManager::new(dm, sc, sm, 4 << 20);

        let a = rm.ensure_replica(7).unwrap();
        let b = rm.ensure_replica(7).unwrap();
        assert_eq!(rm.count(), 1, "same partition must not create a second replica");
        assert_eq!(a.disk_id, b.disk_id);
        assert!(rm.get(7).is_some());
        assert!(rm.get(8).is_none());
    }
}
