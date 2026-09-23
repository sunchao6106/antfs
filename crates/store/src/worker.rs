//! S4 本地维护 worker:数据节点常驻后台,周期性驱动 `blob` 引擎的 flush / compaction。
//!
//! 对齐 Ambry `CompactionManager` 跑在 `StorageManager` 里的定位:**本地、轻、常驻**,只维护本节点
//! 自己的 replica,不做集群级再复制/再平衡(那是 master 的 reconciler)。周期时长全部可配置。
//!
//! 引擎调用是同步阻塞(路线乙),故在 [`tokio::task::spawn_blocking`] 里执行,不阻塞 reactor。

use std::sync::Arc;
use std::time::Duration;

use crate::config::StoreServerConfig;
use crate::metrics::StoreServerMetrics;
use crate::replica::ReplicaManager;

/// 对本节点每个 replica 做一次 flush,返回成功 flush 的 replica 数。
///
/// flush 是持久化边界(§3.1):把已入 page-cache 的记录推进到 fsync,只增强持久性。
/// 单个 replica 失败只记 warn 并继续(维护尽力而为,不因一盘拖垮全局)。
pub fn flush_all(replicas: &ReplicaManager) -> usize {
    let mut n = 0;
    for (pid, h) in replicas.handles() {
        match h.store.flush() {
            Ok(()) => n += 1,
            Err(e) => tracing::warn!(partition = pid, error = %e, "local-worker flush failed"),
        }
    }
    n
}

/// 对本节点每个 replica 按死数据阈值增量压缩,返回实际发生压缩的 replica 数。
pub fn compact_all(replicas: &ReplicaManager, dead_ratio: f64) -> usize {
    let mut n = 0;
    for (pid, h) in replicas.handles() {
        match h.store.compact_if_needed(dead_ratio) {
            Ok(Some(_)) => n += 1,
            Ok(None) => {}
            Err(e) => tracing::warn!(partition = pid, error = %e, "local-worker compaction failed"),
        }
    }
    n
}

/// 起两条独立周期的后台维护 loop(flush / compaction),各自节拍由配置控制。
///
/// 仅在 `local_worker_enabled` 时由 [`crate::server::StoreServer::serve`] 调用。
pub fn spawn_local_worker(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
) {
    spawn_loop(
        Duration::from_secs(config.flush_interval_secs),
        replicas.clone(),
        metrics.clone(),
        |r, m| m.flushes.inc_by(flush_all(&r) as u64),
    );
    let dead_ratio = config.compaction_dead_ratio;
    spawn_loop(
        Duration::from_secs(config.compaction_interval_secs),
        replicas,
        metrics,
        move |r, m| m.compactions.inc_by(compact_all(&r, dead_ratio) as u64),
    );
}

/// 通用维护 loop:按 `period` 节拍,把同步维护动作 offload 到阻塞线程池执行。
fn spawn_loop<F>(
    period: Duration,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    action: F,
) where
    F: Fn(Arc<ReplicaManager>, &StoreServerMetrics) + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let action = Arc::new(action);
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            let r = replicas.clone();
            let m = metrics.clone();
            let action = action.clone();
            let _ = tokio::task::spawn_blocking(move || action(r, &m)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use antfs_blob::{StoreConfig, StoreKey, StoreMetrics};

    use crate::disk::DiskManager;

    /// 本模块的副本都建在 `pid = 1` 上 —— key 前缀必须跟着走(记录只存低 32bit)。
    fn key(n: u8) -> StoreKey {
        StoreKey::from_parts(1, n as u32)
    }

    fn manager(dir: &std::path::Path) -> ReplicaManager {
        let dm = DiskManager::mount(&[dir.to_string_lossy().into_owned()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        ReplicaManager::new(dm, sc, sm, 8 << 20)
    }

    #[test]
    fn flush_all_persists_and_reopen_reads_back() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let h = rm.ensure_replica(1).unwrap();
        h.store.put(key(1), b"durable payload").unwrap();

        assert_eq!(flush_all(&rm), 1, "one replica flushed");

        rm.remove_replica(1);
        let h2 = rm.ensure_replica(1).unwrap();
        assert_eq!(
            h2.store.get(&key(1)).unwrap().as_slice(),
            b"durable payload"
        );
    }

    #[test]
    fn compact_all_reclaims_dead_data() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let h = rm.ensure_replica(1).unwrap();
        // 写满一段以上再全删,制造达阈的封存段死数据,让 compaction 真正触发。
        let big = vec![0xCDu8; 200 * 1024];
        for i in 0..16u8 {
            h.store.put(key(i), &big).unwrap();
            h.store.delete(key(i)).unwrap();
        }
        h.store.flush().unwrap();
        let before = h.store.stats().unwrap().dead_bytes;
        assert!(before > 0, "expected dead bytes before compaction");

        compact_all(&rm, 0.1);

        let after = h.store.stats().unwrap().dead_bytes;
        assert!(
            after < before,
            "compaction must reduce dead bytes: {before} -> {after}"
        );
    }
}
