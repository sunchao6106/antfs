//! Hybrid compaction 调度策略的集成测试:经公开入口 `compact_all` 驱动,验证
//! hybrid 首轮(无状态文件)走 CompactAll、incremental 走 `compact_if_needed`、
//! cost-benefit 走 `compact_cost_benefit`,且指标按**实际走的策略**计数。
//!
//! 全程同步调用 + 有界断言,无 sleep;副本一律建在 pid=1 上(key 前缀随之)。

use std::sync::Arc;

use antfs_blob::{StoreConfig, StoreKey, StoreMetrics};
use antfs_store::config::{CompactionPolicyKind, StoreServerConfig};
use antfs_store::disk::DiskManager;
use antfs_store::disk_pool::DiskPools;
use antfs_store::metrics::StoreServerMetrics;
use antfs_store::replica::{ReplicaHandle, ReplicaManager};
use antfs_store::worker::compact_all;
use antfs_store::{DiskHealthMap, StoreDataDir};

const PID: u32 = 1;

fn key(n: u8) -> StoreKey {
    StoreKey::from_parts(PID, n as u32)
}

fn store_engine() -> StoreConfig {
    StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn manager(dir: &std::path::Path) -> ReplicaManager {
    let declared: StoreDataDir = dir.to_string_lossy().into_owned().into();
    let dm = DiskManager::mount(std::slice::from_ref(&declared)).unwrap();
    let registry = antfs_metric::Registry::new();
    let sm = StoreMetrics::new(&registry).unwrap();
    ReplicaManager::new(dm, store_engine(), sm, 8 << 20)
}

fn pool_for(dir: &std::path::Path) -> Arc<DiskPools> {
    let declared: StoreDataDir = dir.to_string_lossy().into_owned().into();
    let dm = DiskManager::mount(std::slice::from_ref(&declared)).unwrap();
    let registry = antfs_metric::Registry::new();
    let metrics = StoreServerMetrics::new(&registry).unwrap();
    let cfg = StoreServerConfig {
        data_dirs: vec![declared],
        ..Default::default()
    };
    Arc::new(DiskPools::new(&dm, &cfg, &metrics, antfs_store::disk_health::new_map()).unwrap())
}

fn health_and_metrics() -> (DiskHealthMap, StoreServerMetrics) {
    let registry = antfs_metric::Registry::new();
    let metrics = StoreServerMetrics::new(&registry).expect("metrics");
    (antfs_store::disk_health::new_map(), metrics)
}

/// 造"够压"的副本:写满一段以上再全删(每条 put 被随后的墓碑取代 ⇒ 死数据);
/// 副本置 `ACTIVE`(否则 worker 会跳过未追平的副本)。
fn handle_with_dead_data(rm: &ReplicaManager) -> ReplicaHandle {
    let h = rm.ensure_replica(PID).unwrap();
    h.mark_active();
    let big = vec![0xCDu8; 200 * 1024];
    for i in 0..16u8 {
        h.store.put(key(i), &big).unwrap();
        h.store.delete(key(i)).unwrap();
    }
    h.store.flush().unwrap();
    assert!(h.store.stats().unwrap().dead_bytes > 0);
    h
}

fn config() -> StoreServerConfig {
    StoreServerConfig::default()
}

fn policy_used(metrics: &StoreServerMetrics, policy: &str) -> u64 {
    metrics
        .compaction_policy_used
        .with_label_values(&[policy])
        .get()
}

#[tokio::test]
async fn hybrid_first_round_uses_compact_all() {
    let tmp = tempfile::tempdir().unwrap();
    let rm = manager(tmp.path());
    let pool = pool_for(tmp.path());
    let (health, metrics) = health_and_metrics();
    let h = handle_with_dead_data(&rm);
    let before = h.store.stats().unwrap().dead_bytes;

    // 默认配置 = Hybrid;无状态文件 ⇒ do_compact_all=true ⇒ 本轮走 CompactAll。
    let n = compact_all(&pool, &rm, &health, &metrics, &config(), true, true).await;
    assert_eq!(n, 1, "该副本必须真的压了一轮");

    let after = h.store.stats().unwrap().dead_bytes;
    assert!(
        after < before,
        "CompactAll 必须回收死数据: {before} -> {after}"
    );
    assert!(policy_used(&metrics, "compact_all") >= 1);
}

#[tokio::test]
async fn incremental_policy_uses_compact_if_needed() {
    let tmp = tempfile::tempdir().unwrap();
    let rm = manager(tmp.path());
    let pool = pool_for(tmp.path());
    let (health, metrics) = health_and_metrics();
    let h = handle_with_dead_data(&rm);
    let before = h.store.stats().unwrap().dead_bytes;

    let cfg = StoreServerConfig {
        compaction_policy: CompactionPolicyKind::Incremental,
        ..config()
    };
    let n = compact_all(&pool, &rm, &health, &metrics, &cfg, true, false).await;
    assert_eq!(n, 1, "增量策略必须选中并压缩达阈段");

    let after = h.store.stats().unwrap().dead_bytes;
    assert!(
        after < before,
        "增量压缩必须回收死数据: {before} -> {after}"
    );
    assert!(policy_used(&metrics, "incremental") >= 1);
}

#[tokio::test]
async fn cost_benefit_policy_uses_cost_benefit() {
    let tmp = tempfile::tempdir().unwrap();
    let rm = manager(tmp.path());
    let pool = pool_for(tmp.path());
    let (health, metrics) = health_and_metrics();
    let h = handle_with_dead_data(&rm);
    let before = h.store.stats().unwrap().dead_bytes;

    let cfg = StoreServerConfig {
        compaction_policy: CompactionPolicyKind::CostBenefit,
        ..config()
    };
    let n = compact_all(&pool, &rm, &health, &metrics, &cfg, true, false).await;
    assert_eq!(n, 1, "多段死数据下 cost-benefit 必须选中区间");

    let after = h.store.stats().unwrap().dead_bytes;
    assert!(
        after < before,
        "cost-benefit 必须回收死数据: {before} -> {after}"
    );
    assert!(policy_used(&metrics, "cost_benefit") >= 1);
}
