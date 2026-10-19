//! S4b 后台一致性巡检(bit-rot 蜗牛爬):把"运维手点的 verify"变成常驻低速率的自动扫描。
//!
//! 防震荡四条(见 openspec `store-scrub-background` / store.md §12.13):
//! - **同盘串行**:每批照 verify 端点同一条路进**每盘有界池**(D1),与 put/get/compaction 在
//!   一个池里排队;池饱和 ⇒ 本轮跳过退避,绝不重试轰炸;
//! - **跨盘总闸**:`scrub_background_max_in_flight`(默认 1)= 全节点同时在爬的盘数上限
//!   (家风同 master `balance_max_in_flight`:宁可慢,不打满);
//! - **蜗牛爬节拍**:每 tick 每 lane 最多一个 batch(`scrub_max_batch_bytes`),tick 间睡
//!   `scrub_background_interval_secs` 叠 ±25% 抖动 —— 恒定低速,不与 compaction 钟点共振成尖峰;
//! - **只报警不修复**:发现的坏记录大声 warn 并进指标,处置(Repair/Move/重 eval)永远是运维动作。
//!
//! 候选集与 compaction 刻意不同:**SEALED 也扫**(封存冷数据正是巡检查护的对象,巡检只读不搬);
//! 集群模式下未追平(`BOOTSTRAP`)的副本跳过 —— 还在被拉复制填充,扫了也白扫。盘
//! `UNAVAILABLE/DEGRADED` 跳过并计 `disk_worker_skipped{task="scrub"}`。运维事故中用
//! `/admin/scrub-control` 掐停本循环(手动 verify 端点不受影响 —— 应急检查能力必须常在)。
//!
//! 续扫游标(`verify_from`)只在内存;进程重启从各副本头部重新开始一轮 —— verify 只读幂等,
//! 多扫无害。周期走完(`end_cursor = None`)自动从头开新周期,新写入的数据在下一周期覆盖。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use antfs_blob::BlobStore;

use crate::config::StoreServerConfig;
use crate::disk_health::DiskHealthMap;
use crate::disk_pool::DiskPools;
use crate::metrics::StoreServerMetrics;
use crate::replica::{ReplicaHandle, ReplicaManager, STATE_ACTIVE};
use crate::PartitionId;

/// 单轮挑出的候选:分区号 / 引擎 / 所在盘 / 续扫游标(下一条未扫字节;缺省从头部扫)。
type Candidate = (PartitionId, Arc<BlobStore>, String, Option<(u64, u64)>);

/// 巡检循环的可变状态(游标轮转 + round-robin 起点;均内存态,见模块头)。
struct CrawlState {
    cursors: HashMap<PartitionId, Option<(u64, u64)>>,
    rr: usize,
}

/// 起后台巡检循环(仅 `scrub_background_enabled` 时由 [`crate::server::StoreServer::serve`] 调用)。
/// 配置已过 `Validate`,这里只做防御性 `max(1)`。
pub fn spawn_scrub_worker(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    pool: Arc<DiskPools>,
    health: DiskHealthMap,
    paused: Arc<AtomicBool>,
) {
    let interval = config.scrub_background_interval_secs.max(1);
    let lanes = (config.scrub_background_max_in_flight as usize).max(1);
    let batch_bytes = config.scrub_max_batch_bytes.max(1);
    tokio::spawn(async move {
        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        loop {
            tokio::time::sleep(jittered(interval)).await;
            if paused.load(Ordering::Relaxed) {
                continue;
            }
            scrub_round(
                &pool,
                &replicas,
                &health,
                &metrics,
                &mut state,
                lanes,
                batch_bytes,
            )
            .await;
        }
    });
}

/// 一次扫描批的汇拢结果(JoinSet 逐 lane 收拢)。
struct BatchOutcome {
    pid: PartitionId,
    result: Result<Result<antfs_blob::VerifyReport, antfs_blob::Error>, String>,
}

/// 单轮 tick:round-robin 挑至多 `lanes` 个候选,各提交一个 batch 到它自己那块盘的池并发执行。
/// 返回完成的 batch 数(池拒/盘跳过/校验失败均不算 —— 游标不动,下轮原地重试,节奏受 tick 限制)。
///
/// 候选按 `partition_id` 定序后从 `state.rr` 起轮转(多副本公平);游标表里已消失的副本清孤儿。
async fn scrub_round(
    pool: &Arc<DiskPools>,
    replicas: &Arc<ReplicaManager>,
    health: &DiskHealthMap,
    metrics: &Arc<StoreServerMetrics>,
    state: &mut CrawlState,
    lanes: usize,
    batch_bytes: u64,
) -> usize {
    let mut candidates: Vec<Candidate> = Vec::new();
    for (pid, h) in replicas.handles() {
        if !eligible(&h, pid, health, metrics) {
            continue;
        }
        candidates.push((
            pid,
            h.store.clone(),
            h.disk_id.clone(),
            state.cursors.get(&pid).copied().flatten(),
        ));
    }
    state.cursors.retain(|pid, _| replicas.get(*pid).is_some());
    let n = candidates.len();
    if n == 0 {
        return 0;
    }
    let start = state.rr % n;
    let picks = lanes.min(n);
    state.rr += picks;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..picks {
        let (pid, store, disk, cursor) = candidates[(start + i) % n].clone();
        let pool = Arc::clone(pool);
        set.spawn(async move {
            let result = pool
                .run(&disk, "verify", move || {
                    store.verify_from(cursor, batch_bytes)
                })
                .await
                .map_err(|reject| reject.as_str().to_string());
            BatchOutcome { pid, result }
        });
    }
    let mut completed = 0;
    while let Some(joined) = set.join_next().await {
        let Ok(outcome) = joined else {
            continue;
        };
        match outcome.result {
            Ok(Ok(report)) => {
                completed += 1;
                metrics.scrub_background_batches.inc();
                metrics.scrub_background_bytes.inc_by(report.bytes_read);
                if report.bad_records > 0 || report.index_mismatch > 0 {
                    metrics
                        .scrub_background_bad_records
                        .inc_by(report.bad_records + report.index_mismatch);
                    tracing::warn!(
                        partition = outcome.pid,
                        bad_records = report.bad_records,
                        index_mismatch = report.index_mismatch,
                        first_bad_offset = ?report.first_bad_offset,
                        "background scrub found damage (operator action: verify/Repair/Move)"
                    );
                }
                state.cursors.insert(outcome.pid, report.end_cursor);
            }
            Ok(Err(e)) => {
                tracing::warn!(partition = outcome.pid, error = %e, "background scrub batch failed");
            }
            Err(reject) => {
                tracing::debug!(
                    partition = outcome.pid,
                    reason = %reject,
                    "background scrub batch skipped (disk pool busy or unavailable)"
                );
            }
        }
    }
    completed
}

/// 健康与追平两道闸(候选判据,返回 `false` 的都会留下可见痕迹或 debug 日志)。
fn eligible(
    h: &ReplicaHandle,
    pid: PartitionId,
    health: &DiskHealthMap,
    metrics: &StoreServerMetrics,
) -> bool {
    if !crate::disk_health::is_available(health, &h.disk_id) {
        metrics
            .disk_worker_skipped
            .with_label_values(&[h.disk_id.as_str(), "scrub"])
            .inc();
        tracing::debug!(
            partition = pid,
            disk = %h.disk_id,
            "background scrub skipped: disk unavailable"
        );
        return false;
    }
    let state = h.partition_state_now();
    if state.is_empty() {
        // 单机模式:没有"追平"语义,本地照扫(与 compaction 候选判据同一家的例外)。
        return true;
    }
    let current = h.current_state.read().expect("current_state lock poisoned");
    if current.as_str() != STATE_ACTIVE {
        tracing::debug!(
            partition = pid,
            replica_state = %current,
            "background scrub skipped: replica not caught up"
        );
        return false;
    }
    true
}

/// 基础间隔 ±25% 抖动:恒速定时器与 compaction 钟点/多节点同拍会共振成周期性尖峰,抖动去相位。
/// 抖动源用墙上时钟纳秒(不引 rand 依赖;各节点天然错相)。间隔 <4s 时无抖动余量,原样返回。
fn jittered(interval_secs: u64) -> Duration {
    let spread = interval_secs / 4;
    if spread == 0 {
        return Duration::from_secs(interval_secs);
    }
    let nano = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    let offset = (nano % (2 * spread + 1)) as i64 - spread as i64;
    let jittered = (interval_secs as i64 + offset).max(1);
    Duration::from_secs(jittered as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    use antfs_blob::{StoreConfig, StoreKey, StoreMetrics};

    use crate::config::{DataDir, StoreServerConfig};
    use crate::disk::DiskManager;
    use crate::health::DiskHealth;
    use crate::replica::STATE_BOOTSTRAP;

    /// 本模块的副本都建在 `pid = 1` 上 —— key 前缀必须跟着走(记录只存低 32bit)。
    fn key(n: u8) -> StoreKey {
        StoreKey::from_parts(1, n as u32)
    }

    fn manager(dir: &std::path::Path) -> ReplicaManager {
        let dm = DiskManager::mount(&[dir.to_string_lossy().into_owned().into()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
            defer_seal_flush: true,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        ReplicaManager::new(dm, sc, sm, 8 << 20)
    }

    /// 同工目录起一份每盘池(盘 id 与 `manager` 建出的副本一致,取目录 basename)。
    fn pool_for(dir: &std::path::Path) -> Arc<DiskPools> {
        let declared: DataDir = dir.to_string_lossy().into_owned().into();
        let dm = DiskManager::mount(std::slice::from_ref(&declared)).unwrap();
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();
        let cfg = StoreServerConfig {
            data_dirs: vec![declared],
            ..Default::default()
        };
        Arc::new(DiskPools::new(&dm, &cfg, &metrics, crate::disk_health::new_map()).unwrap())
    }

    /// 同上,但显式定制池参数(0 = 不排队 / 不等,便于造"满池")。
    fn pool_with(
        dir: &std::path::Path,
        workers: u32,
        queue: u32,
        timeout_ms: u64,
    ) -> Arc<DiskPools> {
        let declared: DataDir = dir.to_string_lossy().into_owned().into();
        let dm = DiskManager::mount(std::slice::from_ref(&declared)).unwrap();
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();
        let cfg = StoreServerConfig {
            data_dirs: vec![declared],
            pool_default_size: workers,
            pool_queue_size: queue,
            pool_acquire_timeout_ms: timeout_ms,
            ..Default::default()
        };
        Arc::new(DiskPools::new(&dm, &cfg, &metrics, crate::disk_health::new_map()).unwrap())
    }

    type Fixture = (
        tempfile::TempDir,
        Arc<ReplicaManager>,
        Arc<DiskPools>,
        DiskHealthMap,
        Arc<StoreServerMetrics>,
    );

    /// 一套可复用的被测对象(同一 Registry —— 指标断言对真实接入的那个实例做)。
    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let rm = Arc::new(manager(tmp.path()));
        let pool = pool_for(tmp.path());
        let registry = antfs_metric::Registry::new();
        let metrics = Arc::new(StoreServerMetrics::new(&registry).unwrap());
        (tmp, rm, pool, crate::disk_health::new_map(), metrics)
    }

    /// 3 条 128B 记录(每条磁盘占 152B = 24 头开销 + 128 数据);单条预算 = 152。
    const REC: u64 = 24 + 128;

    fn populated_replica(rm: &ReplicaManager) -> ReplicaHandle {
        let h = rm.ensure_replica(1).unwrap();
        h.mark_active();
        for n in 0..3u8 {
            h.store.put(key(n), &[n; 128]).unwrap();
        }
        h
    }

    /// 预算恰一条:三轮接力恰好覆盖全库一次(字节数 = 3×152 且不重扫),第四轮从头部开新周期
    /// (`end_cursor = None` 之后游标归零)。每轮只完成 1 个 batch —— 蜗牛爬步长受预算钳制。
    #[tokio::test]
    async fn round_crawls_stepwise_and_repeats_cycles() {
        let (tmp, rm, pool, health, metrics) = fixture();
        let h = populated_replica(&rm);
        h.store.flush().unwrap();

        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        for expect_done in [1usize, 1, 1, 1] {
            let done = scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC).await;
            assert_eq!(done, expect_done, "lane=1 每 tick 恰一个 batch");
        }
        assert_eq!(
            metrics.scrub_background_batches.get(),
            4,
            "三轮覆盖一轮周期 + 第四轮开新周期"
        );
        assert_eq!(
            metrics.scrub_background_bytes.get(),
            4 * REC,
            "3×152 覆盖一次 + 新周期第一批:不重不漏"
        );
        let _ = tmp; // 持有寿命
    }

    /// 盘被喂成 UNAVAILABLE(与生产同一条迟滞路径):本轮整盘跳过且可计数
    /// (`disk_worker_skipped{task="scrub"}`),不产生任何读。
    #[tokio::test]
    async fn round_skips_unavailable_disk_visibly() {
        let (tmp, rm, pool, health, metrics) = fixture();
        populated_replica(&rm);
        let disk = rm.handles()[0].1.disk_id.clone();

        let hyst = StoreServerConfig::default().hysteresis();
        for _ in 0..hyst.bad_rounds.max(1) {
            crate::disk_health::observe_disk(
                &health,
                &metrics,
                &disk,
                DiskHealth::IoTimeout,
                &hyst,
            );
        }
        assert!(
            !crate::disk_health::is_available(&health, &disk),
            "前置:盘应已摘"
        );

        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        let done = scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC).await;
        assert_eq!(done, 0, "摘掉的盘本轮必须跳过");
        assert_eq!(
            metrics
                .disk_worker_skipped
                .with_label_values(&[disk.as_str(), "scrub"])
                .get(),
            1,
            "跳过必须可见(否则表现为'巡检悄悄不跑')"
        );
        let _ = tmp;
    }

    /// 集群模式下未追平(BOOTSTRAP)的副本不扫(还在被拉复制填充);单机/未知状态不受限。
    #[tokio::test]
    async fn round_skips_bootstrap_replica_in_cluster_mode() {
        let (tmp, rm, pool, health, metrics) = fixture();
        let h = populated_replica(&rm);
        assert!(rm.set_partition_state(1, "RW"), "状态下发应写入");
        *h.current_state.write().unwrap() = STATE_BOOTSTRAP.to_string();

        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        assert_eq!(
            scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC).await,
            0,
            "BOOTSTRAP 副本本轮跳过"
        );
        // 单机模式(没收到过状态下发):本地照扫 —— 用清掉状态下发的第二套用例表达。
        let (tmp2, rm2, pool2, health2, metrics2) = fixture();
        populated_replica(&rm2);
        let mut state2 = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        assert_eq!(
            scrub_round(&pool2, &rm2, &health2, &metrics2, &mut state2, 1, REC).await,
            1,
            "单机模式(未知状态)不受追平闸限制"
        );
        let _ = (tmp, tmp2);
    }

    /// SEALED 分区的副本**照扫** —— 与 compaction 候选集刻意相反:封存冷数据正是巡检的对象,
    /// 而巡检只读不搬,不会干扰后续 EC 转换。
    #[tokio::test]
    async fn round_still_scans_sealed_partition() {
        let (tmp, rm, pool, health, metrics) = fixture();
        populated_replica(&rm);
        assert!(rm.set_partition_state(1, "SEALED"));

        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        assert_eq!(
            scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC).await,
            1,
            "SEALED 不在 compaction 候选集,但在巡检候选集"
        );
        let _ = tmp;
    }

    /// 池满(唯一 worker 被占、队列 0):本轮退避返回 0 而不是挂等 —— 一块忙盘不该冻住巡检 tick,
    /// 放行后同池照常执行。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_pool_round_backs_off_without_hang() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = Arc::new(manager(tmp.path()));
        let pool = pool_with(tmp.path(), 1, 0, 0);
        let registry = antfs_metric::Registry::new();
        let metrics = Arc::new(StoreServerMetrics::new(&registry).unwrap());
        let health = crate::disk_health::new_map();
        populated_replica(&rm);
        let disk = rm.handles()[0].1.disk_id.clone();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let p = pool.clone();
        let d = disk.clone();
        let hold = tokio::spawn(async move {
            p.run(&d, "test_hold", move || {
                let _ = started_tx.send(());
                let _ = release_rx.blocking_recv();
            })
            .await
        });
        started_rx.await.expect("占住唯一 worker");

        let mut state = CrawlState {
            cursors: HashMap::new(),
            rr: 0,
        };
        let done = tokio::time::timeout(
            Duration::from_secs(10),
            scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC),
        )
        .await
        .expect("池满时本轮必须快速返回");
        assert_eq!(done, 0, "池满 ⇒ 跳过本轮,绝不挂等");

        release_tx.send(()).unwrap();
        hold.await.expect("join hold").expect("hold 应完成");
        let done = tokio::time::timeout(
            Duration::from_secs(10),
            scrub_round(&pool, &rm, &health, &metrics, &mut state, 1, REC),
        )
        .await
        .expect("放行后照常执行");
        assert_eq!(done, 1, "放行后该盘恢复巡检");
    }

    /// 抖动界:所有抽样都落在 [0.75×interval, 1.25×interval] 内;间隔 <4s 无余量原样返回。
    #[test]
    fn jitter_stays_within_quarter_band() {
        let interval = 8u64;
        for _ in 0..200 {
            let d = jittered(interval);
            assert!(
                d >= Duration::from_secs(6) && d <= Duration::from_secs(10),
                "{d:?}"
            );
        }
        for small in 1u64..4 {
            assert_eq!(jittered(small), Duration::from_secs(small));
        }
    }
}
