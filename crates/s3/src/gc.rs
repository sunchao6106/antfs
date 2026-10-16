//! S3 网关后台 GC:`s3-metastore` 里 spec 承诺的「软删行保留期清理 + 孤儿 multipart TTL 回收」。
//!
//! 三面泄漏语义(见 `openspec/changes/s3-gc/`):
//! 1. **软删/遮蔽行**:`delete_object`/overwrite 时 `state=0, deleted_at=now`,行进保留期;
//!    本循环 `purge_expired_deletions` 过期清行 —— blob 已在语义动作时被 handler 删除,此处不碰 blob。
//! 2. **孤儿 multipart**:`take_expired_uploads` 原子取件(DELETE ... RETURNING)后走既有
//!    `abort_multipart` 语义(逐片 `router.delete` + 两表清理)。
//! 3. 多实例安全:取件语句原子;blob 重复删除由引擎墓碑幂等兜底。
//!
//! 循环形态:每实例各自周期跑(无需 leader);逐条目失败只记 `s3_gc_errors_total{kind}` 继续,
//! 循环体永不 panic(后台线程,panic 即服务面缺环)。

use crate::config::GcConfig;
use crate::metastore::types::BlobId;
use crate::server::AppState;

/// 单轮至多清的行数 / 回收的 session 数 / 补删的 WAL 行数 —— 背压常量,
/// 防长事务与一次海量 delete 打爆盘池。
const GC_BATCH_ROWS: i64 = 1000;
const GC_BATCH_UPLOADS: i64 = 100;
const GC_BATCH_RECLAIMS: u64 = 200;

/// 起后台 GC 循环(spawn;调用方持有 AppState 的 clone 即可,返回 JoinHandle 供测试/关停)。
pub fn spawn(state: AppState, cfg: GcConfig) -> tokio::task::JoinHandle<()> {
    if !cfg.enabled {
        tracing::info!("s3 gc: disabled by config");
        return tokio::spawn(async {});
    }
    tracing::info!(
        interval_secs = cfg.interval_secs,
        retention_secs = cfg.deletion_retention_secs,
        upload_ttl_secs = cfg.upload_ttl_secs,
        "s3 gc loop starting"
    );
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(cfg.interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // 先做事再等:上线即刻清一茬积压(部署后第一轮不等满周期)。
            run_once(&state, &cfg).await;
            tick.tick().await;
        }
    })
}

/// 单轮 GC:可独立调用(测试直接驱动)。幂等、逐条容错。
pub async fn run_once(state: &AppState, cfg: &GcConfig) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    run_once_at(state, cfg, now_ms).await
}

/// [`run_once`](Self::run_once) 的时间注入版:测试用未来时刻驱动 TTL/保留到期,免 sleep。
pub async fn run_once_at(state: &AppState, cfg: &GcConfig, now_ms: i64) {
    let timer = state.metrics.gc_duration_seconds.start_timer();
    let rows_cutoff = now_ms - cfg.deletion_retention_secs as i64 * 1000;
    let uploads_cutoff = now_ms - cfg.upload_ttl_secs as i64 * 1000;

    // ---- 1. 软删/遮蔽行清理(blob 不碰,死点在语义动作时)----
    loop {
        match state
            .metastore
            .purge_expired_deletions(rows_cutoff, GC_BATCH_ROWS as u64)
            .await
        {
            // 单语句原子取件:两实例并发同批,行级锁保证不重复计数。
            Ok(0) => break,
            Ok(n) => {
                state.metrics.gc_purged_rows_total.inc_by(n);
                if n < GC_BATCH_ROWS as u64 {
                    break;
                }
                // 满批 = 可能还有积压,立即续批(不睡interval)。
            }
            Err(e) => {
                state
                    .metrics
                    .gc_errors_total
                    .with_label_values(&["purge"])
                    .inc();
                tracing::warn!(error = %e, "s3 gc: purge_expired_deletions failed");
                break;
            }
        }
    }

    // ---- 2. 孤儿 multipart 回收(take 已删 session 行 → parts 由本循环直接清)----
    // 不走 `abort_multipart`:其前置检查依赖 session 行存在,而 take 已删之;
    // parts 清理(取件删行 + 逐 blob 墓碑)在此内联,与 abort 同语义。
    loop {
        match state
            .metastore
            .take_expired_uploads(uploads_cutoff, GC_BATCH_UPLOADS as u64)
            .await
        {
            Ok(ids) if ids.is_empty() => break,
            Ok(ids) => {
                let full = ids.len() as i64 >= GC_BATCH_UPLOADS;
                for upload_id in ids {
                    state.metrics.gc_orphan_uploads_total.inc();
                    match state.metastore.delete_parts_of_session(&upload_id).await {
                        Ok(blob_ids) => {
                            for bid in blob_ids {
                                state.metrics.gc_orphan_parts_blobs_total.inc();
                                if let Err(e) = state.router.delete(&bid).await {
                                    // 交叠窗重复删安全;真失败记 kind 继续下一片。
                                    state
                                        .metrics
                                        .gc_errors_total
                                        .with_label_values(&["blob"])
                                        .inc();
                                    tracing::warn!(error = %e, "s3 gc: orphan part blob delete failed");
                                }
                            }
                        }
                        Err(e) => {
                            state
                                .metrics
                                .gc_errors_total
                                .with_label_values(&["parts"])
                                .inc();
                            tracing::warn!(error = %e, upload_id = %upload_id, "s3 gc: orphan parts purge failed");
                        }
                    }
                }
                if !full {
                    break;
                }
            }
            Err(e) => {
                state
                    .metrics
                    .gc_errors_total
                    .with_label_values(&["take"])
                    .inc();
                tracing::warn!(error = %e, "s3 gc: take_expired_uploads failed");
                break;
            }
        }
    }

    // ---- 3. 回收 WAL 补删(崩窗/墓碑失败残行;非破坏读,成功才 resolve)----
    match state
        .metastore
        .pending_reclaims_batch(GC_BATCH_RECLAIMS)
        .await
    {
        Ok(pending) => {
            let mut resolved: Vec<BlobId> = Vec::with_capacity(pending.len());
            for bid in &pending {
                if state.router.delete(bid).await.is_ok() {
                    resolved.push(*bid);
                } else {
                    // 行留,下轮重试;交叠双删幂等安全。
                    state
                        .metrics
                        .gc_errors_total
                        .with_label_values(&["pending"])
                        .inc();
                    tracing::warn!(blob = ?bid, "s3 gc: pending reclaim tombstone failed (row kept for retry)");
                }
            }
            let n = resolved.len() as u64;
            if let Err(e) = state.metastore.resolve_pending_reclaims(&resolved).await {
                state
                    .metrics
                    .gc_errors_total
                    .with_label_values(&["pending-resolve"])
                    .inc();
                tracing::warn!(error = %e, "s3 gc: pending reclaim resolve failed (rows kept)");
            } else if n > 0 {
                state.metrics.gc_pending_resolved_total.inc_by(n);
            }
        }
        Err(e) => {
            state
                .metrics
                .gc_errors_total
                .with_label_values(&["pending-take"])
                .inc();
            tracing::warn!(error = %e, "s3 gc: pending_reclaims_batch failed");
        }
    }
    match state.metastore.pending_reclaims_count().await {
        Ok(backlog) => state.metrics.gc_pending_backlog.set(backlog as i64),
        Err(e) => {
            tracing::warn!(error = %e, "s3 gc: pending_reclaims_count failed (gauge skipped)");
        }
    }

    state.metrics.gc_runs_total.inc();
    timer.observe_duration();
}
