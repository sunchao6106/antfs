//! S4 本地维护 worker:数据节点常驻后台,周期性驱动 `blob` 引擎的 flush / compaction。
//!
//! 对齐 Ambry `CompactionManager` 跑在 `StorageManager` 里的定位:**本地、轻、常驻**,只维护本节点
//! 自己的 replica,不做集群级再复制/再平衡(那是 master 的 reconciler)。
//!
//! 两件事的节拍不同,故分两条 loop:
//! - **flush**:固定周期(默认 30s),只推进持久化边界;
//! - **compaction**:按「首跑钟点 + 周期」调度(默认每天本地 01:00 一次),跑在低峰;
//!   候选集与触发条件见 [`compact_all`]。
//!
//! 引擎调用是同步阻塞(路线乙),故按盘提交到 [`DiskPools`] 的**每盘池**执行:不阻塞 reactor,
//! 且一块盘挂死只占它自己的 worker 线程(见 `disk_pool` 与 store.md §12.11)。

use std::sync::Arc;
use std::time::Duration;

use antfs_blob::CompactPolicy;

use crate::config::{parse_hhmm, CompactionPolicyKind, StoreServerConfig};
use crate::disk_health::DiskHealthMap;
use crate::disk_pool::DiskPools;
use crate::metrics::StoreServerMetrics;
use crate::replica::{ReplicaHandle, ReplicaManager};

/// 对本节点每个 replica 做一次 flush,返回成功 flush 的 replica 数。
///
/// flush 是持久化边界(§3.1):把已入 page-cache 的记录推进到 fsync,只增强持久性。
/// 单个 replica 失败只记 warn 并继续(维护尽力而为,不因一盘拖垮全局)。
///
/// **每个 replica 各自并发提交到它那块盘的池**(而不是顺序跑一遍):一块盘挂死只卡住它自己的任务,
/// 其余盘该 flush 的照 flush —— 顺序版本会让一块坏盘把全节点的持久化边界一起冻住。
///
/// **盘不可服务 ⇒ 整块跳过**(D2):往一块已知卡死的盘上再塞任务,只是白占一条收不回来的线程,
/// 而且下一轮还会再塞一个。跳过计数落 `disk_worker_skipped{task="flush"}`,让"这块盘没在推进"
/// 这件事**可见**(否则表现为"副本悄悄不 flush 了")。
pub async fn flush_all(
    pool: &Arc<DiskPools>,
    replicas: &ReplicaManager,
    health: &DiskHealthMap,
    metrics: &StoreServerMetrics,
) -> usize {
    let mut tasks = Vec::new();
    for (pid, h) in replicas.handles() {
        if !crate::disk_health::is_available(health, &h.disk_id) {
            metrics
                .disk_worker_skipped
                .with_label_values(&[h.disk_id.as_str(), "flush"])
                .inc();
            tracing::debug!(
                partition = pid,
                disk = %h.disk_id,
                "local-worker flush skipped: disk unavailable"
            );
            continue;
        }
        let store = h.store.clone();
        let disk = h.disk_id.clone();
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            match pool.run(&disk, "flush", move || store.flush()).await {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    tracing::warn!(partition = pid, error = %e, "local-worker flush failed");
                    false
                }
                Err(reject) => {
                    tracing::warn!(
                        partition = pid,
                        disk = %disk,
                        reason = reject.as_str(),
                        "local-worker flush skipped by disk pool"
                    );
                    false
                }
            }
        }));
    }
    let mut n = 0;
    for t in tasks {
        if let Ok(true) = t.await {
            n += 1;
        }
    }
    n
}

/// 对本节点每个 replica 按策略压缩,返回实际发生压缩的 replica 数。
///
/// **策略分派**(`config.compaction_policy`,对齐 Ambry `CompactionPolicy` 体系):
/// - `Incremental` → [`BlobStore::compact_if_needed`](antfs_blob::BlobStore::compact_if_needed);
/// - `CostBenefit` → [`BlobStore::compact_cost_benefit`](antfs_blob::BlobStore::compact_cost_benefit);
/// - `Hybrid` → `do_compact_all`(本轮是否轮到全库重写,由 [`should_compact_all`] 逐轮判定)为真时
///   走 [`BlobStore::compact`](antfs_blob::BlobStore::compact)(CompactAll),否则走 cost-benefit。
///
/// `do_compact_all` 是**节点级、逐轮**的判定,非 Hybrid 策略恒为 `false`(它们从不自动触发 CompactAll)。
///
/// **候选集**(按 master 下发的分区可写态,见 [`crate::replica::ReplicaHandle::partition_state_now`]):
/// - `SEALED`:永不入选(封存分区留给后续 EC 转换,不在其身上搬字节);
/// - `RO`:总是入选;
/// - `RW`:仅当 `include_rw`(默认纳入)—— 引擎只压其**封存段**,活跃段照写不误;
/// - 未知(`""`,未接 master 的单机模式 / 指令还没到):按可压处理,保持改造前的行为。
///
/// **额外跳过**:已接 master(收到过状态下发)时,**未追平**的副本不压 —— 压掉的可回收字节马上又要
/// 从 peer 拉回来,而且 compaction 改写 offset 会让在飞的复制游标失效。单机模式没有"追平"语义
/// (副本永远停在 `BOOTSTRAP`),故不按实然态跳过,否则本地维护在这类部署里会彻底停摆。
///
/// **触发条件**在引擎内:partition 整体可回收占比 ≥ `policy.dead_ratio` 才动手(回收口径 = 被取代的
/// 旧记录 + TTL 已过期对象 + 墓碑已过保留期**且过了墓碑回收闸门**)。
pub async fn compact_all(
    pool: &Arc<DiskPools>,
    replicas: &ReplicaManager,
    health: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    config: &StoreServerConfig,
    include_rw: bool,
    do_compact_all: bool,
) -> usize {
    let mut n = 0;
    for (pid, h) in replicas.handles() {
        // 盘不可服务 ⇒ 跳过(D2)。**放在最前面**:下面那些判据(候选集/追平)都要先读副本状态,
        // 而对一块已知坏盘的副本,任何判据都没有意义 —— 早退一条,少一轮白干。
        if !crate::disk_health::is_available(health, &h.disk_id) {
            metrics
                .disk_worker_skipped
                .with_label_values(&[h.disk_id.as_str(), "compaction"])
                .inc();
            tracing::debug!(
                partition = pid,
                disk = %h.disk_id,
                "local-worker compaction skipped: disk unavailable"
            );
            continue;
        }
        let state = h.partition_state_now();
        if !compaction_candidate(&state, include_rw) {
            continue;
        }
        if !state.is_empty() {
            if let Some(reason) = skip_reason(&h) {
                tracing::debug!(partition = pid, reason, "local-worker compaction skipped");
                continue;
            }
        }
        // 策略**逐副本现算**:墓碑回收闸门是**副本级**的(master 每拍按分区权威下发),全节点共用
        // 一个值会让"还有副本没追平"的分区被误回收墓碑;`now_ms` 也必须新鲜(保留期/过期判定
        // 以"现在"为准)。
        let policy = policy_from(
            config,
            tombstones_allowed(config.master_addr.is_empty(), &h),
        );
        let kind = config.compaction_policy;
        let min_reclaim = config.compaction_min_log_segment_count_to_reclaim;
        let store = h.store.clone();
        match pool
            .run(&h.disk_id, "compaction", move || match kind {
                CompactionPolicyKind::Incremental => store.compact_if_needed(&policy),
                CompactionPolicyKind::CostBenefit => {
                    store.compact_cost_benefit(&policy, min_reclaim)
                }
                CompactionPolicyKind::Hybrid if do_compact_all => store.compact(&policy).map(Some),
                CompactionPolicyKind::Hybrid => store.compact_cost_benefit(&policy, min_reclaim),
            })
            .await
        {
            Ok(Ok(Some(_))) => {
                let label = match (kind, do_compact_all) {
                    (CompactionPolicyKind::Incremental, _) => "incremental",
                    (CompactionPolicyKind::CostBenefit, _) => "cost_benefit",
                    (CompactionPolicyKind::Hybrid, true) => "compact_all",
                    (CompactionPolicyKind::Hybrid, false) => "cost_benefit",
                };
                metrics
                    .compaction_policy_used
                    .with_label_values(&[label])
                    .inc();
                n += 1;
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => {
                tracing::warn!(partition = pid, error = %e, "local-worker compaction failed")
            }
            // 池拒(盘被压满/未知盘):本轮跳过,下一轮再来(压缩是尽力而为)。
            Err(reject) => tracing::warn!(
                partition = pid,
                disk = %h.disk_id,
                reason = reject.as_str(),
                "local-worker compaction skipped by disk pool"
            ),
        }
    }
    n
}

/// Hybrid 模式:本轮是否该用 CompactAll 而不是 cost-benefit。
///
/// - 非 Hybrid 策略:`false`(它们从不自动触发 CompactAll);
/// - Hybrid 且无历史(`None`):`true`(首跑触发一次全库重写,对齐 Ambry);
/// - Hybrid 且距上次 ≥ `switch_days` 天:`true`。
fn should_compact_all(
    policy: CompactionPolicyKind,
    switch_days: u32,
    last_compact_all: Option<i64>,
    now: i64,
) -> bool {
    if policy != CompactionPolicyKind::Hybrid {
        return false;
    }
    match last_compact_all {
        None => true,
        Some(last) => {
            let elapsed_secs = now.saturating_sub(last);
            elapsed_secs >= i64::from(switch_days) * 86_400
        }
    }
}

/// 分区可写态 → 是否进 compaction 候选集。
fn compaction_candidate(state: &str, include_rw: bool) -> bool {
    match state {
        "RO" => true,
        "RW" => include_rw,
        "SEALED" => false,
        // 未知状态:单机模式或指令未到,按可压处理(不因缺信息就永久不维护本地数据)。
        _ => true,
    }
}

/// 副本还没追平时跳过 compaction 的理由(返回 `None` = 可以压)。
///
/// `BOOTSTRAP`(还在追赶 peer)期间压本地是**白费力气**:被回收的字节马上又要从 peer 拉回来;
/// 更糟的是 compaction 改写 offset 会让在飞的复制游标失效。等追平(`ACTIVE`)再压。
fn skip_reason(h: &ReplicaHandle) -> Option<&'static str> {
    let state = h.current_state.read().expect("current_state lock poisoned");
    (state.as_str() != crate::replica::STATE_ACTIVE).then_some("replica not active")
}

/// 起两条后台维护 loop:flush(固定周期)+ compaction(首跑钟点对齐、之后按周期)。
///
/// 仅在 `local_worker_enabled` 时由 [`crate::server::StoreServer::serve`] 调用。
pub fn spawn_local_worker(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    pool: Arc<DiskPools>,
    health: DiskHealthMap,
) {
    spawn_loop(
        Duration::from_secs(config.flush_interval_secs),
        replicas.clone(),
        metrics.clone(),
        pool.clone(),
        health.clone(),
        |r, m, p, h| async move {
            let n = flush_all(&p, &r, &h, &m).await;
            m.flushes.inc_by(n as u64);
        },
    );
    spawn_compaction_loop(config, replicas, metrics, pool, health);
}

/// compaction loop:首跑对齐本地 `compaction_start_time`,之后每 `compaction_interval_secs` 一次。
///
/// 每轮**重新现算**策略(`now_ms` 必须新鲜,否则保留期/过期判定会用到旧时间),故 loop 里不留
/// 陈旧策略。等待时长由 [`next_delay_secs`] 纯函数算出,便于单测。
fn spawn_compaction_loop(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    pool: Arc<DiskPools>,
    health: DiskHealthMap,
) {
    // 配置已过 `Validate`,这里的解析失败只可能是"运行时改坏了字符串",退回默认钟点继续跑。
    let start_secs_of_day = match parse_hhmm(&config.compaction_start_time) {
        Ok(s) => s,
        Err(why) => {
            tracing::warn!(
                value = %config.compaction_start_time,
                error = %why,
                "invalid compaction_start_time, falling back to 01:00"
            );
            3600
        }
    };
    let period_secs = config.compaction_interval_secs;
    let clock = SystemLocalClock;

    tokio::spawn(async move {
        let mut last_fire: Option<i64> = None;
        // Hybrid 调度状态:上次 CompactAll 的 unix 秒。首轮从盘上读回(重启不丢),此后由本 loop
        // 维护 —— 每轮据此决定走 CompactAll 还是 cost-benefit。
        let mut last_compact_all: Option<i64> = None;
        let mut state_loaded = false;
        loop {
            let now = now_epoch_secs();
            let delay = next_delay_secs(now, last_fire, start_secs_of_day, period_secs, &clock);
            tokio::time::sleep(Duration::from_secs(delay)).await;

            if !state_loaded {
                last_compact_all = read_round_compact_state(&replicas);
                state_loaded = true;
            }

            let round_now = now_epoch_secs();
            let do_compact_all = should_compact_all(
                config.compaction_policy,
                config.compaction_policy_switch_days,
                last_compact_all,
                round_now,
            );

            let cfg = config.clone();
            let r = replicas.clone();
            let m = metrics.clone();
            let h = health.clone();
            let n = compact_all(
                &pool,
                &r,
                &h,
                &m,
                &cfg,
                cfg.compaction_include_rw,
                do_compact_all,
            )
            .await;
            m.compactions.inc_by(n as u64);

            // 本轮真的发生了全库重写(至少一个副本)才推进时间戳并落盘;全部副本都被跳过时保持旧值,
            // 下一轮再试(安全方向:宁可多跑一次,不可漏跑)。
            if do_compact_all && n > 0 {
                last_compact_all = Some(round_now);
                write_round_compact_state(&replicas, round_now);
            }
            last_fire = Some(now_epoch_secs());
        }
    });
}

/// 扫描本节点所有 replica 的状态文件,取**最新**的 CompactAll 时间戳。
/// 时间戳是**节点级**概念(所有副本在同一轮做同一次 CompactAll),故任一副本落了盘即有效,取最大值。
fn read_round_compact_state(replicas: &ReplicaManager) -> Option<i64> {
    replicas
        .handles()
        .iter()
        .filter_map(|(pid, _)| replicas.replica_dir(*pid))
        .filter_map(|dir| crate::compact_state::read_last_compact_all(&dir))
        .max()
}

/// 把本轮的 CompactAll 时间戳写到**每个** replica 目录(节点级状态冗余落盘:任一副本目录被读到即可)。
fn write_round_compact_state(replicas: &ReplicaManager, epoch_secs: i64) {
    for (pid, _) in replicas.handles() {
        let Some(dir) = replicas.replica_dir(pid) else {
            continue;
        };
        if let Err(e) = crate::compact_state::write_last_compact_all(&dir, epoch_secs) {
            tracing::warn!(
                partition = pid,
                error = %e,
                "failed to persist compact-all timestamp"
            );
        }
    }
}

/// 由配置装配 compaction 策略;**每次调用都取当前时钟**(过期/保留期判定以"现在"为准),
/// 并按该副本的**墓碑回收闸门**决定要不要回收墓碑(见 [`tombstones_allowed`])。
pub(crate) fn policy_from(config: &StoreServerConfig, tombstones_allowed: bool) -> CompactPolicy {
    CompactPolicy::new(
        config.compaction_dead_ratio,
        now_epoch_ms(),
        (config.compaction_tombstone_retention_secs as i64).saturating_mul(1000),
    )
    .with_tombstones_allowed(tombstones_allowed)
}

/// 该副本此刻是否**放行墓碑回收**(compaction 判活口径的一部分)。
///
/// 闸门是 **master 权威**的(每拍随 `partition_states` 全量下发,语义 = 该分区**所有副本都已追平**):
/// 还有副本没追平时回收墓碑,那份滞后副本就永远学不到删除,已删对象会以"活数据"永久留在它上面
/// (僵尸数据)。故**没拿到权威放行前一律不放行**(句柄初值 `false`)。
///
/// **例外:单机模式**(`standalone` = 未接 master,`master_addr` 为空)—— 没有别的副本可滞后,闸门
/// 恒放行;否则墓碑会在这类部署里永久堆在盘上(改造前不是这样)。
///
/// 定时 compaction 与手动 compact 共用本判据,免得两处口径漂移。
pub(crate) fn tombstones_allowed(standalone: bool, h: &ReplicaHandle) -> bool {
    standalone || h.tombstones_reclaim_allowed()
}

/// 当前 unix 秒。
pub(crate) fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 当前 unix 毫秒(策略的"现在";手动 compaction 也用同一把尺子)。
pub(crate) fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 距下次 compaction 触发的等待秒数(`0` = 立刻跑)。
///
/// - **首跑**(`last_fire` 为 `None`):下一个**本地** `start_secs_of_day` 时刻 —— 今天还没到就是今天,
///   已经过了就是明天(例:起点 02:00、现在 23:00 ⇒ 明天 02:00);
/// - **之后**:上次触发 + `period_secs`(`period = 86400` 即每天同一钟点;周期以秒计,跨夏令时会整体
///   平移一小时 —— 对凌晨维护窗口无碍)。
fn next_delay_secs<C: LocalClock>(
    now: i64,
    last_fire: Option<i64>,
    start_secs_of_day: u32,
    period_secs: u64,
    clock: &C,
) -> u64 {
    let next = match last_fire {
        None => {
            let today = clock.secs_of_day(now);
            let days = if today < start_secs_of_day { 0 } else { 1 };
            clock.at_local_day_offset(now, days, start_secs_of_day)
        }
        Some(prev) => prev.saturating_add(period_secs as i64),
    };
    next.saturating_sub(now).max(0) as u64
}

/// 本地时钟的两件事:把 epoch 拆成"本地当日秒数"、以及在**本地日历**上顺延 N 天的指定时刻。
///
/// 抽成 trait 是为了让调度可单测:测试用固定偏移的假时钟,生产用 [`SystemLocalClock`]。
trait LocalClock {
    /// 本地当日秒数(0..86400)。
    fn secs_of_day(&self, epoch: i64) -> u32;
    /// 在**本地日历**上从 `epoch` 所在那天顺延 `days` 天的 `secs_of_day` 时刻。
    ///
    /// 顺延必须是"本地日历的一天"而不是"加 86400 秒" —— 夏令时切换日两者差一小时。
    fn at_local_day_offset(&self, epoch: i64, days: i64, secs_of_day: u32) -> i64;
}

/// 系统本地时区(libc `localtime_r` / `mktime`,macOS/Linux 通用)。
struct SystemLocalClock;

impl LocalClock for SystemLocalClock {
    fn secs_of_day(&self, epoch: i64) -> u32 {
        let t = epoch as libc::time_t;
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        if unsafe { libc::localtime_r(&t, &mut tm) }.is_null() {
            // 拿不到本地时区时退回 UTC 口径:维护任务照跑,只是钟点按 UTC 算(记日志已足够)。
            return epoch.rem_euclid(86_400) as u32;
        }
        (tm.tm_hour as u32) * 3600 + (tm.tm_min as u32) * 60 + tm.tm_sec as u32
    }

    fn at_local_day_offset(&self, epoch: i64, days: i64, secs_of_day: u32) -> i64 {
        let fallback = || epoch - epoch.rem_euclid(86_400) + days * 86_400 + i64::from(secs_of_day);
        let t = epoch as libc::time_t;
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        if unsafe { libc::localtime_r(&t, &mut tm) }.is_null() {
            return fallback();
        }
        tm.tm_hour = (secs_of_day / 3600) as libc::c_int;
        tm.tm_min = ((secs_of_day % 3600) / 60) as libc::c_int;
        tm.tm_sec = 0;
        tm.tm_mday += days as libc::c_int; // mktime 会做日期归一(跨月/跨年)
        tm.tm_isdst = -1; // 让 mktime 自己判定夏令时
        let out = unsafe { libc::mktime(&mut tm) };
        if out == -1 {
            fallback()
        } else {
            out as i64
        }
    }
}

/// 通用维护 loop:按 `period` 节拍,把同步维护动作 offload 到阻塞线程池执行。
fn spawn_loop<F, Fut>(
    period: Duration,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    pool: Arc<DiskPools>,
    health: DiskHealthMap,
    action: F,
) where
    F: Fn(Arc<ReplicaManager>, Arc<StoreServerMetrics>, Arc<DiskPools>, DiskHealthMap) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let action = Arc::new(action);
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            let (r, m, p, h) = (
                replicas.clone(),
                metrics.clone(),
                pool.clone(),
                health.clone(),
            );
            let action = action.clone();
            action(r, m, p, h).await;
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

    /// 同一目录上再建一份每盘池(盘 id 与 [`manager`] 建出来的副本一致,都取目录 basename);
    /// 池参数全用**默认值**。
    fn pool_for(dir: &std::path::Path) -> Arc<DiskPools> {
        let declared: crate::config::DataDir = dir.to_string_lossy().into_owned().into();
        let dm = DiskManager::mount(std::slice::from_ref(&declared)).unwrap();
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();
        let cfg = StoreServerConfig {
            data_dirs: vec![declared],
            ..Default::default()
        };
        Arc::new(DiskPools::new(&dm, &cfg, &metrics, crate::disk_health::new_map()).unwrap())
    }

    /// 同上,但**显式**指定三个池参数(`0` 就是 0:不排队 / 不等 —— 没有"0 = 用默认值"的隐式约定,
    /// 那种约定会让"想造一个满池"的用例其实造出一个带队列的池)。返回池与该盘 id。
    fn pool_with(
        dir: &std::path::Path,
        workers: u32,
        queue: u32,
        timeout_ms: u64,
    ) -> (Arc<DiskPools>, String) {
        let declared: crate::config::DataDir = dir.to_string_lossy().into_owned().into();
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
        let id = dm.disks()[0].id.clone();
        (
            Arc::new(DiskPools::new(&dm, &cfg, &metrics, crate::disk_health::new_map()).unwrap()),
            id,
        )
    }

    /// 池被压满时,flush / compaction **跳过该盘而不是卡死整轮** —— 一块坏盘不该冻住全节点的本地维护;
    /// 放行之后同一块盘照常维护(拒绝没留下副作用)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_disk_pool_makes_worker_skip_not_hang() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let (pool, disk) = pool_with(tmp.path(), 1, 0, 0);
        let (health, metrics) = health_and_metrics();
        let _h = handle_with_dead_data(&rm);

        // 占住该盘唯一的 worker(等测试放行)。
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
        started_rx.await.expect("占住 worker");

        // 两条维护路径都必须**返回**(而不是等满准入超时后还挂在那里):池满 ⇒ 本轮跳过该盘。
        let flushed = tokio::time::timeout(
            Duration::from_secs(10),
            flush_all(&pool, &rm, &health, &metrics),
        )
        .await
        .expect("池满时 flush 不该卡住");
        assert_eq!(flushed, 0, "池满 ⇒ 该盘本轮 flush 跳过");
        let compacted = tokio::time::timeout(
            Duration::from_secs(10),
            compact_all(&pool, &rm, &health, &metrics, &config(), true, false),
        )
        .await
        .expect("池满时 compaction 不该卡住");
        assert_eq!(compacted, 0, "池满 ⇒ 该盘本轮 compaction 跳过");

        // 放行 ⇒ 同一块盘恢复正常维护(许可没漏、也没有状态残留)。
        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("hold 应完成");
        let flushed = tokio::time::timeout(
            Duration::from_secs(10),
            flush_all(&pool, &rm, &health, &metrics),
        )
        .await
        .expect("放行后 flush 不该卡住");
        assert_eq!(flushed, 1, "放行后该盘 flush 应正常完成");
    }

    fn manager(dir: &std::path::Path) -> ReplicaManager {
        let dm = DiskManager::mount(&[dir.to_string_lossy().into_owned().into()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        ReplicaManager::new(dm, sc, sm, 8 << 20)
    }

    /// 定时 compaction 的默认口径配置(默认阈值与保留期)。这些用例只盯"死记录被回收",
    /// 墓碑闸门由 [`tombstone_reclamation_needs_the_master_gate`] 单独覆盖。
    fn config() -> StoreServerConfig {
        StoreServerConfig::default()
    }

    /// 测试用的**健康表 + 指标**:空表 = 每块盘都"从未观测 ⇒ 可服务",故后台维护**一块都不跳过**。
    /// 要造"某盘不可服务"的场景,用 [`crate::disk_health::observe_disk`] 把它喂成 `UNAVAILABLE`
    /// (经 `bad_rounds` 次坏观测,与生产同一条路径)。
    fn health_and_metrics() -> (DiskHealthMap, StoreServerMetrics) {
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        (crate::disk_health::new_map(), metrics)
    }

    /// 造一份"够压"的副本:写满一段以上再全删,产生达阈的封存段死数据。
    /// 副本实然态置 `ACTIVE`(否则 worker 会跳过未追平的副本)。
    fn handle_with_dead_data(rm: &ReplicaManager) -> ReplicaHandle {
        let h = rm.ensure_replica(1).unwrap();
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

    #[tokio::test]
    async fn flush_all_persists_and_reopen_reads_back() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let pool = pool_for(tmp.path());
        let (health, metrics) = health_and_metrics();
        let h = rm.ensure_replica(1).unwrap();
        h.store.put(key(1), b"durable payload").unwrap();

        assert_eq!(
            flush_all(&pool, &rm, &health, &metrics).await,
            1,
            "one replica flushed"
        );

        rm.remove_replica(1);
        let h2 = rm.ensure_replica(1).unwrap();
        assert_eq!(
            h2.store.get(&key(1)).unwrap().as_slice(),
            b"durable payload"
        );
    }

    #[tokio::test]
    async fn compact_all_reclaims_dead_data() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let pool = pool_for(tmp.path());
        let (health, metrics) = health_and_metrics();
        let h = handle_with_dead_data(&rm);
        let before = h.store.stats().unwrap().dead_bytes;

        compact_all(&pool, &rm, &health, &metrics, &config(), true, false).await;

        let after = h.store.stats().unwrap().dead_bytes;
        assert!(
            after < before,
            "compaction must reduce dead bytes: {before} -> {after}"
        );
    }

    /// 策略指标按**实际走的路径**计:一次落到 CompactAll 的 compaction 必须让
    /// `compaction_policy_used{policy="compact_all"}` +1(而不是按配置值猜)。
    #[tokio::test]
    async fn compaction_policy_used_counts_compact_all() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let pool = pool_for(tmp.path());
        let (health, metrics) = health_and_metrics();
        let h = handle_with_dead_data(&rm);
        let before = h.store.stats().unwrap().dead_bytes;

        // 默认配置即 Hybrid;无状态文件 ⇒ `do_compact_all = true` ⇒ 本轮走 CompactAll。
        compact_all(&pool, &rm, &health, &metrics, &config(), true, true).await;

        let after = h.store.stats().unwrap().dead_bytes;
        assert!(
            after < before,
            "本用例的前提是确实压了一轮: {before} -> {after}"
        );
        assert!(
            metrics
                .compaction_policy_used
                .with_label_values(&["compact_all"])
                .get()
                >= 1,
            "走了 CompactAll 就必须计入 policy=compact_all"
        );
    }

    /// 墓碑回收闸门(master 权威):`false` 时墓碑**一律照搬**(连已过保留期的也不回收)——
    /// 因为还有副本没追平,墓碑一收它就永远学不到那条删除,已删对象会以"活数据"永久留在它上面
    /// (僵尸数据)。放行后同样的墓碑才真正消失。
    ///
    /// 单机模式(未接 master)没有别的副本可滞后 → 闸门恒放行,保持改造前的行为。
    #[test]
    fn tombstone_reclamation_needs_the_master_gate() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let h = rm.ensure_replica(1).unwrap();
        h.mark_active();
        h.store.put(key(1), b"payload-to-be-deleted").unwrap();
        h.store.delete(key(1)).unwrap();
        h.store.flush().unwrap();

        // 集群模式(接了 master)+ 保留期 0:墓碑一过删除时刻就"够期"—— 闸门是唯一变因。
        let cluster = StoreServerConfig {
            master_addr: "127.0.0.1:1".to_string(),
            compaction_tombstone_retention_secs: 0,
            ..Default::default()
        };
        let compact = |h: &ReplicaHandle| {
            h.store
                .compact(&policy_from(
                    &cluster,
                    tombstones_allowed(cluster.master_addr.is_empty(), h),
                ))
                .unwrap()
        };

        // 未放行:墓碑照搬,已删对象对外仍是 Deleted(而非 NotFound)。
        assert!(
            !h.tombstones_reclaim_allowed(),
            "没得到 master 放行前,句柄的闸门就该是关的"
        );
        let st = compact(&h);
        assert_eq!(
            st.tombstone_reclaimed_bytes, 0,
            "闸门关着不得回收墓碑:{st:?}"
        );
        assert!(
            matches!(h.store.get(&key(1)), Err(antfs_blob::Error::Deleted)),
            "墓碑必须原样留着"
        );

        // 放行:已过保留期的墓碑连同索引条目一起被回收。
        assert!(rm.set_tombstones_reclaim_allowed(1, true));
        assert!(!rm.set_tombstones_reclaim_allowed(1, true), "重复置位幂等");
        let st = compact(&h);
        assert!(st.tombstone_reclaimed_bytes > 0, "放行后应真的回收:{st:?}");
        assert!(
            matches!(h.store.get(&key(1)), Err(antfs_blob::Error::NotFound)),
            "墓碑应已被回收"
        );

        // 单机模式(未接 master):闸门恒放行(别让墓碑在这类部署里永久堆在盘上)。
        let standalone = StoreServerConfig::default();
        assert!(standalone.master_addr.is_empty());
        assert!(
            rm.set_tombstones_reclaim_allowed(1, false),
            "master 收回放行时必须真的关回去"
        );
        assert!(
            tombstones_allowed(standalone.master_addr.is_empty(), &h),
            "单机模式没有别的副本可滞后 → 恒放行(闸门关着也放行)"
        );
    }

    /// 候选集闸门矩阵:SEALED 永不入选;RW 由 `include_rw` 决定;RO 与"未知"总是入选。
    #[tokio::test]
    async fn candidate_gate_by_partition_state() {
        let cases = [
            ("RO", false, 1usize),
            ("RO", true, 1),
            ("RW", true, 1),
            ("RW", false, 0),
            ("SEALED", true, 0),
            ("SEALED", false, 0),
            ("", false, 1), // 单机模式:没有 master 指令也要维护本地数据
        ];
        for (state, include_rw, want) in cases {
            let tmp = tempfile::tempdir().unwrap();
            let rm = manager(tmp.path());
            let pool = pool_for(tmp.path());
            let (health, metrics) = health_and_metrics();
            let h = handle_with_dead_data(&rm);
            let before = h.store.stats().unwrap().dead_bytes;
            if !state.is_empty() {
                assert!(rm.set_partition_state(1, state), "状态应写入句柄");
            }
            let got =
                compact_all(&pool, &rm, &health, &metrics, &config(), include_rw, false).await;
            assert_eq!(got, want, "state={state:?} include_rw={include_rw}");
            let after = h.store.stats().unwrap().dead_bytes;
            if want == 1 {
                assert!(after < before, "入选就该真的回收: {before} -> {after}");
            } else {
                assert_eq!(after, before, "未入选不得动数据");
            }
        }
    }

    /// 未追平的副本(`BOOTSTRAP`)在**集群模式**下不压:盘上还在追赶,回收了也要从 peer 拉回来。
    /// 单机模式(状态为空 = 没接 master)不受此限,由 `compact_all` 里的判据区分。
    #[tokio::test]
    async fn bootstrap_replica_is_skipped_in_cluster_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let pool = pool_for(tmp.path());
        let (health, metrics) = health_and_metrics();
        let _h = handle_with_dead_data(&rm);
        rm.set_partition_state(1, "RO"); // 状态放行,卡在实然态上
        *rm.get(1).unwrap().current_state.write().unwrap() =
            crate::replica::STATE_BOOTSTRAP.to_string();
        assert_eq!(
            compact_all(&pool, &rm, &health, &metrics, &config(), true, false).await,
            0
        );

        // 单机模式(从未收到状态下发):没有"追平"语义,本地维护照跑。
        let tmp2 = tempfile::tempdir().unwrap();
        let rm2 = manager(tmp2.path());
        let pool2 = pool_for(tmp2.path());
        let (health2, metrics2) = health_and_metrics();
        let h2 = handle_with_dead_data(&rm2);
        // handle_with_dead_data 已置 ACTIVE;这里退回 BOOTSTRAP 并清掉状态,模拟单机副本。
        *h2.current_state.write().unwrap() = crate::replica::STATE_BOOTSTRAP.to_string();
        assert!(!rm2.set_partition_state(1, ""), "状态本来就是空的");
        assert_eq!(
            compact_all(&pool2, &rm2, &health2, &metrics2, &config(), true, false).await,
            1
        );
    }

    /// 固定偏移(UTC)假时钟:让调度逻辑与机器时区无关地可测。
    struct FakeClock;

    impl LocalClock for FakeClock {
        fn secs_of_day(&self, epoch: i64) -> u32 {
            epoch.rem_euclid(86_400) as u32
        }
        fn at_local_day_offset(&self, epoch: i64, days: i64, secs_of_day: u32) -> i64 {
            epoch - epoch.rem_euclid(86_400) + days * 86_400 + i64::from(secs_of_day)
        }
    }

    const DAY_START: i64 = 1_700_000_000 - (1_700_000_000 % 86_400); // 某天 00:00:00
    const ONE_AM: u32 = 3600;

    #[test]
    fn first_fire_is_next_local_start_time() {
        let c = FakeClock;
        // 23:00 起跑 → 明天 01:00(2 小时)。
        assert_eq!(
            next_delay_secs(DAY_START + 23 * 3600, None, ONE_AM, 86_400, &c),
            2 * 3600
        );
        // 00:30 → 今天 01:00(半小时)。
        assert_eq!(
            next_delay_secs(DAY_START + 1800, None, ONE_AM, 86_400, &c),
            1800
        );
        // 正好 01:00 → 今天的这一跑已经"过点",顺延到明天(不会立刻重复触发)。
        assert_eq!(
            next_delay_secs(DAY_START + 3600, None, ONE_AM, 86_400, &c),
            86_400
        );
    }

    #[test]
    fn later_fires_follow_the_period() {
        let c = FakeClock;
        let last = DAY_START + ONE_AM as i64;
        // 刚跑完 → 等一整天。
        assert_eq!(
            next_delay_secs(last, Some(last), ONE_AM, 86_400, &c),
            86_400
        );
        // 已经超时(如进程被挂起) → 立刻跑,不欠账。
        assert_eq!(
            next_delay_secs(last + 2 * 86_400, Some(last), ONE_AM, 86_400, &c),
            0
        );
        // 非整日周期:6 小时一跑。
        assert_eq!(
            next_delay_secs(last + 3600, Some(last), ONE_AM, 6 * 3600, &c),
            5 * 3600
        );
    }

    /// Hybrid 策略的逐轮判定矩阵:
    /// - 非 Hybrid(`Incremental`/`CostBenefit`)恒 `false`(从不自动 CompactAll);
    /// - Hybrid + 无历史 ⇒ `true`(首跑全库重写,对齐 Ambry);
    /// - Hybrid + 未满 `switch_days` ⇒ `false`;恰满 / 超过 ⇒ `true`。
    #[test]
    fn should_compact_all_matrix() {
        const DAY: i64 = 86_400;
        let now = 1_700_000_000;

        for policy in [
            CompactionPolicyKind::Incremental,
            CompactionPolicyKind::CostBenefit,
        ] {
            assert!(
                !should_compact_all(policy, 6, None, now),
                "非 Hybrid 策略不得自动 CompactAll(无历史也不行)"
            );
            assert!(
                !should_compact_all(policy, 6, Some(now - 100 * DAY), now),
                "非 Hybrid 策略永远为 false"
            );
        }

        assert!(
            should_compact_all(CompactionPolicyKind::Hybrid, 6, None, now),
            "Hybrid 无历史 ⇒ 首跑触发 CompactAll"
        );
        assert!(
            !should_compact_all(CompactionPolicyKind::Hybrid, 6, Some(now - 5 * DAY), now),
            "5 天 < 6 天 ⇒ 不触发"
        );
        assert!(
            should_compact_all(CompactionPolicyKind::Hybrid, 6, Some(now - 6 * DAY), now),
            "恰满 6 天 ⇒ 触发(≥ 语义)"
        );
        assert!(
            should_compact_all(CompactionPolicyKind::Hybrid, 6, Some(now - 10 * DAY), now),
            "超过 6 天 ⇒ 触发"
        );
        assert!(
            !should_compact_all(CompactionPolicyKind::Hybrid, 6, Some(now + DAY), now),
            "时间戳在未来(时钟回拨)⇒ 不得触发(饱和减法不 panic)"
        );
    }
}
