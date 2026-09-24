//! 主动磁盘健康探测(照搬 Ambry `DiskManager$DiskHealthCheck`):周期 write→read→verify→delete,
//! 带超时抓盘 hang。**仅上报观测**(S2 不据此自动下线;放置避让留后续)。默认关(对齐 Ambry)。
//!
//! **每盘至多一个在飞探测**(盘 hang 隔离):`tokio::time::timeout` 只能放弃**等待**,卡在盘里的
//! 那个阻塞线程收不回来(D 态不可取消、`spawn_blocking` 不可取消)。若下一轮照常再发,一块坏盘
//! 每轮漏一个线程,默认 60s 一轮下 ~8.5 小时就能把**进程共享**的阻塞池(默认 512)吃光 —— 与业务
//! 流量无关,是探测自己在漏,而池一满则全体盘(含健康盘)的请求一起排队。故立 `StuckProbe` 槽位:
//! 上一发没回来就**跳过本轮**(状态保持上一轮的坏值,绝不因"没探到"而报 `HEALTHY`);它回来了则
//! **取它的结果**作为本轮观测 —— 那是一次迟到的、真实发生的回答。
//!
//! 线程永不回来时,进程内无从得知盘况(任何新探测同样会卡死在同一设备队列里),那属于内核
//! fail-fast / 熔断 / 重启的范畴,不在本文件解决。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::disk::Disk;
use crate::disk_health::{observe_disk, Hysteresis};
use crate::metrics::StoreServerMetrics;

/// 磁盘健康状态(对齐 Ambry `DiskHealthStatus`)。
///
/// [`DiskHealth::IoTimeout`](Self::IoTimeout) 是 antfs 自己的扩展(Ambry 无对应项):它来自**看门狗**
/// (`disk_health.rs`),判据是"一次已在盘里的 IO 超过 `disk_watchdog_ms` 未归",而不是某一轮探测的
/// 具体步骤 —— 故不并入 `WriteTimeout`/`ReadTimeout`(那会谎报"我们在写/读",实际操作的可能是
/// flush、compaction 或 stat)。它同样只是一个**原始观测**,是否摘盘仍由迟滞决定。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskHealth {
    Healthy,
    MountNotAccessible,
    WriteTimeout,
    WriteError,
    ReadTimeout,
    ReadError,
    ReadMismatch,
    DeleteError,
    /// 一次**已经进入盘里**的 IO 超过看门狗 deadline 未归(与探测步骤无关的通用超期)。
    IoTimeout,
}

impl DiskHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            DiskHealth::Healthy => "HEALTHY",
            DiskHealth::MountNotAccessible => "MOUNT_NOT_ACCESSIBLE",
            DiskHealth::WriteTimeout => "WRITE_TIMEOUT",
            DiskHealth::WriteError => "WRITE_ERROR",
            DiskHealth::ReadTimeout => "READ_TIMEOUT",
            DiskHealth::ReadError => "READ_ERROR",
            DiskHealth::ReadMismatch => "READ_MISMATCH",
            DiskHealth::DeleteError => "DELETE_ERROR",
            DiskHealth::IoTimeout => "IO_TIMEOUT",
        }
    }
}

/// 共享的 `disk_id -> 判定状态`(prober / 看门狗**写**,数据面健康闸门 / 心跳**读**)。
///
/// 注意值不是"一个枚举"而是 [`crate::disk_health::DiskStatus`]:它同时带**原始轮次判定**与
/// **迟滞后的可服务性**。这个区分是 D2 的全部要害 —— 单轮判定直接拿来摘盘 = 一次抽风就踢掉一块好盘。
pub use crate::disk_health::DiskHealthMap as HealthMap;

/// 探测进度。探测任务每进入一步写一次,超时后据此判定"卡在哪一步"(单发探测只有一发句柄,
/// 没有它就只能报一个笼统的超时)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Exists = 0,
    Write = 1,
    Read = 2,
    Delete = 3,
}

impl Step {
    /// 卡在该步 → 按对应的**超时**类型上报。
    fn timeout_verdict(self) -> DiskHealth {
        match self {
            // 元数据调用(stat)都卡住了 = 挂载点不可访问;`DiskHealth` 不新增变体,沿用既有语义。
            Step::Exists => DiskHealth::MountNotAccessible,
            Step::Write => DiskHealth::WriteTimeout,
            Step::Read => DiskHealth::ReadTimeout,
            // 无 `DELETE_TIMEOUT` 变体:删除在时限内没完成,记删除失败。
            Step::Delete => DiskHealth::DeleteError,
        }
    }

    /// 探测任务异常结束(panic)→ 按对应的**错误**类型上报。
    fn error_verdict(self) -> DiskHealth {
        match self {
            Step::Exists => DiskHealth::MountNotAccessible,
            Step::Write => DiskHealth::WriteError,
            Step::Read => DiskHealth::ReadError,
            Step::Delete => DiskHealth::DeleteError,
        }
    }

    fn from_u8(v: u8) -> Step {
        match v {
            1 => Step::Write,
            2 => Step::Read,
            3 => Step::Delete,
            _ => Step::Exists,
        }
    }
}

/// 步骤槽位(探测任务写、超时判定读)。`AtomicU8` 而非 `Cell`:任务在别的线程上写。
type StepSlot = Arc<AtomicU8>;

fn set_step(slot: &StepSlot, step: Step) {
    slot.store(step as u8, Ordering::Relaxed);
}

fn current_step(slot: &StepSlot) -> Step {
    Step::from_u8(slot.load(Ordering::Relaxed))
}

/// 同步 5 步探测。**不设内部超时** —— 超时一律由异步侧加(这样卡住的线程本身就是一次未完成的
/// 观测,见模块注释)。删除也并进本任务:坏盘上 `unlink` 同样可能挂住,原先它在超时之外裸跑。
fn probe_disk_sync(dir: &Path, step: &StepSlot) -> DiskHealth {
    set_step(step, Step::Exists);
    if !dir.exists() {
        return DiskHealth::MountNotAccessible;
    }
    let file = dir.join(".ambry_healthcheck");
    let payload = format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    set_step(step, Step::Write);
    if std::fs::write(&file, payload.as_bytes()).is_err() {
        return DiskHealth::WriteError;
    }

    set_step(step, Step::Read);
    let content = match std::fs::read(&file) {
        Ok(c) => c,
        Err(_) => return DiskHealth::ReadError,
    };
    if content != payload.as_bytes() {
        return DiskHealth::ReadMismatch;
    }

    set_step(step, Step::Delete);
    match std::fs::remove_file(&file) {
        Ok(()) => DiskHealth::Healthy,
        Err(_) if !file.exists() => DiskHealth::Healthy,
        Err(_) => DiskHealth::DeleteError,
    }
}

/// 一发**未完成**的探测:线程还卡在盘里。
struct StuckProbe {
    /// 超时时刻的判定(按卡住的步骤)。任务永不回来时,这就是该盘最终上报的状态。
    verdict: DiskHealth,
    /// 那个还在盘里的探测任务。**必须留着** —— 丢了它就又漏一个池线程,且再也收不到它的回答。
    handle: JoinHandle<DiskHealth>,
}

/// 一轮探测的结果。
enum ProbeOutcome {
    Done(DiskHealth),
    Stuck(StuckProbe),
}

/// 带超时地跑一发探测任务。
///
/// 超时对象借的是 `&mut handle` 而不是 `handle`:`timeout` 会**消费**传入的 future,而超时之后我们
/// 恰恰要把这个句柄留给下一轮(收尸/取迟到结果)。`JoinHandle` 是 `Unpin` 的,`&mut JoinHandle`
/// 自身也实现 `Future`,故借用即可。
async fn probe_once<F>(task: F, step: StepSlot, timeout: Duration) -> ProbeOutcome
where
    F: FnOnce() -> DiskHealth + Send + 'static,
{
    let mut handle = tokio::task::spawn_blocking(task);
    match tokio::time::timeout(timeout, &mut handle).await {
        Ok(Ok(h)) => ProbeOutcome::Done(h),
        // 探测任务 panic(同步探测里没有 `unwrap`,实际不可达):按当下步骤记一次错误。
        Ok(Err(_)) => ProbeOutcome::Done(current_step(&step).error_verdict()),
        Err(_) => ProbeOutcome::Stuck(StuckProbe {
            verdict: current_step(&step).timeout_verdict(),
            handle,
        }),
    }
}

/// 对 `dir` 跑一发带超时的探测(测试与一次性调用用)。
///
/// 超时后**丢弃**卡住的句柄 —— 一次性调用没人再等它(线程照样收不回)。生产路径是
/// [`spawn_prober`]:它用槽位留住句柄,并且不再往同一块盘上叠加探测。
pub async fn probe_disk(dir: PathBuf, timeout: Duration) -> DiskHealth {
    match probe_dir_once(dir, timeout).await {
        ProbeOutcome::Done(h) => h,
        ProbeOutcome::Stuck(s) => {
            drop(s.handle);
            s.verdict
        }
    }
}

/// 生产路径的一发探测:带上步骤槽位,超时即按卡住的那一步判定。
async fn probe_dir_once(dir: PathBuf, timeout: Duration) -> ProbeOutcome {
    let step: StepSlot = Arc::new(AtomicU8::new(Step::Exists as u8));
    let inner = step.clone();
    probe_once(move || probe_disk_sync(&dir, &inner), step, timeout).await
}

/// 本轮对某盘探测槽位的处置(纯判定,单测直接打)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotAction {
    /// 槽位空:正常探一发。
    Probe,
    /// 上一发还在盘里:跳过本轮 —— 不往坏盘上叠加探测,也就不会再漏池线程。
    Skip,
    /// 上一发回来了:本轮取它的结果(不再新发探测)。
    Adopt,
}

fn slot_action(slot: Option<&StuckProbe>) -> SlotAction {
    match slot {
        None => SlotAction::Probe,
        Some(s) if s.handle.is_finished() => SlotAction::Adopt,
        Some(_) => SlotAction::Skip,
    }
}

/// 每盘一个槽位:上一轮没回来的那发探测(进程内内存态)。
type ProbeSlots = HashMap<String, StuckProbe>;

/// 后台周期探测所有盘,结果写入共享 `HealthMap`。
///
/// 每盘至多一个在飞探测(见模块注释):跳过的轮次数落在 `disk_probe_stuck_rounds` 上;**跳过期间
/// 不写 `HealthMap`** —— 该盘保持上一轮的坏值,绝不因"这一轮没探到"而回到 `HEALTHY`。
///
/// 探到的结论不直接写死,而是经 [`observe_disk`] 喂给迟滞状态机(D2):单轮坏只是**一次坏观测**,
/// 是否摘盘由 `disk_health_bad_rounds` / `disk_health_bad_secs` 决定。
pub fn spawn_prober(
    disks: Vec<Disk>,
    interval: Duration,
    timeout: Duration,
    out: HealthMap,
    hysteresis: Hysteresis,
    metrics: Arc<StoreServerMetrics>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        let mut slots: ProbeSlots = HashMap::new();
        loop {
            tick.tick().await;
            probe_round(&disks, timeout, &out, &mut slots, &hysteresis, &metrics).await;
        }
    });
}

/// 走**一轮**探测(所有盘)。
///
/// 抽成独立函数是为了让"跳过轮**不覆盖**健康值"这条规则可测 —— `spawn_prober` 是常驻循环,带真定时器,
/// 没法直接断言;而这条规则恰恰是"未知 ≠ 健康"的落地点(CLAUDE.md §3.1:宁可保守也别谎报健康)。
async fn probe_round(
    disks: &[Disk],
    timeout: Duration,
    out: &HealthMap,
    slots: &mut ProbeSlots,
    hysteresis: &Hysteresis,
    metrics: &StoreServerMetrics,
) {
    for d in disks {
        match slot_action(slots.get(&d.id)) {
            SlotAction::Skip => {
                // 上一发还卡在盘里:本轮**不写 `HealthMap`**(保持上一轮的坏值,绝不因"没探到"
                // 而回到 HEALTHY),只把"卡住/跳过"如实打点。
                metrics.disk_probe_stuck.with_label_values(&[&d.id]).set(1);
                metrics.disk_probe_skipped.with_label_values(&[&d.id]).inc();
                continue;
            }
            SlotAction::Adopt => {
                let s = match slots.remove(&d.id) {
                    Some(s) => s,
                    None => continue,
                };
                // `is_finished()` 已为真,`await` 立即返回;join 失败只可能是 panic。
                let verdict = match s.handle.await {
                    Ok(h) => h,
                    Err(_) => s.verdict,
                };
                tracing::warn!(
                    disk = %d.id,
                    health = verdict.as_str(),
                    "stuck disk probe finally returned; adopting its late verdict"
                );
                metrics.disk_probe_stuck.with_label_values(&[&d.id]).set(0);
                observe_disk(out, metrics, &d.id, verdict, hysteresis);
                continue;
            }
            SlotAction::Probe => {}
        }
        match probe_dir_once(d.dir.clone(), timeout).await {
            ProbeOutcome::Done(h) => {
                observe_disk(out, metrics, &d.id, h, hysteresis);
            }
            ProbeOutcome::Stuck(s) => {
                // 先按超时类型上报(本轮唯一可得的结论),再把句柄收进槽位:**不再重探该盘**。
                tracing::warn!(
                    disk = %d.id,
                    health = s.verdict.as_str(),
                    timeout_secs = timeout.as_secs(),
                    "disk probe timed out; holding the stuck probe and not re-probing this disk"
                );
                metrics.disk_probe_stuck.with_label_values(&[&d.id]).set(1);
                observe_disk(out, metrics, &d.id, s.verdict, hysteresis);
                slots.insert(d.id.clone(), s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[tokio::test]
    async fn healthy_disk_probes_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let h = probe_disk(tmp.path().to_path_buf(), Duration::from_secs(5)).await;
        assert_eq!(h, DiskHealth::Healthy);
    }

    #[tokio::test]
    async fn missing_mount_is_not_accessible() {
        let missing = std::path::PathBuf::from("/nonexistent/ambry/disk/xyz");
        let h = probe_disk(missing, Duration::from_secs(5)).await;
        assert_eq!(h, DiskHealth::MountNotAccessible);
    }

    /// 卡住的探测:超时即按卡住的步骤判类型,但**句柄被留住**;它回来之后,那次迟到的结果可被采纳。
    ///
    /// 用法:零超时 —— `timeout` 先 poll 内层 future(必然未完成)再判超时,故这里必定 `Stuck`,
    /// 不需要 `sleep`(确定性等待由 channel 控制,符合 CLAUDE.md §6)。
    #[tokio::test]
    async fn stuck_probe_is_held_and_its_late_verdict_is_adopted() {
        let (tx, rx) = mpsc::channel::<()>();
        let step: StepSlot = Arc::new(AtomicU8::new(Step::Write as u8));
        let task = move || {
            let _ = rx.recv();
            DiskHealth::Healthy
        };

        let stuck = match probe_once(task, step, Duration::ZERO).await {
            ProbeOutcome::Stuck(s) => s,
            ProbeOutcome::Done(h) => panic!("零超时必定 Stuck,实际 Done({h:?})"),
        };
        assert_eq!(stuck.verdict, DiskHealth::WriteTimeout, "卡在写步骤");
        assert_eq!(
            slot_action(Some(&stuck)),
            SlotAction::Skip,
            "未回来 → 本轮跳过,不叠加探测"
        );
        assert_eq!(slot_action(None), SlotAction::Probe, "空槽 → 正常探");

        // 放行那一发:它回来 = 一次迟到的真实回答(这里盘已恢复)。
        tx.send(()).unwrap();
        let late = stuck.handle.await.unwrap();
        assert_eq!(late, DiskHealth::Healthy);
    }

    /// 一轮探测:每块盘都被探到并写入观测(好盘 HEALTHY、不存在的挂载点 MOUNT_NOT_ACCESSIBLE)。
    /// **单轮坏不等于摘盘** —— 可服务性由迟滞决定(D2),这里只断言"原始判定被如实记下"。
    #[tokio::test]
    async fn probe_round_covers_every_disk() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        let disks = vec![disk(&tmp, "d0"), disk_missing("d1")];
        let out: HealthMap = crate::disk_health::new_map();
        let mut slots: ProbeSlots = HashMap::new();

        probe_round(
            &disks,
            Duration::from_secs(5),
            &out,
            &mut slots,
            &hyst(),
            &metrics,
        )
        .await;

        let map = out.read().expect("health map poisoned");
        assert_eq!(
            map.get("d0").and_then(|s| s.verdict()),
            Some(DiskHealth::Healthy),
            "好盘应报 HEALTHY"
        );
        assert_eq!(
            map.get("d1").and_then(|s| s.verdict()),
            Some(DiskHealth::MountNotAccessible),
            "不存在的挂载点应报 MOUNT_NOT_ACCESSIBLE"
        );
        assert!(
            map.get("d1").is_some_and(|s| s.is_available()),
            "单轮坏 → 原始判定变坏,但闸门仍放行(迟滞说了算)"
        );
        assert!(slots.is_empty(), "都探成功 ⇒ 不留任何在飞槽位");
    }

    /// **跳过轮不覆盖健康值**(核心规则):上一发未归的那块盘,本轮不探、重探计数 +1、
    /// `HealthMap` 保持上一轮的坏值(绝不能回到 HEALTHY),句柄继续留在槽位里。
    #[tokio::test]
    async fn skipped_round_keeps_the_previous_verdict() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        let disks = vec![disk(&tmp, "d0")];
        let out: HealthMap = crate::disk_health::new_map();
        crate::disk_health::observe_disk(&out, &metrics, "d0", DiskHealth::WriteTimeout, &hyst());

        // 造一发"卡在盘里"的探测:任务阻塞在 channel 上(由测试放行),句柄留在槽位里。
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let handle = tokio::task::spawn_blocking(move || {
            let _ = release_rx.recv();
            DiskHealth::Healthy // 迟到的"盘已恢复"
        });
        let mut slots: ProbeSlots = HashMap::new();
        slots.insert(
            "d0".to_string(),
            StuckProbe {
                verdict: DiskHealth::WriteTimeout,
                handle,
            },
        );

        probe_round(
            &disks,
            Duration::from_secs(5),
            &out,
            &mut slots,
            &hyst(),
            &metrics,
        )
        .await;

        {
            let map = out.read().expect("health map poisoned");
            assert_eq!(
                map.get("d0").map(|s| s.reported_state()),
                Some("WRITE_TIMEOUT"),
                "跳过轮绝不能把坏值刷成 HEALTHY"
            );
        }
        assert_eq!(slots.len(), 1, "未归的那发应继续留在槽位里");
        let text = antfs_metric::gather(&registry).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_probe_stuck{disk_id="d0"} 1"#),
            "卡住状态应如实上报:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_probe_skipped_total{disk_id="d0"} 1"#),
            "跳过轮次应计数:\n{text}"
        );

        // 放行那发(迟到的 HEALTHY 会在**下一轮**被采纳,见下一条用例);这里只清理任务。
        release_tx.send(()).unwrap();
        let late = slots.remove("d0").expect("槽位还在").handle;
        assert_eq!(late.await.expect("join"), DiskHealth::Healthy);
    }

    /// 迟到的探测结果在**下一轮**被采纳,并清掉"卡住"gauge(慢盘恢复的正常路径)。
    #[tokio::test]
    async fn late_verdict_is_adopted_on_the_next_round() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        let disks = vec![disk(&tmp, "d0")];
        let out: HealthMap = crate::disk_health::new_map();
        crate::disk_health::observe_disk(&out, &metrics, "d0", DiskHealth::WriteTimeout, &hyst());

        // 一发**已完成**的探测:句柄返回 ReadMismatch(模拟"迟到且如实"的回答)。
        //
        // 注意:这里**只能看 `is_finished()`**,不能自己 `await` 它 —— 句柄被 poll 过之后再被轮次里的
        // `handle.await` 碰一次,就会 panic "JoinHandle polled after completion"(实测踩到)。
        let handle = tokio::task::spawn_blocking(|| DiskHealth::ReadMismatch);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !handle.is_finished() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert!(handle.is_finished(), "这一发应当已经完成");
        let mut slots: ProbeSlots = HashMap::new();
        slots.insert(
            "d0".to_string(),
            StuckProbe {
                verdict: DiskHealth::WriteTimeout,
                handle,
            },
        );
        metrics.disk_probe_stuck.with_label_values(&["d0"]).set(1);

        probe_round(
            &disks,
            Duration::from_secs(5),
            &out,
            &mut slots,
            &hyst(),
            &metrics,
        )
        .await;

        let map = out.read().expect("health map poisoned");
        assert_eq!(
            map.get("d0").and_then(|s| s.verdict()),
            Some(DiskHealth::ReadMismatch),
            "迟到的结果应被采纳为该盘本轮的观测"
        );
        drop(map);
        assert!(slots.is_empty(), "采纳后槽位应清空");
        let text = antfs_metric::gather(&registry).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_probe_stuck{disk_id="d0"} 0"#),
            "采纳后'卡住'应归 0:\n{text}"
        );
    }

    /// 探测/看门狗测试用的迟滞参数:与配置默认值同形(**3 轮坏 / 30s / 3 轮好 / 120s**)。
    /// 取默认值而不是随手捏一个,是为了让"默认配置下会发生什么"能被单测钉住。
    fn hyst() -> Hysteresis {
        Hysteresis {
            bad_rounds: 3,
            bad_secs: 30,
            good_rounds: 3,
            recover_stable_secs: 120,
        }
    }

    /// 造一块临时目录上的盘。
    fn disk(tmp: &tempfile::TempDir, id: &str) -> Disk {
        Disk {
            id: id.to_string(),
            dir: tmp.path().to_path_buf(),
            capacity_bytes: 1,
            free_bytes: 1,
        }
    }

    /// 造一块"挂载点不存在"的盘。
    fn disk_missing(id: &str) -> Disk {
        Disk {
            id: id.to_string(),
            dir: std::path::PathBuf::from("/nonexistent/ambry/disk/xyz"),
            capacity_bytes: 1,
            free_bytes: 1,
        }
    }

    /// `slot_action` 的 `Adopt` 分支:句柄完成后应转为"取迟到结果",而非继续跳过。
    #[tokio::test]
    async fn finished_slot_is_adopted() {
        let handle = tokio::task::spawn_blocking(|| DiskHealth::ReadError);
        // 有界等待它完成;**不 poll 句柄本身**(poll 过之后调用方就再也不能 await 它了)。
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !handle.is_finished() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert!(handle.is_finished(), "任务应已完成");
        let slot = StuckProbe {
            verdict: DiskHealth::ReadTimeout,
            handle,
        };
        assert_eq!(slot_action(Some(&slot)), SlotAction::Adopt);
    }
}
