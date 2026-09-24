//! 每盘**可服务性**(健康闸门,design D2)+ **IO 看门狗**(design D3),以及二者共享的进程内状态。
//!
//! 承 [`openspec/changes/store-disk-isolation/design.md`](../../../openspec/changes/store-disk-isolation/design.md)
//! §4/§5。`health.rs` 回答"这一轮探一块盘,探出了什么"(**原始轮次判定**);本模块回答
//! "这些判定合起来意味着这块盘现在**能不能服务**"(迟滞后的**派生态**),外加
//! "一次已经进到盘里的 IO 多久没回来了"(看门狗)。
//!
//! ## 为什么必须分两层
//!
//! 单轮判定直接拿来摘盘 = "偶尔一次抽风就踢掉一块好盘",而盘越忙越容易闪一下。所以:
//!
//! - **原始轮次判定** = [`DiskStatus::verdict`],最近一次观测的结论;
//! - **可服务性** = [`DiskStatus::is_available`],迟滞之后派生的布尔值 —— **闸门只用它**。
//!
//! ## 两条硬规则
//!
//! 1. **从未观测过绝不报 `HEALTHY`**:报 [`STATE_UNKNOWN`]。"没检查过"与"检查过且健康"是两件事,
//!    混起来的后果是"从没看过一眼的盘"在 master 侧被当成健康盘接流量。默认关探测的部署里每块盘
//!    都是这个值 —— 因此 master 侧**必须**把 `UNKNOWN` 当可用(`placement.rs`),否则默认部署
//!    一块盘都放不了。
//! 2. **宁可迟,不可早**:所有阈值都偏向"再等一轮"。一次不必要的搬迁 = 真金白银的 GB 级网络 +
//!    盘写,还可能让新副本与源副本一起进入追平;多留一份健康副本几乎免费。
//!
//! ## 看门狗为什么不受 `disk_health_enabled` 约束
//!
//! 主动探测可以关(对齐 Ambry,而且**默认就是关**),但看门狗不能跟着一起关:默认部署才是绝大多数,
//! 若它也被关,`UNAVAILABLE` 就再没有第二个来源,"坏盘被隔离 + 从坏盘撤离"整条链路在默认配置下全哑。
//!
//! ## 诚实边界(别假装做到了)
//!
//! 已经卡在 syscall 里的那一次 IO **取消不掉**(Rust 既不能取消 blocking 任务,也不能 kill 线程)。
//! 看门狗把"泄漏 + 无人知晓"变成"泄漏 + 已知 + 有界决策",它**不解决泄漏**;进程内也没有任何回收
//! D 态线程的手段。故"让在途请求快速失败"只对**排队/后续**请求成立。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::health::DiskHealth;
use crate::metrics::StoreServerMetrics;

/// 上报值:健康(迟滞后的可服务态 + 最近一轮也健康)。
pub const STATE_HEALTHY: &str = "HEALTHY";
/// 上报值:迟滞判定后的**隔离态**(闸门据此 503)。
pub const STATE_UNAVAILABLE: &str = "UNAVAILABLE";
/// 上报值:**延迟劣化**(慢但可用)。
///
/// 语义:该盘**仍在服务**(不 503,闸门放行),但它的实时慢 IO 占比已连续多轮超过阈值 ——
/// master 据此**不再往它放新副本**(它 != `HEALTHY`/`UNKNOWN`/空串,天然被排除),也**不**触发撤离
/// (撤离只认 `UNAVAILABLE`,避免"有点慢就搬 GB 数据"的churn)。
///
/// 为什么需要这一档:只有"可用/不可用"两档时,一块 p99 从 1ms 涨到 2s 的盘**既不会被隔离、
/// 也没人会停止往它放副本** —— 它就这么一直烂在那儿(离看门狗 deadline 还差得远)。
pub const STATE_DEGRADED: &str = "DEGRADED";
/// 上报值:**从未有过**有效观测(刚启动 / 探测未启用——不知 ≠ 健康)。
pub const STATE_UNKNOWN: &str = "UNKNOWN";

/// 上报值域全集:仪表盘要把每块盘这几条 series **都摆平**(只置 1 不置 0 会让旧状态永远留 1)。
///
/// 与 `master` 侧的判定配合:可放新副本 = `""` | `HEALTHY` | `UNKNOWN`(见
/// [`crate::disk_health::STATE_UNKNOWN`] 的说明与 `crates/master/src/placement.rs` 的单测)。
pub const REPORTED_STATES: [&str; 12] = [
    STATE_HEALTHY,
    "MOUNT_NOT_ACCESSIBLE",
    "WRITE_TIMEOUT",
    "WRITE_ERROR",
    "READ_TIMEOUT",
    "READ_ERROR",
    "READ_MISMATCH",
    "DELETE_ERROR",
    "IO_TIMEOUT",
    STATE_DEGRADED,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
];

/// 迟滞参数(来自 `store_server` 配置;`Hysteresis::from_config`)。
///
/// 两个方向都给了"轮数"与"时长"两条判据,取**先到者**:
/// - 只按轮数:探测周期调大到 300s 时,"连续 3 轮"就是 15 分钟,一块真死的盘要 15 分钟才被隔离;
/// - 只按时长:周期调小(比如 5s)时,"持续 30s"等于 6 轮都不防抖。
///
/// 两个维度都卡住才两头不漏。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hysteresis {
    /// 连续坏 N 轮 ⇒ 置 `UNAVAILABLE`。
    pub bad_rounds: u32,
    /// 坏态持续 ≥ T 秒 ⇒ 也置 `UNAVAILABLE`(与上者取先到)。
    pub bad_secs: u64,
    /// 复位要求**连续好 M 轮**。
    pub good_rounds: u32,
    /// 复位还要求"稳定满 T2 秒"(从进入连续好态算起)。
    pub recover_stable_secs: u64,
}

/// 一次观测引起的状态迁移(调用方据此打 `disk_health_transitions_total` 与日志)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// 可服务性未变(仍可用,或仍不可用)。
    None,
    /// 可用 → 不可用(迟滞判据满足)。
    BecameUnavailable,
    /// 不可用 → 可用(连续好 + 稳定达标)。
    BecameAvailable,
}

/// **一台**迟滞状态机:进入"坏"需要连续 `bad_rounds` 轮或持续 `bad_secs` 秒(先到者);
/// 退出"坏"需要连续 `good_rounds` 轮**且**稳定满 `recover_stable_secs` 秒。
///
/// 抽成独立类型是因为它现在被**两台机器**共用:
/// - **可服务性**(探测/看门狗给出的"盘坏没坏")→ 决定 `503`;
/// - **延迟劣化**(实时慢 IO 占比超阈值)→ 决定 `DEGRADED`(不 503,但 master 不再往它放新副本)。
///
/// 两台机器共享同一套已经测透的迟滞逻辑(含"计数不跨坏态片段泄漏"、"长期坏不翻转"这些坑),
/// 而不是把那段逻辑抄第二遍。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Latch {
    /// 是否处于"坏"(初始 `false` = 无证据 ⇒ 不算坏)。
    bad: bool,
    /// 当前"连续坏"的轮数(好轮即归零)。
    bad_streak: u32,
    /// 当前"连续好"的轮数(坏轮即归零;`bad == false` 时恒为 0)。
    good_streak: u32,
    /// 本轮连续坏的**起始时刻**(好轮即清空)—— `bad_secs` 的计时起点。
    bad_since: Option<Instant>,
    /// 本轮连续好的**起始时刻**(坏轮即清空)—— `recover_stable_secs` 的计时起点。
    good_since: Option<Instant>,
}

impl Latch {
    fn healthy() -> Self {
        Latch {
            bad: false,
            bad_streak: 0,
            good_streak: 0,
            bad_since: None,
            good_since: None,
        }
    }

    fn is_bad(&self) -> bool {
        self.bad
    }

    /// 喂一次"这一轮算不算坏",推进状态机。`now` 显式传入(单测注入合成时刻,免 sleep)。
    fn observe(&mut self, now: Instant, is_bad: bool, h: &Hysteresis) -> Transition {
        if !is_bad {
            // 好轮:连续坏清零(否则"连续 N 轮坏"名不副实)。
            self.bad_streak = 0;
            self.bad_since = None;
            if !self.bad {
                // 已经好:连续好计数无意义,保持 0(避免无界增长与陈旧起点)。
                self.good_streak = 0;
                self.good_since = None;
                return Transition::None;
            }
            self.good_streak += 1;
            let since = *self.good_since.get_or_insert(now);
            let stable =
                now.saturating_duration_since(since) >= Duration::from_secs(h.recover_stable_secs);
            if self.good_streak >= h.good_rounds && stable {
                self.bad = false;
                self.good_streak = 0;
                self.good_since = None;
                return Transition::BecameAvailable;
            }
            return Transition::None;
        }
        // 坏轮:连续好清零(复位必须"连续 M 轮好",不能被中间一个坏轮糊过去)。
        self.good_streak = 0;
        self.good_since = None;
        if self.bad {
            // 已经在坏态:保持,不再产迁移(重复 503/重复统计是既定行为,不是新事件)。
            return Transition::None;
        }
        self.bad_streak += 1;
        let since = *self.bad_since.get_or_insert(now);
        let over_time = now.saturating_duration_since(since) >= Duration::from_secs(h.bad_secs);
        if self.bad_streak >= h.bad_rounds || over_time {
            self.bad = true;
            // 本段坏态收尾:清零,免得复位后**第一个**坏轮就凭旧计数再次触发(那样迟滞等于没有)。
            self.bad_streak = 0;
            self.bad_since = None;
            return Transition::BecameUnavailable;
        }
        Transition::None
    }
}

/// 一块盘的判定状态:**原始轮次判定** + **迟滞后的可服务性** + **迟滞后的延迟劣化**(纯内存,不持久化)。
///
/// `Instant` 只在进程内有意义 —— 迟滞本来就是"这一进程最近看到的样子",重启后从
/// [`DiskStatus::never_observed`] 重新开始(`UNKNOWN` ⇒ 可用)是**刻意**的:重启后没有任何观测
/// 证据说它坏,把整节点盘锁死才是灾难。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskStatus {
    /// 最近一次**原始轮次判定**;`None` = 从未观测过。
    verdict: Option<DiskHealth>,
    /// 可服务性迟滞机(闸门唯一判据):坏 ⇒ `503`。
    availability: Latch,
    /// **延迟劣化**迟滞机(DEGRADED):坏 ⇒ 不 `503`,但对外报 `DEGRADED`(master 据此不再放新副本)。
    latency: Latch,
}

impl DiskStatus {
    /// 从未观测过:报 `UNKNOWN`,**可服务**(默认关探测的部署行为不变)。
    pub fn never_observed() -> Self {
        DiskStatus {
            verdict: None,
            availability: Latch::healthy(),
            latency: Latch::healthy(),
        }
    }

    /// 闸门判据:能不能服务。**`DEGRADED` 不影响这一条** —— 慢盘仍然服务,只是不再接新副本。
    pub fn is_available(&self) -> bool {
        !self.availability.is_bad()
    }

    /// 是否处于**延迟劣化**态(慢但可用)。仅作上报/放置参考,绝不用于 `503`。
    pub fn is_degraded(&self) -> bool {
        self.is_available() && self.latency.is_bad()
    }

    /// 最近一次原始轮次判定(自省/日志用;闸门**不用**它)。
    pub fn verdict(&self) -> Option<DiskHealth> {
        self.verdict
    }

    /// 当前应该**上报/展示**的状态串(这就是 master 心跳里看到的那个值)。
    ///
    /// 顺序很重要:`UNAVAILABLE` 是派生态,一旦隔离就压过一切 —— 运维看板要看的头号信息是
    /// "这块盘现在被摘了",而不是"它当时是写超时还是读超时"(后者在日志里)。
    /// 其次是 `DEGRADED`:它同样压过具体故障枚举,因为它才是"要不要继续往这块盘放副本"的答案。
    pub fn reported_state(&self) -> &'static str {
        if !self.is_available() {
            STATE_UNAVAILABLE
        } else if self.is_degraded() {
            STATE_DEGRADED
        } else {
            match self.verdict {
                None => STATE_UNKNOWN,
                Some(v) => v.as_str(),
            }
        }
    }

    /// 喂一次**健康观测**(探测/看门狗),推进可服务性迟滞机。`now` 显式传入(单测注入,免 sleep)。
    pub fn observe(&mut self, now: Instant, verdict: DiskHealth, h: &Hysteresis) -> Transition {
        self.verdict = Some(verdict);
        self.availability
            .observe(now, verdict != DiskHealth::Healthy, h)
    }

    /// 喂一次**延迟观测**:这一轮该盘的实时慢 IO 占比是否超阈值。
    ///
    /// 与 [`Self::observe`] 是**两台独立的机器**:慢盘的慢不该把它判成"坏"(那是 `503`),
    /// 而"坏"也不代表慢(`DEGRADED` 与 `UNAVAILABLE` 是两件事)。
    pub fn observe_latency(&mut self, now: Instant, violated: bool, h: &Hysteresis) -> Transition {
        self.latency.observe(now, violated, h)
    }
}

/// 共享的 `disk_id -> DiskStatus`(prober / 看门狗写,数据面闸门 / 心跳读)。
pub type DiskHealthMap = Arc<RwLock<HashMap<String, DiskStatus>>>;

/// 建一张空表(空表里任何盘都"从未观测 ⇒ 可用")。
pub fn new_map() -> DiskHealthMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// 读一条(锁中毒也恢复着用:表内容永远结构有效,而在数据面热路径上 panic 等于自断服务)。
fn read<'a>(
    map: &'a DiskHealthMap,
    disk_id: &str,
) -> std::sync::RwLockReadGuard<'a, HashMap<String, DiskStatus>> {
    let _ = disk_id;
    map.read().unwrap_or_else(|e| e.into_inner())
}

/// 该盘当前是否可服务(数据面闸门判据)。**表里没有该盘 = 从未观测 = 可服务**。
pub fn is_available(map: &DiskHealthMap, disk_id: &str) -> bool {
    read(map, disk_id)
        .get(disk_id)
        .map(DiskStatus::is_available)
        .unwrap_or(true)
}

/// 该盘当前是否**延迟劣化**(慢但可用)。表里没有该盘 → `false`(从未观测 ⇒ 不算劣化)。
pub fn is_degraded(map: &DiskHealthMap, disk_id: &str) -> bool {
    read(map, disk_id)
        .get(disk_id)
        .map(DiskStatus::is_degraded)
        .unwrap_or(false)
}

/// 该盘当前的上报状态串(心跳用;表里没有 → `UNKNOWN`)。
pub fn reported_state(map: &DiskHealthMap, disk_id: &str) -> &'static str {
    read(map, disk_id)
        .get(disk_id)
        .map(DiskStatus::reported_state)
        .unwrap_or(STATE_UNKNOWN)
}

/// 喂一次观测并打全副作用(状态 gauge 摆平 + 迁移计数 + 迁移日志)。
pub fn observe_disk(
    map: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    disk_id: &str,
    verdict: DiskHealth,
    h: &Hysteresis,
) -> Transition {
    observe_disk_at(map, metrics, disk_id, verdict, h, Instant::now())
}

/// 同 [`observe_disk`],但显式给时刻(单测注入,免 sleep)。
pub fn observe_disk_at(
    map: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    disk_id: &str,
    verdict: DiskHealth,
    h: &Hysteresis,
    now: Instant,
) -> Transition {
    let (transition, from, to) = {
        let mut guard = map.write().unwrap_or_else(|e| e.into_inner());
        let status = guard
            .entry(disk_id.to_string())
            .or_insert_with(DiskStatus::never_observed);
        let from = status.reported_state();
        let t = status.observe(now, verdict, h);
        (t, from, status.reported_state())
    };
    set_state_gauge(metrics, disk_id, to);
    if let Transition::BecameUnavailable | Transition::BecameAvailable = transition {
        metrics
            .disk_health_transitions
            .with_label_values(&[disk_id, from, to])
            .inc();
        tracing::warn!(
            disk = %disk_id,
            from = from,
            to = to,
            verdict = verdict.as_str(),
            "disk availability changed"
        );
    }
    transition
}

/// 喂一次**延迟观测**并打全副作用(状态 gauge 摆平 + 迁移计数 + 迁移日志)—— 与 [`observe_disk`] 对称。
///
/// 只影响上报值(可能变 `DEGRADED`),**不影响可服务性**:慢盘照常服务,只是不再接新副本。
pub fn observe_latency(
    map: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    disk_id: &str,
    violated: bool,
    h: &Hysteresis,
) -> Transition {
    observe_latency_at(map, metrics, disk_id, violated, h, Instant::now())
}

/// 同 [`observe_latency`],但显式给时刻(单测注入,免 sleep)。
pub fn observe_latency_at(
    map: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    disk_id: &str,
    violated: bool,
    h: &Hysteresis,
    now: Instant,
) -> Transition {
    let (transition, from, to) = {
        let mut guard = map.write().unwrap_or_else(|e| e.into_inner());
        let status = guard
            .entry(disk_id.to_string())
            .or_insert_with(DiskStatus::never_observed);
        let from = status.reported_state();
        let t = status.observe_latency(now, violated, h);
        (t, from, status.reported_state())
    };
    set_state_gauge(metrics, disk_id, to);
    if let Transition::BecameUnavailable | Transition::BecameAvailable = transition {
        metrics
            .disk_health_transitions
            .with_label_values(&[disk_id, from, to])
            .inc();
        tracing::warn!(
            disk = %disk_id,
            from = from,
            to = to,
            violating = violated,
            "disk latency state changed"
        );
    }
    transition
}

/// 把该盘的 `disk_health_state` family **整体摆平**:当前值置 1、其余置 0。
///
/// 只置 1 不置 0 是这类"状态枚举 gauge"最常见的坑 —— 状态迁移后旧值永远留着 1,看板上出现
/// "一块盘同时健康又坏掉"。
pub fn set_state_gauge(metrics: &StoreServerMetrics, disk_id: &str, state: &str) {
    for s in REPORTED_STATES {
        metrics
            .disk_health_state
            .with_label_values(&[disk_id, s])
            .set(i64::from(s == state));
    }
}

/// 启动时把每块盘先摆成 `UNKNOWN`(从未观测)。
///
/// 为什么要有这一步:否则"探测没开 / 一轮还没跑"的盘在 `/metrics` 上**一条 series 都没有**,
/// 看板显示的空白既可能是"没探过"也可能是"抓取坏了"。
pub fn init_state_gauges<'a>(
    disks: impl IntoIterator<Item = &'a str>,
    metrics: &StoreServerMetrics,
) {
    for id in disks {
        set_state_gauge(metrics, id, STATE_UNKNOWN);
    }
}

/// 看门狗一轮的产出(测试断言它;`abort()` 的**执行**留在常驻任务里,不在这个纯判定函数里)。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WatchdogRound {
    /// 本轮被记为"IO 超期"的盘(已喂进迟滞状态机)。
    pub tripped: Vec<String>,
    /// 达到逃生门槛、应当 `process::abort()` 的盘(仅 `escalate_secs > 0` 时可能非空)。
    pub escalate: Option<String>,
}

/// **延迟劣化的判定函数**:`(本轮慢 IO 数, 本轮 IO 总数) -> 该轮是否算劣化`。
///
/// `None` = **本轮不产生结论**(例如一发 IO 都没有:空闲 ≠ 慢)。口径本身留在配置侧
/// ([`crate::config::StoreServerConfig::degraded_round`]),那里有单测;看门狗只负责"读走本轮的增量计数、喂给迟滞机"。
pub type LatencyJudge = Arc<dyn Fn(u64, u64) -> Option<bool> + Send + Sync>;

/// 看门狗的一轮设置(打成结构体:参数多到一定程度,位置参数就开始互相"借位",也过不了
/// clippy 的 `too_many_arguments`)。
#[derive(Clone)]
pub struct WatchdogSettings {
    /// 默认门槛(未按 op 覆盖的 op 用它,也决定 tick 周期)。
    pub default_deadline: Duration,
    /// **按 op 覆盖**的门槛(毫秒);空 = 全部用 `default_deadline`。
    pub by_op_ms: std::collections::BTreeMap<String, u64>,
    /// 逃生门槛秒数(`0` = 永不升级)。
    pub escalate_secs: u64,
    /// DEGRADED 的判定函数;`None` = 本部署没开(也零开销:连增量计数都不读)。
    pub degraded: Option<LatencyJudge>,
}

impl WatchdogSettings {
    /// 某个 op 的门槛;tick 周期取所有门槛的**最小值**(故给某 op 设更紧的门槛不会被默认周期拖慢)。
    fn deadline_for(&self, op: &str) -> Duration {
        let ms = self
            .by_op_ms
            .get(op)
            .copied()
            .unwrap_or_else(|| self.default_deadline.as_millis().max(1) as u64);
        Duration::from_millis(ms.max(1))
    }

    fn tick(&self) -> Duration {
        self.by_op_ms
            .keys()
            .map(|op| self.deadline_for(op))
            .chain(std::iter::once(self.default_deadline))
            .min()
            .unwrap_or(self.default_deadline)
            .max(Duration::from_millis(1))
    }
}

/// 看门狗一轮的输入(打成结构体:九个参数既难读也过不了 clippy 的 `too_many_arguments`)。
pub struct WatchdogInput<'a> {
    /// 该盘**当前有在途 IO** 的 op 列表(空闲的 op 不必检查)。**按 op 分档**的入口。
    pub ops_in_flight: &'a dyn Fn(&str) -> Vec<&'static str>,
    /// 该盘某个 op 上**最久未归**的那一发开始时刻(闭包内口径,见 [`crate::disk_pool::IoWatch`])。
    pub oldest: &'a dyn Fn(&str, &str) -> Option<Instant>,
    /// 该盘**全部** op 合计的在途条数(逃生判据用:"是不是每条 worker 都被占住了")。
    pub in_flight_total: &'a dyn Fn(&str) -> u32,
    /// 该盘池的 worker 数(`in_flight_total >= workers` 才可能达到逃生门槛)。
    pub workers: &'a dyn Fn(&str) -> u32,
    /// 要检查的盘(**有序**,便于确定性遍历)。
    pub disks: &'a [String],
    /// **某个 op 的**超期门槛。
    ///
    /// 为什么按 op 分档:compaction / flush 要拷整段,天然比一次 4KiB 读慢几个数量级。
    /// 若共用一个门槛,要么门槛大到"真死的盘半天不被隔离",要么小到"健康盘的 compaction 天天误报"。
    /// 分档之后每个 op 用它自己的尺子。
    pub deadline: &'a dyn Fn(&str) -> Duration,
    /// 超期只算**一次坏观测**,是否摘盘由这台状态机定。
    pub hysteresis: &'a Hysteresis,
    /// 逃生门槛秒数(`0` = 永不升级)。
    pub escalate_secs: u64,
}

/// 跑一轮看门狗(纯判定,不 sleep)。
///
/// 判据是**闭包内**测得的 syscall 时长口径:`oldest` 是该盘当前最早一发在途 IO 的**开始时刻**
/// (见 [`crate::disk_pool::IoWatch`]),故排队时间**不计入** —— 盘正常但池忙不会把健康盘判成坏盘
/// (design §5.1 那一处最容易写错的地方)。
///
/// 超期只算**一次坏观测**,照走迟滞(`bad_rounds`/`bad_secs`)—— 单次超时不摘盘。
pub fn watchdog_round(
    statuses: &DiskHealthMap,
    metrics: &StoreServerMetrics,
    input: WatchdogInput<'_>,
) -> WatchdogRound {
    let WatchdogInput {
        ops_in_flight,
        oldest,
        in_flight_total,
        workers,
        disks,
        deadline,
        hysteresis: h,
        escalate_secs,
    } = input;
    let now = Instant::now();
    let mut round = WatchdogRound::default();
    for d in disks {
        // 逐个 op 用它**自己的**门槛检查(空闲的 op 直接跳过)。
        for op in ops_in_flight(d) {
            let Some(start) = oldest(d, op) else { continue };
            let waited = now.saturating_duration_since(start);
            let limit = deadline(op);
            if waited < limit {
                continue;
            }
            metrics
                .disk_io_watchdog_trips
                .with_label_values(&[d.as_str()])
                .inc();
            tracing::error!(
                disk = %d,
                op = op,
                waited_ms = waited.as_millis() as u64,
                deadline_ms = limit.as_millis() as u64,
                "disk IO exceeded its per-op watchdog deadline; recording a bad observation"
            );
            observe_disk_at(statuses, metrics, d, DiskHealth::IoTimeout, h, now);
            // 一块盘可能同时有多个 op 超期:只记一次"这块盘出了问题"。
            if !round.tripped.contains(d) {
                round.tripped.push(d.clone());
            }
            // 逃生舱:该盘池**全部 worker 都被超期操作占住**,且最早那一发已经超过 escalate 秒数。
            // 默认 0 = 永不升级(单进程多盘,误杀一次 = 自造一次全节点故障,见 design §5.3)。
            if round.escalate.is_none()
                && should_escalate(
                    escalate_secs,
                    workers(d),
                    in_flight_total(d),
                    Some(start),
                    now,
                )
            {
                round.escalate = Some(d.clone());
            }
        }
    }
    round
}

/// 逃生判据(纯函数,单测直接打):`escalate_secs == 0` ⇒ 永不升级。
///
/// 判据取"已准入未完成数 ≥ worker 数"**且**"最早那一发在途已超过 `escalate_secs`":两者同时成立
/// 才说明"每条 worker 都被超期操作占住,且不是刚发生"。`escalate_secs` 应 **≥** 看门狗 deadline
/// (配置校验会拦下更小的值,否则会把"刚刚超期一次"误判成"永远不会回来了")。
pub fn should_escalate(
    escalate_secs: u64,
    worker_count: u32,
    in_flight: u32,
    oldest_start: Option<Instant>,
    now: Instant,
) -> bool {
    if escalate_secs == 0 || worker_count == 0 || in_flight < worker_count {
        return false;
    }
    match oldest_start {
        Some(t) => now.saturating_duration_since(t) >= Duration::from_secs(escalate_secs),
        None => false,
    }
}

/// 常驻看门狗。
///
/// **独立性是它的全部意义**:一个**自己**的 tokio 任务,既不在任何盘的池里跑(否则它会被同一批卡死的
/// 线程挡住),也不受 `disk_health_enabled` 约束(默认关探测的部署才是绝大多数,跟着关掉就等于
/// `UNAVAILABLE` 再无第二个来源)。
///
/// tick 周期 = **所有门槛里最小的那个**(还没配按 op 覆盖时就是 `disk_watchdog_ms` 本身)⇒
/// 最坏发现延迟是 tick 的 2 倍。这一点必须写进配置文档,别让运维按"30s"去理解 SLA。
///
/// `by_op_ms` 是**按 op 覆盖**的门槛(毫秒);空 = 全部用 `default_deadline`。覆盖值可以比默认**小**
/// (例如给读操作更紧的门槛),tick 会自动跟着变小,不会因为"默认 30s"而让 5s 的门槛迟迟不被检查。
pub fn spawn_watchdog(
    pools: Arc<crate::disk_pool::DiskPools>,
    statuses: DiskHealthMap,
    hysteresis: Hysteresis,
    settings: WatchdogSettings,
    metrics: Arc<StoreServerMetrics>,
) {
    let tick_len = settings.tick();
    let escalate_secs = settings.escalate_secs;
    let degraded = settings.degraded.clone();
    let deadline_of = move |op: &str| settings.deadline_for(op);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(tick_len);
        loop {
            tick.tick().await;
            let watch = pools.io_watch();
            let disks = pools.disk_ids();
            let round = watchdog_round(
                &statuses,
                &metrics,
                WatchdogInput {
                    ops_in_flight: &|d| watch.ops_in_flight(d),
                    oldest: &|d, op| watch.oldest(d, op),
                    in_flight_total: &|d| watch.in_flight_total(d),
                    workers: &|d| pools.worker_count(d).unwrap_or(0),
                    disks: &disks,
                    deadline: &deadline_of,
                    hysteresis: &hysteresis,
                    escalate_secs,
                },
            );
            // DEGRADED:读走本轮的增量计数(`swap(0)`,保证每发 IO 只被一轮统计),算占比,
            // 喂给**延迟**迟滞机。它只影响上报值(可能变 DEGRADED),不影响可服务性 —— 慢盘照常服务。
            if let Some(judge) = &degraded {
                for d in &disks {
                    let Some((slow, total)) = pools.take_io_stats(d) else {
                        continue;
                    };
                    // 本轮没有任何 IO ⇒ 不产生结论(空闲 ≠ 慢),迟滞机停在上一次判定上。
                    let Some(violated) = judge(slow, total) else {
                        continue;
                    };
                    observe_latency(&statuses, &metrics, d, violated, &hysteresis);
                }
            }
            if let Some(disk) = round.escalate {
                escalate_abort(&disk, escalate_secs);
            }
        }
    });
}

/// 逃生舱:某盘池被超期 IO 全部占住且已持续过久 ⇒ 判"这块盘永远不会回来了",**自杀**交 supervisor
/// (`systemd Restart=always` / `docker-compose restart: always`)重启。
///
/// **代价写在代码旁边**(design §5.3):antfs 是**单进程多盘**,自杀会**带走健康盘上的服务** ——
/// 所以它默认关(`escalate_secs = 0`,由 [`should_escalate`] 把住),门槛取得极高,且**先观察再开**。
/// 它是"两害相权"的终局手段:不重启的代价是一台慢慢烂掉的节点(线程被吃光、所有盘一起停),更贵。
///
/// 用 `abort()` 而非 panic/unwind:半截状态不该被任何 `Drop` 处理,也不该再试着做收尾 IO(那 IO 正是
/// 卡住的原因)。`abort()` 不做清理,正是这里想要的语义。
fn escalate_abort(disk: &str, escalate_secs: u64) -> ! {
    tracing::error!(
        disk = %disk,
        escalate_secs = escalate_secs,
        "disk pool fully occupied by overdue IO; aborting the process for supervisor restart"
    );
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::DiskHealth as V;

    fn hyst(bad_rounds: u32, bad_secs: u64, good_rounds: u32, recover: u64) -> Hysteresis {
        Hysteresis {
            bad_rounds,
            bad_secs,
            good_rounds,
            recover_stable_secs: recover,
        }
    }

    /// 默认档(3 轮 / 30s / 3 轮 / 120s),探测周期 60s —— 与配置默认值同形。
    fn default_hyst() -> Hysteresis {
        hyst(3, 30, 3, 120)
    }

    fn metrics() -> (StoreServerMetrics, antfs_metric::Registry) {
        let reg = antfs_metric::Registry::new();
        let m = StoreServerMetrics::new(&reg).expect("metrics");
        (m, reg)
    }

    /// **从未观测 ⇒ `UNKNOWN`,且不等于 `HEALTHY`**(规则 1)。
    #[test]
    fn never_observed_is_unknown_and_still_serves() {
        let s = DiskStatus::never_observed();
        assert_eq!(s.reported_state(), STATE_UNKNOWN);
        assert_ne!(s.reported_state(), STATE_HEALTHY, "未知绝不报健康");
        assert!(
            s.is_available(),
            "没有证据说它坏 ⇒ 不能拦(默认关探测的部署行为不变)"
        );

        let map = new_map();
        assert!(is_available(&map, "d0"), "表里没有该盘 = 从未观测 = 可服务");
        assert_eq!(reported_state(&map, "d0"), STATE_UNKNOWN);
    }

    /// 一次坏轮**不摘盘**(这就是"偶尔抽风别踢盘"),但会如实反映在原始判定里。
    #[test]
    fn a_single_bad_round_does_not_take_the_disk_out() {
        let mut s = DiskStatus::never_observed();
        let t0 = Instant::now();
        assert_eq!(
            s.observe(t0, V::WriteTimeout, &default_hyst()),
            Transition::None
        );
        assert!(s.is_available(), "一轮坏不是摘盘理由");
        assert_eq!(
            s.reported_state(),
            "WRITE_TIMEOUT",
            "原始判定如实上报(让 master 先别往这儿放新副本),但闸门仍放行"
        );
        assert_eq!(s.verdict(), Some(V::WriteTimeout));
    }

    /// 连续 N 轮坏 ⇒ 隔离(轮数判据)。
    #[test]
    fn bad_rounds_trigger_unavailable() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let t0 = Instant::now();
        assert_eq!(s.observe(t0, V::ReadTimeout, &h), Transition::None);
        assert_eq!(
            s.observe(t0 + Duration::from_secs(1), V::ReadTimeout, &h),
            Transition::None,
            "第二轮还不够(阈值 3)"
        );
        assert_eq!(
            s.observe(t0 + Duration::from_secs(2), V::ReadTimeout, &h),
            Transition::BecameUnavailable
        );
        assert!(!s.is_available());
        assert_eq!(
            s.reported_state(),
            STATE_UNAVAILABLE,
            "派生态压过具体故障枚举"
        );
    }

    /// **坏态持续 ≥ T 也隔离**(时长判据),且与轮数判据**取先到**。
    ///
    /// 场景:探测周期被调到 300s —— 只按轮数要等 15 分钟;时长判据在第 2 轮(已过 300s ≥ 30s)就动手。
    #[test]
    fn bad_secs_triggers_first_when_the_interval_is_long() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let t0 = Instant::now();
        assert_eq!(s.observe(t0, V::WriteTimeout, &h), Transition::None);
        assert_eq!(
            s.observe(t0 + Duration::from_secs(300), V::WriteTimeout, &h),
            Transition::BecameUnavailable,
            "两轮 < bad_rounds(3),但已过 300s ≥ bad_secs(30) ⇒ 先到者生效"
        );
    }

    /// 坏好交替**不抖**:好轮清零连续坏计数,几次抖动都摘不掉这块盘。
    #[test]
    fn alternating_rounds_never_trip_the_disk() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let mut t = Instant::now();
        for i in 0..20 {
            let v = if i % 2 == 0 {
                V::WriteTimeout
            } else {
                V::Healthy
            };
            t += Duration::from_secs(20); // 每轮都没超过 bad_secs(30)
            assert_eq!(s.observe(t, v, &h), Transition::None, "第 {i} 轮不该摘盘");
            assert!(s.is_available(), "抖动不该把盘摘掉(第 {i} 轮)");
        }
    }

    /// 复位比进入更严:要求**连续 M 轮好 且 稳定满 T2**;中间夹一个坏轮就重新计数。
    #[test]
    fn recovery_is_stricter_than_entry() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let t0 = Instant::now();
        for i in 0..3 {
            s.observe(t0 + Duration::from_secs(i), V::WriteTimeout, &h);
        }
        assert!(!s.is_available());

        // 连续 3 轮好,但只过了 2 秒 < recover_stable_secs(120) ⇒ **仍不可用**。
        let t1 = t0 + Duration::from_secs(10);
        assert_eq!(s.observe(t1, V::Healthy, &h), Transition::None);
        assert_eq!(
            s.observe(t1 + Duration::from_secs(1), V::Healthy, &h),
            Transition::None
        );
        assert_eq!(
            s.observe(t1 + Duration::from_secs(2), V::Healthy, &h),
            Transition::None,
            "轮数够了但稳定期不够 ⇒ 不复位"
        );
        assert!(!s.is_available(), "稳定期未满必须保持隔离");

        // 再来一轮好,且稳定期已满 120s ⇒ 复位。
        assert_eq!(
            s.observe(t1 + Duration::from_secs(120), V::Healthy, &h),
            Transition::BecameAvailable,
            "连续好轮数达标 + 稳定满 120s ⇒ 复位"
        );
        assert!(s.is_available());
        assert_eq!(s.reported_state(), STATE_HEALTHY);
    }

    /// 复位途中夹一个坏轮 ⇒ 连续好清零,必须重新凑够 M 轮(不许"糊过去")。
    #[test]
    fn a_bad_round_resets_the_good_streak() {
        let mut s = DiskStatus::never_observed();
        let h = hyst(2, 30, 3, 0); // 稳定期 0,专心盯"连续 M 轮"
        let t0 = Instant::now();
        s.observe(t0, V::WriteTimeout, &h);
        s.observe(t0 + Duration::from_secs(1), V::WriteTimeout, &h);
        assert!(!s.is_available());

        assert_eq!(
            s.observe(t0 + Duration::from_secs(2), V::Healthy, &h),
            Transition::None
        );
        assert_eq!(
            s.observe(t0 + Duration::from_secs(3), V::Healthy, &h),
            Transition::None
        );
        // 第 3 轮好之前插一个坏轮:连续好必须清零。
        assert_eq!(
            s.observe(t0 + Duration::from_secs(4), V::ReadError, &h),
            Transition::None
        );
        assert_eq!(
            s.observe(t0 + Duration::from_secs(5), V::Healthy, &h),
            Transition::None,
            "清零后只有 1 轮好,不该复位"
        );
        assert!(!s.is_available());
        s.observe(t0 + Duration::from_secs(6), V::Healthy, &h);
        assert_eq!(
            s.observe(t0 + Duration::from_secs(7), V::Healthy, &h),
            Transition::BecameAvailable,
            "重新凑满 3 轮好才复位"
        );
    }

    /// **复位后第一个坏轮不得凭旧计数立刻再摘**(迟滞不能因为计数残留而失效)。
    #[test]
    fn counters_do_not_leak_across_episodes() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let t0 = Instant::now();
        // 第一段坏态:5 轮(远超 bad_rounds)。
        for i in 0..5 {
            s.observe(t0 + Duration::from_secs(i), V::WriteTimeout, &h);
        }
        assert!(!s.is_available());
        // 复位。
        let t1 = t0 + Duration::from_secs(1000);
        for i in 0..3 {
            s.observe(t1 + Duration::from_secs(i * 60), V::Healthy, &h);
        }
        assert!(s.is_available());
        // 第二段坏态:第 1 轮**绝不能**立刻摘盘(旧计数必须已清零)。
        assert_eq!(
            s.observe(t1 + Duration::from_secs(500), V::WriteTimeout, &h),
            Transition::None,
            "计数不得跨坏态片段泄漏"
        );
        assert!(s.is_available());
    }

    /// 已经在坏态:重复坏轮不产迁移事件(只有**变化**才算迁移,免得计数刷成噪声)。
    #[test]
    fn repeated_bad_rounds_while_unavailable_do_not_re_transition() {
        let mut s = DiskStatus::never_observed();
        let h = default_hyst();
        let t0 = Instant::now();
        for i in 0..3 {
            s.observe(t0 + Duration::from_secs(i), V::WriteTimeout, &h);
        }
        assert!(!s.is_available());
        for i in 3..10 {
            assert_eq!(
                s.observe(t0 + Duration::from_secs(i), V::WriteTimeout, &h),
                Transition::None
            );
        }
        assert_eq!(s.reported_state(), STATE_UNAVAILABLE);
    }

    // ── DEGRADED(慢但可用)─────────────────────────────────────────────────────────────
    //
    // 这一档存在的理由:只有"可用/不可用"两档时,一块 p99 从 1ms 涨到 2s 的盘**既不会被隔离、
    // 也没人停止往它放副本** —— 它就一直烂在那儿(离看门狗 deadline 还差得远)。

    /// 延迟劣化**不影响可服务性**:慢盘照常服务(不 503),只是上报值变成 `DEGRADED`。
    #[test]
    fn degraded_disk_still_serves() {
        let mut s = DiskStatus::never_observed();
        let h = hyst(1, 30, 1, 0); // 1 轮即生效,专心盯语义
        let t0 = Instant::now();
        // 先有一次健康观测(报 HEALTHY)。
        s.observe(t0, V::Healthy, &h);
        assert_eq!(s.reported_state(), STATE_HEALTHY);

        // 延迟劣化命中 ⇒ DEGRADED。
        assert_eq!(
            s.observe_latency(t0 + Duration::from_secs(1), true, &h),
            Transition::BecameUnavailable
        );
        assert!(s.is_degraded(), "应处于延迟劣化态");
        assert!(
            s.is_available(),
            "**慢盘仍然服务** —— DEGRADED 绝不能变成 503(否则一次抖动就把盘摘了)"
        );
        assert_eq!(
            s.reported_state(),
            STATE_DEGRADED,
            "上报值应压过 HEALTHY,让 master 停止往它放新副本"
        );
    }

    /// `DEGRADED` 与 `UNAVAILABLE` 是**两件事**、**互相独立**:
    /// - 真坏了 ⇒ 报 `UNAVAILABLE`(压过 DEGRADED,因为那才是头号信息);
    /// - 隔离后延迟机仍可保持劣化,但不影响"已隔离"这个结论。
    #[test]
    fn unavailable_outranks_degraded_and_the_two_machines_are_independent() {
        let mut s = DiskStatus::never_observed();
        let h = hyst(1, 30, 1, 0);
        let t0 = Instant::now();
        s.observe_latency(t0, true, &h); // 先劣化
        assert_eq!(s.reported_state(), STATE_DEGRADED);
        s.observe(t0 + Duration::from_secs(1), V::WriteTimeout, &h); // 再判坏
        assert!(!s.is_available());
        assert_eq!(
            s.reported_state(),
            STATE_UNAVAILABLE,
            "已隔离时 UNAVAILABLE 压过 DEGRADED(头号信息是'被摘了')"
        );
        assert!(!s.is_degraded(), "已隔离的盘不再是'慢但可用'(它压根不可用)");
    }

    /// 延迟机有自己的迟滞:一轮劣化不置 `DEGRADED`(与健康机同一套纪律)。
    ///
    /// 这里把稳定期置 0,专心盯"轮数那一半";"复位还要求稳定满 T2"由
    /// [`Self::recovery_is_stricter_than_entry`] 用注入时刻钉住(两台机器共用同一套 `Latch` 逻辑)。
    #[test]
    fn latency_needs_its_own_hysteresis() {
        let mut s = DiskStatus::never_observed();
        let h = hyst(3, 30, 3, 0); // 3 轮 / 30s,复位不要求稳定期
        let t0 = Instant::now();
        assert_eq!(
            s.observe_latency(t0, true, &h),
            Transition::None,
            "一轮慢不算劣化"
        );
        assert!(!s.is_degraded());
        assert_eq!(
            s.observe_latency(t0 + Duration::from_secs(1), true, &h),
            Transition::None
        );
        assert_eq!(
            s.observe_latency(t0 + Duration::from_secs(2), true, &h),
            Transition::BecameUnavailable,
            "连续 3 轮慢 ⇒ DEGRADED"
        );
        assert!(s.is_degraded());
        // 慢下来之后要连续好轮才复位,且不影响可服务性。
        assert_eq!(
            s.observe_latency(t0 + Duration::from_secs(3), false, &h),
            Transition::None
        );
        assert!(s.is_degraded(), "一轮快不足以复位");
        s.observe_latency(t0 + Duration::from_secs(4), false, &h);
        assert_eq!(
            s.observe_latency(t0 + Duration::from_secs(5), false, &h),
            Transition::BecameAvailable
        );
        assert!(!s.is_degraded());
        assert!(s.is_available(), "全程都可用");
    }

    /// `DEGRADED` 进上报值域全集(否则状态 gauge 摆不平 —— 旧状态会永远留 1)。
    #[test]
    fn degraded_is_in_the_reported_state_domain() {
        assert!(REPORTED_STATES.contains(&STATE_DEGRADED));
        let (m, reg) = metrics();
        let map = new_map();
        let h = hyst(1, 30, 1, 0);
        let t0 = Instant::now();
        init_state_gauges(["d0"], &m);
        observe_latency_at(&map, &m, "d0", true, &h, t0);
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="DEGRADED"} 1"#),
            "DEGRADED 必须能上报:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="UNKNOWN"} 0"#),
            "旧状态必须被摆平:\n{text}"
        );
    }

    /// 观测落表 + 指标副作用:状态 gauge **整体摆平**(当前 1、其余 0)、迁移计数按 `from,to` 记。
    #[test]
    fn observe_disk_writes_gauges_and_transitions() {
        let (m, reg) = metrics();
        let map = new_map();
        let h = default_hyst();
        let t0 = Instant::now();
        init_state_gauges(["d0"], &m);

        observe_disk_at(&map, &m, "d0", V::Healthy, &h, t0);
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="HEALTHY"} 1"#),
            "健康态应为 1:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="UNKNOWN"} 0"#),
            "旧状态必须被摆平为 0:\n{text}"
        );

        observe_disk_at(
            &map,
            &m,
            "d0",
            V::WriteTimeout,
            &h,
            t0 + Duration::from_secs(1),
        );
        observe_disk_at(
            &map,
            &m,
            "d0",
            V::WriteTimeout,
            &h,
            t0 + Duration::from_secs(2),
        );
        assert_eq!(
            observe_disk_at(
                &map,
                &m,
                "d0",
                V::WriteTimeout,
                &h,
                t0 + Duration::from_secs(3)
            ),
            Transition::BecameUnavailable
        );
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="UNAVAILABLE"} 1"#),
            "隔离态应为 1:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_health_state{disk_id="d0",state="HEALTHY"} 0"#),
            "旧状态(HEALTHY)必须被摆平为 0 —— 否则看板上会出现'又健康又坏掉':\n{text}"
        );
        // **迁移只记"可服务性变了"那一次**。第 1 轮坏把上报值从 `HEALTHY` 变成 `WRITE_TIMEOUT`
        // (原始判定变了),但闸门仍放行 —— 那**不是**一次可用性迁移,故不该出现在这个计数器里。
        assert!(
            !text.contains(r#"from="HEALTHY",to="WRITE_TIMEOUT""#),
            "原始判定变化 ≠ 可用性迁移,不该计数:\n{text}"
        );
        assert!(
            text.contains(
                r#"store_server_disk_health_transitions_total{disk_id="d0",from="WRITE_TIMEOUT",to="UNAVAILABLE"} 1"#
            ),
            "迁移计数应按'变化前后两个上报值'记:\n{text}"
        );
        assert!(
            !text.contains(r#"from="HEALTHY",to="UNAVAILABLE""#),
            "第 3 轮之前的 from 已经是 WRITE_TIMEOUT,不该记成 HEALTHY:\n{text}"
        );
    }

    /// 逃生判据:`escalate_secs = 0` **永不**升级;只有"每条 worker 都被超期占住"才升级。
    #[test]
    fn escalation_requires_all_workers_stuck_and_is_off_by_default() {
        let now = Instant::now();
        let old = Some(now - Duration::from_secs(900));
        assert!(
            !should_escalate(0, 4, 4, old, now),
            "0 = 永不升级(默认;单进程多盘,误杀 = 自造全节点故障)"
        );
        assert!(
            !should_escalate(600, 4, 3, old, now),
            "还有 worker 空着 ⇒ 不是'永远回不来'"
        );
        assert!(
            !should_escalate(600, 4, 4, Some(now - Duration::from_secs(10)), now),
            "全占住但还没到时长门槛 ⇒ 先观察"
        );
        assert!(
            should_escalate(600, 4, 4, old, now),
            "全部 worker 被占住且最早一发已超 600s ⇒ 达到逃生门槛"
        );
        assert!(
            !should_escalate(600, 4, 4, None, now),
            "没有在途却判升级 = 自相矛盾,必须为假"
        );
    }

    /// 看门狗一轮:超期的盘被记一次 `IO_TIMEOUT` 坏观测并计入 trips;未超期的盘一点不碰。
    #[test]
    fn watchdog_round_only_touches_overdue_disks() {
        let (m, reg) = metrics();
        let map = new_map();
        let h = default_hyst();
        let now = Instant::now();
        let started = HashMap::from([
            ("late".to_string(), now - Duration::from_secs(31)),
            ("fresh".to_string(), now - Duration::from_secs(1)),
        ]);
        let disks = vec!["late".to_string(), "fresh".to_string(), "idle".to_string()];
        let round = watchdog_round(
            &map,
            &m,
            WatchdogInput {
                // 每块盘一发在途(单一 op,便于断言 trips 恰好 1;按 op 的倍数情形由
                // `multiple_overdue_ops_on_one_disk_are_reported_once_per_disk` 单独覆盖)。
                ops_in_flight: &|_| vec!["put"],
                oldest: &|d: &str, _op: &str| started.get(d).copied(),
                in_flight_total: &|_| 1,
                workers: &|_| 8,
                disks: &disks,
                deadline: &|_| Duration::from_secs(30),
                hysteresis: &h,
                escalate_secs: 0,
            },
        );
        assert_eq!(round.tripped, vec!["late".to_string()], "只碰超期的那块");
        assert!(round.escalate.is_none(), "未开逃生");
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_io_watchdog_trips_total{disk_id="late"} 1"#),
            "超期必须计数:\n{text}"
        );
        assert!(
            !text.contains(r#"disk_io_watchdog_trips_total{disk_id="fresh"}"#),
            "没超期的盘不该出现在指标里:\n{text}"
        );
        // 超期只是一次**坏观测**:单次超时不摘盘(走迟滞)。
        assert!(is_available(&map, "late"), "单次超期不摘盘");
        assert_eq!(reported_state(&map, "late"), "IO_TIMEOUT");
        assert!(is_available(&map, "fresh"), "从未超期的盘一点没动");
        assert_eq!(reported_state(&map, "fresh"), STATE_UNKNOWN);
    }

    /// **按 op 分档门槛的核心收益**:一次"大 IO 慢、读很快"的盘**不该**被判超期。
    ///
    /// 场景:`compaction` 已经跑了 100s(拷整段,慢是正常的),同一块盘的 `get` 只跑了 1s。
    /// 若两者共用一个 30s 门槛,这块**健康**盘会被判成超期 → 迟滞 → 隔离,一场误杀。
    /// 分档之后:compaction 用 300s 的门槛(还没到),get 用 30s(也没到)⇒ **一次超期都不记**。
    #[test]
    fn a_slow_compaction_does_not_trip_the_read_deadline() {
        let (m, reg) = metrics();
        let map = new_map();
        let h = default_hyst();
        let now = Instant::now();
        let started = HashMap::from([
            ("compaction".to_string(), now - Duration::from_secs(100)),
            ("get".to_string(), now - Duration::from_secs(1)),
        ]);
        let disks = vec!["d0".to_string()];
        let by_op = HashMap::from([
            ("compaction".to_string(), Duration::from_secs(300)),
            ("get".to_string(), Duration::from_secs(30)),
        ]);
        let round = watchdog_round(
            &map,
            &m,
            WatchdogInput {
                ops_in_flight: &|_| vec!["compaction", "get"],
                oldest: &|_, op: &str| started.get(op).copied(),
                in_flight_total: &|_| 2,
                workers: &|_| 8,
                disks: &disks,
                deadline: &|op: &str| *by_op.get(op).unwrap(),
                hysteresis: &h,
                escalate_secs: 0,
            },
        );
        assert!(
            round.tripped.is_empty(),
            "大 IO 用的是它自己那档门槛,不该连累读操作、也不该记超期:{:?}",
            round.tripped
        );
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            !text.contains("disk_io_watchdog_trips_total"),
            "一次超期都不该产生:\n{text}"
        );
        assert!(is_available(&map, "d0"), "健康盘一点没动");

        // 反证:同一个 compaction 一旦也超过它**自己**那档 300s 门槛,照样要记超期。
        let older = HashMap::from([
            ("compaction".to_string(), now - Duration::from_secs(400)),
            ("get".to_string(), now - Duration::from_secs(1)),
        ]);
        let round = watchdog_round(
            &map,
            &m,
            WatchdogInput {
                ops_in_flight: &|_| vec!["compaction", "get"],
                oldest: &|_, op: &str| older.get(op).copied(),
                in_flight_total: &|_| 2,
                workers: &|_| 8,
                disks: &disks,
                deadline: &|op: &str| *by_op.get(op).unwrap(),
                hysteresis: &h,
                escalate_secs: 0,
            },
        );
        assert_eq!(
            round.tripped,
            vec!["d0".to_string()],
            "超过自己那档门槛就该记超期(分档不是豁免)"
        );
    }

    /// 一块盘上**多个 op** 同时超期 ⇒ 只记一次"这块盘出了问题"(`tripped` 去重),
    /// 但每个 op 各计一次 trips(便于看出是哪类 IO 在超期)。
    #[test]
    fn multiple_overdue_ops_on_one_disk_are_reported_once_per_disk() {
        let (m, reg) = metrics();
        let map = new_map();
        let h = default_hyst();
        let now = Instant::now();
        let stale = now - Duration::from_secs(600);
        let disks = vec!["d0".to_string()];
        let round = watchdog_round(
            &map,
            &m,
            WatchdogInput {
                ops_in_flight: &|_| vec!["flush", "get"],
                oldest: &|_, _| Some(stale),
                in_flight_total: &|_| 2,
                workers: &|_| 8,
                disks: &disks,
                deadline: &|_| Duration::from_secs(30),
                hysteresis: &h,
                escalate_secs: 0,
            },
        );
        assert_eq!(round.tripped, vec!["d0".to_string()], "按盘去重");
        let text = antfs_metric::gather(&reg).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_io_watchdog_trips_total{disk_id="d0"} 2"#),
            "两个 op 各计一次(看得出是哪类 IO 在超期):\n{text}"
        );
    }

    /// 看门狗反复超期 ⇒ 累计到迟滞阈值后**摘盘**(与探测路径共用同一台状态机)。
    #[test]
    fn repeated_watchdog_trips_take_the_disk_out_via_hysteresis() {
        let (m, _reg) = metrics();
        let map = new_map();
        let h = hyst(2, 30, 3, 0);
        let disks = vec!["d0".to_string()];
        let now = Instant::now();
        let stale = Some(now - Duration::from_secs(60));
        for _ in 0..2 {
            watchdog_round(
                &map,
                &m,
                WatchdogInput {
                    ops_in_flight: &|_| vec!["put"],
                    oldest: &|_, _| stale,
                    in_flight_total: &|_| 1,
                    workers: &|_| 8,
                    disks: &disks,
                    deadline: &|_| Duration::from_secs(30),
                    hysteresis: &h,
                    escalate_secs: 0,
                },
            );
        }
        assert!(
            !is_available(&map, "d0"),
            "两次超期达到 bad_rounds=2 ⇒ 隔离"
        );
        assert_eq!(reported_state(&map, "d0"), STATE_UNAVAILABLE);
    }
}
