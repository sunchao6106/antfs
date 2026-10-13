//! 每盘有界池(D1/N4):本盘的引擎 IO 走**本盘自己的 worker 线程**,不再共用进程级阻塞池。
//!
//! ## 为什么必须"归该盘所有"
//!
//! 引擎 IO 是同步阻塞(`std::fs`),而坏盘上的一次 `write`/`read` 会停在**内核 D 态** —— 收不回来,
//! 也取消不掉。原先所有盘挤 `tokio::task::spawn_blocking` 那一个进程级池(默认 512 线程):一块盘挂死
//! 就能把池吃满,于是**连健康盘上的请求也一起排队**(见 `docs/design/store.md` §12.11)。只加信号量不够
//! —— 信号量只限制"同时在飞多少",线程仍是共享池的,租约耗尽时坏盘照样占着别人的线程。故本模块让
//! 每块盘**自己拥有一组线程**:最坏情况是"这块盘的线程全卡死",与别的盘无关。
//!
//! ## 准入语义:短暂排队,超时才拒
//!
//! 每盘容量 = `pool_size`(worker 线程数)+ `pool_queue_size`(排队容量),用一把信号量表达。提交时:
//! - 有空位 → 立刻准入(进队列,由本盘 worker 取走执行);
//! - 没空位 → **最多等 `pool_acquire_timeout_ms`**,等到即准入,等不到 → `Saturated`(调用方快速失败,
//!   HTTP 面回 503)。
//!
//! 排队**有界**是刻意的:无限排队会推高延迟、占死连接,还会掩盖"这块盘已经不行了"这件事;快速失败
//! 让上游(router 的 W 多数派 / frontend)立刻改用别的副本。
//!
//! **不负责的**:一次**已准入**的操作没有超时。盘慢不慢由健康闸门/看门狗
//! (`openspec/changes/store-disk-isolation/` D2/D3)判定后让**新**请求快速失败,而不是在这里把一次
//! 已经开始的写中途判死。同理,健康探测与盘容量 `statvfs` **不走本池**(它们有自己的单飞守卫):让探测
//! 去抢请求的池,等于坏盘把探测器一起拖进去。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::{DataDir, StoreServerConfig};
use crate::disk::DiskManager;
use crate::error::{Error, Result};
use crate::metrics::StoreServerMetrics;

/// 池拒绝一次提交的理由(调用方据此决定状态码/日志)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolReject {
    /// 池满:等满 `pool_acquire_timeout_ms` 仍无空位 → **快速失败**(HTTP 503)。
    Saturated,
    /// 该盘没有池(未知 disk_id)。既有池却查不到盘 = 配置/代码错,不该出现。
    UnknownDisk,
    /// 池已关闭(进程收尾,或该池的持有者已析构)。
    Closed,
    /// 任务在 worker 上 **panic**(引擎里炸了)。这是内部错误,不是"盘忙" —— 调用方要回 500 而不是 503,
    /// 也不该让上层把它当成可重试的过载。
    JobPanicked,
    /// **任务尚未开始、而该盘已被判定不可服务**(D2):丢弃它,快失败,别去碰一块已知不会好的盘。
    ///
    /// 与 [`Saturated`](Self::Saturated) 的区别:**过载**是"再等等也许会好",**盘不可服务**是
    /// "这块盘现在别碰"。两者都是 `503 + Retry-After`,但 `reason` 标签不同,看板上能分清。
    DiskUnavailable,
}

impl PoolReject {
    /// 指标/日志里的稳定取值(`reason` 标签)。
    pub fn as_str(self) -> &'static str {
        match self {
            PoolReject::Saturated => "saturated",
            PoolReject::UnknownDisk => "unknown_disk",
            PoolReject::Closed => "closed",
            PoolReject::JobPanicked => "job_panicked",
            PoolReject::DiskUnavailable => "disk_unavailable",
        }
    }
}

/// 测试专用:`disk_id -> 当前还活着的 worker 线程数`。
///
/// 为什么需要它:"worker 线程该随池析构退出"是生命周期契约的一部分,而线程泄漏在测试/CI 里最难发现
/// (反复建销 store-server 会攒下成百上千条线程)。**按盘计数**是为了让用例只看自己那几块盘的线程
/// (cargo 并行跑用例时,全局计数会被别的用例搅浑)。线上零开销(整段 `cfg(test)`)。
#[cfg(test)]
pub(crate) fn live_workers(disk_id: &str) -> usize {
    LIVE_WORKERS_BY_DISK
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(disk_id)
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
static LIVE_WORKERS_BY_DISK: std::sync::OnceLock<Mutex<std::collections::HashMap<String, usize>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn worker_live_count(disk_id: &str, delta: isize) {
    let map = LIVE_WORKERS_BY_DISK.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(|e| e.into_inner());
    let entry = guard.entry(disk_id.to_string()).or_insert(0);
    *entry = (*entry as isize + delta).max(0) as usize;
}

/// 一次任务的载荷。闭包持有 [`JobGuard`],收尾(含 panic 展开)由守卫负责。
type Job = Box<dyn FnOnce() + Send + 'static>;

/// 每盘**在途 IO 的开始时刻多重集**(由 worker 在**闭包内**盖戳,供看门狗读"最久没回来的那一发")。
///
/// ## 为什么是多重集,而不是"最早开始时刻"这一个标量
///
/// 任务**完成的顺序与开始的顺序无关**。若只维护一个"最早开始时刻"标量:A 在 t0 开始、B 在 t1 开始、
/// **A 先结束** —— 标量仍停在 t0,于是一块健康盘的"最久在途"被越算越大,最后被看门狗误判成坏盘。
/// 用 `BTreeMap<开始时刻, 条数>` 取 `first_key` 才是真值。
///
/// ## 为什么在闭包内盖戳(而不是提交时)
///
/// 提交时刻到闭包开跑之间的那段是**池排队**。把它算进来意味着"盘正常但池忙"也会推高"最久在途",
/// 于是看门狗把健康盘判死(design §5.1)。排队深度另有 `disk_pool_queued` 可看,职责不混。
/// 每个 `(盘, op)` 的在途开始时刻表:`开始时刻 -> 该时刻开跑的条数`。
type OpStarts = BTreeMap<Instant, u32>;
/// `(盘, op) -> 该 op 的在途开始时刻表`。
type WatchTable = HashMap<(String, &'static str), OpStarts>;

pub struct IoWatch {
    /// `(disk_id, op) -> (开始时刻 -> 该时刻开跑的条数)`。空 entry 会被顺手删掉(不留垃圾)。
    ///
    /// **按 op 再分一层**是为了支持"按 op 分档的超期门槛"(慢盘那一轮):compaction 拷整段天然比一次
    /// 4KiB 读慢得多,共用一个门槛就会把健康盘的 compaction 判成超期。分了层,看门狗才能对
    /// 每个 op 用它自己的 deadline,而不是让最慢的那个 op 决定全盘的命运。
    by_key: Mutex<WatchTable>,
}

impl Default for IoWatch {
    fn default() -> Self {
        Self::new()
    }
}

impl IoWatch {
    pub fn new() -> Self {
        IoWatch {
            by_key: Mutex::new(WatchTable::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WatchTable> {
        // 中毒也恢复着用:表内容永远是结构有效的,而在这里 panic 会把一个正在跑的任务的收尾吃掉。
        self.by_key.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 登记一发 IO 开始(worker 在闭包内调);返回的守卫 `Drop` 时注销(panic 展开也照跑)。
    pub fn begin(self: &Arc<Self>, disk_id: &str, op: &'static str) -> IoWatchGuard {
        let now = Instant::now();
        *self
            .lock()
            .entry((disk_id.to_string(), op))
            .or_default()
            .entry(now)
            .or_insert(0) += 1;
        IoWatchGuard {
            watch: self.clone(),
            key: (disk_id.to_string(), op),
            start: now,
        }
    }

    fn end(&self, key: &(String, &'static str), start: Instant) {
        let mut map = self.lock();
        let Some(entries) = map.get_mut(key) else {
            return;
        };
        if let Some(n) = entries.get_mut(&start) {
            *n -= 1;
            if *n == 0 {
                entries.remove(&start);
            }
        }
        if entries.is_empty() {
            map.remove(key);
        }
    }

    /// 该盘该 op 上最久未归的一发 IO 的**开始时刻**(无在途 → `None`)。看门狗的判据就是这个。
    pub fn oldest(&self, disk_id: &str, op: &str) -> Option<Instant> {
        let op = op_static(op)?;
        self.lock()
            .get(&(disk_id.to_string(), op))
            .and_then(|m| m.keys().next().copied())
    }

    /// 该盘该 op 上**正在 syscall 里**的 IO 条数(不含排队 —— 排队的那一发还没盖戳)。
    pub fn in_flight(&self, disk_id: &str, op: &str) -> u32 {
        let Some(op) = op_static(op) else {
            return 0; // 未知 op(拼错):按"没有在途"处理,不在数据面路径上 panic。
        };
        self.lock()
            .get(&(disk_id.to_string(), op))
            .map(|m| m.values().sum())
            .unwrap_or(0)
    }

    /// 该盘**全部** op 合计的在途条数(逃生判据用:"是不是每条 worker 都被占住了")。
    pub fn in_flight_total(&self, disk_id: &str) -> u32 {
        self.lock()
            .iter()
            .filter(|((d, _), _)| d == disk_id)
            .map(|(_, m)| m.values().sum::<u32>())
            .sum()
    }

    /// 该盘**当前有在途 IO** 的 op 列表(空闲的 op 不必检查;看门狗据此遍历)。
    pub fn ops_in_flight(&self, disk_id: &str) -> Vec<&'static str> {
        let mut ops: Vec<&'static str> = self
            .lock()
            .keys()
            .filter(|(d, _)| d == disk_id)
            .map(|(_, op)| *op)
            .collect();
        ops.sort_unstable();
        ops.dedup();
        ops
    }
}

/// `&str` → `&'static str` 的**受控**转换:op 名全部来自代码里的字面量(`pool.run(&disk, "get", ..)`),
/// 故查表时命中的一定是那些字面量本身。查不到(拼错的 op)返回 `None`,调用方按"没有在途"处理 ——
/// 宁可漏看一发,也不 panic 在数据面路径上。
fn op_static(op: &str) -> Option<&'static str> {
    KNOWN_OPS.iter().copied().find(|k| *k == op)
}

/// 池上出现过的 op 名(**唯一真值表**)。新增池上调用点时请同步这里 ——
/// `config` 的按 op 分档校验也用它(拼错的 key 会在**起不来**时被拒,而不是静默失效)。
pub const KNOWN_OPS: [&str; 14] = [
    "put",
    "get",
    "delete",
    "metadata",
    "replica_metadata",
    "segments",
    "segment",
    "scrub_report",
    "verify",
    "compact",
    "compaction",
    "flush",
    "replication",
    "test_hold",
];

/// [`IoWatch`] 的一次登记;`Drop` 时注销(故必须活在闭包内)。
pub struct IoWatchGuard {
    watch: Arc<IoWatch>,
    key: (String, &'static str),
    start: Instant,
}

impl Drop for IoWatchGuard {
    fn drop(&mut self) {
        self.watch.end(&self.key, self.start);
    }
}

/// 一次任务的**闭包内计时守卫**:`Drop` 时 observe 一次(`start` 在闭包内取)。
///
/// 用守卫而不是"函数尾部写一行",是为了让 panic 路径也有观测 —— 于是有一条干净的不变量:
/// **每个真正开跑的 job 恰好产生一个时长样本**(与"准入后在飞数"能一一对上)。否则"引擎在某条
/// 路径上必炸"时直方图会一片空白,看起来像"这块盘没有 IO",反而更难查。
struct JobTimer {
    duration: antfs_metric::HistogramVec,
    disk: String,
    op: &'static str,
    start: Instant,
    /// **延迟劣化**(DEGRADED)的分类阈值(毫秒);`0` = 不分类(该部署没开这一档)。
    slow_threshold_ms: u64,
    /// 该盘增量计数器:`(慢 IO 数, IO 总数)`。看门狗每轮读一次差值算占比。
    slow_io: Arc<std::sync::atomic::AtomicU64>,
    io_total: Arc<std::sync::atomic::AtomicU64>,
}

impl Drop for JobTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.duration
            .with_label_values(&[self.disk.as_str(), self.op])
            .observe(elapsed.as_secs_f64());
        // 增量计数(不是直方图求差分):直方图只有桶且不带 op 之外的维度,而"慢 IO 占比"要的是
        // **精确的分子分母**,并且必须能被看门狗按轮读取-清零。守卫在 panic 展开时也照走同一段,
        // 于是"每个开跑的 job 恰好贡献一份分类样本"这条不变量成立。
        if self.slow_threshold_ms > 0 {
            use std::sync::atomic::Ordering;
            self.io_total.fetch_add(1, Ordering::Relaxed);
            if elapsed.as_millis() as u64 >= self.slow_threshold_ms {
                self.slow_io.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// 一次执行从"准入"到"收尾"的记账守卫:**Drop 里**递减在飞并释放 permit。
///
/// 为什么不写在闭包尾部:引擎调用 panic 时(某个断言/越界/`unwrap` 炸了),尾部那两行根本执行不到 ——
/// 在飞计数会**永久漂高**(面板上那块盘永远显示"还在跑"),permit 也会漏,等于悄悄缩容。Drop 在 panic
/// 展开时照跑,故把这两件事绑在守卫上。
struct JobGuard {
    inner: Arc<PoolInner>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        self.inner.in_flight.dec();
    }
}

/// 池的可共享部分:队列 + 关停标志 + 计数。
///
/// 与 [`DiskPool`] 分开是为了**不产生引用环**:worker 线程长期持 `Arc<PoolInner>`,而 `DiskPool` 是
/// "可析构的把手"——它的 `Drop` 清队列、置 `closed` 并唤醒 worker,worker 随后退出并释放 `Arc`,
/// 于是进程内(以及测试里反复建/销 store-server 时)既不留线程也不留内存。
struct PoolInner {
    /// 盘 id(worker 日志/panic 报告用;线程名里也有,但日志得自报家门)。
    disk_id: String,
    queue: Mutex<VecDeque<Job>>,
    cv: Condvar,
    closed: AtomicBool,
    in_flight: antfs_metric::IntGauge,
    queued: antfs_metric::IntGauge,
    /// 在途 IO 的开始时刻表(D3 看门狗的数据源)。
    watch: Arc<IoWatch>,
    /// **闭包内**测得的单次 IO 时长直方图(syscall 口径,不含排队 —— 见 [`IoWatch`] 的说明)。
    io_duration: antfs_metric::HistogramVec,
    /// **延迟劣化**的分类阈值(毫秒;`0` = 不分类 —— 该部署没开 DEGRADED)。
    slow_threshold_ms: u64,
    /// 该盘的增量计数器:`(慢 IO 数, IO 总数)`。看门狗每轮读差值算"慢 IO 占比"。
    slow_io: Arc<std::sync::atomic::AtomicU64>,
    io_total: Arc<std::sync::atomic::AtomicU64>,
    /// 该盘的可服务性(D2)。**任务开跑前查一次** ⇒ 已被判定不可服务的盘上,那些"还排在队列里"的请求
    /// 被直接丢掉,不去碰一块已知不会好的盘(design §5.2 的最后一条)。
    ///
    /// 为什么是"开跑前查实时状态"而不是"状态变化时置个标志位":标志位需要**同步**,而同步总有一拍延迟 ——
    /// 盘**恢复**之后那一拍里,所有任务都会被误拒(真踩过这个设计陷阱)。查实时状态没有这个窗口。
    health: crate::disk_health::DiskHealthMap,
}

impl PoolInner {
    /// 置关停标志并唤醒全部 worker(幂等)。
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.cv.notify_all();
    }
}

/// 一块盘的池(把手;`Arc` 共享给所有提交方)。
pub struct DiskPool {
    inner: Arc<PoolInner>,
    /// 准入容量 = worker 数 + 排队容量;permit 随任务移动,任务结束时释放。
    permits: Arc<tokio::sync::Semaphore>,
    worker_count: u32,
    queue_capacity: u32,
}

impl Drop for DiskPool {
    fn drop(&mut self) {
        // **先置关停标志,再清队列**:清队列会把排队任务的 `oneshot` sender 丢掉、等待方被唤醒,
        // 它按"此刻 `closed` 是否已置位"归因(见 `run` 的收尾)——顺序反了(先清后置)就会出现
        // "sender 已丢、`closed` 还没置"的窗口,把关停误报成 `JobPanicked`。
        // 代价是反方向的误报:关停进行中又 panic 的任务会被报成 `Closed` —— 反正池在收尾,可接受。
        self.inner.close();
        {
            let mut q = self.inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            q.clear();
        }
    }
}

impl DiskPool {
    /// 起 `workers` 条**本盘专属**线程(线程名带盘 id,`top -H`/调试一眼认出),队列容量 `queue_capacity`。
    fn start(
        disk_id: &str,
        workers: u32,
        queue_capacity: u32,
        watch: Arc<IoWatch>,
        health: crate::disk_health::DiskHealthMap,
        slow_threshold_ms: u64,
        metrics: &StoreServerMetrics,
    ) -> Self {
        let workers = workers.max(1);
        let inner = Arc::new(PoolInner {
            disk_id: disk_id.to_string(),
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            in_flight: metrics.disk_pool_in_flight.with_label_values(&[disk_id]),
            queued: metrics.disk_pool_queued.with_label_values(&[disk_id]),
            watch,
            io_duration: metrics.disk_io_duration.clone(),
            slow_threshold_ms,
            slow_io: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            io_total: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            health,
        });
        metrics
            .disk_pool_workers
            .with_label_values(&[disk_id])
            .set(i64::from(workers));
        for i in 0..workers {
            let inner = inner.clone();
            let name = format!("antfs-disk-{disk_id}-{i}");
            // 线程创建失败(极端:线程数/内存打满)不该让节点起不来:少一条 worker 只是并发降一点。
            let spawned = thread::Builder::new().name(name.clone()).spawn(move || {
                let _ =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker_loop(&inner)));
            });
            if let Err(e) = spawned {
                tracing::warn!(disk = %disk_id, worker = %name, error = %e, "disk pool worker thread not started");
            }
        }
        DiskPool {
            inner,
            permits: Arc::new(tokio::sync::Semaphore::new(
                (workers + queue_capacity) as usize,
            )),
            worker_count: workers,
            queue_capacity,
        }
    }
}

/// worker 主循环:取任务 → 跑任务(锁外)→ 回到取任务;收到关停即退出。
fn worker_loop(inner: &Arc<PoolInner>) {
    // 进来登记、退出注销(仅测试统计;worker 退出 = 池析构时收到关停标志)。
    #[cfg(test)]
    struct LiveGuard(String);
    #[cfg(test)]
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            worker_live_count(&self.0, -1);
        }
    }
    #[cfg(test)]
    let _live = {
        worker_live_count(&inner.disk_id, 1);
        LiveGuard(inner.disk_id.clone())
    };

    let mut queue = inner.queue.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        if let Some(job) = queue.pop_front() {
            inner.queued.dec();
            drop(queue); // 任务在**锁外**执行:执行期间的提交/关停都不被它挡住
                         // **逐任务**接住 panic:一个任务炸了不能把这条 worker 线程带走 —— 那等于这块盘永久少一份
                         // 并发,反复几次并发就归零了(表现为"所有请求都要等满准入超时才 503",而没人知道为什么)。
                         // 记账与 permit 由 `JobGuard` 在展开时收尾,这里只负责"活下来 + 大声报"。
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err();
            if panicked {
                tracing::error!(
                    disk = %inner.disk_id,
                    "disk pool job panicked; worker thread continues serving"
                );
            }
            queue = inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            continue;
        }
        if inner.closed.load(Ordering::SeqCst) {
            return;
        }
        queue = inner.cv.wait(queue).unwrap_or_else(|e| e.into_inner());
    }
}

/// 全节点所有盘的池。
pub struct DiskPools {
    pools: BTreeMap<String, Arc<DiskPool>>,
    acquire_timeout: Duration,
    rejected: antfs_metric::IntCounterVec,
    /// **全节点共用一张**在途 IO 表(按 `disk_id` 分):看门狗读它判超期。
    ///
    /// 全节点一张而不是每池一张,是为了让看门狗只需一个句柄;数据仍按盘分开,互不影响。
    watch: Arc<IoWatch>,
}

impl DiskPools {
    /// 按配置为每块盘建池。
    pub fn new(
        disks: &DiskManager,
        config: &StoreServerConfig,
        metrics: &StoreServerMetrics,
        health: crate::disk_health::DiskHealthMap,
    ) -> Result<Self> {
        let sizes = allocate_workers(disks, config.pool_default_size, &config.data_dirs)?;
        let watch = Arc::new(IoWatch::new());
        let mut pools = BTreeMap::new();
        for disk in disks.disks() {
            let workers = sizes.get(&disk.id).copied().unwrap_or(1);
            pools.insert(
                disk.id.clone(),
                Arc::new(DiskPool::start(
                    &disk.id,
                    workers,
                    config.pool_queue_size,
                    watch.clone(),
                    health.clone(),
                    config.disk_degraded_io_ms,
                    metrics,
                )),
            );
        }
        Ok(DiskPools {
            pools,
            acquire_timeout: Duration::from_millis(config.pool_acquire_timeout_ms),
            rejected: metrics.disk_pool_rejected.clone(),
            watch,
        })
    }

    /// 在途 IO 表(看门狗/测试读)。
    pub fn io_watch(&self) -> Arc<IoWatch> {
        self.watch.clone()
    }

    /// **读走**该盘的增量计数 `(慢 IO 数, IO 总数)` 并**清零** —— 看门狗每轮读一次算占比。
    ///
    /// 为什么是"读走并清零"而不是快照:`swap(0)` 让每一发 IO 恰好被**一轮**统计到,
    /// 不会出现"相邻两轮都把同一批样本算进去"(那会让占比偏大、DEGRADED 迟滞失真)。
    pub fn take_io_stats(&self, disk_id: &str) -> Option<(u64, u64)> {
        use std::sync::atomic::Ordering;
        let p = self.pools.get(disk_id)?;
        let slow = p.inner.slow_io.swap(0, Ordering::Relaxed);
        let total = p.inner.io_total.swap(0, Ordering::Relaxed);
        Some((slow, total))
    }

    /// 全部盘 id(**有序**,便于确定性遍历)。
    pub fn disk_ids(&self) -> Vec<String> {
        self.pools.keys().cloned().collect()
    }

    /// 该盘池的 worker 数(自省/测试用)。
    pub fn worker_count(&self, disk_id: &str) -> Option<u32> {
        self.pools.get(disk_id).map(|p| p.worker_count)
    }

    /// 该盘池的排队容量(自省/测试用)。
    pub fn queue_capacity(&self, disk_id: &str) -> Option<u32> {
        self.pools.get(disk_id).map(|p| p.queue_capacity)
    }

    /// 把一次阻塞任务提交到 `disk_id` 的池上执行并等它返回。
    ///
    /// - `op`:只用于拒绝计数 `disk_pool_rejected{disk_id,op,reason}` 与日志。
    /// - **准入**有超时(池满 ⇒ 短暂排队,超时即 `Saturated`);**准入之后不再设超时**(见模块注释)。
    pub async fn run<T, F>(
        &self,
        disk_id: &str,
        op: &'static str,
        f: F,
    ) -> std::result::Result<T, PoolReject>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let pool = match self.pools.get(disk_id) {
            Some(p) => p.clone(),
            None => {
                self.rejected
                    .with_label_values(&[disk_id, op, PoolReject::UnknownDisk.as_str()])
                    .inc();
                return Err(PoolReject::UnknownDisk);
            }
        };
        // `acquire_owned`(而非 `acquire`):permit 要跟着任务搬进 worker 线程的闭包里,而
        // `SemaphorePermit<'_>` 借的是信号量本身、活不到那里;owned 版持有 `Arc`,无生命周期。
        let permit =
            match tokio::time::timeout(self.acquire_timeout, pool.permits.clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                // 信号量关闭:进程收尾。
                Ok(Err(_)) => {
                    self.rejected
                        .with_label_values(&[disk_id, op, PoolReject::Closed.as_str()])
                        .inc();
                    return Err(PoolReject::Closed);
                }
                Err(_) => {
                    self.rejected
                        .with_label_values(&[disk_id, op, PoolReject::Saturated.as_str()])
                        .inc();
                    return Err(PoolReject::Saturated);
                }
            };

        // 通道送的是 `Result<T, PoolReject>`:让"任务**没跑**就被丢掉"有精确的归因 ——
        // 若只送 `T`,丢弃(G)后调用方只能靠"池的某个标志位"猜原因,而标志位会误伤并发的 panic。
        let (tx, rx) = tokio::sync::oneshot::channel::<std::result::Result<T, PoolReject>>();
        let inner = pool.inner.clone();
        let guard = JobGuard {
            inner: inner.clone(),
            _permit: permit,
        };
        // 闭包需要自己那份盘 id(参数 `disk_id` 后面还要用于拒绝打点),以及自己那份 `inner`
        // (下面那块代码正借着 `inner.queue`,闭包不能再把它整个移走)。
        let job_disk = disk_id.to_string();
        let job_inner = inner.clone();
        {
            // 中毒也**恢复**着用:队列内容不会因为某次 panic 变得结构无效,而在这里 panic 会
            // 把"已拿到 permit"的调用方拖死(许可也不会归还)。恢复比炸掉安全。
            let mut q = inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            q.push_back(Box::new(move || {
                // **开跑前先查该盘还能不能服务**(D2):不能 ⇒ 直接丢弃这一发,连 syscall 都不碰。
                // 覆盖的正是"盘刚被判坏时,队列里还压着的那批" —— 它们进队时盘还是好的。
                // 只看**实时状态**,不引入标志位同步,故盘一恢复就立刻恢复正常服务(没有误拒窗口)。
                if !crate::disk_health::is_available(&job_inner.health, &job_disk) {
                    let _ = tx.send(Err(PoolReject::DiskUnavailable));
                    drop(guard); // 释放 permit + 递减在飞(guard 的 Drop 负责)
                    return;
                }
                // 计时点**紧贴 syscall 前后**:这段才是"这块盘自己快不快"。排队时间不算 —— 见
                // `IoWatch` 的说明(用端到端口径做隔离判据会把"池忙"判成"盘坏")。
                //
                // 两个守卫都在闭包内 Drop ⇒ panic 展开时也照常注销/记账,计数不会漂。
                let _w = job_inner.watch.begin(&job_disk, op);
                let _t = JobTimer {
                    duration: job_inner.io_duration.clone(),
                    disk: job_disk.clone(),
                    op,
                    start: Instant::now(),
                    slow_threshold_ms: job_inner.slow_threshold_ms,
                    slow_io: job_inner.slow_io.clone(),
                    io_total: job_inner.io_total.clone(),
                };
                let out = f();
                // 先收尾观测与在途登记,再交结果:调用方拿到值时计数已落回稳态。
                drop(_t);
                drop(_w);
                let _ = tx.send(Ok(out));
                drop(guard);
            }));
            // **入队之后**才递增计数:这样"观测到 queued ≥ 1"严格蕴含"任务已在队列里"(调用方/测试
            // 拿它当"已入队"的判据是可靠的),而不是"马上要入队"。
            inner.in_flight.inc();
            inner.queued.inc();
        }
        inner.cv.notify_one();

        let outcome = match rx.await {
            Ok(Ok(out)) => return Ok(out),
            // 任务**跑了但拒绝执行**(开跑前发现盘不可服务):归因精确,不会误伤并发的 panic。
            Ok(Err(reject)) => reject,
            // 送结果的一端消失了(闭包被丢、或任务 panic)。两者对上游含义不同,按 `closed` 区分:
            Err(_) => {
                if inner.closed.load(Ordering::SeqCst) {
                    PoolReject::Closed
                } else {
                    PoolReject::JobPanicked
                }
            }
        };
        self.rejected
            .with_label_values(&[disk_id, op, outcome.as_str()])
            .inc();
        Err(outcome)
    }
}

/// 每块盘的 worker 数:**该盘显式指定的 `pool_size`**,没给就用 `pool_default_size`。
///
/// 口径刻意做成"每盘一个默认值"而不是"总预算均分":运维脑子里的模型是"每块盘开多少并发",
/// 而"盘数一变、每盘大小跟着变"隐式耦合,很难预料(加一块盘就把其它盘的并发悄悄改了)。
///
/// 盘与声明的对应关系按**目录逐字匹配**(`data_dirs` 就是声明本身,故正常不会有对不上的)。
fn allocate_workers(
    disks: &DiskManager,
    default: u32,
    specs: &[DataDir],
) -> Result<BTreeMap<String, u32>> {
    let mut out: BTreeMap<String, u32> = BTreeMap::new();
    for d in disks.disks() {
        let declared = specs
            .iter()
            .find(|s| d.dir.as_os_str() == std::ffi::OsStr::new(s.dir()));
        let Some(declared) = declared else {
            return Err(Error::Config(antfs_config::ConfigError::invalid(
                "store_server.data_dirs",
                format!(
                    "mounted dir {} is not declared in store_server.data_dirs",
                    d.dir.display()
                ),
            )));
        };
        out.insert(d.id.clone(), declared.pool_size().unwrap_or(default).max(1));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DiskSpec;
    use crate::metrics::StoreServerMetrics;
    use std::sync::mpsc;
    use tokio::sync::oneshot;

    /// 测试夹具:每块盘一个 tempdir(保活),拿到池 + **池实际接入的那个 registry**。
    ///
    /// 断言走该 registry(CLAUDE.md §6:校验指标必须对被测对象真正接入的那个实例断言)。
    struct Fixture {
        pool: Arc<DiskPools>,
        registry: antfs_metric::Registry,
        ids: Vec<String>,
        /// 池实际接入的那份健康表:用例用它做**故障注入**(喂坏观测把某盘判成 `UNAVAILABLE`)。
        health: crate::disk_health::DiskHealthMap,
        /// 故障注入时喂观测要用的指标句柄(与池同一个 registry)。
        metrics: StoreServerMetrics,
        _dirs: Vec<tempfile::TempDir>,
    }

    fn fixture(n: usize, cfg: &StoreServerConfig) -> Fixture {
        let dirs: Vec<tempfile::TempDir> = (0..n)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let ids: Vec<String> = dirs.iter().map(disk_id).collect();
        let paths: Vec<DataDir> = dirs
            .iter()
            .map(|d| DataDir::Plain(d.path().to_string_lossy().into_owned()))
            .collect();
        let dm = DiskManager::mount(&paths).expect("mount");
        // 盘声明必须与实际挂载一致(池大小就是从 `data_dirs` 的每盘参数里读的)。
        let mut config = cfg.clone();
        config.data_dirs = paths;
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        let health = crate::disk_health::new_map();
        let pool = Arc::new(DiskPools::new(&dm, &config, &metrics, health.clone()).expect("pools"));
        Fixture {
            pool,
            registry,
            ids,
            health,
            metrics,
            _dirs: dirs,
        }
    }

    /// **故障注入**:把该盘立刻判成 `UNAVAILABLE`(喂 1 次坏观测,迟滞阈值取 1 ⇒ 一次到位)。
    fn mark_unavailable(f: &Fixture, disk_id: &str) {
        let h = crate::disk_health::Hysteresis {
            bad_rounds: 1,
            bad_secs: 1,
            good_rounds: 1,
            recover_stable_secs: 0,
        };
        crate::disk_health::observe_disk(
            &f.health,
            &f.metrics,
            disk_id,
            crate::health::DiskHealth::WriteTimeout,
            &h,
        );
        assert!(!crate::disk_health::is_available(&f.health, disk_id));
    }

    /// 故障恢复:喂 1 次健康观测(同上一套阈值)⇒ 立刻恢复可服务。
    fn mark_available(f: &Fixture, disk_id: &str) {
        let h = crate::disk_health::Hysteresis {
            bad_rounds: 1,
            bad_secs: 1,
            good_rounds: 1,
            recover_stable_secs: 0,
        };
        crate::disk_health::observe_disk(
            &f.health,
            &f.metrics,
            disk_id,
            crate::health::DiskHealth::Healthy,
            &h,
        );
        assert!(crate::disk_health::is_available(&f.health, disk_id));
    }

    fn cfg(workers_total: u32, queue: u32, timeout_ms: u64) -> StoreServerConfig {
        StoreServerConfig {
            pool_default_size: workers_total,
            pool_queue_size: queue,
            pool_acquire_timeout_ms: timeout_ms,
            ..Default::default()
        }
    }

    fn disk_id(dir: &tempfile::TempDir) -> String {
        dir.path()
            .file_name()
            .expect("tempdir has a name")
            .to_string_lossy()
            .into_owned()
    }

    fn metrics_text(f: &Fixture) -> String {
        antfs_metric::gather(&f.registry).expect("gather")
    }

    /// 指标里某条序列的值(读不到 → panic,让用例**当场红**而不是静默通过)。
    fn series(f: &Fixture, line: &str) -> String {
        let text = metrics_text(f);
        assert!(text.contains(line), "指标缺 `{line}`:\\n{text}");
        line.to_string()
    }

    /// 有界等待:等到 `cond` 为真;超时就 panic。
    ///
    /// 为什么必须有它:这是**地基件**,用例坏掉的形态多半是"永远等下去" —— 挂死会拖着整个 CI 一起卡,
    /// 显式超时才能把问题当场钉在报告里。它不是同步手段(同步仍靠 channel/计数),只是失败上限。
    async fn wait_until(what: &str, cond: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("等待超时(10s):{what}");
    }

    /// 跑一发任务,占住 worker 直到 `release` 被 send;`started` 在**已被 worker 取走执行**时发出。
    fn hold_job(
        started: Option<oneshot::Sender<()>>,
        release: oneshot::Receiver<()>,
    ) -> impl FnOnce() + Send + 'static {
        move || {
            if let Some(tx) = started {
                let _ = tx.send(());
            }
            let _ = release.blocking_recv();
        }
    }

    /// 结果与计数闭环:任务跑在**池线程**上(不是调用方线程),线程名带盘 id,收尾后计数归 0。
    #[tokio::test]
    async fn runs_on_a_pool_thread_and_closes_the_loop() {
        let f = fixture(1, &cfg(1, 4, 200));
        let id = f.ids[0].clone();
        let caller = std::thread::current().id();
        let out = f
            .pool
            .run(&id, "get", move || {
                (
                    std::thread::current().id(),
                    std::thread::current()
                        .name()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .await
            .expect("准入");
        assert_ne!(out.0, caller, "任务必须跑在池线程上,不是调用方线程");
        assert!(out.1.starts_with("antfs-disk-"), "线程名带盘 id:{}", out.1);
        assert_eq!(f.pool.worker_count(&id), Some(1));
        series(
            &f,
            &format!(r#"store_server_disk_pool_workers{{disk_id="{id}"}} 1"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#),
        );
    }

    /// worker 是**并行**的:N 个 worker 能同时跑 N 发(不是被一把锁串起来)。
    ///
    /// 这条直接盯"吞吐被静默腰斩"这类退化:若实现改成"持锁执行",这里最多只能等到 1 发 started。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn workers_run_tasks_in_parallel() {
        let workers = 4u32;
        let f = fixture(1, &cfg(workers, 0, 0));
        let id = f.ids[0].clone();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

        let mut releases = Vec::new();
        for i in 0..workers {
            let (release_tx, release_rx) = oneshot::channel::<()>();
            releases.push(release_tx);
            let p = f.pool.clone();
            let id = id.clone();
            let started = started_tx.clone();
            tokio::spawn(async move {
                p.run(&id, "get", move || {
                    let _ = started.send(i);
                    let _ = release_rx.blocking_recv();
                })
                .await
                .expect("准入")
            });
        }
        // 4 发必须**全部**开跑;少一发就说明并行度没兑现。
        for i in 0..workers {
            tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("第 {i} 发没能同时开跑(并行度不足)"))
                .expect("started");
        }
        series(
            &f,
            &format!(r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} {workers}"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#),
        );
        for tx in releases {
            tx.send(()).unwrap();
        }
        wait_until("4 发都收尾", || {
            metrics_text(&f).contains(&format!(
                r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#
            ))
        })
        .await;
    }

    /// 容量**恰好**是 workers + queue:在飞计数在运行期就是那个值,多出来的那发被拒,且被拒**不占容量**。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn capacity_is_exactly_workers_plus_queue_and_rejection_leaks_nothing() {
        let (workers, queue) = (2u32, 3u32);
        let f = fixture(1, &cfg(workers, queue, 0));
        let id = f.ids[0].clone();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel::<u8>();
        let mut releases = Vec::new();
        let mut jobs = Vec::new();
        for _ in 0..(workers + queue) {
            let (release_tx, release_rx) = oneshot::channel::<()>();
            releases.push(release_tx);
            let p = f.pool.clone();
            let id = id.clone();
            let started = started_tx.clone();
            jobs.push(tokio::spawn(async move {
                p.run(&id, "put", move || {
                    let _ = started.send(0u8);
                    let _ = release_rx.blocking_recv();
                })
                .await
            }));
        }
        // 只有 workers 发能开跑(其余在队列里)。
        for _ in 0..workers {
            tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
                .await
                .expect("started 不超时")
                .expect("started");
        }
        // 等全部 `workers + queue` 发都真的入队(计数在锁内递增),再断言精确值。
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("全部入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_in_flight{{disk_id="{id_wait}"}} 5"#
            ))
        })
        .await;
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{id}"}} 3"#),
        );

        // 第 6 发:worker 与队列都满 ⇒ 立刻被拒(准入超时 0)。
        match f.pool.run(&id, "put", || ()).await {
            Err(PoolReject::Saturated) => {}
            other => panic!("超出 workers+queue 应被拒,实际 {other:?}"),
        }
        series(
            &f,
            &format!(
                r#"store_server_disk_pool_rejected_total{{disk_id="{id}",op="put",reason="saturated"}} 1"#
            ),
        );

        // 放行全部:被拒那发**没有**吃掉许可,5 发都应正常完成。
        for tx in releases {
            tx.send(()).unwrap();
        }
        for (i, job) in jobs.into_iter().enumerate() {
            job.await
                .unwrap_or_else(|e| panic!("第 {i} 发 join 失败:{e}"))
                .unwrap_or_else(|e| panic!("第 {i} 发应成功,实际 {e:?}"));
        }
        wait_until("计数归零", || {
            let t = metrics_text(&f);
            t.contains(&format!(
                r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#
            )) && t.contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#
            ))
        })
        .await;
        // 拒过之后仍能正常干活(许可没漏)。
        f.pool
            .run(&id, "put", || 1u8)
            .await
            .expect("拒过之后仍可提交");
    }

    /// 队列是 **FIFO**:先提交的先执行(同一块盘上的顺序可预期)。
    ///
    /// **同步方式很关键**:多线程运行时下"spawn 之后 yield 一次"**不保证**那一发已经入队(任务可能在
    /// 另一个 worker 上还没跑到)—— 那样测出来的是"提交顺序"而不是队列顺序,会假红/假绿。这里改用
    /// **契约本身**:计数是在**锁内、入队之后**递增的(见 `run`),所以"观测到 in_flight == i+1"严格
    /// 蕴含"第 i 发已在队列里"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_is_fifo() {
        let f = fixture(1, &cfg(1, 32, 5_000));
        let id = f.ids[0].clone();
        let order: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        // 一发"占住 worker"的任务,保证后面 5 发都排在队列里。
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id_hold = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id_hold, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("占住 worker");

        // 依次提交 0..5:每提交一发都**等到它真的入队**(计数 +1)再提交下一发。
        let mut jobs = Vec::new();
        for i in 0..5u8 {
            let p = f.pool.clone();
            let id_used = id.clone();
            let order = order.clone();
            jobs.push(tokio::spawn(async move {
                p.run(&id_used, "put", move || {
                    order.lock().expect("order").push(i)
                })
                .await
            }));
            let want = i as i64 + 2; // hold 占 1,再加已提交的 i+1 发
            let id_wait = id.clone();
            let f_ref = &f;
            wait_until("该发已入队", || {
                metrics_text(f_ref).contains(&format!(
                    r#"store_server_disk_pool_in_flight{{disk_id="{id_wait}"}} {want}"#
                ))
            })
            .await;
        }
        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("hold");
        for job in jobs {
            job.await.expect("join").expect("queued");
        }
        assert_eq!(
            order.lock().expect("order").as_slice(),
            &[0u8, 1, 2, 3, 4],
            "队列必须 FIFO"
        );
    }

    /// **任务 panic 不能打死 worker**(那等于这块盘永久少一份并发),也不能让记账漂高或漏许可。
    #[tokio::test]
    async fn panicking_job_neither_kills_the_worker_nor_drifts_the_counts() {
        let f = fixture(1, &cfg(1, 0, 0));
        let id = f.ids[0].clone();
        match f.pool.run(&id, "put", || panic!("engine blew up")).await {
            Err(PoolReject::JobPanicked) => {}
            other => panic!("任务 panic 应报 JobPanicked,实际 {other:?}"),
        }
        series(
            &f,
            &format!(
                r#"store_server_disk_pool_rejected_total{{disk_id="{id}",op="put",reason="job_panicked"}} 1"#
            ),
        );
        // 计数必须收回来(否则面板上这块盘永远"还在跑")。
        series(
            &f,
            &format!(r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#),
        );
        // worker 还活着:紧接着的任务照常跑(若线程被 panic 带走,这里会 Saturated)。
        assert_eq!(
            f.pool
                .run(&id, "put", || 42u8)
                .await
                .expect("worker 应存活"),
            42
        );
    }

    /// 高并发提交:结果一个不丢、不错位,许可不多不少(收尾后计数归 0)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_concurrent_submitters_lose_nothing() {
        let f = fixture(1, &cfg(8, 512, 5_000));
        let id = f.ids[0].clone();
        let mut jobs = Vec::new();
        for i in 0..200u32 {
            let p = f.pool.clone();
            let id = id.clone();
            jobs.push(tokio::spawn(
                async move { p.run(&id, "get", move || i).await },
            ));
        }
        let mut got = Vec::new();
        for (i, job) in jobs.into_iter().enumerate() {
            let v = tokio::time::timeout(Duration::from_secs(30), job)
                .await
                .unwrap_or_else(|_| panic!("第 {i} 发超时未完成"))
                .expect("join")
                .expect("准入");
            got.push(v);
        }
        got.sort_unstable();
        assert_eq!(
            got,
            (0..200u32).collect::<Vec<_>>(),
            "结果必须一个不丢、不错位"
        );
        wait_until("计数归零", || {
            let t = metrics_text(&f);
            t.contains(&format!(
                r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#
            )) && t.contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#
            ))
        })
        .await;
    }

    /// 一块盘被压满(worker 卡住)**不得影响另一块盘**,且不许把请求挤到别的盘上去。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_saturated_disk_does_not_block_another_disk() {
        // 每盘**各自** 1 worker(注意 `pool_default_size` 是每盘的值)、无排队、不等:
        // 第一发占住 A 的唯一线程后,A 上的第二发必须**立刻**被拒。
        let f = fixture(2, &cfg(1, 0, 0));
        let (ida, idb) = (f.ids[0].clone(), f.ids[1].clone());
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let p = f.pool.clone();
        let ida2 = ida.clone();
        let hold = tokio::spawn(async move {
            p.run(&ida2, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("A 的任务应已开跑");

        match f.pool.run(&ida, "put", || ()).await {
            Err(PoolReject::Saturated) => {}
            other => panic!("A 满时应为 Saturated,实际 {other:?}"),
        }
        series(
            &f,
            &format!(
                r#"store_server_disk_pool_rejected_total{{disk_id="{ida}",op="put",reason="saturated"}} 1"#
            ),
        );

        // B 照常干活(连干几发),且 B 的队列始终是空的 —— 请求没有被挪到 B 上。
        for want in 0..3u8 {
            assert_eq!(
                f.pool
                    .run(&idb, "get", move || want)
                    .await
                    .expect("B 不受 A 影响"),
                want
            );
        }
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{idb}"}} 0"#),
        );

        release_tx.send(()).unwrap();
        assert!(hold.await.expect("join").is_ok(), "A 的任务应正常完成");
    }

    /// 短暂排队**能等到**:worker 全忙时,有排队位的请求进队列等着,worker 腾出来就跑。
    #[tokio::test]
    async fn queue_absorbs_a_burst_when_worker_frees_up() {
        let f = fixture(1, &cfg(1, 1, 5_000));
        let id = f.ids[0].clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let p = f.pool.clone();
        let id_hold = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id_hold, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("占住 worker");

        let p2 = f.pool.clone();
        let id2 = id.clone();
        let queued = tokio::spawn(async move { p2.run(&id2, "put", || 1u8).await });
        // 等它**真的入队**(计数在锁内递增)再放行 worker,否则测不出"排队"这件事。
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("第二发已入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id_wait}"}} 1"#
            ))
        })
        .await;

        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("hold 应完成");
        assert_eq!(queued.await.expect("join").expect("排队的那发应成功"), 1);
    }

    /// 排队**满且等到超时**才拒:队列容量 0 时,worker 一忙,下一发只能等 —— 到点即 `Saturated`。
    #[tokio::test]
    async fn queue_full_waits_then_rejects() {
        let f = fixture(1, &cfg(1, 0, 250));
        let id = f.ids[0].clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let p = f.pool.clone();
        let id_hold = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id_hold, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("占住 worker");

        let started_at = std::time::Instant::now();
        match f.pool.run(&id, "put", || 2u8).await {
            Err(PoolReject::Saturated) => {}
            other => panic!("满且等到超时应为 Saturated,实际 {other:?}"),
        }
        assert!(
            started_at.elapsed() >= Duration::from_millis(200),
            "应当是**等过**才拒(短暂排队),不是立刻拒:实际 {:?}",
            started_at.elapsed()
        );

        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("hold 应完成");
    }

    /// 未知盘 = 明确失败(不静默落到别的盘,也不落共享池)。
    #[tokio::test]
    async fn unknown_disk_is_rejected() {
        let f = fixture(1, &cfg(1, 1, 0));
        match f.pool.run("no-such-disk", "get", || ()).await {
            Err(PoolReject::UnknownDisk) => {}
            other => panic!("未知盘应为 UnknownDisk,实际 {other:?}"),
        }
        series(
            &f,
            r#"store_server_disk_pool_rejected_total{disk_id="no-such-disk",op="get",reason="unknown_disk"} 1"#,
        );
    }

    /// 池关停(store-server 析构那一瞬):排队中的任务立刻拿到明确失败,而不是永远吊着。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn closing_a_pool_fails_queued_waiters() {
        let f = fixture(1, &cfg(1, 1, 5_000));
        let id = f.ids[0].clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let p = f.pool.clone();
        let id_hold = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id_hold, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("占住 worker");

        let p2 = f.pool.clone();
        let id2 = id.clone();
        let queued = tokio::spawn(async move { p2.run(&id2, "put", || 9u8).await });
        // **等它真的进队列**再关(不要用 `yield_now` 猜调度:任务没入队就清队列,这一发就会在
        // 关停之后才入队并永远等下去 —— 那是测试自己的竞态,不是被测代码的问题)。
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("第二发已入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id_wait}"}} 1"#
            ))
        })
        .await;

        // 手工走一遍 `Drop for DiskPool` 的动作(测试里提交方自己持 `Arc<DiskPools>`,真 drop 要等它们
        // 退出 —— 那样测的反而是循环等待)。**顺序必须与 Drop 一致:先置关停再清队列**——清队列会
        // 唤醒排队等待方,它按"此刻 `closed` 是否已置位"归因;顺序反了,唤醒方会在 `closed` 置位前
        // 读到 false,把关停误报成 `JobPanicked`(ubuntu CI 上踩中过)。
        {
            let pool = f.pool.pools.get(&id).expect("pool exists").clone();
            pool.inner.close();
            pool.inner.queue.lock().expect("queue").clear();
        }

        match queued.await.expect("join") {
            Err(PoolReject::Closed) => {}
            other => panic!("池关停后排队任务应 Closed,实际 {other:?}"),
        }
        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("已开跑的那发不受关停影响");
    }

    /// 每盘池大小:该盘显式 `pool_size` 优先,没给就用 `pool_default_size`(不再有"总预算均分")。
    #[test]
    fn worker_allocation_prefers_per_disk_size_then_default() {
        let dirs: Vec<tempfile::TempDir> = (0..3)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let ids: Vec<String> = dirs.iter().map(disk_id).collect();
        let specs: Vec<DataDir> = dirs
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let dir = d.path().to_string_lossy().into_owned();
                if i == 0 {
                    DataDir::Spec(DiskSpec {
                        dir,
                        pool_size: Some(64),
                    })
                } else {
                    DataDir::Plain(dir)
                }
            })
            .collect();
        let paths: Vec<DataDir> = dirs
            .iter()
            .map(|d| DataDir::Plain(d.path().to_string_lossy().into_owned()))
            .collect();
        let dm = DiskManager::mount(&paths).expect("mount");

        let sizes = allocate_workers(&dm, 8, &specs).expect("allocate");
        assert_eq!(sizes.get(&ids[0]).copied(), Some(64), "显式指定优先");
        assert_eq!(sizes.get(&ids[1]).copied(), Some(8), "没给的用每盘默认");
        assert_eq!(sizes.get(&ids[2]).copied(), Some(8));

        // 声明与实际挂载对不上(理论上装不出来):如实报错而不是静默给默认值。
        let mismatched = vec![DataDir::Plain("/not/a/mounted/dir".to_string())];
        assert!(allocate_workers(&dm, 8, &mismatched).is_err());
    }

    /// 反例保护:队列满时的**拒绝**必须是"等过才拒",不能退化成"永远排队"(那会把连接占死)。
    /// 这条同时守住上面 `queue_full_waits_then_rejects` 的上界:超时必须真的兑现。
    #[tokio::test]
    async fn rejection_is_bounded_not_unbounded_queueing() {
        let f = fixture(1, &cfg(1, 0, 0));
        let id = f.ids[0].clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id_hold = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id_hold, "put", hold_job(Some(started_tx), release_rx))
                .await
        });
        started_rx.await.expect("占住 worker");
        // 准入超时 0 ⇒ 立刻拒(不是排队)。
        let t0 = std::time::Instant::now();
        assert!(matches!(
            f.pool.run(&id, "put", || ()).await,
            Err(PoolReject::Saturated)
        ));
        assert!(t0.elapsed() < Duration::from_millis(100), "应当是立刻拒");
        release_tx.send(()).unwrap();
        hold.await.expect("join").expect("hold");
    }

    // ── 4.3:盘被判坏之后,队列里那批"进队时盘还好"的请求要被**丢掉**,而不是去碰一块已知不会好的盘 ──

    /// 排队中的任务在**开跑前**发现该盘不可服务 ⇒ 被丢掉,**连 `f()` 都不执行**。
    ///
    /// 这条是 4.3 的核心:盘 hang 住时,worker 全卡在 syscall 里,队列里那批会**永远等下去**;
    /// 而它们等的是一个我们**已经知道不会好**的盘。丢掉它们,把"排队等死"换成"立刻 503"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_queued_job_is_dropped_when_its_disk_goes_unavailable() {
        let f = fixture(1, &cfg(1, 2, 5_000));
        let id = f.ids[0].clone();
        // 占住唯一 worker(这一发**在跑**,不会被丢)。
        let (s1, r1) = oneshot::channel::<()>();
        let (rel1, rx1) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id1 = id.clone();
        let running =
            tokio::spawn(async move { p.run(&id1, "put", hold_job(Some(s1), rx1)).await });
        r1.await.expect("第一发已开跑");

        // 第二发入队(此时盘还是好的)。
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = ran.clone();
        let p2 = f.pool.clone();
        let id2 = id.clone();
        let queued = tokio::spawn(async move {
            p2.run(&id2, "get", move || {
                ran2.store(true, std::sync::atomic::Ordering::SeqCst);
                7u8
            })
            .await
        });
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("第二发已入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id_wait}"}} 1"#
            ))
        })
        .await;

        // 盘被判坏(故障注入走与生产同一条迟滞路径),然后放行 worker。
        mark_unavailable(&f, &id);
        rel1.send(()).unwrap();
        running.await.expect("join").expect("第一发应正常完成");

        match queued.await.expect("join") {
            Err(PoolReject::DiskUnavailable) => {}
            other => panic!("坏盘上排队的那发应报 DiskUnavailable,实际 {other:?}"),
        }
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "被丢掉的那发**绝不能**碰盘(否则就是往一块已知不会好的盘上再塞一次 IO)"
        );
        series(
            &f,
            &format!(
                r#"store_server_disk_pool_rejected_total{{disk_id="{id}",op="get",reason="disk_unavailable"}} 1"#
            ),
        );
        // 许可与计数照常收回(丢掉不等于漏账)。
        series(
            &f,
            &format!(r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_queued{{disk_id="{id}"}} 0"#),
        );
    }

    /// **已经开跑**的那发不受影响:它已经进了 syscall,取消不掉(Rust 既不能取消 blocking 任务,
    /// 也不能 kill 线程)—— 这是硬边界,不假装做到了。丢掉只对**排队/后续**成立。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_already_running_job_is_not_killed_by_its_disk_going_unavailable() {
        let f = fixture(1, &cfg(1, 0, 0));
        let id = f.ids[0].clone();
        let (s1, r1) = oneshot::channel::<()>();
        let (rel1, rx1) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id1 = id.clone();
        let running = tokio::spawn(async move {
            p.run(&id1, "put", move || {
                let _ = s1.send(());
                let _ = rx1.blocking_recv();
                5u8
            })
            .await
        });
        r1.await.expect("已开跑");
        mark_unavailable(&f, &id);
        rel1.send(()).unwrap();
        assert_eq!(
            running
                .await
                .expect("join")
                .expect("已开跑的那发应照常完成"),
            5,
            "已经在 syscall 里的那一发无法取消 —— 只能是这个结果"
        );
    }

    /// **恢复后立刻正常服务**:状态是执行前**实时查**的,没有"标志位同步"那一拍的误拒窗口。
    ///
    /// (第一版设计想用"状态变化时置标志位",那样盘恢复后的一拍里所有任务都会被误拒 —— 这是
    /// 选"实时查"最实际的理由。)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn service_resumes_immediately_after_recovery() {
        let f = fixture(1, &cfg(1, 1, 0));
        let id = f.ids[0].clone();
        assert_eq!(f.pool.run(&id, "get", || 1u8).await.expect("正常"), 1);

        mark_unavailable(&f, &id);
        assert!(matches!(
            f.pool.run(&id, "get", || 2u8).await,
            Err(PoolReject::DiskUnavailable)
        ));

        mark_available(&f, &id);
        assert_eq!(
            f.pool
                .run(&id, "get", || 3u8)
                .await
                .expect("恢复后应立刻能服务"),
            3,
            "盘一恢复就该照常跑,不该再等一个同步周期"
        );
    }

    /// **panic 的归因不被抢走**:即便此刻该盘正被判定不可服务,一个**真的炸了**的任务仍要报
    /// `job_panicked`(500 + `join_error`),而不是被笼统归成 `disk_unavailable`(503,可以重试。
    /// 那会把"引擎有 bug"误导成"盘忙,换一个副本就行")。
    ///
    /// 这条正是"通道送 `Result<T, PoolReject>`"这个设计的收益 —— 若改用池级标志位猜原因,
    /// 这里就会误报。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_job_still_reports_job_panicked_even_when_the_disk_is_unavailable() {
        let f = fixture(1, &cfg(1, 1, 0));
        let id = f.ids[0].clone();
        // 盘此刻不可服务,但这里的关键是"任务**跑了**并炸了" ⇒ 不能归成"没跑就被丢"。
        // 用一个在跑的任务把 worker 占住,再让故障注入发生在它开跑之后。
        let (s1, r1) = oneshot::channel::<()>();
        let (rel1, rx1) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id1 = id.clone();
        let hold = tokio::spawn(async move {
            p.run(&id1, "get", move || {
                let _ = s1.send(());
                let _ = rx1.blocking_recv();
            })
            .await
        });
        r1.await.expect("占住 worker");
        mark_unavailable(&f, &id);

        // 注意**不能在这里直接 await**:worker 被占住 ⇒ 这一发只能排队,而排队的那发要等 worker
        // 腾出来才会被 worker 取走判死;直接 await 就是自己把自己锁死(第一版正是这么写的)。
        let p2 = f.pool.clone();
        let id2 = id.clone();
        let queued = tokio::spawn(async move { p2.run(&id2, "get", || panic!("boom")).await });
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("那一发已入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id_wait}"}} 1"#
            ))
        })
        .await;

        // 放行占位任务 ⇒ worker 取出排队的那发;**盘仍不可服务** ⇒ 它被丢掉,**不执行**(不 panic)。
        rel1.send(()).unwrap();
        hold.await.expect("join").expect("占位任务");
        match queued.await.expect("join") {
            Err(PoolReject::DiskUnavailable) => {}
            other => panic!("没跑就被丢的应报 DiskUnavailable(而不是 panic),实际 {other:?}"),
        }

        // 盘恢复后再炸一次:这次任务**真的跑了**,必须如实报 panic ——
        // 这一条就是"通道送 `Result<T, PoolReject>`"相对"池级标志位猜原因"的收益。
        mark_available(&f, &id);
        match f.pool.run(&id, "get", || panic!("boom")).await {
            Err(PoolReject::JobPanicked) => {}
            other => panic!("真的炸了必须报 JobPanicked,实际 {other:?}"),
        }
    }

    // ── DEGRADED 的数据源:每盘"慢 IO / 总 IO"增量计数 ─────────────────────────────────────

    /// 慢 IO 的分类计数必须**精确**,且 `take_io_stats` 是"读走并清零"(每发 IO 只被一轮统计)。
    ///
    /// 为什么不用直方图差分:直方图只有桶,而"慢 IO 占比"要的是**精确的分子分母**;
    /// 且"读走清零"能保证相邻两轮不会把同一批样本各算一遍(那会让占比偏大)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_io_counters_are_exact_and_reset_on_read() {
        let mut c = cfg(2, 0, 5_000);
        // 阈值 100ms、慢 IO 睡 500ms:"慢"这一侧留 5 倍余量;而"不慢"那一侧是**空闭包**,
        // 要被算成慢需要一次 >100ms 的调度停顿。旧写法阈值 1ms + 空跑,CI runner 抖动一下就会把
        // 空跑也计成慢,断言 (2, 3) 会变成 (3, 3) —— 用例不能依赖机器速度。
        c.disk_degraded_io_ms = 100;
        let f = fixture(1, &c);
        let id = f.ids[0].clone();

        // 关着阈值时计数器不动(默认部署零开销)。
        let mut off = cfg(2, 0, 5_000);
        off.disk_degraded_io_ms = 0;
        let f_off = fixture(1, &off);
        let id_off = f_off.ids[0].clone();
        f_off
            .pool
            .run(&id_off, "get", || {
                std::thread::sleep(Duration::from_millis(5))
            })
            .await
            .expect("准入");
        assert_eq!(
            f_off.pool.take_io_stats(&id_off),
            Some((0, 0)),
            "阈值 0 = 不分类:计数器一个都不该动"
        );

        // 两发慢 IO(500ms,远超 100ms 的阈值)+ 一发瞬时 IO(空闭包)。
        for _ in 0..2 {
            f.pool
                .run(&id, "put", || {
                    std::thread::sleep(Duration::from_millis(500))
                })
                .await
                .expect("准入");
        }
        f.pool.run(&id, "get", || ()).await.expect("准入");

        assert_eq!(
            f.pool.take_io_stats(&id),
            Some((2, 3)),
            "慢 2 / 总 3:分子分母都要精确"
        );
        assert_eq!(
            f.pool.take_io_stats(&id),
            Some((0, 0)),
            "读走即清零 —— 同一批样本绝不能被下一轮再算一遍"
        );
        // 未知盘:明确 None(而不是恐慌或造一个假的 0)。
        assert_eq!(f.pool.take_io_stats("no-such-disk"), None);
    }

    /// **DEGRADED 全链路**:真跑几发慢 IO → 增量计数 → 看门狗读走算占比 → 延迟迟滞机 → 上报 `DEGRADED`。
    ///
    /// 关键断言是**最后一条**:`DEGRADED` **不影响可服务性**(`is_available` 仍为真)——
    /// 慢盘的处置是"不再接新副本 + 让它可见",**不是** 503 摘盘。若哪天有人把它接到闸门上,
    /// 这条用例会红。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn watchdog_marks_a_consistently_slow_disk_degraded_without_disabling_it() {
        let mut c = cfg(2, 4, 5_000);
        // 阈值 50ms、慢 IO 睡 200ms:两侧都留 4 倍以上余量,不依赖机器速度(见本用例末尾的说明)。
        c.disk_degraded_io_ms = 50;
        let f = fixture(1, &c);
        let id = f.ids[0].clone();

        // 看门狗:**独立任务**,tick 取最小门槛(这里给 10ms 便于观察);
        // 判定函数"本轮有 IO 且至少一半是慢的" ⇒ 一轮即劣化(迟滞阈值取 1)。
        let pools = f.pool.clone();
        let statuses = f.health.clone();
        let metrics = Arc::new(f.metrics.clone());
        let judge: Arc<dyn Fn(u64, u64) -> Option<bool> + Send + Sync> =
            Arc::new(|slow: u64, total: u64| (total > 0).then(|| slow * 2 >= total));
        crate::disk_health::spawn_watchdog(
            pools,
            statuses.clone(),
            crate::disk_health::Hysteresis {
                bad_rounds: 1,
                bad_secs: 1,
                good_rounds: 1,
                recover_stable_secs: 0,
            },
            crate::disk_health::WatchdogSettings {
                // tick 取**所有门槛的最小值**:保持 10ms 的快观察周期。
                default_deadline: Duration::from_millis(10),
                // 但本用例只跑 `put`,把它的门槛放到 60s —— 否则 CI runner 上一次调度抖动就能把
                // 200ms 的"慢 IO"量成"超期 IO",迟滞(bad_rounds = 1)立刻判 UNAVAILABLE,下一发
                // 准入被拒 `DiskUnavailable`(实测在 GitHub runner 上就是这么红的)。DEGRADED 的判定
                // 走池里的**增量计数**(阈值 `disk_degraded_io_ms`),与这个门槛无关。
                by_op_ms: BTreeMap::from([("put".to_string(), 60_000)]),
                escalate_secs: 0,
                degraded: Some(judge),
            },
            metrics,
        );

        // 跑几发慢 IO(每发 200ms,4 倍于 50ms 的阈值)。
        for _ in 0..3 {
            f.pool
                .run(&id, "put", || {
                    std::thread::sleep(Duration::from_millis(200))
                })
                .await
                .expect("准入");
        }

        let health = f.health.clone();
        let id_wait = id.clone();
        wait_until("该盘被判 DEGRADED", || {
            crate::disk_health::is_degraded(&health, &id_wait)
        })
        .await;

        assert_eq!(
            crate::disk_health::reported_state(&f.health, &id),
            "DEGRADED",
            "上报值应变成 DEGRADED(master 据此不再放新副本)"
        );
        assert!(
            crate::disk_health::is_available(&f.health, &id),
            "**慢盘仍然服务** —— DEGRADED 绝不能变成 503(否则慢盘=被摘,只会更动荡)"
        );
        // 而且它照常干活。
        assert_eq!(f.pool.run(&id, "get", || 42u8).await.expect("准入"), 42);
    }

    /// 池线程按盘分开、且线程名可辨认(排查线上"哪个盘的线程卡住了"就靠它)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_disk_owns_its_own_named_threads() {
        let f = fixture(2, &cfg(2, 0, 0));
        let mut names = Vec::new();
        for id in &f.ids {
            let p = f.pool.clone();
            let id = id.clone();
            names.push(
                p.run(&id, "get", move || {
                    std::thread::current()
                        .name()
                        .unwrap_or_default()
                        .to_string()
                })
                .await
                .expect("准入"),
            );
        }
        assert!(
            names[0].starts_with(&format!("antfs-disk-{}-", f.ids[0])),
            "{}",
            names[0]
        );
        assert!(
            names[1].starts_with(&format!("antfs-disk-{}-", f.ids[1])),
            "{}",
            names[1]
        );
    }

    /// worker 线程必须**随池析构退出**:反复建销 store-server 若攒线程,测试/CI 与线上滚动重启都会烂。
    ///
    /// 只统计**本用例那几块盘**的线程(cargo 并行跑其它用例时全局计数不可比)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_threads_exit_when_pools_are_dropped() {
        let dirs: Vec<tempfile::TempDir> = (0..5)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let disk_ids: Vec<String> = dirs.iter().map(disk_id).collect();
        let paths: Vec<DataDir> = dirs
            .iter()
            .map(|d| DataDir::Plain(d.path().to_string_lossy().into_owned()))
            .collect();
        let dm = DiskManager::mount(&paths).expect("mount");
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).expect("metrics");
        let mut config = cfg(3, 1, 10);
        config.data_dirs = paths;

        for round in 0..3 {
            let pools = Arc::new(
                DiskPools::new(&dm, &config, &metrics, crate::disk_health::new_map())
                    .expect("pools"),
            );
            // 真跑一发,确保 worker 确实起来了。
            let id = dm.disks()[0].id.clone();
            pools.run(&id, "get", || 1u8).await.expect("准入");
            // worker 线程是**逐个 spawn** 起来的,`DiskPools::new` 返回时未必都跑到了登记点 ⇒ 有界等待。
            let ids_now = disk_ids.clone();
            wait_until("本用例的 worker 全部起来", || {
                ids_now.iter().map(|d| live_workers(d)).sum::<usize>() == 5 * 3
            })
            .await;
            let live: usize = disk_ids.iter().map(|d| live_workers(d)).sum();
            assert_eq!(
                live,
                5 * 3,
                "第 {round} 轮:5 盘 × 3 worker 都该活着,实际 {live}"
            );
            drop(pools);
            // 析构后本用例的 worker 应自行退出(有界等待,坏了一律当场红)。
            let mut left = usize::MAX;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while tokio::time::Instant::now() < deadline {
                left = disk_ids.iter().map(|d| live_workers(d)).sum();
                if left == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(
                left, 0,
                "第 {round} 轮:析构后 worker 线程应全部退出,仍剩 {left}"
            );
        }
    }

    /// 反复 panic 不能把池耗弱:50 发全炸之后,worker 数不变、后续照常服务、计数归零。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_panics_do_not_degrade_the_pool() {
        let f = fixture(1, &cfg(2, 4, 100));
        let id = f.ids[0].clone();
        for _ in 0..50 {
            match f.pool.run(&id, "put", || panic!("boom")).await {
                Err(PoolReject::JobPanicked) => {}
                other => panic!("应报 JobPanicked,实际 {other:?}"),
            }
        }
        series(
            &f,
            &format!(r#"store_server_disk_pool_workers{{disk_id="{id}"}} 2"#),
        );
        series(
            &f,
            &format!(r#"store_server_disk_pool_in_flight{{disk_id="{id}"}} 0"#),
        );
        series(
            &f,
            &format!(
                r#"store_server_disk_pool_rejected_total{{disk_id="{id}",op="put",reason="job_panicked"}} 50"#
            ),
        );
        // 并发度没被吃掉:两发能同时跑起来。
        let (t1, r1) = oneshot::channel::<()>();
        let (_rel1, rx1) = oneshot::channel::<()>();
        let (t2, r2) = oneshot::channel::<()>();
        let (rel2, rx2) = oneshot::channel::<()>();
        let (p1, p2) = (f.pool.clone(), f.pool.clone());
        let (id1, id2) = (id.clone(), id.clone());
        let h1 = tokio::spawn(async move { p1.run(&id1, "get", hold_job(Some(t1), rx1)).await });
        let h2 = tokio::spawn(async move { p2.run(&id2, "get", hold_job(Some(t2), rx2)).await });
        for (i, r) in [r1, r2].into_iter().enumerate() {
            tokio::time::timeout(Duration::from_secs(10), r)
                .await
                .unwrap_or_else(|_| panic!("panic 之后第 {i} 发没能开跑(并发被吃掉?)"))
                .expect("started");
        }
        let _ = (_rel1.send(()), rel2.send(()));
        h1.await.expect("join").expect("h1");
        h2.await.expect("join").expect("h2");
    }

    /// **被拒不吃容量**:准入超时 0 时,"恰好多一发被拒"之后,容量仍是 workers(不多不少)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rejection_never_consumes_capacity() {
        let workers = 3u32;
        let f = fixture(1, &cfg(workers, 0, 0));
        let id = f.ids[0].clone();
        let mut releases = Vec::new();
        let mut jobs = Vec::new();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel::<u8>();
        for _ in 0..workers {
            let (rel_tx, rel_rx) = oneshot::channel::<()>();
            releases.push(rel_tx);
            let p = f.pool.clone();
            let id = id.clone();
            let started = started_tx.clone();
            jobs.push(tokio::spawn(async move {
                p.run(&id, "get", move || {
                    let _ = started.send(0u8);
                    let _ = rel_rx.blocking_recv();
                })
                .await
            }));
        }
        for _ in 0..workers {
            tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
                .await
                .expect("started 不超时")
                .expect("started");
        }
        // 连拒 10 次:一次都不该吃掉容量。
        for _ in 0..10 {
            assert!(matches!(
                f.pool.run(&id, "get", || ()).await,
                Err(PoolReject::Saturated)
            ));
        }
        for tx in releases {
            tx.send(()).unwrap();
        }
        for job in jobs {
            job.await.expect("join").expect("已准入的应完成");
        }
        // 容量仍是 workers:连开 workers 发都能立刻准入(第 workers+1 发才被拒)。
        let mut ok = 0;
        let mut rel2 = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..workers {
            let (tx, rx) = oneshot::channel::<()>();
            rel2.push(tx);
            let p = f.pool.clone();
            let id = id.clone();
            let h = tokio::spawn(async move { p.run(&id, "get", hold_job(None, rx)).await });
            // 让出一次,让它拿到许可入队(不等它跑完);任务**在跑/在等放行**就说明准入成功。
            tokio::task::yield_now().await;
            if h.is_finished() {
                panic!("第 {} 发本应立即准入却被拒", ok + 1);
            }
            // 句柄留着(任务仍在等放行);到用例结束随 `rel2` 的 sender 一起收尾。
            handles.push(h);
            ok += 1;
        }
        assert_eq!(ok, workers as i32, "容量应恰好是 workers");
        // 收尾:放行 + 等任务结束(不留悬挂任务)。
        for tx in rel2 {
            tx.send(()).unwrap();
        }
        for h in handles {
            h.await.expect("join").expect("已准入的应完成");
        }
    }

    /// 载荷是任意 `Send` 值(不是只有 Copy):字节缓冲这类"非 Copy"结果同样要能原样取回。
    #[tokio::test]
    async fn job_payload_can_be_non_copy() {
        let f = fixture(1, &cfg(1, 1, 10));
        let id = f.ids[0].clone();
        let out = f
            .pool
            .run(&id, "get", || vec![7u8; 4096])
            .await
            .expect("准入");
        assert_eq!(out.len(), 4096);
        assert!(out.iter().all(|b| *b == 7));
    }

    /// **准入之后不再设超时**:准入超时给 0,也不影响一次已经开跑的长时间操作(它只约束"等空位")。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_job_is_not_killed_by_the_acquire_timeout() {
        let f = fixture(1, &cfg(1, 1, 0));
        let id = f.ids[0].clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id_hold = id.clone();
        let job = tokio::spawn(async move {
            p.run(&id_hold, "get", move || {
                let _ = started_tx.send(());
                let _ = release_rx.blocking_recv();
                99u8
            })
            .await
        });
        started_rx.await.expect("已开跑");
        // 此刻它在"跑",而不是在"等空位" —— 准入超时(0)不该把它判掉。
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!job.is_finished(), "已准入的任务不该被超时判定打断");
        release_tx.send(()).unwrap();
        assert_eq!(job.await.expect("join").expect("应完成"), 99);
    }

    /// 配置兜底:每盘池大小给 0 时钳到 1(不让"0 个 worker"这种必然全线 503 的配置跑起来)。
    #[test]
    fn zero_per_disk_pool_size_is_clamped_to_one() {
        let dir = tempfile::tempdir().unwrap();
        let paths = vec![DataDir::Plain(dir.path().to_string_lossy().into_owned())];
        let dm = DiskManager::mount(&paths).expect("mount");
        let specs = vec![DataDir::spec(
            dir.path().to_string_lossy().into_owned(),
            Some(0),
        )];
        let sizes = allocate_workers(&dm, 1, &specs).expect("allocate");
        assert_eq!(sizes.values().copied().collect::<Vec<_>>(), vec![1]);
    }

    /// 未用到的导入在此显式消歧(避免"以为用了其实没用"的静默漂移)。
    #[allow(dead_code)]
    fn _channel_types_are_used() {
        let (_tx, _rx) = mpsc::channel::<()>();
    }

    // ── D3 看门狗的数据源:`IoWatch`(在途 IO 的开始时刻表)────────────────────────────────────
    //
    // 这几条是本 change 里**最容易写错**的地方(design §5.1):一旦计时点选在"提交时"而不是
    // "闭包内",排队就会被算成"盘慢",于是池一忙,健康盘就被看门狗判死。故这里既测数据结构本身,
    // 也测"排队真的不计入"这条端到端性质。

    /// **完成顺序 ≠ 开始顺序**:A 先开始、B 后开始、A 先结束 —— "最久在途"必须变成 B 的开始时刻。
    ///
    /// 若实现只维护一个"最早开始"标量,A 结束后它仍停在 B 之前的时刻,把健康盘的"最久在途"越算越大,
    /// 最后被看门狗误判。这条用例就是钉死那个反例。
    #[test]
    fn io_watch_tracks_the_oldest_start_across_out_of_order_completion() {
        let w = Arc::new(IoWatch::new());
        assert_eq!(w.oldest("d0", "put"), None, "空表 → 无在途");
        assert_eq!(w.in_flight("d0", "put"), 0);

        let a = w.begin("d0", "put");
        let start_a = w.oldest("d0", "put").expect("A 在途");
        std::thread::sleep(Duration::from_millis(2)); // 让两个开始时刻可区分(此处不用于同步判定)
        let b = w.begin("d0", "put");
        let start_b = w.oldest("d0", "put").expect("A/B 都在途");
        assert_eq!(start_b, start_a, "最早那一发仍是 A");
        assert_eq!(w.in_flight("d0", "put"), 2);
        assert_eq!(w.in_flight_total("d0"), 2, "合计口径");

        // A 先结束(后开始的 B 还在):最早开始必须**推进到 B**。
        drop(a);
        assert_eq!(w.in_flight("d0", "put"), 1);
        assert_eq!(
            w.oldest("d0", "put"),
            Some(b.start),
            "A 结束后'最久在途'应变成 B 的开始时刻,而不是停在 A 的"
        );
        drop(b);
        assert_eq!(w.oldest("d0", "put"), None, "全结束 → 无在途,不留空 entry");
        assert_eq!(w.in_flight("d0", "put"), 0);
    }

    /// **按 op 分开计**:`put` 的在途不会被算进 `compaction`,反之亦然。
    ///
    /// 这是"按 op 分档门槛"的前提:若两者混在一张表里,compaction 的长耗时会把读操作的最久在途
    /// 一起推高,给读操作设的更紧门槛就会误报。
    #[test]
    fn io_watch_keeps_ops_apart() {
        let w = Arc::new(IoWatch::new());
        let _read = w.begin("d0", "get");
        let _big = w.begin("d0", "compaction");
        assert_eq!(w.in_flight("d0", "get"), 1);
        assert_eq!(w.in_flight("d0", "compaction"), 1);
        assert_eq!(w.in_flight_total("d0"), 2);
        assert_eq!(
            w.ops_in_flight("d0"),
            vec!["compaction", "get"],
            "有在途的 op 列表(有序)"
        );
        // 未知 op:按"没有在途"处理,绝不 panic 在数据面路径上。
        assert_eq!(w.in_flight("d0", "typo_op"), 0);
        assert_eq!(w.oldest("d0", "typo_op"), None);
    }

    /// 同一块盘的多个盘外键互不影响;未知盘的查询不会 panic / 不会造出空 entry。
    #[test]
    fn io_watch_is_per_disk_and_read_only_queries_do_not_leak_entries() {
        let w = Arc::new(IoWatch::new());
        let _a = w.begin("d0", "get");
        assert_eq!(w.in_flight("d1", "get"), 0);
        assert_eq!(w.oldest("d1", "get"), None);
        assert_eq!(w.in_flight("d0", "get"), 1);
        assert!(
            !w.lock().keys().any(|(d, _)| d == "d1"),
            "只读查询不该在表里造出空 entry"
        );
    }

    /// **排队不计入在途 IO / 不计入时长**:worker 被占住时,第二发在队列里等 —— 它不该出现在
    /// `IoWatch` 里,"最久在途"也不该被它推高,且它最终记下的时长必须是**自己那次 syscall**,
    /// **不含**排队(这正是 design §5.1 要防的误判:把"池忙"读成"盘慢")。
    ///
    /// 判定方式(确定性,不靠 sleep 对表):让第一发在闭包里**按住**,直到第二发**确认入队**
    /// (`queued == 1`,计数在锁内递增)后再继续按住一个**下限时长**(50ms),然后才放行。
    /// 于是"第二发至少排了 50ms"是**构造出来**的事实;它记下的时长若 ≥ 50ms,就说明计时点错在
    /// 提交侧 —— 用例当场红。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queueing_is_excluded_from_the_io_watch_and_duration() {
        let f = fixture(1, &cfg(1, 1, 5_000));
        let id = f.ids[0].clone();
        let watch = f.pool.io_watch();
        let put_count =
            format!(r#"store_server_disk_io_duration_seconds_count{{disk_id="{id}",op="put"}}"#);
        let get_count =
            format!(r#"store_server_disk_io_duration_seconds_count{{disk_id="{id}",op="get"}}"#);

        // 第一发:占住唯一 worker(op=put),闭包里等测试放行。
        let (s1_tx, s1_rx) = oneshot::channel::<()>();
        let (rel1_tx, rel1_rx) = oneshot::channel::<()>();
        let p = f.pool.clone();
        let id1 = id.clone();
        let h1 =
            tokio::spawn(async move { p.run(&id1, "put", hold_job(Some(s1_tx), rel1_rx)).await });
        s1_rx.await.expect("worker 被第一发占住");
        let start1 = watch.oldest(&id, "put").expect("正在跑的那发已盖戳");
        assert_eq!(
            watch.in_flight(&id, "put"),
            1,
            "只有正在 syscall 的那一算在途"
        );

        // 第二发(op=get):入队等 worker 空出来。
        let p2 = f.pool.clone();
        let id2 = id.clone();
        let h2 = tokio::spawn(async move { p2.run(&id2, "get", || 1u8).await });
        let id_wait = id.clone();
        let f_ref = &f;
        wait_until("第二发已入队", || {
            metrics_text(f_ref).contains(&format!(
                r#"store_server_disk_pool_queued{{disk_id="{id_wait}"}} 1"#
            ))
        })
        .await;

        // 关键断言(排队中):一点都没进 IoWatch,也没有任何时长观测。
        assert_eq!(
            watch.in_flight(&id, "put"),
            1,
            "排队中的任务还没进 syscall,不该算在途(否则'池忙'会被读成'盘慢')"
        );
        assert_eq!(
            watch.oldest(&id, "put"),
            Some(start1),
            "排队不该推高'最久在途'"
        );
        let text = metrics_text(&f);
        assert!(
            !text.contains(&put_count)
                && !text.contains(&get_count)
                && !text.contains(&format!("{put_count} 0")),
            "排队期间不该有任何 io_duration 观测:\n{text}"
        );

        // 把"第二发至少排了 50ms"这件事**构造**出来(有界等待,超时即红;不是用 sleep 做同步)。
        let floor = Duration::from_millis(50);
        let t0 = std::time::Instant::now();
        while t0.elapsed() < floor {
            tokio::task::yield_now().await;
        }
        let queued_at_least = t0.elapsed();
        rel1_tx.send(()).unwrap();
        h1.await.expect("join").expect("第一发应完成");
        let got = h2.await.expect("join").expect("排队的那发应成功");
        assert_eq!(got, 1);

        // 第二发记下的时长必须 ≪ 它排队的时间;再把两发的样本数对上。
        let text = metrics_text(&f);
        assert!(
            text.contains(&format!("{put_count} 1")) && text.contains(&format!("{get_count} 1")),
            "两发各产生恰好一个时长样本:\n{text}"
        );
        let sum_line =
            format!(r#"store_server_disk_io_duration_seconds_sum{{disk_id="{id}",op="get"}}"#);
        let sum: f64 = text
            .lines()
            .find(|l| l.starts_with(&sum_line))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("读不到 {sum_line}:\n{text}"));
        assert!(
            sum < queued_at_least.as_secs_f64(),
            "第二发排队 ≥ {:?},但它记下的 IO 时长是 {sum}s —— 排队被算进了 syscall 口径(计时点错在提交侧)",
            queued_at_least
        );
        assert_eq!(watch.oldest(&id, "put"), None, "全部收尾后表里不留残留");
        assert_eq!(watch.oldest(&id, "get"), None, "另一个 op 也收干净了");
    }

    /// **任务 panic 也必须注销它的在途登记**:否则那块盘的"最久在途"永远停在那个早已死掉的开始时刻,
    /// 看门狗会误判一块完全健康的盘(而它其实只是有一次引擎调用炸了)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_job_still_releases_its_io_watch_entry() {
        let f = fixture(1, &cfg(1, 0, 0));
        let id = f.ids[0].clone();
        let watch = f.pool.io_watch();
        match f.pool.run(&id, "put", || panic!("engine blew up")).await {
            Err(PoolReject::JobPanicked) => {}
            other => panic!("应报 JobPanicked,实际 {other:?}"),
        }
        assert_eq!(watch.in_flight(&id, "put"), 0, "panic 后不得留下'幽灵在途'");
        assert_eq!(
            watch.oldest(&id, "put"),
            None,
            "panic 后不得留下幽灵开始时刻"
        );
        // 而且它**确实被计过时**(计时在 syscall 外围,panic 也走同一段)。
        let text = metrics_text(&f);
        assert!(
            text.contains(&format!(
                r#"store_server_disk_io_duration_seconds_count{{disk_id="{id}",op="put"}} 1"#
            )),
            "炸掉的那一发同样要有时长观测(它是真实发生过的一次 IO 尝试):\n{text}"
        );
    }
}
