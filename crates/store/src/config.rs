//! `StoreServerConfig`:数据节点配置(对齐 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §9)。
//!
//! 字段名即 TOML key(在 `[store_server]` 表下)。默认值/范围须与本文件 `impl Default` + `Validate`
//! 对齐(CLAUDE.md §7)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// Compaction 调度策略(对齐 Ambry `CompactionPolicy` 体系)。
///
/// - `Incremental`:按段死占比选段增量压缩(改造前的默认行为);
/// - `CostBenefit`:按 cost/benefit 比选最优连续段区间(对齐 Ambry `StatsBasedCompactionPolicy`);
/// - `Hybrid`(默认):日常走 `CostBenefit`,每隔 `compaction_policy_switch_days` 穿插一次
///   CompactAll 全库重写(对齐 Ambry `HybridCompactionPolicy`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionPolicyKind {
    Incremental,
    CostBenefit,
    Hybrid,
}

/// 一块盘的**声明**:目录 + 该盘自己的参数。
///
/// 两种写法都吃(TOML):
/// - 只给目录(简单部署/兼容旧配置):`data_dirs = ["/data/d1"]` ⇒ 池大小走 [`StoreServerConfig::pool_default_size`];
/// - 给出每盘参数:`[[store_server.data_dirs]]` 下 `dir = "/data/nvme0"` + `pool_size = 64`。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum DataDir {
    /// 只给目录。
    Plain(String),
    /// 目录 + 该盘参数。
    Spec(DiskSpec),
}

/// 单块盘的参数(`[[store_server.data_dirs]]` 的一条)。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DiskSpec {
    /// 盘目录(与 master/admin/心跳看到的盘 id = 该目录 basename)。
    pub dir: String,
    /// 该盘池的 worker 线程数(可选;不给 = 用 `pool_default_size`)。
    #[serde(default)]
    pub pool_size: Option<u32>,
}

impl DataDir {
    /// 盘目录。
    pub fn dir(&self) -> &str {
        match self {
            DataDir::Plain(dir) => dir,
            DataDir::Spec(spec) => &spec.dir,
        }
    }

    /// 该盘显式指定的池大小(只有形态 B 能给)。
    pub fn pool_size(&self) -> Option<u32> {
        match self {
            DataDir::Plain(_) => None,
            DataDir::Spec(spec) => spec.pool_size,
        }
    }
}

impl DataDir {
    /// 只给目录(池大小走 `pool_default_size`)。嵌入方/测试构造配置用。
    pub fn plain(dir: impl Into<String>) -> Self {
        DataDir::Plain(dir.into())
    }

    /// 目录 + 该盘显式池大小。
    pub fn spec(dir: impl Into<String>, pool_size: Option<u32>) -> Self {
        DataDir::Spec(DiskSpec {
            dir: dir.into(),
            pool_size,
        })
    }
}

impl From<String> for DataDir {
    fn from(dir: String) -> Self {
        DataDir::Plain(dir)
    }
}

impl From<&str> for DataDir {
    fn from(dir: &str) -> Self {
        DataDir::Plain(dir.to_string())
    }
}

/// 数据节点配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct StoreServerConfig {
    /// 本数据节点 id(与 master 侧一致)。
    pub node_id: u64,
    /// 挂载的盘列表(每个目录当一块盘,`statfs` 探真实容量)。每块盘可自带参数(目前是池大小)。
    pub data_dirs: Vec<DataDir>,
    /// 数据面 HTTP 监听地址(host:port)。
    pub http_addr: String,
    /// 每个分区副本 `BlobStore` 的容量上界(字节),对齐 master `partition_capacity_bytes`。
    pub partition_capacity_bytes: u64,
    /// master 控制面 gRPC 地址;**空 = 不接 master**(S1 独立模式)。
    pub master_addr: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致);空 = 关校验。
    pub cluster_secret: String,
    /// 心跳周期(秒)。
    pub heartbeat_interval_secs: u64,
    /// 是否开启主动磁盘探测(默认关,对齐 Ambry)。
    pub disk_health_enabled: bool,
    /// 主动磁盘探测周期(秒)。
    pub disk_health_interval_secs: u64,
    /// 主动磁盘探测单次读写的超时(秒,抓盘 hang)。
    pub disk_health_timeout_secs: u64,
    /// 是否开启拉复制(S3a);默认关(安全)。需 `master_addr` 才能发现 peer。
    pub replication_enabled: bool,
    /// 拉复制周期(秒)。
    pub replication_interval_secs: u64,
    /// 单次向某 peer 拉取元数据的批字节上限(限流)。
    pub replication_max_batch_bytes: u64,
    /// 是否用封存段物理拷贝加速 bootstrap(S3b);默认 true,随 `replication_enabled` 生效。
    pub bootstrap_copy_enabled: bool,
    /// 是否开启本地维护 worker(S4:周期 flush / compaction);默认 true。
    pub local_worker_enabled: bool,
    /// 本地 worker flush 周期(秒):推进持久化边界,缩小 W=2 page-cache 掉电窗口。
    pub flush_interval_secs: u64,
    /// flush **早醒水位**(字节):某副本自上次 flush 尝试以来未持久化字节 ≥ 此值时,
    /// 本地 worker 在周期 tick 之外**立即只 flush 该副本**(有界脏窗口,fsync 拆窗)。
    /// **默认 0 = 关**:真机 APFS/GB-s 口径证据为反例(水位臂吞吐 −60%、max 反劣,见
    /// `flush-bounded-dirty-window` 档「真机证据」),生产低写入速率(百 MB/s 级)两态
    /// 行为等价,ext4 未测 —— 开启与否交由 Operator 按部署实测。过小值自限(复位后需
    /// 等新字节重新武装),建议 ≥ 1 MiB。
    pub flush_dirty_threshold_bytes: u64,
    /// 本地 worker compaction 的**周期**(秒):跑一次之后隔多久再跑;默认 86400 = 一天一次。
    pub compaction_interval_secs: u64,
    /// compaction 的**首跑钟点**(本地时间,`HH:MM`):首次触发对齐到下一个该钟点(今天没过就是今天,
    /// 过了就是明天),之后每 [`compaction_interval_secs`](Self::compaction_interval_secs) 一次。
    /// 默认 `"01:00"` = 每天凌晨一点。
    pub compaction_start_time: String,
    /// compaction 触发阈值 (0,1]:partition 的**可回收字节占比** ≥ 此值才压缩。
    ///
    /// 可回收 = 被取代的旧记录 + TTL 已过期对象 + 墓碑已过保留期;段级选择用同一阈值。
    pub compaction_dead_ratio: f64,
    /// 是否把 **RW 状态分区**也纳入定时 compaction 候选集(只压其**封存段**,活跃段不动)。
    ///
    /// `SEALED` 分区永不入选;`false` 时只压 RO 分区。未知状态(未接 master 的单机模式)按可压处理。
    pub compaction_include_rw: bool,
    /// 墓碑保留期(秒):`delete` 之后满此时长才允许被 compaction 回收(`0` = 立即允许)。
    ///
    /// 保留期是防「副本修复把已删对象复活」的窗口,应大于副本可能滞后的上界;默认 24h。
    pub compaction_tombstone_retention_secs: u64,
    /// Compaction 调度策略;默认 `Hybrid`(对齐 Ambry `HybridCompactionPolicy`)。
    pub compaction_policy: CompactionPolicyKind,
    /// Hybrid 模式下两次 CompactAll 之间的最少天数;默认 6(对齐 Ambry
    /// `store.compaction.policy.switch.timestamp.days`)。
    pub compaction_policy_switch_days: u32,
    /// cost-benefit 策略的最小回收段数阈值:选出的区间净减段数 < 此值时不动手;默认 1(对齐 Ambry
    /// `store.min.log.segment.count.to.reclaim.to.trigger.compaction`)。
    pub compaction_min_log_segment_count_to_reclaim: usize,
    /// 数据面单请求 body 上限(字节),经 axum `DefaultBodyLimit` 生效。
    /// **默认 5 MiB > router 默认恒切片宽 4 MiB**(2026-10-22 所有者显式指示,走 CLAUDE.md §5.1
    /// 明示流程):分体部署的裸默认自此自洽 —— 裸默认 store 不再对 router 的 4 MiB 整片逐一 413;
    /// 历史上"2 MiB 对齐 axum `DefaultBodyLimit` 默认值"的理由随之作废。要收真正的大 blob(超
    /// 单笔片量级)仍需显式调大;调小则超限请求在 extractor 前即被 413 拒掉。
    pub max_body_bytes: u64,
    /// 请求静默(`/admin/request-control`)未指定 `ttl_secs` 时的默认禁用时长(秒)。
    /// **0 = 不设默认时长**(即未指定 `ttl_secs` 时永久禁用,须显式开回)。
    pub request_control_default_ttl_secs: u64,
    /// 本地完整性校验(`GET /partitions/{pid}/verify`)单次扫描的**读取字节上限**(默认 64 MiB):
    /// 深扫会读全量数据体,限流以免打爆盘;到点返回部分结果并在响应里标 `truncated`。
    pub scrub_max_batch_bytes: u64,
    /// **后台一致性巡检**(bit-rot 蜗牛爬)总开关;默认关。开启后本节点定时对本节点每个副本做
    /// 容错深扫(逐记录 CRC,坏记录记下继续),**只报警不修复**(修复走运维弧 Repair/Move/重 eval)。
    /// 与手动 verify 端点共用引擎与扫描预算,不共用节拍;停它用 `/admin/scrub-control`(不动端点)。
    pub scrub_background_enabled: bool,
    /// 后台巡检两次扫描 batch 之间的**基础间隔**(秒);实际睡眠叠加 ±25% 抖动去相位(避免与
    /// compaction 钟点或多节点同拍共振成周期性尖峰)。单 batch 字节预算 = [`Self::scrub_max_batch_bytes`]。
    pub scrub_background_interval_secs: u64,
    /// 全节点同时 in-flight 的扫描 batch 数上限(默认 1 = 同一时刻只有一块盘在爬;家风同 master
    /// `balance_max_in_flight`:宁可慢,不打满)。管的是**跨盘**并发;**同盘**内由该盘的有界池
    ///(D1)自行串行,池饱和 = 本轮跳过退避,绝不重试轰炸。
    pub scrub_background_max_in_flight: u32,
    /// **每盘默认池大小**(worker 线程数):`data_dirs` 里没给自己 `pool_size` 的盘都用它。
    ///
    /// 引擎 IO 走**每盘自己的** worker 线程(见 [`crate::disk_pool`]),不再共用进程级阻塞池 ——
    /// 一块盘挂死只占自己的线程。按盘类型分别调,就在该盘那条 `data_dirs` 里写 `pool_size`。
    ///
    /// 取值经验:每个 worker = 1 条 OS 线程,同时最多 1 次在飞 IO。机械盘有用队列深度通常只有个位数,
    /// NVMe 可以 32~128 —— 故默认给 16 这个偏保守的中间值;调大前先看
    /// `store_server_disk_pool_queued`(持续排队才是真不够)与磁盘自身的 IO 队列深度。
    pub pool_default_size: u32,
    /// **每盘排队容量**(池内 worker 全忙时允许等待的请求数):`0` = 不排队,满即拒。
    ///
    /// 排队**有界**是刻意的 —— 无限排队只会把延迟推高、把连接占死,还掩盖"这块盘已经不行了"。
    pub pool_queue_size: u32,
    /// **准入等待上限(毫秒)**:池满时最多等这么久(等的是"有空位",不是"这次 IO 做完"),超时即拒。
    ///
    /// 短暂排队能吸收瞬时突发,又不会把请求拖成"看起来挂了";`0` = 不等(满即拒)。拒绝时 HTTP 面回
    /// **503**(带 `Retry-After`),让 router 立刻改用别的副本。
    pub pool_acquire_timeout_ms: u64,
    /// **单次引擎 IO 的系统调用时长软上限(毫秒)**:超过即认为这块盘"这一发没回来",记一次
    /// `IO_TIMEOUT` 坏观测(是否摘盘由下面的迟滞参数决定)。
    ///
    /// 口径是**闭包内**的 syscall 耗时,**不含池排队**(见 [`crate::disk_pool::IoWatch`])。取值必须
    /// **远大于** p99.9 与 compaction 的大块拷贝时长,否则会把健康盘判成坏盘。
    ///
    /// **不受 [`disk_health_enabled`](Self::disk_health_enabled) 约束**:主动探测可以关(默认就是关),
    /// 但看门狗不能跟着关 —— 否则默认部署里 `UNAVAILABLE` 就再没有第二个来源,坏盘隔离与撤离全哑。
    pub disk_watchdog_ms: u64,
    /// **延迟劣化的判定阈值**(毫秒;`0` = **关闭**这一档,默认)。
    ///
    /// 计法:一次**闭包内**测得的 IO 耗时 ≥ 本值就记一次"慢 IO";看门狗每轮算一次
    /// `慢 IO / 本轮总 IO` 的占比,连续超过 [`disk_degraded_ratio`](Self::disk_degraded_ratio) 就报
    /// **`DEGRADED`**(走 `disk_health_bad_rounds` / `bad_secs` 那套迟滞)。
    ///
    /// **`DEGRADED` 的具体含义**(三条,别记错):① 该盘**仍然服务**(不 `503`);
    /// ② master 侧**不再往它放新副本**(因为它 != `HEALTHY`/`UNKNOWN`/空串,天然被排除);
    /// ③ **不触发撤离**(撤离只认 `UNAVAILABLE`,避免"有点慢就搬 GB 数据"的 churn)。
    ///
    /// 默认 `0 = 关`:它是新引入的判定口径,观测一周期再开(CLAUDE.md §5.1 不擅改默认)。
    /// 取值参考:健康盘 p99 的 2~3 倍(标定步骤见 [`docs/design/cluster.md` §9.3] —— 精确到"哪条查询"。
    pub disk_degraded_io_ms: u64,
    /// **延迟劣化的占比门槛**(0,1]:一轮里"慢 IO"占比 ≥ 本值就算该轮劣化。默认 `0.1`。
    ///
    /// 为什么是占比而不是"有任何一个慢 IO":一次偶发的慢 IO 在一百万次里是噪声。占比才反映
    /// "这块盘**持续**慢下来了"。与 [`disk_degraded_io_ms`](Self::disk_degraded_io_ms) 配合使用
    /// (后者关着时本项无意义)。
    pub disk_degraded_ratio: f64,
    /// **按 op 覆盖看门狗门槛**(毫秒;key = op 名,见 [`crate::disk_pool::KNOWN_OPS`];空 = 全部用
    /// [`disk_watchdog_ms`](Self::disk_watchdog_ms))。
    ///
    /// 为什么需要它:compaction / flush 要拷整段,天然比一次 4KiB 读**慢几个数量级**。共用一个门槛时
    /// 只有两个坏选择 —— 要么门槛大到"真死的盘半天不被隔离",要么小到"健康盘的 compaction 天天误报"。
    ///
    /// 用法:保留 `disk_watchdog_ms` 给普通读写,给大 IO 单开一档,例如
    /// `[store_server.disk_watchdog_ms_by_op]` 下 `compaction = 300000`、`flush = 120000`。
    ///
    /// 覆盖值**可以比默认小**(会给该 op 更紧的门槛);看门狗的 tick 周期取所有门槛的**最小值**,
    /// 故不会出现"门槛很小却迟迟不被检查"。
    pub disk_watchdog_ms_by_op: std::collections::BTreeMap<String, u64>,
    /// **逃生舱(进程自杀)门槛秒数**:某盘池**全部 worker 都被超期 IO 占住**且最久那一发已超过此秒数
    /// ⇒ `process::abort()`,交 supervisor 重启。
    ///
    /// **默认 0 = 永不升级**(CLAUDE.md §5.1 不擅自改默认)。为什么默认关:antfs 是**单进程多盘**
    /// (Ceph 是一 OSD 一进程),自杀会**带走健康盘上的服务** —— 误杀一次等于自造一次全节点故障。
    /// 开启时建议 ≥ 600,且必须 ≥ [`disk_watchdog_ms`](Self::disk_watchdog_ms)(否则"刚超期一次"
    /// 就会被当成"永远不会回来了")。
    pub disk_watchdog_escalate_secs: u64,
    /// **连续坏 N 轮** ⇒ 该盘置 `UNAVAILABLE`(数据面 503)。迟滞,防"偶尔一次抽风就摘盘"。
    pub disk_health_bad_rounds: u64,
    /// **坏态持续 ≥ T 秒** ⇒ 也置 `UNAVAILABLE`;与上者取**先到者**。
    ///
    /// 为什么两条都要:只按轮数,探测周期调到 300s 时"连续 3 轮"是 15 分钟,一块真死的盘要等 15 分钟
    /// 才被隔离;只按时长,周期调小(5s)时"持续 30s"等于 6 轮都不防抖。
    pub disk_health_bad_secs: u64,
    /// 复位要求**连续好 M 轮**(且满足下面的稳定时长)。
    pub disk_health_good_rounds: u64,
    /// 复位还要求"从进入连续好态起**稳定满 T2 秒**"。
    ///
    /// 复位比进入更严是刻意的:一块刚缓过来的盘立刻接回全部流量,大概率又被压垮 ——
    /// 与 `master` 侧"盘将满 RW→RO 只置不回"同一种防抖精神。
    pub disk_health_recover_stable_secs: u64,
}

impl Default for StoreServerConfig {
    fn default() -> Self {
        StoreServerConfig {
            node_id: 1,
            data_dirs: Vec::new(),
            http_addr: "127.0.0.1:7200".to_string(),
            partition_capacity_bytes: 100 * 1024 * 1024 * 1024,
            master_addr: String::new(),
            cluster_secret: String::new(),
            heartbeat_interval_secs: 3,
            disk_health_enabled: false,
            disk_health_interval_secs: 60,
            disk_health_timeout_secs: 10,
            replication_enabled: false,
            replication_interval_secs: 5,
            replication_max_batch_bytes: 4 * 1024 * 1024,
            bootstrap_copy_enabled: true,
            local_worker_enabled: true,
            flush_interval_secs: 30,
            flush_dirty_threshold_bytes: 0,
            compaction_interval_secs: 86400,
            compaction_start_time: "01:00".to_string(),
            compaction_dead_ratio: 0.3,
            compaction_include_rw: true,
            compaction_tombstone_retention_secs: 86400,
            compaction_policy: CompactionPolicyKind::Hybrid,
            compaction_policy_switch_days: 6,
            compaction_min_log_segment_count_to_reclaim: 1,
            // 2026-10-22 所有者明示:5 MiB 盖过 router 默认片宽 4 MiB(裁决 #17),分体裸默认自洽。
            max_body_bytes: 5 * 1024 * 1024,
            request_control_default_ttl_secs: 1800,
            scrub_max_batch_bytes: 64 << 20,
            scrub_background_enabled: false,
            scrub_background_interval_secs: 60,
            scrub_background_max_in_flight: 1,
            // 经验初值(线上按 `disk_pool_queued` / `disk_pool_rejected` 标定;见字段注释)。
            pool_default_size: 16,
            pool_queue_size: 32,
            pool_acquire_timeout_ms: 200,
            // 经验初值(见各字段注释;上线前用 `disk_io_duration_seconds` 的分位与
            // `disk_io_watchdog_trips_total` 标定)。
            disk_watchdog_ms: 30_000,
            // DEGRADED 默认关(新判定口径,先观测一周期);阈值一旦开启需与 io_ms 一起标定。
            disk_degraded_io_ms: 0,
            disk_degraded_ratio: 0.1,
            // 空 = 全部 op 用上面那个默认门槛(不擅自替运维定"compaction 该多慢")。
            disk_watchdog_ms_by_op: std::collections::BTreeMap::new(),
            disk_watchdog_escalate_secs: 0,
            disk_health_bad_rounds: 3,
            disk_health_bad_secs: 30,
            disk_health_good_rounds: 3,
            disk_health_recover_stable_secs: 120,
        }
    }
}

impl StoreServerConfig {
    /// **某一轮的延迟观测**:`(慢 IO 数, 本轮 IO 总数)` → 该轮是否算"劣化"。
    ///
    /// 三条边界(都有单测):
    /// - 本档关着(`disk_degraded_io_ms == 0`)⇒ 永远 `false`(不引入任何行为);
    /// - 本轮**一发 IO 都没有** ⇒ `None`:没有观测就**不产生结论**,让迟滞状态机停在上一次判定上
    ///   (与探测"跳过轮不覆盖健康值"同一条纪律 —— 空闲不等于慢);
    /// - 有观测 ⇒ 占比 ≥ `disk_degraded_ratio` 才算劣化。
    pub fn degraded_round(&self, slow: u64, total: u64) -> Option<bool> {
        if self.disk_degraded_io_ms == 0 || total == 0 {
            return None;
        }
        Some(slow as f64 / total as f64 >= self.disk_degraded_ratio)
    }

    /// 某个 op 的看门狗门槛(毫秒):按 op 覆盖优先,否则用 [`Self::disk_watchdog_ms`]。
    pub fn watchdog_ms_for(&self, op: &str) -> u64 {
        self.disk_watchdog_ms_by_op
            .get(op)
            .copied()
            .unwrap_or(self.disk_watchdog_ms)
    }

    /// 盘健康的**迟滞参数**(design D2):探测与看门狗共用同一台状态机,故阈值也只有一份。
    pub fn hysteresis(&self) -> crate::disk_health::Hysteresis {
        crate::disk_health::Hysteresis {
            bad_rounds: self.disk_health_bad_rounds.min(u32::MAX as u64) as u32,
            bad_secs: self.disk_health_bad_secs,
            good_rounds: self.disk_health_good_rounds.min(u32::MAX as u64) as u32,
            recover_stable_secs: self.disk_health_recover_stable_secs,
        }
    }
}

impl Validate for StoreServerConfig {
    fn validate(&self) -> Result<()> {
        at_least("store_server.node_id", self.node_id, 1)?;
        at_least(
            "store_server.partition_capacity_bytes",
            self.partition_capacity_bytes,
            1,
        )?;
        if self.data_dirs.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.data_dirs",
                "must declare at least one disk directory",
            ));
        }
        if self.http_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.http_addr",
                "must not be empty",
            ));
        }
        at_least(
            "store_server.heartbeat_interval_secs",
            self.heartbeat_interval_secs,
            1,
        )?;
        at_least(
            "store_server.disk_health_interval_secs",
            self.disk_health_interval_secs,
            1,
        )?;
        at_least(
            "store_server.disk_health_timeout_secs",
            self.disk_health_timeout_secs,
            1,
        )?;
        at_least(
            "store_server.replication_interval_secs",
            self.replication_interval_secs,
            1,
        )?;
        at_least(
            "store_server.replication_max_batch_bytes",
            self.replication_max_batch_bytes,
            1,
        )?;
        at_least(
            "store_server.flush_interval_secs",
            self.flush_interval_secs,
            1,
        )?;
        at_least(
            "store_server.compaction_interval_secs",
            self.compaction_interval_secs,
            1,
        )?;
        at_least("store_server.max_body_bytes", self.max_body_bytes, 1)?;
        // 0 合法 = 不设默认时长(永久禁用),故下界为 0。
        at_least(
            "store_server.request_control_default_ttl_secs",
            self.request_control_default_ttl_secs,
            0,
        )?;
        at_least(
            "store_server.scrub_max_batch_bytes",
            self.scrub_max_batch_bytes,
            1,
        )?;
        // 后台巡检:间隔 ≥1(0 会让循环退化成忙转);lane 数 ≥1(0 等于没开起来)。
        // enabled 是开关不是数值,无需下界。
        at_least(
            "store_server.scrub_background_interval_secs",
            self.scrub_background_interval_secs,
            1,
        )?;
        at_least(
            "store_server.scrub_background_max_in_flight",
            u64::from(self.scrub_background_max_in_flight),
            1,
        )?;
        // 每盘池:默认大小 ≥1(0 会让每盘都退化成 1 线程,几乎肯定是笔误;真要 1 就写 1);
        // 队列与准入超时可 0(0 = 不排队 / 不等)。
        at_least("store_server.pool_default_size", self.pool_default_size, 1)?;
        // 看门狗 deadline:必须 ≥1(0 会让每一发 IO 都算超期,等于把整节点瞬间摘空)。
        at_least("store_server.disk_watchdog_ms", self.disk_watchdog_ms, 1)?;
        // DEGRADED:阈值 0 = 关闭(合法);占比必须在 (0,1] —— 0 会让"没有任何慢 IO"也算劣化,
        // >1 则永远不触发(等于配了个假的开关)。
        if !(self.disk_degraded_ratio > 0.0 && self.disk_degraded_ratio <= 1.0) {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.disk_degraded_ratio",
                "must be in (0, 1]",
            ));
        }
        // 按 op 覆盖:key 必须是**已知的池上 op**,值 ≥1。
        // 拼错的 key 会让"设了门槛却毫无作用",而这类错误在运行期完全无声 —— 故当场拒。
        for (op, ms) in &self.disk_watchdog_ms_by_op {
            if !crate::disk_pool::KNOWN_OPS.contains(&op.as_str()) {
                return Err(antfs_config::ConfigError::invalid(
                    "store_server.disk_watchdog_ms_by_op",
                    format!(
                        "unknown op {:?}; known ops: {}",
                        op,
                        crate::disk_pool::KNOWN_OPS.join(", ")
                    ),
                ));
            }
            if *ms < 1 {
                return Err(antfs_config::ConfigError::invalid(
                    "store_server.disk_watchdog_ms_by_op",
                    format!("deadline for {op:?} must be at least 1 ms"),
                ));
            }
        }
        // 迟滞的进入判据都要 ≥1:0 轮 / 0 秒等于"一发坏观测就摘盘",正是迟滞要防的事。
        at_least(
            "store_server.disk_health_bad_rounds",
            self.disk_health_bad_rounds,
            1,
        )?;
        at_least(
            "store_server.disk_health_bad_secs",
            self.disk_health_bad_secs,
            1,
        )?;
        at_least(
            "store_server.disk_health_good_rounds",
            self.disk_health_good_rounds,
            1,
        )?;
        // `recover_stable_secs` 允许 0(= 只按连续好轮数复位);`escalate` 允许 0(= 永不升级)。
        // 逃生门槛必须 ≥ 看门狗 deadline:低于它就等于"某一发刚刚超期一次"就被判成"永远不会回来了"。
        let watchdog_secs = self.disk_watchdog_ms.div_ceil(1000);
        if self.disk_watchdog_escalate_secs != 0 && self.disk_watchdog_escalate_secs < watchdog_secs
        {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.disk_watchdog_escalate_secs",
                format!(
                    "must be 0 (never escalate) or at least the watchdog deadline ({watchdog_secs}s); \
                     a smaller value would abort the process on a single overdue IO"
                ),
            ));
        }
        for (i, d) in self.data_dirs.iter().enumerate() {
            if d.dir().is_empty() {
                return Err(antfs_config::ConfigError::invalid(
                    "store_server.data_dirs",
                    format!("entry #{i} has an empty dir"),
                ));
            }
            if let Some(size) = d.pool_size() {
                if size < 1 {
                    return Err(antfs_config::ConfigError::invalid(
                        "store_server.data_dirs",
                        format!("pool_size of {} must be at least 1", d.dir()),
                    ));
                }
            }
        }
        // **盘 id = 目录 basename**,重复就等于两块盘共用一个 id:池、心跳、master 放置全都会串。
        // 这种配置以前能"跑起来"(坏得很安静),现在直接拒。
        for (i, d) in self.data_dirs.iter().enumerate() {
            let base = std::path::Path::new(d.dir())
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            if self.data_dirs.iter().skip(i + 1).any(|other| {
                std::path::Path::new(other.dir())
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
                    == base
            }) {
                return Err(antfs_config::ConfigError::invalid(
                    "store_server.data_dirs",
                    format!("duplicate disk id {base:?}: disk id is the dir basename, it must be unique"),
                ));
            }
        }
        if !(self.compaction_dead_ratio > 0.0 && self.compaction_dead_ratio <= 1.0) {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.compaction_dead_ratio",
                "must be in (0, 1]",
            ));
        }
        parse_hhmm(&self.compaction_start_time).map_err(|why| {
            antfs_config::ConfigError::invalid(
                "store_server.compaction_start_time",
                format!("must be local time \"HH:MM\": {why}"),
            )
        })?;
        if self.compaction_policy_switch_days < 1 || self.compaction_policy_switch_days > 365 {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.compaction_policy_switch_days",
                "must be in 1..=365",
            ));
        }
        at_least(
            "store_server.compaction_min_log_segment_count_to_reclaim",
            self.compaction_min_log_segment_count_to_reclaim as u64,
            1,
        )?;
        Ok(())
    }
}

/// 解析 `"HH:MM"`(本地时间)= 当日零点起的秒数。
///
/// 只接受严格的两位时分(如 `"01:00"`、`"23:59"`):写成 `"1:00"` 之类多半是笔误,宁可起不来也别
/// 悄悄按别的钟点跑。本地维护 worker 也用这把尺子(配置校验与调度解析同一实现)。
pub(crate) fn parse_hhmm(s: &str) -> std::result::Result<u32, &'static str> {
    let (h, m) = s.split_once(':').ok_or("expected \"HH:MM\"")?;
    if h.len() != 2 || m.len() != 2 {
        return Err("hours and minutes must be two digits");
    }
    let h: u32 = h.parse().map_err(|_| "hours must be a number")?;
    let m: u32 = m.parse().map_err(|_| "minutes must be a number")?;
    if h > 23 {
        return Err("hours must be 00..=23");
    }
    if m > 59 {
        return Err("minutes must be 00..=59");
    }
    Ok(h * 3600 + m * 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hhmm_accepts_strict_zero_padded_times() {
        assert_eq!(parse_hhmm("00:00").unwrap(), 0);
        assert_eq!(parse_hhmm("01:00").unwrap(), 3600);
        assert_eq!(parse_hhmm("23:59").unwrap(), 86_340);
    }

    #[test]
    fn hhmm_rejects_typos_and_out_of_range() {
        for bad in [
            "", "1:00", "01:0", "24:00", "01:60", "0100", "01:0a", "-1:00",
        ] {
            assert!(parse_hhmm(bad).is_err(), "{bad:?} 应被拒");
        }
    }

    /// 每盘池的默认值:每盘默认 16 个 worker、排队 32、准入等待 200ms。
    /// 改这几个默认值必须显式改本测试(CLAUDE.md §5.1)。
    #[test]
    fn pool_defaults_match_documented_values() {
        let c = StoreServerConfig::default();
        assert_eq!(c.pool_default_size, 16, "每盘默认池大小");
        assert_eq!(c.pool_queue_size, 32, "每盘排队容量默认 32");
        assert_eq!(c.pool_acquire_timeout_ms, 200, "准入等待默认 200ms");
    }

    /// 盘声明的两种写法都要能被**真正的加载器**吃下(不是拿 serde 手搓):裸目录(走默认池大小)
    /// 与"目录 + 每盘参数"(`[[data_dirs]]` 表)。走 `antfs_config::Loader`,与线上读 `store.toml`
    /// 是同一条路。
    #[test]
    fn data_dirs_accept_both_plain_and_spec_forms_through_the_loader() {
        let tmp = tempfile::tempdir().unwrap();

        let plain = tmp.path().join("plain.toml");
        std::fs::write(
            &plain,
            "[store_server]\ndata_dirs = [\"/data/d1\", \"/data/d2\"]\n",
        )
        .unwrap();
        let cfg: StoreServerConfig = antfs_config::Loader::new()
            .with_file(plain.to_string_lossy().into_owned(), true)
            .load_table("store_server")
            .expect("裸目录写法应可加载");
        assert_eq!(cfg.data_dirs.len(), 2);
        assert_eq!(cfg.data_dirs[0].dir(), "/data/d1");
        assert_eq!(cfg.data_dirs[0].pool_size(), None, "裸目录不给池大小");
        assert!(cfg.validate().is_ok());

        let spec = tmp.path().join("spec.toml");
        std::fs::write(
            &spec,
            "[store_server]\n\
             pool_default_size = 4\n\
             [[store_server.data_dirs]]\n\
             dir = \"/data/nvme0\"\n\
             pool_size = 64\n\
             [[store_server.data_dirs]]\n\
             dir = \"/data/hdd0\"\n",
        )
        .unwrap();
        let cfg: StoreServerConfig = antfs_config::Loader::new()
            .with_file(spec.to_string_lossy().into_owned(), true)
            .load_table("store_server")
            .expect("每盘参数写法应可加载");
        assert_eq!(cfg.data_dirs[0].pool_size(), Some(64), "第一个盘显式 64");
        assert_eq!(
            cfg.data_dirs[1].pool_size(),
            None,
            "第二个盘走 pool_default_size"
        );
        assert_eq!(cfg.pool_default_size, 4, "默认值应被显式配置覆盖");
        assert!(cfg.validate().is_ok());
    }

    /// 盘声明的校验:空目录、每盘池大小 <1、以及**盘 id 重复**(= 目录 basename 相同)都要当场拒。
    /// 盘 id 重复以前能安静地跑起来,但池/心跳/master 放置会互相串 —— 宁可起不来。
    #[test]
    fn data_dir_validation_rejects_empty_duplicate_and_zero_pool() {
        let ok = StoreServerConfig {
            data_dirs: vec![
                DataDir::Plain("/data/a".to_string()),
                DataDir::Spec(DiskSpec {
                    dir: "/data/b".to_string(),
                    pool_size: Some(8),
                }),
            ],
            ..Default::default()
        };
        assert!(ok.validate().is_ok(), "正常声明应通过");

        let empty = StoreServerConfig {
            data_dirs: vec![DataDir::Plain(String::new())],
            ..Default::default()
        };
        assert!(empty.validate().is_err(), "空目录应被拒");

        let zero = StoreServerConfig {
            data_dirs: vec![DataDir::Spec(DiskSpec {
                dir: "/data/a".to_string(),
                pool_size: Some(0),
            })],
            ..Default::default()
        };
        assert!(zero.validate().is_err(), "每盘池大小 0 应被拒");

        // 同 basename 的两个目录 = 两块盘共用一个盘 id:池/心跳/master 全会串,必须拒。
        let dup = StoreServerConfig {
            data_dirs: vec![
                DataDir::Plain("/data/set-a/store".to_string()),
                DataDir::Plain("/data/set-b/store".to_string()),
            ],
            ..Default::default()
        };
        assert!(dup.validate().is_err(), "盘 id(basename)重复应被拒");
    }

    /// 每盘池的默认大小校验:0 会让每盘都退化成 1 线程(几乎肯定是笔误);队列与准入等待允许 0。
    #[test]
    fn pool_validation_rejects_zero_default_and_allows_zero_queue() {
        let base = || StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..Default::default()
        };
        assert!(base().validate().is_ok(), "默认配置应合法");

        let zero = StoreServerConfig {
            pool_default_size: 0,
            ..base()
        };
        assert!(zero.validate().is_err(), "每盘默认池大小 0 应被拒");

        let no_queue = StoreServerConfig {
            pool_queue_size: 0,
            pool_acquire_timeout_ms: 0,
            ..base()
        };
        assert!(no_queue.validate().is_ok(), "0 合法 = 不排队 / 满即拒");
    }

    /// 健康闸门 / 看门狗(design D2/D3)的**默认值**:全部是**经验初值**,改它必须显式改本测试
    /// (CLAUDE.md §5.1)。数字与 `openspec/changes/store-disk-isolation/design.md` §10.1 的提案值
    /// 一一对应,实现时未"顺手另定一个"。
    #[test]
    fn disk_health_defaults_match_documented_proposal() {
        let c = StoreServerConfig::default();
        assert_eq!(c.disk_watchdog_ms, 30_000, "看门狗 deadline 30s");
        assert_eq!(
            c.disk_watchdog_escalate_secs, 0,
            "逃生舱默认关:单进程多盘,误杀 = 自造全节点故障"
        );
        assert_eq!(c.disk_health_bad_rounds, 3, "连续 3 轮坏才摘盘");
        assert_eq!(c.disk_health_bad_secs, 30, "坏态满 30s 也摘盘(与上者先到)");
        assert_eq!(c.disk_health_good_rounds, 3, "连续 3 轮好才考虑复位");
        assert_eq!(
            c.disk_health_recover_stable_secs, 120,
            "复位还要稳定满 120s"
        );
        assert_eq!(
            c.hysteresis(),
            crate::disk_health::Hysteresis {
                bad_rounds: 3,
                bad_secs: 30,
                good_rounds: 3,
                recover_stable_secs: 120,
            },
            "配置 → 迟滞参数的映射必须与默认值一致"
        );
    }

    /// **DEGRADED 的每轮判定**(纯函数)三条边界:
    /// 关着(阈值 0)⇒ 永远不产生结论;本轮**没有 IO** ⇒ 不产生结论(空闲 ≠ 慢);
    /// 有观测 ⇒ 占比达阈值才算劣化。
    #[test]
    fn degraded_round_needs_traffic_and_respects_the_ratio() {
        let off = StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            disk_degraded_io_ms: 0,
            ..Default::default()
        };
        assert_eq!(
            off.degraded_round(100, 100),
            None,
            "阈值 0 = 关闭:一律不产生结论"
        );

        let on = StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            disk_degraded_io_ms: 50,
            disk_degraded_ratio: 0.1,
            ..Default::default()
        };
        assert_eq!(
            on.degraded_round(0, 0),
            None,
            "本轮没有任何 IO ⇒ 不产生结论(空闲不等于慢)"
        );
        assert_eq!(on.degraded_round(5, 100), Some(false), "5% < 10% ⇒ 不劣化");
        assert_eq!(
            on.degraded_round(10, 100),
            Some(true),
            "恰好 10% ⇒ 劣化(≥ 语义)"
        );
        assert_eq!(on.degraded_round(90, 100), Some(true));

        // 占比配置的边界:<=0 与 >1 都是"配了个假的开关" ⇒ 配置期就拒。
        for bad in [0.0, -0.1, 1.5] {
            let c = StoreServerConfig {
                data_dirs: vec![DataDir::Plain("/data/a".to_string())],
                disk_degraded_ratio: bad,
                ..Default::default()
            };
            assert!(c.validate().is_err(), "占比 {bad} 必须被拒");
        }
        let one = StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            disk_degraded_ratio: 1.0,
            ..Default::default()
        };
        assert!(one.validate().is_ok(), "1.0 合法(要求该轮全部 IO 都慢)");
    }

    /// DEGRADED 的默认值闸门:阈值 `0 = 关`(新判定口径,先观测一周期再开)。
    /// 改它必须显式改本测试(CLAUDE.md §5.1)。
    #[test]
    fn degraded_defaults_are_off_pending_calibration() {
        let c = StoreServerConfig::default();
        assert_eq!(c.disk_degraded_io_ms, 0, "DEGRADED 默认关");
        assert_eq!(c.disk_degraded_ratio, 0.1, "占比默认 10%");
    }

    /// **按 op 分档的看门狗门槛**:已知 op 可设、可覆盖默认;拼错的 op 与 0 值**当场拒**
    /// (拼错会让你以为设了门槛其实毫无作用,而这类错误在运行期完全无声)。
    #[test]
    fn watchdog_per_op_overrides_accept_known_ops_and_reject_typos() {
        use std::collections::BTreeMap;

        let base = || StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..Default::default()
        };

        // 默认:空表 ⇒ 所有 op 用同一个门槛。
        let c = base();
        assert!(c.disk_watchdog_ms_by_op.is_empty());
        assert_eq!(c.watchdog_ms_for("get"), c.disk_watchdog_ms);
        assert_eq!(c.watchdog_ms_for("compaction"), c.disk_watchdog_ms);

        // 覆盖最慢的那类大 IO;其余照旧。
        let c = StoreServerConfig {
            disk_watchdog_ms_by_op: BTreeMap::from([
                ("compaction".to_string(), 300_000u64),
                ("flush".to_string(), 120_000),
            ]),
            ..base()
        };
        assert!(c.validate().is_ok(), "已知 op 应通过");
        assert_eq!(c.watchdog_ms_for("compaction"), 300_000);
        assert_eq!(c.watchdog_ms_for("flush"), 120_000);
        assert_eq!(c.watchdog_ms_for("get"), 30_000, "没覆盖的走默认");

        // 拼错的 op:拒。
        let typo = StoreServerConfig {
            disk_watchdog_ms_by_op: BTreeMap::from([("compation".to_string(), 1000u64)]),
            ..base()
        };
        assert!(
            typo.validate().is_err(),
            "拼错的 op 必须被拒(否则门槛静默失效)"
        );

        // 0 值:拒(0 会让该 op 的每一发 IO 都算超期)。
        let zero = StoreServerConfig {
            disk_watchdog_ms_by_op: BTreeMap::from([("get".to_string(), 0u64)]),
            ..base()
        };
        assert!(zero.validate().is_err(), "0 门槛必须被拒");

        // 覆盖值**小于**默认是合法的(给该 op 更紧的门槛);tick 会自动取最小门槛。
        let tighter = StoreServerConfig {
            disk_watchdog_ms_by_op: BTreeMap::from([("get".to_string(), 5_000u64)]),
            ..base()
        };
        assert!(tighter.validate().is_ok(), "更紧的门槛合法");
        assert_eq!(tighter.watchdog_ms_for("get"), 5_000);
    }

    /// 从 TOML 加载分档表(走真加载器,不是拿 serde 手搓):`[store_server.disk_watchdog_ms_by_op]`。
    #[test]
    fn watchdog_per_op_overrides_load_from_toml() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("store.toml");
        std::fs::write(
            &f,
            "[store_server]\n\
             data_dirs = [\"/data/d1\"]\n\
             disk_watchdog_ms = 20000\n\
             [store_server.disk_watchdog_ms_by_op]\n\
             compaction = 300000\n\
             flush = 120000\n",
        )
        .unwrap();
        let cfg: StoreServerConfig = antfs_config::Loader::new()
            .with_file(f.to_string_lossy().into_owned(), true)
            .load_table("store_server")
            .expect("分档表应可加载");
        assert_eq!(cfg.watchdog_ms_for("compaction"), 300_000);
        assert_eq!(cfg.watchdog_ms_for("flush"), 120_000);
        assert_eq!(cfg.watchdog_ms_for("get"), 20_000, "默认值被显式配置覆盖");
        assert!(cfg.validate().is_ok());
    }

    /// 健康闸门 / 看门狗参数的校验:
    /// - `disk_watchdog_ms = 0` 会让**每一发** IO 都算超期(整节点瞬间被摘空)⇒ 必须拒;
    /// - 进入判据 0 轮 / 0 秒等于"一发坏观测就摘盘",正是迟滞要防的事 ⇒ 必须拒;
    /// - 逃生门槛要么 0(永不升级),要么 **≥** 看门狗 deadline,否则"刚超期一次"就被判成
    ///   "永远不会回来了";
    /// - `recover_stable_secs = 0` 合法(只按连续好轮数复位)。
    #[test]
    fn disk_health_validation_rejects_dangerous_values() {
        let base = || StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..Default::default()
        };
        assert!(base().validate().is_ok(), "默认配置应合法");

        for (field, cfg) in [
            (
                "disk_watchdog_ms",
                StoreServerConfig {
                    disk_watchdog_ms: 0,
                    ..base()
                },
            ),
            (
                "disk_health_bad_rounds",
                StoreServerConfig {
                    disk_health_bad_rounds: 0,
                    ..base()
                },
            ),
            (
                "disk_health_bad_secs",
                StoreServerConfig {
                    disk_health_bad_secs: 0,
                    ..base()
                },
            ),
            (
                "disk_health_good_rounds",
                StoreServerConfig {
                    disk_health_good_rounds: 0,
                    ..base()
                },
            ),
        ] {
            assert!(cfg.validate().is_err(), "{field} 的危险取值必须被拒");
        }

        // 逃生门槛低于 deadline(30s)⇒ 拒;等于或大于 ⇒ 通过;0 ⇒ 通过(永不升级)。
        let too_low = StoreServerConfig {
            disk_watchdog_escalate_secs: 5,
            ..base()
        };
        assert!(too_low.validate().is_err(), "5s < 30s deadline,必须被拒");
        for ok in [0, 30, 600] {
            let cfg = StoreServerConfig {
                disk_watchdog_escalate_secs: ok,
                ..base()
            };
            assert!(cfg.validate().is_ok(), "escalate={ok} 应合法");
        }

        // 复位稳定期 0 = 只按连续好轮数复位 ⇒ 合法。
        let no_stable = StoreServerConfig {
            disk_health_recover_stable_secs: 0,
            ..base()
        };
        assert!(no_stable.validate().is_ok(), "稳定期 0 合法");
    }

    /// 后台巡检默认:关 / 60s / 单 lane。改动这几个默认值必须显式改本测试(CLAUDE.md §5.1)。
    #[test]
    fn scrub_background_defaults_off_and_gentle() {
        let d = StoreServerConfig::default();
        assert!(!d.scrub_background_enabled, "默认必须关(后台 IO 不加塞)");
        assert_eq!(d.scrub_background_interval_secs, 60);
        assert_eq!(d.scrub_background_max_in_flight, 1);
        let with_disk = StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..d
        };
        assert!(with_disk.validate().is_ok());
    }

    /// 后台巡检数值下界:间隔 0(忙转)与 lane 0(形同没开)必须当场拒 —— 拼错值起不来,
    /// 好过静默跑成一个错误的节拍。
    #[test]
    fn scrub_background_bounds_reject_zero() {
        let base = || StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..Default::default()
        };
        let interval0 = StoreServerConfig {
            scrub_background_interval_secs: 0,
            ..base()
        };
        assert!(interval0.validate().is_err(), "间隔 0 = 忙转,必须拒");
        let lanes0 = StoreServerConfig {
            scrub_background_max_in_flight: 0,
            ..base()
        };
        assert!(lanes0.validate().is_err(), "lane 0 = 形同没开,必须拒");
    }

    /// 改造后的 compaction 默认值:每天凌晨一点一次、阈值 30%、RW 纳入、墓碑保留 24h。
    /// 改动这几个默认值必须显式改本测试(CLAUDE.md §5.1)。
    #[test]
    fn compaction_defaults_match_documented_policy() {
        let c = StoreServerConfig::default();
        assert_eq!(c.compaction_interval_secs, 86_400, "默认一天一次");
        assert_eq!(c.compaction_start_time, "01:00", "默认凌晨一点首跑");
        assert_eq!(c.compaction_dead_ratio, 0.3, "默认可回收占比阈值 30%");
        assert!(c.compaction_include_rw, "RW 分区默认纳入候选集");
        assert_eq!(
            c.compaction_tombstone_retention_secs, 86_400,
            "墓碑保留 24h"
        );
        assert_eq!(parse_hhmm(&c.compaction_start_time).unwrap(), 3600);
    }

    #[test]
    fn compaction_policy_defaults_and_validation() {
        let base = || StoreServerConfig {
            data_dirs: vec![DataDir::Plain("/data/a".to_string())],
            ..Default::default()
        };
        let c = base();
        assert_eq!(c.compaction_policy, CompactionPolicyKind::Hybrid);
        assert_eq!(c.compaction_policy_switch_days, 6);
        assert_eq!(c.compaction_min_log_segment_count_to_reclaim, 1);
        assert!(c.validate().is_ok());

        let bad = StoreServerConfig {
            compaction_policy_switch_days: 0,
            ..base()
        };
        assert!(bad.validate().is_err(), "switch_days=0 must be rejected");
        let bad = StoreServerConfig {
            compaction_policy_switch_days: 366,
            ..base()
        };
        assert!(bad.validate().is_err(), "switch_days=366 must be rejected");
        let ok = StoreServerConfig {
            compaction_policy_switch_days: 1,
            ..base()
        };
        assert!(ok.validate().is_ok());
        let ok = StoreServerConfig {
            compaction_policy_switch_days: 365,
            ..base()
        };
        assert!(ok.validate().is_ok());

        let bad = StoreServerConfig {
            compaction_min_log_segment_count_to_reclaim: 0,
            ..base()
        };
        assert!(bad.validate().is_err(), "min_reclaim=0 must be rejected");
    }

    #[test]
    fn compaction_policy_kind_deserializes_kebab_case() {
        #[derive(Deserialize)]
        struct W {
            p: CompactionPolicyKind,
        }
        let w = serde_json::from_str::<W>(r#"{"p":"incremental"}"#).unwrap();
        assert_eq!(w.p, CompactionPolicyKind::Incremental);
        let w = serde_json::from_str::<W>(r#"{"p":"cost-benefit"}"#).unwrap();
        assert_eq!(w.p, CompactionPolicyKind::CostBenefit);
        let w = serde_json::from_str::<W>(r#"{"p":"hybrid"}"#).unwrap();
        assert_eq!(w.p, CompactionPolicyKind::Hybrid);
        assert!(
            serde_json::from_str::<W>(r#"{"p":"magic"}"#).is_err(),
            "unknown policy must be rejected"
        );
    }
}
