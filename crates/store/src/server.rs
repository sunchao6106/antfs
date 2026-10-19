//! 数据节点装配 + HTTP 数据面(`axum`,D2)。
//!
//! store 引擎是**同步阻塞**(路线乙),故 async handler 里的引擎 IO 一律 offload,**不阻塞 reactor**;
//! 且自 N4/D1 起按盘归口到 [`crate::disk_pool`] 的**每盘有界池**(本盘自己的 worker 线程 + 有界队列),
//! 不再共用进程级阻塞池 —— 一块盘挂死只占自己的线程,池满则短暂排队后 503。

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use antfs_blob::StoreKey;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use tokio_stream::StreamExt;

use crate::compaction_jobs::{CompactionJob, CompactionJobs, CompactionResult};
use crate::config::StoreServerConfig;
use crate::disk::DiskManager;
use crate::error::Result;
use crate::metrics::{size_bucket, StoreServerMetrics};
use crate::replica::ReplicaManager;
use crate::replication::{token_from, MetadataResponse};
use crate::request_control::{self, DataOp, Gate, RequestControl};
use crate::PartitionId;

#[derive(Clone)]
struct AppState {
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    registry: antfs_metric::Registry,
    /// 节点级请求静默(与 `StoreServer` 共享同一份,`serve()` 不另建)。
    request_control: Arc<RequestControl>,
    /// A7 运行期复制开关:暂停后本节点**主动拉取**跳过本轮(别人来拉不受影响)。
    replication_paused: Arc<AtomicBool>,
    /// 后台巡检开关:暂停后蜗牛爬循环空转跳批(**不动**手动 verify/scrub-report 端点 ——
    /// 应急检查能力必须常在)。家风同 `replication_paused`:不持久化,重启即恢复。
    scrub_paused: Arc<AtomicBool>,
    /// `scrub_max_batch_bytes`:verify 缺省扫描上限(来自配置,避免 handler 再持 config)。
    scrub_max_batch_bytes: u64,
    /// 慢请求观测阈值(= `disk_health_timeout_secs`,避免为纯观测新增配置):端到端超过它的请求
    /// 计一次 `disk_slow_requests{disk_id,op}`。**纯观测,不做拒绝**。
    slow_op_threshold: Duration,
    /// A3 手动 compaction 的异步任务台账(内存态,最多保留最近 [`MAX_TRACKED_JOBS`] 条)。
    compactions: Arc<CompactionJobs>,
    /// 手动 compaction 的墓碑保留期(毫秒):与定时 compaction 同一套回收口径
    /// (手动触发不享受"豁免保留期"—— 保留期防的是复制复活,与谁发起无关)。
    compaction_tombstone_retention_ms: i64,
    /// 单请求 body 上限(字节):流式 put 用它做 `Content-Length` 前置校验,缓冲回落路径用它做抽取上限。
    max_body_bytes: u64,
    /// **每盘有界池**(D1/N4):所有引擎 IO 按盘归口到这里执行,不再挤进程级阻塞池。
    pool: Arc<crate::disk_pool::DiskPools>,
    /// 每盘**可服务性**判定(D2 健康闸门的数据源):探测与看门狗**写**,数据面闸门 / 心跳 / 后台
    /// worker / 拉复制**读**。`available == false` 的盘上,新请求一律 503 快失败。
    health: crate::disk_health::DiskHealthMap,
    /// 未接 master 的单机模式(`master_addr` 为空):副本的**墓碑回收闸门**恒放行 —— 没有别的副本
    /// 可滞后。集群模式则只在 master 放行时开(见 [`crate::worker::tombstones_allowed`])。
    standalone: bool,
    /// 存量回填(`store-composed-backfill`)的单飞闸:同一时刻至多一轮扫在本节点跑。
    backfill_in_flight: Arc<AtomicBool>,
}

impl AppState {
    /// 手动 compact 的策略:运维强制动作,故**不受 partition 状态/可回收占比闸门约束**
    /// (阈值给 0 ⇒ 有可回收数据就动手),但回收口径与定时 compaction 一致:TTL 过期对象与
    /// 过保留期的墓碑同样被真正回收,保留期内的墓碑照搬。
    ///
    /// **墓碑回收闸门照样过**(`tombstones_allowed`):"还有副本没追平"时回收墓碑会造僵尸数据,
    /// 这条不因为"是运维手动发起"就豁免(CLAUDE.md §3.1)。
    fn manual_compact_policy(&self, tombstones_allowed: bool) -> antfs_blob::CompactPolicy {
        antfs_blob::CompactPolicy::new(
            0.0,
            crate::worker::now_epoch_ms(),
            self.compaction_tombstone_retention_ms,
        )
        .with_tombstones_allowed(tombstones_allowed)
    }
}

/// 装配好的数据节点。
pub struct StoreServer {
    config: StoreServerConfig,
    state: AppState,
    disks: DiskManager,
}

impl StoreServer {
    /// 挂盘、建 store 引擎依赖、装配 replica 管理器(用默认 [`antfs_blob::StoreConfig`])。
    pub fn start(config: StoreServerConfig, registry: &antfs_metric::Registry) -> Result<Self> {
        Self::start_with(config, antfs_blob::StoreConfig::default(), registry)
    }

    /// 同 [`start`](Self::start),但显式传入 store 引擎配置(测试/小部署可用小段大小)。
    pub fn start_with(
        config: StoreServerConfig,
        store_config: antfs_blob::StoreConfig,
        registry: &antfs_metric::Registry,
    ) -> Result<Self> {
        antfs_config::Validate::validate(&config)?;
        // 启动自检(openspec 7.2 / `cluster.md` §9.5 A):盘故障的两条**被动**输入默认都是关的 ——
        // 设备层 EIO 不会让引擎自关、主动探测也不跑;此时"某块盘 hang"只能靠 IO 看门狗兜,而看门狗
        // 拿不到 errno,只能超时隔离。这里只说清楚当前状态,**不动默认值**(CLAUDE.md §5.1)。
        if store_config.io_error_count_to_trigger_shutdown == 0 || !config.disk_health_enabled {
            tracing::warn!(
                io_error_shutdown_at = store_config.io_error_count_to_trigger_shutdown,
                active_probing = config.disk_health_enabled,
                "startup self-check: both passive disk-failure inputs are off — device-level \
                 EIO will not shut the engine down (recommend enabling \
                 store.io_error_count_to_trigger_shutdown, 3-10), and active disk probing is \
                 off too (store_server.disk_health_enabled); only the IO watchdog can produce \
                 UNAVAILABLE in this state. \
                 See docs/design/cluster.md §9.2 and §9.5"
            );
        }
        let disks = DiskManager::mount_with(&config.data_dirs, staging_dir(&config))?;
        let store_metrics = antfs_blob::StoreMetrics::new(registry)?;
        let metrics = Arc::new(StoreServerMetrics::new(registry)?);
        let replicas = Arc::new(ReplicaManager::new(
            disks.clone(),
            store_config,
            store_metrics,
            config.partition_capacity_bytes,
        ));
        let health = crate::disk_health::new_map();
        // 启动就把每块盘摆成 `UNKNOWN`(从未观测):否则"探测没开 / 一轮还没跑"的盘在 `/metrics`
        // 上一条 series 都没有,看板上的空白既可能是"没探过"也可能是"抓取坏了"。
        crate::disk_health::init_state_gauges(
            disks.disks().iter().map(|d| d.id.as_str()),
            &metrics,
        );
        let request_control =
            Arc::new(RequestControl::new(config.request_control_default_ttl_secs));
        // A7 复制开关:与 `request_control` 同风格,起时全开(false = 正常拉复制)。
        let replication_paused = Arc::new(AtomicBool::new(false));
        // 后台巡检开关:同风格,起时全开(false = 正常轮转;只有 enabled 时才起循环)。
        let scrub_paused = Arc::new(AtomicBool::new(false));
        // A3 compaction 台账:进程内内存态,重启即丢。
        let compactions = Arc::new(CompactionJobs::new());
        // 先取出标量再 move `config`(handlers 需要 body 上限做流式 put 的前置校验)。
        let max_body_bytes = config.max_body_bytes;
        let scrub_max_batch_bytes = config.scrub_max_batch_bytes;
        // 慢请求观测阈值直接复用探测超时:纯观测项不为它单开配置(改配置就要动文档/默认值)。
        let slow_op_threshold = Duration::from_secs(config.disk_health_timeout_secs);
        let standalone = config.master_addr.is_empty();
        let compaction_tombstone_retention_ms =
            (config.compaction_tombstone_retention_secs as i64).saturating_mul(1000);
        // 每盘池:按配置为每块盘起自己的 worker 线程(建不起来=起不来,宁可当场失败)。
        // 池也拿一份健康表:任务的**开跑前检查**要读它(见 `disk_pool` 的说明)。
        let pool = Arc::new(crate::disk_pool::DiskPools::new(
            &disks,
            &config,
            &metrics,
            health.clone(),
        )?);
        Ok(StoreServer {
            config,
            state: AppState {
                replicas,
                metrics,
                registry: registry.clone(),
                request_control,
                replication_paused,
                scrub_paused,
                scrub_max_batch_bytes,
                slow_op_threshold,
                compactions,
                compaction_tombstone_retention_ms,
                max_body_bytes,
                pool,
                health,
                standalone,
                backfill_in_flight: Arc::new(AtomicBool::new(false)),
            },
            disks,
        })
    }
    /// axum 路由(测试可直接拿去 `oneshot`;生产用 [`serve`](Self::serve))。
    ///
    /// 数据面路由先合并再挂两层:内层 `DefaultBodyLimit`(按 [`StoreServerConfig::max_body_bytes`]),
    /// 外层 `gate`(请求静默)。`gate` 在 extractor **之前**短路,故被禁的 put 连 body 都不缓冲;
    /// `/metrics` 与 `/admin/**` 是独立路由,不经过 `gate`(且 [`request_control::classify`] 也判其豁免)。
    pub fn router(&self) -> Router {
        let data_plane = Router::new()
            .route(
                "/partitions/{pid}/blobs/{key}",
                put(put_blob).get(get_blob).delete(delete_blob),
            )
            .route(
                "/partitions/{pid}/blobs/{key}/metadata",
                get(get_blob_metadata),
            )
            .route("/partitions/{pid}/replica-metadata", get(replica_metadata))
            .route("/partitions/{pid}/scrub-report", get(scrub_report))
            .route("/partitions/{pid}/verify", get(verify_partition))
            .route("/partitions/{pid}/segments", get(list_segments))
            .route("/partitions/{pid}/segments/{name}", get(get_segment))
            .layer(DefaultBodyLimit::max(body_limit(
                self.config.max_body_bytes,
            )))
            .layer(middleware::from_fn_with_state(self.state.clone(), gate));
        Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/admin/partitions/{pid}", post(create_replica))
            .route("/admin/partitions/{pid}/compact", post(compact_partition))
            .route("/admin/compactions", get(list_compactions))
            .route("/admin/compactions/{job_id}", get(get_compaction))
            .route(
                "/admin/request-control",
                post(set_request_control).get(get_request_control),
            )
            .route(
                "/admin/replication-control",
                post(set_replication_control).get(get_replication_control),
            )
            .route(
                "/admin/scrub-control",
                post(set_scrub_control).get(get_scrub_control),
            )
            .route("/admin/backfill-composed", post(backfill_composed))
            .merge(data_plane)
            .with_state(self.state.clone())
    }

    /// 绑定 `http_addr` 后台运行数据面 + (若配置)接入 master 的注册/心跳 + (若开启)主动磁盘探测。
    pub async fn serve(&self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.config.http_addr).await?;
        let router = self.router();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        if self.config.disk_health_enabled {
            crate::health::spawn_prober(
                self.disks.disks().to_vec(),
                std::time::Duration::from_secs(self.config.disk_health_interval_secs),
                std::time::Duration::from_secs(self.config.disk_health_timeout_secs),
                self.state.health.clone(),
                self.config.hysteresis(),
                self.state.metrics.clone(),
            );
        } else {
            // 默认部署走的就是这条路:探测关着,但**看门狗照开**(见 `disk_watchdog_ms` 的字段注释)。
            // 提示语不在这里打 —— 它要和"引擎自关阈值"一起说才完整,故统一挪到 `start_with` 的启动自检。
        }
        // 看门狗:**独立于每盘池**(否则它自己会被卡死在同一批线程里),也**不受探测开关约束**。
        crate::disk_health::spawn_watchdog(
            self.state.pool.clone(),
            self.state.health.clone(),
            self.config.hysteresis(),
            crate::disk_health::WatchdogSettings {
                default_deadline: std::time::Duration::from_millis(self.config.disk_watchdog_ms),
                by_op_ms: self.config.disk_watchdog_ms_by_op.clone(),
                escalate_secs: self.config.disk_watchdog_escalate_secs,
                // DEGRADED:阈值 0 = 本部署没开 ⇒ `None`,看门狗连增量计数都不读(零开销)。
                degraded: (self.config.disk_degraded_io_ms > 0).then(|| {
                    let cfg = self.config.clone();
                    Arc::new(move |slow: u64, total: u64| cfg.degraded_round(slow, total))
                        as crate::disk_health::LatencyJudge
                }),
            },
            self.state.metrics.clone(),
        );
        if !self.config.master_addr.is_empty() {
            crate::heartbeat::spawn_cluster_client(
                self.config.clone(),
                self.disks.clone(),
                self.state.replicas.clone(),
                self.state.health.clone(),
                self.state.metrics.clone(),
            );
            if self.config.replication_enabled {
                crate::replication::spawn_replicator(
                    self.config.clone(),
                    self.state.replicas.clone(),
                    self.state.metrics.clone(),
                    self.state.replication_paused.clone(),
                    self.state.pool.clone(),
                    self.state.health.clone(),
                );
            }
        }
        if self.config.local_worker_enabled {
            crate::worker::spawn_local_worker(
                self.config.clone(),
                self.state.replicas.clone(),
                self.state.metrics.clone(),
                self.state.pool.clone(),
                self.state.health.clone(),
            );
        }
        if self.config.scrub_background_enabled {
            crate::scrub_worker::spawn_scrub_worker(
                self.config.clone(),
                self.state.replicas.clone(),
                self.state.metrics.clone(),
                self.state.pool.clone(),
                self.state.health.clone(),
                self.state.scrub_paused.clone(),
            );
        }
        Ok(())
    }

    /// 每盘有界池(自省/测试用:例如验证"池满 → 503 + Retry-After")。
    pub fn disk_pools(&self) -> Arc<crate::disk_pool::DiskPools> {
        self.state.pool.clone()
    }

    /// 每盘可服务性表(自省/测试用:故障注入 = 直接把某块盘喂成 `UNAVAILABLE`)。
    pub fn disk_health(&self) -> crate::disk_health::DiskHealthMap {
        self.state.health.clone()
    }

    /// 本节点的数据面指标集(测试/自省用:直接从被测对象真正接入的那个 `Registry` 取,**不必**
    /// 绕道 `/metrics` 文本 —— 需要类型化句柄的断言(例如"跳过计数 +1")只能走这里)。
    pub fn metrics(&self) -> Arc<StoreServerMetrics> {
        self.state.metrics.clone()
    }

    /// 本节点生效的配置(测试/自省用:例如按真实配置取迟滞参数,而不是在测试里另抄一份默认值)。
    pub fn config(&self) -> &StoreServerConfig {
        &self.config
    }

    /// **故障注入(运维演练 / 集成测试用)**:让该盘**立刻**判定为 `UNAVAILABLE`。
    ///
    /// 走的是**与生产完全相同**的迟滞状态机(喂 `disk_health_bad_rounds` 次合成坏观测),不绕过任何
    /// 判据 —— 只是省掉"真把一块盘弄坏"这件事(真 D 状态在测试里造不出来,也会把测试机弄脏)。
    ///
    /// **生产代码里没有任何调用点**。误用的代价是"该盘请求全部 503,且不会自行恢复"(状态是内存态,
    /// 只被后续观测推进)⇒ 只能 [`force_disk_available`](Self::force_disk_available) 或重启进程。
    /// 故它只该出现在演练与测试里。
    pub fn force_disk_unavailable(&self, disk_id: &str) -> u32 {
        let h = self.config.hysteresis();
        for _ in 0..h.bad_rounds.max(1) {
            self.feed_disk(disk_id, crate::health::DiskHealth::IoTimeout, &h);
        }
        h.bad_rounds.max(1)
    }

    /// 故障注入的另一半(演练 / 测试):让该盘**立刻**恢复可服务 —— 喂 `good_rounds` 次合成健康观测。
    ///
    /// 注意 [`Self::force_disk_unavailable`] 的恢复门槛还含 `disk_health_recover_stable_secs`
    /// ("稳定满 T2 秒");本方法只喂**轮数**那一半。想让它立刻生效,把该配置置 0(演练/测试配置),
    /// 否则它就是"连续 N 轮好、但稳定期未满 ⇒ 仍不可用" —— 那时**行为是正确的**,不要在测试里绕开它。
    pub fn force_disk_available(&self, disk_id: &str) -> u32 {
        let h = self.config.hysteresis();
        for _ in 0..h.good_rounds.max(1) {
            self.feed_disk(disk_id, crate::health::DiskHealth::Healthy, &h);
        }
        h.good_rounds.max(1)
    }

    /// 把一次合成观测喂进迟滞状态机(与探测/看门狗同一条路径、同一份副作用)。
    fn feed_disk(
        &self,
        disk_id: &str,
        verdict: crate::health::DiskHealth,
        h: &crate::disk_health::Hysteresis,
    ) {
        crate::disk_health::observe_disk(
            &self.state.health,
            &self.state.metrics,
            disk_id,
            verdict,
            h,
        );
    }

    /// 本地 replica 管理器(测试/S2 心跳用)。
    pub fn replicas(&self) -> Arc<ReplicaManager> {
        self.state.replicas.clone()
    }

    /// 节点级请求静默(测试/运维端点用)。
    pub fn request_control(&self) -> Arc<RequestControl> {
        self.state.request_control.clone()
    }
}

/// 配置里的暂存目录 → `Option<PathBuf>`:空串 = 用各盘默认的 `tmp/` 子目录。
fn staging_dir(config: &StoreServerConfig) -> Option<std::path::PathBuf> {
    (!config.staging_dir.is_empty()).then(|| std::path::PathBuf::from(&config.staging_dir))
}

/// 配置的 body 上限 → `usize`;超出 `usize`(仅 32 位平台)`usize::MAX` 等价于不限制。
fn body_limit(max_body_bytes: u64) -> usize {
    usize::try_from(max_body_bytes).unwrap_or(usize::MAX)
}

/// 数据面请求门。两层,顺序固定:
///
/// 1. **盘健康闸门(D2)**:任何 `/partitions/{pid}/...` 请求,只要其**本地副本所在的盘**已被判定
///    不可服务 ⇒ 立刻 `503`,**不进引擎**。
/// 2. **节点级静默**([`RequestControl`]):命中禁用 → `503` + 说明文本(有剩余时长时带 `Retry-After`)。
///
/// ## 为什么盘闸门**不**用 `classify` 的豁免清单(与静默闸门的关键区别)
///
/// [`request_control::classify`] 刻意**豁免**拉复制读 / `replica-metadata` / `segments*` ——
/// 那是为了"别把自己变成收不到数据的孤岛"。**盘健康闸门不能沿用那套豁免**:从一块已经坏了的盘上
/// 拉数据/拉段**本来就拉不动**,放它进去只会再占一条卡死的线程、再漏一个,对端也什么都拿不到。
/// 所以这里只看"路径是不是 `/partitions/{pid}/...`",**不分 op**(`path_op` 只用于打点标签)。
///
/// ## 仍然豁免 `/metrics` 与任何 `/admin/**`
///
/// 理由与 `RequestControl` 的非目标 #2 一模一样:**不能做出一个无法远程恢复的节点**;盘坏了运维更
/// 要能看见、能下发指令。这一条由路由结构保证 —— 本中间件只挂在数据面子 router 上,`/metrics` 与
/// `/admin/**` 是**另外**的路由,从不经过这里(下面还显式判一次路径前缀,防止后人把路由合并回去)。
///
/// 被拒请求**不**计入 `errors_total` / `request_duration` / `in_flight`(与静默同一口径)。
async fn gate(State(st): State<AppState>, req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path();
    if path.starts_with("/partitions/") {
        if let Some(pid) = request_control::partition_of_path(path) {
            // 与 handler 里那一次寻盘是**同一个** `ReplicaManager::get`(不另起一套盘定位逻辑)。
            if let Some(handle) = st.replicas.get(pid) {
                if !crate::disk_health::is_available(&st.health, &handle.disk_id) {
                    let op = request_control::path_op(req.method(), path, req.uri().query());
                    return disk_unavailable_response(&st.metrics, op);
                }
            }
        }
    }
    let (op, partition) = match request_control::classify(req.method(), path, req.uri().query()) {
        Some(op) => (op, request_control::partition_of_path(path)),
        None => return next.run(req).await,
    };
    match st.request_control.check(op, partition) {
        Gate::Open => next.run(req).await,
        Gate::Disabled { retry_after_secs } => {
            st.metrics
                .rejected_requests
                .with_label_values(&[op.as_str(), "disabled"])
                .inc();
            let body = format!("{} is temporarily disabled on this node", op.as_str());
            let mut resp = (StatusCode::SERVICE_UNAVAILABLE, body).into_response();
            if let Some(secs) = retry_after_secs {
                if let Ok(v) = axum::http::HeaderValue::from_str(&secs.to_string()) {
                    resp.headers_mut()
                        .insert(axum::http::header::RETRY_AFTER, v);
                }
            }
            resp
        }
    }
}

/// 盘不可服务 → `503` + `Retry-After: 1`,并计 `rejected_requests_total{op, reason="disk_unavailable"}`。
///
/// `Retry-After` 只给**一秒**:这块盘上现在发来的请求必然失败,提示上游立刻改用别的副本
/// (router 的 W 多数派会换副本重试)。给更长的值反而会拖长上游的恢复时间。
///
/// **不**计 `errors_total`:这不是引擎失败,而是"在进引擎之前就挡下了",与池满 / 人工静默同一口径
/// —— 三者靠 `reason` 区分:**盘不可用** / **池满** / **人工静默**。
fn disk_unavailable_response(metrics: &StoreServerMetrics, op: &str) -> Response {
    metrics
        .rejected_requests
        .with_label_values(&[op, "disk_unavailable"])
        .inc();
    let body = format!("disk unavailable on this node: {op} rejected");
    let mut resp = (StatusCode::SERVICE_UNAVAILABLE, body).into_response();
    if let Ok(v) = axum::http::HeaderValue::from_str("1") {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
}

// 关于"阻塞池水位"这一项观测的取舍记录在此,免得后来者再翻一遍 tokio 源码:
// `num_blocking_threads` / `num_idle_blocking_threads` / `blocking_queue_depth` 都要
// `RUSTFLAGS="--cfg tokio_unstable"`(tokio 的 `cfg_unstable_metrics!`),不为纯观测给全体部署加一条
// 隐藏构建要求。池自身的水位留给 N4(每盘有界池)—— 池归自己管之后,排队深度是我们自己的账。
// 眼下用 `StoreServerMetrics::disk_in_flight`(按盘在途)当等价信号:"某块盘只增不减"就是它。

/// Prometheus 文本导出(无鉴权,供抓取)。
async fn metrics_handler(State(st): State<AppState>) -> Response {
    match antfs_metric::gather(&st.registry) {
        Ok(t) => (StatusCode::OK, t).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn parse_key(key_hex: &str) -> Option<StoreKey> {
    let bytes = hex::decode(key_hex).ok()?;
    StoreKey::from_slice(&bytes)
}

/// store 引擎错误 → 变更端点 HTTP 状态码。
fn map_mut_err(e: &antfs_blob::Error) -> StatusCode {
    match e {
        antfs_blob::Error::NotFound => StatusCode::NOT_FOUND,
        antfs_blob::Error::Deleted | antfs_blob::Error::AlreadyExists => StatusCode::CONFLICT,
        antfs_blob::Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        // 分区写满:507 让压测/运维一眼看出"该加分区了",别混在 500 里(承 Ambry `Store_Full`)。
        antfs_blob::Error::StoreFull => StatusCode::INSUFFICIENT_STORAGE,
        // 参数非法:PUT 路径上只可能来自"单条记录放不进一个段"(如 body 超过 segment_size)。
        // 这是**客户端可控**的问题(该分片/该调 segment 大小),给 400 而不是笼统的 500。
        antfs_blob::Error::BadArgument(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// 数据面请求观测 guard:入口 inc `in_flight` + 起 `request_duration` 计时,
/// drop 时 dec(计时器 drop 自动记录耗时),覆盖含参数校验早退在内的所有出口。
/// [`elapsed`](Self::elapsed) 供按 blob 大小分桶的延迟单独 observe(仅成功路径调用)。
///
/// 另兼**盘维度**打点(纯观测:不改行为、不做拒绝,只为"要不要上每盘隔离"提供现网数据):
/// 拿到副本句柄后经 [`with_disk`](Self::with_disk) 定位盘,此后该请求计入该盘的请求数 / 在飞 / 延迟,
/// 超阈值再计一次慢请求,成功路径经 [`set_bytes`](Self::set_bytes) 计入带宽。
struct OpTimer {
    started: Instant,
    _timer: antfs_metric::HistogramTimer,
    in_flight: antfs_metric::IntGauge,
    /// 慢请求打点的把手(阈值 = `disk_health_timeout_secs`)。
    metrics: StoreServerMetrics,
    op: &'static str,
    threshold: Duration,
    /// 盘维度的打点把手:还没定位到盘时为 `None`(参数校验早退等),此时一个盘维度点都不碰。
    disk: Option<DiskMeters>,
    /// 本请求落在该盘上的字节数(put/get 成功后由 handler 填;0 = 不参与带宽)。
    /// `Cell` 而非 `mut`:填它发生在 [`finish_put`] 这类只拿 `&OpTimer` 的地方。
    bytes: Cell<u64>,
}

/// 一块盘的打点把手(拿到副本句柄后由 [`OpTimer::with_disk`] 一次建好)。
///
/// 归一处理"请求数 / 带宽 / 延迟 / 在飞 / 慢请求"五件的**计数生命周期**:请求数与在飞在
/// [`with_disk`](OpTimer::with_disk) 里 inc,`in_flight` 与延迟(计时器 drop 即 observe)在
/// [`OpTimer`] 的 `Drop` 里收尾,带宽按 `bytes` 结算。
struct DiskMeters {
    disk: String,
    requests: antfs_metric::IntCounter,
    bytes: antfs_metric::IntCounter,
    /// 只为 `Drop` 副作用而持有(计时器 drop 即 observe),故从不读 —— 与 [`OpTimer::_timer`] 同一写法。
    _duration: antfs_metric::HistogramTimer,
    in_flight: antfs_metric::IntGauge,
}

impl OpTimer {
    fn start(metrics: &StoreServerMetrics, op: &'static str, threshold: Duration) -> Self {
        let in_flight = metrics.in_flight.with_label_values(&[op]);
        in_flight.inc();
        let timer = metrics
            .request_duration
            .with_label_values(&[op])
            .start_timer();
        OpTimer {
            started: Instant::now(),
            _timer: timer,
            in_flight,
            metrics: metrics.clone(),
            op,
            threshold,
            disk: None,
            bytes: Cell::new(0),
        }
    }

    /// 定位到具体盘(拿到副本句柄后调):此后本请求计入该盘的请求数 / 在飞 / 延迟,端到端超阈值
    /// 再计一次 `disk_slow_requests{disk_id,op}`,成功路径经 [`set_bytes`](Self::set_bytes) 计入带宽。
    ///
    /// 取 `self` 而非 `&mut self`:调用点拿到句柄后才能定位,写成消费式即可 `let t = t.with_disk(...)`,
    /// 不必把 timer 绑成 `mut`(每个 handler 少一行噪声)。整值移动不会提前触发 `Drop`。
    ///
    /// **每个 handler 只调一次**(它做 inc,重复调会重复计数)。
    fn with_disk(mut self, disk_id: &str) -> Self {
        let meters = DiskMeters {
            disk: disk_id.to_string(),
            requests: self
                .metrics
                .disk_requests
                .with_label_values(&[disk_id, self.op]),
            bytes: self
                .metrics
                .disk_bytes
                .with_label_values(&[disk_id, self.op]),
            _duration: self
                .metrics
                .disk_request_duration
                .with_label_values(&[disk_id, self.op])
                .start_timer(),
            in_flight: self
                .metrics
                .disk_in_flight
                .with_label_values(&[disk_id, self.op]),
        };
        meters.requests.inc();
        meters.in_flight.inc();
        self.disk = Some(meters);
        self
    }

    /// 本请求定位到的盘 id(引擎调用按它提交到该盘的池;未定位 → 空串,池会以 `unknown_disk` 拒掉
    /// —— 正常路径不会发生:`with_disk` 在提交引擎之前一定调过)。
    fn disk_id(&self) -> &str {
        self.disk.as_ref().map(|d| d.disk.as_str()).unwrap_or("")
    }

    /// 记本请求在该盘上搬运的字节数(成功路径调;口径与 `put_bytes_total` / `get_bytes_total` 一致)。
    fn set_bytes(&self, n: u64) {
        self.bytes.set(n);
    }

    /// 自 handler 入口起的耗时(秒);对齐 Ambry「只在 NoError 时按大小分类计数」的口径,
    /// 只应在本请求**成功**时调用(早退/错误路径不 observe)。
    fn elapsed(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }
}

impl Drop for OpTimer {
    fn drop(&mut self) {
        // 只在已定位到盘时才动盘维度打点:`disk` 非空 ⟺ `with_disk` 调过 ⟺ inc 过。
        if let Some(d) = &self.disk {
            d.in_flight.dec();
            let n = self.bytes.get();
            if n > 0 {
                d.bytes.inc_by(n);
            }
            // 慢请求口径是端到端(含阻塞池排队):它回答"哪里在拖长尾",不区分"盘慢"还是"排队慢"。
            if self.started.elapsed() >= self.threshold {
                self.metrics
                    .disk_slow_requests
                    .with_label_values(&[&d.disk, self.op])
                    .inc();
            }
            // `d._duration` 到这里才随字段 drop 而 observe(字段在 `Drop::drop` 之后才析构)。
        }
        self.in_flight.dec();
    }
}

/// 引擎错误 → `store_server_errors_total{kind}` 的 kind:自关(503)与内部错误(500)分开。
fn err_kind(e: &antfs_blob::Error) -> &'static str {
    match e {
        antfs_blob::Error::Unavailable => "unavailable",
        _ => "io",
    }
}

/// store-server 自身错误 → kind(引擎错误透传分类,其余归 io)。
fn store_err_kind(e: &crate::error::Error) -> &'static str {
    match e {
        crate::error::Error::Store(b) => err_kind(b),
        _ => "io",
    }
}

/// 记一次数据面错误(`join_error` = 进程级阻塞池的任务 panic;数据面引擎 IO 已改走每盘池,故现在
/// 只剩"建副本前没有盘 id"那一处会用到它)。
fn record_err(metrics: &StoreServerMetrics, kind: &str) {
    metrics.errors.with_label_values(&[kind]).inc();
}

/// blob 写的查询参数:`expiration_ms` 给定则**写入时即带 TTL**(缺省 = 不过期)。
///
/// 对象不可变,TTL **只在写入时设定**,写后不可改 —— 分片上传的孤儿回收正需要"出生即带 TTL"。
#[derive(serde::Deserialize, Default)]
struct PutParams {
    #[serde(default)]
    expiration_ms: Option<i64>,
    /// 拼装描述符类型位(`?composed=true` 置位);旧 router 不携带该参数时被忽略,零行为变化。
    #[serde(default)]
    composed: Option<bool>,
}

/// 流式 put 的单块缓冲(引擎 `put_from_reader` 内部也是同量级的分块)。
const STAGE_BUF_BYTES: usize = 64 * 1024;

/// **直接进内存**的 body 上界:≤ 该值走内存路径(`Bytes` → `put_ttl`),> 该值才落盘暂存 + 流式写。
///
/// 为什么小对象不走暂存:暂存要 `create/write/close/unlink` 一个文件,每个 PUT 多 ~5 次文件系统元数据操作;
/// 小对象是绝对主流,大并发下这套开销把 PUT p50 从 ~1ms 拖到 ~9ms(devcluster + bench 实测,release)。
/// 内存上界 = 在途请求数 × 该值(默认 1 MiB),仍远小于"整个 blob 缓进内存"(CLAUDE.md §5.3)的旧形态。
const INLINE_BODY_MAX: u64 = 1024 * 1024;

/// 请求声明的 body 长度(`Content-Length`);缺省 = 分块编码或未声明 → 走缓冲回落路径。
fn content_length(req: &axum::extract::Request) -> Option<u64> {
    req.headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// 暂存文件的 **RAII 守卫**,两条删除路径(正常出口 [`remove`](Self::remove) / 取消出口 `Drop`)。
///
/// 为什么不能只在"`.await` 之后手写 `remove_file`":客户端断开或上游取消会让本 handler 的 future 被
/// **drop**,`.await` 之后的清理代码**永远不执行** → 暂存文件漏在盘上。实测(2 MiB 对象 × 3000 次、
/// router 收满 W 后取消在飞请求)漏出 1600+ 个文件。故正常出口**显式等**一次删除(契约:响应发出 ⇒
/// 暂存文件已清,集成测试就断言这一点),取消出口靠 `Drop` 兜底。
///
/// 语义上安全:被取消时 blocking 任务可能仍在读这个文件,Unix 下 unlink 已打开的文件不影响其 fd(磁盘空间
/// 在关闭时释放),引擎该次写可能照样成功 —— 那对副本一致性无害(第 3 份本就靠拉复制补齐)。
///
/// **两条路径都把删除放进阻塞池**:暂存目录在**数据盘**上,坏盘上 `unlink` 同样会挂,而在 reactor 线程上
/// 直调同步 `remove_file` 就是把 HTTP 工作线程押在一块坏盘上。`Drop` 里没有运行时(进程收尾)时直删兜底,
/// 反正启动时 `clean_tmp_dir` 还会清一遍遗留 `*.tmp`。
struct StagedFile {
    path: std::path::PathBuf,
}

impl StagedFile {
    fn new(dir: &std::path::Path, pid: PartitionId) -> Self {
        Self {
            path: dir.join(format!("put_{pid}_{}.tmp", unique_suffix())),
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// 正常出口:删掉暂存文件并**等它完成**(删除跑在阻塞池里,不占 reactor)。
    ///
    /// 先 `take` 路径、再派任务、最后才 `await`:这样即使在 `await` 中途被取消,删除任务也已经在跑
    /// (`JoinHandle` 被丢弃 = 任务分离,不会取消),而 `self` 落下时路径已空、不会重复删。
    async fn remove(mut self) {
        let path = std::mem::take(&mut self.path);
        let handle = tokio::task::spawn_blocking(move || std::fs::remove_file(&path));
        drop(self);
        let _ = handle.await;
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        // `take` 掉路径:即使下面的删除失败,本结构再被 drop 也不会重复删。
        let path = std::mem::take(&mut self.path);
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                // 取消路径不能 await,只丢一个分离任务(删除本身仍跑在阻塞池上)。
                h.spawn(async move {
                    let _ = tokio::fs::remove_file(&path).await;
                });
            }
            Err(_) => {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

/// 进程内唯一的暂存文件名后缀(时间纳秒 + 自增序号;不为它引 `rand` 依赖)。
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{ns}_{n}")
}

/// 把请求体**流式**落到暂存文件,并校验实际字节数恰等于 `Content-Length`。
///
/// 内存占用 = 单块(不随 body 大小涨)。字节数不符(客户端中途断开/超发)一律报错 —— 该次 put
/// 不产生任何可见记录,调用方负责删掉暂存文件。
async fn stage_body(
    body: axum::body::Body,
    path: &std::path::Path,
    declared: u64,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::File::create(path).await?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("body stream: {e}"))
        })?;
        written += chunk.len() as u64;
        if written > declared {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "body longer than Content-Length",
            ));
        }
        file.write_all(&chunk).await?;
    }
    // 只 flush 到 OS,**不 fsync**:暂存文件是传输中转而非持久化边界 —— 记录真正被 ack 是在引擎把
    // 它写进 log 之后(引擎的持久化边界仍是周期 flush)。给中转文件 fsync 不改持久性语义,却把每次
    // PUT 变成一次同步落盘(macOS 上实测 p50 从 ~1ms 恶化到 ~250ms,见 devcluster + bench 记录)。
    file.flush().await?;
    if written != declared {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("body shorter than Content-Length ({written} < {declared})"),
        ));
    }
    Ok(())
}

/// put 结果 → 响应(指标 + 状态码),三条写入路径(流式/缓冲/带 TTL)共用,避免状态码分叉。
fn finish_put(
    metrics: &StoreServerMetrics,
    t: &OpTimer,
    len: u64,
    r: antfs_blob::Result<()>,
) -> Response {
    match r {
        Ok(()) => {
            metrics.put_total.inc();
            metrics.put_bytes.inc_by(len);
            t.set_bytes(len);
            // 成功路径才按 body 长度分桶 observe(对齐 Ambry 只在 NoError 计数)。
            metrics
                .blob_request_duration
                .with_label_values(&["put", size_bucket(len)])
                .observe(t.elapsed());
            StatusCode::CREATED.into_response()
        }
        Err(e) => {
            record_err(metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
    }
}

/// 盘池拒绝 → 响应。两类含义完全不同:
/// - **过载**(`Saturated`)/ 盘池停了(`Closed`)/ 未知盘 → **503 + `Retry-After`**,让 router 立刻改用
///   别的副本;这类"在进引擎之前就被挡下"的请求不进 `errors_total`,只在
///   `store_server_disk_pool_rejected_total{disk_id,op,reason}` 上体现(与 RequestControl 同一口径)。
/// - **任务 panic**(`JobPanicked`)→ **500** 且计 `errors_total{kind="join_error"}`:引擎里炸了是内部
///   错误,与旧的 `spawn_blocking` join 失败同一口径,绝不能包装成"盘忙,重试就行"。
fn pool_reject_response(
    metrics: &StoreServerMetrics,
    op: &str,
    reject: crate::disk_pool::PoolReject,
) -> Response {
    if let crate::disk_pool::PoolReject::JobPanicked = reject {
        record_err(metrics, "join_error");
        return (StatusCode::INTERNAL_SERVER_ERROR, "disk pool job panicked").into_response();
    }
    let mut resp = (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("{op} rejected by disk pool: {}", reject.as_str()),
    )
        .into_response();
    // 短暂排队都没等到空位 ⇒ 该盘已经供不上;`Retry-After: 1` 提示上游退避(router 也会直接换副本)。
    if let Ok(v) = axum::http::HeaderValue::from_str("1") {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
}

/// 盘池提交结果与引擎错误归一(池拒绝 → 503;引擎错误 → 原状态码映射)。
fn joined_put(
    st: &AppState,
    t: &OpTimer,
    len: u64,
    joined: std::result::Result<antfs_blob::Result<()>, crate::disk_pool::PoolReject>,
) -> Response {
    match joined {
        Ok(r) => finish_put(&st.metrics, t, len, r),
        Err(reject) => pool_reject_response(&st.metrics, "put", reject),
    }
}

/// blob 读的查询参数:`replication=1` 时对已删除/过期对象也返回数据体(供复制忠实 apply)。
#[derive(serde::Deserialize, Default)]
struct GetParams {
    #[serde(default)]
    replication: Option<u8>,
}

/// 复制元数据请求参数:`seg`/`off` 为游标(均缺 = 从头);`max_bytes` 批上限。
#[derive(serde::Deserialize)]
struct MetaParams {
    /// 游标 = 段身份(位置 + 代次)+ 段内偏移 + 副本 incarnation;四件套齐全才可信。
    pos: Option<u32>,
    gen: Option<u32>,
    off: Option<u64>,
    incarnation: Option<u64>,
    #[serde(default = "default_max_bytes")]
    max_bytes: u64,
}

fn default_max_bytes() -> u64 {
    4 * 1024 * 1024
}

async fn create_replica(State(st): State<AppState>, Path(pid): Path<PartitionId>) -> Response {
    let replicas = st.replicas.clone();
    match tokio::task::spawn_blocking(move || replicas.ensure_replica(pid)).await {
        Ok(Ok(_)) => {
            st.metrics.set_replica_gauges(&st.replicas.replica_states());
            StatusCode::CREATED.into_response()
        }
        Ok(Err(e)) => {
            record_err(&st.metrics, store_err_kind(&e));
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        // 建副本**前**还没有盘 id(盘由 master 指令/`pick` 定),故这一处仍走进程级阻塞池;
        // 建完之后的读写都会按盘归口到各自池(见 disk_pool)。
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
    }
}

/// `POST /admin/partitions/{pid}/compact` 的查询参数:`?async=1` 走异步任务(立即返回 `job_id`)。
///
/// `async` 是 Rust 关键字,故字段重命名为 `is_async`;取值口径与 `?replication=` 一致
/// (`非 0` 才算),故 `?async=0` 与不带该参数等价 —— 同步路径行为/响应体**完全不变**。
#[derive(serde::Deserialize, Default)]
struct CompactParams {
    #[serde(rename = "async", default)]
    is_async: Option<u8>,
}

/// `POST /admin/partitions/{pid}/compact`:手动触发该 partition 本地副本的全库 compact
/// (对齐 Ambry TriggerCompaction 运维动作);无本地副本 → 404。
///
/// - 同步(缺省):阻塞到 compact 完成,响应体 = `{live_bytes, dead_bytes, segments_before, segments_after}`;
/// - 异步(`?async=1`):立即 200 `{"job_id": N}`,后台在阻塞池里跑,进度经
///   `GET /admin/compactions[/{job_id}]` 查询(台账语义见 [`crate::compaction_jobs`])。
async fn compact_partition(
    State(st): State<AppState>,
    Path(pid): Path<PartitionId>,
    Query(params): Query<CompactParams>,
) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    st.metrics.manual_compactions.inc();
    let store = handle.store.clone();
    // 墓碑回收闸门:按**该副本当前值**走(与定时 compaction 同一判据)。
    let tombstones_allowed = crate::worker::tombstones_allowed(st.standalone, &handle);
    if params.is_async.unwrap_or(0) != 0 {
        let policy = st.manual_compact_policy(tombstones_allowed);
        return submit_compaction(&st, pid, handle.disk_id.clone(), store, policy);
    }
    let policy = st.manual_compact_policy(tombstones_allowed);
    let disk = handle.disk_id.clone();
    match st
        .pool
        .run(&disk, "compact", move || store.compact(&policy))
        .await
    {
        Ok(Ok(stats)) => Json(CompactionResult::from(stats)).into_response(),
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "compact", reject),
    }
}

/// 异步 compact 的提交响应体。
#[derive(serde::Serialize)]
struct CompactionSubmitted {
    job_id: u64,
}

/// `GET /admin/compactions` 的响应体。
#[derive(serde::Serialize)]
struct CompactionsResponse {
    jobs: Vec<CompactionJob>,
}

/// 提交一个异步 compaction 任务:登记台账(running)→ 后台跑 → 回写终态。
///
/// 任务表是内存态(重启即丢);`store` 用 `Arc` 抓住,即便期间该副本被摘除,compaction 也在原
/// 句柄上跑完(结果只进台账,不改语义)。
fn submit_compaction(
    st: &AppState,
    pid: PartitionId,
    disk_id: String,
    store: Arc<antfs_blob::BlobStore>,
    policy: antfs_blob::CompactPolicy,
) -> Response {
    let job_id = st.compactions.submit(pid);
    st.metrics
        .compaction_jobs
        .with_label_values(&["running"])
        .inc();
    let jobs = st.compactions.clone();
    let metrics = st.metrics.clone();
    let pool = st.pool.clone();
    tokio::spawn(async move {
        match pool
            .run(&disk_id, "compact", move || store.compact(&policy))
            .await
        {
            Ok(Ok(stats)) => {
                jobs.finish(job_id, Ok(stats));
                metrics.compaction_jobs.with_label_values(&["done"]).inc();
            }
            Ok(Err(e)) => {
                record_err(&metrics, err_kind(&e));
                jobs.finish(job_id, Err(e.to_string()));
                metrics.compaction_jobs.with_label_values(&["failed"]).inc();
            }
            // 池拒(盘被压满/未知盘):任务如实失败,交给调用方按 `job_id` 查。
            Err(reject) => {
                jobs.finish(job_id, Err(format!("disk pool: {}", reject.as_str())));
                metrics.compaction_jobs.with_label_values(&["failed"]).inc();
            }
        }
    });
    (StatusCode::OK, Json(CompactionSubmitted { job_id })).into_response()
}

/// `GET /admin/compactions`:最近 32 条 compaction 任务(最新在前;更早的已淘汰)。
async fn list_compactions(State(st): State<AppState>) -> Response {
    Json(CompactionsResponse {
        jobs: st.compactions.snapshot(),
    })
    .into_response()
}

/// `GET /admin/compactions/{job_id}`:单条任务;**未知(或已被淘汰的)`job_id` → 404**。
async fn get_compaction(State(st): State<AppState>, Path(job_id): Path<u64>) -> Response {
    match st.compactions.get(job_id) {
        Some(job) => Json(job).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown job_id").into_response(),
    }
}

/// `POST /admin/replication-control` 的请求体。
#[derive(serde::Deserialize)]
struct ReplicationControlBody {
    /// `true` = 暂停本节点主动拉复制,`false` = 恢复。
    paused: bool,
}

/// `GET|POST /admin/replication-control` 的响应体(操作后的开关状态)。
#[derive(serde::Serialize)]
struct ReplicationControlResponse {
    paused: bool,
}

/// `POST /admin/replication-control`:运行期暂停/恢复**本节点主动拉复制**(A7 复制开关)。
///
/// 只影响本节点向 peer 拉取:别人来拉本节点(`?replication=1` / `replica-metadata` / `segments*`)
/// 完全不受影响,与 [`RequestControl`] 静默正交。状态**不持久化**(重启即恢复拉取),
/// 也不动已积累的复制游标(恢复后从原游标续拉)。
async fn set_replication_control(
    State(st): State<AppState>,
    Json(body): Json<ReplicationControlBody>,
) -> Response {
    st.replication_paused.store(body.paused, Ordering::Relaxed);
    st.metrics.replication_paused.set(i64::from(body.paused));
    if body.paused {
        tracing::info!("pull replication paused by operator");
    } else {
        tracing::info!("pull replication resumed by operator");
    }
    Json(ReplicationControlResponse {
        paused: body.paused,
    })
    .into_response()
}

/// `GET /admin/replication-control`:当前复制开关状态。
async fn get_replication_control(State(st): State<AppState>) -> Response {
    Json(ReplicationControlResponse {
        paused: st.replication_paused.load(Ordering::Relaxed),
    })
    .into_response()
}

/// `POST /admin/scrub-control` 的请求体。
#[derive(serde::Deserialize)]
struct ScrubControlBody {
    /// `true` = 暂停后台巡检循环,`false` = 恢复轮转。
    paused: bool,
}

/// `GET|POST /admin/scrub-control` 的响应体(操作后的开关状态)。
#[derive(serde::Serialize)]
struct ScrubControlResponse {
    paused: bool,
}

/// `POST /admin/scrub-control`:运行期暂停/恢复**后台巡检循环**(只掐定时腿)。
///
/// 手动 `GET /partitions/{pid}/verify` / `scrub-report` 不受影响(应急时检查能力必须常在)。
/// 状态**不持久化**(重启即恢复轮转,与 `/admin/replication-control` 同家风),也不动各副本的
/// 续扫游标(恢复后从原处续爬)。循环未起(`scrub_background_enabled = false`)时置位无副作用,
/// 读回照样可见 —— 它表达的是"运维意愿",不是"循环在跑"。
async fn set_scrub_control(
    State(st): State<AppState>,
    Json(body): Json<ScrubControlBody>,
) -> Response {
    st.scrub_paused.store(body.paused, Ordering::Relaxed);
    st.metrics
        .scrub_background_paused
        .set(i64::from(body.paused));
    if body.paused {
        tracing::info!("background scrub loop paused by operator");
    } else {
        tracing::info!("background scrub loop resumed by operator");
    }
    Json(ScrubControlResponse {
        paused: body.paused,
    })
    .into_response()
}

/// `GET /admin/scrub-control`:当前后台巡检开关状态。
async fn get_scrub_control(State(st): State<AppState>) -> Response {
    Json(ScrubControlResponse {
        paused: st.scrub_paused.load(Ordering::Relaxed),
    })
    .into_response()
}

/// `POST /admin/backfill-composed` 的响应:一轮存量回填的报告。
#[derive(serde::Serialize, Default)]
struct BackfillReport {
    /// 参与的本地副本数(uptime 中注册在册的)。
    replicas: u32,
    /// 逐条过目的赢家条目总数(含跳过项)。
    scanned: u64,
    /// 通过候选过滤(未删、未过期、无位且 ≤ 描述符上限)进入甄别的条目数。
    candidates: u64,
    /// 置换落位的存量真描述符。
    migrated: u64,
    /// 来路上已有位(幂等重跑的常规形态)。
    already: u64,
    /// 赢家是墓碑(如实计数;回填绝不复活)。
    deleted: u64,
    /// MAGIC 形似但 parse 失败(普通对象,如 `ASTC` 前缀文件——**不落位**,是 ① 修复的前提)。
    invalid: u64,
    /// 赢家尚在 TTL 窗内但 expired 已真(留待压实回收,行为同今)。
    expired: u64,
    /// 读写失败(前 16 条,含 pid/key/错误)。
    failed: Vec<BackfillFailure>,
}

/// 一条失败明细(报告里截前 [`MAX_BACKFILL_FAILURES`] 条,防超大响应体)。
#[derive(serde::Serialize)]
struct BackfillFailure {
    partition_id: PartitionId,
    key: String,
    error: String,
}

const MAX_BACKFILL_FAILURES: usize = 16;

/// 存量描述符回填(`store-composed-backfill`):对本节点全部副本做**一次性**同步扫,
/// 把"目标已补、位未落"的存量描述符经引擎置换写补上 `FLAG_COMPOSED`(甄别 = 共享
/// `antfs-descriptor` 的同一 parse,严格口径见 change design D3;**不校验 chunk 引用**,
/// 行为奇偶校验:除 `ASTC` 前缀普通文件(不落位,供 frontend `composed_flag_only` 翻转
/// 修复失真①)外,一切对象读回行为与回填前逐字节相同)。
///
/// 家风:单飞闸(并发 → 409);幂等可续跑 = 构造所得(重跑时已完成条目计 `already`,
/// 被删条目计 `deleted`);**无需停写**(check-then-publish 在引擎写锁内原子,无复活
/// 窗口);建议错峰执行(置换瞬时使每条候选记录字节翻倍,压实自然回收)。零新配置、
/// 零新监控 —— 报告即响应,tracing 记每副本汇总与失败明细。
async fn backfill_composed(State(st): State<AppState>) -> Response {
    if st
        .backfill_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            "a composed backfill sweep is already running on this node",
        )
            .into_response();
    }
    let replicas = st.replicas.clone();
    let in_flight = st.backfill_in_flight.clone();
    let result = tokio::task::spawn_blocking(move || run_composed_backfill(&replicas)).await;
    in_flight.store(false, Ordering::Release);
    match result {
        Ok(report) => Json(report).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("backfill sweep panicked: {e}"),
        )
            .into_response(),
    }
}

/// 扫描本体(阻塞线程里跑):副本间串行、单副本内单遍过索引 —— 一次性运维动作,
/// 写放大上界 = 候选描述符字节总量(每条 ≤ 1 MiB),与蜗牛家风无冲突。
fn run_composed_backfill(replicas: &ReplicaManager) -> BackfillReport {
    let mut rep = BackfillReport::default();
    let mut handles = replicas.handles();
    handles.sort_by_key(|(pid, _)| *pid);
    let now_ms = crate::worker::now_epoch_ms();
    for (pid, handle) in &handles {
        let before = (
            rep.scanned,
            rep.migrated,
            rep.already,
            rep.invalid,
            rep.deleted,
            rep.expired,
        );
        let store = handle.store.clone();
        store.for_each_entry_winner(|key, v| {
            rep.scanned += 1;
            if v.is_deleted() {
                rep.deleted += 1;
            } else if v.is_composed() {
                rep.already += 1;
            } else if v.is_expired_at(now_ms) {
                rep.expired += 1;
            } else if v.size_bytes() <= antfs_descriptor::MAX_BYTES {
                match sweep_one(*pid, &store, &key) {
                    SweepOutcome::CandidateMigrated => {
                        rep.candidates += 1;
                        rep.migrated += 1;
                    }
                    SweepOutcome::CandidateAlready => {
                        rep.candidates += 1;
                        rep.already += 1;
                    }
                    SweepOutcome::CandidateDeleted => {
                        rep.candidates += 1;
                        rep.deleted += 1;
                    }
                    SweepOutcome::CandidateInvalid => {
                        rep.candidates += 1;
                        rep.invalid += 1;
                    }
                    SweepOutcome::PlainBlob => {}
                    SweepOutcome::Failed(err) => {
                        if rep.failed.len() < MAX_BACKFILL_FAILURES {
                            rep.failed.push(BackfillFailure {
                                partition_id: *pid,
                                key: hex::encode(key.as_bytes()),
                                error: err,
                            });
                        }
                    }
                }
            }
        });
        tracing::info!(
            partition_id = *pid,
            scanned = rep.scanned - before.0,
            migrated = rep.migrated - before.1,
            already = rep.already - before.2,
            invalid = rep.invalid - before.3,
            deleted = rep.deleted - before.4,
            expired = rep.expired - before.5,
            "composed backfill sweep finished for replica"
        );
    }
    rep.replicas = handles.len() as u32;
    rep
}

/// 单条候选的甄别与置换:**MAGIC 形似才读入甄别**(非 `ASTC` 前缀的普通对象不进
/// 甄别面 —— invalid 是"MAGIC 撞车"专列,不给全体小对象当噪音);parse 严格通过
/// 才 `mark_composed`。
enum SweepOutcome {
    /// MAGIC 形似 + parse 通过 + 落位。
    CandidateMigrated,
    /// MAGIC 形似 + 位已在(索引快照与引擎判定之间的赛点,以引擎为准)。
    CandidateAlready,
    /// MAGIC 形似 + 落位时已被墓碑盖掉(引擎如实回报,不复活)。
    CandidateDeleted,
    /// MAGIC 形似但 parse 失败(如 `ASTC` 前缀普通文件——**不落位**,是 ① 修复的前提)。
    CandidateInvalid,
    /// 普通对象(非 MAGIC 前缀):不进甄别,计 scanned 即可。
    PlainBlob,
    Failed(String),
}

fn sweep_one(pid: PartitionId, store: &antfs_blob::BlobStore, key: &StoreKey) -> SweepOutcome {
    let data = match store.get(key) {
        Ok(d) => d,
        // 过期窗口的赛点(rare):紧跟 mark 时引擎以墓碑/过期如实回报,不撒谎。
        Err(e) => {
            tracing::warn!(partition_id = pid, key = %hex::encode(key.as_bytes()), error = %e, "backfill read failed");
            return SweepOutcome::Failed(e.to_string());
        }
    };
    if !data.starts_with(antfs_descriptor::MAGIC) {
        return SweepOutcome::PlainBlob;
    }
    if antfs_descriptor::parse(&data).is_err() {
        return SweepOutcome::CandidateInvalid;
    }
    match store.mark_composed(*key) {
        Ok(antfs_blob::MarkComposedOutcome::Migrated) => SweepOutcome::CandidateMigrated,
        Ok(antfs_blob::MarkComposedOutcome::Already) => SweepOutcome::CandidateAlready,
        Ok(antfs_blob::MarkComposedOutcome::Deleted) => SweepOutcome::CandidateDeleted,
        Ok(antfs_blob::MarkComposedOutcome::NotFound) => {
            // 索引快照与引擎判定之间的窄缝(如压实恰回收了过期项):如实不动。
            SweepOutcome::CandidateInvalid
        }
        Err(e) => SweepOutcome::Failed(e.to_string()),
    }
}

/// `POST /admin/request-control` 的请求体。
#[derive(serde::Deserialize)]
struct RequestControlBody {
    /// 操作名([`DataOp::as_str`] 的真值);未知 → 400。
    op: String,
    /// 指定则只作用于该分区(本地无该副本 → 404);缺省 = 节点级。
    #[serde(default)]
    partition_id: Option<PartitionId>,
    /// `true` = 静默,`false` = 恢复。
    enable: bool,
    /// 本次禁用时长(秒);缺省用 `store_server.request_control_default_ttl_secs`。
    /// `0` = 0 秒有效期(立即到期,配合惰性判定等于不禁)。
    #[serde(default)]
    ttl_secs: Option<u64>,
}

/// `POST /admin/request-control`:开/关某类数据面请求的静默(对标 Ambry `RequestControl` 运维动作)。
/// 返回操作后的当前禁用集。状态**不持久化**(重启即全开)。
async fn set_request_control(
    State(st): State<AppState>,
    Json(body): Json<RequestControlBody>,
) -> Response {
    let Some(op) = DataOp::parse(&body.op) else {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown op: {} (expected one of {})", body.op, op_names()),
        )
            .into_response();
    };
    if let Some(pid) = body.partition_id {
        if st.replicas.get(pid).is_none() {
            return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
        }
    }
    st.request_control
        .set(op, body.partition_id, body.enable, body.ttl_secs);
    Json(st.request_control.snapshot()).into_response()
}

/// `GET /admin/request-control`:当前禁用集(已过期条目按"视为启用"剔除)。
async fn get_request_control(State(st): State<AppState>) -> Response {
    Json(st.request_control.snapshot()).into_response()
}

/// 可静默的操作名列表(400 报错信息用)。
fn op_names() -> String {
    DataOp::ALL.map(DataOp::as_str).join(", ")
}

/// `PUT` 的三种形态(按请求头分流,统一走 [`finish_put`] 收敛状态码/指标):
///
/// - **有 `Content-Length` 且在 `max_body_bytes` 内**:流式 —— body 边收边落到该分区所在盘的暂存文件
///   (内存 O(64KiB),**不随文件大小涨**),再交引擎按已知长度顺序写
///   ([`antfs_blob::BlobStore::put_from_reader`])。暂存后再进引擎写锁,避免把网络传输时间串进该盘所有写;
/// - **无 `Content-Length`**(`Transfer-Encoding: chunked`):回落**缓冲**路径(整块进内存),
///   仍受 `max_body_bytes` 限制 —— 保持对既有调用方的向后兼容;
/// - 超限:413,body 不读(有 `Content-Length` 时)。
async fn put_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    Query(params): Query<PutParams>,
    req: axum::extract::Request,
) -> Response {
    let t = OpTimer::start(&st.metrics, "put", st.slow_op_threshold);
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let expiration_ms = params.expiration_ms.unwrap_or(antfs_blob::NO_EXPIRATION);
    let composed = params.composed.unwrap_or(false);
    let store = handle.store.clone();
    let t = t.with_disk(&handle.disk_id);

    match content_length(&req) {
        Some(len) if len > st.max_body_bytes => (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "body {len} exceeds store_server.max_body_bytes {}",
                st.max_body_bytes
            ),
        )
            .into_response(),
        // 小对象走内存(有界):落盘暂存要 create/write/close/unlink 一个文件,小对象是绝对主流,
        // 那套元数据开销在大并发下把 PUT 拖慢数倍(实测 macOS 上 p50 ~1ms → ~9ms)。内存上界 =
        // 并发数 × INLINE_BODY_MAX。大对象才走下面的流式暂存(内存不随大小涨)。
        Some(len) if len <= INLINE_BODY_MAX => {
            let body = match axum::body::to_bytes(req.into_body(), INLINE_BODY_MAX as usize).await {
                Ok(b) => b,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "body shorter than Content-Length (or exceeds the inline limit)",
                    )
                        .into_response()
                }
            };
            let n = body.len() as u64;
            // `to_bytes` 不看 `Content-Length` 头,必须自己核:客户端中途断开/少发时不能当成功写进去。
            if n != len {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("body shorter than Content-Length ({n} < {len})"),
                )
                    .into_response();
            }
            let disk = t.disk_id().to_string();
            let joined = st
                .pool
                .run(&disk, "put", move || {
                    if composed {
                        store.put_composed_ttl(key, &body, expiration_ms)
                    } else {
                        store.put_ttl(key, &body, expiration_ms)
                    }
                })
                .await;
            joined_put(&st, &t, n, joined)
        }
        Some(len) => {
            let tmp_dir = match st.replicas.tmp_dir_for(pid) {
                Ok(d) => d,
                Err(crate::error::Error::NoReplica) => {
                    return (StatusCode::NOT_FOUND, "no replica for partition").into_response()
                }
                Err(e) => {
                    record_err(&st.metrics, "io");
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response();
                }
            };
            let staged = StagedFile::new(&tmp_dir, pid);
            if let Err(e) = stage_body(req.into_body(), staged.path(), len).await {
                record_err(&st.metrics, "io");
                return (StatusCode::BAD_REQUEST, format!("staging body failed: {e}"))
                    .into_response();
            }
            let path = staged.path().to_path_buf();
            let disk = t.disk_id().to_string();
            let joined = st
                .pool
                .run(&disk, "put", move || -> antfs_blob::Result<()> {
                    let f = std::fs::File::open(&path)?;
                    let mut src = std::io::BufReader::with_capacity(STAGE_BUF_BYTES, f);
                    if composed {
                        store.put_composed_from_reader(key, len, &mut src, expiration_ms)
                    } else {
                        store.put_from_reader(key, len, &mut src, expiration_ms)
                    }
                })
                .await;
            // 正常出口:显式等一次删除(删除跑在阻塞池上,不占 reactor)→ "响应发出 ⇒ 暂存文件已清"
            // 是硬契约;若本 handler 在等待期间被**取消**(客户端断开/上游 abort),则靠 `Drop` 兜住 ——
            // 那正是之前漏文件的那条路径(见 `StagedFile` 的注释)。
            staged.remove().await;
            joined_put(&st, &t, len, joined)
        }
        None => {
            let body =
                match axum::body::to_bytes(req.into_body(), body_limit(st.max_body_bytes)).await {
                    Ok(b) => b,
                    Err(_) => {
                        return (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "body exceeds store_server.max_body_bytes (or stream error)",
                        )
                            .into_response()
                    }
                };
            let len = body.len() as u64;
            let disk = t.disk_id().to_string();
            let joined = st
                .pool
                .run(&disk, "put", move || {
                    if composed {
                        store.put_composed_ttl(key, &body, expiration_ms)
                    } else {
                        store.put_ttl(key, &body, expiration_ms)
                    }
                })
                .await;
            joined_put(&st, &t, len, joined)
        }
    }
}

async fn get_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    Query(params): Query<GetParams>,
) -> Response {
    let t = OpTimer::start(&st.metrics, "get", st.slow_op_threshold);
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let t = t.with_disk(&handle.disk_id);
    // replication=1:复制读,连**已过期**对象的数据体也返回(供接收方忠实 apply)。
    // 已删除对象的数据体读不到了(墓碑不保留旧记录指针),复制靠 peer 间的墓碑元数据即可。
    let for_repl = params.replication.unwrap_or(0) != 0;
    let disk = t.disk_id().to_string();
    match st
        .pool
        .run(&disk, "get", move || store.get_with_options(&key, for_repl))
        .await
    {
        Ok(Ok(data)) => {
            let len = data.len() as u64;
            st.metrics.get_total.inc();
            st.metrics.get_bytes.inc_by(len);
            t.set_bytes(len);
            // 成功路径才按返回数据长度分桶 observe(404 等早退不算)。
            st.metrics
                .blob_request_duration
                .with_label_values(&["get", size_bucket(len)])
                .observe(t.elapsed());
            (StatusCode::OK, data).into_response()
        }
        Ok(Err(
            antfs_blob::Error::NotFound | antfs_blob::Error::Deleted | antfs_blob::Error::Expired,
        )) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "get", reject),
    }
}

/// blob 元数据(不读数据体)的 JSON 响应。`antfs_blob` 不依赖 serde,故在此定义镜像 DTO。
#[derive(serde::Serialize)]
struct MetadataDto {
    /// 数据体字节数。
    size: u64,
    /// 过期时刻(毫秒;`-1` = 不过期)。
    expiration_ms: i64,
    /// 已删除(墓碑)。
    deleted: bool,
    /// 已过期(按当前时间判定)。
    expired: bool,
    /// 拼装描述符类型位(`?composed=true` 写入;网关置信读判定依据)。
    composed: bool,
}

/// `GET /partitions/{pid}/blobs/{key}/metadata`:只读元数据,**不读数据体**(对齐 Ambry `GetBlobInfo`)。
///
/// 已删除/已过期的对象也返回 200 + 对应标志 —— "元数据可读"与"数据体可读"是两回事,上层
/// (`frontend` 的 HEAD、运维查墓碑)需要前者。从未写过的 key → 404。
async fn get_blob_metadata(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
) -> Response {
    let _t = OpTimer::start(&st.metrics, "metadata", st.slow_op_threshold);
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let _t = _t.with_disk(&handle.disk_id);
    let disk = _t.disk_id().to_string();
    match st
        .pool
        .run(&disk, "metadata", move || store.stat(&key))
        .await
    {
        Ok(Ok(m)) => Json(MetadataDto {
            size: m.data_len,
            expiration_ms: m.expiration_ms,
            deleted: m.deleted,
            expired: m.expired,
            composed: m.composed,
        })
        .into_response(),
        Ok(Err(antfs_blob::Error::NotFound)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "metadata", reject),
    }
}
/// 复制元数据(step-1)端点:增量返回 `token` 之后的记录元数据(供 peer 拉复制)。
/// `GET /partitions/{pid}/scrub-report`:副本摘要(只走索引/mmap,不读数据体)。
///
/// 供 scrub 比对用:`live_records`/`tombstones`/`live_bytes`/`sealed_segments` + 段清单 + log 末端游标
/// (`token_seg`/`token_off`,与 `replica-metadata` 的游标同口径)。无本地副本 → 404。
async fn scrub_report(State(st): State<AppState>, Path(pid): Path<PartitionId>) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    st.metrics.scrub_reports.inc();
    let store = handle.store.clone();
    let disk = handle.disk_id.clone();
    let out = st
        .pool
        .run(&disk, "scrub_report", move || {
            let stats = store.index_stats();
            let segments: Vec<serde_json::Value> = store
                .sealed_segments()
                .into_iter()
                .map(|(name, data_end)| {
                    serde_json::json!({
                        "position": name.position,
                        "generation": name.generation,
                        "data_end": data_end,
                    })
                })
                .collect();
            let (token_seg, token_off) = match store.log_end() {
                Some((position, data_end)) => (Some(position), Some(data_end)),
                None => (None, None),
            };
            // 打开时的容错恢复报告:带伤启动的副本在这里如实暴露,admin 的一致性比对会直接把它算成差异。
            let damage = store.open_damage();
            serde_json::json!({
                "partition_id": pid,
                "live_records": stats.live_records,
                "tombstones": stats.tombstones,
                "live_bytes": stats.live_bytes,
                "sealed_segments": stats.sealed_segments,
                "segments": segments,
                "token_seg": token_seg,
                "token_off": token_off,
                "damaged_records": damage.bad_records,
                "abandoned_segments": damage.abandoned_segments,
            })
        })
        .await;
    match out {
        Ok(v) => Json(v).into_response(),
        Err(reject) => pool_reject_response(&st.metrics, "scrub_report", reject),
    }
}

/// `GET /partitions/{pid}/verify?max_bytes=`:本地完整性校验(逐记录验 CRC + 索引对拍)。
///
/// **会读全量数据体**(可 `max_bytes` 限流,缺省用配置 `scrub_max_batch_bytes`),故为显式运维动作。
/// 无本地副本 → 404;响应带 `truncated` 说明扫描是否覆盖全量。预算到点时带 `end_cursor`
/// (下一条未扫字节)与 `bytes_read` —— 手动分批运维可据此续扫(交给下一批的 `start` 语义
/// 见 [`antfs_blob::BlobStore::verify_from`]);`null` = 本轮预算内扫完全量。
async fn verify_partition(
    State(st): State<AppState>,
    Path(pid): Path<PartitionId>,
    Query(params): Query<VerifyParams>,
) -> Response {
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    st.metrics.verify_runs.inc();
    let max_bytes = params.max_bytes.unwrap_or(st.scrub_max_batch_bytes);
    let store = handle.store.clone();
    let metrics = st.metrics.clone();
    let started = std::time::Instant::now();
    let disk = handle.disk_id.clone();
    let out = st
        .pool
        .run(&disk, "verify", move || store.verify(max_bytes))
        .await;
    match out {
        Ok(Ok(r)) => {
            metrics
                .verify_duration
                .observe(started.elapsed().as_secs_f64());
            if r.bad_records > 0 {
                metrics.verify_bad_records.inc_by(r.bad_records);
            }
            let first = r.first_bad_offset.map(
                |(segment, offset)| serde_json::json!({ "segment": segment, "offset": offset }),
            );
            let end_cursor = r.end_cursor.map(
                |(segment, offset)| serde_json::json!({ "segment": segment, "offset": offset }),
            );
            Json(serde_json::json!({
                "partition_id": pid,
                "records": r.records,
                "bad_records": r.bad_records,
                "first_bad_offset": first,
                "index_mismatch": r.index_mismatch,
                "truncated": r.truncated,
                "bytes_read": r.bytes_read,
                "end_cursor": end_cursor,
            }))
            .into_response()
        }
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "verify", reject),
    }
}

/// `GET /partitions/{pid}/verify` 的查询参数:`max_bytes` 省略 = 用配置 `scrub_max_batch_bytes`。
#[derive(serde::Deserialize, Default)]
struct VerifyParams {
    max_bytes: Option<u64>,
}

async fn replica_metadata(
    State(st): State<AppState>,
    Path(pid): Path<PartitionId>,
    Query(params): Query<MetaParams>,
) -> Response {
    let _t = OpTimer::start(&st.metrics, "replica_metadata", st.slow_op_threshold);
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let _t = _t.with_disk(&handle.disk_id);
    let incarnation = store.incarnation();
    // 对齐 Ambry `revalidateStoreFindToken` 的"副本身份"那一半:游标声明的 incarnation 与服务端不符
    // (对面换了**一代副本** —— 重建/换盘),或游标残缺 → 一律从头重扫,并如实报"本轮发生过重定位"。
    // 这一层放服务端:incarnation 是**本副本**的事实,引擎(段级)看不到。
    // 没有游标(四件套全空)= 全新拉取,不算重定位。
    let has_cursor = params.pos.is_some() || params.gen.is_some() || params.off.is_some();
    let incarnation_mismatch = match params.incarnation {
        Some(inc) => inc != incarnation,
        None => has_cursor,
    };
    let token = if incarnation_mismatch {
        antfs_blob::FindToken::Uninitialized
    } else {
        token_from(params.pos, params.gen, params.off)
    };
    let max_bytes = params.max_bytes;
    let disk = _t.disk_id().to_string();
    match st
        .pool
        .run(&disk, "replica_metadata", move || {
            store.find_entries_since(&token, max_bytes)
        })
        .await
    {
        Ok(Ok(mut info)) => {
            if incarnation_mismatch {
                info.token_reset = Some(antfs_blob::TokenResetReason::IncarnationMismatch);
            }
            Json(MetadataResponse::from_find(info, incarnation)).into_response()
        }
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "replica_metadata", reject),
    }
}

async fn list_segments(State(st): State<AppState>, Path(pid): Path<PartitionId>) -> Response {
    let _t = OpTimer::start(&st.metrics, "segments", st.slow_op_threshold);
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let _t = _t.with_disk(&handle.disk_id);
    let segments: Vec<crate::replication::SegmentInfo> = handle
        .store
        .sealed_segments()
        .into_iter()
        .map(|(name, data_end)| crate::replication::SegmentInfo {
            position: name.position,
            generation: name.generation,
            data_end,
        })
        .collect();
    // 清单连同**本副本的 incarnation** 与每段的**代次**一起回:bootstrap 的初始游标据此绑定副本身份,
    // 且"拷段途中对端重写 log"能被段头代次当场识破(见 `replication::SealedListing`)。
    Json(crate::replication::SealedListing {
        incarnation: handle.store.incarnation(),
        segments,
    })
    .into_response()
}

/// 取一段封存段的字节(物理 bootstrap 的接收端)。
///
/// 路径参数是**段身份** `{position}_{generation}`(列清单给出的正是这个)而不是裸位置:
/// 对端若在"列清单"与"取这一段"之间重写了 log,同一位置上已经不是同一段了 —— 这时**必须**回 404
/// 让取段方丢弃这份拷贝,而不是把新代次的字节当成清单里那一段发出去(那会装出混合快照)。
async fn get_segment(
    State(st): State<AppState>,
    Path((pid, name)): Path<(PartitionId, String)>,
) -> Response {
    let _t = OpTimer::start(&st.metrics, "segments", st.slow_op_threshold);
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let Some(name) = parse_segment_name(&name) else {
        return (
            StatusCode::BAD_REQUEST,
            "segment name must be {position}_{generation}",
        )
            .into_response();
    };
    let store = handle.store.clone();
    let _t = _t.with_disk(&handle.disk_id);
    let disk = _t.disk_id().to_string();
    match st
        .pool
        .run(&disk, "segments", move || store.read_segment_prefix(name))
        .await
    {
        Ok(Ok(bytes)) => (StatusCode::OK, bytes).into_response(),
        Ok(Err(_)) => (StatusCode::NOT_FOUND, "no such segment").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response(),
    }
}

/// 解析 `{position}_{generation}` 形式的段身份(`LogSegmentName` 的 `Display` 形状)。
fn parse_segment_name(s: &str) -> Option<antfs_blob::LogSegmentName> {
    let (pos, gen) = s.split_once('_')?;
    Some(antfs_blob::LogSegmentName::new(
        pos.parse().ok()?,
        gen.parse().ok()?,
    ))
}

async fn delete_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
) -> Response {
    let _t = OpTimer::start(&st.metrics, "delete", st.slow_op_threshold);
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    let _t = _t.with_disk(&handle.disk_id);
    let disk = _t.disk_id().to_string();
    match st
        .pool
        .run(&disk, "delete", move || store.delete(key))
        .await
    {
        Ok(Ok(())) => {
            st.metrics.mutations.inc();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Err(e)) => {
            let code = map_mut_err(&e);
            if code == StatusCode::INTERNAL_SERVER_ERROR {
                record_err(&st.metrics, err_kind(&e));
            }
            code.into_response()
        }
        Err(reject) => pool_reject_response(&st.metrics, "delete", reject),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 池拒绝 → 状态码语义:**过载 = 503 + `Retry-After`**(可重试、让 router 换副本);
    /// **任务 panic = 500 + `errors_total{kind="join_error"}`**(内部错误,不能包装成"盘忙")。
    #[test]
    fn pool_reject_maps_overload_to_503_and_job_panic_to_500() {
        use crate::disk_pool::PoolReject;
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();

        let resp = pool_reject_response(&metrics, "get", PoolReject::Saturated);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "池满应 503");
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap()),
            Some("1"),
            "503 应带 Retry-After 提示上游退避"
        );
        // 过载**不**进 errors_total(它记的是真正进到引擎的失败,与 RequestControl 同一口径)。
        let text = antfs_metric::gather(&registry).expect("gather");
        assert!(
            !text.contains(r#"store_server_errors_total{kind="join_error"}"#),
            "过载不该计 join_error:\n{text}"
        );

        let resp = pool_reject_response(&metrics, "get", PoolReject::JobPanicked);
        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "任务 panic 是内部错误(500),不是过载"
        );
        assert!(
            resp.headers().get("retry-after").is_none(),
            "内部错误没有退避语义"
        );
        let text = antfs_metric::gather(&registry).expect("gather");
        assert!(
            text.contains(r#"store_server_errors_total{kind="join_error"} 1"#),
            "任务 panic 应与旧的 join 失败同一口径计数:\n{text}"
        );

        // 未知盘 / 池关停 / **盘已被判定不可服务**(4.3:排队中的那一发被丢掉)⇒ 对上游都是
        // "这块盘现在别碰" ⇒ 同样 503 + 退避,让 router 立刻改用别的副本。
        for reject in [
            PoolReject::UnknownDisk,
            PoolReject::Closed,
            PoolReject::DiskUnavailable,
        ] {
            assert_eq!(
                pool_reject_response(&metrics, "get", reject).status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{reject:?} 应 503"
            );
        }
    }

    /// 盘维度打点必须**配平**且不越界:请求数/在飞/延迟/带宽都在 `with_disk` 建、`Drop` 收尾;
    /// 没定位到盘的请求(参数校验早退)一个盘维度点都不该产生 —— 否则"某盘在堆积"的告警会变噪声。
    ///
    /// 阈值给 `ZERO`:任何耗时都算慢请求,于是断言不必依赖 `sleep`(CLAUDE.md §6)。
    #[test]
    fn op_timer_meters_each_disk_request() {
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();
        let gather = || antfs_metric::gather(&registry).expect("gather");

        let t = OpTimer::start(&metrics, "get", Duration::ZERO).with_disk("d0");
        t.set_bytes(4096);
        let text = gather();
        assert!(
            text.contains(r#"store_server_disk_in_flight_requests{disk_id="d0",op="get"} 1"#),
            "在飞应在 drop 前为 1:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_requests_total{disk_id="d0",op="get"} 1"#),
            "请求数应已计入:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_bytes_total{disk_id="d0",op="get"} 0"#),
            "带宽只在收尾时结算:\n{text}"
        );
        drop(t);

        let text = gather();
        assert!(
            text.contains(r#"store_server_disk_in_flight_requests{disk_id="d0",op="get"} 0"#),
            "drop 后必须配平回 0:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_bytes_total{disk_id="d0",op="get"} 4096"#),
            "成功请求的字节数应计入带宽:\n{text}"
        );
        assert!(
            text.contains(r#"store_server_disk_slow_requests_total{disk_id="d0",op="get"} 1"#),
            "阈值 0 ⇒ 该请求计一次慢请求:\n{text}"
        );
        assert!(
            text.contains(
                r#"store_server_disk_request_duration_seconds_count{disk_id="d0",op="get"} 1"#
            ),
            "盘维度延迟应按请求 observe 一次:\n{text}"
        );

        // 参数校验早退(没拿到副本句柄→没盘):盘维度不该多出这个组合。
        drop(OpTimer::start(&metrics, "put", Duration::ZERO));
        let text = gather();
        assert!(
            !text.contains(r#"disk_id="d0",op="put""#),
            "早退请求不该落盘维度:\n{text}"
        );
    }

    /// 暂存文件的 `Drop` 必须真的把文件删掉 —— 删除已丢进阻塞池(分离任务),故这里做**有界轮询**
    /// (不用 `sleep`:让出调度 + 文件删除本身是瞬时的)。
    #[tokio::test]
    async fn staged_file_drop_removes_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        let staged = StagedFile::new(tmp.path(), 0);
        let path = staged.path().to_path_buf();
        std::fs::write(&path, b"staged").unwrap();
        assert!(path.exists(), "守卫建好后文件应存在");
        drop(staged);

        let mut removed = false;
        for _ in 0..10_000 {
            if !path.exists() {
                removed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(removed, "Drop 应把暂存文件删掉(经阻塞池)");
    }

    /// 没调 `set_bytes` 的请求(delete / metadata / segments)不该产生 0 字节的带宽样本:
    /// 带宽是"搬了多少字节",不是"发生了几次操作"。
    #[test]
    fn op_timer_without_bytes_does_not_touch_bandwidth() {
        let registry = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&registry).unwrap();
        drop(OpTimer::start(&metrics, "delete", Duration::ZERO).with_disk("d0"));
        let text = antfs_metric::gather(&registry).expect("gather");
        assert!(
            text.contains(r#"store_server_disk_bytes_total{disk_id="d0",op="delete"} 0"#),
            "未搬字节 ⇒ 带宽停在 0:\n{text}"
        );
    }
}
