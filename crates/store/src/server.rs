//! 数据节点装配 + HTTP 数据面(`axum`,D2)。
//!
//! store 引擎是**同步阻塞**(路线乙),故 async handler 里的 put/get/ensure 一律经
//! [`tokio::task::spawn_blocking`] offload,不阻塞 reactor(衔接 N4;专用每盘线程池留 S4/性能期)。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

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
    /// `scrub_max_batch_bytes`:verify 缺省扫描上限(来自配置,避免 handler 再持 config)。
    scrub_max_batch_bytes: u64,
    /// A3 手动 compaction 的异步任务台账(内存态,最多保留最近 [`MAX_TRACKED_JOBS`] 条)。
    compactions: Arc<CompactionJobs>,
    /// 手动 compaction 的墓碑保留期(毫秒):与定时 compaction 同一套回收口径
    /// (手动触发不享受"豁免保留期"—— 保留期防的是复制复活,与谁发起无关)。
    compaction_tombstone_retention_ms: i64,
    /// 单请求 body 上限(字节):流式 put 用它做 `Content-Length` 前置校验,缓冲回落路径用它做抽取上限。
    max_body_bytes: u64,
    /// 未接 master 的单机模式(`master_addr` 为空):副本的**墓碑回收闸门**恒放行 —— 没有别的副本
    /// 可滞后。集群模式则只在 master 放行时开(见 [`crate::worker::tombstones_allowed`])。
    standalone: bool,
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
    health: crate::health::HealthMap,
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
        let disks = DiskManager::mount(&config.data_dirs)?;
        let store_metrics = antfs_blob::StoreMetrics::new(registry)?;
        let metrics = Arc::new(StoreServerMetrics::new(registry)?);
        let replicas = Arc::new(ReplicaManager::new(
            disks.clone(),
            store_config,
            store_metrics,
            config.partition_capacity_bytes,
        ));
        let health = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let request_control =
            Arc::new(RequestControl::new(config.request_control_default_ttl_secs));
        // A7 复制开关:与 `request_control` 同风格,起时全开(false = 正常拉复制)。
        let replication_paused = Arc::new(AtomicBool::new(false));
        // A3 compaction 台账:进程内内存态,重启即丢。
        let compactions = Arc::new(CompactionJobs::new());
        // 先取出标量再 move `config`(handlers 需要 body 上限做流式 put 的前置校验)。
        let max_body_bytes = config.max_body_bytes;
        let scrub_max_batch_bytes = config.scrub_max_batch_bytes;
        let standalone = config.master_addr.is_empty();
        let compaction_tombstone_retention_ms =
            (config.compaction_tombstone_retention_secs as i64).saturating_mul(1000);
        Ok(StoreServer {
            config,
            state: AppState {
                replicas,
                metrics,
                registry: registry.clone(),
                request_control,
                replication_paused,
                scrub_max_batch_bytes,
                compactions,
                compaction_tombstone_retention_ms,
                max_body_bytes,
                standalone,
            },
            disks,
            health,
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
                self.health.clone(),
            );
        }
        if !self.config.master_addr.is_empty() {
            crate::heartbeat::spawn_cluster_client(
                self.config.clone(),
                self.disks.clone(),
                self.state.replicas.clone(),
                self.health.clone(),
                self.state.metrics.clone(),
            );
            if self.config.replication_enabled {
                crate::replication::spawn_replicator(
                    self.config.clone(),
                    self.state.replicas.clone(),
                    self.state.metrics.clone(),
                    self.state.replication_paused.clone(),
                );
            }
        }
        if self.config.local_worker_enabled {
            crate::worker::spawn_local_worker(
                self.config.clone(),
                self.state.replicas.clone(),
                self.state.metrics.clone(),
            );
        }
        Ok(())
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

/// 配置的 body 上限 → `usize`;超出 `usize`(仅 32 位平台)`usize::MAX` 等价于不限制。
fn body_limit(max_body_bytes: u64) -> usize {
    usize::try_from(max_body_bytes).unwrap_or(usize::MAX)
}

/// 请求门(节点级静默,对标 Ambry `RequestControl`):命中禁用 → 503 + 说明文本(有剩余时长时带
/// `Retry-After`);否则直接放行。
///
/// 被拒请求**不**计入 `errors_total` / `request_duration` / `in_flight`——那三个记的是真正进到引擎的
/// 请求;静默只计 `rejected_requests_total{op,reason="disabled"}`。
async fn gate(State(st): State<AppState>, req: axum::extract::Request, next: Next) -> Response {
    let (op, partition) =
        match request_control::classify(req.method(), req.uri().path(), req.uri().query()) {
            Some(op) => (op, request_control::partition_of_path(req.uri().path())),
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
struct OpTimer {
    started: Instant,
    _timer: antfs_metric::HistogramTimer,
    in_flight: antfs_metric::IntGauge,
}

impl OpTimer {
    fn start(metrics: &StoreServerMetrics, op: &str) -> Self {
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
        }
    }

    /// 自 handler 入口起的耗时(秒);对齐 Ambry「只在 NoError 时按大小分类计数」的口径,
    /// 只应在本请求**成功**时调用(早退/错误路径不 observe)。
    fn elapsed(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }
}

impl Drop for OpTimer {
    fn drop(&mut self) {
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

/// 记一次数据面错误(`join_error` = spawn_blocking join 失败,非引擎错误)。
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

/// 暂存文件的 **RAII 守卫**:Drop 时删文件。
///
/// 为什么不用"`.await` 之后手写 `remove_file`":客户端断开或上游取消会让本 handler 的 future 被 **drop**,
/// `.await` 之后的清理代码**永远不执行** → 暂存文件漏在盘上。实测(2 MiB 对象 × 3000 次、router 收满 W 后
/// 取消在飞请求)漏出 1600+ 个文件。Drop 在 future 被 drop 时照样跑,故所有出口都干净。
///
/// 语义上安全:被取消时 blocking 任务可能仍在读这个文件,Unix 下 unlink 已打开的文件不影响其 fd(磁盘空间
/// 在关闭时释放),引擎该次写可能照样成功 —— 那对副本一致性无害(第 3 份本就靠拉复制补齐)。
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
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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

/// `spawn_blocking` 的 join 失败与引擎错误归一(join 失败只可能是 panic,单独计 `join_error`)。
fn joined_put(
    st: &AppState,
    t: &OpTimer,
    len: u64,
    joined: std::result::Result<antfs_blob::Result<()>, tokio::task::JoinError>,
) -> Response {
    match joined {
        Ok(r) => finish_put(&st.metrics, t, len, r),
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
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
        return submit_compaction(&st, pid, store, policy);
    }
    let policy = st.manual_compact_policy(tombstones_allowed);
    match tokio::task::spawn_blocking(move || store.compact(&policy)).await {
        Ok(Ok(stats)) => Json(CompactionResult::from(stats)).into_response(),
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
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
    tokio::spawn(async move {
        match tokio::task::spawn_blocking(move || store.compact(&policy)).await {
            Ok(Ok(stats)) => {
                jobs.finish(job_id, Ok(stats));
                metrics.compaction_jobs.with_label_values(&["done"]).inc();
            }
            Ok(Err(e)) => {
                record_err(&metrics, err_kind(&e));
                jobs.finish(job_id, Err(e.to_string()));
                metrics.compaction_jobs.with_label_values(&["failed"]).inc();
            }
            Err(_) => {
                record_err(&metrics, "join_error");
                jobs.finish(job_id, Err("join error".to_string()));
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
    let t = OpTimer::start(&st.metrics, "put");
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let expiration_ms = params.expiration_ms.unwrap_or(antfs_blob::NO_EXPIRATION);
    let store = handle.store.clone();

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
            let joined =
                tokio::task::spawn_blocking(move || store.put_ttl(key, &body, expiration_ms)).await;
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
            let joined = tokio::task::spawn_blocking(move || -> antfs_blob::Result<()> {
                let f = std::fs::File::open(&path)?;
                let mut src = std::io::BufReader::with_capacity(STAGE_BUF_BYTES, f);
                store.put_from_reader(key, len, &mut src, expiration_ms)
            })
            .await;
            // `staged` 在这里 drop → 删文件;若本 handler 被**取消**(客户端断开/上游 abort),
            // .await 之后的代码根本不执行,则靠 Drop 兜住 —— 这正是之前漏文件的那条路径。
            drop(staged);
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
            let joined =
                tokio::task::spawn_blocking(move || store.put_ttl(key, &body, expiration_ms)).await;
            joined_put(&st, &t, len, joined)
        }
    }
}

async fn get_blob(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
    Query(params): Query<GetParams>,
) -> Response {
    let t = OpTimer::start(&st.metrics, "get");
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    // replication=1:复制读,连**已过期**对象的数据体也返回(供接收方忠实 apply)。
    // 已删除对象的数据体读不到了(墓碑不保留旧记录指针),复制靠 peer 间的墓碑元数据即可。
    let for_repl = params.replication.unwrap_or(0) != 0;
    match tokio::task::spawn_blocking(move || store.get_with_options(&key, for_repl)).await {
        Ok(Ok(data)) => {
            let len = data.len() as u64;
            st.metrics.get_total.inc();
            st.metrics.get_bytes.inc_by(len);
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
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
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
}

/// `GET /partitions/{pid}/blobs/{key}/metadata`:只读元数据,**不读数据体**(对齐 Ambry `GetBlobInfo`)。
///
/// 已删除/已过期的对象也返回 200 + 对应标志 —— "元数据可读"与"数据体可读"是两回事,上层
/// (`frontend` 的 HEAD、运维查墓碑)需要前者。从未写过的 key → 404。
async fn get_blob_metadata(
    State(st): State<AppState>,
    Path((pid, key_hex)): Path<(PartitionId, String)>,
) -> Response {
    let _t = OpTimer::start(&st.metrics, "metadata");
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.stat(&key)).await {
        Ok(Ok(m)) => Json(MetadataDto {
            size: m.data_len,
            expiration_ms: m.expiration_ms,
            deleted: m.deleted,
            expired: m.expired,
        })
        .into_response(),
        Ok(Err(antfs_blob::Error::NotFound)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
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
    let out = tokio::task::spawn_blocking(move || {
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
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
    }
}

/// `GET /partitions/{pid}/verify?max_bytes=`:本地完整性校验(逐记录验 CRC + 索引对拍)。
///
/// **会读全量数据体**(可 `max_bytes` 限流,缺省用配置 `scrub_max_batch_bytes`),故为显式运维动作。
/// 无本地副本 → 404;响应带 `truncated` 说明扫描是否覆盖全量。
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
    let out = tokio::task::spawn_blocking(move || store.verify(max_bytes)).await;
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
            Json(serde_json::json!({
                "partition_id": pid,
                "records": r.records,
                "bad_records": r.bad_records,
                "first_bad_offset": first,
                "index_mismatch": r.index_mismatch,
                "truncated": r.truncated,
            }))
            .into_response()
        }
        Ok(Err(e)) => {
            record_err(&st.metrics, err_kind(&e));
            (map_mut_err(&e), format!("{e}")).into_response()
        }
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
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
    let _t = OpTimer::start(&st.metrics, "replica_metadata");
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
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
    match tokio::task::spawn_blocking(move || store.find_entries_since(&token, max_bytes)).await {
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
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
    }
}

async fn list_segments(State(st): State<AppState>, Path(pid): Path<PartitionId>) -> Response {
    let _t = OpTimer::start(&st.metrics, "segments");
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
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
    let _t = OpTimer::start(&st.metrics, "segments");
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
    match tokio::task::spawn_blocking(move || store.read_segment_prefix(name)).await {
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
    let _t = OpTimer::start(&st.metrics, "delete");
    let Some(key) = parse_key(&key_hex) else {
        return (StatusCode::BAD_REQUEST, "key must be 8 bytes (16 hex)").into_response();
    };
    let Some(handle) = st.replicas.get(pid) else {
        return (StatusCode::NOT_FOUND, "no replica for partition").into_response();
    };
    let store = handle.store.clone();
    match tokio::task::spawn_blocking(move || store.delete(key)).await {
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
        Err(_) => {
            record_err(&st.metrics, "join_error");
            (StatusCode::INTERNAL_SERVER_ERROR, "join error").into_response()
        }
    }
}
