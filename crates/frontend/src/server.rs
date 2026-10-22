//! frontend HTTP 服务:装配、路由、handler(HTTP ↔ router SDK 的翻译层)。
//!
//! **本层只做翻译**:写 W(该分区多数派)/ 读 R=1 / 向 master 取 blobId 号段全部在
//! [`antfs_router`](router SDK)里实现(`cluster.md §10`),这里不重新实现任何一致性逻辑,
//! 也不落每对象元数据(D3:无中心每对象元数据)—— 故服务**无状态、可水平扩展**。
//!
//! 两条与"无状态"直接相关的设计:
//! - **分片拼装(`POST /stitch`)不用服务端 session 表**:拼装关系写进一个**描述符 blob**
//!   (见 [`encode_descriptor`]),片本身是普通 blob,`GET` 读到描述符即逐片流式拼装;
//! - **上传零本地临时盘(④边收边分片,upload-zero-spool 2026-10-21)**:内存决定窗(=
//!   `max_body_bytes`)+ 流式分片直入集群,内存不随对象大小涨(CLAUDE.md §5.3)且全程
//!   不写任何本地文件;代价 = 网关崩溃窗可能留下无人认领的孤儿片(所有者裁决 ④ 已接受,
//!   缓解路线见该 change 设计档)。
//!
//! **服务端自动分片(恒切,2026-10-21 裁决)**(`POST /blobs` body > `max_body_bytes` 必切):
//! 边收边按 `auto_chunk_bytes` 切片逐片入库,EOF 对账(总量 == 声明、片数上限)后写拼装
//! 描述符并返回其 blobId —— `GET`/`HEAD` 走既有的描述符透明拼装路径,客户端无感知(见
//! [`upload`] / [`stream_upload`])。

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use tower_http::cors::{AllowOrigin, CorsLayer};

use antfs_metric::IntCounterVec;
use antfs_router::{
    Error as RouterError, ObjectStat, Router as BlobRouter, StoreKey, NO_EXPIRATION,
};

use crate::config::FrontendConfig;
use crate::metrics::{
    FrontendMetrics, OP_DELETE, OP_DOWNLOAD, OP_HEAD, OP_STITCH, OP_UPLOAD, OUTCOME_OK,
};

// ── 请求/响应头(`x-antfs-*` 契约;前身沿用 Ambry 的 `x-ambry-*`,2026-10 整体改名)──────────────

/// `x-antfs-session`:分片上传的会话标识(客户端生成;片就是普通 blob,frontend 只把它记进描述符)。
const HEADER_SESSION: HeaderName = HeaderName::from_static("x-antfs-session");
/// `x-antfs-ttl-ms`:上传时可选带的过期时刻(epoch 毫秒)。
const HEADER_TTL_MS: HeaderName = HeaderName::from_static("x-antfs-ttl-ms");
/// `x-antfs-blob-id`:blobId 的 16 位 hex。
const HEADER_BLOB_ID: HeaderName = HeaderName::from_static("x-antfs-blob-id");
/// `x-antfs-blob-size`:数据体字节数。
const HEADER_BLOB_SIZE: HeaderName = HeaderName::from_static("x-antfs-blob-size");
/// `x-antfs-blob-deleted`:`"true"`/`"false"`。
const HEADER_BLOB_DELETED: HeaderName = HeaderName::from_static("x-antfs-blob-deleted");
/// `x-antfs-blob-expired`:`"true"`/`"false"`。
const HEADER_BLOB_EXPIRED: HeaderName = HeaderName::from_static("x-antfs-blob-expired");
/// `x-antfs-blob-expiration-ms`:过期时刻(epoch 毫秒;`-1` = 不过期)。
const HEADER_BLOB_EXPIRATION_MS: HeaderName = HeaderName::from_static("x-antfs-blob-expiration-ms");
/// `x-antfs-blob-stitched`:该对象是否为分片拼装(`HEAD` 专用;`GET` 会拼装成逻辑字节流)。
const HEADER_BLOB_STITCHED: HeaderName = HeaderName::from_static("x-antfs-blob-stitched");
/// `x-antfs-blob-stitch-chunks`:拼装的片数(`HEAD` 专用,仅拼装对象有)。
const HEADER_BLOB_STITCH_CHUNKS: HeaderName = HeaderName::from_static("x-antfs-blob-stitch-chunks");

/// **直接进内存**的 body 上界:≤ 该值走内存路径(`Bytes` → `Router::put`);> 该值(或未声明长度)
/// 走 [`stream_upload`] 恒切流式(内存决定窗 + 攒满即发片,零本地文件;`Router::put_file`
/// 家族已移除,大 body 一律经恒切收敛为 ≤ 片宽的单笔 Bytes 写)。
///
/// 为什么小对象不走暂存:暂存要 `create/write/close/unlink` 一个文件,每个上传多 ~5 次文件系统元数据操作;
/// 小对象是绝对主流,大并发下这套开销把 PUT p50 从 ~1ms 拖到 ~9ms(devcluster + bench 实测,release)。
/// 内存上界 = 在途请求数 × 该值(1 MiB)。与 `store` 侧的同名阈值同因(见 `crates/store/src/server.rs`)。
const INLINE_BODY_MAX: u64 = 1024 * 1024;

/// 跨域时允许的请求头(仅本接口真正用到的三个)。
const CORS_ALLOWED_HEADERS: [HeaderName; 3] = [header::CONTENT_TYPE, HEADER_SESSION, HEADER_TTL_MS];
/// 跨域时暴露给浏览器的响应头(全部 `x-antfs-*` + `Location`)。
const CORS_EXPOSED_HEADERS: [HeaderName; 8] = [
    header::LOCATION,
    HEADER_BLOB_ID,
    HEADER_BLOB_SIZE,
    HEADER_BLOB_DELETED,
    HEADER_BLOB_EXPIRED,
    HEADER_BLOB_EXPIRATION_MS,
    HEADER_BLOB_STITCHED,
    HEADER_BLOB_STITCH_CHUNKS,
];

/// blobId 的文本形式必须是 8 字节 hex —— 唯一的报错文案(blobId 是自定位的 64bit 号,D3)。
const BLOB_ID_INVALID: &str = "blob id must be 8 bytes (16 hex)";

// ── 拼装描述符格式(写入侧;读侧拼装已下沉 router,⑩)────────────────────────────────
// 格式单点在 `antfs-descriptor`;此处按本层错误口径做薄壳映射。

/// 单个拼装对象的片数上限。
const MAX_CHUNKS: usize = antfs_descriptor::MAX_CHUNKS;

/// frontend 的错误出口:边界校验类自身错误 + SDK 错误的归一。
///
/// 映射规则集中在本类型的 [`Error::status`](唯一真值),handler 里不得另判。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 调用方请求非法(blobId 格式、缺参数、片数越界、session 过长…)。
    #[error("{0}")]
    Invalid(String),

    /// 请求体超过该路径的 body 上限:未开自动分片时即 `frontend.max_body_bytes`,开启后为
    /// 自动分片总量上限(片数上限 × 单片大小,见 [`auto_chunk_total_limit`])。
    #[error("body {len} exceeds limit {max}")]
    TooLarge { len: u64, max: u64 },

    /// 对象在所有副本上都不存在(R=1 试遍全部副本仍 404)。
    #[error("blob not found")]
    NotFound,

    /// 上游数据不一致(拼装对象缺片 / 描述符损坏)—— 调用方没做错什么,是数据面状态不对。
    #[error("upstream data inconsistent: {0}")]
    Upstream(String),

    /// router SDK 报错(多数派写、副本读、发号)。
    #[error(transparent)]
    Router(#[from] RouterError),

    /// 暂存文件等本地 IO。
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// 错误 → HTTP 状态码(**唯一映射点**)。
    fn status(&self) -> StatusCode {
        match self {
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            // 超 body 上限:413(与 store 数据面同一约定;不能混进 400)。
            Error::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Error::NotFound => StatusCode::NOT_FOUND,
            // 上游数据不一致:不是调用方的问题,也不是本层的崩溃(Gateway 语义)。
            Error::Upstream(_) => StatusCode::BAD_GATEWAY,
            Error::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Router(e) => router_status(e),
        }
    }

    /// 指标 label `outcome`(稳定取值;SDK 错误沿用 [`antfs_router::Error::kind`])。
    fn outcome(&self) -> &'static str {
        match self {
            Error::Invalid(_) => "invalid",
            Error::TooLarge { .. } => "too_large",
            Error::NotFound => "not_found",
            Error::Upstream(_) => "upstream",
            Error::Io(_) => "io",
            Error::Router(e) => e.kind(),
        }
    }

    /// 透传 `Retry-After`(仅在副本**请求静默**且给了剩余秒数时;`None` = 不带头)。
    fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Error::Router(RouterError::Silenced {
                retry_after_secs, ..
            }) => *retry_after_secs,
            _ => None,
        }
    }
}

/// router 错误 → HTTP 状态码。
fn router_status(e: &RouterError) -> StatusCode {
    match e {
        // 所有副本都没有该对象。
        RouterError::NotFound { .. } => StatusCode::NOT_FOUND,
        // 恒切管线容量拒绝(片数超描述符上限):超大对象,413(与公共上限同一 4xx 家族)。
        RouterError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        // 拼装对象上游损坏(描述符/片不一致):响亮 502(与既有 corrupt-descriptor 口径一致)。
        RouterError::BadComposed(_) => StatusCode::BAD_GATEWAY,
        // 分区写满:507 让调用方/压测一眼看出"该加分区了"(与 store 数据面同一约定),不混进 500。
        RouterError::StoreFull { .. } => StatusCode::INSUFFICIENT_STORAGE,
        // key 冲突(副本 409 透传,keyed 写全失败且皆为冲突):与 store 数据面同码,
        // 调用方确定性不可重试 —— 不该归进 503 装作暂时性失败。
        RouterError::AlreadyExists { .. } => StatusCode::CONFLICT,
        // 暂时性失败:未达多数派 / 副本不可达 / 被静默 / 路由到无副本节点 / 号段耗尽 / master 不可用。
        // 都可能自愈或换个副本就好,统一 503 让调用方重试。
        RouterError::Quorum { .. }
        | RouterError::Http { .. }
        | RouterError::NoReplica { .. }
        | RouterError::Silenced { .. }
        | RouterError::IdExhausted(_)
        | RouterError::NoWritablePartition
        | RouterError::Master(_) => StatusCode::SERVICE_UNAVAILABLE,
        // master 明确业务性拒绝(非 leader 自愈重试后仍失败 / 未知分区 / 分区不可写):调用方改不了,
        // 也不是本层 bug —— NotLeader 上抛到这里 = router 自愈已尽(防抖压制或提示不可用),503 让调用方稍后再试。
        RouterError::NotLeader { .. } | RouterError::Rejected(_) => StatusCode::SERVICE_UNAVAILABLE,
        // 副本返回的状态码:**与 store 数据面一致地透传**调用方可控的那几类 —— 400/413 是 payload 问题、
        // 507 是容量问题、503 是暂时不可用;其余才算意料之外,归 500(契约:以上各类之外 → 500)。
        RouterError::ReplicaStatus { status, .. } => match *status {
            400 => StatusCode::BAD_REQUEST,
            413 => StatusCode::PAYLOAD_TOO_LARGE,
            503 => StatusCode::SERVICE_UNAVAILABLE,
            507 => StatusCode::INSUFFICIENT_STORAGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        // (号段复用形态的 `AlreadyExists` 已在上方按 409 透传;此处不再列)
        // blobId 与目标分区不符 / 配置或传输层损坏:运维或数据损坏问题。
        RouterError::PartitionMismatch { .. }
        | RouterError::Config(_)
        | RouterError::Connect(_)
        | RouterError::Metric(_)
        | RouterError::HttpClient(_)
        | RouterError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status();
        let retry_after = self.retry_after_secs();
        let mut resp = (status, self.to_string()).into_response();
        if let Some(secs) = retry_after {
            put_header(&mut resp, header::RETRY_AFTER, &secs.to_string());
        }
        resp
    }
}

/// frontend 的装配状态(handler 的 `State`;测试直接构造,与生产走同一装配路径)。
#[derive(Clone)]
pub struct AppState {
    /// router(写 W / 读 R / 发号的真实现)。
    pub router: Arc<BlobRouter>,
    /// HTTP 层指标。
    pub metrics: Arc<FrontendMetrics>,
    /// `/metrics` 用的注册表(与 [`AppState::metrics`] 接入的是同一个)。
    pub registry: antfs_metric::Registry,
    /// 内部分档界(有声明且 ≤ 它 = "内存整收"中档;超过转边收边分片;不再是契约线)。
    pub max_body_bytes: u64,
    /// **公共单文件上限**(恒切契约的分界线;与 S3 网关共用同一值,来自 `[upload]` 表)。
    pub max_object_bytes: u64,
    /// 自动分片单片大小(字节;配置校验保证 1..=1 GiB,默认 4 MiB)。
    pub auto_chunk_bytes: u64,
    /// CORS 允许来源;空 = 不挂 CORS 层。
    pub cors_allowed_origins: Vec<String>,
    /// 孤儿收账簿(`None` = 未启用,④ 崩溃窗孤儿无兜底;启用后有清扫任务,见 [`upload_wal`])。
    pub upload_wal: Option<Arc<crate::upload_wal::UploadWal>>,
}

/// frontend 装配错误:router 连不上 / 收账簿目录不可用 / 指标注册失败 —— **启动即失败**,
/// 拒绝静默降级(孤儿兜底是承诺过的功能,静默关掉比报错更危险)。
#[derive(Debug)]
pub enum FrontendError {
    Router(RouterError),
    Wal(std::io::Error),
    Metrics(String),
}

impl From<RouterError> for FrontendError {
    fn from(e: RouterError) -> Self {
        FrontendError::Router(e)
    }
}

impl std::fmt::Display for FrontendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrontendError::Router(e) => write!(f, "router connect: {e}"),
            FrontendError::Wal(e) => write!(f, "upload session wal: {e}"),
            FrontendError::Metrics(e) => write!(f, "metrics register: {e}"),
        }
    }
}

impl std::error::Error for FrontendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrontendError::Router(e) => Some(e),
            FrontendError::Wal(e) => Some(e),
            FrontendError::Metrics(_) => None,
        }
    }
}

impl AppState {
    /// 按配置装配:连 router SDK(懒连接 master)+ 注册指标 + 可选孤儿收账簿
    /// (启用 = 目录可用性启动校验 + 清扫任务随即展开:启动即扫一拍,此后周期拍)。
    pub async fn from_config(
        cfg: &FrontendConfig,
        upload: &antfs_config::UploadConfig,
        registry: &antfs_metric::Registry,
    ) -> Result<Self, FrontendError> {
        let router = Arc::new(BlobRouter::connect(cfg.router.clone(), registry).await?);
        let metrics = Arc::new(
            FrontendMetrics::new(registry).map_err(|e| FrontendError::Metrics(e.to_string()))?,
        );
        let upload_wal = if cfg.upload_session_wal_dir.as_os_str().is_empty() {
            None
        } else {
            let wal = crate::upload_wal::UploadWal::open(cfg.upload_session_wal_dir.clone())
                .await
                .map_err(FrontendError::Wal)?;
            spawn_upload_sweeper(wal.clone(), router.clone(), metrics.clone());
            Some(wal)
        };
        Ok(AppState {
            router,
            metrics,
            registry: registry.clone(),
            max_body_bytes: cfg.max_body_bytes,
            max_object_bytes: upload.max_object_bytes,
            auto_chunk_bytes: cfg.auto_chunk_bytes,
            cors_allowed_origins: cfg.cors_allowed_origins.clone(),
            upload_wal,
        })
    }
}

/// 孤儿清扫一拍:超时会话逐片墓碑(全删成 → 会话核销;有失败留下一拍重试),
/// 收尾请求账簿瘦身;拍数与墓碑数入指标。
async fn sweep_uploads(
    wal: &Arc<crate::upload_wal::UploadWal>,
    router: &Arc<BlobRouter>,
    metrics: &Arc<FrontendMetrics>,
) {
    metrics.upload_sweeper_runs_total.inc();
    let now = crate::upload_wal::now_ms();
    for session in wal.scan_stale(now).await {
        let mut ok = true;
        for blob_id in &session.chunk_ids {
            let key = match parse_blob_id(blob_id) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        blob_id = %blob_id,
                        error = %e,
                        "sweeper: 账簿片 id 无法解析,跳过该行"
                    );
                    continue;
                }
            };
            match router.delete(&key).await {
                Ok(_report) => metrics.upload_sweeper_tombstones_total.inc(),
                Err(e) => {
                    ok = false;
                    tracing::warn!(
                        blob_id = %blob_id,
                        error = %e,
                        "sweeper: 孤儿片墓碑失败,留待下一拍"
                    );
                }
            }
        }
        if ok {
            wal.record_abort(session.sid, now).await;
        }
    }
    wal.compact_maybe().await;
}

/// 清扫任务:`interval` 首拍立即(= 启动即扫),此后每 [`upload_wal::SWEEP_INTERVAL_SECS`] 一拍;
/// 进程退出随运行时消亡。
fn spawn_upload_sweeper(
    wal: Arc<crate::upload_wal::UploadWal>,
    router: Arc<BlobRouter>,
    metrics: Arc<FrontendMetrics>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(
            crate::upload_wal::SWEEP_INTERVAL_SECS,
        ));
        loop {
            tick.tick().await;
            sweep_uploads(&wal, &router, &metrics).await;
        }
    });
}

/// 建路由:`/blobs*` 与 `/stitch` 是对象接口,`/metrics` 是 Prometheus 抓取口(无鉴权,抓取惯例),
/// `/healthCheck` 是进程探活(无鉴权,恒 200,见 [`health_check`])。
///
/// body 上限**分路由挂**:`/blobs` 开自动分片后上限放大到分片总量上限(裸字节体由 handler
/// 边收边计数,`DefaultBodyLimit` 只是流式读的兜底);`/stitch` 的 JSON 体重持 `max_body_bytes`
/// (无分片路径,放大它只是让恶意请求能白写更多信息)。
pub fn build_router(state: AppState) -> Router {
    // `/blobs` 的兜底字节上限 = 公共单文件上限(恒切;裸体由 handler 边收边计数)。
    let blobs_limit = state.max_object_bytes;
    let router = Router::new()
        .route(
            "/blobs",
            post(post_blob).layer(DefaultBodyLimit::max(body_limit(blobs_limit))),
        )
        .route(
            "/blobs/{blob_id}",
            get(get_blob).head(head_blob).delete(delete_blob),
        )
        .route(
            "/stitch",
            post(stitch).layer(DefaultBodyLimit::max(body_limit(state.max_body_bytes))),
        )
        .route("/metrics", get(get_metrics))
        .route("/healthCheck", get(health_check))
        .with_state(state.clone());

    match cors_layer(&state.cors_allowed_origins) {
        Some(cors) => router.layer(cors),
        None => router,
    }
}

/// CORS 层:默认**不挂**(返回 `None`)。显式列出来源时才构造,且不用 `permissive()`
/// (那等于 `Access-Control-Allow-Origin: *`,放开跨域是安全问题)。
fn cors_layer(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }
    // 配置校验已保证每个来源能编成 header 值、且不含 `*`(含 `*` 会让 `AllowOrigin::list` panic),
    // 故此处 filter_map 只是兜底。
    let allow: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allow))
            .allow_methods([Method::GET, Method::POST, Method::HEAD, Method::DELETE])
            .allow_headers(CORS_ALLOWED_HEADERS)
            .expose_headers(CORS_EXPOSED_HEADERS),
    )
}

// ── handler ─────────────────────────────────────────────────────────────────────────────────

/// `POST /blobs`:body = 裸 blob 字节 → 201 + `Location` / `x-antfs-blob-id` / `x-antfs-blob-size`。
///
/// 分片上传的**片就是普通 blob**(带 `x-antfs-session` 也不改变写入路径),片清单由客户端自己记
/// —— frontend 无状态(D3)。可选头 `x-antfs-ttl-ms` 见 [`upload`]。
/// body 超过 `max_body_bytes` 且开启自动分片时,切片+描述符路径见 [`stream_upload`]。
async fn post_blob(State(st): State<AppState>, req: Request) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_UPLOAD);
    match upload(&st, req).await {
        Ok(done) => {
            st.metrics
                .upload_total
                .with_label_values(&[OUTCOME_OK])
                .inc();
            st.metrics.upload_bytes.inc_by(done.len);
            if let Some(n) = done.auto_chunks {
                // 失败侧在 `upload()` 的分片出口记;两处合成 `upload_auto_chunk_total` 的全集。
                st.metrics
                    .upload_auto_chunk_total
                    .with_label_values(&[OUTCOME_OK])
                    .inc();
                st.metrics
                    .upload_auto_chunk_chunks_total
                    .inc_by(u64::from(n));
            }
            // 每请求成功的细节走 debug:压测下 2 万次上传会在 INFO 打出 2 万行,日志本身成为热点。
            tracing::debug!(
                blob_id = %hex::encode(done.key.as_bytes()),
                size = done.len,
                auto_chunks = done.auto_chunks.unwrap_or(0),
                "blob stored"
            );
            created_response(done.key, done.len)
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.upload_total, &[], &e);
            tracing::warn!(error = %e, "upload failed");
            e.into_response()
        }
    }
}

/// 一次成功上传的落点(handler 据此记指标 + 回响应)。
struct Uploaded {
    key: StoreKey,
    /// 回报给 `x-antfs-blob-size` 的字节数:单 blob = 数据体;自动分片 = 逻辑总量(各片之和,
    /// 与 GET/HEAD 的拼装口径一致)。
    len: u64,
    /// 走了自动分片路径时的数据片数;单 blob 路径为 `None`。
    auto_chunks: Option<u32>,
}

/// 上传实现:边界校验 → 声明 ≤ `max_body_bytes` 走单 blob 路径(≤ 1 MiB 小档内存直进;
/// 中档有界内存整收、核验一致才写)→ 超过(且开启自动分片)或无声明长度走 [`stream_upload`]
/// (④ 边收边分片,零本地文件)→ 写入时定 TTL(`x-antfs-ttl-ms`,缺省不过期)。
async fn upload(st: &AppState, req: Request) -> Result<Uploaded, Error> {
    // 先解析 TTL 头:非法立即 400,不白读一遍 body。
    let expiration_ms = ttl_header(req.headers())?.unwrap_or(NO_EXPIRATION);
    // session 只有走分片路径才会写进描述符;提前取值(之后 req 被 into_body 消费)。
    let session = req.headers().get(HEADER_SESSION).cloned();
    let declared = content_length(&req);
    // 该路径收得下的总量:关自动分片 = `max_body_bytes`(超 → 413,旧契约);开 = 分片总量上限。
    // 恒切契约的单文件上限 = 公共值([upload] 表,REST 与 S3 同一处);声明超限在读体前 413。
    let total_limit = st.max_object_bytes;
    if let Some(len) = declared {
        if len > total_limit {
            return Err(Error::TooLarge {
                len,
                max: total_limit,
            });
        }
    }
    // 单 blob 路径的判据是"长度确知 ≤ max_body_bytes":开了自动分片后,声明长度可以落在
    // (max_body_bytes, total_limit] 区间,不能再只按 INLINE 档分派(否则超限体被当单 blob 写)。
    //
    // TTL 随**同一次**写请求下发(`PUT ?expiration_ms=`),故不存在"对象已创建但没带 TTL"的中间态。
    let result = match declared {
        Some(len) if len <= INLINE_BODY_MAX && len <= st.max_body_bytes => {
            let bytes = axum::body::to_bytes(req.into_body(), INLINE_BODY_MAX as usize)
                .await
                .map_err(|_| Error::Invalid("body shorter than Content-Length".into()))?;
            // `to_bytes` 不看 `Content-Length` 头,必须自己核(否则短读会被当成功写入)。
            if bytes.len() as u64 != len {
                return Err(Error::Invalid(format!(
                    "body shorter than Content-Length ({} < {len})",
                    bytes.len()
                )));
            }
            let (key, _report) = st.router.put_auto_with_ttl(bytes, expiration_ms).await?;
            Uploaded {
                key,
                len,
                auto_chunks: None,
            }
        }
        // 中档(1 MiB < 声明 ≤ max):有界内存整收,声明核验一致才写 —— 该档失败零孤儿。
        // 内存上界 = 并发数 × max_body_bytes(与 S3 网关同口径)。
        Some(len) if len <= st.max_body_bytes => {
            let cap = usize::try_from(st.max_body_bytes).unwrap_or(usize::MAX);
            let bytes = axum::body::to_bytes(req.into_body(), cap)
                .await
                .map_err(|_| Error::Invalid("body read failed".into()))?;
            // `to_bytes` 不看 `Content-Length` 头,必须自己核(短读/超读都不得当成功写)。
            if bytes.len() as u64 != len {
                return Err(Error::Invalid(format!(
                    "body shorter than Content-Length ({} < {len})",
                    bytes.len()
                )));
            }
            let (key, _report) = st.router.put_auto_with_ttl(bytes, expiration_ms).await?;
            Uploaded {
                key,
                len,
                auto_chunks: None,
            }
        }
        // 大档(声明 > max)或无声明长度:恒切(④)边收边分片 —— 零本地文件。
        _ => stream_upload(st, req.into_body(), declared, expiration_ms, session).await?,
    };
    Ok(result)
}

/// 分片路径的 session 头口径(与 `/stitch` 一致):给了就必须能作 header 文本读出、长度 ≤ 65535
/// (描述符的 u16 长度域)。单 blob 路径不看这个头(分片上传的片就是普通 blob)。
fn session_of(raw: Option<HeaderValue>) -> Result<Option<String>, Error> {
    let raw = match raw {
        None => return Ok(None),
        Some(v) => v,
    };
    let text = raw
        .to_str()
        .map_err(|_| Error::Invalid("x-antfs-session must be valid text".into()))?
        .to_string();
    if text.len() > u16::MAX as usize {
        return Err(Error::Invalid(
            "x-antfs-session must be at most 65535 bytes".into(),
        ));
    }
    Ok(Some(text))
}

/// 大 body / 无声明长度的流式上传(④ 边收边分片,upload-zero-spool):**内存决定窗** =
/// `max_body_bytes` —— EOF 在窗内 ⇒ 单 blob(声明核验一致才写,该分支零孤儿);一旦越窗
/// (或声明即 > max)转**分片模式**:以 `auto_chunk_bytes` 为片长、攒满即 `put` 一片,EOF 后
/// 对账(总量 == 声明;声明 ≤ 4096 × 片长已在入口 413)再写拼装描述符。内存 = 窗口 + 单片
/// 缓冲(不随对象大小涨),**全程零本地文件**。
///
/// 持久性/失败口径(CLAUDE.md §3.3,对齐被替换的先暂存后切片路径):
/// - 每片与描述符都是**一次多数派写**(`put_auto_with_ttl`,内部发号 + 507 换分区重试);
/// - 断流 / 超发 / 欠发 / 某片或描述符写失败 → 已 ack 片逐个写墓碑尽力回收
///   ([`cleanup_acked_chunks`])后回原错误;调用方重试拿到全新 blobId,互不相扰;
/// - 与"先暂存后切片"的净语义差 = **网关崩溃窗孤儿片**(无人认领)—— 裁决演进(2026-10-21):
///   恒切无开关;启用 [`upload_wal`](crate::upload_wal) 收账簿后,超时孤儿会话由清扫任务
///   自动回收(Ambry 式收账;残余风险与窗口见该模块注释);未启用 = ④ 无兜底语义。
async fn stream_upload(
    st: &AppState,
    body: Body,
    declared: Option<u64>,
    expiration_ms: i64,
    session: Option<HeaderValue>,
) -> Result<Uploaded, Error> {
    // 声明即大档:session 头校验在任何片入库之前(与旧路径同一 400 位点);自适应路径在转档时校验。
    // 孤儿收账簿(未配置 = None,语义 = ④ 无兜底):begin → 逐片 chunk → 描述符 commit → 失败 abort。
    let wal = st.upload_wal.clone();
    let mut tracked: Option<u64> = None;
    let mut session_raw = session;
    let mut session_val: Option<String> = None;
    let mut chunked = declared.map(|d| d > st.max_body_bytes).unwrap_or(false);
    if chunked {
        session_val = session_of(session_raw.take())?;
        if let Some(w) = &wal {
            tracked = Some(w.begin(crate::upload_wal::now_ms()).await);
        }
    }
    // 片长/窗口 ∈ usize 由 config Validate 保证(auto_chunk_bytes ≤ 1 GiB、max_body ≥ 1)。
    let window = st.max_body_bytes;
    let chunk_len =
        usize::try_from(st.auto_chunk_bytes).expect("auto_chunk_bytes is validated to ≤ 1 GiB");
    let mut buf: bytes::BytesMut = bytes::BytesMut::new();
    let mut written: u64 = 0;
    let mut chunks: Vec<DescriptorChunk> = Vec::new();
    let mut acked: Vec<StoreKey> = Vec::new();
    // 恒切:总量上限 = 公共单文件上限;分片数的物理保底(4096)由发送环另带帽。
    let total_limit = st.max_object_bytes;
    let mut stream = body.into_data_stream();
    let aborted: Option<Error> = 'outer: loop {
        match stream.next().await {
            None => break None, // EOF:单 blob / 收口残片在循环外统一处理
            Some(Err(e)) => {
                break Some(Error::Invalid(format!("body stream: {e}")));
            }
            Some(Ok(bytes)) => {
                written += bytes.len() as u64;
                buf.extend_from_slice(&bytes);
                // 超发(实收 > 声明)与无声明的总量上限:到头即断,不再多读多写。
                if let Some(d) = declared {
                    if written > d {
                        break Some(Error::Invalid("body longer than Content-Length".into()));
                    }
                }
                if declared.is_none() && written > total_limit {
                    break Some(Error::TooLarge {
                        len: written,
                        max: total_limit,
                    });
                }
                if !chunked {
                    if written <= window {
                        continue; // 决定窗内:EOF 前不入库、无片可清
                    }
                    // 越窗 ⇒ 转分片模式(buf 自头部切片,与既有片边界语义一致)。
                    chunked = true;
                    match session_of(session_raw.take()) {
                        Ok(s) => session_val = s,
                        Err(e) => break Some(e),
                    }
                    if let Some(w) = &wal {
                        tracked = Some(w.begin(crate::upload_wal::now_ms()).await);
                    }
                }
                // 攒满即出片:片长恒定;残片留给 EOF 收口。
                while buf.len() >= chunk_len {
                    if chunks.len() >= MAX_CHUNKS {
                        break 'outer Some(Error::TooLarge {
                            len: written,
                            max: total_limit,
                        });
                    }
                    let chunk = buf.split_to(chunk_len).freeze();
                    match st.router.put_auto_with_ttl(chunk, expiration_ms).await {
                        Ok((key, _report)) => {
                            if let (Some(w), Some(sid)) = (wal.as_ref(), tracked) {
                                w.record_chunk(
                                    sid,
                                    &hex::encode(key.as_bytes()),
                                    crate::upload_wal::now_ms(),
                                )
                                .await;
                            }
                            chunks.push(DescriptorChunk {
                                id: key,
                                size: chunk_len as u64,
                                expiration_ms,
                            });
                            acked.push(key);
                        }
                        Err(e) => break 'outer Some(Error::Router(e)),
                    }
                }
            }
        }
    };
    // 分片模式的失败出口:清理 + 失败侧 auto_chunk 计数(窗口阶段失败 = 普通上传失败,不走此口)。
    async fn chunked_fail(
        st: &AppState,
        wal: &Option<Arc<crate::upload_wal::UploadWal>>,
        tracked: Option<u64>,
        acked: &[StoreKey],
        e: Error,
    ) -> Error {
        if let (Some(w), Some(sid)) = (wal, tracked) {
            w.record_abort(sid, crate::upload_wal::now_ms()).await;
        }
        if !acked.is_empty() {
            st.metrics.upload_chunk_cleanups_total.inc();
            cleanup_acked_chunks(st, acked).await;
        }
        fail(&st.metrics, &st.metrics.upload_auto_chunk_total, &[], &e);
        e
    }
    if let Some(e) = aborted {
        return if chunked {
            Err(chunked_fail(st, &wal, tracked, &acked, e).await)
        } else {
            Err(e)
        };
    }
    // EOF:先对账(欠发 = 断流的等价类),再决定单 blob / 残片 + 描述符。
    if let Some(d) = declared {
        if written != d {
            let e = Error::Invalid(format!(
                "body shorter than Content-Length ({written} < {d})"
            ));
            return if chunked {
                Err(chunked_fail(st, &wal, tracked, &acked, e).await)
            } else {
                Err(e)
            };
        }
    }
    if !chunked {
        // 窗口内 EOF = 单 blob(含无声明中小体量与空 body;声明核验已过)。
        let len = written;
        let data = buf.split().freeze();
        let (key, _report) = st.router.put_auto_with_ttl(data, expiration_ms).await?;
        return Ok(Uploaded {
            key,
            len,
            auto_chunks: None,
        });
    }
    if !buf.is_empty() {
        if chunks.len() >= MAX_CHUNKS {
            return Err(chunked_fail(
                st,
                &wal,
                tracked,
                &acked,
                Error::TooLarge {
                    len: written,
                    max: total_limit,
                },
            )
            .await);
        }
        let size = buf.len() as u64;
        let chunk = buf.split().freeze();
        match st.router.put_auto_with_ttl(chunk, expiration_ms).await {
            Ok((key, _report)) => {
                if let (Some(w), Some(sid)) = (wal.as_ref(), tracked) {
                    w.record_chunk(
                        sid,
                        &hex::encode(key.as_bytes()),
                        crate::upload_wal::now_ms(),
                    )
                    .await;
                }
                chunks.push(DescriptorChunk {
                    id: key,
                    size,
                    expiration_ms,
                });
                acked.push(key);
            }
            Err(e) => return Err(chunked_fail(st, &wal, tracked, &acked, Error::Router(e)).await),
        }
    }
    // 描述符与片同 TTL:整个逻辑对象一起过期(与单 blob 的 TTL 读行为对称)。
    let descriptor = encode_descriptor(session_val.as_deref().unwrap_or("").as_bytes(), &chunks)?;
    match st
        .router
        .put_auto_composed_with_ttl(Bytes::from(descriptor), expiration_ms)
        .await
    {
        Ok((key, _report)) => {
            // commit 行先于 201 回报落账:此后清扫器永不作废这些片。
            if let (Some(w), Some(sid)) = (wal.as_ref(), tracked) {
                w.record_commit(
                    sid,
                    &hex::encode(key.as_bytes()),
                    crate::upload_wal::now_ms(),
                )
                .await;
            }
            Ok(Uploaded {
                key,
                len: written,
                auto_chunks: Some(u32::try_from(chunks.len()).expect("chunks ≤ MAX_CHUNKS")),
            })
        }
        Err(e) => Err(chunked_fail(st, &wal, tracked, &acked, Error::Router(e)).await),
    }
}

/// 中途失败后的孤儿片回收:对**本轮已 commit**(多数派 ack)的片逐个写墓碑,尽力而为 ——
/// 删失败只记日志并计入指标,不影响回报给调用方的原错误;孤儿是垃圾、不是数据损坏。片若已写到
/// 少数副本,墓碑多数派 + 拉复制"墓碑恒胜"会把它们盖掉。
async fn cleanup_acked_chunks(st: &AppState, acked: &[StoreKey]) {
    for key in acked {
        st.metrics.upload_auto_chunk_orphan_deletes_total.inc();
        if let Err(e) = st.router.delete(key).await {
            tracing::warn!(
                blob_id = %hex::encode(key.as_bytes()),
                error = %e,
                "auto-chunk orphan cleanup failed"
            );
        }
    }
}

/// `GET /blobs/{blobId}`:200 + 原始字节(R=1 首个成功)。全副本 404 → 404;不可达/未达多数派类 → 503。
async fn get_blob(
    State(st): State<AppState>,
    headers: HeaderMap,
    AxumPath(blob_id): AxumPath<String>,
) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_DOWNLOAD);
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match download(&st, &blob_id, range).await {
        Ok((resp, len)) => {
            st.metrics
                .download_total
                .with_label_values(&[OUTCOME_OK])
                .inc();
            st.metrics.download_bytes.inc_by(len);
            resp
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.download_total, &[], &e);
            e.into_response()
        }
    }
}

/// 读一个对象,返回 `(响应, 响应体字节数)`。
///
/// - 类型与拼装全在 router 层裁决(⑩:网关不识描述符):`stat_object` 给**逻辑大小**,
///   `get_object_*stream` 给流式逻辑字节(内存 = 传输块级,全程零本地文件 —— ⑫);
/// - `Range` 头(blob-range-read):忽略族 → 200 全对象;单区间 → 206 + `Content-Range`,
///   越界/空对象/`bytes=-0` → 416;拼装对象窗口 = router 只取覆盖片(10⁴ 倍浪费消解)。
async fn download(
    st: &AppState,
    blob_id: &str,
    range: Option<&str>,
) -> Result<(Response, u64), Error> {
    let key = parse_blob_id(blob_id)?;
    // 元数据先行:① 逻辑大小(Range 解析基准);② 让"对象不存在"给出干净的 404(而非读一半才发现)。
    let meta = st.router.stat_object(&key).await?;
    let total = meta.size;
    match crate::range::parse_range(range, total) {
        crate::range::RangeOutcome::Full => {
            let (len, stream) = st.router.get_object_stream(&key).await?;
            let mut resp = (StatusCode::OK, Body::from_stream(stream)).into_response();
            put_header(&mut resp, header::ACCEPT_RANGES, "bytes");
            put_header(&mut resp, header::CONTENT_LENGTH, &len.to_string());
            Ok((resp, len))
        }
        crate::range::RangeOutcome::Partial(w) => {
            let stream = st
                .router
                .get_object_range_stream(&key, w.start, w.len)
                .await?;
            let mut resp = (StatusCode::PARTIAL_CONTENT, Body::from_stream(stream)).into_response();
            put_header(&mut resp, header::ACCEPT_RANGES, "bytes");
            put_header(&mut resp, header::CONTENT_RANGE, &w.content_range(total));
            put_header(&mut resp, header::CONTENT_LENGTH, &w.len.to_string());
            Ok((resp, w.len))
        }
        crate::range::RangeOutcome::Unsatisfiable { spec } => {
            // 416 + `Content-Range: bytes */total`(RFC 7233 §4.2 推荐形态,客户端可据此纠偏)。
            tracing::debug!(range = %spec, total, "unsatisfiable range");
            let mut resp = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            put_header(
                &mut resp,
                header::CONTENT_RANGE,
                &format!("bytes */{total}"),
            );
            Ok((resp, 0))
        }
    }
}

/// `HEAD /blobs/{blobId}`:200 + 无 body,状态经 `x-antfs-blob-*` 响应头透出(不读数据体)。
async fn head_blob(
    State(st): State<AppState>,
    headers: HeaderMap,
    AxumPath(blob_id): AxumPath<String>,
) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_HEAD);
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let result = async {
        let key = parse_blob_id(&blob_id)?;
        // 逻辑元数据(拼装对象的 size = 各片之和、片数;router 层语义,⑩)。
        let meta = st.router.stat_object(&key).await?;
        Ok(meta)
    }
    .await;
    match result {
        Ok(meta) => {
            st.metrics
                .mutation_total
                .with_label_values(&[OP_HEAD, OUTCOME_OK])
                .inc();
            let total = meta.size;
            let mut resp = match crate::range::parse_range(range, total) {
                // Ambry 口径(blob-range-read D3):HEAD 带 Range 也回 206(无 body)。
                crate::range::RangeOutcome::Partial(w) => {
                    let mut r = StatusCode::PARTIAL_CONTENT.into_response();
                    put_header(&mut r, header::ACCEPT_RANGES, "bytes");
                    put_header(&mut r, header::CONTENT_RANGE, &w.content_range(total));
                    r
                }
                crate::range::RangeOutcome::Unsatisfiable { .. } => {
                    let mut r = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                    put_header(&mut r, header::CONTENT_RANGE, &format!("bytes */{total}"));
                    r
                }
                crate::range::RangeOutcome::Full => StatusCode::OK.into_response(),
            };
            for (name, value) in stat_headers(&meta) {
                put_header(&mut resp, name, &value);
            }
            // 拼装对象:stitched 两头(逻辑片数)。
            if let Some(n) = meta.chunks {
                put_header(&mut resp, HEADER_BLOB_STITCHED, "true");
                put_header(&mut resp, HEADER_BLOB_STITCH_CHUNKS, &n.to_string());
            }
            resp
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.mutation_total, &[OP_HEAD], &e);
            e.into_response()
        }
    }
}

/// `DELETE /blobs/{blobId}`:写墓碑达多数派 → 204;全副本 404 → 404。
async fn delete_blob(State(st): State<AppState>, AxumPath(blob_id): AxumPath<String>) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_DELETE);
    let result = async {
        let key = parse_blob_id(&blob_id)?;
        st.router.delete(&key).await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            st.metrics
                .delete_total
                .with_label_values(&[OUTCOME_OK])
                .inc();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.delete_total, &[], &e);
            e.into_response()
        }
    }
}

/// `POST /stitch` 的请求体。
#[derive(Deserialize)]
struct StitchRequest {
    /// 片的 blobId 列表(16 hex 一个),顺序即拼装顺序。
    /// `default`:字段缺失按空列表处理 → 统一走"片数 0 → 400"(而不是 axum 抽取器的 422)。
    #[serde(rename = "signedChunkIds", default)]
    signed_chunk_ids: Vec<String>,
}

/// `POST /stitch`:把已上传的片拼成一个对象 → 201(头同 `POST /blobs`)。
///
/// **无状态**:不建 session 表,拼装关系写进一个描述符 blob(片本身是普通 blob),
/// `GET` 读到该描述符即逐片取回并流式拼装。
///
/// body 由本 handler 自己解析(而非 `Json` 抽取器):JSON 语法错误也要按契约回 400(不是 422)。
async fn stitch(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_STITCH);
    let result = async {
        let req: StitchRequest = serde_json::from_slice(&body)
            .map_err(|e| Error::Invalid(format!("invalid JSON body: {e}")))?;
        stitch_inner(&st, &headers, req).await
    }
    .await;
    match result {
        Ok((key, len)) => {
            st.metrics
                .mutation_total
                .with_label_values(&[OP_STITCH, OUTCOME_OK])
                .inc();
            // 同 `post_blob`:每请求细节走 debug,避免压测时刷屏。
            tracing::debug!(
                blob_id = %hex::encode(key.as_bytes()),
                size = len,
                "stitched blob stored"
            );
            created_response(key, len)
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.mutation_total, &[OP_STITCH], &e);
            e.into_response()
        }
    }
}

async fn stitch_inner(
    st: &AppState,
    headers: &HeaderMap,
    body: StitchRequest,
) -> Result<(StoreKey, u64), Error> {
    // session 必填(客户端侧的分片上传会话标识);frontend 只把它写进描述符,不建表、不校验其内容。
    let session = headers
        .get(HEADER_SESSION)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Invalid("missing header x-antfs-session".into()))?;
    if body.signed_chunk_ids.is_empty() || body.signed_chunk_ids.len() > MAX_CHUNKS {
        return Err(Error::Invalid(format!(
            "signedChunkIds must have 1..={MAX_CHUNKS} entries"
        )));
    }
    let mut chunks = Vec::with_capacity(body.signed_chunk_ids.len());
    for id in &body.signed_chunk_ids {
        let key = parse_blob_id(id)
            .map_err(|_| Error::Invalid(format!("chunk {id}: {BLOB_ID_INVALID}")))?;
        // 提交时校验每个片可读,并记下它的长度/过期时刻(读路径据此核对,见 `fetch_chunk`)。
        let meta = st
            .router
            .stat(&key)
            .await
            .map_err(|e| Error::Invalid(format!("chunk {id} is not readable: {e}")))?;
        chunks.push(DescriptorChunk {
            id: key,
            size: meta.size,
            expiration_ms: meta.expiration_ms,
        });
    }
    let descriptor = encode_descriptor(session.as_bytes(), &chunks)?;
    let len = descriptor.len() as u64;
    // 描述符以组合位落库:读侧 stat 报 composed,位是唯一类型判定。
    let key = st.router.allocate_one().await?;
    st.router.put_composed(key, Bytes::from(descriptor)).await?;
    Ok((key, len))
}

/// `GET /metrics`:Prometheus 文本(抓取口,无鉴权)。
async fn get_metrics(State(st): State<AppState>) -> Response {
    match antfs_metric::gather(&st.registry) {
        Ok(text) => (StatusCode::OK, text).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 进程探活(免鉴权/无副作用/不看集群):恒 200 + `text/plain` 正文 `GOOD`,口径对齐
/// Ambry rest 层同名端点 —— 只代表 frontend **进程**活着,不代表集群健康(后端全挂时
/// 探针仍 200,避免「集群越病越被重启」的反向探针雪崩);readiness 语义将来另立端点,不与 liveness 混码。
async fn health_check() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "GOOD",
    )
        .into_response()
}

// ── 拼装描述符编解码 ────────────────────────────────────────────────────────────────────────

/// 描述符里的一个片(小端落盘)。
struct DescriptorChunk {
    /// 片的 blobId。
    id: StoreKey,
    /// 提交拼装时该片的数据长度(读时核对,不一致即上游数据损坏)。
    size: u64,
    /// 提交拼装时该片的过期时刻(epoch 毫秒;`-1` = 不过期)。
    expiration_ms: i64,
}

/// 编码拼装描述符(小端):
///
/// ```text
/// version(1) | chunk_count(u32) | session_len(u16) | session |
/// × chunk_count: blobId(8) | size(u64) | expiration_ms(i64)
/// ```
///
/// 格式本体在 `antfs-descriptor`;此处把本层的 `DescriptorChunk`(id 是 `StoreKey`)
/// 转到纯字节层并映射错误口径。
fn encode_descriptor(session: &[u8], chunks: &[DescriptorChunk]) -> Result<Vec<u8>, Error> {
    let refs: Vec<antfs_descriptor::ChunkRef> = chunks
        .iter()
        .map(|c| {
            let mut id = [0u8; 8];
            id.copy_from_slice(c.id.as_bytes());
            antfs_descriptor::ChunkRef {
                id,
                size: c.size,
                expiration_ms: c.expiration_ms,
            }
        })
        .collect();
    antfs_descriptor::encode(session, &refs).map_err(|e| Error::Invalid(e.to_string()))
}

// ── 请求/响应辅助 ────────────────────────────────────────────────────────────────────────────

/// 请求观测 guard:入口 inc `in_flight`,drop 时 dec 并记录耗时(错误路径与边界校验早退也计)。
struct RequestTimer {
    in_flight: antfs_metric::IntGauge,
    _duration: antfs_metric::HistogramTimer,
}

impl RequestTimer {
    fn start(metrics: &FrontendMetrics, op: &str) -> Self {
        let in_flight = metrics.in_flight.with_label_values(&[op]);
        in_flight.inc();
        RequestTimer {
            in_flight,
            _duration: metrics
                .request_duration
                .with_label_values(&[op])
                .start_timer(),
        }
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        self.in_flight.dec();
    }
}

/// 失败收口:主 counter_vec 记 `outcome`,并把 SDK 错误的 `kind` 计入 `frontend_replica_errors_total`。
fn fail(metrics: &FrontendMetrics, counter: &IntCounterVec, labels: &[&str], e: &Error) {
    let mut all: Vec<&str> = labels.to_vec();
    all.push(e.outcome());
    counter.with_label_values(&all).inc();
    if let Error::Router(source) = e {
        metrics
            .replica_errors
            .with_label_values(&[source.kind()])
            .inc();
    }
}

/// blobId 文本(16 hex)→ [`StoreKey`];非 8 字节一律 [`Error::Invalid`](400)。
fn parse_blob_id(blob_id: &str) -> Result<StoreKey, Error> {
    let bytes = hex::decode(blob_id).map_err(|_| Error::Invalid(BLOB_ID_INVALID.into()))?;
    StoreKey::from_slice(&bytes).ok_or_else(|| Error::Invalid(BLOB_ID_INVALID.into()))
}

/// 已提交对象的响应(201 + `Location` / `x-antfs-blob-id` / `x-antfs-blob-size`),上传与拼装共用。
fn created_response(key: StoreKey, len: u64) -> Response {
    let blob_id = hex::encode(key.as_bytes());
    let mut resp = StatusCode::CREATED.into_response();
    put_header(&mut resp, header::LOCATION, &format!("/blobs/{blob_id}"));
    put_header(&mut resp, HEADER_BLOB_ID, &blob_id);
    put_header(&mut resp, HEADER_BLOB_SIZE, &len.to_string());
    resp
}

/// `HEAD` 的状态头(布尔值用 `"true"`/`"false"`)。
fn stat_headers(meta: &ObjectStat) -> [(HeaderName, String); 4] {
    [
        (HEADER_BLOB_SIZE, meta.size.to_string()),
        (HEADER_BLOB_DELETED, meta.deleted.to_string()),
        (HEADER_BLOB_EXPIRED, meta.expired.to_string()),
        (HEADER_BLOB_EXPIRATION_MS, meta.expiration_ms.to_string()),
    ]
}

/// 写响应头。值都是数字/布尔/hex,编不成 header 值不可能发生;真发生就**不带头**(禁 unwrap)。
fn put_header(resp: &mut Response, name: HeaderName, value: &str) {
    match HeaderValue::from_str(value) {
        Ok(v) => {
            resp.headers_mut().insert(name, v);
        }
        Err(e) => tracing::warn!(header = %name, error = %e, "response header dropped"),
    }
}

/// 上传可选头 `x-antfs-ttl-ms`:给了就用带 TTL 的写入;给了但非法 → 400。
fn ttl_header(headers: &HeaderMap) -> Result<Option<i64>, Error> {
    match headers.get(HEADER_TTL_MS) {
        None => Ok(None),
        Some(raw) => {
            let text = raw.to_str().map_err(|_| {
                Error::Invalid("x-antfs-ttl-ms must be an integer (epoch ms)".into())
            })?;
            text.trim()
                .parse::<i64>()
                .map(Some)
                .map_err(|_| Error::Invalid("x-antfs-ttl-ms must be an integer (epoch ms)".into()))
        }
    }
}

/// 请求声明的 body 长度(`Content-Length`);缺省 = chunked 或未声明 → 边收边计数。
fn content_length(req: &Request) -> Option<u64> {
    req.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// 配置的 body 上限 → `usize`;超出 `usize`(仅 32 位平台)`usize::MAX` 等价于不限制。
fn body_limit(max_body_bytes: u64) -> usize {
    usize::try_from(max_body_bytes).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(pid: u32, object: u32, size: u64) -> DescriptorChunk {
        DescriptorChunk {
            id: StoreKey::from_parts(pid, object),
            size,
            expiration_ms: -1,
        }
    }

    /// 描述符编解码往返:片序、长度、过期时刻、session 长度都原样保留。
    #[test]
    fn descriptor_roundtrip() {
        let chunks = vec![chunk(7, 1, 100), chunk(7, 2, 4096), chunk(9, 3, 0)];
        let buf = encode_descriptor(b"session-abc", &chunks).expect("encode");
        assert_eq!(buf[0], antfs_descriptor::VERSION, "版本位在首字节");
        let back = antfs_descriptor::parse(&buf).expect("parse");
        assert_eq!(back.len(), 3);
        for (got, want) in back.iter().zip(chunks.iter()) {
            assert_eq!(got.id, *want.id.as_bytes(), "片序必须原样保留");
            assert_eq!(got.size, want.size);
            assert_eq!(got.expiration_ms, want.expiration_ms);
        }
        // 头长 + session + 每条目定长:描述符长度可精确算出。
        assert_eq!(
            buf.len(),
            antfs_descriptor::HEADER_BYTES
                + "session-abc".len()
                + 3 * antfs_descriptor::ENTRY_BYTES
        );
    }

    /// 编解码的边界:空片列表、片数超上限、session 超长、截断/多余字节、版本不认识。
    #[test]
    fn descriptor_rejects_bad_input() {
        assert!(encode_descriptor(b"s", &[]).is_err(), "0 片必须拒");
        let too_many: Vec<DescriptorChunk> =
            (0..MAX_CHUNKS + 1).map(|i| chunk(1, i as u32, 1)).collect();
        assert!(
            encode_descriptor(b"s", &too_many).is_err(),
            "片数超上限必须拒"
        );
        let long_session = vec![b'x'; 70_000];
        assert!(
            encode_descriptor(&long_session, &[chunk(1, 1, 1)]).is_err(),
            "session 超过 u16 长度域必须拒"
        );

        let buf = encode_descriptor(b"s", &[chunk(1, 1, 5)]).expect("encode");
        assert!(
            antfs_descriptor::parse(&buf[..buf.len() - 1]).is_err(),
            "截断必须拒"
        );
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(
            antfs_descriptor::parse(&trailing).is_err(),
            "多余字节必须拒"
        );
        let mut bad_version = buf.clone();
        bad_version[0] = antfs_descriptor::VERSION + 1;
        assert!(
            antfs_descriptor::parse(&bad_version).is_err(),
            "未知版本必须拒"
        );
        assert!(
            antfs_descriptor::parse(b"nope-nope-nope").is_err(),
            "坏版本必须拒"
        );
        assert!(antfs_descriptor::parse(&[]).is_err(), "空 body 必须拒");
    }

    /// blobId 文本形式:非 hex / 长度不是 8 字节都归 400,且文案固定。
    #[test]
    fn parse_blob_id_requires_16_hex() {
        let key = parse_blob_id("0000000100000002").expect("16 hex 必须能解析");
        assert_eq!(key, StoreKey::from_parts(1, 2));
        for bad in ["zz", "", "0f", "000000010000000200"] {
            let err = parse_blob_id(bad).expect_err("必须拒");
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
            assert!(
                err.to_string().contains(BLOB_ID_INVALID),
                "文案必须稳定: {err}"
            );
        }
    }

    /// 状态码映射:404 / 507 / 503 / 400 / 500 各归其位(唯一映射点的回归护栏)。
    #[test]
    fn status_mapping() {
        use antfs_router::Error as E;
        assert_eq!(
            router_status(&E::NotFound { pid: 1 }),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            router_status(&E::StoreFull { addr: "a:1".into() }),
            StatusCode::INSUFFICIENT_STORAGE,
            "分区写满必须 507,不混进 500"
        );
        for e in [
            E::Quorum {
                acks: 1,
                required: 2,
                total: 3,
                detail: "d".into(),
            },
            E::NoReplica {
                addr: "a:1".into(),
                pid: 2,
            },
            E::Silenced {
                addr: "a:1".into(),
                retry_after_secs: Some(30),
            },
            E::Rejected("not leader".into()),
            E::NoWritablePartition,
            E::IdExhausted(3),
        ] {
            assert_eq!(router_status(&e), StatusCode::SERVICE_UNAVAILABLE, "{e}");
        }
        // key 冲突(副本 409 全败透传)→ 409(与 store 数据面同码);本地 IO:不是调用方能改的,归 500。
        assert_eq!(
            router_status(&E::AlreadyExists { addr: "a:1".into() }),
            StatusCode::CONFLICT
        );
        assert_eq!(
            router_status(&E::Io(std::io::Error::other("x"))),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(Error::Invalid("x".into()).status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            Error::TooLarge { len: 10, max: 1 }.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "超 body 上限必须 413"
        );
        assert_eq!(Error::NotFound.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            Error::Upstream("x".into()).status(),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            Error::Io(std::io::Error::other("x")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        // `Silenced` 必须把剩余秒数透传成 `Retry-After`;缺省则不带该头。
        let resp = Error::Router(E::Silenced {
            addr: "a:1".into(),
            retry_after_secs: Some(30),
        })
        .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(header::RETRY_AFTER)
                .map(|v| v.as_bytes()),
            Some(b"30".as_slice())
        );
        let resp = Error::Router(E::Silenced {
            addr: "a:1".into(),
            retry_after_secs: None,
        })
        .into_response();
        assert!(
            resp.headers().get(header::RETRY_AFTER).is_none(),
            "永久静默不带 Retry-After"
        );
    }

    /// `outcome` label 是稳定取值:SDK 错误沿用 `Error::kind()`,本层错误用固定分类。
    #[test]
    fn outcome_labels_are_stable() {
        assert_eq!(
            Error::Router(RouterError::Quorum {
                acks: 1,
                required: 2,
                total: 3,
                detail: String::new()
            })
            .outcome(),
            "quorum"
        );
        assert_eq!(Error::Invalid("x".into()).outcome(), "invalid");
        assert_eq!(Error::TooLarge { len: 9, max: 1 }.outcome(), "too_large");
        assert_eq!(Error::NotFound.outcome(), "not_found");
        assert_eq!(Error::Upstream("x".into()).outcome(), "upstream");
    }

    #[test]
    fn cors_layer_is_absent_by_default() {
        assert!(cors_layer(&[]).is_none(), "默认不挂 CORS 层");
        assert!(
            cors_layer(&["https://a.example".into()]).is_some(),
            "显式来源才挂"
        );
    }

    /// 自动分片的数学口径:片数向上取整;单文件容量 = 片数上限 × 单片(公共上限须 ≤ 它)。
    #[test]
    fn auto_chunk_math() {
        let chunk = 4 * 1024 * 1024;
        assert_eq!(1u64.div_ceil(chunk), 1);
        assert_eq!(chunk.div_ceil(chunk), 1);
        assert_eq!((chunk + 1).div_ceil(chunk), 2, "恰整除不加片,过了界加一片");
        // 默认片大小下描述符容量 = 16 GiB(4096 × 4 MiB),恰等于公共上限默认(跨界校验基线)。
        assert_eq!(MAX_CHUNKS as u64 * chunk, 16 * 1024 * 1024 * 1024);
        // 配置上限内的单片,乘积必须在 u64 内(校验层防溢出的回归护栏)。
        assert!(MAX_CHUNKS as u64 * crate::config::AUTO_CHUNK_MAX_ALLOWED_BYTES < u64::MAX);
    }

    /// 分片路径的 session 头:缺省/正常通过;非文本、超 65535 都必须 400(且不往集群写任何片)。
    #[test]
    fn session_validation_for_auto_chunk() {
        assert_eq!(session_of(None).unwrap(), None);
        assert_eq!(
            session_of(HeaderValue::from_str("sess-abc").ok()).unwrap(),
            Some("sess-abc".to_string())
        );

        let err =
            session_of(HeaderValue::from_bytes(&[0xff, 0xfe]).ok()).expect_err("非文本必须拒");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        let long = "x".repeat(u16::MAX as usize + 1);
        let err = session_of(HeaderValue::from_str(&long).ok()).expect_err("超长必须拒");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
