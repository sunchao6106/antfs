//! frontend HTTP 服务:装配、路由、handler(HTTP ↔ router SDK 的翻译层)。
//!
//! **本层只做翻译**:写 W(该分区多数派)/ 读 R=1 / 向 master 取 blobId 号段全部在
//! [`antfs_router`](router SDK)里实现(`cluster.md §10`),这里不重新实现任何一致性逻辑,
//! 也不落每对象元数据(D3:无中心每对象元数据)—— 故服务**无状态、可水平扩展**。
//!
//! 两条与"无状态"直接相关的设计:
//! - **分片拼装(`POST /stitch`)不用服务端 session 表**:拼装关系写进一个**描述符 blob**
//!   (见 [`encode_descriptor`]),片本身是普通 blob,`GET` 读到描述符即逐片流式拼装;
//! - **上传先落暂存文件再提交**:SDK 的流式写口是 `put_file(path)`,代价是一次额外落盘(与 store
//!   数据面同一取舍),换来 frontend 内存不随对象大小涨(CLAUDE.md §5.3)。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tower_http::cors::{AllowOrigin, CorsLayer};

use antfs_metric::IntCounterVec;
use antfs_router::{BlobStat, Error as RouterError, Router as BlobRouter, StoreKey, NO_EXPIRATION};

use crate::config::FrontendConfig;
use crate::metrics::{
    FrontendMetrics, OP_DELETE, OP_DOWNLOAD, OP_HEAD, OP_STITCH, OP_UPLOAD, OUTCOME_OK,
};

// ── 请求/响应头(对齐 Ambry frontend 的 `x-ambry-*` 契约)────────────────────────────────────

/// `x-ambry-session`:分片上传的会话标识(客户端生成;片就是普通 blob,frontend 只把它记进描述符)。
const HEADER_SESSION: HeaderName = HeaderName::from_static("x-ambry-session");
/// `x-ambry-ttl-ms`:上传时可选带的过期时刻(epoch 毫秒)。
const HEADER_TTL_MS: HeaderName = HeaderName::from_static("x-ambry-ttl-ms");
/// `x-ambry-blob-id`:blobId 的 16 位 hex。
const HEADER_BLOB_ID: HeaderName = HeaderName::from_static("x-ambry-blob-id");
/// `x-ambry-blob-size`:数据体字节数。
const HEADER_BLOB_SIZE: HeaderName = HeaderName::from_static("x-ambry-blob-size");
/// `x-ambry-blob-deleted`:`"true"`/`"false"`。
const HEADER_BLOB_DELETED: HeaderName = HeaderName::from_static("x-ambry-blob-deleted");
/// `x-ambry-blob-expired`:`"true"`/`"false"`。
const HEADER_BLOB_EXPIRED: HeaderName = HeaderName::from_static("x-ambry-blob-expired");
/// `x-ambry-blob-expiration-ms`:过期时刻(epoch 毫秒;`-1` = 不过期)。
const HEADER_BLOB_EXPIRATION_MS: HeaderName = HeaderName::from_static("x-ambry-blob-expiration-ms");
/// `x-ambry-blob-stitched`:该对象是否为分片拼装(`HEAD` 专用;`GET` 会拼装成逻辑字节流)。
const HEADER_BLOB_STITCHED: HeaderName = HeaderName::from_static("x-ambry-blob-stitched");
/// `x-ambry-blob-stitch-chunks`:拼装的片数(`HEAD` 专用,仅拼装对象有)。
const HEADER_BLOB_STITCH_CHUNKS: HeaderName = HeaderName::from_static("x-ambry-blob-stitch-chunks");

/// **直接进内存**的 body 上界:≤ 该值走内存路径(`Bytes` → `Router::put`),> 该值(或未声明长度)
/// 才落盘暂存 + [`Router::put_file`] 流式写。
///
/// 为什么小对象不走暂存:暂存要 `create/write/close/unlink` 一个文件,每个上传多 ~5 次文件系统元数据操作;
/// 小对象是绝对主流,大并发下这套开销把 PUT p50 从 ~1ms 拖到 ~9ms(devcluster + bench 实测,release)。
/// 内存上界 = 在途请求数 × 该值(1 MiB)。与 `store` 侧的同名阈值同因(见 `crates/store/src/server.rs`)。
const INLINE_BODY_MAX: u64 = 1024 * 1024;

/// 跨域时允许的请求头(仅本接口真正用到的三个)。
const CORS_ALLOWED_HEADERS: [HeaderName; 3] = [header::CONTENT_TYPE, HEADER_SESSION, HEADER_TTL_MS];
/// 跨域时暴露给浏览器的响应头(全部 `x-ambry-*` + `Location`)。
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

// ── 拼装描述符格式(无状态拼装的落盘契约;格式演进见 CLAUDE.md §3.4)────────────────────────────

/// 描述符 MAGIC(4 字节):`GET` 见此前缀即按描述符解析。
const DESCRIPTOR_MAGIC: &[u8; 4] = b"ASTC";
/// 描述符格式版本(读到不认识的版本即报上游数据损坏)。
const DESCRIPTOR_VERSION: u8 = 1;
/// 单个拼装对象的片数上限。
const MAX_CHUNKS: usize = 4096;
/// 描述符字节上限(1 MiB):`GET` 据此决定"整体读出判断 MAGIC"还是"直接流式"(CLAUDE.md §5.3);
/// 片数与会话长度各自设限后,正常永远到不了这个上界(4096 片最多约 160 KiB)。
const DESCRIPTOR_MAX_BYTES: u64 = 1024 * 1024;
/// 描述符固定头:`MAGIC(4) | version(1) | chunk_count(u32) | session_len(u16)`。
const DESCRIPTOR_HEADER_BYTES: usize = 11;
/// 描述符里每片的条目长:`blobId(8) | size(u64) | expiration_ms(i64)`。
const DESCRIPTOR_ENTRY_BYTES: usize = 24;

// ── 流式读写参数 ────────────────────────────────────────────────────────────────────────────

/// 暂存文件/响应体的单块缓冲(内存 = 一块,不随对象大小涨)。
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// frontend 的错误出口:边界校验类自身错误 + SDK 错误的归一。
///
/// 映射规则集中在本类型的 [`Error::status`](唯一真值),handler 里不得另判。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 调用方请求非法(blobId 格式、缺参数、片数越界、session 过长…)。
    #[error("{0}")]
    Invalid(String),

    /// 请求体超过 `frontend.max_body_bytes`。
    #[error("body {len} exceeds frontend.max_body_bytes {max}")]
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
        // 分区写满:507 让调用方/压测一眼看出"该加分区了"(与 store 数据面同一约定),不混进 500。
        RouterError::StoreFull { .. } => StatusCode::INSUFFICIENT_STORAGE,
        // 暂时性失败:未达多数派 / 副本不可达 / 被静默 / 路由到无副本节点 / 号段耗尽 / master 不可用。
        // 都可能自愈或换个副本就好,统一 503 让调用方重试。
        RouterError::Quorum { .. }
        | RouterError::Http { .. }
        | RouterError::NoReplica { .. }
        | RouterError::Silenced { .. }
        | RouterError::IdExhausted(_)
        | RouterError::NoWritablePartition
        | RouterError::Master(_) => StatusCode::SERVICE_UNAVAILABLE,
        // master 明确业务性拒绝(非 leader / 未知分区 / 分区不可写):调用方改不了,也不是本层 bug。
        RouterError::Rejected(_) => StatusCode::SERVICE_UNAVAILABLE,
        // 副本返回的状态码:**与 store 数据面一致地透传**调用方可控的那几类 —— 400/413 是 payload 问题、
        // 507 是容量问题、503 是暂时不可用;其余才算意料之外,归 500(契约:以上各类之外 → 500)。
        RouterError::ReplicaStatus { status, .. } => match *status {
            400 => StatusCode::BAD_REQUEST,
            413 => StatusCode::PAYLOAD_TOO_LARGE,
            503 => StatusCode::SERVICE_UNAVAILABLE,
            507 => StatusCode::INSUFFICIENT_STORAGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        // 号被复用 / blobId 与目标分区不符 / 配置或传输层损坏:运维或数据损坏问题。
        RouterError::AlreadyExists { .. }
        | RouterError::PartitionMismatch { .. }
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
    /// 单请求 body 上限(字节)。
    pub max_body_bytes: u64,
    /// CORS 允许来源;空 = 不挂 CORS 层。
    pub cors_allowed_origins: Vec<String>,
}

impl AppState {
    /// 按配置装配:连 router SDK(懒连接 master)+ 注册指标。
    pub async fn from_config(
        cfg: &FrontendConfig,
        registry: &antfs_metric::Registry,
    ) -> Result<Self, RouterError> {
        let router = Arc::new(BlobRouter::connect(cfg.router.clone(), registry).await?);
        Ok(AppState {
            router,
            metrics: Arc::new(FrontendMetrics::new(registry)?),
            registry: registry.clone(),
            max_body_bytes: cfg.max_body_bytes,
            cors_allowed_origins: cfg.cors_allowed_origins.clone(),
        })
    }
}

/// 建路由:`/blobs*` 与 `/stitch` 是对象接口,`/metrics` 是 Prometheus 抓取口(无鉴权,抓取惯例)。
///
/// body 上限两层:router 级 [`DefaultBodyLimit`] 兜住 JSON 抽取器(如 `/stitch`),`/blobs` 的裸字节
/// 体由 handler 自己边收边计数(见 [`stage_body`])。
pub fn build_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/blobs", post(post_blob))
        .route(
            "/blobs/{blob_id}",
            get(get_blob).head(head_blob).delete(delete_blob),
        )
        .route("/stitch", post(stitch))
        .route("/metrics", get(get_metrics))
        .layer(DefaultBodyLimit::max(body_limit(state.max_body_bytes)))
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

/// `POST /blobs`:body = 裸 blob 字节 → 201 + `Location` / `x-ambry-blob-id` / `x-ambry-blob-size`。
///
/// 分片上传的**片就是普通 blob**(带 `x-ambry-session` 也不改变写入路径),片清单由客户端自己记
/// —— frontend 无状态(D3)。可选头 `x-ambry-ttl-ms` 见 [`upload`]。
async fn post_blob(State(st): State<AppState>, req: Request) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_UPLOAD);
    match upload(&st, req).await {
        Ok((key, len)) => {
            st.metrics
                .upload_total
                .with_label_values(&[OUTCOME_OK])
                .inc();
            st.metrics.upload_bytes.inc_by(len);
            // 每请求成功的细节走 debug:压测下 2 万次上传会在 INFO 打出 2 万行,日志本身成为热点。
            tracing::debug!(
                blob_id = %hex::encode(key.as_bytes()),
                size = len,
                "blob stored"
            );
            created_response(key, len)
        }
        Err(e) => {
            fail(&st.metrics, &st.metrics.upload_total, &[], &e);
            tracing::warn!(error = %e, "upload failed");
            e.into_response()
        }
    }
}

/// 上传实现:边界校验 → 小 body 进内存([`INLINE_BODY_MAX`] 内)/大 body 落暂存流式 → 写入时定 TTL
/// (`x-ambry-ttl-ms`,缺省不过期)→ **一次**多数派写。
async fn upload(st: &AppState, req: Request) -> Result<(StoreKey, u64), Error> {
    // 先解析 TTL 头:非法立即 400,不白读一遍 body。
    let expiration_ms = ttl_header(req.headers())?.unwrap_or(NO_EXPIRATION);
    let declared = content_length(&req);
    if let Some(len) = declared {
        if len > st.max_body_bytes {
            return Err(Error::TooLarge {
                len,
                max: st.max_body_bytes,
            });
        }
    }
    // 小 body 直接进内存(有界):见 `INLINE_BODY_MAX` —— 每个小对象都落一个暂存文件在 macOS 上
    // 代价过高(实测 PUT p50 ~1ms → ~9ms)。大 body(或未声明长度)才落盘 + 流式,内存不随大小涨。
    //
    // TTL 随**同一次**写请求下发(`PUT ?expiration_ms=`),故不存在"对象已创建但没带 TTL"的中间态。
    let (key, written) = match declared {
        Some(len) if len <= INLINE_BODY_MAX => {
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
            let key = st.router.allocate_one().await?;
            st.router.put_with_ttl(key, bytes, expiration_ms).await?;
            (key, len)
        }
        _ => {
            let staging = TempFile::create();
            let written =
                stage_body(req.into_body(), staging.path(), declared, st.max_body_bytes).await?;
            let (key, _report) = st
                .router
                .put_file_with_ttl(staging.path(), expiration_ms)
                .await?;
            (key, written)
        }
    };
    Ok((key, written))
}

/// `GET /blobs/{blobId}`:200 + 原始字节(R=1 首个成功)。全副本 404 → 404;不可达/未达多数派类 → 503。
async fn get_blob(State(st): State<AppState>, AxumPath(blob_id): AxumPath<String>) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_DOWNLOAD);
    match download(&st, &blob_id).await {
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
/// - **小对象**(`stat.size <= DESCRIPTOR_MAX_BYTES`)整体读回,以便判断它是不是拼装描述符;
/// - **大对象不整体进内存**(CLAUDE.md §5.3):`get_file` 落暂存文件后流式回给调用方。
///   SDK 只有 `get`(整体进内存)与 `get_file`(落文件)两个读口,后者才不把对象整个缓进内存。
async fn download(st: &AppState, blob_id: &str) -> Result<(Response, u64), Error> {
    let key = parse_blob_id(blob_id)?;
    // 先取元数据:① 拿 size 决定读路径;② 顺便让"对象不存在"给出干净的 404(而非读一半才发现)。
    let meta = st.router.stat(&key).await?;
    if meta.size <= DESCRIPTOR_MAX_BYTES {
        let body = st.router.get(&key).await?;
        if body.starts_with(DESCRIPTOR_MAGIC) {
            // 契约:前 4 字节 == MAGIC 即按描述符解析。
            // ⚠ 已知待议(见 `openspec/changes/blob-io-end-to-end/tasks.md` 待办首条):描述符与普通对象在存储层
            // 无类型区分,只能靠这里嗅探 → ≤1 MiB 且以 `ASTC` 开头的普通 blob 会被误判(502,或被当清单展开后
            // **静默返回别人的内容**)。修法建议:给 store 记录头的 flags 加一个 composed 位(SeaweedFS needle
            // `FlagIsChunkManifest` 同款),届时这里改成只看标志位、不再嗅探。
            return stitched_body(st, &body).await;
        }
        let len = body.len() as u64;
        return Ok(((StatusCode::OK, body).into_response(), len));
    }
    let staging = TempFile::create();
    let len = st.router.get_file(&key, staging.path()).await?;
    let file = tokio::fs::File::open(staging.path()).await?;
    // 暂存文件随响应体流一起 drop → 发完/连接断开即删。
    let stream = file_stream(file, staging, STREAM_CHUNK_BYTES);
    Ok((Body::from_stream(stream).into_response(), len))
}

/// `HEAD /blobs/{blobId}`:200 + 无 body,状态经 `x-ambry-blob-*` 响应头透出(不读数据体)。
async fn head_blob(State(st): State<AppState>, AxumPath(blob_id): AxumPath<String>) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_HEAD);
    let result = async {
        let key = parse_blob_id(&blob_id)?;
        let mut meta = st.router.stat(&key).await?;
        // 拼装对象:`stat` 只看到**描述符**那个 blob(几百字节),直接报出去会与 `GET`(拼装后的逻辑字节流)对不上。
        // 故这里做与 `download` 同样的 MAGIC 嗅探,把 size 换成各片之和,并回 `x-ambry-blob-stitched`。
        // 代价可控:描述符按构造 ≤ DESCRIPTOR_MAX_BYTES,故**只有小对象**才需要多读一次数据体(≤1 MiB);
        // size 更大者不可能是描述符,直接按原 size 回(不读数据体,保持 HEAD 的语义)。
        let mut stitched_chunks = None;
        if !meta.deleted && !meta.expired && meta.size <= DESCRIPTOR_MAX_BYTES {
            if let Ok(body) = st.router.get(&key).await {
                if body.starts_with(DESCRIPTOR_MAGIC) {
                    if let Ok(chunks) = decode_descriptor(&body) {
                        meta.size = chunks.iter().map(|c| c.size).sum();
                        stitched_chunks = Some(chunks.len());
                    }
                }
            }
        }
        Ok((meta, stitched_chunks))
    }
    .await;
    match result {
        Ok((meta, stitched_chunks)) => {
            st.metrics
                .mutation_total
                .with_label_values(&[OP_HEAD, OUTCOME_OK])
                .inc();
            let mut resp = StatusCode::OK.into_response();
            for (name, value) in stat_headers(&meta) {
                put_header(&mut resp, name, &value);
            }
            if let Some(n) = stitched_chunks {
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
        .ok_or_else(|| Error::Invalid("missing header x-ambry-session".into()))?;
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
    // 描述符本身就是个普通 blob(故它只有描述符那么小,`GET` 能整体读出来嗅探 MAGIC)。
    let key = st.router.allocate_one().await?;
    st.router.put(key, Bytes::from(descriptor)).await?;
    Ok((key, len))
}

/// `GET /metrics`:Prometheus 文本(抓取口,无鉴权)。
async fn get_metrics(State(st): State<AppState>) -> Response {
    match antfs_metric::gather(&st.registry) {
        Ok(text) => (StatusCode::OK, text).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
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
/// MAGIC(4) | version(1) | chunk_count(u32) | session_len(u16) | session |
/// × chunk_count: blobId(8) | size(u64) | expiration_ms(i64)
/// ```
fn encode_descriptor(session: &[u8], chunks: &[DescriptorChunk]) -> Result<Vec<u8>, Error> {
    let chunk_count = u32::try_from(chunks.len())
        .map_err(|_| Error::Invalid(format!("chunk count must be in 1..={MAX_CHUNKS}")))?;
    if chunk_count == 0 || chunks.len() > MAX_CHUNKS {
        return Err(Error::Invalid(format!(
            "chunk count must be in 1..={MAX_CHUNKS}"
        )));
    }
    let session_len = u16::try_from(session.len())
        .map_err(|_| Error::Invalid("x-ambry-session must be at most 65535 bytes".into()))?;
    let mut out = Vec::with_capacity(
        DESCRIPTOR_HEADER_BYTES + session.len() + chunks.len() * DESCRIPTOR_ENTRY_BYTES,
    );
    out.extend_from_slice(DESCRIPTOR_MAGIC);
    out.push(DESCRIPTOR_VERSION);
    out.extend_from_slice(&chunk_count.to_le_bytes());
    out.extend_from_slice(&session_len.to_le_bytes());
    out.extend_from_slice(session);
    for c in chunks {
        out.extend_from_slice(c.id.as_bytes());
        out.extend_from_slice(&c.size.to_le_bytes());
        out.extend_from_slice(&c.expiration_ms.to_le_bytes());
    }
    // 兜底:片数与 session 长度各自设限后到不了这个上界,守住它才能保证 `GET` 的整体读判定成立。
    if out.len() as u64 > DESCRIPTOR_MAX_BYTES {
        return Err(Error::Invalid(format!(
            "stitch descriptor exceeds {DESCRIPTOR_MAX_BYTES} bytes"
        )));
    }
    Ok(out)
}

/// 解码拼装描述符。任何结构性错误 → [`Error::Upstream`](502):
/// 能读到 MAGIC 说明这是本层写下的对象,解析不了即上游数据损坏。
fn decode_descriptor(buf: &[u8]) -> Result<Vec<DescriptorChunk>, Error> {
    let bad = |msg: &str| Error::Upstream(format!("corrupt stitch descriptor: {msg}"));
    if !buf.starts_with(DESCRIPTOR_MAGIC) {
        return Err(bad("bad magic"));
    }
    let mut cur = Cursor {
        buf,
        pos: DESCRIPTOR_MAGIC.len(),
    };
    let version = cur.u8()?;
    if version != DESCRIPTOR_VERSION {
        return Err(bad(&format!("unsupported descriptor version {version}")));
    }
    let chunk_count = usize::try_from(cur.u32()?).map_err(|_| bad("chunk count overflow"))?;
    if chunk_count == 0 || chunk_count > MAX_CHUNKS {
        return Err(bad("chunk count out of range"));
    }
    let session_len = usize::from(cur.u16()?);
    cur.take(session_len)?; // session 内容只在客户端侧有意义,这里只跳过。
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        let id = StoreKey::from_slice(cur.take(8)?).ok_or_else(|| bad("bad chunk id"))?;
        chunks.push(DescriptorChunk {
            id,
            size: cur.u64()?,
            expiration_ms: cur.i64()?,
        });
    }
    if cur.pos != buf.len() {
        return Err(bad("trailing bytes"));
    }
    Ok(chunks)
}

/// 描述符的小端游标:越界即报错,不做任何索引/切片 panic。
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let bad = |msg: &str| Error::Upstream(format!("corrupt stitch descriptor: {msg}"));
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| bad("length overflow"))?;
        let out = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| bad("truncated"))?;
        self.pos = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| Error::Upstream("corrupt stitch descriptor: truncated".into()))
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(u8::from_le_bytes(self.array::<1>()?))
    }

    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(self.array::<2>()?))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.array::<4>()?))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.array::<8>()?))
    }

    fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_le_bytes(self.array::<8>()?))
    }
}

// ── 读取与拼装 ──────────────────────────────────────────────────────────────────────────────

/// 把描述符描述的片**流式**拼进响应体(R=1 逐片取;内存 = 单片,不随对象大小涨)。
async fn stitched_body(st: &AppState, descriptor: &[u8]) -> Result<(Response, u64), Error> {
    let chunks = decode_descriptor(descriptor)?;
    let total: u64 = chunks.iter().map(|c| c.size).sum();
    // 首片先做一次元数据预检:整对象已不可读时给调用方一个明确的 502,而不是"发到一半才断"的 200。
    // 其余片在流里惰性取(描述符写入时已校验全部片可读;读时缺片即上游数据被删/损坏)。
    let first = chunks
        .first()
        .ok_or_else(|| Error::Upstream("descriptor has no chunk".into()))?;
    st.router.stat(&first.id).await.map_err(|e| {
        Error::Upstream(format!(
            "chunk {} is unreadable: {e}",
            hex::encode(first.id.as_bytes())
        ))
    })?;

    let client = st.router.clone();
    let body = stream::iter(chunks).then(move |chunk| {
        let client = client.clone();
        async move { fetch_chunk(&client, &chunk).await }
    });
    Ok((Body::from_stream(body).into_response(), total))
}

/// 取一个片,并核对长度与描述符记录一致(不一致 = 上游数据损坏:记录并中断响应体,CLAUDE.md §3.3)。
async fn fetch_chunk(client: &BlobRouter, chunk: &DescriptorChunk) -> std::io::Result<Bytes> {
    let id = hex::encode(chunk.id.as_bytes());
    let data = client
        .get(&chunk.id)
        .await
        .map_err(|e| std::io::Error::other(format!("chunk {id} unavailable: {e}")))?;
    if data.len() as u64 != chunk.size {
        tracing::warn!(
            chunk_id = %id,
            expected = chunk.size,
            actual = data.len(),
            "stitched chunk size mismatch"
        );
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("chunk {id} size mismatch"),
        ));
    }
    Ok(data)
}

/// 把一个本地文件读成流(每次一块,内存 = 单块)。`guard` 随流一起 drop → 响应体发完/连接断开时
/// 删掉暂存文件。
fn file_stream(
    file: tokio::fs::File,
    guard: TempFile,
    buf_bytes: usize,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    struct State {
        file: tokio::fs::File,
        _guard: TempFile,
        buf: Vec<u8>,
    }
    let init = State {
        file,
        _guard: guard,
        buf: vec![0u8; buf_bytes],
    };
    stream::try_unfold(init, |mut st| async move {
        let n = st.file.read(&mut st.buf).await?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some((Bytes::copy_from_slice(&st.buf[..n]), st)))
    })
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

/// 已提交对象的响应(201 + `Location` / `x-ambry-blob-id` / `x-ambry-blob-size`),上传与拼装共用。
fn created_response(key: StoreKey, len: u64) -> Response {
    let blob_id = hex::encode(key.as_bytes());
    let mut resp = StatusCode::CREATED.into_response();
    put_header(&mut resp, header::LOCATION, &format!("/blobs/{blob_id}"));
    put_header(&mut resp, HEADER_BLOB_ID, &blob_id);
    put_header(&mut resp, HEADER_BLOB_SIZE, &len.to_string());
    resp
}

/// `HEAD` 的状态头(布尔值用 `"true"`/`"false"`)。
fn stat_headers(meta: &BlobStat) -> [(HeaderName, String); 4] {
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

/// 上传可选头 `x-ambry-ttl-ms`:给了就用带 TTL 的写入;给了但非法 → 400。
fn ttl_header(headers: &HeaderMap) -> Result<Option<i64>, Error> {
    match headers.get(HEADER_TTL_MS) {
        None => Ok(None),
        Some(raw) => {
            let text = raw.to_str().map_err(|_| {
                Error::Invalid("x-ambry-ttl-ms must be an integer (epoch ms)".into())
            })?;
            text.trim()
                .parse::<i64>()
                .map(Some)
                .map_err(|_| Error::Invalid("x-ambry-ttl-ms must be an integer (epoch ms)".into()))
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

/// 进程内唯一的暂存文件名后缀(纳秒 + 自增序号;不为它引 `rand`)。
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos}-{seq}")
}

/// 暂存文件(系统临时目录):**无论成功失败**都在 drop 时删掉。
///
/// 上传(SDK 要完整字节的 `put` → 用流式的 `put_file`)与下载大对象(用 `get_file`)都借它落地。
struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn create() -> Self {
        TempFile {
            path: std::env::temp_dir().join(format!(
                "antfs-frontend-{}-{}.tmp",
                std::process::id(),
                unique_suffix()
            )),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "failed to remove staging file"
            ),
        }
    }
}

/// 把请求体**流式**落到暂存文件(内存 = 单块),返回实际字节数。
///
/// - 有 `Content-Length`:超过 `max_body_bytes` → 413(调用方已在读 body 前挡过一道,这里兜住超发);
/// - 无 `Content-Length`(chunked):边收边计数,超限立即停手 → 413;
/// - 中途断流/与声明长度不符 → 400(该次上传不产生任何可见对象)。
async fn stage_body(
    body: Body,
    path: &Path,
    declared: Option<u64>,
    max_body_bytes: u64,
) -> Result<u64, Error> {
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::File::create(path).await?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Error::Invalid(format!("body stream: {e}")))?;
        written += chunk.len() as u64;
        if written > max_body_bytes {
            return Err(Error::TooLarge {
                len: written,
                max: max_body_bytes,
            });
        }
        if let Some(declared) = declared {
            if written > declared {
                return Err(Error::Invalid("body longer than Content-Length".into()));
            }
        }
        file.write_all(&chunk).await?;
    }
    // 只 flush 到 OS(不 fsync):暂存文件只是**传输中转**,不是持久化边界 —— 数据真正被 ack 是在
    // router 把记录写进 store 日志之后(其持久化边界仍是日志的周期 flush)。对中转文件做 fsync
    // 不改变任何持久性语义,却把每次上传变成一次同步落盘(macOS 上实测 PUT p50 从 ~1ms 涨到 ~250ms)。
    file.flush().await?;
    if let Some(declared) = declared {
        if written != declared {
            return Err(Error::Invalid(format!(
                "body shorter than Content-Length ({written} < {declared})"
            )));
        }
    }
    Ok(written)
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
        assert!(buf.starts_with(DESCRIPTOR_MAGIC));
        assert_eq!(buf[4], DESCRIPTOR_VERSION, "版本位必须在头部第二字节");
        let back = decode_descriptor(&buf).expect("decode");
        assert_eq!(back.len(), 3);
        for (got, want) in back.iter().zip(chunks.iter()) {
            assert_eq!(got.id, want.id, "片序必须原样保留");
            assert_eq!(got.size, want.size);
            assert_eq!(got.expiration_ms, want.expiration_ms);
        }
        // 头长 + session + 每条目定长:描述符长度可精确算出。
        assert_eq!(
            buf.len(),
            DESCRIPTOR_HEADER_BYTES + "session-abc".len() + 3 * DESCRIPTOR_ENTRY_BYTES
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
            decode_descriptor(&buf[..buf.len() - 1]).is_err(),
            "截断必须拒"
        );
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(decode_descriptor(&trailing).is_err(), "多余字节必须拒");
        let mut bad_version = buf.clone();
        bad_version[4] = DESCRIPTOR_VERSION + 1;
        assert!(decode_descriptor(&bad_version).is_err(), "未知版本必须拒");
        assert!(
            decode_descriptor(b"nope-nope-nope").is_err(),
            "无 MAGIC 必须拒"
        );
        assert!(
            decode_descriptor(DESCRIPTOR_MAGIC).is_err(),
            "只有 MAGIC 必须拒"
        );
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
        // 号被复用 / 本地 IO:不是调用方能改的,归 500。
        assert_eq!(
            router_status(&E::AlreadyExists { addr: "a:1".into() }),
            StatusCode::INTERNAL_SERVER_ERROR
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
}
