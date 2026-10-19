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
//!
//! **服务端自动分片**(`POST /blobs` body > `max_body_bytes` 且开启 `auto_chunk_enabled`):先
//! 暂存整包校验,再按 `auto_chunk_bytes` 切片逐片入库存,最后写拼装描述符并返回其 blobId ——
//! `GET`/`HEAD` 走既有的描述符透明拼装路径,客户端无感知(见 [`upload`])。

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

/// **直接进内存**的 body 上界:≤ 该值走内存路径(`Bytes` → `Router::put`),> 该值(或未声明长度)
/// 才落盘暂存 + [`Router::put_file`] 流式写。
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

// ── 拼装描述符格式(无状态拼装的落盘契约;格式演进见 CLAUDE.md §3.4)────────────────────────────
// 格式单点在 `antfs-descriptor`;此处按本层错误口径做薄壳映射。

/// 单个拼装对象的片数上限。
const MAX_CHUNKS: usize = antfs_descriptor::MAX_CHUNKS;
/// 描述符字节上限(1 MiB):`GET` 据此决定"整体读出判定"还是"直接流式"(CLAUDE.md §5.3);
/// 片数与会话长度各自设限后,正常永远到不了这个上界(4096 片最多约 160 KiB)。
const DESCRIPTOR_MAX_BYTES: u64 = antfs_descriptor::MAX_BYTES;

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
    /// 单请求 body 上限(字节);开启自动分片后,"单 blob 路径"仍以它为界,超过者转分片。
    pub max_body_bytes: u64,
    /// 服务端自动分片开关(body > `max_body_bytes` 时切片;关则 413)。
    pub auto_chunk_enabled: bool,
    /// 自动分片单片大小(字节;配置校验保证 1..=1 GiB)。
    pub auto_chunk_bytes: u64,
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
            auto_chunk_enabled: cfg.auto_chunk_enabled,
            auto_chunk_bytes: cfg.auto_chunk_bytes,
            cors_allowed_origins: cfg.cors_allowed_origins.clone(),
        })
    }
}

/// 自动分片路径收下的总量上限 = 片数上限(`MAX_CHUNKS`)× 单片大小(更大者描述符装不下;
/// `Content-Length` 预检与暂存计数两处各拦一道)。配置已把单片限在 ≤1 GiB,乘积无 u64 溢出(≤4 TiB)。
fn auto_chunk_total_limit(auto_chunk_bytes: u64) -> u64 {
    MAX_CHUNKS as u64 * auto_chunk_bytes
}

/// 建路由:`/blobs*` 与 `/stitch` 是对象接口,`/metrics` 是 Prometheus 抓取口(无鉴权,抓取惯例),
/// `/healthCheck` 是进程探活(无鉴权,恒 200,见 [`health_check`])。
///
/// body 上限**分路由挂**:`/blobs` 开自动分片后上限放大到分片总量上限(裸字节体由 handler
/// 边收边计数,`DefaultBodyLimit` 只是流式读的兜底);`/stitch` 的 JSON 体重持 `max_body_bytes`
/// (无分片路径,放大它只是让恶意请求能白写更多信息)。
pub fn build_router(state: AppState) -> Router {
    let blobs_limit = if state.auto_chunk_enabled {
        auto_chunk_total_limit(state.auto_chunk_bytes)
    } else {
        state.max_body_bytes
    };
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
/// body 超过 `max_body_bytes` 且开启自动分片时,切片+描述符路径见 [`auto_chunk_store`]。
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

/// 上传实现:边界校验 → body ≤ `max_body_bytes` 时走单 blob 路径(小进内存 / 大落暂存流式,行为
/// 与自动分片之前完全一致);超过(且开启自动分片)→ 暂存整包后切片多次上传(见 [`auto_chunk_store`])
/// → 写入时定 TTL(`x-antfs-ttl-ms`,缺省不过期)。
async fn upload(st: &AppState, req: Request) -> Result<Uploaded, Error> {
    // 先解析 TTL 头:非法立即 400,不白读一遍 body。
    let expiration_ms = ttl_header(req.headers())?.unwrap_or(NO_EXPIRATION);
    // session 只有走分片路径才会写进描述符;提前取值(之后 req 被 into_body 消费)。
    let session = req.headers().get(HEADER_SESSION).cloned();
    let declared = content_length(&req);
    // 该路径收得下的总量:关自动分片 = `max_body_bytes`(超 → 413,旧契约);开 = 分片总量上限。
    let total_limit = if st.auto_chunk_enabled {
        auto_chunk_total_limit(st.auto_chunk_bytes)
    } else {
        st.max_body_bytes
    };
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
        _ => {
            // 大 body(或未声明长度):先**整体**落暂存并校验长度,再决定单 blob 写或分片。
            // 先暂存后切片是刻意取舍(与单 blob 大对象同一哲学):① 客户端中途断流/谎报长度时,
            // 在写任何一片进集群**之前**就失败,不产生孤儿;② 暂存复用既有的边收边计路径。
            let staging = TempFile::create();
            let written =
                stage_body(req.into_body(), staging.path(), declared, total_limit).await?;
            if written <= st.max_body_bytes {
                let (key, _report) = st
                    .router
                    .put_file_with_ttl(staging.path(), expiration_ms)
                    .await?;
                Uploaded {
                    key,
                    len: written,
                    auto_chunks: None,
                }
            } else {
                // written > max_body_bytes:自动分片开启才可能到这(关闭时暂存上限即 max,已 413)。
                let session = session_of(session)?;
                let done = match auto_chunk_store(
                    st,
                    staging.path(),
                    written,
                    expiration_ms,
                    session.as_deref(),
                )
                .await
                {
                    Ok(done) => done,
                    Err(e) => {
                        // 成功侧在 handler 记;失败在此记,两处合成该 counter 的全集。
                        fail(&st.metrics, &st.metrics.upload_auto_chunk_total, &[], &e);
                        return Err(e);
                    }
                };
                done
            }
        }
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

/// 把已暂存的大对象(> `max_body_bytes`)按 `auto_chunk_bytes` 切片**逐片**入库,最后写拼装描述符
/// (同 [`stitch_inner`] 的描述符,`GET`/`HEAD` 用既有的透明拼装路径读回),返回描述符的 key 与总量。
///
/// 持久性口径(CLAUDE.md §3.3):
/// - 每片与描述符都是**一次多数派写**(`put_auto_with_ttl`,内部发号 + 507 换分区重试);
/// - 中途失败(某片或描述符没写进去)→ 对**已 ack** 的片逐个写墓碑尽力回收(见 [`cleanup_acked_chunks`]),
///   回报调用方原错误;调用方重试安全 —— 重试拿到一套全新 blobId,与本轮不相扰,不产生可见的重复对象;
/// - 内存 = 单片(一个驻留的读写缓冲,大小 = `auto_chunk_bytes`,不随对象大小涨)。
async fn auto_chunk_store(
    st: &AppState,
    staged: &Path,
    total: u64,
    expiration_ms: i64,
    session: Option<&str>,
) -> Result<Uploaded, Error> {
    let chunk_count = match u32::try_from(total.div_ceil(st.auto_chunk_bytes)) {
        Ok(n) if n as usize <= MAX_CHUNKS => n,
        // 暂存上限已保证 ≤ MAX_CHUNKS;真到这(将来改上限口径)也走 413,不静默写装不下的描述符。
        _ => {
            return Err(Error::TooLarge {
                len: total,
                max: auto_chunk_total_limit(st.auto_chunk_bytes),
            })
        }
    };
    // 复读暂存:文件按构造恰好有 total 字节(stage_body 校验过长度),读不满/读超 = 本地 IO 错误。
    let mut file = tokio::fs::File::open(staged).await?;
    // 单片读写缓冲:配置校验保证 auto_chunk_bytes ∈ [1, 1 GiB](任何测试/生产平台可分配)。
    let chunk_usize = usize::try_from(st.auto_chunk_bytes)
        .expect("auto_chunk_bytes is validated to at most 1 GiB");
    let mut buf = vec![0u8; chunk_usize];
    let mut chunks: Vec<DescriptorChunk> = Vec::with_capacity(chunk_count as usize);
    let mut acked: Vec<StoreKey> = Vec::with_capacity(chunk_count as usize);
    let mut remaining = total;
    while remaining > 0 {
        let want = remaining.min(st.auto_chunk_bytes);
        let want_usize = usize::try_from(want).expect("want ≤ auto_chunk_bytes ≤ 1 GiB");
        file.read_exact(&mut buf[..want_usize]).await?;
        let (key, _report) = match st
            .router
            .put_auto_with_ttl(Bytes::copy_from_slice(&buf[..want_usize]), expiration_ms)
            .await
        {
            Ok(ok) => ok,
            Err(e) => {
                let e = Error::Router(e);
                cleanup_acked_chunks(st, &acked).await;
                return Err(e);
            }
        };
        chunks.push(DescriptorChunk {
            id: key,
            size: want,
            expiration_ms,
        });
        acked.push(key);
        remaining -= want;
    }
    // 描述符与片同 TTL:整个逻辑对象一起过期(与单 blob 的 TTL 读行为对称)。
    let result = async {
        let descriptor = encode_descriptor(session.unwrap_or("").as_bytes(), &chunks)?;
        let (key, _report) = st
            .router
            .put_auto_composed_with_ttl(Bytes::from(descriptor), expiration_ms)
            .await?;
        Ok::<StoreKey, Error>(key)
    }
    .await;
    match result {
        Ok(key) => Ok(Uploaded {
            key,
            len: total,
            auto_chunks: Some(chunk_count),
        }),
        Err(e) => {
            cleanup_acked_chunks(st, &acked).await;
            Err(e)
        }
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
/// - **类型只看 `composed` 位**(descriptor-drop-magic 后唯一判定):置位 = 拼装描述符
///   → 整体读回解析拼装(超限/坏描述符响亮 502,信任位不降级);无位 = 普通对象原字节;
/// - **小对象**整体读回;**大对象不整体进内存**(CLAUDE.md §5.3):`get_file` 落暂存文件后
///   流式回给调用方。SDK 只有 `get`(整体进内存)与 `get_file`(落文件)两个读口,
///   后者才不把对象整个缓进内存。
async fn download(st: &AppState, blob_id: &str) -> Result<(Response, u64), Error> {
    let key = parse_blob_id(blob_id)?;
    // 先取元数据:① 拿 size 决定读路径;② 顺便让"对象不存在"给出干净的 404(而非读一半才发现)。
    let meta = st.router.stat(&key).await?;
    if meta.composed {
        if meta.size > DESCRIPTOR_MAX_BYTES {
            return Err(Error::Upstream(
                "composed blob exceeds the descriptor size bound".into(),
            ));
        }
        let body = st.router.get(&key).await?;
        return stitched_body(st, &body).await;
    }
    if meta.size <= DESCRIPTOR_MAX_BYTES {
        let body = st.router.get(&key).await?;
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

/// `HEAD /blobs/{blobId}`:200 + 无 body,状态经 `x-antfs-blob-*` 响应头透出(不读数据体)。
async fn head_blob(State(st): State<AppState>, AxumPath(blob_id): AxumPath<String>) -> Response {
    let _timer = RequestTimer::start(&st.metrics, OP_HEAD);
    let result = async {
        let key = parse_blob_id(&blob_id)?;
        let mut meta = st.router.stat(&key).await?;
        // 拼装对象:`stat` 只看到**描述符**那个 blob(几百字节),直接报出去会与 `GET`(拼装后的逻辑字节流)对不上。
        // 故对**置位行**做小对象读回,把 size 换成各片之和,并回 `x-antfs-blob-stitched` 头
        // (无位行不读数据体:位是唯一类型判定,普通对象没有"逻辑大小"一说)。
        // 代价可控:描述符按构造 ≤ DESCRIPTOR_MAX_BYTES,置位行才需要多读一次数据体(≤1 MiB)。
        // 已删/已过期的拼装对象读不出逻辑大小(数据面拒读已过期体,`get` 的失败被静默忽略)
        // —— 退回描述符自身的 stat 是已知取舍;过期的对象本就不再是"取内容"的语义。
        let mut stitched_chunks = None;
        if meta.composed && !meta.deleted && !meta.expired && meta.size <= DESCRIPTOR_MAX_BYTES {
            if let Ok(body) = st.router.get(&key).await {
                if let Ok(chunks) = decode_descriptor(&body) {
                    meta.size = chunks.iter().map(|c| c.size).sum();
                    stitched_chunks = Some(chunks.len());
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

/// 解码拼装描述符。任何结构性错误 → [`Error::Upstream`](502):
/// 描述符字节由组合写口落位(parse 只吃位字节),解析不了即上游损坏。
fn decode_descriptor(buf: &[u8]) -> Result<Vec<DescriptorChunk>, Error> {
    let refs = antfs_descriptor::parse(buf).map_err(|e| Error::Upstream(e.to_string()))?;
    let mut chunks = Vec::with_capacity(refs.len());
    for r in refs {
        let id = StoreKey::from_slice(&r.id)
            .ok_or_else(|| Error::Upstream("corrupt stitch descriptor: bad chunk id".into()))?;
        chunks.push(DescriptorChunk {
            id,
            size: r.size,
            expiration_ms: r.expiration_ms,
        });
    }
    Ok(chunks)
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
        assert_eq!(buf[0], antfs_descriptor::VERSION, "版本位在首字节");
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
            decode_descriptor(&buf[..buf.len() - 1]).is_err(),
            "截断必须拒"
        );
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(decode_descriptor(&trailing).is_err(), "多余字节必须拒");
        let mut bad_version = buf.clone();
        bad_version[0] = antfs_descriptor::VERSION + 1;
        assert!(decode_descriptor(&bad_version).is_err(), "未知版本必须拒");
        assert!(
            decode_descriptor(b"nope-nope-nope").is_err(),
            "坏版本必须拒"
        );
        assert!(decode_descriptor(&[]).is_err(), "空 body 必须拒");
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

    /// 自动分片的数学口径:片数向上取整;总量上限 = 片数上限 × 单片;两者都无溢出。
    #[test]
    fn auto_chunk_math() {
        let chunk = 4 * 1024 * 1024;
        assert_eq!(1u64.div_ceil(chunk), 1);
        assert_eq!(chunk.div_ceil(chunk), 1);
        assert_eq!((chunk + 1).div_ceil(chunk), 2, "恰整除不加片,过了界加一片");
        assert_eq!(
            auto_chunk_total_limit(chunk),
            MAX_CHUNKS as u64 * chunk,
            "默认片大小下总量上限 = 16 GiB(4096 × 4 MiB)"
        );
        // 配置上限内的单片,乘积必须在 u64 内(校验层防溢出的回归护栏)。
        assert!(auto_chunk_total_limit(crate::config::AUTO_CHUNK_MAX_ALLOWED_BYTES) < u64::MAX);
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
