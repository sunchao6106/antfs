//! admin → store 数据面的 HTTP 客户端(`/api/metrics` 抓 `/metrics` 与 compact 转发共用同一套 client/超时)。

use std::time::Duration;

use axum::http::StatusCode;

/// 抓单个 store `/metrics` 端点的超时(挂死的节点不能拖慢整个 `/api/metrics`)。
///
/// `/metrics` 是内存 gather,正常在毫秒级;1s 足够,且让一个失联节点最多把整轮采集拖慢 1s
/// (抓取是并发的,总耗时 ≈ 最慢的那个节点)。
pub const SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

/// 转发手动 compact 的超时。store 侧是全库 compact,可能长于该值——此时 admin 报超时,
/// store 侧继续完成(端点幂等,可重试)。
pub const COMPACT_TIMEOUT: Duration = Duration::from_secs(2);

/// 转发 `/admin/request-control` 的超时。store 侧只读写内存里的禁用集,应即时返回;
/// 超时即认为该节点不可用(与 compact 不同,这里没有"后台仍在做"的语义)。
pub const ADMIN_OP_TIMEOUT: Duration = Duration::from_secs(2);

/// admin 侧发往 store 的请求共用这一种 client 类型。body 用 [`Full`](http_body_util::Full)
/// 以同时承载空体(`POST .../compact`)与 JSON 体(`POST /admin/request-control`)。
pub type HttpClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    http_body_util::Full<axum::body::Bytes>,
>;

pub fn new_client() -> HttpClient {
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build_http()
}

/// 空请求体(hyper 见 `is_end_stream` 即视为无 body)。
fn empty_body() -> http_body_util::Full<axum::body::Bytes> {
    http_body_util::Full::new(axum::body::Bytes::new())
}

/// `GET url` 取文本响应体;超时 / 连接失败 / 非 2xx / 非法 UTF-8 → `None`。
pub async fn get_text(client: &HttpClient, url: &str) -> Option<String> {
    let fetch = async {
        let req = axum::http::Request::get(url).body(empty_body()).ok()?;
        let resp = client.request(req).await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .ok()?
            .to_bytes();
        String::from_utf8(bytes.to_vec()).ok()
    };
    tokio::time::timeout(SCRAPE_TIMEOUT, fetch)
        .await
        .ok()
        .flatten()
}

/// `POST url`(空 body)→ `(状态码, 去空白的响应文本)`;超时 / 连接失败 → `Err(人读原因)`。
pub async fn post_empty(
    client: &HttpClient,
    url: &str,
) -> std::result::Result<(StatusCode, String), String> {
    send(client, "POST", url, empty_body(), COMPACT_TIMEOUT).await
}

/// `POST url`(JSON body,须自带 `Content-Type: application/json`)→ `(状态码, 去空白的响应文本)`。
/// 响应状态**原样透出**(store 的 400/404 要能传回调用方),只有超时/连接失败才是 `Err`。
pub async fn post_json(
    client: &HttpClient,
    url: &str,
    body: String,
) -> std::result::Result<(StatusCode, String), String> {
    send(
        client,
        "POST",
        url,
        http_body_util::Full::new(axum::body::Bytes::from(body)),
        ADMIN_OP_TIMEOUT,
    )
    .await
}

/// `GET url` → `(状态码, 去空白的响应文本)`;超时 / 连接失败 → `Err(人读原因)`。
pub async fn get_status(
    client: &HttpClient,
    url: &str,
) -> std::result::Result<(StatusCode, String), String> {
    send(client, "GET", url, empty_body(), ADMIN_OP_TIMEOUT).await
}

/// 发一个请求并读回文本;超时 / 连接失败 / 读体失败 → `Err`(文本里带原因,透给操作者)。
async fn send(
    client: &HttpClient,
    method: &str,
    url: &str,
    body: http_body_util::Full<axum::body::Bytes>,
    timeout: Duration,
) -> std::result::Result<(StatusCode, String), String> {
    let send = async {
        let req = axum::http::Request::builder()
            .method(method)
            .uri(url)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(|e| format!("bad request: {e}"))?;
        let resp = client
            .request(req)
            .await
            .map_err(|e| format!("network error: {e}"))?;
        let status = resp.status();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .map_err(|e| format!("read body: {e}"))?
            .to_bytes();
        Ok::<_, String>((status, String::from_utf8_lossy(&bytes).trim().to_string()))
    };
    match tokio::time::timeout(timeout, send).await {
        Ok(r) => r,
        Err(_) => Err(format!("timeout after {}s", timeout.as_secs())),
    }
}

/// store `POST /admin/partitions/{pid}/compact` 的响应体(同步路径)与
/// `GET /admin/compactions` 里 `done` 任务的 `result` 字段(同一口径)。
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct CompactStats {
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub segments_before: usize,
    pub segments_after: usize,
}

/// store `POST /admin/partitions/{pid}/compact?async=1` 的响应体:提交即回,任务在 store 后台跑。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CompactionSubmitted {
    pub job_id: u64,
}

/// store `GET /admin/compactions` 的响应体(store 台账最多保留最近 32 条,最新在前)。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CompactionsResponse {
    pub jobs: Vec<CompactionJob>,
}

/// store 侧一条 compaction 任务(与 `crates/store/src/compaction_jobs.rs::CompactionJob` 同契约:
/// 未完成字段**不出现在 JSON 里**,故这里 `Option` + `skip_serializing_if` 原样保持)。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionJob {
    pub job_id: u64,
    pub partition_id: u32,
    /// `running` / `done` / `failed`。
    pub state: String,
    pub started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<CompactStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
