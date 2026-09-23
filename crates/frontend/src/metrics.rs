//! `FrontendMetrics`:HTTP 接口层的打点(接 common/metric 的 `Registry`)。
//!
//! 名字与 `docs/design/frontend.md`「监控项清单」严格一致(CLAUDE.md §7:改代码即改文档)。
//!
//! **与 SDK 的 `RouterMetrics` 分层**:这里只记「HTTP 视角」的请求数 / 字节数 / 延迟 / 在途数;
//! 写 W、读 R、单副本失败明细在 router 指标(`router_*`)里 —— frontend 只看得到 SDK 聚合后的结果,
//! 故 `frontend_replica_errors_total` 的 `kind` 取 [`antfs_router::Error::kind`](SDK 的稳定分类)。

use antfs_metric::{
    counter, counter_vec, gauge_vec, latency_seconds_vec, HistogramVec, IntCounter, IntCounterVec,
    IntGaugeVec, Registry, Result,
};

/// op 口径(延迟/在途/变更类指标的 label 真值):一个 HTTP 端点 = 一个 op。
pub const OP_UPLOAD: &str = "upload";
/// `GET /blobs/{blobId}`。
pub const OP_DOWNLOAD: &str = "download";
/// `HEAD /blobs/{blobId}`。
pub const OP_HEAD: &str = "head";
/// `DELETE /blobs/{blobId}`。
pub const OP_DELETE: &str = "delete";
/// `POST /blobs/{blobId}/undelete`。
pub const OP_UNDELETE: &str = "undelete";
/// `POST /blobs/{blobId}/ttl`。
pub const OP_TTL: &str = "ttl";
/// `POST /stitch`。
pub const OP_STITCH: &str = "stitch";

/// 成功口径(所有 `*_total{outcome}` 共用;其余取值是错误分类,见 `Error::outcome`)。
pub const OUTCOME_OK: &str = "ok";

/// frontend 指标集,构造时注册进传入 `Registry`。
#[derive(Clone)]
pub struct FrontendMetrics {
    /// 上传请求数(`POST /blobs`),label `outcome`。
    pub upload_total: IntCounterVec,
    /// 上传写入的字节数(counter,只计成功)。
    pub upload_bytes: IntCounter,
    /// 下载请求数(`GET /blobs/{blobId}`),label `outcome`。
    pub download_total: IntCounterVec,
    /// 下载返回的字节数(counter,只计成功;拼装对象按描述符里各片长度求和)。
    pub download_bytes: IntCounter,
    /// 删除请求数(`DELETE /blobs/{blobId}`),label `outcome`。
    pub delete_total: IntCounterVec,
    /// 其余变更请求数,label `op`(head/undelete/ttl/stitch)+ `outcome`。
    pub mutation_total: IntCounterVec,
    /// 请求延迟(秒),label `op`;覆盖含边界校验早退在内的所有出口。
    pub request_duration: HistogramVec,
    /// 在途请求数,label `op`。
    pub in_flight: IntGaugeVec,
    /// router 调用失败数,label `kind`(`antfs_router::Error::kind()` 的稳定分类)。
    pub replica_errors: IntCounterVec,
}

impl FrontendMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(FrontendMetrics {
            upload_total: counter_vec(
                registry,
                "frontend_upload_total",
                "blob upload requests by outcome",
                &["outcome"],
            )?,
            upload_bytes: counter(
                registry,
                "frontend_upload_bytes_total",
                "bytes uploaded (successful uploads)",
            )?,
            download_total: counter_vec(
                registry,
                "frontend_download_total",
                "blob download requests by outcome",
                &["outcome"],
            )?,
            download_bytes: counter(
                registry,
                "frontend_download_bytes_total",
                "bytes downloaded (successful downloads)",
            )?,
            delete_total: counter_vec(
                registry,
                "frontend_delete_total",
                "blob delete requests by outcome",
                &["outcome"],
            )?,
            mutation_total: counter_vec(
                registry,
                "frontend_mutation_total",
                "head/undelete/ttl/stitch requests by op and outcome",
                &["op", "outcome"],
            )?,
            request_duration: latency_seconds_vec(
                registry,
                "frontend_request_duration_seconds",
                "HTTP request latency (seconds) by op",
                &["op"],
            )?,
            in_flight: gauge_vec(
                registry,
                "frontend_in_flight_requests",
                "in-flight HTTP requests by op",
                &["op"],
            )?,
            replica_errors: counter_vec(
                registry,
                "frontend_replica_errors_total",
                "failed router (SDK) calls by error kind",
                &["kind"],
            )?,
        })
    }
}
