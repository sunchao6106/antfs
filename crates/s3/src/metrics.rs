use antfs_metric::{
    counter, counter_vec, gauge, latency_seconds, latency_seconds_vec, HistogramVec, IntCounterVec,
    Registry,
};

pub struct S3Metrics {
    pub requests_total: IntCounterVec,
    pub quota_rejected_total: IntCounterVec,
    pub request_duration_seconds: HistogramVec,
    pub metastore_queries_total: IntCounterVec,
    pub metastore_errors_total: IntCounterVec,
    pub gc_runs_total: antfs_metric::IntCounter,
    pub gc_purged_rows_total: antfs_metric::IntCounter,
    pub gc_orphan_uploads_total: antfs_metric::IntCounter,
    pub gc_orphan_parts_blobs_total: antfs_metric::IntCounter,
    pub gc_errors_total: IntCounterVec,
    pub gc_duration_seconds: antfs_metric::Histogram,
    pub gc_pending_resolved_total: antfs_metric::IntCounter,
    pub gc_pending_backlog: antfs_metric::IntGauge,
    pub chunked_requests_total: antfs_metric::IntCounter,
    pub chunked_rejected_total: IntCounterVec,
    pub meta_cache_lookups_total: IntCounterVec,
    /// 大对象 PutObject 流式内部 multipart(2026-10-22):对象粒度与片粒度各一。
    pub put_objects_streamed_total: antfs_metric::IntCounter,
    pub put_object_streamed_parts_total: antfs_metric::IntCounter,
}

impl S3Metrics {
    pub fn new(registry: &Registry) -> std::result::Result<Self, antfs_metric::Error> {
        Ok(Self {
            requests_total: counter_vec(
                registry,
                "s3_requests_total",
                "S3 API requests",
                &["op", "status"],
            )?,
            quota_rejected_total: counter_vec(
                registry,
                "s3_quota_rejected_total",
                "Write requests rejected by bucket storage quota (HTTP 507)",
                &["bucket"],
            )?,
            request_duration_seconds: latency_seconds_vec(
                registry,
                "s3_request_duration_seconds",
                "S3 API request latency",
                &["op"],
            )?,
            metastore_queries_total: counter_vec(
                registry,
                "s3_metastore_queries_total",
                "MetaStore query count",
                &["op"],
            )?,
            metastore_errors_total: counter_vec(
                registry,
                "s3_metastore_errors_total",
                "MetaStore error count",
                &["op"],
            )?,
            gc_runs_total: counter(
                registry,
                "s3_gc_runs_total",
                "S3 gateway GC cycles completed",
            )?,
            gc_purged_rows_total: counter(
                registry,
                "s3_gc_purged_rows_total",
                "Shadow-marked rows purged by GC",
            )?,
            gc_orphan_uploads_total: counter(
                registry,
                "s3_gc_orphan_uploads_total",
                "Expired multipart sessions reclaimed by GC",
            )?,
            gc_orphan_parts_blobs_total: counter(
                registry,
                "s3_gc_orphan_parts_blobs_total",
                "Part blobs reclaimed from expired multipart sessions",
            )?,
            gc_errors_total: counter_vec(
                registry,
                "s3_gc_errors_total",
                "GC item-level failures (loop continues)",
                &["kind"],
            )?,
            gc_pending_resolved_total: counter(
                registry,
                "s3_gc_pending_resolved_total",
                "Blobs reclaimed through the pending-reclaims WAL by the GC loop                  (crash-window or failed-tombstone residue)",
            )?,
            gc_pending_backlog: gauge(
                registry,
                "s3_gc_pending_backlog",
                "Rows remaining in the pending-reclaims WAL at cycle end (0 in                  steady state; sustained >0 means tombstones keep failing)",
            )?,
            gc_duration_seconds: latency_seconds(
                registry,
                "s3_gc_duration_seconds",
                "S3 gateway GC cycle duration",
            )?,
            chunked_requests_total: counter(
                registry,
                "s3_chunked_requests_total",
                "Requests carrying an aws-chunked streaming body that entered decoding",
            )?,
            chunked_rejected_total: counter_vec(
                registry,
                "s3_chunked_rejected_total",
                "aws-chunked bodies rejected at declaration or decode (framing/signature/length/oversize)",
                &["reason"],
            )?,
            meta_cache_lookups_total: counter_vec(
                registry,
                "s3_meta_cache_lookups_total",
                "Per-request credential/bucket metastore lookups served by the s3-meta-cache layer",
                &["kind", "result"],
            )?,
            put_objects_streamed_total: counter(
                registry,
                "s3_put_objects_streamed_total",
                "PutObjects larger than max_body_bytes streamed through the internal multipart machinery",
            )?,
            put_object_streamed_parts_total: counter(
                registry,
                "s3_put_object_streamed_parts_total",
                "Internal parts written for streamed PutObjects",
            )?,
        })
    }
}
