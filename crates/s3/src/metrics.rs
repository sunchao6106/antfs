use antfs_metric::{counter_vec, latency_seconds_vec, HistogramVec, IntCounterVec, Registry};

pub struct S3Metrics {
    pub requests_total: IntCounterVec,
    pub request_duration_seconds: HistogramVec,
    pub metastore_queries_total: IntCounterVec,
    pub metastore_errors_total: IntCounterVec,
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
        })
    }
}
