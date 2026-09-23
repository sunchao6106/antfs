//! `AdminMetrics`:admin 服务打点(接 common/metric 的 `Registry`)。

use antfs_metric::{counter, IntCounter, Registry, Result};

/// admin 服务指标集,构造时注册进传入 `Registry`。
#[derive(Clone)]
pub struct AdminMetrics {
    pub api_requests: IntCounter,
    pub ops: IntCounter,
    pub master_rpc_errors: IntCounter,
    pub store_scrape_errors: IntCounter,
    /// scrub 轮次数(经 `POST /api/partitions/{id}/scrub` 触发)。
    pub scrub_runs: IntCounter,
    /// scrub 检出的差异总数(摘要级 + key 级 + 本地校验)。
    pub scrub_divergences: IntCounter,
}

impl AdminMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(AdminMetrics {
            api_requests: counter(
                registry,
                "admin_api_requests_total",
                "admin API requests served",
            )?,
            ops: counter(
                registry,
                "admin_ops_total",
                "admin ops (create-partitions/decommission)",
            )?,
            master_rpc_errors: counter(
                registry,
                "admin_master_rpc_errors_total",
                "failed master gRPC calls",
            )?,
            store_scrape_errors: counter(
                registry,
                "admin_store_scrape_errors_total",
                "failed scrapes of store /metrics endpoints",
            )?,
            scrub_runs: counter(
                registry,
                "admin_scrub_runs_total",
                "scrub runs triggered via admin",
            )?,
            scrub_divergences: counter(
                registry,
                "admin_scrub_divergences_total",
                "divergences found by scrub runs",
            )?,
        })
    }
}
