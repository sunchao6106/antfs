//! `AdminMetrics`:admin 服务打点(接 common/metric 的 `Registry`)。

use antfs_metric::{counter, IntCounter, Registry, Result};

/// admin 服务指标集,构造时注册进传入 `Registry`。
#[derive(Clone)]
pub struct AdminMetrics {
    pub api_requests: IntCounter,
    pub ops: IntCounter,
    pub master_rpc_errors: IntCounter,
    pub store_scrape_errors: IntCounter,
}

impl AdminMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(AdminMetrics {
            api_requests: counter(registry, "admin_api_requests_total", "admin API requests served")?,
            ops: counter(registry, "admin_ops_total", "admin ops (create-partitions/decommission)")?,
            master_rpc_errors: counter(registry, "admin_master_rpc_errors_total", "failed master gRPC calls")?,
            store_scrape_errors: counter(registry, "admin_store_scrape_errors_total", "failed scrapes of store /metrics endpoints")?,
        })
    }
}
