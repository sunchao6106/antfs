//! `StoreServerMetrics`:数据节点打点(接 common/metric 的 `Registry`)。

use antfs_metric::{counter, gauge, IntCounter, IntGauge, Registry, Result};

/// 数据节点指标集,构造时注册进传入 `Registry`。
#[derive(Clone)]
pub struct StoreServerMetrics {
    pub put_total: IntCounter,
    pub get_total: IntCounter,
    pub put_bytes: IntCounter,
    pub get_bytes: IntCounter,
    pub replicas: IntGauge,
    pub errors: IntCounter,
    pub heartbeats: IntCounter,
    pub mutations: IntCounter,
    pub replication_rounds: IntCounter,
    pub replication_applied: IntCounter,
    pub replication_bytes: IntCounter,
    pub replication_errors: IntCounter,
    pub bootstrap_segments: IntCounter,
    pub bootstrap_bytes: IntCounter,
    pub flushes: IntCounter,
    pub compactions: IntCounter,
    pub hard_deleted: IntCounter,
}

impl StoreServerMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(StoreServerMetrics {
            put_total: counter(registry, "store_server_put_total", "blob put requests")?,
            get_total: counter(registry, "store_server_get_total", "blob get requests")?,
            put_bytes: counter(registry, "store_server_put_bytes_total", "bytes written")?,
            get_bytes: counter(registry, "store_server_get_bytes_total", "bytes read")?,
            replicas: gauge(registry, "store_server_replicas", "local partition replicas")?,
            errors: counter(registry, "store_server_errors_total", "data-plane errors")?,
            heartbeats: counter(registry, "store_server_heartbeats_total", "heartbeat directives received")?,
            mutations: counter(registry, "store_server_mutations_total", "delete/undelete/ttl mutations")?,
            replication_rounds: counter(registry, "store_server_replication_rounds_total", "pull-replication rounds run")?,
            replication_applied: counter(registry, "store_server_replication_applied_total", "records applied from peers")?,
            replication_bytes: counter(registry, "store_server_replication_bytes_total", "bytes applied from peers")?,
            replication_errors: counter(registry, "store_server_replication_errors_total", "pull-replication errors")?,
            bootstrap_segments: counter(registry, "store_server_bootstrap_segments_total", "sealed segments physically copied on bootstrap")?,
            bootstrap_bytes: counter(registry, "store_server_bootstrap_bytes_total", "bytes physically copied on bootstrap")?,
            flushes: counter(registry, "store_server_flushes_total", "local-worker flushes run")?,
            compactions: counter(registry, "store_server_compactions_total", "local-worker segment compactions run")?,
            hard_deleted: counter(registry, "store_server_hard_deleted_total", "records hard-deleted (data body erased) by local worker")?,
        })
    }
}
