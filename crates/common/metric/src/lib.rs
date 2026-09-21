//! common/metric:项目统一的**统计打点**。
//!
//! 设计对齐 Ambry 语义、实现用 Rust 原生方案([`prometheus`](https://docs.rs/prometheus)):
//!
//! - Ambry 用 Dropwizard `MetricRegistry`,每个模块一个 `*Metrics` 类,构造时把 `Counter` /
//!   `Histogram` / `Timer` 等 handle 注册进 registry 并存成字段。
//! - 这里保留**同一形状**:一个 [`Registry`],每个模块定义一个 `*Metrics` struct,构造时用本模块的
//!   [`counter`] / [`gauge`] / [`histogram`] / [`latency_seconds`] 把 typed handle 注册进 registry 存成字段。
//! - 差异(现代做法):Dropwizard 客户端算 `Meter` 的 EWMA 速率、`Histogram` 的 reservoir 分位;
//!   Prometheus 改为**服务端**从计数/桶用 `rate()` / `histogram_quantile()` 算,客户端只累加,更省更准。
//!
//! ```
//! use metric::{counter, latency_seconds, gather, Registry, IntCounter, Histogram};
//!
//! struct StoreMetrics {
//!     get_response: Histogram,
//!     store_start_failure: IntCounter,
//! }
//! impl StoreMetrics {
//!     fn new(registry: &Registry) -> metric::Result<Self> {
//!         Ok(Self {
//!             get_response: latency_seconds(registry, "store_get_response_seconds", "get latency")?,
//!             store_start_failure: counter(registry, "store_start_failure_total", "start failures")?,
//!         })
//!     }
//! }
//!
//! let registry = Registry::new();
//! let m = StoreMetrics::new(&registry).unwrap();
//! m.store_start_failure.inc();
//! { let _t = m.get_response.start_timer(); }
//! assert!(gather(&registry).unwrap().contains("store_start_failure_total"));
//! ```

pub use prometheus::{
    self, Error, Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry,
};

use prometheus::{Encoder, TextEncoder};

pub type Result<T> = prometheus::Result<T>;

/// 单调计数器(次数、字节数等只增量)。对齐 Dropwizard `Counter`,导出为 Prometheus counter。
pub fn counter(registry: &Registry, name: &str, help: &str) -> Result<IntCounter> {
    let c = IntCounter::with_opts(Opts::new(name, help))?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

/// 可增可减的瞬时量(队列深度、内存占用等)。对齐 Dropwizard `Gauge`。
pub fn gauge(registry: &Registry, name: &str, help: &str) -> Result<IntGauge> {
    let g = IntGauge::with_opts(Opts::new(name, help))?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

/// 自定义桶的直方图(分布统计)。对齐 Dropwizard `Histogram`。
pub fn histogram(registry: &Registry, name: &str, help: &str, buckets: Vec<f64>) -> Result<Histogram> {
    let h = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))?;
    registry.register(Box::new(h.clone()))?;
    Ok(h)
}

/// 延迟直方图(秒,用 Prometheus 默认桶)。对齐 Dropwizard `Timer`:配合 [`Histogram::start_timer`]
/// 返回的 [`HistogramTimer`] 在 drop 时记录耗时。
pub fn latency_seconds(registry: &Registry, name: &str, help: &str) -> Result<Histogram> {
    let h = Histogram::with_opts(HistogramOpts::new(name, help))?;
    registry.register(Box::new(h.clone()))?;
    Ok(h)
}

/// 带 label 的计数器(多实例共享 Registry 用,见设计文档 common.md §2.2)。每个 Registry 只注册一次,
/// 各实例用 [`IntCounterVec::with_label_values`] 取子 handle。
pub fn counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntCounterVec> {
    let v = IntCounterVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

/// 带 label 的 gauge。
pub fn gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
    let v = IntGaugeVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

/// 带 label 的自定义桶直方图。
pub fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> Result<HistogramVec> {
    let v = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

/// 带 label 的延迟直方图(秒,默认桶)。
pub fn latency_seconds_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<HistogramVec> {
    let v = HistogramVec::new(HistogramOpts::new(name, help), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

/// 把 registry 的当前指标编码成 Prometheus 文本导出格式(供 `/metrics` 端点或调试)。
pub fn gather(registry: &Registry) -> Result<String> {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut buf)?;
    String::from_utf8(buf).map_err(|e| Error::Msg(e.to_string()))
}

/// 拼指标名:`subsystem_name`。Prometheus 名不允许 `.`(不同于 Ambry 的 `MetricRegistry.name` 用 `.`),用 `_` 连接。
pub fn metric_name(subsystem: &str, name: &str) -> String {
    format!("{subsystem}_{name}")
}
