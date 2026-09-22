use antfs_metric::{counter, gather, gauge, histogram, latency_seconds, Histogram, IntCounter, Registry};

struct StoreMetrics {
    get_response: Histogram,
    put_response: Histogram,
    store_start_failure: IntCounter,
}

impl StoreMetrics {
    fn new(registry: &Registry) -> antfs_metric::Result<Self> {
        Ok(StoreMetrics {
            get_response: latency_seconds(registry, "store_get_response_seconds", "get latency")?,
            put_response: latency_seconds(registry, "store_put_response_seconds", "put latency")?,
            store_start_failure: counter(
                registry,
                "store_start_failure_total",
                "store start failures",
            )?,
        })
    }
}

#[test]
fn counter_increments_and_exports() {
    let registry = Registry::new();
    let c = counter(&registry, "ops_total", "op count").unwrap();
    c.inc();
    c.inc_by(4);
    assert_eq!(c.get(), 5);
    let text = gather(&registry).unwrap();
    assert!(text.contains("ops_total 5"));
}

#[test]
fn gauge_goes_up_and_down() {
    let registry = Registry::new();
    let g = gauge(&registry, "queue_depth", "pending items").unwrap();
    g.add(10);
    g.sub(3);
    assert_eq!(g.get(), 7);
}

#[test]
fn histogram_records_observations() {
    let registry = Registry::new();
    let h = histogram(&registry, "blob_size_bytes", "blob sizes", vec![10.0, 100.0, 1000.0]).unwrap();
    h.observe(5.0);
    h.observe(50.0);
    h.observe(500.0);
    assert_eq!(h.get_sample_count(), 3);
    assert_eq!(h.get_sample_sum(), 555.0);
}

#[test]
fn timer_guard_records_on_drop() {
    let registry = Registry::new();
    let h = latency_seconds(&registry, "op_latency_seconds", "op latency").unwrap();
    {
        let _timer = h.start_timer();
    }
    assert_eq!(h.get_sample_count(), 1);
}

#[test]
fn metrics_struct_pattern_registers_all_fields() {
    let registry = Registry::new();
    let m = StoreMetrics::new(&registry).unwrap();
    m.store_start_failure.inc();
    {
        let _t = m.get_response.start_timer();
    }
    m.put_response.observe(0.01);

    let text = gather(&registry).unwrap();
    assert!(text.contains("store_start_failure_total 1"));
    assert!(text.contains("store_get_response_seconds"));
    assert!(text.contains("store_put_response_seconds"));
}

#[test]
fn duplicate_registration_is_error() {
    let registry = Registry::new();
    counter(&registry, "dup_total", "first").unwrap();
    assert!(counter(&registry, "dup_total", "second").is_err());
}

#[test]
fn labeled_vec_distinguishes_instances_on_shared_registry() {
    let registry = Registry::new();
    let ops = antfs_metric::counter_vec(&registry, "store_ops_total", "ops", &["store", "op"]).unwrap();
    // 两个 store 共享同一 Vec,用 label 区分(common.md §2.2)。
    ops.with_label_values(&["p1", "put"]).inc();
    ops.with_label_values(&["p1", "put"]).inc();
    ops.with_label_values(&["p2", "put"]).inc();

    assert_eq!(ops.with_label_values(&["p1", "put"]).get(), 2);
    assert_eq!(ops.with_label_values(&["p2", "put"]).get(), 1);

    let text = gather(&registry).unwrap();
    assert!(text.contains("store_ops_total{op=\"put\",store=\"p1\"} 2"));
    assert!(text.contains("store_ops_total{op=\"put\",store=\"p2\"} 1"));
}

#[test]
fn labeled_gauge_and_histogram_vec() {
    let registry = Registry::new();
    let cap = antfs_metric::gauge_vec(&registry, "store_capacity_bytes", "cap", &["store"]).unwrap();
    cap.with_label_values(&["p1"]).set(4096);
    assert_eq!(cap.with_label_values(&["p1"]).get(), 4096);

    let lat =
        antfs_metric::latency_seconds_vec(&registry, "store_req_seconds", "latency", &["store", "op"])
            .unwrap();
    lat.with_label_values(&["p1", "get"]).observe(0.01);
    assert_eq!(lat.with_label_values(&["p1", "get"]).get_sample_count(), 1);
}
