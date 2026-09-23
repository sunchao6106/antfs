//! store 接入 config / metric 的集成测试(DI 形状,对齐 Ambry `StorageManager`→`BlobStore`)。
//!
//! 校验:① `StoreConfig` 加载/默认/校验;② 注入的 `Registry` 上能观测到真实打点增量
//! (对齐 CLAUDE.md §6:对被测对象实际接入的那个 Registry 断言);③ 多 store 共享一个
//! Registry 靠 `store` label 区分(common.md §2.2)。

use std::sync::atomic::{AtomicU64, Ordering};

use antfs_config::{Loader, Validate};
use antfs_metric::{gather, Registry};
use antfs_blob::{BlobStore, StoreConfig, StoreKey, StoreMetrics};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_dir() -> std::path::PathBuf {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let p = std::path::PathBuf::from(format!("/var/tmp/ambry_store_cm_{pid}_{ns}_{seq}"));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn key(n: u8) -> StoreKey {
    StoreKey::new([n; 8])
}

#[test]
fn store_config_defaults_override_and_validate() {
    let def: StoreConfig = Loader::new().load().unwrap();
    assert_eq!(def, StoreConfig::default());

    let cfg: StoreConfig = Loader::new()
        .with_inline_toml("segment_size_bytes = 4096\nindex_max_inmem_elements = 500")
        .load()
        .unwrap();
    assert_eq!(cfg.segment_size_bytes, 4096);
    assert_eq!(cfg.index_max_inmem_elements, 500);

    let bad: StoreConfig = Loader::new()
        .with_inline_toml("segment_size_bytes = 0")
        .load()
        .unwrap();
    assert!(bad.validate().is_err());
}

#[test]
fn di_constructor_emits_metrics_on_injected_registry() {
    let dir = unique_dir();
    let registry = Registry::new();
    let metrics = StoreMetrics::new(&registry).unwrap();
    let cfg = StoreConfig {
        segment_size_bytes: 4096,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    };
    let store = BlobStore::create_with(&dir, 4096 * 8, &cfg, &metrics, "p1").unwrap();

    store.put(key(1), b"hello").unwrap();
    store.put(key(2), b"world").unwrap();
    assert_eq!(store.get(&key(1)).unwrap(), b"hello");
    // 重复 put 命中已存在 -> already_exists 计数。
    assert!(store.put(key(1), b"dup").is_err());
    store.delete(key(2)).unwrap();
    store.flush().unwrap();

    let text = gather(&registry).unwrap();
    // 操作直方图带 store + op label。
    assert!(
        text.contains("store_request_duration_seconds_count{op=\"put\",store=\"p1\"}"),
        "missing put timer:\n{text}"
    );
    assert!(text.contains("store_request_duration_seconds_count{op=\"get\",store=\"p1\"}"));
    assert!(text.contains("store_request_duration_seconds_count{op=\"delete\",store=\"p1\"}"));
    assert!(text.contains("store_already_exists_total{store=\"p1\"} 1"));
    // flush 后容量 gauge 已刷新(>0)。
    assert!(
        text.contains("store_log_used_capacity_bytes{store=\"p1\"}"),
        "missing capacity gauge:\n{text}"
    );
}

#[test]
fn two_stores_share_registry_distinguished_by_label() {
    let registry = Registry::new();
    let metrics = StoreMetrics::new(&registry).unwrap();
    let cfg = StoreConfig {
        segment_size_bytes: 4096,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    };

    let s1 = BlobStore::create_with(&unique_dir(), 4096 * 8, &cfg, &metrics, "p1").unwrap();
    let s2 = BlobStore::create_with(&unique_dir(), 4096 * 8, &cfg, &metrics, "p2").unwrap();
    s1.put(key(1), b"a").unwrap();
    s1.put(key(2), b"b").unwrap();
    s2.put(key(1), b"c").unwrap();

    let text = gather(&registry).unwrap();
    assert!(text.contains("store_request_duration_seconds_count{op=\"put\",store=\"p1\"} 2"));
    assert!(text.contains("store_request_duration_seconds_count{op=\"put\",store=\"p2\"} 1"));
}

/// 默认段大小的**结构性不变量**:段 ≤ 4 GiB 时,单条记录 ≤ `段 − 64(段头) − 38(记录头尾) < u32::MAX`,
/// 故"`data_len` 只有 u32 → 长度回绕写出坏记录"在默认配置下**不可能发生**
/// (`record_data_len` 的显式校验是第二道保险,供把段配大时兜底)。
///
/// 同时这是一道"别静默改默认值"的闸门(CLAUDE.md §5.1:改默认需显式指示):
/// 若要把默认段调大过 4 GiB,必须同时确认上面那条不变量已不再被依赖 —— 那时改这条断言并说明理由。
#[test]
fn default_segment_size_keeps_single_record_under_u32() {
    const SEG_HEADER: u64 = 64; // `log_segment.rs::HEADER_SIZE`
    const RECORD_OVERHEAD: u64 = 38; // `blob.rs::RECORD_OVERHEAD`(KEY_SIZE = 8)
    // 硬编码 4 GiB,而不用 `StoreConfig::default()` —— 这样"默认被改"必然让本测试变红。
    let seg = 4 * 1024 * 1024 * 1024u64;
    assert_eq!(
        StoreConfig::default().segment_size_bytes,
        seg,
        "默认段大小变更须知会(CLAUDE.md §5.1),并复核本条不变量"
    );
    let max_record = seg - SEG_HEADER;
    let max_data = max_record - RECORD_OVERHEAD;
    assert!(
        max_data < u32::MAX as u64,
        "默认段下单条数据体必须 < u32::MAX,实际上限 {max_data}"
    );
}
