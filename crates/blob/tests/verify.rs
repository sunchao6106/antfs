//! `BlobStore::verify` / `index_stats` 的集成测试(scrub 的本地完整性校验)。
//!
//! 覆盖:干净副本零坏记录、人为损坏可定位、`max_bytes` 限流截断、摘要口径(活记录/墓碑/活字节)。

use std::sync::atomic::{AtomicU64, Ordering};

use antfs_blob::{BlobStore, StoreConfig, StoreKey};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_dir() -> std::path::PathBuf {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let p = std::path::PathBuf::from(format!("/var/tmp/ambry_verify_{}_{ns}_{seq}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn small_config() -> StoreConfig {
    StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn key(n: u8) -> StoreKey {
    StoreKey::new([n; 8])
}


#[test]
fn clean_store_verifies_and_summarises() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::create_with(&dir, 1 << 30, &small_config(), &metrics, "p0").unwrap();

    store.put(key(1), b"aaaa").unwrap();
    store.put(key(2), b"bbbbbb").unwrap();
    store.put(key(3), b"cc").unwrap();
    store.delete(key(3)).unwrap(); // 变成墓碑

    let r = store.verify(u64::MAX).unwrap();
    assert_eq!(r.bad_records, 0, "干净副本不应有坏记录");
    assert_eq!(r.index_mismatch, 0, "索引与 log 应一致");
    assert!(r.first_bad_offset.is_none());
    assert!(!r.truncated);
    assert_eq!(r.records, 4, "3 次 PUT + 1 次 DELETE 墓碑 = 4 条记录");

    let s = store.index_stats();
    assert_eq!(s.live_records, 2, "两个活 key");
    assert_eq!(s.tombstones, 1, "一个墓碑");
    assert_eq!(s.live_bytes, 4 + 6, "活记录的数据字节(不含记录头/CRC)");
    assert_eq!(s.sealed_segments, 0, "单段小库无封存段");
}

#[test]
fn verify_locates_corrupted_record() {
    // 在线损坏场景(副本已在跑):写完后直接翻转段文件里第一条记录的 magic,再 verify。
    //
    // 注:这里**不重开** store —— 见文件末尾"已知缺口"一节:open() 的索引重建遇到坏记录会整体失败,
    // 那种"副本起不来"的形态无法经本 API 检查(属后续增强)。
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::create_with(&dir, 1 << 30, &small_config(), &metrics, "p0").unwrap();
    for n in 0..6u8 {
        store.put(key(n), &[n; 64]).unwrap();
    }
    store.flush().unwrap();

    let log_dir = dir.join("log");
    let seg = std::fs::read_dir(&log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
        .expect("至少一个段文件");
    let mut bytes = std::fs::read(&seg).unwrap();
    assert!(bytes.len() > 200, "段应有内容");
    // 段头固定 64 字节(物理),数据 offset 0 映射到物理 64 → 翻转第 64 字节 = 破坏第一条记录的 magic。
    // 只坏 magic、不动 data_len,故"重同步到下一条"这条路径也被覆盖。
    bytes[64] ^= 0xff;
    std::fs::write(&seg, &bytes).unwrap();

    let r = store.verify(u64::MAX).unwrap();
    assert_eq!(r.bad_records, 1, "恰好第一条记录坏: {r:?}");
    assert_eq!(r.first_bad_offset, Some((0, 0)), "应定位到首段 offset 0: {r:?}");
    assert_eq!(r.records, 5, "坏记录之后仍继续扫(共 6 条,记下继续): {r:?}");
}

#[test]
fn verify_respects_max_bytes_and_reports_truncated() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::create_with(&dir, 1 << 30, &small_config(), &metrics, "p0").unwrap();
    for n in 0..5u8 {
        store.put(key(n), &[n; 128]).unwrap();
    }
    store.flush().unwrap();

    let full = store.verify(u64::MAX).unwrap();
    assert!(!full.truncated);
    let partial = store.verify(1).unwrap();
    assert!(partial.truncated, "max_bytes=1 应立即到点");
    assert!(partial.records < full.records, "部分扫描的记录数应更少");
}

#[test]
fn index_stats_tracks_growth_after_readd_and_tombstone() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::create_with(&dir, 1 << 30, &small_config(), &metrics, "p0").unwrap();

    store.put(key(9), b"hello").unwrap();
    assert_eq!(store.index_stats().live_records, 1);
    store.delete(key(9)).unwrap();
    let s = store.index_stats();
    assert_eq!(s.live_records, 0, "删除后无活记录");
    assert_eq!(s.tombstones, 1);
    assert_eq!(s.live_bytes, 0);
    store.undelete(key(9)).unwrap();
    let s = store.index_stats();
    assert_eq!(s.live_records, 1, "复活后又是活记录");
    assert_eq!(s.tombstones, 0);
    assert_eq!(s.live_bytes, 5);
}

// 已知缺口(本次未修,需产品决策,已在 docs/design/store.md 记录):
// 记录损坏会让 `BlobStore::open`(索引重建)整体失败 → 该副本**无法启动**,因而也无法经 verify 检查。
// 若要"坏记录也允许启动并大声报错"(Ambry 的 IndexRecovery 容错方向),需改恢复路径的语义 —— 属后续增强。
