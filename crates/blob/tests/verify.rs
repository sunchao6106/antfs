//! `BlobStore::verify` / `index_stats` / **容错打开** 的集成测试(scrub 的本地完整性校验)。
//!
//! 覆盖:干净副本零坏记录、人为损坏可定位、`max_bytes` 限流截断、摘要口径(活记录/墓碑/活字节),
//! 以及"坏记录也允许启动并大声报错"(`open_damage` + 指标 + 其余记录照常可读可写)。

use antfs_blob::{BlobStore, StoreConfig, StoreKey};

/// RAII 临时目录(测试结束整目录回收,`$TMPDIR` 下,带 ambry 前缀可辨认)。
fn unique_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("ambry_verify_")
        .tempdir()
        .unwrap()
}

fn small_config() -> StoreConfig {
    StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

fn key(n: u8) -> StoreKey {
    StoreKey::from_parts(0, n as u32)
}

#[test]
fn clean_store_verifies_and_summarises() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store =
        BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();

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
    let store =
        BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();
    for n in 0..6u8 {
        store.put(key(n), &[n; 64]).unwrap();
    }
    store.flush().unwrap();

    let log_dir = dir.path().join("log");
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
    assert_eq!(
        r.first_bad_offset,
        Some((0, 0)),
        "应定位到首段 offset 0: {r:?}"
    );
    assert_eq!(r.records, 5, "坏记录之后仍继续扫(共 6 条,记下继续): {r:?}");
}

#[test]
fn verify_respects_max_bytes_and_reports_truncated() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store =
        BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();
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
    let store =
        BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();

    store.put(key(9), b"hello").unwrap();
    assert_eq!(store.index_stats().live_records, 1);
    store.delete(key(9)).unwrap();
    let s = store.index_stats();
    assert_eq!(s.live_records, 0, "删除后无活记录");
    assert_eq!(s.tombstones, 1);
    assert_eq!(s.live_bytes, 0);
    // 删除不可逆:blobId 永久占用 —— 删后没有"复活"这条路,重写只能报 AlreadyExists。
    assert!(matches!(
        store.put(key(9), b"hello"),
        Err(antfs_blob::Error::AlreadyExists)
    ));
    let s = store.index_stats();
    assert_eq!(s.live_records, 0, "墓碑仍是终态");
    assert_eq!(s.tombstones, 1);
}

// 注:记录损坏不再让 `open` 整体失败 —— 见下文的容错打开测试(跳过坏记录 + `open_damage` +
// `store_open_bad_records_total` + ERROR 日志;盘上字节不删不改)。

/// 打开时的容错:`open` 遇到坏记录**跳过并继续**,副本仍能起来、其余数据仍可读可写,
/// 损伤经 `open_damage()` 与指标大声暴露(盘上字节不删不改)。
#[test]
fn open_tolerates_damaged_record_and_reports_damage() {
    let dir = unique_dir();
    {
        let registry = antfs_metric::Registry::new();
        let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
        let store =
            BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();
        for n in 0..6u8 {
            store.put(key(n), &[n; 64]).unwrap();
        }
        store.flush().unwrap();
    }

    // 破坏第一条记录的 magic(物理 64 = 数据 offset 0),data_len 保持完好 → 可重同步。
    let seg = first_segment(dir.path());
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes[64] ^= 0xff;
    std::fs::write(&seg, &bytes).unwrap();

    // ① 打开成功(不再整体失败),损伤被如实记录。
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::open_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0")
        .expect("坏记录不应让副本起不来");
    let d = store.open_damage();
    assert_eq!(d.bad_records, 1, "应记录 1 条被跳过的坏记录: {d:?}");
    assert_eq!(
        d.first_bad_offset,
        Some((0, 0)),
        "应定位到首段 offset 0: {d:?}"
    );
    assert_eq!(d.abandoned_segments, 0, "可重同步,不应放弃段: {d:?}");
    assert!(!d.is_clean());

    // ② 其余 key 仍可读(坏记录只影响它自己)。
    for n in 1..6u8 {
        assert_eq!(store.get(&key(n)).unwrap(), vec![n; 64], "key {n} 应仍可读");
    }
    assert!(store.get(&key(0)).is_err(), "坏掉的那条读不到(如实报错)");

    // ③ 副本仍可写(带伤服务)。
    store.put(key(9), b"after-recovery").unwrap();
    assert_eq!(store.get(&key(9)).unwrap(), b"after-recovery");

    // ④ 大声报错:指标 + verify 都能看到。
    let text = antfs_metric::gather(&registry).unwrap();
    assert!(
        text.contains(r#"store_open_bad_records_total{store="p0"} 1"#),
        "指标应记录跳过数, got:\n{text}"
    );
    let v = store.verify(u64::MAX).unwrap();
    assert_eq!(v.bad_records, 1, "verify 也应看到同一条: {v:?}");
}

/// 无法重同步时:放弃该段剩余部分(记 `abandoned_segments`),但**后续段仍要被恢复**到索引,
/// 且报告可见 —— 既不静默,也不因为一段坏掉就丢整个副本。
#[test]
fn open_abandons_unresyncable_segment_but_recovers_later_segments() {
    let dir = unique_dir();
    // 极小的段:保证写多条记录后滚出多个段(每段一条记录 + 头)。
    let cfg = StoreConfig {
        segment_size_bytes: 256,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    };
    {
        let registry = antfs_metric::Registry::new();
        let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
        let store = BlobStore::create_with(dir.path(), 1 << 30, &cfg, &metrics, "p0").unwrap();
        for n in 0..4u8 {
            store.put(key(n), &[n; 100]).unwrap();
        }
        store.flush().unwrap();
    }
    let segs = segment_files(dir.path());
    assert!(segs.len() >= 2, "应滚出至少两个段: {segs:?}");

    // 破坏**第一段**的记录头(magic + data_len 一起改)→ 无法重同步。
    let first = &segs[0];
    let mut bytes = std::fs::read(first).unwrap();
    bytes[64] = 0x00; // magic 低位
    bytes[65] = 0x00; // magic 高位
                      // data_len 在记录内偏移 16(u32;magic@0、version@2、flags@3、key_suffix@4、exp@8、data_len@16):
                      // 写成极大值使"按头重同步"越界 → 判为不可重同步。
    for b in bytes[64 + 16..64 + 20].iter_mut() {
        *b = 0xff;
    }
    std::fs::write(first, &bytes).unwrap();

    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    let store = BlobStore::open_with(dir.path(), 1 << 30, &cfg, &metrics, "p0")
        .expect("不可重同步也不该让副本起不来");
    let d = store.open_damage();
    assert_eq!(d.bad_records, 1, "第一段那条算坏记录: {d:?}");
    assert_eq!(d.abandoned_segments, 1, "该段剩余部分应被放弃: {d:?}");

    // 后续段的记录必须仍在索引里(可读)。
    let mut readable = 0;
    for n in 0..4u8 {
        if store.get(&key(n)).is_ok() {
            readable += 1;
        }
    }
    assert!(
        readable >= 2,
        "后续段的记录应被恢复(实得 {readable} 条可读): {d:?}"
    );

    let text = antfs_metric::gather(&registry).unwrap();
    assert!(
        text.contains(r#"store_open_abandoned_segments_total{store="p0"} 1"#),
        "放弃段数应上指标, got:\n{text}"
    );
}

/// 干净副本:零损伤、无副作用(不刷日志/不涨指标)。
#[test]
fn clean_open_reports_no_damage() {
    let dir = unique_dir();
    let registry = antfs_metric::Registry::new();
    let metrics = antfs_blob::StoreMetrics::new(&registry).unwrap();
    {
        let store =
            BlobStore::create_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();
        store.put(key(1), b"clean").unwrap();
        store.flush().unwrap();
    }
    let store = BlobStore::open_with(dir.path(), 1 << 30, &small_config(), &metrics, "p0").unwrap();
    assert!(store.open_damage().is_clean(), "{:?}", store.open_damage());
    let text = antfs_metric::gather(&registry).unwrap();
    assert!(
        !text.contains(r#"store_open_bad_records_total{store="p0"} 1"#),
        "干净副本不该涨指标: {text}"
    );
}

/// 段文件辅助:首个 / 全部 `.seg`。
fn first_segment(dir: &std::path::Path) -> std::path::PathBuf {
    segment_files(dir)
        .into_iter()
        .next()
        .expect("至少一个段文件")
}

fn segment_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir.join("log"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
        .collect();
    v.sort();
    v
}
