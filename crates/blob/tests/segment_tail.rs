//! 容量几何:总容量**允许不是**段容量的整数倍 —— 最后一段是"残段",装不下记录时那部分空间视为写满。
//!
//! 为什么单独立测试:这条规则决定了"配置项之间是否解耦"(100 GiB 分区 + 8 GiB 段 = 12 整段 + 1 个 4 GiB 残段),
//! 以及"写不下"到底报 400(记录超过任何段)还是 507(分区写满)——两者给运维的处置完全不同。
//!
//! 注意 `Log` 是**裸字节**日志(记录头/CRC 由 `BlobStore` 编码),故这里直接给 `Log::append` 的长度就是
//! `payload().len()`;`BlobStore` 那一条才带 38B 记录开销。

use std::path::Path;

use antfs_blob::{BlobStore, Error, Log, LogSegmentName, StoreKey};

const SEG_HDR: u64 = 64; // `LogSegment` 段头
const REC_OVERHEAD: u64 = 38; // `BlobStore` 的 记录头 + 尾 CRC(KEY_SIZE = 8)
const DATA: u64 = 500; // 负载长度
const LOG_REC: u64 = DATA; // 直接给 Log::append 的裸记录长度

fn tmpdir(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "antfs_tail_{}_{ns}_{}_{tag}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 某容量的段里能放几条 `rec` 长度的记录(残段也适用;容量小于段头时得 0)。
fn fits(seg_capacity: u64, rec: u64) -> u64 {
    seg_capacity.saturating_sub(SEG_HDR) / rec
}

fn payload() -> Vec<u8> {
    vec![7u8; DATA as usize]
}

fn seg_file_size(dir: &Path, pos: u64) -> u64 {
    std::fs::metadata(dir.join(LogSegmentName(pos).filename()))
        .expect("segment file")
        .len()
}

/// 不整除的总容量:段数 = ⌈total/segment⌉,末段容量 = 余数,记录能一直写到残段塞满为止。
#[test]
fn non_multiple_capacity_uses_remainder_segment() {
    let dir = tmpdir("rem");
    let seg = 4096u64;
    let tail = 2048u64;
    let total = seg * 2 + tail;
    let log = Log::create(&dir, total, seg, 0).unwrap();

    let per_full = fits(seg, LOG_REC);
    let in_tail = fits(tail, LOG_REC);
    let want_total = per_full * 2 + in_tail;
    assert!(
        per_full > 0 && in_tail > 0,
        "测试参数的残段必须装得下至少一条"
    );

    let mut n = 0u64;
    while log.append(&payload()).is_ok() {
        n += 1;
        assert!(n <= want_total, "不该写超过声明容量");
    }
    assert_eq!(n, want_total, "应写满 2 个整段 + 残段");
    assert!(matches!(log.append(&payload()), Err(Error::SegmentFull)));
    assert_eq!(
        log.segment_spans(),
        vec![
            (0, per_full * LOG_REC),
            (1, per_full * LOG_REC),
            (2, in_tail * LOG_REC)
        ],
        "三段几何:两个整段 + 一个残段"
    );
    // 段文件物理大小 = 各自容量(残段文件确实是 2048 而非 4096)
    assert_eq!(seg_file_size(&dir, 0), seg);
    assert_eq!(seg_file_size(&dir, 1), seg);
    assert_eq!(seg_file_size(&dir, 2), tail);
}

/// 残段连"段头都装不下"时:不建无意义的空段,**直接按写满处理**(不误报"记录超过段容量")。
#[test]
fn remainder_too_small_is_treated_as_full_not_bad_argument() {
    let dir = tmpdir("tiny");
    let seg = 4096u64;
    let total = seg * 2 + 20; // 残段 20B < 段头 64B
    let log = Log::create(&dir, total, seg, 0).unwrap();

    let per_full = fits(seg, LOG_REC);
    let mut n = 0u64;
    while log.append(&payload()).is_ok() {
        n += 1;
    }
    assert_eq!(n, per_full * 2, "只能写满两个整段");
    assert!(
        matches!(log.append(&payload()), Err(Error::SegmentFull)),
        "残段放不下 → SegmentFull(而非参数错)"
    );
    assert_eq!(log.segment_spans().len(), 2, "不该建出用不了的残段文件");
    assert!(!dir.join(LogSegmentName(2).filename()).exists());
}

/// `total < segment`:退化为**单段**(容量 = total,对齐 Ambry `segmentCapacity = min(capacity, segment)`)。
#[test]
fn total_smaller_than_segment_is_a_single_partial_segment() {
    let dir = tmpdir("single");
    let total = 1000u64;
    let seg = 4096u64;
    let log = Log::create(&dir, total, seg, 0).unwrap();

    let n_fit = fits(total, LOG_REC);
    let mut n = 0u64;
    while log.append(&payload()).is_ok() {
        n += 1;
    }
    assert_eq!(n, n_fit);
    assert!(matches!(log.append(&payload()), Err(Error::SegmentFull)));
    assert_eq!(log.segment_spans(), vec![(0, n_fit * LOG_REC)]);
    assert_eq!(seg_file_size(&dir, 0), total, "单段容量就是总容量");
}

/// 重开后残段几何不变(段容量从各段自己的 header 读回),且仍是写满状态。
#[test]
fn remainder_geometry_survives_reopen() {
    let dir = tmpdir("reopen");
    let seg = 4096u64;
    let total = seg * 2 + 2048;

    let spans_before = {
        let log = Log::create(&dir, total, seg, 0).unwrap();
        while log.append(&payload()).is_ok() {}
        log.flush().unwrap();
        log.segment_spans()
    };
    assert_eq!(spans_before.len(), 3);

    let log = Log::open(&dir, total, seg).unwrap();
    assert_eq!(log.segment_spans(), spans_before, "重开后段几何必须一致");
    assert!(
        matches!(log.append(&payload()), Err(Error::SegmentFull)),
        "重开后仍是写满"
    );
}

/// 引擎层语义:残段写不下时报 `StoreFull`(→ 数据面 507 "该加分区了"),**不是** `BadArgument`(→ 400)。
#[test]
fn blob_store_reports_store_full_at_remainder_boundary() {
    let dir = tmpdir("store");
    let seg = 4096u64;
    let tail = 2048u64;
    let total = seg * 2 + tail;
    let rec = DATA + REC_OVERHEAD; // BlobStore 会自己加记录头尾
    let store = BlobStore::create(&dir, total, seg, 1000).unwrap();

    let mut n = 0u32;
    loop {
        match store.put(StoreKey::from_parts(0, n), &payload()) {
            Ok(()) => n += 1,
            Err(e) => {
                assert!(
                    matches!(e, Error::StoreFull),
                    "残段写不下应是 StoreFull,实际 {e:?}"
                );
                break;
            }
        }
    }
    assert_eq!(n as u64, fits(seg, rec) * 2 + fits(tail, rec));
    assert!(store.is_full(), "到这一步该报满了");
}
