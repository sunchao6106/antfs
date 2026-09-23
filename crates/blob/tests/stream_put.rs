//! 流式写(`BlobStore::put_from_reader`)的集成测试:与整块 `put` 语义等价 + 失败/放弃预留的不变量。
//!
//! 覆盖点(对齐 `put_from_reader` 的文档承诺):
//!   1. 与整块 `put` **逐字段等价**(数据、元数据、占用字节);
//!   2. 跨 64KiB 分块、跨段滚动都正确;
//!   3. 读流出错/长度非法 → **不发布任何内容**(索引里查不到,后续写覆盖该区间);
//!   4. flush 后重开可读(崩溃边界语义与整块写一致);
//!   5. 与既有语义交互:`AlreadyExists`、TTL 过期。

use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use antfs_blob::{BlobStore, Error, StoreKey};

// `seg_data` = **段容量**(含 64B 段头,故数据容量 = `seg_data - 64`)。
// (总容量不必是段容量的整数倍 —— 末段可为残段,见 `tests/segment_tail.rs`;这里仍取整数倍保持简单。)

fn tmpdir(tag: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "antfs_stream_{}_{ns}_{seq}_{tag}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn key(n: u32) -> StoreKey {
    StoreKey::from_parts(0, n)
}

/// 确定性伪随机负载(避免全零数据掩盖 crc/拷贝问题)。
fn payload(len: usize) -> Vec<u8> {
    let mut s = 0x9E37_79B9_7F4A_7C15u64;
    (0..len)
        .map(|_| {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            (s.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) as u8
        })
        .collect()
}

/// 读到 `fail_at` 字节后必定报错(模拟上游读流出错/断连)。
struct FailingReader {
    data: Vec<u8>,
    pos: usize,
    fail_at: usize,
}

impl Read for FailingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.fail_at {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected read failure",
            ));
        }
        let n = std::cmp::min(self.fail_at - self.pos, buf.len());
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[test]
fn stream_matches_buffered_put() {
    let seg_data = 1 << 20;
    let dir_a = tmpdir("buffered");
    let dir_b = tmpdir("stream");
    let a = BlobStore::create(&dir_a, seg_data * 4, seg_data, 1000).unwrap();
    let b = BlobStore::create(&dir_b, seg_data * 4, seg_data, 1000).unwrap();

    // 跨多个 64KiB 分块(且远小于段容量)。
    let data = payload(300 * 1024);
    a.put(key(1), &data).unwrap();
    b.put_from_reader(key(1), data.len() as u64, &mut &data[..], -1)
        .unwrap();

    assert_eq!(a.get(&key(1)).unwrap(), data);
    assert_eq!(b.get(&key(1)).unwrap(), data, "streamed data must match");
    assert_eq!(
        a.stat(&key(1)).unwrap(),
        b.stat(&key(1)).unwrap(),
        "metadata must be byte-for-byte equivalent"
    );
    assert_eq!(
        a.size_in_bytes(),
        b.size_in_bytes(),
        "record size (data + overhead) must match"
    );

    a.close().unwrap();
    b.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

#[test]
fn stream_rolls_over_to_new_segment() {
    let seg_data = 1 << 20;
    let dir = tmpdir("roll");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    // 先占掉大半个段(整块写)。
    let first = payload(700 * 1024);
    store.put(key(1), &first).unwrap();
    // 再流式写一条放不进剩余空间的记录 → 触发滚动。
    let second = payload(500 * 1024);
    store
        .put_from_reader(key(2), second.len() as u64, &mut &second[..], -1)
        .unwrap();

    assert_eq!(store.get(&key(1)).unwrap(), first);
    assert_eq!(store.get(&key(2)).unwrap(), second);

    // 重开后索引由 log 重建,跨段的流式记录仍可读。
    store.flush().unwrap();
    store.close().unwrap();
    let reopened = BlobStore::open(&dir, seg_data * 4, seg_data, 1000).unwrap();
    assert_eq!(reopened.get(&key(1)).unwrap(), first);
    assert_eq!(reopened.get(&key(2)).unwrap(), second);
    reopened.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_error_leaves_no_record_and_store_stays_usable() {
    let seg_data = 1 << 20;
    let dir = tmpdir("readerr");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    let mut bad = FailingReader {
        data: payload(4096),
        pos: 0,
        fail_at: 1000,
    };
    let err = store
        .put_from_reader(key(1), 4096, &mut bad, -1)
        .unwrap_err();
    assert!(
        matches!(err, Error::Io(_)),
        "read failure must surface as Io, got {err:?}"
    );

    // 未发布的记录对索引不可见。
    assert!(matches!(store.get(&key(1)), Err(Error::NotFound)));
    assert!(matches!(store.stat(&key(1)), Err(Error::NotFound)));

    // 放弃的预留区间被后续写覆盖,store 仍可用且自洽。
    let data = payload(2000);
    store.put(key(2), &data).unwrap();
    assert_eq!(store.get(&key(2)).unwrap(), data);

    store.close().unwrap();
    let reopened = BlobStore::open(&dir, seg_data * 4, seg_data, 1000).unwrap();
    assert_eq!(reopened.get(&key(2)).unwrap(), data);
    assert!(
        matches!(reopened.get(&key(1)), Err(Error::NotFound)),
        "abandoned reservation must never become visible, even after reopen"
    );
    reopened.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stream_rejects_illegal_len() {
    let seg_data = 1 << 20;
    let dir = tmpdir("badlen");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    // 超 u32(记录内 data_len 为 u32):在碰 reader 之前就拒。
    let mut src = &b"xyz"[..];
    let err = store
        .put_from_reader(key(1), u32::MAX as u64 + 1, &mut src, -1)
        .unwrap_err();
    assert!(matches!(err, Error::BadArgument(_)), "got {err:?}");
    assert_eq!(
        store.size_in_bytes(),
        0,
        "rejected put must not consume log space"
    );

    // 单条记录不得跨段:超段数据容量同样拒。
    let mut src = &b"xyz"[..];
    let err = store
        .put_from_reader(key(2), seg_data + 1, &mut src, -1)
        .unwrap_err();
    assert!(
        matches!(err, Error::BadArgument(_) | Error::StoreFull),
        "got {err:?}"
    );
    assert_eq!(store.size_in_bytes(), 0);

    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stream_respects_already_exists() {
    let seg_data = 1 << 20;
    let dir = tmpdir("dup");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    let data = payload(1024);
    store.put(key(1), &data).unwrap();
    let err = store
        .put_from_reader(key(1), data.len() as u64, &mut &data[..], -1)
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists), "got {err:?}");
    assert_eq!(store.get(&key(1)).unwrap(), data);

    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stream_records_expiration() {
    let seg_data = 1 << 20;
    let dir = tmpdir("ttl");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    let data = payload(1024);
    // 已过去的过期时刻 → 立即可判定过期(不依赖 sleep,见 CLAUDE.md §5.6)。
    let past = 1_000i64;
    store
        .put_from_reader(key(1), data.len() as u64, &mut &data[..], past)
        .unwrap();
    assert_eq!(store.stat(&key(1)).unwrap().expiration_ms, past);
    assert!(matches!(store.get(&key(1)), Err(Error::Expired)));

    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn streamed_record_survives_reopen() {
    let seg_data = 1 << 20;
    let dir = tmpdir("persist");
    let store = BlobStore::create(&dir, seg_data * 4, seg_data, 1000).unwrap();

    let data = payload(400 * 1024);
    store
        .put_from_reader(key(7), data.len() as u64, &mut &data[..], -1)
        .unwrap();
    store.flush().unwrap();
    store.close().unwrap();

    let reopened = BlobStore::open(&dir, seg_data * 4, seg_data, 1000).unwrap();
    assert_eq!(reopened.get(&key(7)).unwrap(), data);
    assert_eq!(reopened.stat(&key(7)).unwrap().data_len, data.len() as u64);
    reopened.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
