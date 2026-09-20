//! 崩溃一致性属性测试(受 ShardStore 的"参考模型对拍"启发)。
//!
//! 不变式:**`flush()` 是持久化边界**。对任意 Append/Flush 操作序列,
//! "崩溃"(不做最终 flush 直接关闭)后重新打开:
//!   1. 恢复出的 `data_end` 恰等于最后一次成功 flush 时的 `data_end`;
//!   2. 所有完整落在该边界内的记录都能原样读回。
//!
//! 说明:本实现的 IO 是 buffered(进 OS page cache),"close 不 flush"本身不丢数据
//! (page cache 跨进程存活)。真正等价于"断电崩溃"的,是磁盘上 header 只反映
//! **上一次 flush 的状态**——故本测试直接校验:恢复出的 data_end 严格等于最后
//! 一次 flush 的 data_end,未 flush 尾部不进入恢复视图。

use std::sync::atomic::{AtomicU64, Ordering};

use ambry_store::LogSegment;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Append(Vec<u8>),
    Flush,
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        3 => proptest::collection::vec(any::<u8>(), 0..2500).prop_map(Op::Append),
        1 => Just(Op::Flush),
    ];
    proptest::collection::vec(op, 0..50)
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_path() -> std::path::PathBuf {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::path::PathBuf::from(format!("/var/tmp/ambry_prop_{pid}_{ns}_{seq}.seg"))
}

fn run_case(ops: Vec<Op>) {
    let path = unique_path();
    let data_capacity = 512u64 * 1024;
    let capacity = 64 + data_capacity;
    let mut seg = LogSegment::create(&path, capacity).unwrap();

    let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut durable_end: u64 = 0;

    for op in ops {
        match op {
            Op::Append(r) => {
                if seg.remaining() < r.len() as u64 {
                    continue;
                }
                let off = seg.append(&r).unwrap();
                records.push((off, r));
            }
            Op::Flush => {
                seg.flush().unwrap();
                durable_end = seg.data_end();
            }
        }
    }

    // 打开态:无论是否 flush,所有记录都应可读(buffered 语义下未 flush 数据在 page cache 可读)。
    for (off, data) in &records {
        let got = seg.read(*off, data.len()).unwrap();
        assert_eq!(&got, data, "live read mismatch @ {off}");
    }

    // 模拟崩溃:不做最终 flush,直接关闭。
    seg.close().unwrap();

    // 重新打开:恢复出的 data_end 恰等于最后一次 flush 的状态(持久化边界)。
    let seg = LogSegment::open(&path).unwrap();
    assert_eq!(
        seg.data_end(),
        durable_end,
        "recovered data_end must equal last flushed data_end"
    );
    for (off, data) in &records {
        if off + data.len() as u64 <= durable_end {
            let got = seg.read(*off, data.len()).unwrap();
            assert_eq!(&got, data, "durable record must survive crash @ {off}");
        }
    }

    seg.close().unwrap();
    let _ = std::fs::remove_file(&path);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn flush_is_durability_boundary(ops in ops_strategy()) {
        run_case(ops);
    }
}