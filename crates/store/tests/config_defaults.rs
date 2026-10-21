//! **默认配置之间**的跨 crate 不变量(store server ↔ blob 引擎)。
//!
//! 这两条都是"默认值单独看没问题、凑在一起就用不了"的类型,故单独立一个测试文件钉住:
//! 改动任一默认值(CLAUDE.md §5.1 要求显式指示)都会在这里变红。

use antfs_blob::StoreConfig;
use antfs_store::StoreServerConfig;

/// **非整数倍的总容量现在是合法的**:最后一段是"残段"(容量 = `total − k × segment`)。
/// 这条盯的是"配置项之间已解耦" —— 例如 100 GiB 分区 + 3 GiB 段 = 33 个整段 + 1 个 1 GiB 残段。
/// (段大小本身仍有 4 GiB 硬上界,故例子里的段不能大于 4 GiB。)
///
/// 历史:`blob::log::validate_capacities` 曾硬校验 `total % segment == 0`,导致**当时的默认组合**
/// (100 GiB 分区 + 8 GiB 段,那时还没有 4 GiB 段上限)直接 `BadArgument`、建不出 replica;现由
/// `crates/blob/tests/segment_tail.rs` 覆盖残段的几何与"写不下"语义。
#[test]
fn non_multiple_partition_and_segment_sizes_are_legal() {
    let part = StoreServerConfig::default().partition_capacity_bytes;
    let seg = StoreConfig::default().segment_size_bytes;
    let segments = part.div_ceil(seg);
    let tail = part - (segments - 1) * seg;
    assert_eq!(segments, 25, "默认组合 100 GiB / 4 GiB = 25 段");
    assert_eq!(tail, seg, "整倍时末段就是整段");
    // 3 GiB 段:33 整段 + 1 GiB 残段 —— 过去这是"非法配置",现在完全可用。
    let s3 = 3 * 1024 * 1024 * 1024u64;
    assert_eq!(part.div_ceil(s3), 34);
    assert_eq!(
        part - 33 * s3,
        1024 * 1024 * 1024,
        "多出一个 1 GiB 残段,不再浪费/报错"
    );
}

/// **默认段大小下单条记录的数据体必然 < `u32::MAX`** —— 记录内 `data_len` 是 u32,
/// 段 ≤ 4 GiB 时"长度回绕写出坏记录"在结构上不可能(显式校验 `record_data_len` 只是第二道保险)。
#[test]
fn default_segment_size_keeps_single_record_under_u32() {
    const SEG_HEADER: u64 = 64; // `log_segment.rs::HEADER_SIZE`
    const RECORD_OVERHEAD: u64 = 22; // `blob.rs::RECORD_OVERHEAD`
    let seg = StoreConfig::default().segment_size_bytes;
    assert_eq!(
        seg,
        4 * 1024 * 1024 * 1024,
        "默认段大小变更须复核本条不变量"
    );
    let max_data = seg - SEG_HEADER - RECORD_OVERHEAD;
    assert!(
        max_data < u32::MAX as u64,
        "上限 {max_data} 必须 < u32::MAX"
    );
}

/// **单请求上限必须放得进一个段** —— 否则出现"大小检查放行、引擎却存不下"的 400
/// (`max_body_bytes` 经 HTTP 校验通过,引擎却因"单条记录不得跨段"拒掉)。
#[test]
fn default_max_body_bytes_fits_in_one_segment() {
    const SEG_HEADER: u64 = 64;
    const RECORD_OVERHEAD: u64 = 22;
    let max_body = StoreServerConfig::default().max_body_bytes;
    let seg = StoreConfig::default().segment_size_bytes;
    assert!(
        max_body <= seg - SEG_HEADER - RECORD_OVERHEAD,
        "默认 max_body_bytes {max_body} 必须 ≤ 段内单条记录上限 {}",
        seg - SEG_HEADER - RECORD_OVERHEAD
    );
}
