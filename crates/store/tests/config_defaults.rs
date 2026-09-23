//! **默认配置之间**的跨 crate 不变量(store server ↔ blob 引擎)。
//!
//! 这两条都是"默认值单独看没问题、凑在一起就用不了"的类型,故单独立一个测试文件钉住:
//! 改动任一默认值(CLAUDE.md §5.1 要求显式指示)都会在这里变红。

use antfs_blob::StoreConfig;
use antfs_store::StoreServerConfig;

/// **默认段大小必须整除默认分区容量** —— `blob::log::validate_capacities` 是硬校验
/// (`total % segment == 0`,否则 `BadArgument`),不整除的直接后果是:**纯默认配置的 store 一个 replica 都建不出来**。
///
/// 历史:`StoreConfig::segment_size_bytes` 曾是 8 GiB,而分区容量默认 100 GiB —— `100 % 8 != 0`,
/// 即旧默认组合会在 `ensure_replica` 建库时报
/// `total capacity 107374182400 must be a multiple of segment capacity 8589934592`。
/// 现默认 4 GiB:`100 / 4 = 25` 整,同时满足下面那条 u32 不变量。
#[test]
fn default_segment_size_divides_default_partition_capacity() {
    let part = StoreServerConfig::default().partition_capacity_bytes;
    let seg = StoreConfig::default().segment_size_bytes;
    assert_eq!(
        part % seg,
        0,
        "默认分区容量 {part} 不能被默认段大小 {seg} 整除(会 BadArgument,建不出 replica)"
    );
    assert_eq!(part / seg, 25, "默认组合应为 25 段/分区");
}

/// **默认段大小下单条记录的数据体必然 < `u32::MAX`** —— 记录内 `data_len` 是 u32,
/// 段 ≤ 4 GiB 时"长度回绕写出坏记录"在结构上不可能(显式校验 `record_data_len` 只是第二道保险)。
#[test]
fn default_segment_size_keeps_single_record_under_u32() {
    const SEG_HEADER: u64 = 64; // `log_segment.rs::HEADER_SIZE`
    const RECORD_OVERHEAD: u64 = 38; // `blob.rs::RECORD_OVERHEAD`(KEY_SIZE = 8)
    let seg = StoreConfig::default().segment_size_bytes;
    assert_eq!(seg, 4 * 1024 * 1024 * 1024, "默认段大小变更须复核本条不变量");
    let max_data = seg - SEG_HEADER - RECORD_OVERHEAD;
    assert!(max_data < u32::MAX as u64, "上限 {max_data} 必须 < u32::MAX");
}

/// **单请求上限必须放得进一个段** —— 否则出现"大小检查放行、引擎却存不下"的 400
/// (`max_body_bytes` 经 HTTP 校验通过,引擎却因"单条记录不得跨段"拒掉)。
#[test]
fn default_max_body_bytes_fits_in_one_segment() {
    const SEG_HEADER: u64 = 64;
    const RECORD_OVERHEAD: u64 = 38;
    let max_body = StoreServerConfig::default().max_body_bytes;
    let seg = StoreConfig::default().segment_size_bytes;
    assert!(
        max_body <= seg - SEG_HEADER - RECORD_OVERHEAD,
        "默认 max_body_bytes {max_body} 必须 ≤ 段内单条记录上限 {}",
        seg - SEG_HEADER - RECORD_OVERHEAD
    );
}
