//! Q5 自动扩容的**纯决策函数**(护栏在此,便于确定性单测)。
//!
//! 后台 loop 每周期调 [`plan_auto_grow`] 得出"本周期建几个",再走与手动 `CreatePartitions` 相同的
//! 选盘 + `client_write` 路径。护栏:低水位滞回(达标即 0,不抖动)、单周期限速(`batch`)、总量上限(`max_total`)。

/// 本周期应自动创建的分区数。
///
/// - `rw_count`:当前可写(RW)分区数;`total_count`:分区总数。
/// - `min_writable`:低水位;`rw_count ≥ min_writable` → 返回 0(**防抖/滞回**,不过量建)。
/// - `batch`:单周期上限(**限速**);`max_total`:分区总量上限(**护栏**)。
pub fn plan_auto_grow(
    rw_count: u64,
    total_count: u64,
    min_writable: u64,
    max_total: u64,
    batch: u64,
) -> u64 {
    if rw_count >= min_writable {
        return 0;
    }
    let deficit = min_writable - rw_count;
    let headroom = max_total.saturating_sub(total_count);
    deficit.min(batch).min(headroom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn at_or_above_watermark_creates_nothing() {
        assert_eq!(plan_auto_grow(4, 4, 4, 100, 4), 0);
        assert_eq!(plan_auto_grow(5, 5, 4, 100, 4), 0);
    }

    #[test]
    fn below_watermark_fills_deficit_within_batch() {
        // 缺 2(4-2),batch=4,headroom 充足 → 2。
        assert_eq!(plan_auto_grow(2, 2, 4, 100, 4), 2);
    }

    #[test]
    fn rate_limited_by_batch() {
        // 缺 10(10-0),batch=4 → 限速到 4。
        assert_eq!(plan_auto_grow(0, 0, 10, 100, 4), 4);
    }

    #[test]
    fn capped_by_total_max() {
        // 缺 4,batch=4,但总量上限只剩 1(99→100) → 1。
        assert_eq!(plan_auto_grow(0, 99, 4, 100, 4), 1);
        // 已达上限 → 0。
        assert_eq!(plan_auto_grow(0, 100, 4, 100, 4), 0);
    }
}
