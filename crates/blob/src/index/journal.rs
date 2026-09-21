//! `Journal`:内存中按 log offset 排序的「近期记录」游标索引(对齐 Ambry `Journal`)。
//!
//! 维护 `LogOffset → StoreKey` 有序映射,仅保留最近 `max_entries` 条(超限逐出最旧 offset),
//! 供复制增量拉取(`find_entries_since`)在「追近日志尾部」时免扫 log、直接按 offset 序取键。

use std::collections::BTreeMap;

use crate::types::{LogOffset, StoreKey};

pub struct Journal {
    entries: BTreeMap<LogOffset, StoreKey>,
    max_entries: usize,
}

impl Journal {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            max_entries: max_entries.max(1),
        }
    }

    /// 追加一条;超上限则逐出最旧(最小 offset)。offset 单调递增(log 追加序),故新条目恒在尾部。
    pub fn add(&mut self, offset: LogOffset, key: StoreKey) {
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&offset) {
            if let Some(&first) = self.entries.keys().next() {
                self.entries.remove(&first);
            }
        }
        self.entries.insert(offset, key);
    }

    pub fn first_offset(&self) -> Option<LogOffset> {
        self.entries.keys().next().copied()
    }

    /// journal 中 offset ≥ `from`(`inclusive` 决定是否含 `from` 本身)的 `(offset, key)`,按序。
    /// `from` 落在覆盖范围外(< 最旧 / > 最新 / 非已记录 offset)返回 `None`——调用方回退扫 log
    /// (对齐 Ambry `getEntriesSince` 越界返回 null)。
    pub fn entries_since(
        &self,
        from: LogOffset,
        inclusive: bool,
    ) -> Option<Vec<(LogOffset, StoreKey)>> {
        let first = *self.entries.keys().next()?;
        let last = *self.entries.keys().next_back()?;
        if from < first || from > last || !self.entries.contains_key(&from) {
            return None;
        }
        let out = self
            .entries
            .range(from..)
            .filter(|(&off, _)| inclusive || off != from)
            .map(|(&off, &key)| (off, key))
            .collect();
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> StoreKey {
        let mut b = [0u8; 32];
        b[0..8].copy_from_slice(&n.to_le_bytes());
        StoreKey::new(b)
    }

    fn off(n: u64) -> LogOffset {
        LogOffset::new(0, n * 100)
    }

    #[test]
    fn bounded_evicts_oldest() {
        let mut j = Journal::new(3);
        for i in 0..5u64 {
            j.add(off(i), key(i));
        }
        assert_eq!(j.first_offset(), Some(off(2)), "oldest two evicted");
        assert!(
            j.entries_since(off(0), true).is_none(),
            "evicted offset out of range"
        );
        let got = j.entries_since(off(2), true).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], (off(2), key(2)));
    }

    #[test]
    fn exclusive_skips_from_offset() {
        let mut j = Journal::new(10);
        for i in 0..4u64 {
            j.add(off(i), key(i));
        }
        let got = j.entries_since(off(1), false).unwrap();
        assert_eq!(
            got.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            vec![off(2), off(3)]
        );
    }

    #[test]
    fn out_of_range_returns_none() {
        let mut j = Journal::new(10);
        j.add(off(5), key(5));
        assert!(j.entries_since(off(9), true).is_none(), "beyond last");
        assert!(j.entries_since(off(1), true).is_none(), "before first");
    }
}
