//! `PersistentIndex`:key → [`IndexValue`] 的分段索引(同步 API,索引文件走 std::fs)。
//!
//! - 活跃段:内存有序 `BTreeMap`,写入即插入;无过滤器(直接查 map)。
//! - 封存段:活跃段达阈值 → 排序落盘 + 构建 Binary Fuse 过滤器 → 转为只读 [`SealedSegment`]。
//! - 查找 newest-wins:先活跃段,再按 seq 从新到旧扫封存段,首个命中即返回。
//!   删除是"往活跃段写一条带 `FLAG_DELETED` 的新条目",不改旧段(契合不变性)。
//!
//! 段文件名 `{seq}.idx`;活跃段以 `sealed=0` 落盘、封存时原地改写为 `sealed=1`。

pub mod value;
mod segment;

use crossbeam_skiplist::SkipMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::types::StoreKey;

pub use value::IndexValue;

use segment::{read_segment_file, SealedSegment};

pub struct PersistentIndex {
    dir: PathBuf,
    max_entries_per_segment: usize,
    /// 活跃段:并发有序映射,写入即插入;无过滤器。
    active: SkipMap<StoreKey, IndexValue>,
    sealed: Vec<SealedSegment>,
    active_seq: u64,
    next_seq: u64,
}

impl PersistentIndex {
    /// 在空目录创建新索引。
    pub fn create(dir: impl AsRef<Path>, max_entries_per_segment: usize) -> Result<Self> {
        if max_entries_per_segment == 0 {
            return Err(Error::BadArgument("max_entries_per_segment must be > 0".into()));
        }
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            max_entries_per_segment,
            active: SkipMap::new(),
            active_seq: 0,
            sealed: Vec::new(),
            next_seq: 1,
        })
    }

    /// 打开已有索引:扫描 `{seq}.idx` + `{seq}.fuse` 恢复封存段与活跃段。
    pub fn open(dir: impl AsRef<Path>, max_entries_per_segment: usize) -> Result<Self> {
        if max_entries_per_segment == 0 {
            return Err(Error::BadArgument("max_entries_per_segment must be > 0".into()));
        }
        let dir = dir.as_ref().to_path_buf();

        let mut seqs: Vec<u64> = Vec::new();
        for ent in std::fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(name) = ent.file_name().to_str() {
                if let Some(seq) = name.strip_suffix(".idx").and_then(|s| s.parse::<u64>().ok()) {
                    seqs.push(seq);
                }
            }
        }
        seqs.sort_unstable();

        let mut sealed = Vec::new();
        let active = SkipMap::new();
        let mut active_seq = 0u64;
        let mut max_seq = 0u64;
        let mut has_active = false;

        // 识别活跃段:活跃段落盘时也是 `{seq}.idx`,但最后一个 seq 若是活跃则无 `.fuse`。
        // 约定:活跃段文件 `{seq}.idx` 但没有配套 `{seq}.fuse`。
        for seq in &seqs {
            max_seq = max_seq.max(*seq);
            let has_fuse = dir.join(format!("{seq}.fuse")).exists();
            if has_fuse {
                sealed.push(SealedSegment::open(&dir, *seq)?);
            } else {
                if has_active {
                    return Err(Error::Corrupt("multiple active (unsealed) index segments".into()));
                }
                has_active = true;
                active_seq = *seq;
                let loaded = read_segment_file(&dir.join(format!("{seq}.idx")))?;
                for (k, v) in loaded.entries {
                    active.insert(k, v);
                }
            }
        }
        // 按 seq 升序,保证 newest-wins 扫描从尾部(最新)开始。
        sealed.sort_by_key(|s| s.seq());

        let (active_seq, next_seq) = if has_active {
            (active_seq, max_seq + 1)
        } else {
            let s = max_seq + 1;
            (s, s + 1)
        };

        Ok(Self {
            dir,
            max_entries_per_segment,
            active,
            sealed,
            active_seq,
            next_seq,
        })
    }

    /// 写入一条索引。活跃段为并发映射;满阈值自动封存。
    pub fn add(&mut self, key: StoreKey, value: IndexValue) -> Result<()> {
        self.active.insert(key, value);
        if self.active.len() >= self.max_entries_per_segment {
            self.seal_active()?;
        }
        Ok(())
    }

    /// 查找 key 的最新值(newest-wins):活跃段 → 封存段(新→旧)。
    pub fn find(&self, key: &StoreKey) -> Option<IndexValue> {
        if let Some(v) = self.active.get(key) {
            return Some(*v.value());
        }
        for seg in self.sealed.iter().rev() {
            if let Some(v) = seg.find(key) {
                return Some(v);
            }
        }
        None
    }

    /// 持久化活跃段(以 `{seq}.idx` 落盘,无 `.fuse` 标识活跃),使重启可恢复。
    pub fn flush(&mut self) -> Result<()> {
        if self.active.is_empty() {
            return Ok(());
        }
        let entries: Vec<_> = self
            .active
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect();
        let path = self.dir.join(format!("{}.idx", self.active_seq));
        segment::write_segment_file(&path, &entries)
    }

    /// 封存当前活跃段:写 `.idx` + `.fuse`(mmap 载入)→ 起一个新活跃段。
    pub fn seal_active(&mut self) -> Result<()> {
        if self.active.is_empty() {
            return Ok(());
        }
        let entries: Vec<_> = self
            .active
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect();
        let sealed = SealedSegment::from_entries(&self.dir, self.active_seq, &entries)?;
        self.sealed.push(sealed);
        self.active.clear();

        self.active_seq = self.next_seq;
        self.next_seq += 1;
        Ok(())
    }

    pub fn sealed_segment_count(&self) -> usize {
        self.sealed.len()
    }

    pub fn active_len(&self) -> usize {
        self.active.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::value::FLAG_DELETED;
    use crate::types::LogOffset;

    fn temp_dir(tag: &str) -> PathBuf {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(format!("/var/tmp/ambry_idx_{tag}_{ns}"))
    }

    fn key(n: u64) -> StoreKey {
        let mut b = [0u8; 32];
        b[0..8].copy_from_slice(&n.to_le_bytes());
        // 让 hash64 有差异化输入
        b[24..32].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        StoreKey::new(b)
    }

    fn put(seg: u64, off: u64) -> IndexValue {
        IndexValue::put(LogOffset::new(seg, off), 100, 1)
    }

    #[test]
    fn add_and_find_active() {
        let dir = temp_dir("active");
        let mut idx = PersistentIndex::create(&dir, 1000).unwrap();
        idx.add(key(1), put(0, 0)).unwrap();
        idx.add(key(2), put(0, 100)).unwrap();
        assert_eq!(idx.find(&key(1)).unwrap().offset, LogOffset::new(0, 0));
        assert_eq!(idx.find(&key(2)).unwrap().offset, LogOffset::new(0, 100));
        assert!(idx.find(&key(999)).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn seals_and_finds_across_segments() {
        let dir = temp_dir("seal");
        let mut idx = PersistentIndex::create(&dir, 4).unwrap();
        for i in 0..10u64 {
            idx.add(key(i), put(0, i * 100)).unwrap();
        }
        assert!(idx.sealed_segment_count() >= 2, "should have sealed segments");
        for i in 0..10u64 {
            assert_eq!(idx.find(&key(i)).unwrap().offset, LogOffset::new(0, i * 100));
        }
        assert!(idx.find(&key(12345)).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn newest_wins_delete_after_put() {
        let dir = temp_dir("delete");
        let mut idx = PersistentIndex::create(&dir, 4).unwrap();
        idx.add(key(1), put(0, 0)).unwrap();
        for i in 2..6u64 {
            idx.add(key(i), put(0, i * 100)).unwrap(); // 触发封存,key(1) 进 sealed
        }
        assert!(!idx.find(&key(1)).unwrap().is_deleted());

        // 删除:往活跃段写带 FLAG_DELETED 的新条目
        let mut del = put(0, 9999);
        del.flags = FLAG_DELETED;
        idx.add(key(1), del).unwrap();
        assert!(idx.find(&key(1)).unwrap().is_deleted(), "delete must win");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_recovers_sealed_and_active() {
        let dir = temp_dir("reopen");
        {
            let mut idx = PersistentIndex::create(&dir, 4).unwrap();
            for i in 0..10u64 {
                idx.add(key(i), put(0, i * 100)).unwrap();
            }
            idx.flush().unwrap(); // 持久化活跃段
        }
        let idx = PersistentIndex::open(&dir, 4).unwrap();
        for i in 0..10u64 {
            assert_eq!(
                idx.find(&key(i)).unwrap().offset,
                LogOffset::new(0, i * 100),
                "key {i} lost after reopen"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
