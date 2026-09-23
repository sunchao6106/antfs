//! `PersistentIndex`:key → [`IndexValue`] 的分段索引(同步 API,索引文件走 std::fs)。
//!
//! - 活跃段:内存有序 `BTreeMap`,写入即插入;无过滤器(直接查 map)。
//! - 封存段:活跃段达阈值 → 排序落盘 + 构建 Binary Fuse 过滤器 → 转为只读 [`SealedSegment`]。
//! - 查找 newest-wins:先活跃段,再按 seq 从新到旧扫封存段,首个命中即返回。
//!   删除是"往活跃段写一条带 `FLAG_DELETED` 的新条目",不改旧段(契合不变性)。
//!
//! 段文件名 `{seq}.idx`;活跃段以 `sealed=0` 落盘、封存时原地改写为 `sealed=1`。

mod journal;
mod segment;
pub mod value;

use crossbeam_skiplist::SkipMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use crate::error::{Error, Result};
use crate::types::{LogOffset, StoreKey};

pub use value::IndexValue;

use journal::Journal;
use segment::{read_segment_file, SealedSegment};

/// journal 容量相对 `max_entries_per_segment` 的倍数:journal 覆盖「最近若干段」的复制拉取窗口。
/// 暂以段大小的倍数近似(对齐 Ambry 独立配置的 journal 尺寸,待专门配置项时再解耦)。
const JOURNAL_CAPACITY_FACTOR: usize = 4;

pub struct PersistentIndex {
    dir: PathBuf,
    max_entries_per_segment: usize,
    /// 活跃段:并发有序映射,写入即插入;无过滤器。
    active: SkipMap<StoreKey, IndexValue>,
    /// 封存段:`RwLock` 保护(读锁扫描、封存时写锁 push);内部可变以支持 `&self` 写路径。
    sealed: RwLock<Vec<SealedSegment>>,
    /// 近期记录的 offset→key 游标(复制增量拉取用);写路径 `add` 喂入,`&self` 下用 `Mutex`。
    journal: Mutex<Journal>,
    active_seq: AtomicU64,
    next_seq: AtomicU64,
}

impl PersistentIndex {
    /// 在空目录创建新索引。
    pub fn create(dir: impl AsRef<Path>, max_entries_per_segment: usize) -> Result<Self> {
        if max_entries_per_segment == 0 {
            return Err(Error::BadArgument(
                "max_entries_per_segment must be > 0".into(),
            ));
        }
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            max_entries_per_segment,
            active: SkipMap::new(),
            active_seq: AtomicU64::new(0),
            sealed: RwLock::new(Vec::new()),
            journal: Mutex::new(Journal::new(
                max_entries_per_segment.saturating_mul(JOURNAL_CAPACITY_FACTOR),
            )),
            next_seq: AtomicU64::new(1),
        })
    }

    /// 打开已有索引:扫描 `{seq}.idx` + `{seq}.fuse` 恢复封存段与活跃段。
    pub fn open(dir: impl AsRef<Path>, max_entries_per_segment: usize) -> Result<Self> {
        if max_entries_per_segment == 0 {
            return Err(Error::BadArgument(
                "max_entries_per_segment must be > 0".into(),
            ));
        }
        let dir = dir.as_ref().to_path_buf();

        let mut seqs: Vec<u64> = Vec::new();
        for ent in std::fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(name) = ent.file_name().to_str() {
                if let Some(seq) = name
                    .strip_suffix(".idx")
                    .and_then(|s| s.parse::<u64>().ok())
                {
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
                    return Err(Error::Corrupt(
                        "multiple active (unsealed) index segments".into(),
                    ));
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

        // 重建 journal:从活跃段(最新一段、已在内存)喂入近期 offset→key;更旧的封存段不触碰
        // (保持 mmap 惰性),复制若拉取到更旧游标则回退扫 log。
        let mut journal =
            Journal::new(max_entries_per_segment.saturating_mul(JOURNAL_CAPACITY_FACTOR));
        for e in active.iter() {
            journal.add(e.value().offset, *e.key());
        }

        Ok(Self {
            dir,
            max_entries_per_segment,
            active,
            sealed: RwLock::new(sealed),
            journal: Mutex::new(journal),
            active_seq: AtomicU64::new(active_seq),
            next_seq: AtomicU64::new(next_seq),
        })
    }

    /// 写入一条索引。活跃段为并发映射;满阈值自动封存。同时喂入 journal(复制游标)。
    pub fn add(&self, key: StoreKey, value: IndexValue) -> Result<()> {
        self.active.insert(key, value);
        self.journal
            .lock()
            .expect("index journal poisoned")
            .add(value.offset, key);
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
        for seg in self.sealed.read().unwrap().iter().rev() {
            if let Some(v) = seg.find(key) {
                return Some(v);
            }
        }
        None
    }

    /// 持久化活跃段(以 `{seq}.idx` 落盘,无 `.fuse` 标识活跃),使重启可恢复。
    pub fn flush(&self) -> Result<()> {
        if self.active.is_empty() {
            return Ok(());
        }
        let entries: Vec<_> = self.active.iter().map(|e| (*e.key(), *e.value())).collect();
        let path = self
            .dir
            .join(format!("{}.idx", self.active_seq.load(Ordering::Acquire)));
        segment::write_segment_file(&path, &entries)
    }

    /// 封存当前活跃段:写 `.idx` + `.fuse`(mmap 载入)→ 起一个新活跃段。
    ///
    /// 顺序关键:**先把封存段 push 进 sealed(写锁)再 clear 活跃段**,保证并发读者不会在
    /// 「活跃段已清、封存段未入」的窗口丢失条目(D4 无锁读的正确性前提)。
    pub fn seal_active(&self) -> Result<()> {
        if self.active.is_empty() {
            return Ok(());
        }
        let entries: Vec<_> = self.active.iter().map(|e| (*e.key(), *e.value())).collect();
        let active_seq = self.active_seq.load(Ordering::Acquire);
        let sealed = SealedSegment::from_entries(&self.dir, active_seq, &entries)?;
        self.sealed.write().unwrap().push(sealed);
        self.active.clear();

        let new_active = self.next_seq.fetch_add(1, Ordering::AcqRel);
        self.active_seq.store(new_active, Ordering::Release);
        Ok(())
    }

    /// 遍历全部索引条目(活跃 SkipMap + 各封存段的升序条目),供只读统计/摘要用。
    ///
    /// 只走内存索引与 mmap 条目(**不读数据体**),故开销与 key 数成正比、与数据量无关。
    pub fn for_each(&self, mut f: impl FnMut(&StoreKey, IndexValue)) {
        for e in self.active.iter() {
            f(e.key(), *e.value());
        }
        for seg in self.sealed.read().unwrap().iter() {
            seg.for_each_entry(&mut f);
        }
    }

    pub fn sealed_segment_count(&self) -> usize {
        self.sealed.read().unwrap().len()
    }

    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    /// journal 覆盖的最旧 offset(无则 `None`);供 `find_entries_since` 判断能否走 journal 快路径。
    pub fn journal_first_offset(&self) -> Option<LogOffset> {
        self.journal
            .lock()
            .expect("index journal poisoned")
            .first_offset()
    }

    /// journal 中 offset ≥ `from` 的 `(offset, key)`(按序);`from` 越出 journal 覆盖范围返回 `None`
    /// ——调用方回退扫 log。见 [`journal::Journal::entries_since`]。
    pub fn journal_entries_since(
        &self,
        from: LogOffset,
        inclusive: bool,
    ) -> Option<Vec<(LogOffset, StoreKey)>> {
        self.journal
            .lock()
            .expect("index journal poisoned")
            .entries_since(from, inclusive)
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
        StoreKey::new(n.to_be_bytes())
    }

    fn put(seg: u64, off: u64) -> IndexValue {
        IndexValue::put(LogOffset::new(seg, off), 100)
    }

    #[test]
    fn add_and_find_active() {
        let dir = temp_dir("active");
        let idx = PersistentIndex::create(&dir, 1000).unwrap();
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
        let idx = PersistentIndex::create(&dir, 4).unwrap();
        for i in 0..10u64 {
            idx.add(key(i), put(0, i * 100)).unwrap();
        }
        assert!(
            idx.sealed_segment_count() >= 2,
            "should have sealed segments"
        );
        for i in 0..10u64 {
            assert_eq!(
                idx.find(&key(i)).unwrap().offset,
                LogOffset::new(0, i * 100)
            );
        }
        assert!(idx.find(&key(12345)).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn newest_wins_delete_after_put() {
        let dir = temp_dir("delete");
        let idx = PersistentIndex::create(&dir, 4).unwrap();
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
            let idx = PersistentIndex::create(&dir, 4).unwrap();
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
