//! `Log`:一个 partition-replica 的数据日志,由若干只追加的 [`LogSegment`] 组成。
//!
//! 写满当前活跃段则滚动(rollover)到新段;跨段读取按 [`LogOffset`] 定位。
//! 与 Ambry 一致:总容量必须是段容量的整数倍;单条记录不能超过一个段的数据容量。

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};
use crate::log_segment::LogSegment;
use crate::types::{LogOffset, LogSegmentName};

struct Entry {
    position: u64,
    segment: Arc<LogSegment>,
}

/// 多段日志。并发读安全 + 单写者:写者由上层 `write_lock` 串行化,`entries` 用 `RwLock`
/// 仅在滚动(push 新段)时取写锁、读/定位时取读锁(纳秒级,克隆 `Arc<LogSegment>` 后即释放)。
/// 段内数据的读写可见性由 [`LogSegment`] 的 `AtomicU64` data_end 保证,故读者全程不阻塞写者的追加。
pub struct Log {
    dir: PathBuf,
    total_capacity: u64,
    segment_capacity: u64,
    max_segments: u64,
    entries: RwLock<Vec<Entry>>,
}

impl Log {
    /// 在空目录创建新日志,并预建第一个段。
    pub fn create(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        validate_capacities(total_capacity, segment_capacity)?;
        std::fs::create_dir_all(&dir)?;

        let path = dir.join(LogSegmentName(0).filename());
        let segment = Arc::new(LogSegment::create(&path, segment_capacity)?);
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_segments: total_capacity / segment_capacity,
            entries: RwLock::new(vec![Entry {
                position: 0,
                segment,
            }]),
        })
    }

    /// 打开已有日志,扫描目录里的段文件并按位置排序恢复。
    pub fn open(dir: impl AsRef<Path>, total_capacity: u64, segment_capacity: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        validate_capacities(total_capacity, segment_capacity)?;

        let mut positions: Vec<u64> = Vec::new();
        for ent in std::fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(name) = ent.file_name().to_str() {
                if let Some(seg) = LogSegmentName::parse(name) {
                    positions.push(seg.0);
                }
            }
        }
        positions.sort_unstable();

        if positions.is_empty() {
            return Err(Error::Corrupt(format!(
                "no log segments found in {}",
                dir.display()
            )));
        }

        let mut entries = Vec::with_capacity(positions.len());
        for pos in positions {
            let path = dir.join(LogSegmentName(pos).filename());
            let segment = Arc::new(LogSegment::open(&path)?);
            entries.push(Entry {
                position: pos,
                segment,
            });
        }

        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_segments: total_capacity / segment_capacity,
            entries: RwLock::new(entries),
        })
    }

    /// 追加一条记录,返回其全局 [`LogOffset`]。段放不下则先 flush 旧段再滚动到新段。
    ///
    /// 由上层 `write_lock` 保证单写者:活跃段克隆出 `Arc` 后释放 `entries` 读锁再走 `&self` 追加,
    /// 不长时间占锁,读者(read/segment_spans)不被阻塞。
    pub fn append(&self, record: &[u8]) -> Result<LogOffset> {
        let need = record.len() as u64;
        let (position, segment) = self.active_segment();
        let seg_data_cap = segment.data_capacity();
        if need > seg_data_cap {
            return Err(Error::BadArgument(format!(
                "record size {need} exceeds segment data capacity {seg_data_cap}"
            )));
        }

        let (position, segment) = if segment.remaining() < need {
            self.roll_over()?;
            self.active_segment()
        } else {
            (position, segment)
        };
        let offset = segment.append(record)?;
        Ok(LogOffset::new(position, offset))
    }

    /// 读取 `at` 起 `len` 字节(不得跨段)。
    pub fn read(&self, at: LogOffset, len: usize) -> Result<Vec<u8>> {
        let segment = self.segment_at(at.segment).ok_or(Error::OutOfRange {
            offset: at.offset,
            len,
            data_end: 0,
        })?;
        segment.read(at.offset, len)
    }

    /// 就地覆盖 `at` 起的已写字节(不改 data_end,不得跨段)。用于硬删除原地擦除。
    pub fn overwrite_at(&self, at: LogOffset, bytes: &[u8]) -> Result<()> {
        let segment = self.segment_at(at.segment).ok_or(Error::OutOfRange {
            offset: at.offset,
            len: bytes.len(),
            data_end: 0,
        })?;
        segment.overwrite_at(at.offset, bytes)
    }

    /// 持久化所有段(硬删除擦除多段后统一 fsync)。
    pub fn flush_all(&self) -> Result<()> {
        let segments: Vec<Arc<LogSegment>> = {
            let entries = self.entries.read().expect("log entries poisoned");
            entries.iter().map(|e| e.segment.clone()).collect()
        };
        for s in segments {
            s.flush()?;
        }
        Ok(())
    }

    /// 持久化活跃段(已滚动的旧段在滚动时已 flush)。
    pub fn flush(&self) -> Result<()> {
        let (_, segment) = self.active_segment();
        segment.flush()
    }

    /// 当前写入位置。
    pub fn end_offset(&self) -> LogOffset {
        let (position, segment) = self.active_segment();
        LogOffset::new(position, segment.data_end())
    }

    pub fn segment_count(&self) -> usize {
        self.entries.read().expect("log entries poisoned").len()
    }

    /// 所有段的 `(position, data_end)`,按位置升序。供上层顺序扫描全部记录用。
    pub fn segment_spans(&self) -> Vec<(u64, u64)> {
        self.entries
            .read()
            .expect("log entries poisoned")
            .iter()
            .map(|e| (e.position, e.segment.data_end()))
            .collect()
    }

    pub fn total_capacity(&self) -> u64 {
        self.total_capacity
    }

    /// 关闭全部段(消费 self;残余 `Arc` 引用由读者持有,末次释放时才关文件)。
    pub fn close(self) -> Result<()> {
        let entries = self.entries.into_inner().expect("log entries poisoned");
        for e in entries {
            if let Ok(seg) = Arc::try_unwrap(e.segment) {
                seg.close()?;
            }
        }
        Ok(())
    }

    fn active_segment(&self) -> (u64, Arc<LogSegment>) {
        let entries = self.entries.read().expect("log entries poisoned");
        let e = entries.last().expect("log always has an active segment");
        (e.position, e.segment.clone())
    }

    fn segment_at(&self, position: u64) -> Option<Arc<LogSegment>> {
        let entries = self.entries.read().expect("log entries poisoned");
        entries
            .iter()
            .find(|e| e.position == position)
            .map(|e| e.segment.clone())
    }

    fn roll_over(&self) -> Result<()> {
        let mut entries = self.entries.write().expect("log entries poisoned");
        if entries.len() as u64 >= self.max_segments {
            return Err(Error::SegmentFull);
        }
        let active = entries.last().expect("log always has an active segment");
        active.segment.flush()?;
        let next = active.position + 1;
        let path = self.dir.join(LogSegmentName(next).filename());
        let segment = Arc::new(LogSegment::create(&path, self.segment_capacity)?);
        entries.push(Entry {
            position: next,
            segment,
        });
        Ok(())
    }
}

fn validate_capacities(total: u64, segment: u64) -> Result<()> {
    if segment == 0 || total == 0 {
        return Err(Error::BadArgument("capacities must be positive".into()));
    }
    if !total.is_multiple_of(segment) {
        return Err(Error::BadArgument(format!(
            "total capacity {total} must be a multiple of segment capacity {segment}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK: u64 = 1024;
    const HEADER: u64 = 64;

    fn temp_dir(tag: &str) -> PathBuf {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(format!("/var/tmp/ambry_log_{tag}_{ns}"))
    }

    fn seg_cap(blocks: u64) -> u64 {
        HEADER + blocks * BLOCK
    }

    #[test]
    fn rollover_across_segments() {
        let dir = temp_dir("roll");
        let seg = seg_cap(2);
        let log = Log::create(&dir, seg * 3, seg).unwrap();

        let rec = vec![9u8; BLOCK as usize + 100];
        let o1 = log.append(&rec).unwrap();
        let o2 = log.append(&rec).unwrap();
        assert_eq!(o1.segment, 0);
        assert_eq!(o2.segment, 1, "should roll to a new segment");
        assert_eq!(log.segment_count(), 2);

        log.flush().unwrap();
        assert_eq!(log.read(o1, rec.len()).unwrap(), rec);
        assert_eq!(log.read(o2, rec.len()).unwrap(), rec);

        log.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_full_when_all_segments_exhausted() {
        let dir = temp_dir("full");
        let seg = seg_cap(1);
        let log = Log::create(&dir, seg * 2, seg).unwrap();

        let rec = vec![1u8; BLOCK as usize];
        log.append(&rec).unwrap();
        log.append(&rec).unwrap();
        match log.append(&rec) {
            Err(Error::SegmentFull) => {}
            other => panic!("expected SegmentFull, got {other:?}"),
        }
        log.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_too_large_for_segment() {
        let dir = temp_dir("toobig");
        let seg = seg_cap(2);
        let log = Log::create(&dir, seg * 2, seg).unwrap();
        let rec = vec![0u8; (2 * BLOCK) as usize + 1];
        match log.append(&rec) {
            Err(Error::BadArgument(_)) => {}
            other => panic!("expected BadArgument, got {other:?}"),
        }
        log.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_recovers_all_segments() {
        let dir = temp_dir("reopen");
        let seg = seg_cap(2);
        let rec = vec![5u8; BLOCK as usize + 50];
        let (o1, o2, end);
        {
            let log = Log::create(&dir, seg * 4, seg).unwrap();
            o1 = log.append(&rec).unwrap();
            o2 = log.append(&rec).unwrap();
            log.flush().unwrap();
            end = log.end_offset();
            log.close().unwrap();
        }
        let log = Log::open(&dir, seg * 4, seg).unwrap();
        assert_eq!(log.segment_count(), 2);
        assert_eq!(log.end_offset(), end);
        assert_eq!(log.read(o1, rec.len()).unwrap(), rec);
        assert_eq!(log.read(o2, rec.len()).unwrap(), rec);
        log.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
