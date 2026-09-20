//! `Log`:一个 partition-replica 的数据日志,由若干只追加的 [`LogSegment`] 组成。
//!
//! 写满当前活跃段则滚动(rollover)到新段;跨段读取按 [`LogOffset`] 定位。
//! 与 Ambry 一致:总容量必须是段容量的整数倍;单条记录不能超过一个段的数据容量。

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::log_segment::LogSegment;
use crate::types::{LogOffset, LogSegmentName};

struct Entry {
    position: u64,
    segment: LogSegment,
}

/// 多段日志。非并发安全:单写者(`&mut self`)。
pub struct Log {
    dir: PathBuf,
    total_capacity: u64,
    segment_capacity: u64,
    max_segments: u64,
    entries: Vec<Entry>,
}

impl Log {
    /// 在空目录创建新日志,并预建第一个段。
    pub fn create(dir: impl AsRef<Path>, total_capacity: u64, segment_capacity: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        validate_capacities(total_capacity, segment_capacity)?;
        std::fs::create_dir_all(&dir)?;

        let mut log = Self {
            dir,
            total_capacity,
            segment_capacity,
            max_segments: total_capacity / segment_capacity,
            entries: Vec::new(),
        };
        log.add_segment(0)?;
        Ok(log)
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
            let segment = LogSegment::open(&path)?;
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
            entries,
        })
    }

    /// 追加一条记录,返回其全局 [`LogOffset`]。段放不下则先 flush 旧段再滚动到新段。
    pub fn append(&mut self, record: &[u8]) -> Result<LogOffset> {
        let need = record.len() as u64;
        let seg_data_cap = self.active().segment.data_capacity();
        if need > seg_data_cap {
            return Err(Error::BadArgument(format!(
                "record size {need} exceeds segment data capacity {seg_data_cap}"
            )));
        }

        if self.active().segment.remaining() < need {
            self.roll_over()?;
        }

        let position = self.active().position;
        let offset = self.active_mut().segment.append(record)?;
        Ok(LogOffset::new(position, offset))
    }

    /// 读取 `at` 起 `len` 字节(不得跨段)。
    pub fn read(&self, at: LogOffset, len: usize) -> Result<Vec<u8>> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.position == at.segment)
            .ok_or(Error::OutOfRange {
                offset: at.offset,
                len,
                data_end: 0,
            })?;
        entry.segment.read(at.offset, len)
    }

    /// 持久化活跃段(已滚动的旧段在滚动时已 flush)。
    pub fn flush(&mut self) -> Result<()> {
        self.active_mut().segment.flush()
    }

    /// 当前写入位置。
    pub fn end_offset(&self) -> LogOffset {
        let e = self.active();
        LogOffset::new(e.position, e.segment.data_end())
    }

    pub fn segment_count(&self) -> usize {
        self.entries.len()
    }

    /// 所有段的 `(position, data_end)`,按位置升序。供上层顺序扫描全部记录用。
    pub fn segment_spans(&self) -> Vec<(u64, u64)> {
        self.entries
            .iter()
            .map(|e| (e.position, e.segment.data_end()))
            .collect()
    }

    pub fn total_capacity(&self) -> u64 {
        self.total_capacity
    }

    /// 关闭全部段。
    pub fn close(self) -> Result<()> {
        for e in self.entries {
            e.segment.close()?;
        }
        Ok(())
    }

    fn active(&self) -> &Entry {
        self.entries
            .last()
            .expect("log always has an active segment")
    }

    fn active_mut(&mut self) -> &mut Entry {
        self.entries
            .last_mut()
            .expect("log always has an active segment")
    }

    fn roll_over(&mut self) -> Result<()> {
        if self.entries.len() as u64 >= self.max_segments {
            return Err(Error::SegmentFull);
        }
        self.active_mut().segment.flush()?;
        let next = self.active().position + 1;
        self.add_segment(next)
    }

    fn add_segment(&mut self, position: u64) -> Result<()> {
        let path = self.dir.join(LogSegmentName(position).filename());
        let segment = LogSegment::create(&path, self.segment_capacity)?;
        self.entries.push(Entry { position, segment });
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
        let mut log = Log::create(&dir, seg * 3, seg).unwrap();

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
        let mut log = Log::create(&dir, seg * 2, seg).unwrap();

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
        let mut log = Log::create(&dir, seg * 2, seg).unwrap();
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
            let mut log = Log::create(&dir, seg * 4, seg).unwrap();
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
