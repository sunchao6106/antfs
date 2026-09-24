//! `Log`:一个 partition-replica 的数据日志,由若干只追加的 [`LogSegment`] 组成。
//!
//! 写满当前活跃段则滚动(rollover)到新段;跨段读取按 [`LogOffset`] 定位。
//!
//! 两条容量规则:
//! - **单条记录不得跨段**,且不得超过一个段的数据容量(对齐 Ambry "buffer 写进单个 log segment");
//! - **`total` 允许不是 `segment` 的整数倍**:最后一段是"残段"(容量 = `total − k × segment`),
//!   装不下任何记录时该部分空间视为写满(`SegmentFull` → `StoreFull`)。这样配置项之间解耦
//!   (100 GiB 分区 + 3 GiB 段 = 33 整段 + 1 个 1 GiB 残段),`total < segment` 也退化为单段。

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};
use crate::log_segment::LogSegment;
use crate::types::{LogOffset, LogSegmentName};

struct Entry {
    /// 段在本 log 内的位置序号(逻辑顺序、残段容量计算用)。
    position: u64,
    /// 段的**代次**(见 [`LogSegmentName`]):同一目录内单调递增,跨全库 compact 继续递增,
    /// 故"段身份 = (position, generation)"在整个副本历史上永不复用。
    generation: u64,
    segment: Arc<LogSegment>,
}

/// 段身份(precise:位置 + 代次)。文件名即 `{position}_{generation}_log.seg`。
fn seg_name(position: u64, generation: u64) -> LogSegmentName {
    LogSegmentName::new(position, generation)
}

/// 已预留但尚未发布的连续写入区间(用于**长度已知的流式记录**)。
///
/// [`Log::reserve`] 产出:先在活跃段内占好 `total` 字节,调用方分块写入(可多次
/// [`write`](Self::write)),最后 [`commit`](Self::commit) 一次性推进 `data_end` 使其对读者可见。
/// 中途放弃(如上游读流出错)不发布任何内容:该区间仍是"未写"状态,后续追加覆盖即可,
/// 读者与恢复扫描都看不到(恢复只认 header 里上一次 flush 的 `data_end`)。
pub struct Reserved {
    position: u64,
    start: u64,
    total: u64,
    segment: Arc<LogSegment>,
}

impl Reserved {
    /// 写预留区内相对 `off` 处的字节(可多次调用,分块写入)。
    pub fn write(&self, off: u64, buf: &[u8]) -> Result<()> {
        self.segment.write_reserved(self.start, off, buf)
    }

    /// 发布整段(一次性推进 `data_end`),返回该记录的全局 [`LogOffset`]。
    pub fn commit(self) -> Result<LogOffset> {
        self.segment.commit_reserved(self.start, self.total)?;
        Ok(LogOffset::new(self.position, self.start))
    }
}

/// 多段日志。并发读安全 + 单写者:写者由上层 `write_lock` 串行化,`entries` 用 `RwLock`
/// 仅在滚动(push 新段)时取写锁、读/定位时取读锁(纳秒级,克隆 `Arc<LogSegment>` 后即释放)。
/// 段内数据的读写可见性由 [`LogSegment`] 的 `AtomicU64` data_end 保证,故读者全程不阻塞写者的追加。
pub struct Log {
    dir: PathBuf,
    total_capacity: u64,
    segment_capacity: u64,
    max_segments: u64,
    /// 本 log 所属副本的 key 前缀(blobId 高 32bit = partitionId)。写入每个段头,
    /// 段内记录只存 key 的低 32bit 后缀(见 `blob.rs` 记录布局)。
    key_prefix: u32,
    entries: RwLock<Vec<Entry>>,
}

impl Log {
    /// 段身份表(除活跃段外)的 `(段身份, data_end)`,按位置升序。
    ///
    /// 物 bootstrap(`install_bootstrap`)按它逐段拷;段身份里的**代次**让"对端在拷贝途
    /// 重写了 log"也能被认出来(同位置、不同代次)。
    pub fn sealed_segment_names(&self) -> Vec<(LogSegmentName, u64)> {
        let entries = self.entries.read().expect("log entries poisoned");
        let n = entries.len();
        entries
            .iter()
            .take(n.saturating_sub(1))
            .map(|e| {
                (
                    LogSegmentName::new(e.position, e.generation),
                    e.segment.data_end(),
                )
            })
            .collect()
    }

    /// 某个位置上的段的**代次**;该位置当前没有段 → `None`。
    ///
    /// 复制侧"段号被复用"的判据:位置还在、代次不同 ⇒ 不是同一个段(全库 compact 后段号会从 0 重来)。
    pub fn generation_of(&self, position: u64) -> Option<u32> {
        self.entries
            .read()
            .expect("log entries poisoned")
            .iter()
            .find(|e| e.position == position)
            .map(|e| u32::try_from(e.generation).expect("generation fits u32"))
    }

    /// 本 log 用过的最大代次(全库 compact 时新目录要接着往后取)。
    pub fn max_generation(&self) -> u64 {
        self.entries
            .read()
            .expect("log entries poisoned")
            .iter()
            .map(|e| e.generation)
            .max()
            .unwrap_or(0)
    }

    /// 在空目录创建新日志,并预建第一个段。`key_prefix` = blobId 高 32bit(partitionId)。
    ///
    /// `first_generation` = 首段的代次:新副本给 1;**全库 compact 给"旧 log 的最大代次 + 1"**
    /// —— 这样新目录里的段身份绝不会与旧目录撞(段位置会从 0 重来,但代次接着涨)。
    pub fn create(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        key_prefix: u32,
        first_generation: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        validate_capacities(total_capacity, segment_capacity)?;
        assert!(first_generation >= 1, "段代次从 1 起(0 不是合法代次)");
        std::fs::create_dir_all(&dir)?;

        // 首段容量 = min(段大小, 总容量):`total < segment` 时退化为单段(容量即 total)。
        let first_capacity = segment_capacity.min(total_capacity);
        let path = dir.join(seg_name(0, first_generation).filename());
        let segment = Arc::new(LogSegment::create(
            &path,
            first_capacity,
            key_prefix,
            u32::try_from(first_generation).expect("generation must fit u32"),
        )?);
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_segments: total_capacity.div_ceil(segment_capacity),
            key_prefix,
            entries: RwLock::new(vec![Entry {
                position: 0,
                generation: first_generation,
                segment,
            }]),
        })
    }

    /// 打开已有日志,扫描目录里的段文件并按位置排序恢复。
    ///
    /// key 前缀由**段头**读出(log 自描述其 partition),并校验各段一致 —— 段头被改坏或
    /// 段文件来自别的 partition 时报 [`Corrupt`](Error::Corrupt),不静默按某一个前缀解释数据。
    ///
    /// 段身份(位置 + 代次)从**段名**解析,代次与段头自述的值必须一致(不一致 = 段文件被换过或
    /// 被改坏 → [`Corrupt`](Error::Corrupt));打开的段表即"本 log 当前有哪些段身份"的权威。
    /// 副本级身份(incarnation)不在这里 —— 它由 `BlobStore` 从 `LOG_INCARNATION` 管理(见
    /// `BlobStore::incarnation`),**刻意不从段头取任何东西**。`LogSegment::created_ns` 只描述"段文件
    /// 是在哪台机器、什么时候诞生的",物理拷贝后它仍是**对端**的时刻,拿它当身份会与对端撞号。
    pub fn open(dir: impl AsRef<Path>, total_capacity: u64, segment_capacity: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        validate_capacities(total_capacity, segment_capacity)?;

        let mut names: Vec<LogSegmentName> = Vec::new();
        for ent in std::fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(name) = ent.file_name().to_str() {
                if let Some(seg) = LogSegmentName::parse(name) {
                    names.push(seg);
                }
            }
        }
        // 段顺序 = 位置序号升序(位置在同一目录内唯一);代次只做身份,不参与排序。
        names.sort_by_key(|n| n.position);

        if names.is_empty() {
            return Err(Error::Corrupt(format!(
                "no log segments found in {}",
                dir.display()
            )));
        }

        let mut entries = Vec::with_capacity(names.len());
        let mut key_prefix: Option<u32> = None;
        for name in names {
            let path = dir.join(name.filename());
            let segment = Arc::new(LogSegment::open(&path)?);
            // 段头自述的代次必须与文件名一致:不一致说明段文件被替换/名字被改过 → 大声失败。
            if segment.generation() != name.generation {
                return Err(Error::Corrupt(format!(
                    "segment {} header says generation {} but the file name says {}",
                    name.position,
                    segment.generation(),
                    name.generation
                )));
            }
            match key_prefix {
                None => key_prefix = Some(segment.key_prefix()),
                Some(p) if p != segment.key_prefix() => {
                    return Err(Error::Corrupt(format!(
                        "segment {} key prefix {:#x} disagrees with {p:#x}",
                        name.position,
                        segment.key_prefix()
                    )));
                }
                Some(_) => {}
            }
            entries.push(Entry {
                position: name.position as u64,
                generation: name.generation as u64,
                segment,
            });
        }

        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_segments: total_capacity.div_ceil(segment_capacity),
            key_prefix: key_prefix.expect("at least one segment"),
            entries: RwLock::new(entries),
        })
    }

    /// 本 log 的 key 前缀(blobId 高 32bit)。记录解码时用它补全 key。
    #[inline]
    pub fn key_prefix(&self) -> u32 {
        self.key_prefix
    }

    /// 追加一条记录,返回其全局 [`LogOffset`]。段放不下则先 flush 旧段再滚动到新段。
    ///
    /// 由上层 `write_lock` 保证单写者:活跃段克隆出 `Arc` 后释放 `entries` 读锁再走 `&self` 追加,
    /// 不长时间占锁,读者(read/segment_spans)不被阻塞。
    pub fn append(&self, record: &[u8]) -> Result<LogOffset> {
        let need = record.len() as u64;
        // "超过任何段"才算客户端/配置问题(→ 上层 400);**残段装不下**属"写满"(→ 上层 507),
        // 由下面的 `LogSegment::append` 报 `SegmentFull` 表达,不在这里误报参数错。
        let max_record_cap = self.max_segment_capacity() - LogSegment::header_size();
        if need > max_record_cap {
            return Err(Error::BadArgument(format!(
                "record size {need} exceeds the largest segment's data capacity {max_record_cap}"
            )));
        }

        let (position, segment) = self.active_segment();
        let (position, segment) = if segment.remaining() < need {
            self.roll_over()?;
            self.active_segment()
        } else {
            (position, segment)
        };
        let offset = segment.append(record)?;
        Ok(LogOffset::new(position, offset))
    }

    /// 预留一段连续空间以**流式写入**一条长度已知的记录,返回 [`Reserved`]。
    ///
    /// 放不下/跨段的判定与 [`append`](Self::append) 完全一致(单条记录不得跨段;活跃段剩余不足则
    /// 先 flush 旧段再滚动)。区别是**不立即发布** `data_end` —— 预留区间在 [`Reserved::commit`]
    /// 之前对读者与 [`segment_spans`](Self::segment_spans) 扫描都不可见。写者由上层 `write_lock`
    /// 串行,故预留期间不会有别的预留/追加插入。
    pub fn reserve(&self, total: u64) -> Result<Reserved> {
        // 与 `append` 同一判定:超过"任何段"才是参数错;残段装不下走 `SegmentFull`(→ StoreFull)。
        let max_record_cap = self.max_segment_capacity() - LogSegment::header_size();
        if total > max_record_cap {
            return Err(Error::BadArgument(format!(
                "record size {total} exceeds the largest segment's data capacity {max_record_cap}"
            )));
        }

        let (position, segment) = self.active_segment();
        let (position, segment) = if segment.remaining() < total {
            self.roll_over()?;
            self.active_segment()
        } else {
            (position, segment)
        };
        let start = segment.reserve(total)?;
        Ok(Reserved {
            position,
            start,
            total,
            segment,
        })
    }

    /// 本 log 里单个段能有的**最大物理容量** = `min(段大小, 总容量)`
    /// (`total < segment` 时只有一段,其容量就是总容量)。
    fn max_segment_capacity(&self) -> u64 {
        self.segment_capacity.min(self.total_capacity)
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

    /// 封存段(除活跃/最高位段外)的 `(position, data_end)`,升序。S3b 物理拷贝只拷这些(不可变)。
    pub fn sealed_spans(&self) -> Vec<(u64, u64)> {
        let entries = self.entries.read().expect("log entries poisoned");
        let n = entries.len();
        entries
            .iter()
            .take(n.saturating_sub(1))
            .map(|e| (e.position, e.segment.data_end()))
            .collect()
    }

    /// 读某段的原始前缀(header + 有效数据),供 S3b 物理拷贝。
    ///
    /// 按**段身份**(位置 + 代次)找段:同位置不同代次 = "你要的那段已经被 compaction 重写了",
    /// 一律按"没有这段"报错(取段方据此丢弃拷贝回落逻辑拉取,见 `bootstrap_from_peer`)。
    pub fn read_segment_prefix(&self, name: LogSegmentName) -> Result<Vec<u8>> {
        self.segment_by_name(name)
            .ok_or_else(|| Error::BadArgument(format!("no such segment {name}")))?
            .raw_prefix()
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

    /// 按**位置**找段(内部读路径:索引值只存位置,且段表本身权威)。
    fn segment_at(&self, position: u64) -> Option<Arc<LogSegment>> {
        let entries = self.entries.read().expect("log entries poisoned");
        entries
            .iter()
            .find(|e| e.position == position)
            .map(|e| e.segment.clone())
    }

    /// 按**段身份**(位置 + 代次)找段(对外的段服务路径:同位置不同代次 = 你要的那段已被重写)。
    fn segment_by_name(&self, name: LogSegmentName) -> Option<Arc<LogSegment>> {
        let entries = self.entries.read().expect("log entries poisoned");
        entries
            .iter()
            .find(|e| {
                e.position == u64::from(name.position) && e.segment.generation() == name.generation
            })
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
        // 代次:同一目录内单调递增,永不复用(段身份 = 位置 + 代次)。
        let next_gen = entries.iter().map(|e| e.generation).max().unwrap_or(0) + 1;
        // **残段**:第 k 段占声明容量的 [k×segment, min((k+1)×segment, total))。
        // 只有最后一段可能不满(前面各段都是满的),故 `consumed = next × segment`。
        let capacity = self.segment_capacity.min(
            self.total_capacity
                .saturating_sub(next.saturating_mul(self.segment_capacity)),
        );
        if capacity <= LogSegment::header_size() {
            // 残段连段头都放不下 → 这段空间用不了,等价写满(不建无意义的空段文件)。
            return Err(Error::SegmentFull);
        }
        let path = self.dir.join(seg_name(next, next_gen).filename());
        let segment = Arc::new(LogSegment::create(
            &path,
            capacity,
            self.key_prefix,
            u32::try_from(next_gen).expect("generation must fit u32"),
        )?);
        entries.push(Entry {
            position: next,
            generation: next_gen,
            segment,
        });
        Ok(())
    }
}

/// 校验容量参数。
///
/// **允许 `total` 不是 `segment` 的整数倍**:最后一段就是"残段",容量 = `total − k × segment`
/// (`Log::roll_over` 按剩余声明容量算,段头自身的 CAPACITY 字段能表达任意大小)。
/// 这样配置项之间不再互相耦合(例如 100 GiB 分区 + 3 GiB 段:33 个整段 + 1 个 1 GiB 残段 —— 段本身
/// 仍受 4 GiB 硬上界约束,见 `LogSegment::MAX_CAPACITY`);也允许 `total < segment`
/// (退化为单段,对齐 Ambry `segmentCapacity = min(capacity, segment)`)。
///
/// 真正不可用的是"连数据区都装不下"的残段:那部分空间会被视为写满(`SegmentFull` → `StoreFull`),
/// 而不是报一个误导性的"记录超过段容量"。
fn validate_capacities(total: u64, segment: u64) -> Result<()> {
    if segment == 0 || total == 0 {
        return Err(Error::BadArgument("capacities must be positive".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK: u64 = 1024;
    const HEADER: u64 = 64;

    /// RAII 临时目录(测试结束整目录回收,`$TMPDIR` 下,带 ambry 前缀可辨认)。
    fn temp_dir(tag: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("ambry_log_{tag}_"))
            .tempdir()
            .unwrap()
    }

    fn seg_cap(blocks: u64) -> u64 {
        HEADER + blocks * BLOCK
    }

    #[test]
    fn rollover_across_segments() {
        let dir = temp_dir("roll");
        let seg = seg_cap(2);
        let log = Log::create(dir.path(), seg * 3, seg, 0x1234, 1).unwrap();

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
    }

    #[test]
    fn log_full_when_all_segments_exhausted() {
        let dir = temp_dir("full");
        let seg = seg_cap(1);
        let log = Log::create(dir.path(), seg * 2, seg, 0x1234, 1).unwrap();

        let rec = vec![1u8; BLOCK as usize];
        log.append(&rec).unwrap();
        log.append(&rec).unwrap();
        match log.append(&rec) {
            Err(Error::SegmentFull) => {}
            other => panic!("expected SegmentFull, got {other:?}"),
        }
        log.close().unwrap();
    }

    #[test]
    fn record_too_large_for_segment() {
        let dir = temp_dir("toobig");
        let seg = seg_cap(2);
        let log = Log::create(dir.path(), seg * 2, seg, 0x1234, 1).unwrap();
        let rec = vec![0u8; (2 * BLOCK) as usize + 1];
        match log.append(&rec) {
            Err(Error::BadArgument(_)) => {}
            other => panic!("expected BadArgument, got {other:?}"),
        }
        log.close().unwrap();
    }

    #[test]
    fn reopen_recovers_all_segments() {
        let dir = temp_dir("reopen");
        let seg = seg_cap(2);
        let rec = vec![5u8; BLOCK as usize + 50];
        let (o1, o2, end);
        {
            let log = Log::create(dir.path(), seg * 4, seg, 0x1234, 1).unwrap();
            o1 = log.append(&rec).unwrap();
            o2 = log.append(&rec).unwrap();
            log.flush().unwrap();
            end = log.end_offset();
            log.close().unwrap();
        }
        let log = Log::open(dir.path(), seg * 4, seg).unwrap();
        assert_eq!(log.segment_count(), 2);
        assert_eq!(
            log.key_prefix(),
            0x1234,
            "prefix comes back from segment header"
        );
        // 段身份 =(位置 0, 代次 1);重开后段头自述的代次与名字一致。
        assert_eq!(
            log.generation_of(0),
            Some(1),
            "段身份(位置 + 代次)随目录持久化"
        );
        assert_eq!(log.end_offset(), end);
        assert_eq!(log.read(o1, rec.len()).unwrap(), rec);
        assert_eq!(log.read(o2, rec.len()).unwrap(), rec);
        log.close().unwrap();
    }

    #[test]
    fn rollover_inherits_key_prefix() {
        let dir = temp_dir("prefix_roll");
        let seg = seg_cap(1);
        let log = Log::create(dir.path(), seg * 2, seg, 0xABCD_EF01, 1).unwrap();
        let rec = vec![1u8; BLOCK as usize];
        log.append(&rec).unwrap();
        log.append(&rec).unwrap();
        assert_eq!(
            log.segment_count(),
            2,
            "second record must roll to a new segment"
        );
        // 两个段:位置 0/1,代次 1/2(每次滚动代次 +1)。
        for (pos, gen) in [(0u64, 1u64), (1, 2)] {
            let path = dir.path().join(LogSegmentName::new(pos, gen).filename());
            assert_eq!(LogSegment::open(&path).unwrap().key_prefix(), 0xABCD_EF01);
        }
        log.close().unwrap();
    }

    /// 各段前缀不一致 → `open` 报 `Corrupt`,不按某一个前缀去解释整库数据。
    #[test]
    fn open_rejects_segments_with_disagreeing_prefix() {
        let dir = temp_dir("prefix_mismatch");
        let seg = seg_cap(1);
        Log::create(dir.path(), seg * 2, seg, 1, 1)
            .unwrap()
            .close()
            .unwrap();
        let p1 = dir.path().join(LogSegmentName::new(1, 1).filename());
        LogSegment::create(&p1, seg, 2, 1).unwrap().close().unwrap();

        assert!(
            matches!(
                Log::open(dir.path(), seg * 2, seg),
                Err(Error::Corrupt(ref m)) if m.contains("key prefix")
            ),
            "各段前缀不一致必须判 Corrupt"
        );
    }

    /// **代次永不复用**:同一目录内每建一个新段代次 +1;跨全库 compact 由调用方接着取
    /// (`min(first_generation = 旧 log 的最大代次 + 1)`)。
    #[test]
    fn generation_is_monotonic_and_never_reused() {
        let dir = temp_dir("gen_monotonic");
        let seg = seg_cap(1);
        let log = Log::create(dir.path(), seg * 4, seg, 0, 1).unwrap();
        let rec = vec![7u8; BLOCK as usize];
        log.append(&rec).unwrap(); // 首段
        log.append(&rec).unwrap(); // 滚到第二段
        log.append(&rec).unwrap(); // 滚到第三段
        assert_eq!(log.segment_count(), 3);
        assert_eq!(log.max_generation(), 3, "每滚一次代次 +1");
        // 位置与代次一一对应(本目录内位置也单调)。
        for pos in 0..3u64 {
            assert_eq!(log.generation_of(pos), Some(pos as u32 + 1));
        }
        assert_eq!(log.generation_of(9), None, "没有的位置 → None(段被删)");
        let old_max = log.max_generation();
        log.close().unwrap();

        // 重写(模拟全库 compact):代次接着旧 log 的最大值往后,段位置从 0 重来。
        let rewritten_dir = temp_dir("gen_rewritten");
        let rewritten = Log::create(rewritten_dir.path(), seg * 4, seg, 0, old_max + 1).unwrap();
        assert_eq!(
            rewritten.generation_of(0),
            Some(old_max as u32 + 1),
            "(位置 0, 新代次) 与旧的 (位置 0, 旧代次) 不同"
        );
        rewritten.close().unwrap();
    }

    /// 段头自述的代次与文件名不一致 → `open` 判 `Corrupt`(段文件被替换/名字被改过)。
    #[test]
    fn open_rejects_generation_mismatch_between_header_and_name() {
        let dir = temp_dir("gen_mismatch");
        let seg = seg_cap(1);
        Log::create(dir.path(), seg * 2, seg, 0, 1)
            .unwrap()
            .close()
            .unwrap();
        // 造一个"名字说代次 1、段头说代次 9"的段。
        let bad = dir.path().join(LogSegmentName::new(1, 1).filename());
        LogSegment::create(&bad, seg, 0, 9)
            .unwrap()
            .close()
            .unwrap();

        assert!(
            matches!(
                Log::open(dir.path(), seg * 2, seg),
                Err(Error::Corrupt(ref m)) if m.contains("generation")
            ),
            "段头与文件名代次不符必须判 Corrupt"
        );
    }
}
