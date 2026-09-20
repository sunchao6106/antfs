//! `BlobStore`:把 [`Log`](crate::log::Log) 与 [`PersistentIndex`](crate::index::PersistentIndex)
//! 串成一个对象存储:`put` / `get` / `delete`。
//!
//! 每次写入都在 Log 追加一条**自描述记录**(含 key/flags/crc),索引只保存
//! `key → (LogOffset, 记录大小, flags)`。因此索引损坏时可由扫描 Log 重建(见路线图)。
//!
//! 记录布局(小端,crc 覆盖其前所有字节):
//! ```text
//! [ magic u16 | version u8 | flags u8 | key(32) | expiration i64 | op_time u64 | data_len u32 | data | crc u32 ]
//! ```
//! `put` 写 `flags=0`;`delete` 写 `flags=FLAG_DELETED`、`data_len=0` 的墓碑。expiration/op_time
//! 落进记录,故从 log 重建索引时 TTL 不丢失。

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::index::value::{FLAG_DELETED, NO_EXPIRATION};
use crate::index::{IndexValue, PersistentIndex};
use crate::log::Log;
use crate::log_segment::LogSegment;
use crate::types::{LogOffset, LogSegmentName, StoreKey, KEY_SIZE};

const REC_MAGIC: u16 = 0x424D; // "MB"
const REC_VERSION: u8 = 2;

mod rec {
    use super::KEY_SIZE;
    pub const MAGIC: usize = 0;
    pub const VERSION: usize = 2;
    pub const FLAGS: usize = 3;
    pub const KEY: usize = 4;
    pub const EXPIRATION: usize = KEY + KEY_SIZE; // i64
    pub const OP_TIME: usize = EXPIRATION + 8; // u64
    pub const DATA_LEN: usize = OP_TIME + 8; // u32
    pub const DATA: usize = DATA_LEN + 4;
    pub const CRC_LEN: usize = 4;
}

/// 记录去掉 data 后的固定开销(头 + 尾部 crc)。
pub const RECORD_OVERHEAD: usize = rec::DATA + rec::CRC_LEN;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn encode_record(
    key: &StoreKey,
    flags: u8,
    expiration_ms: i64,
    operation_time_ms: u64,
    data: &[u8],
) -> Vec<u8> {
    let total = RECORD_OVERHEAD + data.len();
    let mut buf = vec![0u8; total];
    buf[rec::MAGIC..rec::MAGIC + 2].copy_from_slice(&REC_MAGIC.to_le_bytes());
    buf[rec::VERSION] = REC_VERSION;
    buf[rec::FLAGS] = flags;
    buf[rec::KEY..rec::KEY + KEY_SIZE].copy_from_slice(key.as_bytes());
    buf[rec::EXPIRATION..rec::EXPIRATION + 8].copy_from_slice(&expiration_ms.to_le_bytes());
    buf[rec::OP_TIME..rec::OP_TIME + 8].copy_from_slice(&operation_time_ms.to_le_bytes());
    buf[rec::DATA_LEN..rec::DATA_LEN + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    buf[rec::DATA..rec::DATA + data.len()].copy_from_slice(data);
    let crc_at = rec::DATA + data.len();
    let crc = crc32fast::hash(&buf[..crc_at]);
    buf[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
    buf
}

struct DecodedRecord {
    flags: u8,
    key: StoreKey,
    expiration_ms: i64,
    operation_time_ms: u64,
    data: Vec<u8>,
}

fn decode_record(buf: &[u8]) -> Result<DecodedRecord> {
    if buf.len() < RECORD_OVERHEAD {
        return Err(Error::Corrupt(format!("record too short: {}", buf.len())));
    }
    let magic = u16::from_le_bytes(buf[rec::MAGIC..rec::MAGIC + 2].try_into().unwrap());
    if magic != REC_MAGIC {
        return Err(Error::Corrupt(format!("bad record magic {magic:#x}")));
    }
    if buf[rec::VERSION] != REC_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported record version {}",
            buf[rec::VERSION]
        )));
    }
    let flags = buf[rec::FLAGS];
    let key = StoreKey::from_slice(&buf[rec::KEY..rec::KEY + KEY_SIZE]).expect("key size fixed");
    let expiration_ms = i64::from_le_bytes(buf[rec::EXPIRATION..rec::EXPIRATION + 8].try_into().unwrap());
    let operation_time_ms = u64::from_le_bytes(buf[rec::OP_TIME..rec::OP_TIME + 8].try_into().unwrap());
    let data_len = u32::from_le_bytes(buf[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
    let crc_at = rec::DATA + data_len;
    if buf.len() != crc_at + rec::CRC_LEN {
        return Err(Error::Corrupt(format!(
            "record size mismatch: buf={} expected={}",
            buf.len(),
            crc_at + rec::CRC_LEN
        )));
    }
    let stored = u32::from_le_bytes(buf[crc_at..crc_at + 4].try_into().unwrap());
    let computed = crc32fast::hash(&buf[..crc_at]);
    if stored != computed {
        return Err(Error::Corrupt(format!(
            "record crc mismatch: stored={stored:#x} computed={computed:#x}"
        )));
    }
    Ok(DecodedRecord {
        flags,
        key,
        expiration_ms,
        operation_time_ms,
        data: buf[rec::DATA..crc_at].to_vec(),
    })
}

/// 对象存储:单写者(`&mut self` 写路径)。数据走 Log(std::fs 同步阻塞),索引走 std::fs。
pub struct BlobStore {
    dir: PathBuf,
    total_capacity: u64,
    segment_capacity: u64,
    max_index_entries_per_segment: usize,
    log: Log,
    index: PersistentIndex,
}

/// compaction 结果统计。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub segments_before: usize,
    pub segments_after: usize,
}

impl BlobStore {
    /// 在 `dir` 下创建新库:数据在 `dir/log`,索引在 `dir/index`。
    pub fn create(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::create(&log_dir, total_capacity, segment_capacity)?;
        let index = PersistentIndex::create(&index_dir, max_index_entries_per_segment)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            log,
            index,
        })
    }

    /// 打开已有库并恢复索引与日志;若有未完成的 compaction 提交标记,先修复。
    ///
    /// 崩溃安全:加载已落盘索引后,按**持久化水位**回放 log 尾部——把「已落盘到 log 但
    /// 上次索引持久化未覆盖」的记录补进索引(对齐 Ambry `recover()`),避免加载到陈旧索引。
    pub fn open(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        recover_compaction(&dir, total_capacity, segment_capacity, max_index_entries_per_segment)?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let mut index = PersistentIndex::open(&index_dir, max_index_entries_per_segment)?;
        let watermark = read_watermark(&index_dir).unwrap_or(LogOffset::new(0, 0));
        replay_from(&log, &mut index, watermark)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            log,
            index,
        })
    }

    /// 索引损坏/丢失时的兜底:打开 Log,丢弃旧索引文件,顺序扫描全部记录重建索引。
    ///
    /// 记录自描述(含 key/flags),按 Log 追加顺序(旧→新)插入即天然满足 newest-wins。
    /// 只重建到最近一次 flush 的持久化边界(未落盘尾部本就不可见,无残记录)。
    /// 注意:记录目前不落 expiration/op_time,重建后这两项归零(当前无 TTL 写入,不影响可见性)。
    pub fn open_rebuild_index(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        recover_compaction(&dir, total_capacity, segment_capacity, max_index_entries_per_segment)?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let index = rebuild_index(&log, &index_dir, max_index_entries_per_segment)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            log,
            index,
        })
    }

    /// 写入一个对象(无过期)。key 已存在且未删除则报 [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。
    pub fn put(&mut self, key: StoreKey, data: &[u8]) -> Result<()> {
        self.put_ttl(key, data, NO_EXPIRATION)
    }

    /// 写入一个对象并设定过期时间(`expiration_ms`,`NO_EXPIRATION` 表示不过期)。
    pub fn put_ttl(&mut self, key: StoreKey, data: &[u8], expiration_ms: i64) -> Result<()> {
        if let Some(v) = self.index.find(&key) {
            if !v.is_deleted() {
                return Err(Error::AlreadyExists);
            }
        }
        let op_time = now_ms();
        let record = encode_record(&key, 0, expiration_ms, op_time, data);
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.expiration_ms = expiration_ms;
        self.index.add(key, value)?;
        Ok(())
    }

    /// 读取一个对象。不存在/已删除/已过期分别返回对应错误。
    pub fn get(&self, key: &StoreKey) -> Result<Vec<u8>> {
        let value = self.index.find(key).ok_or(Error::NotFound)?;
        if value.is_deleted() {
            return Err(Error::Deleted);
        }
        if value.expiration_ms != NO_EXPIRATION && now_ms() as i64 >= value.expiration_ms {
            return Err(Error::Expired);
        }
        let raw = self.log.read(value.offset, value.size as usize)?;
        let decoded = decode_record(&raw)?;
        if &decoded.key != key {
            return Err(Error::Corrupt(format!(
                "record key mismatch at {}",
                value.offset
            )));
        }
        if decoded.flags & FLAG_DELETED != 0 {
            return Err(Error::Corrupt(format!(
                "live index points at a tombstone record at {}",
                value.offset
            )));
        }
        Ok(decoded.data)
    }

    /// 删除一个对象:追加墓碑记录并在索引写入删除条目。不存在/已删除报错。
    pub fn delete(&mut self, key: StoreKey) -> Result<()> {
        match self.index.find(&key) {
            None => return Err(Error::NotFound),
            Some(v) if v.is_deleted() => return Err(Error::Deleted),
            Some(_) => {}
        }
        let op_time = now_ms();
        let record = encode_record(&key, FLAG_DELETED, NO_EXPIRATION, op_time, &[]);
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.flags = FLAG_DELETED;
        self.index.add(key, value)?;
        Ok(())
    }

    /// 持久化:先落数据(fsync)再落索引,最后写持久化水位(= 当前 log 末尾)。
    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        self.index.flush()?;
        let (_, index_dir) = sub_dirs(&self.dir);
        write_watermark(&index_dir, self.log.end_offset())?;
        Ok(())
    }

    /// flush 后关闭底层日志文件。
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.log.close()
    }

    /// 仅持久化数据日志(推进 log header 的 data_end),**不**落索引/水位。用于测试模拟
    /// 「数据已落盘、索引仍停在旧水位」的崩溃前状态。
    #[cfg(test)]
    fn log_flush_only_for_test(&mut self) {
        self.log.flush().unwrap();
    }

    #[cfg(test)]
    fn log_segment_count_for_test(&self) -> usize {
        self.log.segment_count()
    }

    /// 扫描全库,返回存活/死亡字节统计(死 = 被更新版本取代/删除的旧记录)。
    pub fn stats(&self) -> Result<CompactionStats> {
        let mut live = 0u64;
        let mut dead = 0u64;
        for_each_record(&self.log, |off, raw, decoded| {
            let is_live = matches!(self.index.find(&decoded.key), Some(v) if v.offset == off);
            if is_live {
                live += raw.len() as u64;
            } else {
                dead += raw.len() as u64;
            }
            Ok(())
        })?;
        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: self.log.segment_count(),
            segments_after: self.log.segment_count(),
        })
    }

    /// 死数据占比 ≥ `threshold`(0.0~1.0)才 compact;否则跳过。返回是否执行。
    pub fn compact_if_needed(&mut self, threshold: f64) -> Result<Option<CompactionStats>> {
        let s = self.stats()?;
        let total = s.live_bytes + s.dead_bytes;
        if total == 0 || (s.dead_bytes as f64 / total as f64) < threshold {
            return Ok(None);
        }
        Ok(Some(self.compact()?))
    }

    /// CompactAll:重写整个 log 只保留存活记录(newest-wins 赢家),重建索引,原子换入。
    ///
    /// 崩溃安全:先建 `log.new` 并 fsync → 写提交标记 → 目录 rename 换入(log→log.old,log.new→log)
    /// → 重建索引 → 删标记与 log.old。任一步崩溃,`open()` 依据标记修复(见 `recover_compaction`)。
    pub fn compact(&mut self) -> Result<CompactionStats> {
        self.flush()?;

        let before = self.log.segment_count();
        let (log_dir, index_dir) = sub_dirs(&self.dir);
        let log_new = self.dir.join("log.new");
        let log_old = self.dir.join("log.old");
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;

        // COPY:扫旧 log,存活记录(当前赢家)重写进 log.new。
        let mut new_log = Log::create(&log_new, self.total_capacity, self.segment_capacity)?;
        let mut live = 0u64;
        let mut dead = 0u64;
        for_each_record(&self.log, |off, raw, decoded| {
            let is_live = matches!(self.index.find(&decoded.key), Some(v) if v.offset == off);
            if is_live {
                new_log.append(raw)?;
                live += raw.len() as u64;
            } else {
                dead += raw.len() as u64;
            }
            Ok(())
        })?;
        new_log.flush()?;
        let after = new_log.segment_count();
        new_log.close()?;

        // COMMIT:写标记 → 换入。标记在,open() 会保证 log 是 compacted 的那份并重建索引。
        // Unix 下 rename 目录不受内部已打开 fd 影响,直接换入即可。
        let marker = self.dir.join("COMPACT_COMMIT");
        write_marker(&marker)?;
        std::fs::rename(&log_dir, &log_old)?;
        std::fs::rename(&log_new, &log_dir)?;

        // 重建索引(offset 已变)+ 收尾。
        self.log = Log::open(&log_dir, self.total_capacity, self.segment_capacity)?;
        self.index = rebuild_index(&self.log, &index_dir, self.max_index_entries_per_segment)?;
        let _ = std::fs::remove_file(&marker);
        rm_rf(&log_old)?;

        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: before,
            segments_after: after,
        })
    }

    /// 每个封存段(不含活跃/末段)的 `(position, live_bytes, dead_bytes)`。
    pub fn segment_stats(&self) -> Result<Vec<(u64, u64, u64)>> {
        let spans = self.log.segment_spans();
        let active_pos = spans.last().map(|(p, _)| *p);
        let mut out = Vec::new();
        for (pos, data_end) in spans {
            if Some(pos) == active_pos {
                continue;
            }
            let (mut live, mut dead) = (0u64, 0u64);
            scan_segment(&self.log, pos, data_end, |off, raw, decoded| {
                if matches!(self.index.find(&decoded.key), Some(v) if v.offset == off) {
                    live += raw.len() as u64;
                } else {
                    dead += raw.len() as u64;
                }
                Ok(())
            })?;
            out.push((pos, live, dead));
        }
        Ok(out)
    }

    /// 增量 compaction:只压缩死数据占比 ≥ `threshold` 的**非活跃**段(热/末段不动)。
    ///
    /// 机制:被选中段的存活记录写入位置更高的新段(temp→fsync→rename),删旧段,重建索引。
    /// 只重写高死段、不碰热段(区别于 [`compact`](Self::compact) 的全库重写)。存活赢家搬到更高
    /// 位置仍是其 key 的最新记录,newest-wins 不变。崩溃安全:复用 `COMPACT_COMMIT` 标记 +
    /// open 重建索引;残留 `.tmp`/重复记录在重建时被忽略/收敛。返回 None 表示无段达阈值。
    pub fn compact_incremental(&mut self, threshold: f64) -> Result<Option<CompactionStats>> {
        self.flush()?;
        let stats = self.segment_stats()?;
        let selected: Vec<u64> = stats
            .iter()
            .filter(|(_, live, dead)| {
                let total = live + dead;
                total > 0 && (*dead as f64 / total as f64) >= threshold
            })
            .map(|(pos, _, _)| *pos)
            .collect();
        if selected.is_empty() {
            return Ok(None);
        }

        let (log_dir, index_dir) = sub_dirs(&self.dir);
        let marker = self.dir.join("COMPACT_COMMIT");
        write_marker(&marker)?;

        let mut next_pos = self.log.segment_spans().iter().map(|(p, _)| *p).max().unwrap() + 1;
        let mut live_bytes = 0u64;
        let mut dead_bytes = 0u64;
        let mut tmp_paths: Vec<(PathBuf, PathBuf)> = Vec::new(); // (tmp, final)
        let mut cur: Option<LogSegment> = None;

        for &pos in &selected {
            let data_end = self
                .log
                .segment_spans()
                .iter()
                .find(|(p, _)| *p == pos)
                .map(|(_, e)| *e)
                .unwrap_or(0);
            scan_segment(&self.log, pos, data_end, |off, raw, decoded| {
                let is_live = matches!(self.index.find(&decoded.key), Some(v) if v.offset == off);
                if !is_live {
                    dead_bytes += raw.len() as u64;
                    return Ok(());
                }
                live_bytes += raw.len() as u64;
                let need = raw.len() as u64;
                let fits = cur.as_ref().map(|c| c.remaining() >= need).unwrap_or(false);
                if !fits {
                    if let Some(c) = cur.take() {
                        let mut c = c;
                        c.flush()?;
                        c.close()?;
                    }
                    let final_path = log_dir.join(LogSegmentName(next_pos).filename());
                    let mut tmp = final_path.as_os_str().to_owned();
                    tmp.push(".tmp");
                    let tmp = PathBuf::from(tmp);
                    next_pos += 1;
                    cur = Some(LogSegment::create(&tmp, self.segment_capacity)?);
                    tmp_paths.push((tmp, final_path));
                }
                cur.as_mut().unwrap().append(raw)?;
                Ok(())
            })?;
        }
        if let Some(c) = cur.take() {
            let mut c = c;
            c.flush()?;
            c.close()?;
        }

        // temp 新段 → 正式名(atomic rename)。
        for (tmp, final_path) in &tmp_paths {
            std::fs::rename(tmp, final_path)?;
        }
        // 删被压缩的旧段。
        for &pos in &selected {
            let _ = std::fs::remove_file(log_dir.join(LogSegmentName(pos).filename()));
        }

        let before = stats.len() + 1; // 含活跃段的粗略段数
        self.log = Log::open(&log_dir, self.total_capacity, self.segment_capacity)?;
        self.index = rebuild_index(&self.log, &index_dir, self.max_index_entries_per_segment)?;
        let _ = std::fs::remove_file(&marker);

        Ok(Some(CompactionStats {
            live_bytes,
            dead_bytes,
            segments_before: before,
            segments_after: self.log.segment_count(),
        }))
    }
}

fn sub_dirs(dir: &Path) -> (PathBuf, PathBuf) {
    (dir.join("log"), dir.join("index"))
}

/// 顺序遍历 Log 全部记录,回调 `(offset, 完整记录字节, 解码结果)`。
fn for_each_record(
    log: &Log,
    mut f: impl FnMut(LogOffset, &[u8], &DecodedRecord) -> Result<()>,
) -> Result<()> {
    for (position, data_end) in log.segment_spans() {
        scan_segment(log, position, data_end, &mut f)?;
    }
    Ok(())
}

/// 遍历单个段 `position` 内 `[0, data_end)` 的全部记录。
fn scan_segment(
    log: &Log,
    position: u64,
    data_end: u64,
    mut f: impl FnMut(LogOffset, &[u8], &DecodedRecord) -> Result<()>,
) -> Result<()> {
    let mut off = 0u64;
    while off < data_end {
        let head = log.read(LogOffset::new(position, off), rec::DATA)?;
        let data_len =
            u32::from_le_bytes(head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
        let total = RECORD_OVERHEAD + data_len;
        let raw = log.read(LogOffset::new(position, off), total)?;
        let decoded = decode_record(&raw)?;
        f(LogOffset::new(position, off), &raw, &decoded)?;
        off += total as u64;
    }
    Ok(())
}

/// 清空索引目录并顺序扫描 Log 重建索引(旧→新,天然 newest-wins)。
fn rebuild_index(log: &Log, index_dir: &Path, max_entries: usize) -> Result<PersistentIndex> {
    std::fs::create_dir_all(index_dir)?;
    for ent in std::fs::read_dir(index_dir)? {
        let ent = ent?;
        if let Some(n) = ent.file_name().to_str() {
            if n.ends_with(".idx") || n.ends_with(".fuse") {
                std::fs::remove_file(ent.path())?;
            }
        }
    }
    let mut index = PersistentIndex::create(index_dir, max_entries)?;
    for_each_record(log, |off, raw, decoded| {
        let mut value = IndexValue::put(off, raw.len() as u64, decoded.operation_time_ms);
        value.flags = decoded.flags;
        value.expiration_ms = decoded.expiration_ms;
        index.add(decoded.key, value)?;
        Ok(())
    })?;
    index.flush()?;
    // 重建覆盖了整个 log,水位推进到当前末尾。
    write_watermark(index_dir, log.end_offset())?;
    Ok(index)
}

/// 按持久化水位回放 log 尾部:把 offset ≥ watermark 的记录补进索引(旧→新,保 newest-wins)。
fn replay_from(log: &Log, index: &mut PersistentIndex, watermark: LogOffset) -> Result<()> {
    for_each_record(log, |off, raw, decoded| {
        if off >= watermark {
            let mut value = IndexValue::put(off, raw.len() as u64, decoded.operation_time_ms);
            value.flags = decoded.flags;
            value.expiration_ms = decoded.expiration_ms;
            index.add(decoded.key, value)?;
        }
        Ok(())
    })
}

const WATERMARK_FILE: &str = "WATERMARK";

/// 写持久化水位(segment u64 + offset u64 + crc u32,tmp→fsync→rename)。
fn write_watermark(index_dir: &Path, off: LogOffset) -> Result<()> {
    std::fs::create_dir_all(index_dir)?;
    let mut buf = [0u8; 20];
    buf[0..8].copy_from_slice(&off.segment.to_le_bytes());
    buf[8..16].copy_from_slice(&off.offset.to_le_bytes());
    let crc = crc32fast::hash(&buf[0..16]);
    buf[16..20].copy_from_slice(&crc.to_le_bytes());
    let path = index_dir.join(WATERMARK_FILE);
    let tmp = index_dir.join("WATERMARK.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// 读持久化水位;文件缺失或损坏返回 None(调用方回退到从 0 回放)。
fn read_watermark(index_dir: &Path) -> Option<LogOffset> {
    let buf = std::fs::read(index_dir.join(WATERMARK_FILE)).ok()?;
    if buf.len() != 20 {
        return None;
    }
    let crc = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    if crc32fast::hash(&buf[0..16]) != crc {
        return None;
    }
    let segment = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    Some(LogOffset::new(segment, offset))
}

/// 打开前修复未完成的 compaction:依据 `COMPACT_COMMIT` 标记与 `log.new`/`log.old` 收敛到一致状态。
fn recover_compaction(
    dir: &Path,
    total_capacity: u64,
    segment_capacity: u64,
    max_entries: usize,
) -> Result<()> {
    let (log_dir, index_dir) = sub_dirs(dir);
    let log_new = dir.join("log.new");
    let log_old = dir.join("log.old");
    let marker = dir.join("COMPACT_COMMIT");

    if marker.exists() {
        // 提交进行中:保证 log 是 compacted 的那份。
        if !log_dir.exists() && log_new.exists() {
            // 崩在两次 rename 之间(CompactAll):log.new 尚未换入。
            std::fs::rename(&log_new, &log_dir)?;
        }
        // 其余情况 log_dir 已是新 log(或 log.old 存旧份,丢弃)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
        clean_tmp_segments(&log_dir)?; // 增量 compaction 崩溃遗留的 .tmp 新段
        // 索引可能是旧/半建的,统一重建。
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let _ = rebuild_index(&log, &index_dir, max_entries)?;
        std::fs::remove_file(&marker)?;
    } else {
        // 无标记:COPY 阶段崩溃遗留的 log.new / .tmp 直接丢弃(旧 log 完好)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
        clean_tmp_segments(&log_dir)?;
    }
    Ok(())
}

/// 删除 log 目录下残留的 `*.tmp`(崩溃遗留的增量 compaction 半成品新段)。
fn clean_tmp_segments(log_dir: &Path) -> Result<()> {
    if !log_dir.exists() {
        return Ok(());
    }
    for ent in std::fs::read_dir(log_dir)? {
        let ent = ent?;
        if ent.file_name().to_str().is_some_and(|n| n.ends_with(".tmp")) {
            let _ = std::fs::remove_file(ent.path());
        }
    }
    Ok(())
}

fn write_marker(path: &Path) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    f.write_all(b"COMPACTING")?;
    f.sync_all()?;
    Ok(())
}

fn rm_rf(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: u64 = 64;
    const BLOCK: u64 = 1024;

    fn temp_dir(tag: &str) -> PathBuf {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(format!("/var/tmp/ambry_blob_{tag}_{ns}"))
    }

    fn seg_cap(blocks: u64) -> u64 {
        HEADER + blocks * BLOCK
    }

    fn key(n: u64) -> StoreKey {
        let mut b = [0u8; KEY_SIZE];
        b[0..8].copy_from_slice(&n.to_le_bytes());
        b[24..32].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        StoreKey::new(b)
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = temp_dir("rt");
        let seg = seg_cap(64);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).unwrap();

        let data = b"hello ambry blob".to_vec();
        store.put(key(1), &data).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), data);

        let big: Vec<u8> = (0..(BLOCK * 3 + 77) as usize).map(|i| (i % 251) as u8).collect();
        store.put(key(2), &big).unwrap();
        assert_eq!(store.get(&key(2)).unwrap(), big);

        assert!(matches!(store.get(&key(999)), Err(Error::NotFound)));
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_existing_rejected() {
        let dir = temp_dir("dup");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).unwrap();
        store.put(key(1), b"a").unwrap();
        assert!(matches!(store.put(key(1), b"b"), Err(Error::AlreadyExists)));
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_then_get_and_reput() {
        let dir = temp_dir("del");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).unwrap();
        store.put(key(1), b"v1").unwrap();
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert!(matches!(store.delete(key(1)), Err(Error::Deleted)));
        assert!(matches!(store.delete(key(2)), Err(Error::NotFound)));

        // 删除后允许重新 put(旧条目是 deleted)。
        store.put(key(1), b"v2").unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"v2".to_vec());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_recovers_blobs() {
        let dir = temp_dir("reopen");
        let seg = seg_cap(16);
        {
            let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
            for i in 0..20u64 {
                store.put(key(i), format!("blob-{i}").as_bytes()).unwrap();
            }
            store.delete(key(3)).unwrap();
            store.close().unwrap();
        }
        let store = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        for i in 0..20u64 {
            if i == 3 {
                assert!(matches!(store.get(&key(i)), Err(Error::Deleted)));
            } else {
                assert_eq!(
                    store.get(&key(i)).unwrap(),
                    format!("blob-{i}").into_bytes(),
                    "key {i} lost after reopen"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rebuild_index_from_log_scan() {
        let dir = temp_dir("rebuild");
        let seg = seg_cap(16);
        {
            let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
            for i in 0..25u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            store.delete(key(7)).unwrap();
            store.put(key(7), b"v2-after-delete").unwrap();
            store.close().unwrap();
        }

        let index_dir = dir.join("index");
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().is_some_and(|e| e == "idx") {
                std::fs::remove_file(p).unwrap();
            }
        }

        let store = BlobStore::open_rebuild_index(&dir, seg * 8, seg, 4).unwrap();
        for i in 0..25u64 {
            let want = if i == 7 {
                b"v2-after-delete".to_vec()
            } else {
                format!("v-{i}").into_bytes()
            };
            assert_eq!(store.get(&key(i)).unwrap(), want, "key {i} after rebuild");
        }
        assert!(matches!(store.get(&key(999)), Err(Error::NotFound)));

        drop(store);
        let store2 = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(store2.get(&key(7)).unwrap(), b"v2-after-delete".to_vec());
        drop(store2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_roundtrip_and_crc_guard() {
        let k = key(42);
        let data = b"payload".to_vec();
        let mut buf = encode_record(&k, 0, 1_700_000_000_000, 1_699_000_000_000, &data);
        let d = decode_record(&buf).unwrap();
        assert_eq!(d.flags, 0);
        assert_eq!(d.key, k);
        assert_eq!(d.data, data);
        assert_eq!(d.expiration_ms, 1_700_000_000_000);
        assert_eq!(d.operation_time_ms, 1_699_000_000_000);

        // 翻转一个数据字节,crc 必须失败。
        buf[rec::DATA] ^= 0xFF;
        assert!(matches!(decode_record(&buf), Err(Error::Corrupt(_))));
    }

    #[test]
    fn ttl_survives_rebuild() {
        let dir = temp_dir("ttl");
        let seg = seg_cap(16);
        let exp = now_ms() as i64 + 3_600_000; // 1h 后过期
        {
            let mut store = BlobStore::create(&dir, seg * 4, seg, 100).unwrap();
            store.put_ttl(key(1), b"has-ttl", exp).unwrap();
            store.put(key(2), b"no-ttl").unwrap();
            store.close().unwrap();
        }
        // 抹掉索引,从 log 重建:expiration 应被保留。
        let index_dir = dir.join("index");
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            let _ = std::fs::remove_file(p);
        }
        let store = BlobStore::open_rebuild_index(&dir, seg * 4, seg, 100).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"has-ttl".to_vec());
        assert_eq!(store.get(&key(2)).unwrap(), b"no-ttl".to_vec());
        // 用一个已过期的 TTL 再验证过期路径。
        drop(store);
        {
            let mut s2 = BlobStore::open(&dir, seg * 4, seg, 100).unwrap();
            s2.put_ttl(key(3), b"expired", 1).unwrap(); // expiration=1ms,早已过期
            assert!(matches!(s2.get(&key(3)), Err(Error::Expired)));
            s2.close().unwrap();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_reclaims_dead_space_and_preserves_state() {
        let dir = temp_dir("compact");
        let seg = seg_cap(8);
        let mut store = BlobStore::create(&dir, seg * 16, seg, 4).unwrap();

        // 制造大量死数据:put 0..10,再 delete 一半、覆盖(删后重写)另一半。
        for i in 0..10u64 {
            store.put(key(i), format!("original-value-{i}").as_bytes()).unwrap();
        }
        for i in 0..5u64 {
            store.delete(key(i)).unwrap(); // key 0..4 变墓碑(原 PUT 变死数据)
        }
        for i in 5..10u64 {
            store.delete(key(i)).unwrap();
            store.put(key(i), format!("rewritten-{i}").as_bytes()).unwrap(); // 覆盖:旧 PUT+DELETE 死
        }

        let before = store.stats().unwrap();
        assert!(before.dead_bytes > 0, "should have dead data");

        let st = store.compact().unwrap();
        assert!(st.dead_bytes > 0);
        // compact 后死数据应清零(全库只剩赢家)。
        let after = store.stats().unwrap();
        assert_eq!(after.dead_bytes, 0, "no dead data after compaction");

        // 逻辑状态不变:0..4 已删,5..9 是重写值。
        for i in 0..5u64 {
            assert!(matches!(store.get(&key(i)), Err(Error::Deleted)), "key {i} must stay deleted");
        }
        for i in 5..10u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("rewritten-{i}").into_bytes(),
                "key {i} value must survive compaction"
            );
        }

        // 重开(会经过 recover_compaction 无标记路径)仍一致。
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 16, seg, 4).unwrap();
        for i in 5..10u64 {
            assert_eq!(store.get(&key(i)).unwrap(), format!("rewritten-{i}").into_bytes());
        }
        assert!(matches!(store.get(&key(0)), Err(Error::Deleted)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_if_needed_respects_threshold() {
        let dir = temp_dir("compact_thr");
        let seg = seg_cap(8);
        let mut store = BlobStore::create(&dir, seg * 16, seg, 100).unwrap();
        for i in 0..5u64 {
            store.put(key(i), b"v").unwrap();
        }
        // 全是存活数据,死占比为 0 → 高阈值不触发。
        assert!(store.compact_if_needed(0.5).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recover_finishes_interrupted_commit() {
        // 模拟:compaction 提交阶段崩溃(留下 COMPACT_COMMIT 标记 + log.old),open 应修复。
        let dir = temp_dir("compact_crash");
        let seg = seg_cap(8);
        {
            let mut store = BlobStore::create(&dir, seg * 16, seg, 4).unwrap();
            for i in 0..8u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            for i in 0..4u64 {
                store.delete(key(i)).unwrap();
            }
            store.compact().unwrap();
            store.close().unwrap();
        }
        // 人为伪造"提交中崩溃":放一个标记 + 残留 log.old / log.new,open 必须收敛。
        let marker = dir.join("COMPACT_COMMIT");
        std::fs::write(&marker, b"COMPACTING").unwrap();
        // 残留一个空 log.new(无标记语义下会被丢弃;有标记时若 log 存在则清理)。
        std::fs::create_dir_all(dir.join("log.new")).unwrap();
        std::fs::create_dir_all(dir.join("log.old")).unwrap();

        let store = BlobStore::open(&dir, seg * 16, seg, 4).unwrap();
        assert!(!marker.exists(), "marker cleared after recovery");
        assert!(!dir.join("log.new").exists(), "log.new cleaned");
        assert!(!dir.join("log.old").exists(), "log.old cleaned");
        for i in 4..8u64 {
            assert_eq!(store.get(&key(i)).unwrap(), format!("v-{i}").into_bytes());
        }
        for i in 0..4u64 {
            assert!(matches!(store.get(&key(i)), Err(Error::Deleted)));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn watermark_replays_log_tail_beyond_persisted_index() {
        // 模拟「索引持久化后又写了新数据但未再持久化,然后崩溃」:
        // log 有尾部记录、索引文件停在旧水位。open 应按水位回放 log 尾部补回。
        let dir = temp_dir("wm");
        let seg = seg_cap(64);
        {
            let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).unwrap();
            store.put(key(1), b"a").unwrap();
            store.flush().unwrap(); // 索引 + 水位落到含 key(1)
            // 之后再写 key(2)/key(3),只 flush 数据日志(不落索引、不推进水位),模拟崩溃前状态。
            store.put(key(2), b"b").unwrap();
            store.put(key(3), b"c").unwrap();
            store.log_flush_only_for_test();
            // 直接丢弃内存态(不走 close 的 flush),模拟进程崩溃。
            std::mem::forget(store);
        }
        // 重开:水位=含 key(1) 处,回放 log 尾部应补回 key(2)/key(3)。
        let store = BlobStore::open(&dir, seg * 4, seg, 1000).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"a".to_vec());
        assert_eq!(store.get(&key(2)).unwrap(), b"b".to_vec(), "tail key(2) recovered via replay");
        assert_eq!(store.get(&key(3)).unwrap(), b"c".to_vec(), "tail key(3) recovered via replay");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_compaction_reclaims_only_dead_segments() {
        let dir = temp_dir("incr");
        let seg = seg_cap(1); // 每段数据容量 ~1KiB → 大记录会跨多段
        let mut store = BlobStore::create(&dir, seg * 128, seg, 1000).unwrap();

        let big = |tag: &str| -> Vec<u8> {
            let mut v = vec![b'x'; 400];
            v[..tag.len()].copy_from_slice(tag.as_bytes());
            v
        };

        // 写 40 个大 key(跨很多段),再删前 30 个 → 老段变高死数据。
        for i in 0..40u64 {
            store.put(key(i), &big(&format!("v{i}"))).unwrap();
        }
        for i in 0..30u64 {
            store.delete(key(i)).unwrap();
        }
        store.flush().unwrap();

        let before_segs = store.log_segment_count_for_test();
        assert!(before_segs > 2, "should span multiple segments");
        let st = store.compact_incremental(0.5).unwrap();
        assert!(st.is_some(), "high-dead old segments should be compacted");
        let st = st.unwrap();
        assert!(st.dead_bytes > 0);
        assert!(st.segments_after <= before_segs, "segment count should not grow");

        // 逻辑状态不变:0..29 已删,30..39 存活。
        for i in 0..30u64 {
            assert!(matches!(store.get(&key(i)), Err(Error::Deleted)), "key {i} deleted");
        }
        for i in 30..40u64 {
            assert_eq!(store.get(&key(i)).unwrap(), big(&format!("v{i}")), "key {i} live");
        }

        // 重开一致。
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 128, seg, 1000).unwrap();
        for i in 30..40u64 {
            assert_eq!(store.get(&key(i)).unwrap(), big(&format!("v{i}")));
        }
        assert!(matches!(store.get(&key(5)), Err(Error::Deleted)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
