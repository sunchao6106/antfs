//! `BlobStore`:把 [`Log`](crate::log::Log) 与 [`PersistentIndex`](crate::index::PersistentIndex)
//! 串成一个对象存储:`put` / `get` / `delete`。
//!
//! 每次写入都在 Log 追加一条**自描述记录**(含 key/flags/crc),索引只保存
//! `key → (LogOffset, 记录大小, flags)`。因此索引损坏时可由扫描 Log 重建(见路线图)。
//!
//! 记录布局(小端,crc 覆盖其前所有字节):
//! ```text
//! [ magic u16 | version u8 | flags u8 | key(32) | data_len u32 | data[data_len] | crc u32 ]
//! ```
//! `put` 写 `flags=0` 的记录;`delete` 写 `flags=FLAG_DELETED`、`data_len=0` 的墓碑记录。

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::index::value::{FLAG_DELETED, NO_EXPIRATION};
use crate::index::{IndexValue, PersistentIndex};
use crate::log::Log;
use crate::types::{LogOffset, StoreKey, KEY_SIZE};

const REC_MAGIC: u16 = 0x424D; // "MB"
const REC_VERSION: u8 = 1;

mod rec {
    use super::KEY_SIZE;
    pub const MAGIC: usize = 0;
    pub const VERSION: usize = 2;
    pub const FLAGS: usize = 3;
    pub const KEY: usize = 4;
    pub const DATA_LEN: usize = KEY + KEY_SIZE;
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

fn encode_record(key: &StoreKey, flags: u8, data: &[u8]) -> Vec<u8> {
    let total = RECORD_OVERHEAD + data.len();
    let mut buf = vec![0u8; total];
    buf[rec::MAGIC..rec::MAGIC + 2].copy_from_slice(&REC_MAGIC.to_le_bytes());
    buf[rec::VERSION] = REC_VERSION;
    buf[rec::FLAGS] = flags;
    buf[rec::KEY..rec::KEY + KEY_SIZE].copy_from_slice(key.as_bytes());
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
        let index = PersistentIndex::open(&index_dir, max_index_entries_per_segment)?;
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

    /// 写入一个对象。key 已存在且未删除则报 [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。
    pub fn put(&mut self, key: StoreKey, data: &[u8]) -> Result<()> {
        if let Some(v) = self.index.find(&key) {
            if !v.is_deleted() {
                return Err(Error::AlreadyExists);
            }
        }
        let record = encode_record(&key, 0, data);
        let offset = self.log.append(&record)?;
        self.index
            .add(key, IndexValue::put(offset, record.len() as u64, now_ms()))?;
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
        let record = encode_record(&key, FLAG_DELETED, &[]);
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, now_ms());
        value.flags = FLAG_DELETED;
        self.index.add(key, value)?;
        Ok(())
    }

    /// 持久化:先落数据(fsync)再落索引,保证索引不指向未落盘的数据。
    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        self.index.flush()?;
        Ok(())
    }

    /// flush 后关闭底层日志文件。
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.log.close()
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
        let mut off = 0u64;
        while off < data_end {
            let head = log.read(LogOffset::new(position, off), rec::DATA)?;
            let data_len =
                u32::from_le_bytes(head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap())
                    as usize;
            let total = RECORD_OVERHEAD + data_len;
            let raw = log.read(LogOffset::new(position, off), total)?;
            let decoded = decode_record(&raw)?;
            f(LogOffset::new(position, off), &raw, &decoded)?;
            off += total as u64;
        }
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
        let mut value = IndexValue::put(off, raw.len() as u64, now_ms());
        value.flags = decoded.flags;
        index.add(decoded.key, value)?;
        Ok(())
    })?;
    index.flush()?;
    Ok(index)
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
            // 崩在两次 rename 之间:log.new 尚未换入。
            std::fs::rename(&log_new, &log_dir)?;
        }
        // 其余情况 log_dir 已是新 log(或 log.old 存旧份,丢弃)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
        // 索引可能是旧/半建的,统一重建。
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let _ = rebuild_index(&log, &index_dir, max_entries)?;
        std::fs::remove_file(&marker)?;
    } else {
        // 无标记:COPY 阶段崩溃遗留的 log.new 直接丢弃(旧 log 完好)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
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
        let mut buf = encode_record(&k, 0, &data);
        let d = decode_record(&buf).unwrap();
        assert_eq!(d.flags, 0);
        assert_eq!(d.key, k);
        assert_eq!(d.data, data);

        // 翻转一个数据字节,crc 必须失败。
        buf[rec::DATA] ^= 0xFF;
        assert!(matches!(decode_record(&buf), Err(Error::Corrupt(_))));
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
}
