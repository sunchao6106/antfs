//! `BlobStore`:把 [`Log`](crate::log::Log) 与 [`PersistentIndex`](crate::index::PersistentIndex)
//! 串成一个对象存储:`put` / `get` / `delete`。
//!
//! 每次写入都在 Log 追加一条**自描述记录**(含 key/flags/crc),索引只保存
//! `key → (LogOffset, 记录大小, flags)`。因此索引损坏时可由扫描 Log 重建(见路线图)。
//!
//! 记录布局(小端,crc 覆盖其前所有字节):
//! ```text
//! [ magic u16 | version u8 | flags u8 | key(32) | expiration i64 | op_time u64 | life_version u16 | data_len u32 | data | crc u32 ]
//! ```
//! `put` 写 `flags=0`;`delete` 写 `flags=FLAG_DELETED`、`data_len=0` 的墓碑;`undelete` 写
//! `flags=FLAG_UNDELETED`、复制原数据的整记录并 `life_version+1`。expiration/op_time/life_version
//! 落进记录,故从 log 重建索引时 TTL 与生命版本不丢失。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::index::value::{
    FLAG_DELETED, FLAG_HARD_DELETED, FLAG_TTL_UPDATED, FLAG_UNDELETED, NO_EXPIRATION,
};
use crate::index::{IndexValue, PersistentIndex};
use crate::log::Log;
use crate::log_segment::LogSegment;
use crate::types::{LogOffset, LogSegmentName, StoreKey, KEY_SIZE};

const REC_MAGIC: u16 = 0x424D; // "MB"
const REC_VERSION: u8 = 3;

mod rec {
    use super::KEY_SIZE;
    pub const MAGIC: usize = 0;
    pub const VERSION: usize = 2;
    pub const FLAGS: usize = 3;
    pub const KEY: usize = 4;
    pub const EXPIRATION: usize = KEY + KEY_SIZE; // i64
    pub const OP_TIME: usize = EXPIRATION + 8; // u64
    pub const LIFE_VERSION: usize = OP_TIME + 8; // u16
    pub const DATA_LEN: usize = LIFE_VERSION + 2; // u32
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
    life_version: u16,
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
    buf[rec::LIFE_VERSION..rec::LIFE_VERSION + 2].copy_from_slice(&life_version.to_le_bytes());
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
    life_version: u16,
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
    let expiration_ms = i64::from_le_bytes(
        buf[rec::EXPIRATION..rec::EXPIRATION + 8]
            .try_into()
            .unwrap(),
    );
    let operation_time_ms =
        u64::from_le_bytes(buf[rec::OP_TIME..rec::OP_TIME + 8].try_into().unwrap());
    let life_version = u16::from_le_bytes(
        buf[rec::LIFE_VERSION..rec::LIFE_VERSION + 2]
            .try_into()
            .unwrap(),
    );
    let data_len =
        u32::from_le_bytes(buf[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
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
        life_version,
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
    /// 每段存活/死亡字节的增量计数(put/delete 就地更新,compaction/open 全量重算),
    /// 使 `stats`/`segment_stats` 免于每次全量扫 log(对齐 Ambry `BlobStoreStats` 增量维护)。
    seg_bytes: BTreeMap<u64, SegBytes>,
}

/// 单个 log 段的存活/死亡字节。死 = 被更新版本取代/删除的旧记录(非当前索引赢家)。
#[derive(Clone, Copy, Default)]
struct SegBytes {
    live: u64,
    dead: u64,
}

/// compaction 结果统计。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub segments_before: usize,
    pub segments_after: usize,
}

/// blob 元数据(不读数据体):状态标志 + TTL + 生命版本 + 数据长度。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobMeta {
    pub deleted: bool,
    pub undeleted: bool,
    pub ttl_updated: bool,
    pub expired: bool,
    pub expiration_ms: i64,
    pub life_version: u16,
    pub data_len: u64,
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
            seg_bytes: BTreeMap::new(),
        })
    }

    /// 打开已有库并恢复索引与日志;若有未完成的 compaction 提交标记,先修复。
    ///
    /// 崩溃安全:加载已落盘索引后,按**持久化水位**回放 log 尾部——把「已落盘到 log 但
    /// 上次索引持久化未覆盖」的记录补进索引(对齐 Ambry `recover()`),避免加载到陈旧索引。
    /// 索引文件损坏(`Corrupt`)时自动回退到全量扫 log 重建(数据 fsync 是持久边界,索引可重建)。
    pub fn open(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        recover_compaction(
            &dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
        )?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let index = match load_index_with_replay(&log, &index_dir, max_index_entries_per_segment) {
            Ok(index) => index,
            Err(Error::Corrupt(_)) => {
                rebuild_index(&log, &index_dir, max_index_entries_per_segment)?
            }
            Err(e) => return Err(e),
        };
        let seg_bytes = scan_seg_bytes(&log, &index)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            log,
            index,
            seg_bytes,
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
        recover_compaction(
            &dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
        )?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let index = rebuild_index(&log, &index_dir, max_index_entries_per_segment)?;
        let seg_bytes = scan_seg_bytes(&log, &index)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            log,
            index,
            seg_bytes,
        })
    }

    /// 写入一个对象(无过期)。key 已存在(含墓碑)则报 [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。
    pub fn put(&mut self, key: StoreKey, data: &[u8]) -> Result<()> {
        self.put_ttl(key, data, NO_EXPIRATION)
    }

    /// 写入一个对象并设定过期时间(`expiration_ms`,`NO_EXPIRATION` 表示不过期)。
    ///
    /// 对齐 Ambry:blob id 永久占用——命中任何已存在条目(PUT 或 DELETE 墓碑)一律
    /// [`Error::AlreadyExists`](crate::error::Error::AlreadyExists);复活已删除对象须走
    /// undelete/lifeVersion(后续实现),普通 put 不允许删后重写。
    pub fn put_ttl(&mut self, key: StoreKey, data: &[u8], expiration_ms: i64) -> Result<()> {
        if self.index.find(&key).is_some() {
            return Err(Error::AlreadyExists);
        }
        let op_time = now_ms();
        let record = encode_record(&key, 0, expiration_ms, op_time, 0, data);
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.expiration_ms = expiration_ms;
        self.index.add(key, value)?;
        self.seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        Ok(())
    }

    /// 读取一个对象。不存在/已删除/已过期分别返回对应错误。
    pub fn get(&self, key: &StoreKey) -> Result<Vec<u8>> {
        self.get_with_options(key, false, false)
    }

    /// `get` 的可选版本(对齐 Ambry `StoreGetOptions`):`include_deleted`/`include_expired`
    /// 为真时分别返回已删除/已过期对象的数据。已删除对象的数据从其原 PUT 读取,若 PUT 已被
    /// compaction 回收则报 [`NotFound`](crate::error::Error::NotFound)。
    pub fn get_with_options(
        &self,
        key: &StoreKey,
        include_deleted: bool,
        include_expired: bool,
    ) -> Result<Vec<u8>> {
        let value = self.index.find(key).ok_or(Error::NotFound)?;
        if value.is_deleted() && !include_deleted {
            return Err(Error::Deleted);
        }
        if value.expiration_ms != NO_EXPIRATION
            && now_ms() as i64 >= value.expiration_ms
            && !include_expired
        {
            return Err(Error::Expired);
        }
        // 墓碑无数据,取原 PUT 位置;其余赢家(PUT/UNDELETE/TTL_UPDATE)自带数据。
        let data_off = if value.is_deleted() {
            value.original_offset
        } else {
            value.offset
        };
        let (_, decoded) = read_full_record(&self.log, data_off).map_err(|_| Error::NotFound)?;
        if &decoded.key != key {
            return Err(Error::Corrupt(format!("record key mismatch at {data_off}")));
        }
        if !value.is_deleted() && decoded.flags & FLAG_DELETED != 0 {
            return Err(Error::Corrupt(format!(
                "live index points at a tombstone record at {data_off}"
            )));
        }
        Ok(decoded.data)
    }

    /// 查询 blob 元数据(状态/TTL/生命版本/数据长度),**不读数据体**(对齐 Ambry `findKey`)。
    /// key 不存在报 [`NotFound`](crate::error::Error::NotFound);已删除/已过期仍返回其元数据。
    pub fn stat(&self, key: &StoreKey) -> Result<BlobMeta> {
        let v = self.index.find(key).ok_or(Error::NotFound)?;
        Ok(BlobMeta {
            deleted: v.is_deleted(),
            undeleted: v.is_undeleted(),
            ttl_updated: v.is_ttl_updated(),
            expired: v.expiration_ms != NO_EXPIRATION && now_ms() as i64 >= v.expiration_ms,
            expiration_ms: v.expiration_ms,
            life_version: v.life_version,
            data_len: v.size.saturating_sub(RECORD_OVERHEAD as u64),
        })
    }

    /// 已用日志字节(存活 + 死亡),对齐 Ambry `getSizeInBytes`。
    pub fn size_in_bytes(&self) -> u64 {
        self.seg_bytes.values().map(|v| v.live + v.dead).sum()
    }

    /// 库是否为空(尚无任何记录)。
    pub fn is_empty(&self) -> bool {
        self.size_in_bytes() == 0
    }

    /// 硬删除:把已软删(墓碑)对象的原 PUT 数据字节**就地清零**(对齐 Ambry `HardDeleter` 的隐私擦除),
    /// 在 compaction 回收前即抹除明文。返回被擦除的对象数;幂等(已擦除的跳过)。
    pub fn hard_delete(&mut self) -> Result<usize> {
        let mut puts: Vec<LogOffset> = Vec::new();
        for_each_record(&self.log, |off, _raw, decoded| {
            if decoded.flags & FLAG_DELETED != 0 {
                if let Some(v) = self.index.find(&decoded.key) {
                    if v.is_deleted() && v.offset == off {
                        puts.push(v.original_offset);
                    }
                }
            }
            Ok(())
        })?;
        let mut scrubbed = 0usize;
        for po in puts {
            if self.scrub_put(po)? {
                scrubbed += 1;
            }
        }
        if scrubbed > 0 {
            self.log.flush_all()?;
        }
        Ok(scrubbed)
    }

    /// 把 `at` 处 PUT 记录的数据清零、置 `FLAG_HARD_DELETED` 并重算 crc,整条写回。
    /// PUT 已被回收/非 PUT/已擦除则跳过。返回是否实际擦除。
    fn scrub_put(&mut self, at: LogOffset) -> Result<bool> {
        let (mut raw, decoded) = match read_full_record(&self.log, at) {
            Ok(x) => x,
            Err(_) => return Ok(false),
        };
        if decoded.flags & (FLAG_DELETED | FLAG_HARD_DELETED) != 0 {
            return Ok(false);
        }
        let crc_at = rec::DATA + decoded.data.len();
        for b in raw[rec::DATA..crc_at].iter_mut() {
            *b = 0;
        }
        raw[rec::FLAGS] |= FLAG_HARD_DELETED;
        let crc = crc32fast::hash(&raw[..crc_at]);
        raw[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
        self.log.overwrite_at(at, &raw)?;
        Ok(true)
    }

    /// 删除一个对象:追加墓碑记录并在索引写入删除条目。不存在/已删除报错。
    pub fn delete(&mut self, key: StoreKey) -> Result<()> {
        let old = match self.index.find(&key) {
            None => return Err(Error::NotFound),
            Some(v) if v.is_deleted() => return Err(Error::Deleted),
            Some(v) => v,
        };
        let op_time = now_ms();
        let record = encode_record(
            &key,
            FLAG_DELETED,
            NO_EXPIRATION,
            op_time,
            old.life_version,
            &[],
        );
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.flags = FLAG_DELETED;
        value.life_version = old.life_version;
        value.original_offset = old.original_offset; // 墓碑无数据,指向被删前的 PUT
        self.index.add(key, value)?;
        // 墓碑成为新赢家(live);被取代的旧 PUT 转为该段的死数据。
        self.seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        let e = self.seg_bytes.entry(old.offset.segment).or_default();
        e.live = e.live.saturating_sub(old.size);
        e.dead += old.size;
        Ok(())
    }

    /// 复活一个已删除对象(对齐 Ambry `undelete`):写一条 `FLAG_UNDELETED`、复制原数据、
    /// `life_version+1` 的整记录并指回它自身,返回新的 life_version。
    ///
    /// 状态机:仅当当前为**已删除**才可复活;不存在报 [`NotFound`](crate::error::Error::NotFound),
    /// 未删除(存活/已复活)报 [`NotDeleted`](crate::error::Error::NotDeleted)。若被删对象的 PUT
    /// 已被 compaction 回收(数据不存在),报 [`NotFound`](crate::error::Error::NotFound)
    /// (对齐 Ambry `IDDeletedPermanently`:PUT 被压走后不可复活)。
    pub fn undelete(&mut self, key: StoreKey) -> Result<u16> {
        let cur = match self.index.find(&key) {
            None => return Err(Error::NotFound),
            Some(v) if !v.is_deleted() => return Err(Error::NotDeleted),
            Some(v) => v,
        };
        // 取原 PUT 数据(墓碑 original_offset 指向它)。被 compaction 回收则视为数据不存在。
        let (put_raw, put) =
            read_full_record(&self.log, cur.original_offset).map_err(|_| Error::NotFound)?;
        if put.key != key || put.flags & FLAG_DELETED != 0 {
            return Err(Error::NotFound);
        }
        let _ = put_raw;
        let new_life = cur.life_version + 1;
        let op_time = now_ms();
        let record = encode_record(
            &key,
            FLAG_UNDELETED,
            put.expiration_ms,
            op_time,
            new_life,
            &put.data,
        );
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.flags = FLAG_UNDELETED;
        value.expiration_ms = put.expiration_ms;
        value.life_version = new_life;
        self.index.add(key, value)?;
        // 复活整记录成为新赢家(live);被取代的旧墓碑转死(原 PUT 早在 delete 时已计死)。
        self.seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        let e = self.seg_bytes.entry(cur.offset.segment).or_default();
        e.live = e.live.saturating_sub(cur.size);
        e.dead += cur.size;
        Ok(new_life)
    }

    /// 移除已存在对象的过期时间(置为永久),对齐 Ambry `updateTtl`(仅支持移除 TTL)。
    ///
    /// 写一条 `FLAG_TTL_UPDATED`、复制原数据、`expiration=NO_EXPIRATION`、`life_version` 不变的
    /// 整记录成为新赢家。不存在→[`NotFound`](crate::error::Error::NotFound);已删除→
    /// [`Deleted`](crate::error::Error::Deleted);TTL 已更新→[`AlreadyUpdated`](crate::error::Error::AlreadyUpdated)。
    pub fn update_ttl(&mut self, key: StoreKey) -> Result<()> {
        let cur = match self.index.find(&key) {
            None => return Err(Error::NotFound),
            Some(v) if v.is_deleted() => return Err(Error::Deleted),
            Some(v) if v.is_ttl_updated() => return Err(Error::AlreadyUpdated),
            Some(v) => v,
        };
        // 未删除的赢家(PUT/UNDELETE)自带数据,从其 offset 读回原数据。
        let (_, rec) = read_full_record(&self.log, cur.offset)?;
        let op_time = now_ms();
        let flags = (cur.flags | FLAG_TTL_UPDATED) & !FLAG_DELETED;
        let record = encode_record(
            &key,
            flags,
            NO_EXPIRATION,
            op_time,
            cur.life_version,
            &rec.data,
        );
        let offset = self.log.append(&record)?;
        let mut value = IndexValue::put(offset, record.len() as u64, op_time);
        value.flags = flags;
        value.expiration_ms = NO_EXPIRATION;
        value.life_version = cur.life_version;
        self.index.add(key, value)?;
        self.seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        let e = self.seg_bytes.entry(cur.offset.segment).or_default();
        e.live = e.live.saturating_sub(cur.size);
        e.dead += cur.size;
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

    #[cfg(test)]
    fn life_version_of(&self, key: &StoreKey) -> Option<u16> {
        self.index.find(key).map(|v| v.life_version)
    }

    #[cfg(test)]
    fn expiration_of(&self, key: &StoreKey) -> Option<i64> {
        self.index.find(key).map(|v| v.expiration_ms)
    }

    /// 返回全库存活/死亡字节统计(读增量计数,死 = 被更新版本取代/删除的旧记录)。
    pub fn stats(&self) -> Result<CompactionStats> {
        let mut live = 0u64;
        let mut dead = 0u64;
        for v in self.seg_bytes.values() {
            live += v.live;
            dead += v.dead;
        }
        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: self.log.segment_count(),
            segments_after: self.log.segment_count(),
        })
    }

    #[cfg(test)]
    fn stats_by_scan(&self) -> CompactionStats {
        let m = scan_seg_bytes(&self.log, &self.index).unwrap();
        let (mut live, mut dead) = (0u64, 0u64);
        for v in m.values() {
            live += v.live;
            dead += v.dead;
        }
        CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: self.log.segment_count(),
            segments_after: self.log.segment_count(),
        }
    }

    /// 周期性 GC 入口:对**每个非活跃段**按其死数据占比 ≥ `threshold` 增量压缩(热/末段不动),
    /// 对齐 Ambry StatsBased 的按段触发,而非全库 CompactAll。无达阈段返回 `None`。
    /// 需要全库重写时显式调用 [`compact`](Self::compact)。
    pub fn compact_if_needed(&mut self, threshold: f64) -> Result<Option<CompactionStats>> {
        self.compact_incremental(threshold)
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
        self.seg_bytes = scan_seg_bytes(&self.log, &self.index)?;
        let _ = std::fs::remove_file(&marker);
        rm_rf(&log_old)?;

        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: before,
            segments_after: after,
        })
    }

    /// 每个封存段(不含活跃/末段)的 `(position, live_bytes, dead_bytes)`,读增量计数。
    pub fn segment_stats(&self) -> Result<Vec<(u64, u64, u64)>> {
        let active_pos = self.log.segment_spans().last().map(|(p, _)| *p);
        let mut out = Vec::new();
        for (&pos, v) in &self.seg_bytes {
            if Some(pos) == active_pos {
                continue;
            }
            out.push((pos, v.live, v.dead));
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

        let mut next_pos = self
            .log
            .segment_spans()
            .iter()
            .map(|(p, _)| *p)
            .max()
            .unwrap()
            + 1;
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
        self.seg_bytes = scan_seg_bytes(&self.log, &self.index)?;
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

/// 读取 `at` 处一条完整记录(先读头拿 data_len,再读整条并校验 crc)。
fn read_full_record(log: &Log, at: LogOffset) -> Result<(Vec<u8>, DecodedRecord)> {
    let head = log.read(at, rec::DATA)?;
    let data_len =
        u32::from_le_bytes(head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
    let total = RECORD_OVERHEAD + data_len;
    let raw = log.read(at, total)?;
    let decoded = decode_record(&raw)?;
    Ok((raw, decoded))
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
        let at = LogOffset::new(position, off);
        let (raw, decoded) = read_full_record(log, at)?;
        off += raw.len() as u64;
        f(at, &raw, &decoded)?;
    }
    Ok(())
}

/// 从一条记录构造索引值,继承 `life_version`;墓碑(无数据)的 `original_offset` 沿用该 key
/// 已入索引的数据位置(扫描旧→新,PUT/UNDELETE 先入),否则指向自身。
fn index_value_from_record(
    index: &PersistentIndex,
    off: LogOffset,
    raw_len: usize,
    decoded: &DecodedRecord,
) -> IndexValue {
    let mut value = IndexValue::put(off, raw_len as u64, decoded.operation_time_ms);
    value.flags = decoded.flags;
    value.expiration_ms = decoded.expiration_ms;
    value.life_version = decoded.life_version;
    value.original_offset = if decoded.flags & FLAG_DELETED != 0 {
        index
            .find(&decoded.key)
            .map(|v| v.original_offset)
            .unwrap_or(off)
    } else {
        off
    };
    value
}

/// 全量扫 log 计算每段 `(live, dead)` 字节。冷路径:open/compact/rebuild 后重算,及测试对拍基准。
fn scan_seg_bytes(log: &Log, index: &PersistentIndex) -> Result<BTreeMap<u64, SegBytes>> {
    let mut m: BTreeMap<u64, SegBytes> = BTreeMap::new();
    for (pos, _) in log.segment_spans() {
        m.entry(pos).or_default();
    }
    for_each_record(log, |off, raw, decoded| {
        let e = m.entry(off.segment).or_default();
        if matches!(index.find(&decoded.key), Some(v) if v.offset == off) {
            e.live += raw.len() as u64;
        } else {
            e.dead += raw.len() as u64;
        }
        Ok(())
    })?;
    Ok(m)
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
        let value = index_value_from_record(&index, off, raw.len(), decoded);
        index.add(decoded.key, value)?;
        Ok(())
    })?;
    index.flush()?;
    // 重建覆盖了整个 log,水位推进到当前末尾。
    write_watermark(index_dir, log.end_offset())?;
    Ok(index)
}

/// 加载已落盘索引并按水位回放 log 尾部(增量恢复,非全量重建);`Corrupt` 由调用方回退重建。
fn load_index_with_replay(
    log: &Log,
    index_dir: &Path,
    max_entries: usize,
) -> Result<PersistentIndex> {
    let mut index = PersistentIndex::open(index_dir, max_entries)?;
    let watermark = read_watermark(index_dir).unwrap_or(LogOffset::new(0, 0));
    replay_from(log, &mut index, watermark)?;
    Ok(index)
}

/// 按持久化水位回放 log 尾部:把 offset ≥ watermark 的记录补进索引(旧→新,保 newest-wins)。
fn replay_from(log: &Log, index: &mut PersistentIndex, watermark: LogOffset) -> Result<()> {
    for_each_record(log, |off, raw, decoded| {
        if off >= watermark {
            let value = index_value_from_record(index, off, raw.len(), decoded);
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
        if ent
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with(".tmp"))
        {
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

        let big: Vec<u8> = (0..(BLOCK * 3 + 77) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
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
    fn delete_then_reput_rejected() {
        let dir = temp_dir("del");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).unwrap();
        store.put(key(1), b"v1").unwrap();
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert!(matches!(store.delete(key(1)), Err(Error::Deleted)));
        assert!(matches!(store.delete(key(2)), Err(Error::NotFound)));

        // 对齐 Ambry:blob id 永久占用,删除后普通 put 仍报 AlreadyExists(复活须走 undelete)。
        assert!(matches!(
            store.put(key(1), b"v2"),
            Err(Error::AlreadyExists)
        ));
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
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
            store.delete(key(7)).unwrap(); // key 7:PUT + DELETE 墓碑 → newest-wins 应为 Deleted
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
            if i == 7 {
                assert!(
                    matches!(store.get(&key(7)), Err(Error::Deleted)),
                    "key 7 tombstone after rebuild"
                );
                continue;
            }
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("v-{i}").into_bytes(),
                "key {i} after rebuild"
            );
        }
        assert!(matches!(store.get(&key(999)), Err(Error::NotFound)));

        drop(store);
        let store2 = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        assert!(matches!(store2.get(&key(7)), Err(Error::Deleted)));
        drop(store2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_falls_back_to_rebuild_on_corrupt_index() {
        let dir = temp_dir("corrupt_idx");
        let seg = seg_cap(16);
        {
            let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
            for i in 0..20u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            store.delete(key(3)).unwrap();
            store.close().unwrap();
        }

        // 损坏活跃索引段(`{seq}.idx` 且无配套 `{seq}.fuse`,open 时走 crc 校验的整段载入)。
        let index_dir = dir.join("index");
        let mut active_idx = None;
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            let is_idx = p.extension().is_some_and(|e| e == "idx");
            let has_fuse = p.with_extension("fuse").exists();
            if is_idx && !has_fuse {
                active_idx = Some(p);
                break;
            }
        }
        std::fs::write(active_idx.expect("active idx exists"), b"garbage").unwrap();

        // open 检测到 Corrupt → 自动扫 log 重建;逻辑状态仍正确。
        let store = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        for i in 0..20u64 {
            if i == 3 {
                assert!(
                    matches!(store.get(&key(3)), Err(Error::Deleted)),
                    "key 3 stays deleted"
                );
                continue;
            }
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("v-{i}").into_bytes(),
                "key {i} recovered"
            );
        }
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_roundtrip_and_crc_guard() {
        let k = key(42);
        let data = b"payload".to_vec();
        let mut buf = encode_record(&k, 0, 1_700_000_000_000, 1_699_000_000_000, 7, &data);
        let d = decode_record(&buf).unwrap();
        assert_eq!(d.flags, 0);
        assert_eq!(d.key, k);
        assert_eq!(d.data, data);
        assert_eq!(d.expiration_ms, 1_700_000_000_000);
        assert_eq!(d.operation_time_ms, 1_699_000_000_000);
        assert_eq!(d.life_version, 7);

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

        // 死数据来源(Ambry 语义下无覆盖/无删后重写):delete 使原 PUT 变死,墓碑成为新赢家。
        for i in 0..10u64 {
            store
                .put(key(i), format!("original-value-{i}").as_bytes())
                .unwrap();
        }
        for i in 0..5u64 {
            store.delete(key(i)).unwrap(); // key 0..4 变墓碑(原 PUT 变死数据)
        }

        let before = store.stats().unwrap();
        assert!(before.dead_bytes > 0, "should have dead data");

        let st = store.compact().unwrap();
        assert!(st.dead_bytes > 0);
        // compact 后死数据应清零(全库只剩赢家:5..9 的 PUT + 0..4 的墓碑)。
        let after = store.stats().unwrap();
        assert_eq!(after.dead_bytes, 0, "no dead data after compaction");

        // 逻辑状态不变:0..4 已删,5..9 是原值。
        for i in 0..5u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::Deleted)),
                "key {i} must stay deleted"
            );
        }
        for i in 5..10u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("original-value-{i}").into_bytes(),
                "key {i} value must survive compaction"
            );
        }

        // 重开(会经过 recover_compaction 无标记路径)仍一致。
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 16, seg, 4).unwrap();
        for i in 5..10u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("original-value-{i}").into_bytes()
            );
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
        assert_eq!(
            store.get(&key(2)).unwrap(),
            b"b".to_vec(),
            "tail key(2) recovered via replay"
        );
        assert_eq!(
            store.get(&key(3)).unwrap(),
            b"c".to_vec(),
            "tail key(3) recovered via replay"
        );
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
        assert!(
            st.segments_after <= before_segs,
            "segment count should not grow"
        );

        // 逻辑状态不变:0..29 已删,30..39 存活。
        for i in 0..30u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::Deleted)),
                "key {i} deleted"
            );
        }
        for i in 30..40u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                big(&format!("v{i}")),
                "key {i} live"
            );
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

    #[test]
    fn incremental_stats_match_full_scan() {
        let dir = temp_dir("stats_incr");
        let seg = seg_cap(2);
        let mut store = BlobStore::create(&dir, seg * 64, seg, 8).unwrap();
        for i in 0..40u64 {
            store.put(key(i), format!("value-{i}").as_bytes()).unwrap();
        }
        for i in 0..15u64 {
            store.delete(key(i)).unwrap();
        }
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after put/delete"
        );

        store.compact_if_needed(0.3).unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after incremental compaction"
        );

        store.compact().unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after CompactAll"
        );

        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 64, seg, 8).unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after reopen recompute"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn undelete_restores_deleted_blob_and_bumps_lifeversion() {
        let dir = temp_dir("undel");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        store.put(key(1), b"hello").unwrap();
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));

        assert_eq!(
            store.undelete(key(1)).unwrap(),
            1,
            "undelete bumps life_version to 1"
        );
        assert_eq!(
            store.get(&key(1)).unwrap(),
            b"hello".to_vec(),
            "data restored"
        );
        assert_eq!(store.life_version_of(&key(1)), Some(1));

        // 复活后可再删、再复活,life_version 继续递增。
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert_eq!(store.undelete(key(1)).unwrap(), 2);
        assert_eq!(store.get(&key(1)).unwrap(), b"hello".to_vec());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn undelete_rejects_non_deleted_states() {
        let dir = temp_dir("undel_rej");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        assert!(
            matches!(store.undelete(key(1)), Err(Error::NotFound)),
            "absent"
        );
        store.put(key(1), b"v").unwrap();
        assert!(
            matches!(store.undelete(key(1)), Err(Error::NotDeleted)),
            "live key"
        );
        store.delete(key(1)).unwrap();
        store.undelete(key(1)).unwrap();
        assert!(
            matches!(store.undelete(key(1)), Err(Error::NotDeleted)),
            "already undeleted"
        );
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn undelete_survives_reopen_and_rebuild() {
        let dir = temp_dir("undel_reopen");
        let seg = seg_cap(16);
        {
            let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
            for i in 0..10u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            store.delete(key(3)).unwrap();
            store.undelete(key(3)).unwrap();
            store.close().unwrap();
        }
        let store = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(
            store.get(&key(3)).unwrap(),
            b"v-3".to_vec(),
            "watermark replay keeps undelete"
        );
        assert_eq!(store.life_version_of(&key(3)), Some(1));
        store.close().unwrap();
        let store = BlobStore::open_rebuild_index(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(
            store.get(&key(3)).unwrap(),
            b"v-3".to_vec(),
            "log rebuild keeps undelete"
        );
        assert_eq!(store.life_version_of(&key(3)), Some(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn undeleted_blob_survives_compaction() {
        let dir = temp_dir("undel_compact");
        let seg = seg_cap(2);
        let mut store = BlobStore::create(&dir, seg * 64, seg, 8).unwrap();
        for i in 0..20u64 {
            store.put(key(i), format!("data-{i}").as_bytes()).unwrap();
        }
        store.delete(key(5)).unwrap();
        store.undelete(key(5)).unwrap();
        store.compact().unwrap();
        assert_eq!(
            store.get(&key(5)).unwrap(),
            b"data-5".to_vec(),
            "undeleted data survives compaction"
        );
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 64, seg, 8).unwrap();
        assert_eq!(store.get(&key(5)).unwrap(), b"data-5".to_vec());
        assert_eq!(store.life_version_of(&key(5)), Some(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn undelete_after_put_reclaimed_by_compaction_is_permanent() {
        let dir = temp_dir("undel_perm");
        let seg = seg_cap(2);
        let mut store = BlobStore::create(&dir, seg * 64, seg, 8).unwrap();
        for i in 0..30u64 {
            store.put(key(i), &[b'x'; 200]).unwrap();
        }
        store.delete(key(0)).unwrap();
        store.compact().unwrap(); // 回收死 PUT(含 key0 被删前的原 PUT)
        assert!(matches!(store.get(&key(0)), Err(Error::Deleted)));
        // 原 PUT 数据已被回收 → 不可复活(对齐 Ambry IDDeletedPermanently)。
        assert!(matches!(store.undelete(key(0)), Err(Error::NotFound)));
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_ttl_removes_expiration() {
        let dir = temp_dir("ttl_upd");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        // 过期时间设在过去 → get 立即 Expired。
        store.put_ttl(key(1), b"payload", 1).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Expired)));

        store.update_ttl(key(1)).unwrap();
        assert_eq!(
            store.get(&key(1)).unwrap(),
            b"payload".to_vec(),
            "TTL removed → readable"
        );
        assert_eq!(store.expiration_of(&key(1)), Some(NO_EXPIRATION));

        // 幂等冲突:再次更新报 AlreadyUpdated。
        assert!(matches!(
            store.update_ttl(key(1)),
            Err(Error::AlreadyUpdated)
        ));

        // 重开 + 扫 log 重建都保留永久 TTL。
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"payload".to_vec());
        assert_eq!(store.expiration_of(&key(1)), Some(NO_EXPIRATION));
        store.close().unwrap();
        let store = BlobStore::open_rebuild_index(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(store.expiration_of(&key(1)), Some(NO_EXPIRATION));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_ttl_rejects_absent_and_deleted() {
        let dir = temp_dir("ttl_rej");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        assert!(
            matches!(store.update_ttl(key(1)), Err(Error::NotFound)),
            "absent"
        );
        store.put(key(1), b"v").unwrap();
        store.delete(key(1)).unwrap();
        assert!(
            matches!(store.update_ttl(key(1)), Err(Error::Deleted)),
            "deleted"
        );
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stat_get_options_and_introspection() {
        let dir = temp_dir("stat");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        assert!(store.is_empty());
        assert!(matches!(store.stat(&key(1)), Err(Error::NotFound)));

        store.put(key(1), b"hello").unwrap();
        let m = store.stat(&key(1)).unwrap();
        assert!(!m.deleted && !m.expired && m.life_version == 0);
        assert_eq!(
            m.data_len, 5,
            "payload length reported without reading data"
        );
        assert!(!store.is_empty() && store.size_in_bytes() > 0);

        // 删除后 stat 仍可查状态;get 默认拒,include_deleted 从原 PUT 读回数据。
        store.delete(key(1)).unwrap();
        assert!(store.stat(&key(1)).unwrap().deleted);
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert_eq!(
            store.get_with_options(&key(1), true, false).unwrap(),
            b"hello".to_vec()
        );

        // 过期对象:stat 标记 expired;include_expired 可读。
        store.put_ttl(key(2), b"temp", 1).unwrap();
        assert!(store.stat(&key(2)).unwrap().expired);
        assert!(matches!(store.get(&key(2)), Err(Error::Expired)));
        assert_eq!(
            store.get_with_options(&key(2), false, true).unwrap(),
            b"temp".to_vec()
        );
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hard_delete_scrubs_deleted_blob_data() {
        let dir = temp_dir("hard");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 8, seg, 4).unwrap();
        store.put(key(1), b"secret-data").unwrap();
        store.put(key(2), b"keep-this").unwrap();
        store.delete(key(1)).unwrap();
        // 删除后 include_deleted 仍能读到明文。
        assert_eq!(
            store.get_with_options(&key(1), true, false).unwrap(),
            b"secret-data".to_vec()
        );

        assert_eq!(store.hard_delete().unwrap(), 1, "one deleted blob scrubbed");
        // 原数据被清零;未删除的 key(2) 不受影响;幂等。
        assert_eq!(
            store.get_with_options(&key(1), true, false).unwrap(),
            vec![0u8; 11]
        );
        assert_eq!(store.get(&key(2)).unwrap(), b"keep-this".to_vec());
        assert_eq!(store.hard_delete().unwrap(), 0, "idempotent");

        // 重开后擦除持久。
        store.close().unwrap();
        let store = BlobStore::open(&dir, seg * 8, seg, 4).unwrap();
        assert_eq!(
            store.get_with_options(&key(1), true, false).unwrap(),
            vec![0u8; 11]
        );
        assert_eq!(store.get(&key(2)).unwrap(), b"keep-this".to_vec());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 差分对拍:纯内存模型记录每个 key 的期望状态,随机穿插 put/delete 与
    /// flush/reopen/compact/compact_incremental,每步全量比对 `get()` 与模型。覆盖
    /// newest-wins、墓碑、删后重写、跨段封存、compaction offset 重写、水位回放重开、
    /// 扫 log 全量重建——把已实现的读写/持久化/压缩路径拧在一起验证一致性。
    #[derive(Clone, PartialEq, Eq)]
    enum ModelState {
        Absent,
        Live(Vec<u8>),
        Deleted,
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn data_for(k: u64, ver: u64) -> Vec<u8> {
        let len = 4 + (ver % 13) as usize;
        let mut v = vec![0u8; len];
        for (i, b) in v.iter_mut().enumerate() {
            *b = (k.wrapping_mul(31).wrapping_add(ver).wrapping_add(i as u64) & 0xff) as u8;
        }
        v
    }

    fn verify_all(store: &BlobStore, model: &[ModelState], ctx: &str) {
        for (k, state) in model.iter().enumerate() {
            let got = store.get(&key(k as u64));
            match state {
                ModelState::Absent => {
                    assert!(
                        matches!(got, Err(Error::NotFound)),
                        "{ctx}: key {k} expect NotFound, got {got:?}"
                    );
                }
                ModelState::Deleted => {
                    assert!(
                        matches!(got, Err(Error::Deleted)),
                        "{ctx}: key {k} expect Deleted, got {got:?}"
                    );
                }
                ModelState::Live(data) => {
                    let got =
                        got.unwrap_or_else(|e| panic!("{ctx}: key {k} expect Live, got {e:?}"));
                    assert_eq!(&got, data, "{ctx}: key {k} data mismatch");
                }
            }
        }
    }

    #[test]
    fn reference_model_full_crosscheck() {
        const KEYS: usize = 24;
        let seg = seg_cap(2); // 小段 + 小封存阈值 → 频繁滚段/封存,吃满 mmap+fuse 封存路径与多段扫描
        let total = seg * 256;
        let max_idx = 8usize;

        for &seed in &[0x1234_5678u64, 0xdead_beefu64, 0x0BAD_F00Du64] {
            let dir = temp_dir(&format!("model_{seed:x}"));
            let mut store = BlobStore::create(&dir, total, seg, max_idx).unwrap();
            let mut model = vec![ModelState::Absent; KEYS];
            let mut rng = Rng(seed);
            let mut ver = 0u64;

            for step in 0..400u64 {
                match rng.below(100) {
                    0..=44 => {
                        let k = rng.below(KEYS as u64);
                        ver += 1;
                        let data = data_for(k, ver);
                        let res = store.put(key(k), &data);
                        match &model[k as usize] {
                            ModelState::Absent => {
                                res.unwrap_or_else(|e| {
                                    panic!("step {step}: put absent key {k} failed: {e:?}")
                                });
                                model[k as usize] = ModelState::Live(data);
                            }
                            _ => {
                                assert!(
                                    matches!(res, Err(Error::AlreadyExists)),
                                    "step {step}: put existing key {k} must reject"
                                );
                            }
                        }
                    }
                    45..=64 => {
                        let k = rng.below(KEYS as u64);
                        let res = store.delete(key(k));
                        match &model[k as usize] {
                            ModelState::Absent => assert!(
                                matches!(res, Err(Error::NotFound)),
                                "step {step}: del absent key {k}"
                            ),
                            ModelState::Deleted => assert!(
                                matches!(res, Err(Error::Deleted)),
                                "step {step}: del deleted key {k}"
                            ),
                            ModelState::Live(_) => {
                                res.unwrap_or_else(|e| {
                                    panic!("step {step}: delete key {k} failed: {e:?}")
                                });
                                model[k as usize] = ModelState::Deleted;
                            }
                        }
                    }
                    65..=76 => store.flush().unwrap(),
                    77..=86 => {
                        store.close().unwrap();
                        store = BlobStore::open(&dir, total, seg, max_idx).unwrap();
                        verify_all(&store, &model, &format!("step {step} after reopen"));
                    }
                    87..=92 => {
                        store.compact().unwrap();
                        verify_all(&store, &model, &format!("step {step} after compact"));
                    }
                    _ => {
                        store.compact_incremental(0.2).unwrap();
                        verify_all(
                            &store,
                            &model,
                            &format!("step {step} after compact_incremental"),
                        );
                    }
                }
                verify_all(&store, &model, &format!("step {step}"));
            }

            verify_all(&store, &model, "final live");
            assert_eq!(
                store.stats().unwrap(),
                store.stats_by_scan(),
                "seg_bytes drift vs full scan"
            );
            store.close().unwrap();

            let reopened = BlobStore::open(&dir, total, seg, max_idx).unwrap();
            verify_all(&reopened, &model, "final reopen");
            reopened.close().unwrap();

            let rebuilt = BlobStore::open_rebuild_index(&dir, total, seg, max_idx).unwrap();
            verify_all(&rebuilt, &model, "final rebuild-from-log");
            rebuilt.close().unwrap();

            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
