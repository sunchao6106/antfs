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
use crate::types::{StoreKey, KEY_SIZE};

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

/// 对象存储:单写者(`&mut self` 写路径)。数据走 Log(io_uring+O_DIRECT),索引走 std::fs。
pub struct BlobStore {
    log: Log,
    index: PersistentIndex,
}

impl BlobStore {
    /// 在 `dir` 下创建新库:数据在 `dir/log`,索引在 `dir/index`。
    pub async fn create(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let (log_dir, index_dir) = sub_dirs(dir.as_ref());
        let log = Log::create(&log_dir, total_capacity, segment_capacity).await?;
        let index = PersistentIndex::create(&index_dir, max_index_entries_per_segment)?;
        Ok(Self { log, index })
    }

    /// 打开已有库并恢复索引与日志。
    pub async fn open(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let (log_dir, index_dir) = sub_dirs(dir.as_ref());
        let log = Log::open(&log_dir, total_capacity, segment_capacity).await?;
        let index = PersistentIndex::open(&index_dir, max_index_entries_per_segment)?;
        Ok(Self { log, index })
    }

    /// 写入一个对象。key 已存在且未删除则报 [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。
    pub async fn put(&mut self, key: StoreKey, data: &[u8]) -> Result<()> {
        if let Some(v) = self.index.find(&key) {
            if !v.is_deleted() {
                return Err(Error::AlreadyExists);
            }
        }
        let record = encode_record(&key, 0, data);
        let offset = self.log.append(&record).await?;
        self.index
            .add(key, IndexValue::put(offset, record.len() as u64, now_ms()))?;
        Ok(())
    }

    /// 读取一个对象。不存在/已删除/已过期分别返回对应错误。
    pub async fn get(&self, key: &StoreKey) -> Result<Vec<u8>> {
        let value = self.index.find(key).ok_or(Error::NotFound)?;
        if value.is_deleted() {
            return Err(Error::Deleted);
        }
        if value.expiration_ms != NO_EXPIRATION && now_ms() as i64 >= value.expiration_ms {
            return Err(Error::Expired);
        }
        let raw = self.log.read(value.offset, value.size as usize).await?;
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
    pub async fn delete(&mut self, key: StoreKey) -> Result<()> {
        match self.index.find(&key) {
            None => return Err(Error::NotFound),
            Some(v) if v.is_deleted() => return Err(Error::Deleted),
            Some(_) => {}
        }
        let record = encode_record(&key, FLAG_DELETED, &[]);
        let offset = self.log.append(&record).await?;
        let mut value = IndexValue::put(offset, record.len() as u64, now_ms());
        value.flags = FLAG_DELETED;
        self.index.add(key, value)?;
        Ok(())
    }

    /// 持久化:先落数据(fsync)再落索引,保证索引不指向未落盘的数据。
    pub async fn flush(&mut self) -> Result<()> {
        self.log.flush().await?;
        self.index.flush()?;
        Ok(())
    }

    /// flush 后关闭底层日志文件。
    pub async fn close(mut self) -> Result<()> {
        self.flush().await?;
        self.log.close().await
    }
}

fn sub_dirs(dir: &Path) -> (PathBuf, PathBuf) {
    (dir.join("log"), dir.join("index"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aligned::ALIGN;

    const HEADER: u64 = ALIGN as u64;

    fn temp_dir(tag: &str) -> PathBuf {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(format!("/var/tmp/ambry_blob_{tag}_{ns}"))
    }

    fn seg_cap(blocks: u64) -> u64 {
        HEADER + blocks * ALIGN as u64
    }

    fn key(n: u64) -> StoreKey {
        let mut b = [0u8; KEY_SIZE];
        b[0..8].copy_from_slice(&n.to_le_bytes());
        b[24..32].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        StoreKey::new(b)
    }

    #[monoio::test]
    async fn put_get_roundtrip() {
        let dir = temp_dir("rt");
        let seg = seg_cap(64);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).await.unwrap();

        let data = b"hello ambry blob".to_vec();
        store.put(key(1), &data).await.unwrap();
        assert_eq!(store.get(&key(1)).await.unwrap(), data);

        let big: Vec<u8> = (0..(ALIGN * 3 + 77)).map(|i| (i % 251) as u8).collect();
        store.put(key(2), &big).await.unwrap();
        assert_eq!(store.get(&key(2)).await.unwrap(), big);

        assert!(matches!(store.get(&key(999)).await, Err(Error::NotFound)));
        store.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[monoio::test]
    async fn put_existing_rejected() {
        let dir = temp_dir("dup");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).await.unwrap();
        store.put(key(1), b"a").await.unwrap();
        assert!(matches!(store.put(key(1), b"b").await, Err(Error::AlreadyExists)));
        store.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[monoio::test]
    async fn delete_then_get_and_reput() {
        let dir = temp_dir("del");
        let seg = seg_cap(16);
        let mut store = BlobStore::create(&dir, seg * 4, seg, 1000).await.unwrap();
        store.put(key(1), b"v1").await.unwrap();
        store.delete(key(1)).await.unwrap();
        assert!(matches!(store.get(&key(1)).await, Err(Error::Deleted)));
        assert!(matches!(store.delete(key(1)).await, Err(Error::Deleted)));
        assert!(matches!(store.delete(key(2)).await, Err(Error::NotFound)));

        // 删除后允许重新 put(旧条目是 deleted)。
        store.put(key(1), b"v2").await.unwrap();
        assert_eq!(store.get(&key(1)).await.unwrap(), b"v2".to_vec());
        store.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[monoio::test]
    async fn reopen_recovers_blobs() {
        let dir = temp_dir("reopen");
        let seg = seg_cap(16);
        {
            let mut store = BlobStore::create(&dir, seg * 8, seg, 4).await.unwrap();
            for i in 0..20u64 {
                store.put(key(i), format!("blob-{i}").as_bytes()).await.unwrap();
            }
            store.delete(key(3)).await.unwrap();
            store.close().await.unwrap();
        }
        let store = BlobStore::open(&dir, seg * 8, seg, 4).await.unwrap();
        for i in 0..20u64 {
            if i == 3 {
                assert!(matches!(store.get(&key(i)).await, Err(Error::Deleted)));
            } else {
                assert_eq!(
                    store.get(&key(i)).await.unwrap(),
                    format!("blob-{i}").into_bytes(),
                    "key {i} lost after reopen"
                );
            }
        }
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
}
