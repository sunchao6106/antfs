//! 索引段:活跃段封存后的不可变有序段。
//!
//! - 内存态 [`SealedSegment`]:按 key 排序的 `(StoreKey, IndexValue)` 数组 + Binary Fuse 过滤器;
//!   查找先过滤器,命中再二分。
//! - 磁盘态:定长头 + 排序条目;用同步 `std::fs`(小元数据,非热路径)。
//!   过滤器不落盘,加载时由全部 key 一次性重建(契合"封存即不变")。

use std::path::Path;

use xorf::{BinaryFuse8, Filter};

use crate::error::{Error, Result};
use crate::index::value::{IndexValue, VALUE_SIZE};
use crate::types::{StoreKey, KEY_SIZE};

const IDX_MAGIC: u32 = 0x414D_4258; // "AMBX"
const IDX_VERSION: u16 = 1;
const IDX_HEADER_SIZE: usize = 32;
const ENTRY_SIZE: usize = KEY_SIZE + VALUE_SIZE;

/// 从磁盘读回的原始段(尚未构建过滤器)。
pub(crate) struct LoadedSegment {
    pub sealed: bool,
    pub entries: Vec<(StoreKey, IndexValue)>,
}

/// 不可变的封存段:排序条目 + Binary Fuse 过滤器。
pub(crate) struct SealedSegment {
    seq: u64,
    entries: Vec<(StoreKey, IndexValue)>,
    filter: BinaryFuse8,
}

impl SealedSegment {
    pub fn from_entries(seq: u64, entries: Vec<(StoreKey, IndexValue)>) -> Result<Self> {
        let filter = build_filter(&entries)?;
        Ok(Self {
            seq,
            entries,
            filter,
        })
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn find(&self, key: &StoreKey) -> Option<IndexValue> {
        if !self.filter.contains(&key.hash64()) {
            return None;
        }
        match self.entries.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(i) => Some(self.entries[i].1),
            Err(_) => None,
        }
    }
}

fn build_filter(entries: &[(StoreKey, IndexValue)]) -> Result<BinaryFuse8> {
    let mut hashes: Vec<u64> = entries.iter().map(|(k, _)| k.hash64()).collect();
    // Binary Fuse 构造要求 key 唯一;不同 StoreKey 若散列碰撞,去重后只会带来
    // 极小概率的假阳性(退化为一次二分),不影响索引正确性。
    hashes.sort_unstable();
    hashes.dedup();
    BinaryFuse8::try_from(hashes).map_err(|e| Error::Corrupt(format!("build fuse filter: {e}")))
}

/// 把排序条目写入索引段文件(同步)。
pub(crate) fn write_segment_file(
    path: &Path,
    sealed: bool,
    entries: &[(StoreKey, IndexValue)],
) -> Result<()> {
    let count = entries.len();
    let body_len = count * ENTRY_SIZE;
    let mut buf = vec![0u8; IDX_HEADER_SIZE + body_len];

    let mut body_crc = crc32fast::Hasher::new();
    for (i, (k, v)) in entries.iter().enumerate() {
        let off = IDX_HEADER_SIZE + i * ENTRY_SIZE;
        buf[off..off + KEY_SIZE].copy_from_slice(k.as_bytes());
        buf[off + KEY_SIZE..off + ENTRY_SIZE].copy_from_slice(&v.to_bytes());
        body_crc.update(&buf[off..off + ENTRY_SIZE]);
    }
    let body_crc = body_crc.finalize();

    buf[0..4].copy_from_slice(&IDX_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&IDX_VERSION.to_le_bytes());
    buf[6] = sealed as u8;
    buf[8..10].copy_from_slice(&(KEY_SIZE as u16).to_le_bytes());
    buf[10..12].copy_from_slice(&(VALUE_SIZE as u16).to_le_bytes());
    buf[12..16].copy_from_slice(&(count as u32).to_le_bytes());
    buf[16..20].copy_from_slice(&body_crc.to_le_bytes());
    let header_crc = crc32fast::hash(&buf[0..20]);
    buf[20..24].copy_from_slice(&header_crc.to_le_bytes());

    // 原子替换:写临时文件 + rename。
    let tmp = path.with_extension("idx.tmp");
    std::fs::write(&tmp, &buf)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 读取并校验索引段文件(同步)。
pub(crate) fn read_segment_file(path: &Path) -> Result<LoadedSegment> {
    let buf = std::fs::read(path)?;
    if buf.len() < IDX_HEADER_SIZE {
        return Err(Error::Corrupt(format!("index file too short: {}", path.display())));
    }

    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != IDX_MAGIC {
        return Err(Error::Corrupt(format!("bad index magic {magic:#x}")));
    }
    let header_crc = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    if crc32fast::hash(&buf[0..20]) != header_crc {
        return Err(Error::Corrupt("index header crc mismatch".into()));
    }

    let sealed = buf[6] != 0;
    let key_size = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
    let value_size = u16::from_le_bytes(buf[10..12].try_into().unwrap()) as usize;
    if key_size != KEY_SIZE || value_size != VALUE_SIZE {
        return Err(Error::Corrupt(format!(
            "index layout mismatch: key={key_size} value={value_size}"
        )));
    }
    let count = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    let body_crc = u32::from_le_bytes(buf[16..20].try_into().unwrap());

    let body_len = count * ENTRY_SIZE;
    if buf.len() < IDX_HEADER_SIZE + body_len {
        return Err(Error::Corrupt(format!("index body truncated: {}", path.display())));
    }
    let body = &buf[IDX_HEADER_SIZE..IDX_HEADER_SIZE + body_len];
    if crc32fast::hash(body) != body_crc {
        return Err(Error::Corrupt("index body crc mismatch".into()));
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * ENTRY_SIZE;
        let key = StoreKey::from_slice(&body[off..off + KEY_SIZE]).expect("key size checked");
        let val = IndexValue::from_bytes(&body[off + KEY_SIZE..off + ENTRY_SIZE])
            .expect("value size checked");
        entries.push((key, val));
    }

    Ok(LoadedSegment {
        sealed,
        entries,
    })
}
