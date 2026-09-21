//! Log/索引共用的基础类型。

use std::fmt;

/// 日志段名(位置序号)。文件名形如 `{position}_log.seg`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogSegmentName(pub u64);

impl LogSegmentName {
    pub fn filename(&self) -> String {
        format!("{}_log.seg", self.0)
    }

    /// 从文件名解析位置序号。
    pub fn parse(filename: &str) -> Option<LogSegmentName> {
        let pos = filename.strip_suffix("_log.seg")?;
        pos.parse::<u64>().ok().map(LogSegmentName)
    }
}

impl fmt::Display for LogSegmentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 跨段的全局偏移:段位置 + 段内逻辑偏移(数据区,字节粒度)。
/// 这将是 `PersistentIndex` 引用一条记录的地址。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogOffset {
    pub segment: u64,
    pub offset: u64,
}

impl LogOffset {
    pub fn new(segment: u64, offset: u64) -> Self {
        Self { segment, offset }
    }
}

impl fmt::Display for LogOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.segment, self.offset)
    }
}

/// 对象键长度(紧凑 BlobId,如 version+flags+dc+account+container+partition+uuid ≈ 32B)。
pub const KEY_SIZE: usize = 32;

/// 定长对象键(索引的 key)。写后不改,天然适合排序段 + Binary Fuse 过滤器。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreKey([u8; KEY_SIZE]);

impl StoreKey {
    pub fn new(bytes: [u8; KEY_SIZE]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(s: &[u8]) -> Option<Self> {
        (s.len() == KEY_SIZE).then(|| {
            let mut b = [0u8; KEY_SIZE];
            b.copy_from_slice(s);
            Self(b)
        })
    }

    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.0
    }

    /// FNV-1a 64 位散列,供 Binary Fuse 过滤器使用(进程内一致即可)。
    pub fn hash64(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut h = OFFSET;
        for &b in &self.0 {
            h ^= b as u64;
            h = h.wrapping_mul(PRIME);
        }
        h
    }
}

impl fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreKey(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}
