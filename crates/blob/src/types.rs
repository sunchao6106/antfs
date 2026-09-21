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

/// 对象键长度 = blobId 定长 8B(64bit)。见 blobid-master-assign D8:高 32bit `partitionId`
/// + 低 32bit `objectId`,无 version 位(D7,布局永久冻结)。
pub const KEY_SIZE: usize = 8;

/// 定长对象键 = blobId(索引的 key)。写后不改,天然适合排序段 + Binary Fuse 过滤器。
///
/// **字节序=大端**(blobid-master-assign D8/D11 不变量):8 字节按大端存 `u64`,故
/// 字节字典序 == 数值序 == `(partitionId, objectId)` 序 —— 同一 partition 的 key 按 objectId
/// 排序,索引最大 key 的低 32bit 即该 partition 已用最大 objectId(D11 灾备回填水位所依赖)。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreKey([u8; KEY_SIZE]);

impl StoreKey {
    pub fn new(bytes: [u8; KEY_SIZE]) -> Self {
        Self(bytes)
    }

    /// 由 `(partitionId, objectId)` 组合 blobId:`(partition as u64) << 32 | object`,大端落盘。
    pub fn from_parts(partition_id: u32, object_id: u32) -> Self {
        let v = ((partition_id as u64) << 32) | (object_id as u64);
        Self(v.to_be_bytes())
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

    /// blobId 的 64bit 数值视图(大端解码)。
    pub fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.0)
    }

    /// 高 32bit:定位用的 `partitionId`(自带定位,D3)。
    pub fn partition_id(&self) -> u32 {
        (self.as_u64() >> 32) as u32
    }

    /// 低 32bit:partition 内单调发放的 `objectId`。
    pub fn object_id(&self) -> u32 {
        (self.as_u64() & 0xFFFF_FFFF) as u32
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobid_codec_roundtrips_parts() {
        let k = StoreKey::from_parts(0x0102_0304, 0x0A0B_0C0D);
        assert_eq!(k.partition_id(), 0x0102_0304);
        assert_eq!(k.object_id(), 0x0A0B_0C0D);
        assert_eq!(k.as_u64(), 0x0102_0304_0A0B_0C0D);
    }

    #[test]
    fn blobid_big_endian_layout() {
        let k = StoreKey::from_parts(1, 2);
        assert_eq!(k.as_bytes(), &[0, 0, 0, 1, 0, 0, 0, 2]);
    }

    #[test]
    fn blobid_extremes() {
        let k = StoreKey::from_parts(u32::MAX, u32::MAX);
        assert_eq!(k.partition_id(), u32::MAX);
        assert_eq!(k.object_id(), u32::MAX);
        assert_eq!(k.as_u64(), u64::MAX);
    }

    #[test]
    fn byte_order_equals_partition_then_object_order() {
        // D11 不变量:字节字典序 == (partitionId, objectId) 数值序。
        let same_part_lo = StoreKey::from_parts(5, 10);
        let same_part_hi = StoreKey::from_parts(5, 11);
        let next_part = StoreKey::from_parts(6, 0);
        assert!(same_part_lo < same_part_hi);
        assert!(same_part_hi < next_part);
    }

    #[test]
    fn from_slice_enforces_key_size() {
        assert!(StoreKey::from_slice(&[0u8; KEY_SIZE]).is_some());
        assert!(StoreKey::from_slice(&[0u8; KEY_SIZE - 1]).is_none());
        assert!(StoreKey::from_slice(&[0u8; KEY_SIZE + 1]).is_none());
    }
}
