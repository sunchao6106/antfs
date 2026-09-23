//! Log/索引共用的基础类型。

use std::fmt;

/// 日志段身份 = **位置序号 + 代次(generation)**。文件名形如 `{position}_{generation}_log.seg`。
///
/// - `position`:段在本 log 内的逻辑序号(排序、残段容量计算用),全库 compact 后从 0 重来;
/// - `generation`:同一 log 目录内**单调递增**(每建新段 `max(gen)+1`),且**跨全库 compact 继续递增**
///   (新目录从旧目录的最大代次之后起算)。
///
/// 两者合起来构成"段身份",在整个副本历史上**永不复用** —— 于是"段被重写/删掉/换掉"必然表现为
/// **认不出的段身份**,复制侧"认不出即从头重扫"因此是充分的(对齐 Ambry 段名里的 generation;
/// 见 `docs/design/store.md` 的"复制游标重定位")。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogSegmentName {
    pub position: u32,
    pub generation: u32,
}

impl LogSegmentName {
    /// 由位置与代次构造;超出 u32 直接 panic —— 段地址是结构性不变量
    /// (段内偏移 < 段大小 ≤ 4 GiB,段数与代次都远不到 2³²),宁可崩也不落下截断后的错身份。
    pub fn new(position: u64, generation: u64) -> Self {
        Self {
            position: u32::try_from(position).expect("segment position must fit u32"),
            generation: u32::try_from(generation).expect("segment generation must fit u32"),
        }
    }

    pub fn filename(&self) -> String {
        format!("{}_{}_log.seg", self.position, self.generation)
    }

    /// 从文件名解析段身份(`{position}_{generation}_log.seg`)。
    pub fn parse(filename: &str) -> Option<LogSegmentName> {
        let rest = filename.strip_suffix("_log.seg")?;
        let (pos, gen) = rest.split_once('_')?;
        Some(LogSegmentName {
            position: pos.parse().ok()?,
            generation: gen.parse().ok()?,
        })
    }
}

impl fmt::Display for LogSegmentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.position, self.generation)
    }
}

/// 跨段的全局偏移:段位置序号 + 段内逻辑偏移(数据区,字节粒度)。
/// 这将是 `PersistentIndex` 引用一条记录的地址。
///
/// 注:`segment` 是**位置序号**,不含代次 —— 代次只活在**段名**与**复制游标**里(见
/// [`LogSegmentName`]),这样索引值保持 21B 定长布局不变,而"段号被复用"仍能被识别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogOffset {
    pub segment: u64,
    pub offset: u64,
}

impl LogOffset {
    pub fn new(segment: u64, offset: u64) -> Self {
        Self { segment, offset }
    }

    /// 打包成单个 u64:高 32bit 段号、低 32bit 段内偏移。供索引条目定长落盘(见
    /// [`IndexValue`](crate::index::IndexValue))。
    ///
    /// 两个分量都在 u32 内是**结构性不变量**:段内偏移 < 段大小 ≤ 4 GiB(`StoreConfig::validate`
    /// 强制),段号 ≥ 2³² 需要 2³² × 段大小 的总容量(不可达)。违反即 panic —— 宁可崩,
    /// 也不把截断后的错地址落盘(持久性红线)。
    pub fn pack(self) -> u64 {
        let seg = u32::try_from(self.segment).expect("segment index must fit u32");
        let off =
            u32::try_from(self.offset).expect("in-segment offset must fit u32 (segment ≤ 4 GiB)");
        ((seg as u64) << 32) | (off as u64)
    }

    /// [`pack`](Self::pack) 的逆。
    pub fn unpack(v: u64) -> Self {
        Self {
            segment: (v >> 32) & 0xFFFF_FFFF,
            offset: v & 0xFFFF_FFFF,
        }
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
