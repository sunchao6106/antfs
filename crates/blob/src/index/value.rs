//! `IndexValue`:索引条目的值,定长序列化到磁盘。
//!
//! 布局(小端,共 21B):
//! ```text
//! [ offset(8) | size(4) | flags(1) | expiration_ms(8) ]
//! ```
//! - `offset`:记录位置,`LogOffset::pack()`(段号 u32 ‖ 段内偏移 u32);
//! - `size`:**整条记录**的字节数(记录头 + 数据体 + crc),用于跳过/统计;段 ≤ 4 GiB 故 u32 够;
//! - `flags`:目前只有 `DELETED`(墓碑没有数据体);
//! - `expiration_ms`:TTL,`NO_EXPIRATION` = 不过期。
//!
//! 索引的 key 是 8B blobId(`KEY_SIZE`),即每条目 29B。

use crate::types::LogOffset;

/// 该 key 的当前赢家是墓碑(对象已删除,数据体不存在)。
pub const FLAG_DELETED: u8 = 1 << 0;

/// 无过期时间。
pub const NO_EXPIRATION: i64 = -1;

/// 定长值大小(字节):offset(8) + size(4) + flags(1) + expiration(8)。
pub const VALUE_SIZE: usize = 21;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexValue {
    /// 记录位置(段号 + 段内偏移)。
    pub offset: LogOffset,
    /// 整条记录的字节数(含记录头与尾部 crc)。
    pub size: u32,
    pub flags: u8,
    pub expiration_ms: i64,
}

impl IndexValue {
    pub fn put(offset: LogOffset, size: u32) -> Self {
        Self {
            offset,
            size,
            flags: 0,
            expiration_ms: NO_EXPIRATION,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }

    /// `size` 的 u64 视图(统计累加用)。
    #[inline]
    pub fn size_bytes(&self) -> u64 {
        u64::from(self.size)
    }

    pub fn to_bytes(&self) -> [u8; VALUE_SIZE] {
        let mut b = [0u8; VALUE_SIZE];
        b[0..8].copy_from_slice(&self.offset.pack().to_le_bytes());
        b[8..12].copy_from_slice(&self.size.to_le_bytes());
        b[12] = self.flags;
        b[13..21].copy_from_slice(&self.expiration_ms.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < VALUE_SIZE {
            return None;
        }
        Some(Self {
            offset: LogOffset::unpack(u64::from_le_bytes(b[0..8].try_into().unwrap())),
            size: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            flags: b[12],
            expiration_ms: i64::from_le_bytes(b[13..21].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_roundtrip() {
        let v = IndexValue {
            offset: LogOffset::new(3, 4096),
            size: 12345,
            flags: FLAG_DELETED,
            expiration_ms: 1_700_000_000_000,
        };
        let b = v.to_bytes();
        assert_eq!(b.len(), VALUE_SIZE);
        assert_eq!(b.len(), 21);
        assert_eq!(IndexValue::from_bytes(&b).unwrap(), v);
        assert!(v.is_deleted());
    }

    #[test]
    fn offset_packs_segment_and_offset_without_loss() {
        let v = IndexValue::put(LogOffset::new(0xDEAD_BEEF, 0x0102_0304), 7);
        let got = IndexValue::from_bytes(&v.to_bytes()).unwrap();
        assert_eq!(got.offset, LogOffset::new(0xDEAD_BEEF, 0x0102_0304));
        assert_eq!(got.size, 7);
        assert!(!got.is_deleted());
        assert_eq!(got.expiration_ms, NO_EXPIRATION);
    }

    #[test]
    fn from_bytes_rejects_short_slice() {
        assert!(IndexValue::from_bytes(&[0u8; VALUE_SIZE - 1]).is_none());
    }
}
