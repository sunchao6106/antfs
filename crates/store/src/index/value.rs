//! `IndexValue`:索引条目的值,定长序列化到磁盘。

use crate::types::LogOffset;

pub const FLAG_DELETED: u8 = 1 << 0;
pub const FLAG_TTL_UPDATED: u8 = 1 << 1;
pub const FLAG_UNDELETED: u8 = 1 << 2;

/// 无过期时间。
pub const NO_EXPIRATION: i64 = -1;

/// 定长值大小(字节):offset(16) + size(8) + flags(1) + expiration(8) + op_time(8)
/// + life_version(2) + original_offset(16)。
pub const VALUE_SIZE: usize = 59;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexValue {
    pub offset: LogOffset,
    pub size: u64,
    pub flags: u8,
    pub expiration_ms: i64,
    pub operation_time_ms: u64,
    pub life_version: u16,
    /// 承载 blob 数据的记录位置(读/复活取数据用)。PUT/UNDELETE 记录自带数据故 == `offset`;
    /// DELETE 墓碑无数据,指向被删前的 PUT。
    pub original_offset: LogOffset,
}

impl IndexValue {
    pub fn put(offset: LogOffset, size: u64, operation_time_ms: u64) -> Self {
        Self {
            offset,
            size,
            flags: 0,
            expiration_ms: NO_EXPIRATION,
            operation_time_ms,
            life_version: 0,
            original_offset: offset,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }

    pub fn is_undeleted(&self) -> bool {
        self.flags & FLAG_UNDELETED != 0
    }

    pub fn is_ttl_updated(&self) -> bool {
        self.flags & FLAG_TTL_UPDATED != 0
    }

    pub fn to_bytes(&self) -> [u8; VALUE_SIZE] {
        let mut b = [0u8; VALUE_SIZE];
        b[0..8].copy_from_slice(&self.offset.segment.to_le_bytes());
        b[8..16].copy_from_slice(&self.offset.offset.to_le_bytes());
        b[16..24].copy_from_slice(&self.size.to_le_bytes());
        b[24] = self.flags;
        b[25..33].copy_from_slice(&self.expiration_ms.to_le_bytes());
        b[33..41].copy_from_slice(&self.operation_time_ms.to_le_bytes());
        b[41..43].copy_from_slice(&self.life_version.to_le_bytes());
        b[43..51].copy_from_slice(&self.original_offset.segment.to_le_bytes());
        b[51..59].copy_from_slice(&self.original_offset.offset.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < VALUE_SIZE {
            return None;
        }
        Some(Self {
            offset: LogOffset::new(
                u64::from_le_bytes(b[0..8].try_into().unwrap()),
                u64::from_le_bytes(b[8..16].try_into().unwrap()),
            ),
            size: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            flags: b[24],
            expiration_ms: i64::from_le_bytes(b[25..33].try_into().unwrap()),
            operation_time_ms: u64::from_le_bytes(b[33..41].try_into().unwrap()),
            life_version: u16::from_le_bytes(b[41..43].try_into().unwrap()),
            original_offset: LogOffset::new(
                u64::from_le_bytes(b[43..51].try_into().unwrap()),
                u64::from_le_bytes(b[51..59].try_into().unwrap()),
            ),
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
            flags: FLAG_DELETED | FLAG_TTL_UPDATED,
            expiration_ms: 1_700_000_000_000,
            operation_time_ms: 1_699_000_000_000,
            life_version: 2,
            original_offset: LogOffset::new(1, 512),
        };
        let b = v.to_bytes();
        assert_eq!(b.len(), VALUE_SIZE);
        assert_eq!(IndexValue::from_bytes(&b).unwrap(), v);
        assert!(v.is_deleted());
    }
}
