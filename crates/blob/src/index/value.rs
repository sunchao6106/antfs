//! `IndexValue`:索引条目的值,定长序列化到磁盘。
//!
//! 布局(小端,共 21B):
//! ```text
//! [ offset(8) | size(4) | flags(1) | expiration_ms(8) ]
//! ```
//! - `offset`:记录位置,`LogOffset::pack()`(段号 u32 ‖ 段内偏移 u32);
//! - `size`:**整条记录**的字节数(记录头 + 数据体 + crc),用于跳过/统计;段 ≤ 4 GiB 故 u32 够;
//! - `flags`:`DELETED`(墓碑)与 `COMPOSED`(拼装描述符类型位);位 1-3 预留;
//! - `expiration_ms`:非墓碑 = TTL(`NO_EXPIRATION` = 不过期);**墓碑 = 删除时刻 `deleted_at_ms`**
//!   (复用同一槽位,布局不变 → 无需索引格式迁移;`NO_EXPIRATION` = 删除时间未知,永不自动回收)。
//!
//! 索引的 key 是 8B blobId(`KEY_SIZE`),即每条目 29B。

use crate::types::LogOffset;

/// 该 key 的当前赢家是墓碑(对象已删除,数据体不存在)。
pub const FLAG_DELETED: u8 = 1 << 0;

/// 该记录是**拼装描述符**(frontend stitch/auto-chunk 写下的组合对象清单,
/// `s3` 外另一形态)。类型位,不改变数据的存储与读取;仅在网关置信读路径消费
/// ——此前网关靠 MAGIC 嗅探,普通 blob 撞前缀即失真(`blob-composed-flag`)。
/// 位段 1-3 预留(本 change 只启用 4)。
pub const FLAG_COMPOSED: u8 = 1 << 4;

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

    pub fn is_composed(&self) -> bool {
        self.flags & FLAG_COMPOSED != 0
    }

    /// 该条目的 TTL 语义值:**墓碑恒为[`NO_EXPIRATION`]**(墓碑的 `expiration_ms` 槽位存的是删除时刻,
    /// 不是 TTL)。对外汇报 TTL/过期判定一律走这个口径,避免把删除时间当成过期时刻。
    pub fn ttl_expiration_ms(&self) -> i64 {
        if self.is_deleted() {
            NO_EXPIRATION
        } else {
            self.expiration_ms
        }
    }

    /// 墓碑的删除时刻(仅墓碑有意义);`None` = 删除时间未知(旧数据),永不被自动回收。
    pub fn deleted_at_ms(&self) -> Option<i64> {
        (self.is_deleted() && self.expiration_ms != NO_EXPIRATION).then_some(self.expiration_ms)
    }

    /// 非墓碑且 TTL 已到(`now >= expiration`)。
    pub fn is_expired_at(&self, now_ms: i64) -> bool {
        !self.is_deleted() && self.expiration_ms != NO_EXPIRATION && now_ms >= self.expiration_ms
    }

    /// 墓碑已满保留期(`now − deleted_at >= retention_ms`)。删除时间未知(`-1`)或保留期为
    /// [`i64::MAX`] 时恒为 false —— 回收方向永远"宁可不回收"。
    pub fn is_reclaimable_tombstone(&self, now_ms: i64, retention_ms: i64) -> bool {
        match self.deleted_at_ms() {
            Some(at) => now_ms.saturating_sub(at) >= retention_ms,
            None => false,
        }
    }

    /// compaction 可回收:TTL 已过期,或墓碑已过保留期。
    ///
    /// 这是「死数据」在 compaction 里的完整口径 —— 除了"被更新版本取代的旧记录"(那份字节由
    /// `seg_bytes.dead` 记账,不经本方法),赢家里只有这两类是真正不再需要保留的字节。
    pub fn is_reclaimable_at(&self, now_ms: i64, retention_ms: i64) -> bool {
        self.is_expired_at(now_ms) || self.is_reclaimable_tombstone(now_ms, retention_ms)
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

    /// composed 位:置位 roundtrip 保位;未置位(旧格式值)解出 not composed;与
    /// DELETED 位互不干扰(§3.4 双向兼容的值级用例)。
    #[test]
    fn composed_flag_roundtrip() {
        let mut v = IndexValue::put(LogOffset::new(1, 2), 999);
        assert!(!v.is_composed());
        v.flags = FLAG_COMPOSED;
        let got = IndexValue::from_bytes(&v.to_bytes()).expect("roundtrip");
        assert!(got.is_composed());
        assert!(!got.is_deleted());

        let legacy = IndexValue::from_bytes(&IndexValue::put(LogOffset::new(0, 0), 1).to_bytes())
            .expect("legacy decode");
        assert!(!legacy.is_composed());
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

    /// 本模块的条目构造器(`crate::index::tests::put` 不可见)。
    fn entry() -> IndexValue {
        IndexValue::put(LogOffset::new(0, 0), 100)
    }

    /// 墓碑的 `expiration_ms` 槽位存删除时刻:TTL 口径必须仍报 `NO_EXPIRATION`。
    #[test]
    fn tombstone_reuses_expiration_slot_for_deleted_at() {
        let now = 1_700_000_000_000;
        let mut del = entry();
        del.flags = FLAG_DELETED;
        del.expiration_ms = now - 3_600_000;

        assert_eq!(del.ttl_expiration_ms(), NO_EXPIRATION);
        assert_eq!(del.deleted_at_ms(), Some(now - 3_600_000));
        assert!(!del.is_expired_at(now), "墓碑不参与 TTL 过期判定");
        assert!(del.is_reclaimable_tombstone(now, 1_800_000));
        assert!(!del.is_reclaimable_tombstone(now, 7_200_000), "未满保留期");
    }

    /// 旧数据的墓碑(`expiration_ms = -1`)删除时间未知 → 永不回收。
    #[test]
    fn legacy_tombstone_with_unknown_delete_time_is_never_reclaimed() {
        let mut del = entry();
        del.flags = FLAG_DELETED;
        assert_eq!(del.deleted_at_ms(), None);
        assert!(!del.is_reclaimable_tombstone(i64::MAX / 2, 0));
        assert!(!del.is_reclaimable_at(i64::MAX / 2, 0));
    }

    #[test]
    fn expiration_and_reclaimability_by_ttl() {
        let now = 1_700_000_000_000;
        let mut live = entry();
        live.expiration_ms = now + 1;
        assert!(!live.is_expired_at(now));
        assert!(!live.is_reclaimable_at(now, 0));

        live.expiration_ms = now;
        assert!(live.is_expired_at(now), "now >= expiration 即过期");
        assert!(live.is_reclaimable_at(now, 0));

        let no_ttl = entry();
        assert_eq!(no_ttl.ttl_expiration_ms(), NO_EXPIRATION);
        assert!(!no_ttl.is_expired_at(i64::MAX / 2), "不过期永不回收");
    }
}
