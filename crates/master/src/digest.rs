//! 副本上报摘要(M7 增量心跳):store 与 master **共用同一个哈希口径**。
//!
//! 口径对齐 SeaweedFS 的 `VolumeDigest`:摘要覆盖节点**全部持有**的副本(含本拍未随增量上报的),
//! 逐副本哈希后 **XOR 合成**(顺序无关)。master 按自己的软状态重算,与心跳携带值比对,不符即回
//! `resend_full_report` 要求下一拍全量重发 —— 增量丢项/错项都能在一拍内自愈。
//!
//! 用手写 FNV-1a 而非 `DefaultHasher`:后者不承诺跨版本稳定的输出,而这里 store 与 master 是独立
//! 编译的两个进程,必须逐位一致。

use crate::types::PartitionId;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv_update(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// 单条副本上报的哈希:`(partition_id, disk_id, current_state, lag_bytes, used_bytes)` 全字段参与,
/// 任何一项变化都反映到摘要。
pub fn replica_beat_hash(
    partition_id: PartitionId,
    disk_id: &str,
    current_state: &str,
    lag_bytes: u64,
    used_bytes: u64,
) -> u64 {
    let mut h = FNV_OFFSET;
    h = fnv_update(h, &partition_id.to_le_bytes());
    h = fnv_update(h, disk_id.as_bytes());
    h = fnv_update(h, &[0xff]); // 字段分隔,防拼接歧义
    h = fnv_update(h, current_state.as_bytes());
    h = fnv_update(h, &lag_bytes.to_le_bytes());
    h = fnv_update(h, &used_bytes.to_le_bytes());
    h
}

/// XOR 合成:顺序无关(遍历序不必对齐),且与 SeaweedFS digest 同思路。
pub fn combine_digest(acc: u64, h: u64) -> u64 {
    acc ^ h
}

/// 一组副本上报的摘要:覆盖**全部持有**副本(不只是本拍上报的那些),否则 master 无从判断
/// "应用完这拍增量后是否与节点一致"。空集摘要为 0(与"什么都没上报过"区分不开也无妨:
/// 全量重发会带上 `full_report` 标志)。
pub fn digest_of<'a>(
    entries: impl IntoIterator<Item = (PartitionId, &'a str, &'a str, u64, u64)>,
) -> u64 {
    let mut acc = 0u64;
    for (pid, disk_id, state, lag, used) in entries {
        acc = combine_digest(acc, replica_beat_hash(pid, disk_id, state, lag, used));
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同一输入逐位一致(跨进程契约:store 与 master 独立编译也必须算得同一个值)。
    #[test]
    fn hash_is_stable() {
        let a = replica_beat_hash(7, "disk-1", "ACTIVE", 0, 1024);
        let b = replica_beat_hash(7, "disk-1", "ACTIVE", 0, 1024);
        assert_eq!(a, b);
    }

    /// 任一字段变化都改变哈希。
    #[test]
    fn every_field_feeds_the_hash() {
        let base = replica_beat_hash(7, "disk-1", "ACTIVE", 0, 1024);
        assert_ne!(base, replica_beat_hash(8, "disk-1", "ACTIVE", 0, 1024));
        assert_ne!(base, replica_beat_hash(7, "disk-2", "ACTIVE", 0, 1024));
        assert_ne!(base, replica_beat_hash(7, "disk-1", "BOOTSTRAP", 0, 1024));
        assert_ne!(base, replica_beat_hash(7, "disk-1", "ACTIVE", 1, 1024));
        assert_ne!(base, replica_beat_hash(7, "disk-1", "ACTIVE", 0, 2048));
    }

    /// 合成顺序无关:同一集合不同遍历序得到同一摘要。
    #[test]
    fn digest_is_order_independent() {
        let entries = [
            (1u32, "d1", "ACTIVE", 0u64, 100u64),
            (2, "d2", "BOOTSTRAP", 50, 200),
            (3, "d1", "ACTIVE", 0, 300),
        ];
        let forward = digest_of(entries.iter().copied());
        let backward = digest_of(entries.iter().rev().copied());
        assert_eq!(forward, backward);
    }

    /// 集合变化(增/删/改一项)摘要必变 —— 这是 master 检测"增量应用后与节点不一致"的依据。
    #[test]
    fn set_changes_change_the_digest() {
        let before = digest_of([(1u32, "d1", "ACTIVE", 0u64, 100u64)]);
        let grown = digest_of([
            (1u32, "d1", "ACTIVE", 0u64, 100u64),
            (2, "d2", "ACTIVE", 0, 200),
        ]);
        let changed = digest_of([(1u32, "d1", "ACTIVE", 0u64, 101u64)]);
        assert_ne!(before, grown);
        assert_ne!(before, changed);
        assert_eq!(digest_of([]), 0, "空集摘要为 0");
    }
}
