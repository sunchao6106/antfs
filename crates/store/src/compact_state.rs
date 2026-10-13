//! Hybrid compaction 调度状态的**持久化**:记录上一次 CompactAll 的时间戳。
//!
//! 为什么需要落盘:Hybrid 策略要求「每隔 `compaction_policy_switch_days` 天穿插一次 CompactAll」。
//! 若时间戳只留内存,进程一重启就丢失 → 重启后每轮都误判"从没跑过"而反复触发全库重写(CPU/IO 浪费)。
//! 故把它按 **replica 目录**落一个小 JSON 文件(与副本数据同盘,不引入额外状态文件目录)。
//!
//! 崩溃安全口径与其他落盘路径一致(CLAUDE.md §3.1):**tmp → write → fsync → rename**,
//! 旧值要么完整、要么被完整替换,不留半截文件。

use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// 状态文件名(落在各 replica 目录下)。
const STATE_FILE: &str = "COMPACT_POLICY_STATE";

#[derive(Serialize, Deserialize)]
struct State {
    last_compact_all_epoch_secs: i64,
}

/// 读取该 replica 目录下最近一次 CompactAll 的 unix 秒。
///
/// 文件缺失 / 为空 / 内容损坏都返回 `None`(安全方向:当作"从没跑过",宁可多跑一次全库重写,
/// 也不可谎报"刚跑过"而让 compaction 长期不触发)。
pub fn read_last_compact_all(dir: &Path) -> Option<i64> {
    let path = dir.join(STATE_FILE);
    let content = std::fs::read_to_string(&path).ok()?;
    let state: State = serde_json::from_str(&content).ok()?;
    Some(state.last_compact_all_epoch_secs)
}

/// 写入该 replica 目录下最近一次 CompactAll 的 unix 秒。
///
/// 崩溃安全:**tmp → write → fsync → rename**(§3.1)—— rename 是原子替换,读者永远看不到半截文件。
pub fn write_last_compact_all(dir: &Path, epoch_secs: i64) -> std::io::Result<()> {
    let path = dir.join(STATE_FILE);
    let tmp = dir.join(format!("{STATE_FILE}.tmp"));
    // 只含一个 i64 字段的结构体序列化不会失败;但仍走 `?` 而非 panic(生产路径不得 panic)。
    let content = serde_json::to_string(&State {
        last_compact_all_epoch_secs: epoch_secs,
    })
    .map_err(std::io::Error::other)?;
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        write_last_compact_all(tmp.path(), 1_700_000_000).unwrap();
        assert_eq!(read_last_compact_all(tmp.path()), Some(1_700_000_000));
    }

    #[test]
    fn missing_file_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read_last_compact_all(tmp.path()), None);
    }

    #[test]
    fn corrupt_file_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(STATE_FILE), b"{not json").unwrap();
        assert_eq!(read_last_compact_all(tmp.path()), None);
    }

    #[test]
    fn overwrite_updates_value() {
        let tmp = tempfile::tempdir().unwrap();
        write_last_compact_all(tmp.path(), 100).unwrap();
        write_last_compact_all(tmp.path(), 200).unwrap();
        assert_eq!(read_last_compact_all(tmp.path()), Some(200));
    }
}
