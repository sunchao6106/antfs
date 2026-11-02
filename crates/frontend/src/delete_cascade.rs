//! 组合对象**删除级联账本**(H3 修复,2026-10-26 裁决):
//!
//! REST/blob 模式没有 metastore,组合对象(描述符 + 片)删除时只墓碑描述符会**永久泄漏片**。
//! 本模块 = 删除路径的级联账:
//!
//! 1. **删除时**([`AppState`] 的 `delete_blob`):先 `stat` 确认组合位 → 读描述符(≤ 1 MiB
//!    有界)解析片名单 → [`DeleteCascade::record`] **追加一行并 fsync**,然后才墓碑主描述符。
//!    顺序是持久性纪律:**账先于墓碑落盘** —— 反过来(墓碑先行)会在两步之间崩溃时退回
//!    H3 泄漏;账先行则任何时刻崩溃,重启后账都在。
//! 2. **每日后台清扫**([`DeleteCascade::sweep`]):消费账本。**二次确认**是对"账先于
//!    墓碑"崩溃窗的兜底:账里说"该级联它",但墓碑可能没写成(对象还活着)——消费侧必须
//!    `stat` 确认描述符**确实已删**才动片;活着的对象**跳过**(下一轮再看),超过
//!    [`STALE_AFTER_MS`] 仍活着(客户端放弃删除/账是手工投的)则丢弃该行并告警 ——
//!    宁可退化不级联,绝不误删活对象。
//!
//! 幂等与安全:片墓碑 `router.delete` 幂等(重复删成功、不存在按成功);
//! 账本重写走 **tmp → fsync → rename**(dir 亦 fsync)的原子替换(§3.3);
//! 带 TTL 的对象本可自然过期,级联只是**更早**回收,语义叠合无冲突。
//!
//! **磁盘格式零变更**:不新增任何 blob/索引字段,组合位复用既有 `composed`。

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Mutex;

use antfs_router::Router as BlobRouter;
use serde::{Deserialize, Serialize};

use crate::metrics::FrontendMetrics;
use antfs_router::StoreKey;

/// 账本里一行仍"活着"的最大滞留时长:删除路径先记账后墓碑,若墓碑始终没落成
/// (进程死在两步之间且客户端放弃重删),该行既不能误删也不能永留账上 —— 7 天宽限后
/// 丢弃(级联退化为不发生,不构成数据风险)。
const STALE_AFTER_MS: u64 = 7 * 24 * 3600 * 1000;

/// 一次清扫的计数(观测 + 测试断言用)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    /// 完成级联(描述符确认已删,片全部墓碑)的对象数。
    pub cascaded: usize,
    /// 实际发出的片墓碑数(含"已不存在按成功"的跳过确认)。
    pub chunks_tombstoned: u64,
    /// 描述符仍活着而跳过的对象数(崩溃窗/手工投账的反向保护)。
    pub skipped_alive: usize,
    /// 滞留超 [`STALE_AFTER_MS`] 仍活着而丢弃的账行数。
    pub stale_dropped: usize,
    /// 片删除暂时失败的行数(留下一轮重试)。
    pub failed: usize,
}

/// 账行(JSONL;hex 化避免依赖 StoreKey 的 serde 面)。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    /// 组合对象描述符的 blobId(16 hex)。
    d: String,
    /// 片 blobId 列表(每项 16 hex;顺序无关)。
    c: Vec<String>,
    /// 记账时刻(epoch ms)。
    q: u64,
}

/// 级联账本句柄:`Clone` 后共享同一份内存态与文件。
pub struct DeleteCascade {
    path: PathBuf,
    /// 内存态 = 真相源(record 推入+追加;消费后整体原子重写)。std 同步锁:
    /// 临界区只有内存搬运与文件顺序写,无 await。
    entries: Mutex<Vec<LedgerEntry>>,
    /// 串行化清扫(sweep 期间 record 仍可并发入账,由 entries 锁保互斥)。
    sweep_lock: tokio::sync::Mutex<()>,
}

impl DeleteCascade {
    /// 打开(目录不存在则建;文件坏行 warn 跳过)。启动校验由调用方(`from_config`)负责。
    pub fn open(dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("delete-cascade.jsonl");
        let mut entries = Vec::new();
        if let Ok(text) = std::fs::read_to_string(&path) {
            for (no, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<LedgerEntry>(line) {
                    Ok(e) => entries.push(e),
                    Err(err) => tracing::warn!(
                        line = no,
                        error = %err,
                        "delete-cascade ledger: skip malformed row"
                    ),
                }
            }
        }
        Ok(DeleteCascade {
            path,
            entries: Mutex::new(entries),
            sweep_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// 当前待级联对象数(gauge 配对值,测试断言用)。
    pub fn entries(&self) -> usize {
        self.entries.lock().expect("cascade ledger lock").len()
    }

    /// 删除路径记一笔:**追加 + flush + fsync** —— 账先于主墓碑落盘的持久化纪律。
    /// 任何 IO 失败上抛(调用方降级为"不级联、照常删除",不得挡删除本身)。
    pub fn record(&self, descriptor: &StoreKey, chunks: &[StoreKey]) -> std::io::Result<()> {
        let entry = LedgerEntry {
            d: hex::encode(descriptor.as_bytes()),
            c: chunks.iter().map(|k| hex::encode(k.as_bytes())).collect(),
            q: crate::upload_wal::now_ms(),
        };
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut line = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
        line.push('\n');
        file.write_all(line.as_bytes())?;
        file.flush()?;
        file.sync_data()?;
        let mut entries = self.entries.lock().expect("cascade ledger lock");
        entries.push(entry);
        Ok(())
    }

    /// 清扫一拍:二次确认 → 逐片墓碑 → 原子重写。详见模块头。
    pub async fn sweep(&self, router: &BlobRouter, metrics: &FrontendMetrics) -> SweepStats {
        let _guard = self.sweep_lock.lock().await;
        metrics.delete_cascade_runs_total.inc();
        let mut stats = SweepStats::default();
        // 快照(async 阶段不持 entries 锁,期间新账可并发入)。
        let snapshot: Vec<LedgerEntry> = {
            let entries = self.entries.lock().expect("cascade ledger lock");
            if entries.is_empty() {
                return stats;
            }
            entries.clone()
        };
        let now = crate::upload_wal::now_ms();
        // 决策:完成/丢弃的行号集合;其余(跳过/失败)保留。
        let mut done: Vec<usize> = Vec::new();
        for (idx, entry) in snapshot.iter().enumerate() {
            let Some(descriptor) = key_of(&entry.d) else {
                // 账行自身损坏(非法 hex):丢弃,告警。
                stats.stale_dropped += 1;
                metrics.delete_cascade_stale_drops_total.inc();
                tracing::warn!(row = %entry.d, "delete-cascade: malformed descriptor id, row dropped");
                done.push(idx);
                continue;
            };
            // 二次确认:描述符确实已删(墓碑 200+deleted,或整记录已不存)才级联。
            let dead = match router.stat(&descriptor).await {
                Ok(meta) => meta.deleted,
                Err(antfs_router::Error::NotFound { .. }) => true,
                Err(_) => false, // 暂时性失败:本轮不下结论,留行重试。
            };
            if !dead {
                if now.saturating_sub(entry.q) > STALE_AFTER_MS {
                    stats.stale_dropped += 1;
                    metrics.delete_cascade_stale_drops_total.inc();
                    tracing::warn!(
                        descriptor = %entry.d,
                        "delete-cascade: alive past stale window, row dropped (no cascade)"
                    );
                    done.push(idx);
                } else {
                    stats.skipped_alive += 1;
                    metrics.delete_cascade_skips_total.inc();
                }
                continue;
            }
            // 级联:逐片墓碑(NotFound=已收,按成功;其余失败留行重试)。
            let mut row_failed = false;
            for hex_id in &entry.c {
                let Some(key) = key_of(hex_id) else {
                    tracing::warn!(chunk = %hex_id, "delete-cascade: malformed chunk id, skipped");
                    continue;
                };
                match router.delete(&key).await {
                    Ok(_) | Err(antfs_router::Error::NotFound { .. }) => {
                        metrics.delete_cascade_chunk_tombstones_total.inc();
                        stats.chunks_tombstoned += 1;
                    }
                    Err(err) => {
                        row_failed = true;
                        tracing::warn!(
                            chunk = %hex_id,
                            error = %err,
                            "delete-cascade: chunk tombstone failed, retry next sweep"
                        );
                    }
                }
            }
            if row_failed {
                stats.failed += 1;
            } else {
                stats.cascaded += 1;
                done.push(idx);
            }
        }
        if done.is_empty() {
            return stats;
        }
        // 应用决策(期间可能并发入新账,重写以内存态为准,不丢新行)。
        {
            let mut entries = self.entries.lock().expect("cascade ledger lock");
            for idx in done.iter().rev() {
                entries.remove(*idx);
            }
            let set = self.compact_locked(&entries);
            if let Err(err) = set {
                // 重写失败:行已从内存删除但文件仍在 —— 下次启动会重放旧文件(重复级联,
                // 幂等无害);本轮告警即止。
                tracing::warn!(error = %err, "delete-cascade: ledger compaction failed");
            }
        }
        metrics.delete_cascade_entries.set(self.entries() as i64);
        stats
    }

    /// 整体重写账本文件:tmp 写入 → fsync → 原子 rename → 目录 fsync(§3.3)。
    /// 调用方必须已持 entries 锁(重写内容 = 锁内真相)。
    fn compact_locked(&self, entries: &[LedgerEntry]) -> std::io::Result<()> {
        let tmp = self.path.with_extension("jsonl.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            for entry in entries {
                let mut line = serde_json::to_string(entry).map_err(std::io::Error::other)?;
                line.push('\n');
                file.write_all(line.as_bytes())?;
            }
            file.flush()?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            if let Ok(dir_file) = std::fs::File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }
        Ok(())
    }
}

/// hex16 → StoreKey(空/非 8 字节 = None)。
fn key_of(hex_id: &str) -> Option<StoreKey> {
    let bytes = hex::decode(hex_id).ok()?;
    StoreKey::from_slice(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 记账 → 重开(持久化往返)→ entries 复原;行结构 hex 往返无损。
    #[test]
    fn ledger_roundtrip_and_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = DeleteCascade::open(dir.path().to_path_buf()).expect("open");
        let d = StoreKey::from_parts(3, 7);
        let cs = [StoreKey::from_parts(1, 1), StoreKey::from_parts(2, 2)];
        ledger.record(&d, &cs).expect("record");
        assert_eq!(ledger.entries(), 1);

        let reopened = DeleteCascade::open(dir.path().to_path_buf()).expect("reopen");
        assert_eq!(reopened.entries(), 1, "fsync 后重开账必须在");
        let entries = reopened.entries.lock().expect("lock");
        let row = &entries[0];
        assert_eq!(hex::encode(d.as_bytes()), row.d);
        assert_eq!(row.c.len(), 2);
        assert!(key_of(&row.c[0]).is_some());
    }

    /// 坏行容忍:文件里混入垃圾行不炸、不丢好行。
    #[test]
    fn ledger_tolerates_malformed_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = DeleteCascade::open(dir.path().to_path_buf()).expect("open");
        ledger
            .record(&StoreKey::from_parts(1, 5), &[StoreKey::from_parts(1, 6)])
            .expect("record");
        drop(ledger);
        let path = dir.path().join("delete-cascade.jsonl");
        let text = std::fs::read_to_string(&path).expect("read");
        std::fs::write(&path, format!("{text}not-a-json\n\n")).expect("append garbage");
        let reopened = DeleteCascade::open(dir.path().to_path_buf()).expect("reopen");
        assert_eq!(reopened.entries(), 1, "坏行跳过、好行保留");
    }

    /// compact 原子重写:done 行消失后文件与内存一致。
    #[test]
    fn compact_rewrites_only_remaining_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = DeleteCascade::open(dir.path().to_path_buf()).expect("open");
        ledger
            .record(&StoreKey::from_parts(1, 10), &[])
            .expect("record 1");
        ledger
            .record(&StoreKey::from_parts(1, 11), &[])
            .expect("record 2");
        {
            let mut entries = ledger.entries.lock().expect("lock");
            entries.remove(0);
            ledger.compact_locked(&entries).expect("compact");
        }
        let text = std::fs::read_to_string(&ledger.path).expect("read");
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains(&hex::encode(StoreKey::from_parts(1, 11).as_bytes())));
    }
}
