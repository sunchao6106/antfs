//! 边收边分片的**孤儿收账簿**(Ambry「MySQL 会话表 + 定期清扫」的本地等价物,所有者
//! 裁决 2026-10-21:现在就做)。
//!
//! 形态:网关本地一个 append-only 的 JSONL 账簿(每行几十字节,**不是数据中转** —— 违背
//! ④ 精神的是"再抄一遍数据",账簿只记 id)。分片上传开始记 `start`,每片入库记 `chunk`,
//! 描述符落库记 `commit`(它是这批片的"正名"凭证),失败退出/清扫完成记 `abort`。
//! 单写者线程独占文件,逐行 fsync;进程重启后回放重建,**扫描器**把"超过时限仍无
//! commit 也无 abort"的会话按片清逐一交回调用方作废(墓碑)—— 即 Ambry 清扫 runner
//! 的本地版。
//!
//! 已知残余(如实,对照 Ambry 的 25h TTL 兜底):描述符已落库、但 `commit` 行还在 fsync
//! 途中进程断电 = 会话被误判孤儿,清扫器会作废**活着的**片;窗口 = 毫秒级 fsync 延迟
//! (Ambry 同位置同类窗口靠 TTL 自愈,我们对象不可变、TTL 只写时定,移植不了)。
//! 账簿目录建议落持久卷;卷随 Pod 蒸发 = 该节点历史会话失账(兜底退化为无人清扫,
//! **不会误删活数据**)。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// 账簿文件名(落在配置目录里)。
const WAL_FILENAME: &str = "upload-sessions.jsonl";
/// 扫描周期(启动即扫一次,此后每 10 分钟一拍;扫描 = 查内存表,零盘 IO)。
pub const SWEEP_INTERVAL_SECS: u64 = 600;
/// 会话判老界限:最后一次记账 6 小时仍无终态 → 孤儿会话。刻意宽 —— 慢线路上的
/// 边收边分片上传小时级很正常,绝不能把活上传当孤儿。
pub const STALE_AFTER_MS: u64 = 6 * 60 * 60 * 1000;
/// 账簿瘦身阈(已终结行数):超过则重写为仅 active 会话(原子 tmp→rename)。
const COMPACT_DONE_ROWS: u64 = 2048;
/// 命令通道容量:记账(请求路径)最多欠写线程这些条;满则 await 背压。
const CHANNEL_CAP: usize = 64;

/// 落盘行(单结构 + 可选字段)。
#[derive(Serialize, Deserialize)]
struct WalLine {
    /// 行类:`start` / `chunk` / `commit` / `abort`。
    k: String,
    sid: u64,
    /// 记账时刻(epoch ms;调用方注入,测试可倒拨)。
    t: u64,
    /// `chunk` = 片 blobId(16 hex);`commit` = 描述符 blobId。
    id: Option<String>,
}

/// 扫描器交出的孤儿会话(片 id 为 16 hex,调用方解析后逐一墓碑)。
pub struct StaleSession {
    pub sid: u64,
    pub chunk_ids: Vec<String>,
}

#[derive(Clone, Default)]
struct Session {
    chunk_ids: Vec<String>,
    /// 描述符已落库(正名)→ 绝不动它的片。
    commit: Option<String>,
    /// 已终结核销(失败退出或清扫完成)。
    aborted: bool,
    last_ms: u64,
}

impl Session {
    fn terminal(&self) -> bool {
        self.commit.is_some() || self.aborted
    }
}

enum Cmd {
    Append(WalLine),
    Scan {
        now_ms: u64,
        reply: oneshot::Sender<Vec<StaleSession>>,
    },
    Compact,
}

/// 账簿句柄(Clone 给请求路径与清扫器);所有句柄 drop 后写线程自退。
pub struct UploadWal {
    tx: mpsc::Sender<Cmd>,
    /// 会话号分配(单调;打开时接续回放出的最大 sid,不与历史会话撞号)。
    next_sid: AtomicU64,
}

impl UploadWal {
    /// 打开(或创建)账簿:`spawn_blocking` 内回放 + 起写线程;目录不可建/不可写 →
    /// 启动失败(优于静默关功能)。
    pub async fn open(dir: PathBuf) -> Result<Arc<Self>, std::io::Error> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);
        let (ready_tx, ready_rx) = oneshot::channel();
        let cmd_tx = tx.clone();
        drop(tx);
        tokio::task::spawn_blocking(move || match replay_and_open(&dir) {
            Ok((sessions, file, done_rows)) => {
                let max_sid = sessions.keys().copied().max().unwrap_or(0);
                let handle = Arc::new(UploadWal {
                    tx: cmd_tx,
                    next_sid: AtomicU64::new(max_sid),
                });
                if ready_tx.send(Ok(handle)).is_err() {
                    return;
                }
                std::thread::Builder::new()
                    .name("upload-session-wal".into())
                    .spawn(move || writer_loop(rx, dir, sessions, done_rows, file))
                    .expect("spawn wal writer thread");
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
            }
        });
        ready_rx.await.expect("wal open 回执等得到")
    }

    /// 分片上传开始:分配 sid 并记 `start`。写线程已关(进程退出中)→ 的记账静默
    /// 丢弃(send 失败即弃),不影响在途行为。
    pub async fn begin(&self, t_ms: u64) -> u64 {
        let sid = self
            .next_sid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        self.append(WalLine {
            k: "start".into(),
            sid,
            t: t_ms,
            id: None,
        })
        .await;
        sid
    }

    /// 第 n 片已入库:记 `chunk`(id = 16 hex)。
    pub async fn record_chunk(&self, sid: u64, id: &str, t_ms: u64) {
        self.append(WalLine {
            k: "chunk".into(),
            sid,
            t: t_ms,
            id: Some(id.into()),
        })
        .await;
    }

    /// 描述符已落库(正名):记 `commit`。此刻起清扫器永不作废这些片。
    pub async fn record_commit(&self, sid: u64, descriptor_id: &str, t_ms: u64) {
        self.append(WalLine {
            k: "commit".into(),
            sid,
            t: t_ms,
            id: Some(descriptor_id.into()),
        })
        .await;
    }

    /// 终结核销(失败退出或清扫完成):记 `abort`。
    pub async fn record_abort(&self, sid: u64, t_ms: u64) {
        self.append(WalLine {
            k: "abort".into(),
            sid,
            t: t_ms,
            id: None,
        })
        .await;
    }

    /// 扫描超时孤儿会话(无终态且 `now - 最后记账 > STALE_AFTER_MS`);
    /// 写线程不可达(进程退出中)→ 空。
    pub async fn scan_stale(&self, now_ms: u64) -> Vec<StaleSession> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::Scan { now_ms, reply }).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// 请求瘦身(已终结行超阈值时把账簿重写为仅 active 会话)。
    pub async fn compact_maybe(&self) {
        let _ = self.tx.send(Cmd::Compact).await;
    }

    async fn append(&self, line: WalLine) {
        // 进程退出期 send 失败 → 静默丢弃:账簿尽力而为,不能让上传失败。
        let _ = self.tx.send(Cmd::Append(line)).await;
    }
}

/// 现在的 epoch 毫秒(记账时点)。
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn wal_path(dir: &std::path::Path) -> PathBuf {
    dir.join(WAL_FILENAME)
}

/// 回放 + 打开追加端。截断的尾行(崩溃写一半、无换行)忽略;带换行的坏行 warn 跳过
/// (该会话失账 = 退化为无人清扫,不会误删活数据)。
fn replay_and_open(
    dir: &std::path::Path,
) -> std::io::Result<(HashMap<u64, Session>, std::fs::File, u64)> {
    std::fs::create_dir_all(dir)?;
    let path = wal_path(dir);
    let mut sessions = HashMap::new();
    let mut done_rows = 0u64;
    if let Ok(f) = std::fs::File::open(&path) {
        for line in BufReader::new(f).lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            match serde_json::from_str::<WalLine>(&line) {
                Ok(wl) => apply_line(&mut sessions, &mut done_rows, &wl),
                Err(e) => {
                    if line.ends_with('\n') {
                        tracing::warn!(error = %e, "upload wal: 跳过无法解析的行");
                    }
                }
            }
        }
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    Ok((sessions, file, done_rows))
}

fn apply_line(sessions: &mut HashMap<u64, Session>, done_rows: &mut u64, l: &WalLine) {
    let s = sessions.entry(l.sid).or_default();
    s.last_ms = s.last_ms.max(l.t);
    match l.k.as_str() {
        "start" => {}
        "chunk" => {
            if let Some(id) = &l.id {
                s.chunk_ids.push(id.clone());
            }
        }
        "commit" => {
            if s.commit.is_none() {
                *done_rows += 1;
                s.commit = l.id.clone();
            }
        }
        "abort" => {
            if !s.aborted {
                *done_rows += 1;
            }
            s.aborted = true;
        }
        _ => tracing::warn!(k = %l.k, "upload wal: 未知行类,跳过"),
    }
}

fn persist(file: &mut std::fs::File, l: &WalLine) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(l).expect("wal 行可序列化");
    buf.push(b'\n');
    file.write_all(&buf)?;
    file.flush()?;
    // 逐行 fsync 发生在**写线程**(不在请求路径):账簿持久化边界清晰,代价不外溢。
    file.sync_data()
}

fn writer_loop(
    mut rx: mpsc::Receiver<Cmd>,
    dir: PathBuf,
    mut sessions: HashMap<u64, Session>,
    mut done_rows: u64,
    mut file: std::fs::File,
) {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Append(l) => {
                if persist(&mut file, &l).is_err() {
                    tracing::warn!("upload wal: 记账 IO 失败(该行失账,不影响在途行为)");
                }
                apply_line(&mut sessions, &mut done_rows, &l);
            }
            Cmd::Scan { now_ms, reply } => {
                let stale = sessions
                    .iter()
                    .filter(|(_, s)| !s.terminal())
                    .filter(|(_, s)| now_ms.saturating_sub(s.last_ms) > STALE_AFTER_MS)
                    .map(|(sid, s)| StaleSession {
                        sid: *sid,
                        chunk_ids: s.chunk_ids.clone(),
                    })
                    .collect();
                let _ = reply.send(stale);
            }
            Cmd::Compact => {
                if done_rows > COMPACT_DONE_ROWS {
                    match compact(&dir, &sessions) {
                        Ok(new) => {
                            file = new;
                            sessions.retain(|_, s| !s.terminal());
                            done_rows = 0;
                        }
                        Err(e) => tracing::warn!(error = %e, "upload wal: 瘦身失败,下次再试"),
                    }
                }
            }
        }
    }
}

/// 瘦身:重写为仅 active 会话(start + chunk 行,时间取最后记账时刻),
/// 同目录 tmp → fsync → rename 原子换档;内存表由 writer 同步收缩。
fn compact(
    dir: &std::path::Path,
    sessions: &HashMap<u64, Session>,
) -> std::io::Result<std::fs::File> {
    let tmp = dir.join(format!("{WAL_FILENAME}.tmp-{}", std::process::id()));
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    for (sid, s) in sessions.iter().filter(|(_, s)| !s.terminal()) {
        persist(
            &mut out,
            &WalLine {
                k: "start".into(),
                sid: *sid,
                t: s.last_ms,
                id: None,
            },
        )?;
        for id in &s.chunk_ids {
            persist(
                &mut out,
                &WalLine {
                    k: "chunk".into(),
                    sid: *sid,
                    t: s.last_ms,
                    id: Some(id.clone()),
                },
            )?;
        }
    }
    out.sync_all()?;
    std::fs::rename(&tmp, wal_path(dir))?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path(dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wal_in(dir: &std::path::Path) -> Arc<UploadWal> {
        UploadWal::open(dir.to_path_buf()).await.expect("open wal")
    }

    /// 四类会话判定:新活会话 / 已提交 / 已核销 / 超时孤儿 —— 只有第四类被扫出。
    #[tokio::test]
    async fn scan_classifies_sessions_correctly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal_in(dir.path()).await;

        // s1:超时孤儿(start 很久以前 + 两片)。
        let s1 = wal.begin(100).await;
        wal.record_chunk(s1, "aabbccdd00001111", 200).await;
        wal.record_chunk(s1, "aabbccdd00002222", 300).await;
        // s2:活着(刚记账)。
        let s2 = wal.begin(500).await;
        wal.record_chunk(s2, "aabbccdd00003333", 600).await;
        // s3:已提交(描述符落库;时间再老也不许动)。
        let s3 = wal.begin(50).await;
        wal.record_chunk(s3, "aabbccdd00004444", 60).await;
        wal.record_commit(s3, "aabbccdd00005555", 70).await;
        // s4:失败已核销。
        let s4 = wal.begin(80).await;
        wal.record_chunk(s4, "aabbccdd00006666", 90).await;
        wal.record_abort(s4, 100).await;

        // 判老口径 = 各会话"最后记账 + 6h":now 推到 s1 过线、s2 尚未(ms 级窗口)。
        let now = 300 + STALE_AFTER_MS + 1;
        let stale = wal.scan_stale(now).await;
        assert_eq!(
            stale.len(),
            1,
            "恰一个孤儿会话(s1 过线;新活/已提交/已核销都不动)"
        );
        assert_eq!(stale[0].sid, s1);
        assert_eq!(
            stale[0].chunk_ids,
            vec!["aabbccdd00001111", "aabbccdd00002222"]
        );

        // now 推到 s2 也判老 → 两个孤儿(s3/s4 永不出现:终态会话免死)。
        let now2 = 600 + STALE_AFTER_MS + 1;
        assert_eq!(wal.scan_stale(now2).await.len(), 2);
    }

    /// 崩溃回放:重开后内存状态完整重建(孤儿会话与片);新 sid 不与历史撞号。
    #[tokio::test]
    async fn replay_after_reopen_restores_state_and_sid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = wal_in(dir.path()).await;
        let s1 = first.begin(1000).await;
        first.record_chunk(s1, "1111111111111111", 1100).await;
        let s2 = first.begin(2000).await;
        first.record_chunk(s2, "2222222222222222", 2100).await;
        first.record_commit(s2, "3333333333333333", 2200).await;
        drop(first); // 写线程收尾(异步排干队列:判据 = commit 行已落盘,非固定 sleep)。
        let path = wal_path(dir.path());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let text = std::fs::read_to_string(&path).expect("读账簿");
            if text.contains("commit") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "写线程未在期限内排干:\n{text}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let second = wal_in(dir.path()).await;
        let new_sid = second.begin(9999).await;
        assert!(new_sid > s2, "新会话号必须接续历史({new_sid} > {s2})");

        // now 卡在"s1 过线(其最后记账 1100 距今超 6h)、新会话尚在窗内(距 9999 不足 6h)"。
        let now = 9999 + STALE_AFTER_MS - 100;
        let stale = second.scan_stale(now).await;
        // s1 孤儿;s2 已提交;new_sid 刚记不老。
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].sid, s1);
        assert_eq!(stale[0].chunk_ids, vec!["1111111111111111"]);
    }

    /// 截断尾行(崩溃写一半、无换行符)被忽略,不毒化回放。
    #[tokio::test]
    async fn truncated_tail_is_tolerated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal_in(dir.path()).await;
        let s = wal.begin(10).await;
        wal.record_chunk(s, "4444444444444444", 20).await;
        // 写线程异步排干:判据 = chunk 行已落盘(非固定 sleep),再 drop + 补半截行。
        let wait_path = wal_path(dir.path());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let text = std::fs::read_to_string(&wait_path).expect("读账簿");
            if text.contains("chunk") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "写线程未在期限内排干:\n{text}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        drop(wal);

        let path = wal_path(dir.path());
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open");
        f.write_all(b"{\"k\":\"chunk\",\"sid\":99,\"t\":30,\"id\":\"555")
            .expect("半截行");

        let reopened = wal_in(dir.path()).await;
        assert_eq!(
            reopened.scan_stale(20 + 7 * 60 * 60 * 1000).await.len(),
            1,
            "仅完整行参与判定"
        );
    }

    /// 瘦身:终结行超阈值后文件只剩 active 会话,判老照常(带超时轮询,判据 = 文件
    /// 内容变化,非固定 sleep)。
    #[tokio::test]
    async fn compaction_keeps_only_active_sessions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal_in(dir.path()).await;
        let alive = wal.begin(5).await;
        wal.record_chunk(alive, "6666666666666666", 6).await;
        for i in 0..=COMPACT_DONE_ROWS {
            let sid = wal.begin(i * 10).await;
            wal.record_abort(sid, i * 10 + 1).await;
        }
        wal.compact_maybe().await;

        let path = wal_path(dir.path());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let text = std::fs::read_to_string(&path).expect("读账簿");
            if !text.contains("\"k\":\"abort\"") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "瘦身未在期限内完成:\n{text}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(wal.scan_stale(6 + 7 * 60 * 60 * 1000).await.len(), 1);
    }
}
