//! A9:审计日志(append-only JSONL)。
//!
//! 只记两类请求:① **所有写操作**(POST/PUT/DELETE…);② `/api/me`(登录/鉴权成功与失败)。
//! 记录在响应返回后写,**写失败只 warn** —— 审计绝不改变请求结果;启动时打开失败则整体降级为
//! 空实现(同样只 warn)。
//!
//! 查询(`GET /api/audit`)从文件**尾部反向按块读**,不把大文件整个读进内存;解析失败的行跳过
//! (半行/损坏不阻断其余记录)。文件 append-only、不轮转,长期运行请自行 `logrotate`。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// `GET /api/audit` 未传 `limit` 时的条数。
pub const DEFAULT_AUDIT_LIMIT: usize = 200;
/// `GET /api/audit` 的单次上限(再大就等于把整个文件读进内存)。
pub const MAX_AUDIT_LIMIT: usize = 2000;
/// 反向读的块大小:每次读这么多字节再从中切行(越小越省内存,越大越少 syscall)。
const TAIL_CHUNK: usize = 64 * 1024;

/// 一条审计记录(JSONL 一行,也是 `GET /api/audit` 的数组元素)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// 记录时刻(Unix 毫秒)。
    pub ts_ms: u64,
    /// 操作者名(多用户 = `[[admin.users]].name`;单令牌回落 `operator`/`viewer`;关校验 = `local`;
    /// 令牌无法识别 = `anonymous`)。
    pub actor: String,
    /// 角色名:`operator` / `viewer` / `none`(未通过鉴权)。
    pub role: String,
    pub method: String,
    /// 请求路径(不含 query)。
    pub path: String,
    /// HTTP 状态码(鉴权失败 = 401、viewer 写 = 403 都会落档)。
    pub status: u16,
    /// 摘要:`方法 + 路径 + query`(query 里的 id/flag 一并保留,便于事后追溯改的是谁)。
    pub summary: String,
    /// 来源地址;未启用 `ConnectInfo` 的进程(如单测)为空串。
    pub remote_addr: String,
}

/// 审计日志(进程内单实例)。`file` 为 `None` = 打开失败的**降级空实现**。
pub struct AuditLog {
    path: PathBuf,
    file: Option<Mutex<File>>,
}

impl AuditLog {
    /// 打开(create + append,**读写**:追加写、反向读尾部)。失败**不报错**:warn 后返回降级实例
    /// (所有操作变 no-op),保证"审计写不了"不会拖垮 admin 服务本身。
    pub fn open(path: impl Into<PathBuf>) -> AuditLog {
        let path = path.into();
        // 必须带 `read(true)`:尾部查询要对同一个句柄反向读(append 只管写位置)。
        match OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
        {
            Ok(f) => {
                tracing::info!(path = %path.display(), "admin audit log opened");
                AuditLog {
                    path,
                    file: Some(Mutex::new(f)),
                }
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "admin audit log unavailable, auditing disabled");
                AuditLog { path, file: None }
            }
        }
    }

    /// 审计文件路径(运维查看用)。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 追加一条(`ts_ms` 由调用方填)。任何 IO/序列化失败都只 warn。
    pub fn record(&self, entry: &AuditEntry) {
        let Some(file) = &self.file else {
            return;
        };
        let mut line = match serde_json::to_string(entry) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "audit entry not serialized");
                return;
            }
        };
        line.push('\n');
        let mut f = file.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = f.write_all(line.as_bytes()).and_then(|()| f.flush()) {
            tracing::warn!(path = %self.path.display(), error = %e, "audit entry not written");
        }
    }

    /// 最近 `limit` 条(**最新在前**);降级实例 / 读失败 → 空表。
    pub fn tail(&self, limit: usize) -> Vec<AuditEntry> {
        let Some(file) = &self.file else {
            return Vec::new();
        };
        if limit == 0 {
            return Vec::new();
        }
        let mut f = file.lock().unwrap_or_else(|p| p.into_inner());
        let Ok(len) = f.seek(SeekFrom::End(0)) else {
            return Vec::new();
        };
        let mut pos = len;
        // 上一轮(更靠后的块)切出的**半行**,与本轮前面拼起来才是完整记录。
        let mut carry: Vec<u8> = Vec::new();
        let mut out: Vec<AuditEntry> = Vec::new();
        while pos > 0 && out.len() < limit {
            let want = usize::try_from(pos).unwrap_or(usize::MAX).min(TAIL_CHUNK);
            let Ok(step) = u64::try_from(want) else {
                break;
            };
            pos -= step;
            if f.seek(SeekFrom::Start(pos)).is_err() {
                break;
            }
            let mut buf = vec![0u8; want];
            if f.read_exact(&mut buf).is_err() {
                break;
            }
            buf.extend_from_slice(&carry);
            let mut start = 0;
            if pos > 0 {
                // 本块首个 `\n` 之前只是某条记录的后半段,留到下一轮(更早的块)拼回整行。
                match buf.iter().position(|c| *c == b'\n') {
                    Some(i) => {
                        carry = buf[..i].to_vec();
                        start = i + 1;
                    }
                    None => {
                        carry = buf;
                        continue;
                    }
                }
            }
            push_lines(&buf[start..], limit - out.len(), &mut out);
        }
        out
    }
}

/// 把一段 JSONL 字节**从后往前**解析入 `out`(故 `out` 是最新在前),最多解出 `max` 条;
/// 空行与解析失败的行跳过。
fn push_lines(region: &[u8], max: usize, out: &mut Vec<AuditEntry>) {
    for line in region.split(|c| *c == b'\n').rev() {
        if out.len() >= max {
            return;
        }
        if line.is_empty() {
            continue;
        }
        if let Ok(e) = serde_json::from_slice::<AuditEntry>(line) {
            out.push(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64) -> AuditEntry {
        AuditEntry {
            ts_ms: 1_700_000_000_000 + id,
            actor: "alice".into(),
            role: "operator".into(),
            method: "POST".into(),
            path: format!("/api/partitions/{id}/compact"),
            status: 200,
            summary: format!("POST /api/partitions/{id}/compact?mode=async"),
            remote_addr: "127.0.0.1:5555".into(),
        }
    }

    /// 临时目录里的唯一文件路径(不上 `tempfile`,避免为一个测试加依赖)。
    fn temp_path(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("antfs-audit-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("audit.jsonl")
    }

    #[test]
    fn record_then_tail_is_newest_first() {
        let path = temp_path("basic");
        let _ = std::fs::remove_file(&path);
        let log = AuditLog::open(&path);
        for i in 0..5 {
            log.record(&entry(i));
        }
        let tail = log.tail(3);
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[0].path, "/api/partitions/4/compact", "最新在前");
        assert_eq!(tail[2].path, "/api/partitions/2/compact");
        assert_eq!(tail[0].remote_addr, "127.0.0.1:5555");
        assert_eq!(log.path(), path.as_path());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tail_reads_backwards_across_chunk_boundaries() {
        // 记录约 200B × 600 → 跨过多个 TAIL_CHUNK;反向拼行若错位,末尾几条会解析失败/串行。
        let path = temp_path("chunks");
        let _ = std::fs::remove_file(&path);
        let log = AuditLog::open(&path);
        for i in 0..600 {
            log.record(&entry(i));
        }
        let tail = log.tail(50);
        assert_eq!(tail.len(), 50);
        assert_eq!(tail[0].path, "/api/partitions/599/compact");
        assert_eq!(tail[49].path, "/api/partitions/550/compact");
        // 超过文件条数时回全部(600 条),不重复不丢。
        assert_eq!(log.tail(10_000).len(), 600);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tail_skips_corrupt_lines_and_empty_limit() {
        let path = temp_path("corrupt");
        let _ = std::fs::remove_file(&path);
        let log = AuditLog::open(&path);
        log.record(&entry(1));
        // 手工塞一行半截 JSON(模拟写中断/外部损坏)。
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"{\"ts_ms\":1,\"actor\":\"bob\"\n").unwrap();
        }
        log.record(&entry(2));
        let tail = log.tail(10);
        assert_eq!(tail.len(), 2, "损坏行跳过: {tail:?}");
        assert_eq!(tail[0].path, "/api/partitions/2/compact");
        assert_eq!(tail[1].path, "/api/partitions/1/compact");
        assert!(log.tail(0).is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unusable_path_degrades_to_noop() {
        // 父目录不存在 → 打开失败:record/tail 皆 no-op,不 panic。
        let bad = std::env::temp_dir().join("antfs-audit-nonexistent-dir-xyz/audit.jsonl");
        let _ = std::fs::remove_file(&bad);
        let log = AuditLog::open(&bad);
        log.record(&entry(1));
        assert!(log.tail(10).is_empty());
        assert!(!bad.exists(), "降级实例不创建文件");
    }
}
