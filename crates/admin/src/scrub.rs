//! 副本一致性比对(scrub):admin 编排,store 提供只读素材(摘要 + 最近窗口元数据 + 可选本地校验)。
//!
//! 设计取舍(见 `openspec/changes/balance-and-scrub`):
//! - **只比对逻辑态**(key 是否存在、生命版本/标志位是否一致)与**摘要**(活记录数/活字节/墓碑/段清单),
//!   不做数据体逐字节对拍:副本各自 append、compaction 会重写段,字节层面天然不等。
//! - `partial` 表示某个副本的元数据窗口被 `max_bytes` 截断 → 该轮比对只能算"部分比对",结论保守。
//! - 扫描期有并发写会产生**假不一致**(两侧窗口末端不同) → 报告带各副本 `token`/`bytes_read`,并建议重跑。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

/// 单副本摘要(来自 store `GET /partitions/{pid}/scrub-report`)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSummary {
    pub live_records: u64,
    pub tombstones: u64,
    pub live_bytes: u64,
    pub sealed_segments: usize,
    pub segments: Vec<(u64, u64)>,
    pub token_seg: Option<u64>,
    pub token_off: Option<u64>,
    /// 打开时跳过的坏记录数(>0 = 该副本带伤启动)。
    pub damaged_records: u64,
    /// 打开时放弃扫描剩余部分的段数。
    pub abandoned_segments: u64,
}

/// 某 key 在窗口内的**最终态**(后写覆盖先写)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyState {
    pub deleted: bool,
    pub size: u64,
}

/// 本地完整性校验摘要(来自 store `GET /partitions/{pid}/verify`;仅 `deep=1` 时取)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifySummary {
    pub records: u64,
    pub bad_records: u64,
    pub index_mismatch: u64,
    pub truncated: bool,
}

/// 一个副本的比对素材。
#[derive(Debug, Clone)]
pub struct ReplicaFacts {
    pub node_id: u64,
    /// 该副本素材是否取全(任一请求失败/404 → false)。
    pub ok: bool,
    /// 失败原因(ok=false 时非空)。
    pub error: String,
    /// 摘要(ok=false 时为 None)。
    pub report: Option<ReportSummary>,
    /// 元数据窗口内每 key 的最终态(key_hex → 状态);`None` 表示未取到。
    pub keys: Option<BTreeMap<String, KeyState>>,
    /// 该副本的元数据窗口疑似被 `max_bytes` 截断(不可全比)。
    pub partial: bool,
    /// 本地校验(未请求时有值但 `bad_records` 为 0;`deep=false` 时为 None)。
    pub verify: Option<VerifySummary>,
}

/// 一处差异。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// 摘要级差异(记录数/字节数/墓碑/段数/段清单)。
    Summary { field: String, detail: String },
    /// 本地校验发现坏记录/索引漂移。
    Verify { node_id: u64, detail: String },
    /// key 级差异:该 key 在各副本上的状态/存在性不同。
    Key {
        key_hex: String,
        nodes: Vec<(u64, String)>,
    },
}

/// 比对结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// 全部副本 ok、无差异、未截断。
    pub consistent: bool,
    /// 素材不足以下结论(有副本失败/只有 <2 个副本可取).
    pub inconclusive: bool,
    /// 至少一个副本的窗口被截断 → 只能算部分比对。
    pub partial: bool,
    pub divergences: Vec<Divergence>,
}

/// 单轮最多列出的 key 级差异条数(避免大分区刷爆响应)。
pub const MAX_KEY_DIVERGENCES: usize = 50;

/// 摘要字段的取值器:`(字段名, 从摘要里取该字段的可比字符串)`。
type SummaryField = (&'static str, fn(&ReportSummary) -> String);

/// 比对多个副本的素材(纯函数,便于单测)。
pub fn compare(replicas: &[ReplicaFacts]) -> Verdict {
    let ok: Vec<&ReplicaFacts> = replicas.iter().filter(|r| r.ok).collect();
    // 元数据窗口缺失(replica-metadata 失败)时该副本无法参与 key 级比对 → 一律算部分比对。
    let partial = replicas
        .iter()
        .any(|r| r.ok && (r.partial || r.keys.is_none()));
    let mut divergences = Vec::new();

    // 副本数不足(至少两个可取副本才谈得上"一致")。
    if ok.len() < 2 {
        return Verdict {
            consistent: false,
            inconclusive: true,
            partial,
            divergences,
        };
    }
    // 可取到元数据窗口的副本不足两个 → key 级比对无意义(结论已由 partial 标注)。
    let key_comparison_usable = ok.iter().filter(|r| r.keys.is_some()).count() >= 2;

    // ① 摘要级:逐字段比对(记录数/字节/墓碑/段数/段清单)。
    let fields: [SummaryField; 7] = [
        ("live_records", |s| s.live_records.to_string()),
        ("tombstones", |s| s.tombstones.to_string()),
        ("live_bytes", |s| s.live_bytes.to_string()),
        ("sealed_segments", |s| s.sealed_segments.to_string()),
        ("segments", |s| {
            s.segments
                .iter()
                .map(|(p, e)| format!("{p}:{e}"))
                .collect::<Vec<_>>()
                .join(",")
        }),
        // 带伤启动(打开时跳过坏记录/放弃段)也算摘要级差异:健康副本不该有这些值。
        ("damaged_records", |s| s.damaged_records.to_string()),
        ("abandoned_segments", |s| s.abandoned_segments.to_string()),
    ];
    for (field, get) in fields {
        let mut per_node: Vec<(u64, String)> = Vec::new();
        for r in &ok {
            if let Some(rep) = &r.report {
                per_node.push((r.node_id, get(rep)));
            }
        }
        let distinct: BTreeSet<&String> = per_node.iter().map(|(_, v)| v).collect();
        if distinct.len() > 1 {
            let detail = per_node
                .iter()
                .map(|(n, v)| format!("n{n}={v}"))
                .collect::<Vec<_>>()
                .join(" ");
            divergences.push(Divergence::Summary {
                field: field.to_string(),
                detail,
            });
        }
    }

    // ② key 级:并集里逐 key 比对"存在性 + 最终态"。
    //
    // **窗口取不到的副本不参与 key 级比对**(它的 key 状态是"未知"而非"缺失",把未知当缺失会刷出
    // 一堆误导性差异);这类副本已由 `partial` 标注,结论不会因它而宣称"一致"。
    let comparable: Vec<&&ReplicaFacts> = ok.iter().filter(|r| r.keys.is_some()).collect();
    let mut keys: BTreeSet<&String> = BTreeSet::new();
    for r in &comparable {
        if let Some(map) = &r.keys {
            keys.extend(map.keys());
        }
    }
    let mut key_divergences = 0usize;
    for key in keys {
        let mut states: Vec<(u64, String)> = Vec::new();
        let mut distinct: BTreeSet<String> = BTreeSet::new();
        for r in &comparable {
            let st = match r.keys.as_ref().and_then(|m| m.get(key)) {
                Some(s) => format!("deleted={} size={}", s.deleted, s.size),
                None => "missing".to_string(),
            };
            distinct.insert(st.clone());
            states.push((r.node_id, st));
        }
        if distinct.len() > 1 {
            key_divergences += 1;
            if divergences.len() < MAX_KEY_DIVERGENCES {
                divergences.push(Divergence::Key {
                    key_hex: key.clone(),
                    nodes: states,
                });
            }
        }
    }

    if key_comparison_usable && key_divergences > MAX_KEY_DIVERGENCES {
        divergences.push(Divergence::Summary {
            field: "key_divergences_truncated".to_string(),
            detail: format!("key-level divergences total {key_divergences}; listing only the first {MAX_KEY_DIVERGENCES}"),
        });
    }

    // ③ 本地校验(deep):坏记录/索引漂移本身就是差异。
    for r in &ok {
        if let Some(v) = &r.verify {
            if v.bad_records > 0 || v.index_mismatch > 0 {
                divergences.push(Divergence::Verify {
                    node_id: r.node_id,
                    detail: format!(
                        "bad_records={} index_mismatch={} truncated={}",
                        v.bad_records, v.index_mismatch, v.truncated
                    ),
                });
            }
        }
    }

    let consistent = divergences.is_empty() && !partial;
    Verdict {
        consistent,
        inconclusive: false,
        partial,
        divergences,
    }
}

/// 一轮 scrub 的紧凑摘要(环形缓冲,供 `GET /api/scrub` 列表)。
#[derive(Debug, Clone)]
pub struct ScrubRun {
    pub partition_id: u32,
    pub checked_at_ms: u64,
    pub deep: bool,
    pub consistent: bool,
    pub inconclusive: bool,
    pub partial: bool,
    pub divergence_count: usize,
    /// 逐副本紧凑摘要:`(node_id, ok, live_records, live_bytes)`。
    pub replicas: Vec<(u64, bool, u64, u64)>,
}

/// 最近若干轮的环形缓冲(进程内,不持久化 —— 重启即空,与 compaction 台账同口径)。
pub struct ScrubRuns {
    inner: Mutex<VecDeque<ScrubRun>>,
    cap: usize,
}

impl Default for ScrubRuns {
    fn default() -> Self {
        Self::new(32)
    }
}

impl ScrubRuns {
    pub fn new(cap: usize) -> Self {
        ScrubRuns {
            inner: Mutex::new(VecDeque::new()),
            cap: cap.max(1),
        }
    }

    pub fn push(&self, run: ScrubRun) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if g.len() == self.cap {
            g.pop_front();
        }
        g.push_back(run);
    }

    /// 最新在前。
    pub fn snapshot(&self) -> Vec<ScrubRun> {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.iter().rev().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(node: u64, live: u64, keys: &[(&str, bool, u64)]) -> ReplicaFacts {
        let mut map = BTreeMap::new();
        for (k, deleted, size) in keys {
            map.insert(
                k.to_string(),
                KeyState {
                    deleted: *deleted,
                    size: *size,
                },
            );
        }
        ReplicaFacts {
            node_id: node,
            ok: true,
            error: String::new(),
            report: Some(ReportSummary {
                live_records: live,
                tombstones: 0,
                live_bytes: live * 10,
                sealed_segments: 1,
                segments: vec![(0, 100)],
                token_seg: Some(0),
                token_off: Some(100),
                damaged_records: 0,
                abandoned_segments: 0,
            }),
            keys: Some(map),
            partial: false,
            verify: None,
        }
    }

    #[test]
    fn identical_replicas_are_consistent() {
        let v = compare(&[
            facts(101, 2, &[("aa", false, 4)]),
            facts(102, 2, &[("aa", false, 4)]),
        ]);
        assert!(v.consistent && !v.inconclusive && !v.partial, "{v:?}");
        assert!(v.divergences.is_empty());
    }

    #[test]
    fn summary_mismatch_is_reported_per_field() {
        let v = compare(&[
            facts(101, 2, &[("aa", false, 4)]),
            facts(102, 3, &[("aa", false, 4)]),
        ]);
        assert!(!v.consistent && !v.inconclusive);
        assert_eq!(
            v.divergences.len(),
            2,
            "live_records 与 live_bytes 都应报差异: {v:?}"
        );
        assert!(
            matches!(&v.divergences[0], Divergence::Summary { field, .. } if field == "live_records")
        );
    }

    #[test]
    fn missing_key_on_one_replica_is_key_divergence() {
        let v = compare(&[
            facts(101, 2, &[("aa", false, 4), ("bb", false, 4)]),
            facts(102, 1, &[("aa", false, 4)]),
        ]);
        assert!(!v.consistent);
        let key_div: Vec<_> = v
            .divergences
            .iter()
            .filter(|d| matches!(d, Divergence::Key { .. }))
            .collect();
        assert_eq!(key_div.len(), 1, "{v:?}");
        if let Divergence::Key { key_hex, nodes } = key_div[0] {
            assert_eq!(key_hex, "bb");
            assert_eq!(nodes[0], (101, "deleted=false size=4".to_string()));
            assert_eq!(nodes[1], (102, "missing".to_string()));
        }
    }

    #[test]
    fn lifecycle_mismatch_is_key_divergence() {
        let v = compare(&[
            facts(101, 1, &[("aa", false, 4)]),
            facts(102, 1, &[("aa", true, 4)]),
        ]);
        assert!(!v.consistent);
        assert!(v
            .divergences
            .iter()
            .any(|d| matches!(d, Divergence::Key { key_hex, .. } if key_hex == "aa")));
    }

    #[test]
    fn failed_replica_makes_verdict_inconclusive() {
        let mut broken = facts(102, 2, &[("aa", false, 4)]);
        broken.ok = false;
        broken.error = "404: no replica for partition".into();
        broken.report = None;
        broken.keys = None;
        let v = compare(&[facts(101, 2, &[("aa", false, 4)]), broken]);
        assert!(v.inconclusive && !v.consistent, "{v:?}");
    }

    #[test]
    fn partial_window_is_not_consistent_but_not_divergent() {
        let mut r2 = facts(102, 2, &[("aa", false, 4)]);
        r2.partial = true;
        let v = compare(&[facts(101, 2, &[("aa", false, 4)]), r2]);
        assert!(!v.consistent && v.partial && !v.inconclusive, "{v:?}");
        assert!(
            v.divergences.is_empty(),
            "截断本身不算差异,但要阻止 consistent"
        );
    }

    #[test]
    fn verify_problems_are_divergences() {
        let mut r2 = facts(102, 2, &[("aa", false, 4)]);
        r2.verify = Some(VerifySummary {
            records: 5,
            bad_records: 1,
            index_mismatch: 0,
            truncated: false,
        });
        let v = compare(&[facts(101, 2, &[("aa", false, 4)]), r2]);
        assert!(!v.consistent);
        assert!(v
            .divergences
            .iter()
            .any(|d| matches!(d, Divergence::Verify { node_id: 102, .. })));
    }

    #[test]
    fn runs_ring_keeps_newest_first_and_caps() {
        let ring = ScrubRuns::new(2);
        for i in 0..3u32 {
            ring.push(ScrubRun {
                partition_id: i,
                checked_at_ms: i as u64,
                deep: false,
                consistent: true,
                inconclusive: false,
                partial: false,
                divergence_count: 0,
                replicas: vec![],
            });
        }
        let snap = ring.snapshot();
        assert_eq!(snap.len(), 2, "容量 2:最旧被淘汰");
        assert_eq!(snap[0].partition_id, 2, "最新在前");
        assert_eq!(snap[1].partition_id, 1);
    }
    #[test]
    fn replica_without_window_is_excluded_from_key_comparison() {
        // n103 的元数据窗口取不到(keys=None,partial=true)→ 它**不参与 key 级比对**,
        // 否则会把"未知"报成"缺失"(3 条假差异);结论由 partial 表达"只能部分比对"。
        let mut blind = facts(103, 2, &[("aa", false, 4)]);
        blind.keys = None;
        blind.partial = true;
        blind.error = "replica-metadata unavailable: bad record".into();
        let v = compare(&[
            facts(101, 2, &[("aa", false, 4)]),
            facts(102, 2, &[("aa", false, 4)]),
            blind,
        ]);
        assert!(v.partial && !v.consistent, "{v:?}");
        assert_eq!(
            v.divergences
                .iter()
                .filter(|d| matches!(d, Divergence::Key { .. }))
                .count(),
            0,
            "窗口不可用的副本不应刷出 key 级差异: {v:?}"
        );
    }

    #[test]
    fn damaged_replica_shows_as_summary_divergence() {
        // 带伤启动(打开时跳过坏记录)必须在一致性巡检里表现为差异 —— 否则"副本少数据但报健康"会瞒过运维。
        let mut damaged = facts(102, 2, &[("aa", false, 4)]);
        damaged.report.as_mut().unwrap().damaged_records = 3;
        damaged.report.as_mut().unwrap().abandoned_segments = 1;
        let v = compare(&[facts(101, 2, &[("aa", false, 4)]), damaged]);
        assert!(!v.consistent, "{v:?}");
        assert!(
            v.divergences.iter().any(
                |d| matches!(d, Divergence::Summary { field, .. } if field == "damaged_records")
            ),
            "应报 damaged_records 差异: {v:?}"
        );
        assert!(
            v.divergences.iter().any(
                |d| matches!(d, Divergence::Summary { field, .. } if field == "abandoned_segments")
            ),
            "应报 abandoned_segments 差异: {v:?}"
        );
    }
}
