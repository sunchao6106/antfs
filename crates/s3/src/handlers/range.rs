//! GET 读路径的 Range 语义与 multipart 取件窗口(s3-range-streaming)。
//!
//! 解析纯函数(`parse_range`)与窗口投影(`window_parts`)不触网络,语义矩阵由本文件
//! 内联单测把守(无 DB 依赖)。装配流 `PartStream` 是 multipart GET 的响应体:逐片
//! `router.get`,至多一片驻留内存(单片 ≤ `max_body_bytes` 不变量,见 change design.md D4)。

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;

use antfs_blob::StoreKey;

use crate::error::S3Error;
use crate::metastore::types::ManifestPart;
use crate::server::AppState;

/// 已解析的字节窗口(`len ≥ 1`;start+len ≤ size 由构造路径保证)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteWindow {
    pub start: u64,
    pub len: u64,
}

impl ByteWindow {
    /// `Content-Range: bytes start-end/total` 的 end(含端)。
    fn end_inclusive(&self) -> u64 {
        self.start + self.len - 1
    }
    pub(crate) fn content_range(&self, total: u64) -> String {
        format!("bytes {}-{}/{}", self.start, self.end_inclusive(), total)
    }
}

/// `Range` 头解析结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RangeOutcome {
    /// 无头/语法坏/多区间/单位非 `bytes`:RFC 7233「MUST ignore」→ 回 200 全对象。
    Full,
    /// 可满足窗口 → 206 Partial Content。
    Partial(ByteWindow),
    /// 首 byte 越界 / 空对象 / `bytes=-0`:S3 语义 → 416 `InvalidRange`(携带原文便于观测)。
    Unsatisfiable { spec: String },
}

/// 解析 `Range` 头(单区间语义,规则表见 change design.md D1)。
pub(crate) fn parse_range(raw: Option<&str>, size: u64) -> RangeOutcome {
    let Some(raw) = raw else {
        return RangeOutcome::Full;
    };
    let trim = raw.trim();
    let unsat = || RangeOutcome::Unsatisfiable {
        spec: trim.to_string(),
    };
    let Some(rest) = strip_bytes_unit(trim) else {
        return RangeOutcome::Full;
    };
    // 多区间(`bytes=0-1,3-4`):官方不做多段装配,按忽略回 200,不做 416 放大。
    if rest.contains(',') {
        return RangeOutcome::Full;
    }
    let Some((start_s, end_s)) = rest.split_once('-') else {
        return RangeOutcome::Full;
    };
    let (start_s, end_s) = (start_s.trim(), end_s.trim());
    match (start_s.is_empty(), end_s.is_empty()) {
        // `bytes=s-`:开区间,s < size 时截到对象尾。
        (false, true) => match start_s.parse() {
            Ok(start) if start < size => RangeOutcome::Partial(ByteWindow {
                start,
                len: size - start,
            }),
            Ok(_) => unsat(),
            Err(_) => RangeOutcome::Full,
        },
        // `bytes=-k`:末尾 k 字节。
        (true, false) => match end_s.parse() {
            Ok(k) => {
                if k == 0 || size == 0 {
                    // `bytes=-0` RFC 不可满足;空对象任何 range 皆不可满足。
                    unsat()
                } else if k >= size {
                    RangeOutcome::Partial(ByteWindow {
                        start: 0,
                        len: size,
                    })
                } else {
                    RangeOutcome::Partial(ByteWindow {
                        start: size - k,
                        len: k,
                    })
                }
            }
            Err(_) => RangeOutcome::Full,
        },
        // `bytes=s-e`:闭区间,e 超界截到 size-1。
        (false, false) => match (start_s.parse::<u64>(), end_s.parse::<u64>()) {
            (Ok(start), Ok(end)) => {
                if start > end {
                    // first > last:语法非法 → 忽略(RFC)。
                    RangeOutcome::Full
                } else if start >= size {
                    unsat()
                } else {
                    let end_incl = end.min(size - 1);
                    RangeOutcome::Partial(ByteWindow {
                        start,
                        len: end_incl - start + 1,
                    })
                }
            }
            _ => RangeOutcome::Full,
        },
        // `bytes=-`:双侧皆空,语法坏。
        (true, true) => RangeOutcome::Full,
    }
}

/// `bytes=` 前缀剥离(单位按 ASCII 大小写不敏感)。
fn strip_bytes_unit(spec: &str) -> Option<&str> {
    const UNIT: &str = "bytes=";
    if spec.len() >= UNIT.len() && spec[..UNIT.len()].eq_ignore_ascii_case(UNIT) {
        Some(&spec[UNIT.len()..])
    } else {
        None
    }
}

/// 单片取件计划:读该片 blob 后跳过 `skip` 字节、取 `take` 字节。
#[derive(Debug, Clone, Copy)]
pub(crate) struct WindowPart {
    pub key: StoreKey,
    pub skip: u64,
    pub take: u64,
}

/// 把 `[window.start, window.start+window.len)` 投影到覆盖片(按 manifest 尺寸推算,
/// 不触网络;非覆盖片零取件)。blob_id 解析失败立即 500(在响应发出前挡下)。
pub(crate) fn window_parts(
    parts: &[ManifestPart],
    window: ByteWindow,
) -> Result<Vec<WindowPart>, S3Error> {
    let end_excl = window.start + window.len;
    let mut out = Vec::new();
    let mut offset = 0u64;
    for part in parts {
        let part_end = offset + part.size;
        let cover_start = offset.max(window.start);
        let cover_end = part_end.min(end_excl);
        if cover_start < cover_end {
            let key = StoreKey::from_slice(&part.blob_id)
                .ok_or_else(|| S3Error::InternalError("bad blob_id in manifest".into()))?;
            out.push(WindowPart {
                key,
                skip: cover_start - offset,
                take: cover_end - cover_start,
            });
        }
        offset = part_end;
    }
    Ok(out)
}

/// manifest 各片尺寸之和(与 `named_objects.size` 对账用)。
pub(crate) fn manifest_total_size(parts: &[ManifestPart]) -> u64 {
    parts.iter().map(|p| p.size).sum()
}

/// `?partNumber=n` 的片窗(n 从 1 起);`n < 1` 或 `> 片数` → `None`(调用方 416)。
/// 片窗与 Range 窗同构——投影到取件计划直接走 [`window_parts`]。
pub(crate) fn manifest_part_window(parts: &[ManifestPart], n: i32) -> Option<ByteWindow> {
    if n < 1 {
        return None;
    }
    let n = n as usize;
    if n > parts.len() {
        return None;
    }
    let start: u64 = parts[..n - 1].iter().map(|p| p.size).sum();
    Some(ByteWindow {
        start,
        len: parts[n - 1].size,
    })
}

/// 两窗交集;空交 → `None`(调用方按 416)。`partNumber` 窗 × Range 窗组合用。
pub(crate) fn intersect_windows(a: ByteWindow, b: ByteWindow) -> Option<ByteWindow> {
    let start = a.start.max(b.start);
    let end = (a.start + a.len).min(b.start + b.len);
    if end <= start {
        return None;
    }
    Some(ByteWindow {
        start,
        len: end - start,
    })
}

/// multipart 装配流:顺序逐片 `router.get` →(可能切片)产出;**至多一片在内存**。
/// 尚未开取的每片保有完整的 R=1 failover(`router.get` 内建);首字节(首片)已
/// 发出后某片取回失败 = 响亮中断(连接以体错误收尾,不静默截短)。片短于 manifest
/// 声明 = InternalError(同一响亮原则)。
type PartFetch = Pin<Box<dyn Future<Output = Result<Bytes, S3Error>> + Send>>;

pub(crate) struct PartStream {
    st: AppState,
    parts: Vec<WindowPart>,
    idx: usize,
    pending: Option<PartFetch>,
}

impl PartStream {
    pub(crate) fn new(st: AppState, parts: Vec<WindowPart>) -> Self {
        Self {
            st,
            parts,
            idx: 0,
            pending: None,
        }
    }
}

impl Stream for PartStream {
    type Item = Result<Bytes, S3Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(fut) = self.pending.as_mut() {
                let data = match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(data)) => data,
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => return Poll::Pending,
                };
                self.pending = None;
                let wp = self.parts[self.idx];
                self.idx += 1;
                let start = wp.skip as usize;
                let end = start + wp.take as usize;
                if data.len() < end {
                    return Poll::Ready(Some(Err(S3Error::InternalError(format!(
                        "part blob shorter than manifest entry: got {} bytes, need {}..{}",
                        data.len(),
                        start,
                        end
                    )))));
                }
                return Poll::Ready(Some(Ok(data.slice(start..end))));
            }
            let Some(wp) = self.parts.get(self.idx).copied() else {
                return Poll::Ready(None);
            };
            let st = self.st.clone();
            self.pending = Some(Box::pin(async move {
                st.router.get(&wp.key).await.map_err(S3Error::from)
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn win(start: u64, len: u64) -> RangeOutcome {
        RangeOutcome::Partial(ByteWindow { start, len })
    }

    fn unsat(spec: &str) -> RangeOutcome {
        RangeOutcome::Unsatisfiable { spec: spec.into() }
    }

    #[test]
    fn no_header_is_full() {
        assert_eq!(parse_range(None, 100), RangeOutcome::Full);
    }

    /// `Content-Range` 头文本。
    #[test]
    fn content_range_format() {
        let w = ByteWindow { start: 93, len: 7 };
        assert_eq!(w.content_range(100), "bytes 93-99/100");
        let w = ByteWindow { start: 0, len: 1 };
        assert_eq!(w.content_range(1), "bytes 0-0/1");
    }

    /// 规则矩阵取齐 design.md D1(表驱动)。
    #[test]
    fn rule_matrix() {
        let cases: &[(Option<&str>, u64, RangeOutcome)] = &[
            // 基本闭区间
            (Some("bytes=0-0"), 1, win(0, 1)),
            (Some("bytes=10-19"), 100, win(10, 10)),
            // 开区间截到对象尾
            (Some("bytes=90-"), 100, win(90, 10)),
            (Some("bytes=0-"), 100, win(0, 100)),
            // end 超界截断
            (Some("bytes=90-1000"), 100, win(90, 10)),
            (Some("bytes=99-99"), 100, win(99, 1)),
            // 后缀
            (Some("bytes=-7"), 100, win(93, 7)),
            (Some("bytes=-100"), 100, win(0, 100)),
            (Some("bytes=-1000"), 100, win(0, 100)),
            // 首 byte 越界 → 416
            (Some("bytes=100-"), 100, unsat("bytes=100-")),
            (Some("bytes=100-200"), 100, unsat("bytes=100-200")),
            // 空对象:任何 range 不可满足
            (Some("bytes=0-0"), 0, unsat("bytes=0-0")),
            (Some("bytes=0-"), 0, unsat("bytes=0-")),
            (Some("bytes=-5"), 0, unsat("bytes=-5")),
            // `bytes=-0`:RFC 不可满足
            (Some("bytes=-0"), 100, unsat("bytes=-0")),
            // 语法坏/坏值 → 忽略(200 全对象)
            (Some("bytes=5-1"), 100, RangeOutcome::Full),
            (Some("bytes=abc"), 100, RangeOutcome::Full),
            (Some("bytes=abc-9"), 100, RangeOutcome::Full),
            (Some("bytes=1-xyz"), 100, RangeOutcome::Full),
            (Some("bytes=-"), 100, RangeOutcome::Full),
            (Some("bytes=1-2,4-5"), 100, RangeOutcome::Full),
            (Some("items=0-9"), 100, RangeOutcome::Full),
            // 大小写不敏感单位 + 空白容忍
            (Some("BYTES=1-2"), 100, win(1, 2)),
            (Some(" bytes=93-99 "), 100, win(93, 7)),
            (Some("bytes= 93 - 99 "), 100, win(93, 7)),
        ];
        for (i, (raw, size, want)) in cases.iter().enumerate() {
            let got = parse_range(*raw, *size);
            assert_eq!(got, *want, "case {i}: raw={raw:?} size={size}");
        }
    }

    fn part(id: u8, size: u64) -> ManifestPart {
        ManifestPart {
            blob_id: [id; 8],
            size,
        }
    }

    fn key(id: u8) -> StoreKey {
        StoreKey::from_slice(&[id; 8])
            .expect("StoreKey::from_slice 接受任意 8 字节(已核实,无 magic 校验)")
    }

    /// 覆盖窗口投影:全量 / 恰好整片 / 跨片切片 / 窗口外零取件。
    #[test]
    fn window_projection() {
        let parts = vec![part(1, 100), part(2, 100), part(3, 100)];

        // 全量窗口 = 三片全取。
        let w = window_parts(&parts, ByteWindow { start: 0, len: 300 }).unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(0, 100), (0, 100), (0, 100)]
        );

        // 恰好中间整片(只取片 2,首尾片零取件零网络)。
        let w = window_parts(
            &parts,
            ByteWindow {
                start: 100,
                len: 100,
            },
        )
        .unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(0, 100)]
        );
        assert_eq!(w[0].key.as_u64(), key(2).as_u64());

        // 跨片边界:90..210 → 尾 10B(片 1)+ 整片(片 2)+ 首 10B(片 3)。
        let w = window_parts(
            &parts,
            ByteWindow {
                start: 90,
                len: 120,
            },
        )
        .unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(90, 10), (0, 100), (0, 10)]
        );
        assert_eq!(w[2].key.as_u64(), key(3).as_u64());

        // 尾片纯切片:250..280 → 片 3 内 50..80。
        let w = window_parts(
            &parts,
            ByteWindow {
                start: 250,
                len: 30,
            },
        )
        .unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(50, 30)]
        );
        assert_eq!(w[0].key.as_u64(), key(3).as_u64());

        // 窗口只盖首片头部。
        let w = window_parts(&parts, ByteWindow { start: 0, len: 5 }).unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(0, 5)]
        );

        // 尺寸为零的片不产出取件(跳过,不挡推进)。
        let parts = vec![part(1, 10), part(2, 0), part(3, 10)];
        let w = window_parts(&parts, ByteWindow { start: 0, len: 20 }).unwrap();
        assert_eq!(
            w.iter().map(|p| (p.skip, p.take)).collect::<Vec<_>>(),
            vec![(0, 10), (0, 10)]
        );
        assert_eq!(w[1].key.as_u64(), key(3).as_u64());
    }

    #[test]
    fn manifest_sum() {
        let parts = vec![part(1, 1), part(2, u64::MAX / 2)];
        assert_eq!(manifest_total_size(&parts), 1 + u64::MAX / 2);
        assert_eq!(manifest_total_size(&[]), 0);
    }

    #[test]
    fn part_window_matrix() {
        let parts = vec![part(1, 10), part(2, 20), part(3, 30)];
        assert_eq!(
            manifest_part_window(&parts, 1),
            Some(ByteWindow { start: 0, len: 10 })
        );
        assert_eq!(
            manifest_part_window(&parts, 2),
            Some(ByteWindow { start: 10, len: 20 })
        );
        assert_eq!(
            manifest_part_window(&parts, 3),
            Some(ByteWindow { start: 30, len: 30 })
        );
        assert_eq!(manifest_part_window(&parts, 0), None);
        assert_eq!(manifest_part_window(&parts, 4), None);
        assert_eq!(manifest_part_window(&[], 1), None);
        let parts = vec![part(1, 10), part(2, 0)];
        assert_eq!(
            manifest_part_window(&parts, 2),
            Some(ByteWindow { start: 10, len: 0 })
        );
    }

    #[test]
    fn window_intersection() {
        let a = |s: u64, l: u64| ByteWindow { start: s, len: l };
        assert_eq!(intersect_windows(a(0, 100), a(10, 20)), Some(a(10, 20)));
        assert_eq!(intersect_windows(a(10, 20), a(0, 100)), Some(a(10, 20)));
        assert_eq!(intersect_windows(a(0, 10), a(5, 100)), Some(a(5, 5)));
        assert_eq!(intersect_windows(a(0, 10), a(10, 10)), None);
        assert_eq!(intersect_windows(a(0, 10), a(50, 10)), None);
        assert_eq!(intersect_windows(a(0, 0), a(0, 0)), None);
    }
}
