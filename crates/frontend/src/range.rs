//! REST `/blobs` 的 `Range` 头语义(blob-range-read P3,单区间)。
//!
//! 语义矩阵移植自 s3 网关(`crates/s3/src/handlers/range.rs` 的纯函数,口径两网一致);
//! 纯函数不触网络,矩阵由本文件内联单测把守。窗口装配(覆盖片/裁边)在 router 层
//! (`get_object_range_stream`),与网关解耦。

/// 已解析的字节窗口(`len ≥ 1`、`start + len ≤ size` 由构造路径保证)。
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

/// `Range` 头解析结论(四类)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RangeOutcome {
    /// 无头/语法坏/多区间/单位非 `bytes`:RFC 7233「MUST ignore」→ 200 全对象。
    Full,
    /// 可满足窗口 → 206 Partial Content。
    Partial(ByteWindow),
    /// 首 byte 越界 / 空对象 / `bytes=-0` → 416(携带原文便于观测)。
    Unsatisfiable { spec: String },
}

/// 解析 `Range` 头(单区间)。
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
    // 多区间(`bytes=0-1,3-4`):不做多段装配,按忽略回 200,不做 416 放大。
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
        // `bytes=-k`:末尾 k 字节(k >= size = 全对象)。
        (true, false) => match end_s.parse::<u64>() {
            Ok(k) => {
                if k == 0 || size == 0 {
                    unsat()
                } else {
                    let len = k.min(size);
                    RangeOutcome::Partial(ByteWindow {
                        start: size - len,
                        len,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn win(start: u64, len: u64) -> RangeOutcome {
        RangeOutcome::Partial(ByteWindow { start, len })
    }

    #[test]
    fn range_matrix() {
        const SIZE: u64 = 10;
        // 无头 → 200。
        assert_eq!(parse_range(None, SIZE), RangeOutcome::Full);
        // 三形式。
        assert_eq!(parse_range(Some("bytes=0-4"), SIZE), win(0, 5));
        assert_eq!(parse_range(Some("bytes=5-"), SIZE), win(5, 5));
        assert_eq!(parse_range(Some("bytes=-3"), SIZE), win(7, 3));
        // 后缀 ≥ 总长 = 全对象(非 416)。
        assert_eq!(parse_range(Some("bytes=-99"), SIZE), win(0, SIZE));
        // end 超界截断;start 恰在尾字节。
        assert_eq!(parse_range(Some("bytes=2-99"), SIZE), win(2, 8));
        assert_eq!(parse_range(Some("bytes=9-9"), SIZE), win(9, 1));
        // 首字节越界 → 416。
        assert_eq!(
            parse_range(Some("bytes=10-"), SIZE),
            RangeOutcome::Unsatisfiable {
                spec: "bytes=10-".into()
            }
        );
        // `bytes=-0` 不可满足;空对象任何 range 不可满足。
        assert_eq!(
            parse_range(Some("bytes=-0"), SIZE),
            RangeOutcome::Unsatisfiable {
                spec: "bytes=-0".into()
            }
        );
        assert_eq!(
            parse_range(Some("bytes=0-"), 0),
            RangeOutcome::Unsatisfiable {
                spec: "bytes=0-".into()
            }
        );
        // 忽略族:多区间 / 单位错 / 语法坏 / 数字坏 → 200。
        assert_eq!(parse_range(Some("bytes=0-1,3-4"), SIZE), RangeOutcome::Full);
        assert_eq!(parse_range(Some("chars=0-4"), SIZE), RangeOutcome::Full);
        assert_eq!(parse_range(Some("bytes=-"), SIZE), RangeOutcome::Full);
        assert_eq!(parse_range(Some("bytes=a-b"), SIZE), RangeOutcome::Full);
        assert_eq!(parse_range(Some("bytes=4-2"), SIZE), RangeOutcome::Full);
    }

    #[test]
    fn content_range_shape() {
        let w = ByteWindow { start: 2, len: 5 };
        assert_eq!(w.content_range(10), "bytes 2-6/10");
    }
}
