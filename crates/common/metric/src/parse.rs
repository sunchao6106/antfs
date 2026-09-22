//! Prometheus 文本导出格式的**极简解析器**(供测试断言 / 调试抓取用)。
//!
//! 只认两种样本行:`name value` 与 `name{lk="lv",lk2="lv2"} value`;`#` 注释行与空行跳过;
//! 解析失败的行静默跳过(不 panic)。不支持转义、时间戳、exemplar。

/// 一条样本:指标名 + 有序 label 对 + 值。
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// 解析整段 Prometheus 文本,返回全部可识别样本。
pub fn parse(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (name, labels, rest) = match line.find('{') {
        Some(open) => {
            let close = line.rfind('}')?;
            if close < open {
                return None;
            }
            (&line[..open], parse_labels(&line[open + 1..close])?, &line[close + 1..])
        }
        None => {
            let ws = line.find(char::is_whitespace)?;
            (&line[..ws], Vec::new(), &line[ws..])
        }
    };
    if name.is_empty() {
        return None;
    }
    let value: f64 = rest.split_whitespace().next()?.parse().ok()?;
    Some(Sample { name: name.to_string(), labels, value })
}

/// 解析 `lk="lv",lk2="lv2"`:label 值可含除 `"` 外任意字符(含 `,`/`=`),故逐段扫描而非 split。
fn parse_labels(s: &str) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = s;
    while !rest.is_empty() {
        let eq = rest.find('=')?;
        let key = rest[..eq].trim();
        let after_eq = &rest[eq + 1..];
        if key.is_empty() || !after_eq.starts_with('"') {
            return None;
        }
        let end = after_eq[1..].find('"')?;
        let value = &after_eq[1..1 + end];
        out.push((key.to_string(), value.to_string()));
        rest = after_eq[2 + end..].strip_prefix(',').unwrap_or_default();
        if rest.is_empty() && after_eq.len() > 2 + end {
            // 值后既不是逗号也不是结尾 → 格式错误
            return None;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(s: &Sample) -> &[(String, String)] {
        &s.labels
    }

    #[test]
    fn parses_plain_sample() {
        let s = parse("master_is_leader 1");
        assert_eq!(s, vec![Sample { name: "master_is_leader".into(), labels: vec![], value: 1.0 }]);
    }

    #[test]
    fn parses_float_and_int_values() {
        let s = parse("a 2\nb 2.5\nc -3");
        assert_eq!(s.iter().map(|x| x.value).collect::<Vec<_>>(), vec![2.0, 2.5, -3.0]);
    }

    #[test]
    fn parses_multi_labels() {
        let s = parse(r#"store_get_total{partition="7",replica="r1"} 42"#);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].name, "store_get_total");
        assert_eq!(
            labels(&s[0]),
            &[("partition".to_string(), "7".to_string()), ("replica".to_string(), "r1".to_string())]
        );
        assert_eq!(s[0].value, 42.0);
    }

    #[test]
    fn skips_comments_empty_and_broken_lines() {
        let text = "# HELP x help\n# TYPE x counter\n\nx 1\ngarbage\n{bad 2\ny{} 3\nz 4";
        let s = parse(text);
        let names: Vec<&str> = s.iter().map(|x| x.name.as_str()).collect();
        assert_eq!(names, vec!["x", "y", "z"]);
        assert!(labels(&s[1]).is_empty(), "empty label set must parse to no labels");
    }

    #[test]
    fn parses_histogram_bucket_lines() {
        let text = "# TYPE store_get_response_seconds histogram\nstore_get_response_seconds_bucket{le=\"0.05\"} 10\nstore_get_response_seconds_bucket{le=\"+Inf\"} 12\nstore_get_response_seconds_sum 0.3\nstore_get_response_seconds_count 12";
        let s = parse(text);
        assert_eq!(s.len(), 4);
        assert_eq!(s[0].name, "store_get_response_seconds_bucket");
        assert_eq!(labels(&s[0]), &[("le".to_string(), "0.05".to_string())]);
        assert_eq!(labels(&s[1]), &[("le".to_string(), "+Inf".to_string())]);
        assert_eq!(s[3].name, "store_get_response_seconds_count");
    }

    #[test]
    fn label_value_may_contain_comma_and_equals() {
        let s = parse(r#"m{k="a,b=c"} 1"#);
        assert_eq!(labels(&s[0]), &[("k".to_string(), "a,b=c".to_string())]);
    }
}
