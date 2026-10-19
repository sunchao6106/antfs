//! 桶策略(`s3-bucket-policy`):AWS 形态 JSON 策略文档的**有界子集**——数据面
//! 跨租户授权。put 时全量校验(不支持的字段响亮 400,不静默忽略),读侧把
//! `head_bucket` 载入的文档评估为 Deny/Allow/NotApplicable,叠在既有兜底
//! (owner 蕴含 / canned public-read 读族 / 私有 403)之上:显式 Deny 优先。
//! 存坏文档 = fail-closed 500(鉴权面绝不静默放松)。
//!
//! 出界(记档):Condition 运算符族、IAM ARN principal、非尾缀 glob、策略版本
//! 历史;管理面(?acl/?policy/location/quota/batch)恒 owner,不参与评估。

use crate::error::S3Error;
use serde_json::Value;

/// `buckets.policy` 文档上限(S3 同数字):put 界,越限 400。
const MAX_POLICY_BYTES: usize = 20 * 1024;

const ARN_PREFIX: &str = "arn:antfs:s3:::";
const POLICY_VERSION: &str = "2012-10-17";

/// 数据面动作集(评估 + 资源几何 + 兜底基线;`*` = 语句里的通配)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicyAction {
    GetObject,
    ListBucket,
    PutObject,
    DeleteObject,
    AbortMultipartUpload,
    ListMultipartUploadParts,
    ListBucketMultipartUploads,
}

impl PolicyAction {
    fn s3_name(self) -> &'static str {
        match self {
            PolicyAction::GetObject => "s3:GetObject",
            PolicyAction::ListBucket => "s3:ListBucket",
            PolicyAction::PutObject => "s3:PutObject",
            PolicyAction::DeleteObject => "s3:DeleteObject",
            PolicyAction::AbortMultipartUpload => "s3:AbortMultipartUpload",
            PolicyAction::ListMultipartUploadParts => "s3:ListMultipartUploadParts",
            PolicyAction::ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
        }
    }

    /// 对象级动作匹配 `…:::{bucket}/{key}` 资源;桶级动作只匹配桶裸 ARN。
    fn object_level(self) -> bool {
        !matches!(self, PolicyAction::ListBucket)
    }
}

/// 评估产物。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicyDecision {
    Deny,
    Allow,
    NotApplicable,
}

#[derive(Debug, Clone)]
struct Statement {
    allow: bool,
    /// `None` = 仅 `*`(任何签名者或匿名);Some(集合) = 具名 owner 集合(含 `*`)。
    principals: Vec<String>,
    actions: Vec<String>,
    resources: Vec<String>,
}

/// 已解析的策略文档(put 校验后的受信形态)。
#[derive(Debug, Clone)]
pub(crate) struct BucketPolicy {
    statements: Vec<Statement>,
}

#[derive(serde::Deserialize)]
struct RawDoc {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "Statement", default)]
    statement: Option<Value>,
}

fn bad(detail: impl Into<String>) -> S3Error {
    S3Error::InvalidRequest(detail.into())
}

impl BucketPolicy {
    /// put 入口:文档全量校验 + 解析。`bucket` 用于「资源属本桶」校验。
    pub(crate) fn parse(raw: &str, bucket: &str) -> Result<BucketPolicy, S3Error> {
        if raw.len() > MAX_POLICY_BYTES {
            return Err(S3Error::InvalidRequest(format!(
                "policy document exceeds {MAX_POLICY_BYTES} bytes"
            )));
        }
        if raw.trim().is_empty() {
            return Err(bad("policy document is empty"));
        }
        let doc: RawDoc =
            serde_json::from_str(raw).map_err(|e| bad(format!("policy is not valid JSON: {e}")))?;
        if doc.version != POLICY_VERSION {
            return Err(bad(format!(
                "policy Version must be \"{POLICY_VERSION}\" (got \"{}\")",
                doc.version
            )));
        }
        let Some(statement) = doc.statement else {
            return Err(bad("policy document is missing Statement"));
        };
        ensure_known_keys(&statement)?;
        let statements = parse_statements(&statement, bucket)?;
        Ok(BucketPolicy { statements })
    }

    /// 读侧入口:信任度 = 存库时已验;此路径仅人工改库才可达失败面 → 上层 500。
    pub(crate) fn parse_trusted(raw: &str, bucket: &str) -> Result<BucketPolicy, S3Error> {
        Self::parse(raw, bucket)
    }

    pub(crate) fn evaluate(
        &self,
        owner: Option<&str>,
        action: PolicyAction,
        bucket: &str,
        key: Option<&str>,
    ) -> PolicyDecision {
        let mut allow = false;
        for st in &self.statements {
            if !statement_matches(st, owner, action, bucket, key) {
                continue;
            }
            if !st.allow {
                return PolicyDecision::Deny;
            }
            allow = true;
        }
        if allow {
            PolicyDecision::Allow
        } else {
            PolicyDecision::NotApplicable
        }
    }
}

fn statement_matches(
    st: &Statement,
    owner: Option<&str>,
    action: PolicyAction,
    bucket: &str,
    key: Option<&str>,
) -> bool {
    let principal_hit = st.principals.iter().any(|p| p == "*")
        || owner.is_some_and(|o| st.principals.iter().any(|p| p == o));
    if !principal_hit {
        return false;
    }
    let action_hit = st.actions.iter().any(|a| a == "*" || a == action.s3_name());
    if !action_hit {
        return false;
    }
    st.resources.iter().any(|r| {
        let rest = match r.strip_prefix(ARN_PREFIX) {
            Some(rest) => rest,
            None => return false,
        };
        let (rbucket, suffix) = match rest.find('/') {
            Some(pos) => (&rest[..pos], Some(&rest[pos + 1..])),
            None => (rest, None),
        };
        if rbucket != bucket {
            return false;
        }
        match (suffix, action.object_level(), key) {
            (None, false, _) => true, // 桶裸 ARN 授权桶级动作
            (Some(pattern), true, Some(k)) => object_pattern_matches(pattern, k),
            _ => false, // 桶裸 ARN 不授权对象动作;对象资源对桶级动作不匹配
        }
    })
}

/// 对象资源模式:精确 or 尾缀 `prefix*`。
fn object_pattern_matches(pattern: &str, key: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => key.starts_with(prefix),
        None => pattern == key,
    }
}

/// 拒绝未知顶层/语句键(不支持即拒,防「带 Condition 存进来被静默忽略」的假安全)。
fn ensure_known_keys(statement: &Value) -> Result<(), S3Error> {
    match statement {
        Value::Array(items) => {
            for item in items {
                check_statement_keys(item)?;
                check_unsupported_elements(item)?;
            }
        }
        Value::Object(_) => {
            check_statement_keys(statement)?;
            check_unsupported_elements(statement)?;
        }
        _ => {
            return Err(bad(
                "policy Statement must be an object or array of objects",
            ))
        }
    }
    Ok(())
}

fn check_statement_keys(v: &Value) -> Result<(), S3Error> {
    let Value::Object(map) = v else {
        return Err(bad("policy statement entries must be objects"));
    };
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "Sid" | "Effect" | "Principal" | "Action" | "Resource"
        ) {
            return Err(bad(format!(
                "unsupported policy statement element `{key}` (Condition/Not* families are \
                 not supported by this gateway)"
            )));
        }
    }
    Ok(())
}

fn check_unsupported_elements(v: &Value) -> Result<(), S3Error> {
    let Value::Object(map) = v else {
        return Ok(());
    };
    for key in ["Condition", "NotPrincipal", "NotAction", "NotResource"] {
        if map.contains_key(key) {
            return Err(bad(format!(
                "unsupported policy element `{key}` (this gateway does not evaluate it)"
            )));
        }
    }
    Ok(())
}

fn as_string_list(v: &Value, element: &str) -> Result<Vec<String>, S3Error> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => return Err(bad(format!("policy {element} entries must be strings"))),
                }
            }
            Ok(out)
        }
        _ => Err(bad(format!(
            "policy {element} must be a string or array of strings"
        ))),
    }
}

fn parse_statements(statement: &Value, bucket: &str) -> Result<Vec<Statement>, S3Error> {
    let entries: Vec<&Value> = match statement {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![statement],
        Value::Null => return Ok(Vec::new()),
        _ => {
            return Err(bad(
                "policy Statement must be an object or array of objects",
            ))
        }
    };
    if entries.is_empty() {
        return Err(bad("policy Statement must not be empty"));
    }
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Object(map) = entry else {
            return Err(bad("policy statements must be objects"));
        };
        let effect = map
            .get("Effect")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("policy statement is missing Effect"))?;
        let allow = match effect {
            "Allow" => true,
            "Deny" => false,
            other => {
                return Err(bad(format!(
                    "policy Effect must be Allow or Deny (got \"{other}\")"
                )))
            }
        };
        let principals = principals_of(map.get("Principal"))?;
        let actions = as_string_list(
            map.get("Action")
                .ok_or_else(|| bad("policy statement is missing Action"))?,
            "Action",
        )?;
        for a in &actions {
            validate_action_name(a)?;
        }
        let resources = as_string_list(
            map.get("Resource")
                .ok_or_else(|| bad("policy statement is missing Resource"))?,
            "Resource",
        )?;
        for r in &resources {
            validate_resource(r, bucket)?;
        }
        if let Some(Value::String(sid)) = map.get("Sid") {
            if sid.contains(|c: char| !c.is_ascii() && !c.is_whitespace()) {
                return Err(bad("policy Sid must be a plain ASCII label"));
            }
        }
        out.push(Statement {
            allow,
            principals,
            actions,
            resources,
        });
    }
    Ok(out)
}

fn principals_of(v: Option<&Value>) -> Result<Vec<String>, S3Error> {
    let Some(v) = v else {
        return Err(bad("policy statement is missing Principal"));
    };
    match v {
        Value::String(s) => {
            if s == "*" {
                return Ok(vec!["*".into()]);
            }
            Err(bad("policy Principal must be a wildcard or an AWS map"))
        }
        Value::Object(map) => {
            if map.len() != 1 || !map.contains_key("AWS") {
                return Err(bad("policy Principal supports only the AWS key"));
            }
            let list = as_string_list(&map["AWS"], "Principal.AWS")?;
            for p in &list {
                if p == "*" {
                    continue;
                }
                if p.starts_with("arn:") || p.contains(':') || p.contains('/') {
                    return Err(bad(format!(
                        "unsupported principal `{p}` (IAM ARNs are not supported; use the \
                         credential owner id or \"*\")"
                    )));
                }
            }
            Ok(list)
        }
        _ => Err(bad("policy Principal must be an object or \"*\"")),
    }
}

fn validate_action_name(a: &str) -> Result<(), S3Error> {
    if a == "*" {
        return Ok(());
    }
    let known = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:ListBucketMultipartUploads",
    ];
    if known.contains(&a) {
        Ok(())
    } else {
        Err(bad(format!(
            "unknown or unsupported policy action `{a}` (supported: s3:GetObject, \
             s3:ListBucket, s3:PutObject, s3:DeleteObject, s3:AbortMultipartUpload, \
            s3:ListMultipartUploadParts, s3:ListBucketMultipartUploads, *)"
        )))
    }
}

fn validate_resource(r: &str, bucket: &str) -> Result<(), S3Error> {
    let Some(rest) = r.strip_prefix(ARN_PREFIX) else {
        return Err(bad(format!(
            "policy resource `{r}` must be {ARN_PREFIX}{{bucket}} or \
             {ARN_PREFIX}{{{bucket}/key-pattern}}"
        )));
    };
    let (rbucket, suffix) = match rest.find('/') {
        Some(pos) => (&rest[..pos], &rest[pos + 1..]),
        None => (rest, ""),
    };
    if rbucket != bucket {
        return Err(bad(format!(
            "policy resource `{r}` does not belong to bucket `{bucket}`"
        )));
    }
    if r.matches('*').count() > 1 || (suffix.contains('*') && !suffix.ends_with('*')) {
        return Err(bad(
            "policy resource patterns support only a single trailing `*`",
        ));
    }
    if rest.contains("**") || (!suffix.is_empty() && suffix.starts_with('/') && suffix.len() > 1) {
        return Err(bad("policy resource has a malformed key pattern"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(json: &str) -> BucketPolicy {
        BucketPolicy::parse(json, "b").expect("valid policy")
    }

    fn statement_json(effect: &str, principal: &str, action: &str, resource: &str) -> String {
        format!(
            r#"{{"Effect":"{effect}","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{resource}"}}"#
        )
    }

    fn doc(effect: &str, principal: &str, action: &str, resource: &str) -> String {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{}]}}"#,
            statement_json(effect, principal, action, resource)
        )
    }

    #[test]
    fn parse_rejects_unsupported_elements() {
        for bad_doc in [
            r#"{"Version":"2008-10-17","Statement":[]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"x"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::b/*","Condition":{"a":"b"}}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1:user/u"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::b/*"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"o"},"Action":"s3:TagObjects","Resource":"arn:antfs:s3:::b"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"o"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::other/*"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Maybe","Principal":{"AWS":"o"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::b"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Principal":{"AWS":"o"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::b"}]}"#,
        ] {
            assert!(
                BucketPolicy::parse(bad_doc, "b").is_err(),
                "must reject: {}",
                &bad_doc[..bad_doc.len().min(60)]
            );
        }
    }

    #[test]
    fn evaluation_order_and_geometry() {
        // 跨租户前缀读:Allow getObject on prefix。
        let p = policy(&doc(
            "Allow",
            "tenant-b",
            "s3:GetObject",
            "arn:antfs:s3:::b/Reports/*",
        ));
        assert_eq!(
            p.evaluate(
                Some("tenant-b"),
                PolicyAction::GetObject,
                "b",
                Some("Reports/q1")
            ),
            PolicyDecision::Allow
        );
        assert_eq!(
            p.evaluate(
                Some("tenant-b"),
                PolicyAction::GetObject,
                "b",
                Some("Secrets/x")
            ),
            PolicyDecision::NotApplicable
        );
        assert_eq!(
            p.evaluate(Some("tenant-b"), PolicyAction::ListBucket, "b", None),
            PolicyDecision::NotApplicable
        );
        assert_eq!(
            p.evaluate(None, PolicyAction::GetObject, "b", Some("Reports/q1")),
            PolicyDecision::NotApplicable,
            "匿名只匹配 *"
        );

        // 显式 Deny 压 Allow。
        let both = format!(
            r#"{{"Version":"2012-10-17","Statement":[{},{}]}}"#,
            statement_json("Allow", "o1", "s3:PutObject", "arn:antfs:s3:::b/*"),
            statement_json("Deny", "o1", "s3:PutObject", "arn:antfs:s3:::b/*")
        );
        let p = policy(&both);
        assert_eq!(
            p.evaluate(Some("o1"), PolicyAction::PutObject, "b", Some("k")),
            PolicyDecision::Deny
        );

        // 桶裸 ARN 授权桶级动作,不授权对象动作;`*` principal 放匿名。
        let p = policy(&doc("Allow", "*", "s3:ListBucket", "arn:antfs:s3:::b"));
        assert_eq!(
            p.evaluate(None, PolicyAction::ListBucket, "b", None),
            PolicyDecision::Allow
        );
        assert_eq!(
            p.evaluate(None, PolicyAction::GetObject, "b", Some("k")),
            PolicyDecision::NotApplicable
        );

        // `*` 动作 = 全动作(对象级动作 + 对象资源)。
        let p = policy(&doc("Allow", "*", "*", "arn:antfs:s3:::b/*"));
        assert_eq!(
            p.evaluate(None, PolicyAction::DeleteObject, "b", Some("k")),
            PolicyDecision::Allow
        );
    }
}
