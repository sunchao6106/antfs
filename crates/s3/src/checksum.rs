//! flexible checksums(`s3-checksums`):四算法 registry 与请求侧收集/验证纯函数。
//!
//! 覆盖面:`CRC32`(IEEE)/ `CRC32C` / `SHA1` / `SHA256`,值一律标准 base64(带
//! 填充,AWS 口径);请求载具 = 显式头 `x-amz-checksum-{algo}` ∪ 分帧 trailer;
//! 校验基准 = **明文** body。CRC64NVME 等未实现族 → 501(升级路径名点);同一
//! 算法双载体并存 → 400(载体互斥)。持久化恰一份:并存时按强度取强(全验)。

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::error::S3Error;
use axum::http::HeaderMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChecksumAlgo {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
}

impl ChecksumAlgo {
    /// 请求头/回显头名(全小写)。
    pub fn lower_name(self) -> &'static str {
        match self {
            ChecksumAlgo::Crc32 => "x-amz-checksum-crc32",
            ChecksumAlgo::Crc32c => "x-amz-checksum-crc32c",
            ChecksumAlgo::Sha1 => "x-amz-checksum-sha1",
            ChecksumAlgo::Sha256 => "x-amz-checksum-sha256",
        }
    }
    /// 存储与 XML 元素用的算法名。
    pub fn algo_name(self) -> &'static str {
        match self {
            ChecksumAlgo::Crc32 => "CRC32",
            ChecksumAlgo::Crc32c => "CRC32C",
            ChecksumAlgo::Sha1 => "SHA1",
            ChecksumAlgo::Sha256 => "SHA256",
        }
    }
    fn digest_len(self) -> usize {
        match self {
            ChecksumAlgo::Crc32 | ChecksumAlgo::Crc32c => 4,
            ChecksumAlgo::Sha1 => 20,
            ChecksumAlgo::Sha256 => 32,
        }
    }
    /// var-name 对照:存库行 → 请求头名(migration 005 读回侧用)。
    pub fn from_algo_name(name: &str) -> Option<Self> {
        match name {
            "CRC32" => Some(ChecksumAlgo::Crc32),
            "CRC32C" => Some(ChecksumAlgo::Crc32c),
            "SHA1" => Some(ChecksumAlgo::Sha1),
            "SHA256" => Some(ChecksumAlgo::Sha256),
            _ => None,
        }
    }

    pub fn compute(self, data: &[u8]) -> String {
        let digest: Vec<u8> = match self {
            ChecksumAlgo::Crc32 => crc32fast::hash(data).to_be_bytes().to_vec(),
            ChecksumAlgo::Crc32c => crc32c::crc32c(data).to_be_bytes().to_vec(),
            ChecksumAlgo::Sha1 => {
                use sha1::{Digest, Sha1};
                Sha1::digest(data).to_vec()
            }
            ChecksumAlgo::Sha256 => {
                use sha2::{Digest, Sha256};
                Sha256::digest(data).to_vec()
            }
        };
        BASE64.encode(digest)
    }
}

/// 一条待验证的校验声明(载体:显式头或 trailer)。
#[derive(Debug, Clone)]
pub(crate) struct ChecksumClaim {
    pub algo: ChecksumAlgo,
    /// 原始 base64 文本(校验与持久化都存它;验证成功即合法形态)。
    pub value: String,
    pub from_trailer: bool,
}

/// 校验头形态(消息里点名用)。
fn carrier(algo: ChecksumAlgo, from_trailer: bool) -> String {
    if from_trailer {
        format!("streamed trailer {}", algo.lower_name())
    } else {
        algo.lower_name().to_string()
    }
}

/// 从请求头收集校验声明:已知四族成声明;未实现同族头(crc64nvme 等)即刻 501;
/// `x-amz-checksum-type` 只认 `FULL_OBJECT`(信息面,写路径不另存)。
pub(crate) fn collect_claim_headers(headers: &HeaderMap) -> Result<Vec<ChecksumClaim>, S3Error> {
    let mut claims = Vec::new();
    for (name, value) in headers {
        let Some(rest) = name.as_str().strip_prefix("x-amz-checksum-") else {
            continue;
        };
        if rest == "type" || rest == "mode" {
            continue;
        }
        if matches!(rest, "crc64nvme" | "crc64") {
            return Err(S3Error::NotImplemented(format!(
                "checksum algorithm `{}` is not implemented (supported: CRC32 / CRC32C / SHA1 / SHA256)",
                name.as_str()
            )));
        }
        let algo = match rest {
            "crc32" => ChecksumAlgo::Crc32,
            "crc32c" => ChecksumAlgo::Crc32c,
            "sha1" => ChecksumAlgo::Sha1,
            "sha256" => ChecksumAlgo::Sha256,
            _ => continue,
        };
        let raw = value
            .to_str()
            .map_err(|_| S3Error::InvalidArgument("checksum header value is not ASCII".into()))?;
        claims.push(ChecksumClaim {
            algo,
            value: raw.trim().to_string(),
            from_trailer: false,
        });
    }
    if let Some(type_value) = headers
        .get("x-amz-checksum-type")
        .and_then(|v| v.to_str().ok())
    {
        if !type_value.eq_ignore_ascii_case("FULL_OBJECT") {
            return Err(S3Error::InvalidArgument(format!(
                "x-amz-checksum-type must be FULL_OBJECT (got `{type_value}`)"
            )));
        }
    }
    Ok(claims)
}

/// 把 trailer 收集器(`TrailerSink` 之快照)并入声明集:同算法双载体 → 400;
/// `x-amz-trailer` 声明了校验头而 EOF 未见 → 400(残缺帧)。
pub(crate) fn merge_trailer_claims(
    mut claims: Vec<ChecksumClaim>,
    trailers: &std::collections::HashMap<String, String>,
    declared_trailer: Option<&str>,
) -> Result<Vec<ChecksumClaim>, S3Error> {
    if let Some(declared) = declared_trailer {
        let declared = declared.trim();
        if !declared.is_empty() && !declared.starts_with("x-amz-checksum-") {
            return Err(S3Error::NotImplemented(format!(
                "trailer header `{declared}` is not a checksum trailer (only x-amz-checksum-* is collected)"
            )));
        }
    }
    for (name, value) in trailers {
        let Some(rest) = name.strip_prefix("x-amz-checksum-") else {
            continue;
        };
        if matches!(rest, "crc64nvme" | "crc64") {
            return Err(S3Error::NotImplemented(format!(
                "checksum algorithm `{name}` is not implemented"
            )));
        }
        let Some(algo) = ChecksumAlgo::from_algo_name(&rest.to_uppercase()) else {
            continue;
        };
        if claims.iter().any(|c| c.algo == algo) {
            return Err(S3Error::InvalidArgument(format!(
                "checksum {} was supplied both as a request header and as a trailer (one carrier per form)",
                algo.lower_name()
            )));
        }
        claims.push(ChecksumClaim {
            algo,
            value: value.trim().to_string(),
            from_trailer: true,
        });
    }
    if let Some(declared) = declared_trailer.map(str::trim).filter(|s| !s.is_empty()) {
        if !trailers.contains_key(declared) {
            return Err(S3Error::InvalidArgument(format!(
                "declared trailer `{declared}` did not arrive (frame truncated?)"
            )));
        }
    }
    Ok(claims)
}

/// 解析值形态(严格 base64 + 算法长度),验算前的资格关。
fn parse_claim_value(claim: &ChecksumClaim) -> Result<Vec<u8>, S3Error> {
    let decoded = BASE64.decode(claim.value.as_bytes()).map_err(|_| {
        S3Error::InvalidArgument(format!(
            "checksum value for {} is not valid Base64",
            claim.algo.lower_name()
        ))
    })?;
    if decoded.len() != claim.algo.digest_len() {
        return Err(S3Error::InvalidArgument(format!(
            "checksum value for {} decodes to {} bytes (expected {})",
            claim.algo.lower_name(),
            decoded.len(),
            claim.algo.digest_len()
        )));
    }
    Ok(decoded)
}

/// 逐声明验算(明文 data):失败 → `BadDigest`(消息点名载体);全过 → 返回
/// 恰一份「待存」声明(最强算法)。
pub(crate) fn verify_claims(
    claims: &[ChecksumClaim],
    plaintext: &[u8],
) -> Result<Option<ChecksumClaim>, S3Error> {
    for claim in claims {
        parse_claim_value(claim)?;
        let computed = claim.algo.compute(plaintext);
        if !constant_time_eq(computed.as_bytes(), claim.value.as_bytes()) {
            return Err(S3Error::BadDigest {
                checksum_form: carrier(claim.algo, claim.from_trailer),
            });
        }
    }
    // Ord:Crc32 < Crc32c < Sha1 < Sha256(最强最后)。
    let best = claims.iter().max_by_key(|c| c.algo).cloned();
    Ok(best)
}

/// 常数时间比对:校验结果参与鉴权级判定(与 SigV4 验签同一习惯,防早期退出的
/// 时序侧信道)。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 读请求的 `x-amz-checksum-mode`:`ENABLED` → Some(true)(回显门开);其他值 → 400。
pub(crate) fn checksum_mode_enabled(headers: &HeaderMap) -> Result<bool, S3Error> {
    match headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        None => Ok(false),
        Some("ENABLED") => Ok(true),
        Some(other) => Err(S3Error::InvalidArgument(format!(
            "x-amz-checksum-mode must be ENABLED (got `{other}`)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).expect("lowercase"),
                v.parse().expect("ascii"),
            );
        }
        h
    }

    const ABC: &[u8] = b"abc";

    #[test]
    fn known_digest_vectors() {
        // CRC32("123456789") = 0xCBF43926;CRC32C = 0xE3069283(公开向量,BE 4 字节后 base64)。
        assert_eq!(ChecksumAlgo::Crc32.compute(b"123456789"), "y/Q5Jg==");
        assert_eq!(ChecksumAlgo::Crc32c.compute(b"123456789"), "4waSgw==");
        // SHA 家族以 OpenSSL 口径的公开向量(a2V5 47 井 对照)。
        assert_eq!(
            ChecksumAlgo::Sha1.compute(ABC),
            BASE64.encode(hex::decode("a9993e364706816aba3e25717850c26c9cd0d89d").expect("hex"))
        );
        assert_eq!(
            ChecksumAlgo::Sha256.compute(ABC),
            BASE64.encode(
                hex::decode("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                    .expect("hex")
            )
        );
        assert_eq!(BASE64.decode("y9VD4A==").expect("b64").len(), 4);
    }

    #[test]
    fn precedence_is_strongest_wins() {
        // Ord 序:Sha256 > Sha1 > Crc32c > Crc32。
        assert!(ChecksumAlgo::Sha256 > ChecksumAlgo::Sha1);
        assert!(ChecksumAlgo::Sha1 > ChecksumAlgo::Crc32c);
        assert!(ChecksumAlgo::Crc32c > ChecksumAlgo::Crc32);
        let claims = vec![
            ChecksumClaim {
                algo: ChecksumAlgo::Crc32,
                value: ChecksumAlgo::Crc32.compute(ABC),
                from_trailer: false,
            },
            ChecksumClaim {
                algo: ChecksumAlgo::Sha256,
                value: ChecksumAlgo::Sha256.compute(ABC),
                from_trailer: true,
            },
        ];
        let best = verify_claims(&claims, ABC).expect("both valid");
        assert_eq!(best.expect("some").algo, ChecksumAlgo::Sha256);
    }

    #[test]
    fn collect_and_merge_matrix() {
        let h = headers(&[("x-amz-checksum-crc32", "y9VD4A==")]);
        let claims = collect_claim_headers(&h).expect("known algo");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].algo, ChecksumAlgo::Crc32);
        assert!(!claims[0].from_trailer);

        let h = headers(&[("x-amz-checksum-crc64nvme", "AAAAAA==")]);
        let e = collect_claim_headers(&h).unwrap_err();
        assert!(matches!(e, S3Error::NotImplemented(_)), "{e}");

        let h = headers(&[("x-amz-checksum-type", "COMPOSITE")]);
        let e = collect_claim_headers(&h).unwrap_err();
        assert!(matches!(e, S3Error::InvalidArgument(_)), "{e}");

        let h = headers(&[
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
        ]);
        assert_eq!(collect_claim_headers(&h).expect("info faces").len(), 0);

        let mut trailers = std::collections::HashMap::new();
        trailers.insert(
            "x-amz-checksum-sha256".to_string(),
            ChecksumAlgo::Sha256.compute(ABC),
        );
        let h = headers(&[("x-amz-checksum-crc32", "y9VD4A==")]);
        let merged = merge_trailer_claims(
            collect_claim_headers(&h).expect("headers"),
            &trailers,
            Some("x-amz-checksum-sha256"),
        )
        .expect("distinct algos");
        assert_eq!(merged.len(), 2);
        assert!(merged[1].from_trailer);

        let h = headers(&[("x-amz-checksum-sha256", &BASE64.encode([0u8; 32]))]);
        let e = merge_trailer_claims(collect_claim_headers(&h).expect("headers"), &trailers, None)
            .unwrap_err();
        assert!(
            matches!(e, S3Error::InvalidArgument(_)),
            "dual carrier: {e}"
        );

        let e =
            merge_trailer_claims(Vec::new(), &trailers, Some("x-amz-checksum-crc32")).unwrap_err();
        assert!(
            matches!(e, S3Error::InvalidArgument(_)),
            "declared missing: {e}"
        );

        let e = merge_trailer_claims(
            Vec::new(),
            &std::collections::HashMap::new(),
            Some("x-amz-meta-foo"),
        )
        .unwrap_err();
        assert!(
            matches!(e, S3Error::NotImplemented(_)),
            "non-checksum trailer: {e}"
        );
    }

    #[test]
    fn verify_rejects_malformed_and_mismatch() {
        let claim_of = |value: String, algo: ChecksumAlgo| ChecksumClaim {
            algo,
            value,
            from_trailer: false,
        };
        let e = verify_claims(&[claim_of("not-base64!!".into(), ChecksumAlgo::Crc32)], ABC)
            .unwrap_err();
        assert!(matches!(e, S3Error::InvalidArgument(_)), "{e}");

        let e = verify_claims(
            &[claim_of(BASE64.encode([0u8; 20]), ChecksumAlgo::Crc32)],
            ABC,
        )
        .unwrap_err();
        assert!(matches!(e, S3Error::InvalidArgument(_)), "{e}");

        let e = verify_claims(
            &[claim_of(BASE64.encode([0u8; 4]), ChecksumAlgo::Crc32)],
            ABC,
        )
        .unwrap_err();
        assert!(matches!(e, S3Error::BadDigest { .. }), "{e}");

        let e = verify_claims(
            &[claim_of(
                ChecksumAlgo::Sha1.compute(ABC),
                ChecksumAlgo::Sha1,
            )],
            b"abd",
        )
        .unwrap_err();
        assert!(matches!(e, S3Error::BadDigest { .. }), "{e}");

        assert!(verify_claims(&[], ABC).expect("empty").is_none());
    }

    #[test]
    fn mode_gate() {
        assert!(!checksum_mode_enabled(&HeaderMap::new()).expect("absent"));
        assert!(
            checksum_mode_enabled(&headers(&[("x-amz-checksum-mode", "ENABLED")]))
                .expect("enabled")
        );
        assert!(checksum_mode_enabled(&headers(&[("x-amz-checksum-mode", "true")])).is_err());
    }
}
