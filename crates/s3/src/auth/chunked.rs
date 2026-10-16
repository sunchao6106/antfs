//! `aws-chunked` 流式分帧解码与块签名链验证(`s3-chunked-payload`)。
//!
//! 服务端两件事:①申报一致性(三个 STREAMING 字面量 + `Content-Encoding:
//! aws-chunked` + `x-amz-decoded-content-length` 配套,撕裂即 400,验签前完成、
//! 不触库);②`AwsChunkedBody` 把分帧流解成明文流,带签名风味逐块校验块签名链
//! (种子 = 请求头 Signature),trailer 段形内解析、校验值明示忽略(信任面 =
//! UNSIGNED-PAYLOAD)。解码错误直接以 `S3Error` 出 Body,handler 侧
//! `axum::Error::downcast` 精确回放。
//!
//! Reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html>

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::Request;
use axum::http::HeaderMap;
use bytes::{Buf, Bytes, BytesMut};
use hmac::{Hmac, Mac};
use http_body::{Body as HttpBody, Frame};
use sha2::{Digest, Sha256};

use antfs_metric::IntCounterVec;

use super::sigv4::{self, AuthInfo};
use crate::error::S3Error;

pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
const CHUNK_SIG_EXT: &str = "chunk-signature=";
const CHUNK_PREFIX: &str = "AWS4-HMAC-SHA256-PAYLOAD";
/// 块头行(含 CRLF)上界;合法行远短于此(64 hex 签名 + 尺寸)。
const MAX_HEADER_LINE_BYTES: usize = 1024;
/// trailer 段总字节上界(合法:几行 checksum 头)。
const MAX_TRAILER_BYTES: usize = 8 * 1024;

/// 分帧风味,由 `x-amz-content-sha256` 字面量决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkFlavor {
    /// 数据块带签名,无 trailer 段。
    Signed,
    /// 数据块带签名 + trailer 段。
    SignedTrailer,
    /// 数据块无签名 + trailer 段(块链不验,trailer 校验值忽略)。
    UnsignedTrailer,
}

impl ChunkFlavor {
    fn is_signed(self) -> bool {
        matches!(self, ChunkFlavor::Signed | ChunkFlavor::SignedTrailer)
    }
    fn is_trailer(self) -> bool {
        !matches!(self, ChunkFlavor::Signed)
    }
}

/// 块签名链上下文:与请求头验签同一条密钥链,种子 = 请求签名。
pub struct SigCtx {
    key: [u8; 32],
    timestamp: String,
    scope: String,
    seed: String,
}

/// Debug 印章脱敏:密钥只落 `[redacted]`,种子签名本就随线上内容公开。
impl std::fmt::Debug for SigCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigCtx")
            .field("key", &"[redacted]")
            .field("timestamp", &self.timestamp)
            .field("scope", &self.scope)
            .field("seed", &self.seed)
            .finish()
    }
}

/// 验签后成型的完整分帧情报(交流水线包体)。
#[derive(Debug)]
pub struct ChunkedInfo {
    pub flavor: ChunkFlavor,
    pub declared: u64,
    pub sig: Option<SigCtx>,
}

/// 申报面(分类器产物):凭据验证前的形状一致性结论。
pub struct ChunkedShape {
    flavor: ChunkFlavor,
    declared: u64,
}

impl ChunkedShape {
    /// 凭据验签成功后补齐签名材料;无签名风味无链可验。
    pub fn into_info(self, secret_key: &str, info: &AuthInfo, timestamp: &str) -> ChunkedInfo {
        let sig = if self.flavor.is_signed() {
            Some(SigCtx {
                key: sigv4::signing_key(secret_key, &info.date, &info.region, &info.service)
                    .expect(
                    "signing_key is infallible for any key length once header verification passed",
                ),
                timestamp: timestamp.to_string(),
                scope: sigv4::credential_scope(info),
                seed: info.signature.clone(),
            })
        } else {
            None
        };
        ChunkedInfo {
            flavor: self.flavor,
            declared: self.declared,
            sig,
        }
    }
}

/// 申报一致性分类。`Ok(None)` = 无任何流式信号(既有路径零改动旁路);
/// `Err` = 有信号但不一致(400);`Ok(Some(_))` = 合法申报。
pub fn classify(
    method: &str,
    headers: &HeaderMap,
    presigned: bool,
) -> Result<Option<ChunkedShape>, S3Error> {
    let literal = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok());
    let has_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("aws-chunked"))
        .unwrap_or(false);
    let decoded = headers
        .get("x-amz-decoded-content-length")
        .and_then(|v| v.to_str().ok());

    let flavor = match literal {
        Some(v) if v == STREAMING_SIGNED => Some(ChunkFlavor::Signed),
        Some(v) if v == STREAMING_SIGNED_TRAILER => Some(ChunkFlavor::SignedTrailer),
        Some(v) if v == STREAMING_UNSIGNED_TRAILER => Some(ChunkFlavor::UnsignedTrailer),
        Some(v) if v.starts_with("STREAMING-") => {
            return Err(invalid_argument(format!(
                "unsupported streaming payload algorithm `{v}`"
            )))
        }
        _ => None,
    };

    if flavor.is_none() && !has_encoding && decoded.is_none() {
        return Ok(None);
    }

    let flavor = flavor.ok_or_else(|| {
        invalid_argument("aws-chunked framing requires a STREAMING-* x-amz-content-sha256 literal")
    })?;
    if presigned {
        return Err(invalid_argument(
            "presigned URLs cannot use streaming aws-chunked framing",
        ));
    }
    if !has_encoding {
        return Err(invalid_argument(
            "streaming payload requires Content-Encoding: aws-chunked",
        ));
    }
    if !matches!(method, "PUT" | "POST") {
        return Err(invalid_argument(
            "streaming aws-chunked framing applies to PUT/POST requests only",
        ));
    }
    let decoded = decoded.ok_or_else(|| {
        invalid_argument("missing x-amz-decoded-content-length for streaming payload")
    })?;
    let declared: u64 = decoded
        .parse()
        .map_err(|_| invalid_argument("x-amz-decoded-content-length must be an integer"))?;

    Ok(Some(ChunkedShape { flavor, declared }))
}

/// 流水线包体:申报超限零流读预拒;替体为 `AwsChunkedBody`。
pub fn wrap_request(
    mut req: Request,
    info: ChunkedInfo,
    max_body_bytes: u64,
    requests_total: &antfs_metric::IntCounter,
    rejected_total: &IntCounterVec,
) -> Result<Request, S3Error> {
    if info.declared > max_body_bytes {
        rejected_total.with_label_values(&["oversize"]).inc();
        return Err(S3Error::EntityTooLarge);
    }
    requests_total.inc();
    let inner = std::mem::replace(req.body_mut(), Body::empty());
    let flavor = info.flavor;
    let sink: TrailerSink =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let decoder = AwsChunkedBody::new(
        inner,
        flavor,
        info.declared,
        info.sig,
        sink.clone(),
        rejected_total.clone(),
    );
    *req.body_mut() = Body::new(decoder);
    req.extensions_mut().insert(ChunkedTrailers(sink));
    Ok(req)
}

fn invalid_argument(detail: impl Into<String>) -> S3Error {
    S3Error::InvalidArgument(format!("aws-chunked: {}", detail.into()))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn is_hex_64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

enum DecodeState {
    Header,
    Data {
        remaining: u64,
    },
    /// 块数据出完:消费尾 CRLF,含验签(有链时)。
    DataEnd {
        expected_sig: Option<String>,
        final_chunk: bool,
    },
    Trailer,
    DoneComplete,
}

/// 分帧解码体:`http_body::Body` 包装,`Error = S3Error`(解码错误即最终语义,
/// handler 侧 `axum::Error::downcast::<S3Error>` 精确回放)。
/// trailer 行收集器(名→值;名小写归一)。分帧 body 与写路径 handler 各持克隆,
/// 经 `request extensions` 跨越所有权边界。
pub type TrailerSink = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>;

/// extensions 载体(body 消费完毕后,写路径从这里取 trailer 快照)。
#[derive(Clone)]
pub struct ChunkedTrailers(pub TrailerSink);

pub struct AwsChunkedBody {
    inner: Body,
    flavor: ChunkFlavor,
    declared: u64,
    decoded: u64,
    /// 块签名链:前一块签名(种子 = 请求头 Signature)。
    prev_sig: Option<String>,
    /// 当前线程待出块的头申报签名(Header 相存入,Data 相取出)。
    pending_sig: Option<String>,
    timestamp: String,
    scope: String,
    key: Option<[u8; 32]>,
    hasher: Option<Sha256>,
    buffer: BytesMut,
    eof_inner: bool,
    failed: bool,
    state: DecodeState,
    trailer_bytes: usize,
    /// trailer 段头行收集(写路径校验用);`wrap_request` 把同一 sink 挂进
    /// request extensions,body 读完(EOF)后可读。
    trailers: TrailerSink,
    rejected_total: IntCounterVec,
}

impl AwsChunkedBody {
    fn new(
        inner: Body,
        flavor: ChunkFlavor,
        declared: u64,
        sig: Option<SigCtx>,
        trailers: TrailerSink,
        rejected_total: IntCounterVec,
    ) -> Self {
        let (key, timestamp, scope, prev_sig) = match sig {
            Some(ctx) => (Some(ctx.key), ctx.timestamp, ctx.scope, Some(ctx.seed)),
            None => (None, String::new(), String::new(), None),
        };
        AwsChunkedBody {
            inner,
            flavor,
            declared,
            decoded: 0,
            prev_sig,
            pending_sig: None,
            timestamp,
            scope,
            key,
            hasher: if flavor.is_signed() {
                Some(Sha256::new())
            } else {
                None
            },
            buffer: BytesMut::new(),
            eof_inner: false,
            failed: false,
            state: DecodeState::Header,
            trailer_bytes: 0,
            trailers,
            rejected_total,
        }
    }

    fn fail(
        &mut self,
        reason: &'static str,
        error: S3Error,
    ) -> Poll<Option<Result<Frame<Bytes>, S3Error>>> {
        self.rejected_total.with_label_values(&[reason]).inc();
        self.failed = true;
        Poll::Ready(Some(Err(error)))
    }

    /// 块头行:`hex-size[;chunk-signature=64hex]`;风味与扩展存在性强校验。
    fn on_header_line(&mut self, line: String) -> Result<(), S3Error> {
        let (size_hex, ext) = match line.split_once(';') {
            Some((size, ext)) => (size, Some(ext)),
            None => (line.as_str(), None),
        };
        let size: u64 = u64::from_str_radix(size_hex, 16)
            .map_err(|_| invalid_argument("chunk size must be hexadecimal"))?;
        let expected_sig = match ext {
            Some(ext) => {
                if !self.flavor.is_signed() {
                    return Err(invalid_argument(
                        "chunk-signature present in unsigned streaming flavor",
                    ));
                }
                let sig = ext
                    .strip_prefix(CHUNK_SIG_EXT)
                    .ok_or_else(|| invalid_argument("unsupported chunk header extension"))?;
                if !is_hex_64(sig) {
                    return Err(invalid_argument(
                        "chunk signature must be 64 hex characters",
                    ));
                }
                Some(sig.to_string())
            }
            None => {
                if self.flavor.is_signed() {
                    return Err(invalid_argument("signed chunk missing signature"));
                }
                None
            }
        };
        if self.decoded + size > self.declared {
            return Err(invalid_argument(
                "chunk data exceeds declared decoded length",
            ));
        }
        if self.flavor.is_signed() {
            self.hasher = Some(Sha256::new());
        }
        self.pending_sig = expected_sig;
        self.state = if size == 0 {
            DecodeState::DataEnd {
                expected_sig: self.pending_sig.take(),
                final_chunk: true,
            }
        } else {
            DecodeState::Data { remaining: size }
        };
        Ok(())
    }

    /// 校验当前块头申报签名并推进链(前块签名 = 下块种子)。
    fn verify_chunk_sig(&mut self, expected_sig: &str) -> Result<(), S3Error> {
        let (key, prev) = match (&self.key, &self.prev_sig) {
            (Some(k), Some(p)) => (k, p),
            _ => return Ok(()),
        };
        let digest = self
            .hasher
            .take()
            .map(|h| h.finalize())
            .unwrap_or_else(|| Sha256::digest(b""));
        let string_to_sign = format!(
            "{CHUNK_PREFIX}\n{}\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            prev,
            hex::encode(digest)
        );
        let computed = hex::encode(hmac_sha256(key, string_to_sign.as_bytes()));
        sigv4::constant_time_eq_hex(&computed, expected_sig)?;
        Ok(())
    }

    /// 从内层原始流补缓冲;`Pending` = 无新进展可等。
    fn fill(&mut self, cx: &mut Context<'_>) -> Result<bool, S3Error> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Ok(data) = frame.into_data() {
                    self.buffer.extend_from_slice(&data);
                }
                Ok(true)
            }
            Poll::Ready(Some(Err(e))) => Err(S3Error::InternalError(format!(
                "request body transport error: {e}"
            ))),
            Poll::Ready(None) => {
                self.eof_inner = true;
                Ok(true)
            }
            Poll::Pending => Ok(false),
        }
    }
}

impl HttpBody for AwsChunkedBody {
    type Data = Bytes;
    type Error = S3Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, S3Error>>> {
        let this = &mut *self;
        loop {
            if this.failed {
                return Poll::Ready(Some(Err(S3Error::InternalError(
                    "aws-chunked body already failed".into(),
                ))));
            }
            match this.state {
                DecodeState::Header => match find_crlf(&this.buffer) {
                    Some(pos) => {
                        if pos == 0 {
                            return this
                                .fail("framing", invalid_argument("empty chunk header line"));
                        }
                        if pos > MAX_HEADER_LINE_BYTES {
                            return this
                                .fail("framing", invalid_argument("chunk header line too long"));
                        }
                        let mut line = this.buffer.split_to(pos + 2);
                        line.truncate(pos);
                        let line = String::from_utf8_lossy(&line).into_owned();
                        match this.on_header_line(line) {
                            Ok(()) => continue,
                            Err(e) => return this.fail("framing", e),
                        }
                    }
                    None => {
                        if this.buffer.len() > MAX_HEADER_LINE_BYTES {
                            return this
                                .fail("framing", invalid_argument("chunk header line too long"));
                        }
                        if this.eof_inner {
                            return this.fail("length", S3Error::IncompleteBody);
                        }
                        match this.fill(cx) {
                            Ok(true) => continue,
                            Ok(false) => return Poll::Pending,
                            Err(e) => return this.fail("framing", e),
                        }
                    }
                },
                DecodeState::Data { remaining } => {
                    if this.buffer.is_empty() {
                        if this.eof_inner {
                            return this.fail("length", S3Error::IncompleteBody);
                        }
                        match this.fill(cx) {
                            Ok(true) => continue,
                            Ok(false) => return Poll::Pending,
                            Err(e) => return this.fail("framing", e),
                        }
                    }
                    let n = this.buffer.len().min(remaining as usize);
                    let data: Bytes = Bytes::from(this.buffer.split_to(n));
                    if let Some(h) = this.hasher.as_mut() {
                        h.update(&data);
                    }
                    this.decoded += n as u64;
                    if n as u64 == remaining {
                        this.state = DecodeState::DataEnd {
                            expected_sig: this.pending_sig.take(),
                            final_chunk: false,
                        };
                    } else {
                        this.state = DecodeState::Data {
                            remaining: remaining - n as u64,
                        };
                    }
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                DecodeState::DataEnd {
                    ref expected_sig,
                    final_chunk,
                } => {
                    let expected_sig = expected_sig.clone();
                    if !final_chunk {
                        if this.buffer.len() < 2 {
                            if this.eof_inner {
                                return this.fail("length", S3Error::IncompleteBody);
                            }
                            match this.fill(cx) {
                                Ok(true) => continue,
                                Ok(false) => return Poll::Pending,
                                Err(e) => return this.fail("framing", e),
                            }
                        }
                        if this.buffer[0] != b'\r' || this.buffer[1] != b'\n' {
                            return this.fail(
                                "framing",
                                invalid_argument("missing CRLF after chunk data"),
                            );
                        }
                        this.buffer.advance(2);
                    } else if this.buffer.starts_with(b"\r\n") {
                        this.buffer.advance(2);
                    }
                    if this.flavor.is_signed() {
                        let sig = match expected_sig {
                            Some(sig) => sig,
                            None => {
                                return this.fail(
                                    "framing",
                                    invalid_argument("signed chunk missing signature"),
                                )
                            }
                        };
                        if let Err(e) = this.verify_chunk_sig(&sig) {
                            return this.fail("signature", e);
                        }
                        this.prev_sig = Some(sig);
                    }
                    this.state = if final_chunk {
                        if this.flavor.is_trailer() {
                            DecodeState::Trailer
                        } else {
                            DecodeState::DoneComplete
                        }
                    } else {
                        DecodeState::Header
                    };
                    continue;
                }
                DecodeState::Trailer => match find_crlf(&this.buffer) {
                    Some(pos) => {
                        let mut line = this.buffer.split_to(pos + 2);
                        line.truncate(pos);
                        this.trailer_bytes += pos + 2;
                        if this.trailer_bytes > MAX_TRAILER_BYTES {
                            return this
                                .fail("framing", invalid_argument("trailer section too long"));
                        }
                        if pos == 0 {
                            this.state = DecodeState::DoneComplete;
                            continue;
                        }
                        let line = String::from_utf8_lossy(&line).into_owned();
                        if !line.contains(':') {
                            return this.fail(
                                "framing",
                                invalid_argument("malformed trailer header line"),
                            );
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            this.trailers
                                .lock()
                                .expect("trailer sink lock")
                                .insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
                        }
                        continue;
                    }
                    None => {
                        if this.eof_inner {
                            if this.buffer.is_empty() {
                                this.state = DecodeState::DoneComplete;
                                continue;
                            }
                            return this
                                .fail("framing", invalid_argument("truncated trailer section"));
                        }
                        if this.buffer.len() > MAX_TRAILER_BYTES {
                            return this
                                .fail("framing", invalid_argument("trailer section too long"));
                        }
                        match this.fill(cx) {
                            Ok(true) => continue,
                            Ok(false) => return Poll::Pending,
                            Err(e) => return this.fail("framing", e),
                        }
                    }
                },
                DecodeState::DoneComplete => {
                    if !this.buffer.is_empty() {
                        if this.buffer.iter().all(|b| *b == b'\r' || *b == b'\n') {
                            this.buffer.clear();
                        } else {
                            return this.fail(
                                "framing",
                                invalid_argument("trailing bytes after final chunk"),
                            );
                        }
                    }
                    if !this.eof_inner {
                        if !this.buffer.is_empty() {
                            continue;
                        }
                        match this.fill(cx) {
                            Ok(true) => continue,
                            Ok(false) => return Poll::Pending,
                            Err(e) => return this.fail("framing", e),
                        }
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/7MDENG+bPxRfiCYEXAMPLEKEY";
    const TS: &str = "20261016T000000Z";
    const SCOPE: &str = "20261016/us-east-1/s3/aws4_request";
    const SEED: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn key() -> [u8; 32] {
        sigv4::signing_key(SECRET, "20261016", "us-east-1", "s3").unwrap()
    }

    fn sig_ctx() -> SigCtx {
        SigCtx {
            key: key(),
            timestamp: TS.to_string(),
            scope: SCOPE.to_string(),
            seed: SEED.to_string(),
        }
    }

    fn chunk_sig(key: &[u8], prev: &str, data: &[u8]) -> String {
        let sts = format!(
            "{CHUNK_PREFIX}\n{TS}\n{SCOPE}\n{prev}\n{}",
            hex::encode(Sha256::digest(data))
        );
        hex::encode(hmac_sha256(key, sts.as_bytes()))
    }

    fn encode_signed(data: &[u8], chunk_len: usize) -> Bytes {
        let key = key();
        let mut prev = SEED.to_string();
        let mut out = Vec::new();
        for piece in data.chunks(chunk_len.max(1)) {
            let sig = chunk_sig(&key, &prev, piece);
            out.extend_from_slice(
                format!("{:x};{}{sig}\r\n", piece.len(), CHUNK_SIG_EXT).as_bytes(),
            );
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
            prev = sig;
        }
        let final_sig = chunk_sig(&key, &prev, b"");
        out.extend_from_slice(format!("0;{}{final_sig}\r\n\r\n", CHUNK_SIG_EXT).as_bytes());
        Bytes::from(out)
    }

    fn encode_unsigned_trailer(data: &[u8], chunk_len: usize) -> Bytes {
        let mut out = Vec::new();
        for piece in data.chunks(chunk_len.max(1)) {
            out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"0\r\n");
        out.extend_from_slice(b"x-amz-checksum-crc32c:AAAAAA==\r\n\r\n");
        Bytes::from(out)
    }

    fn drain(mut body: AwsChunkedBody) -> Result<Bytes, S3Error> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = BytesMut::new();
        loop {
            match Pin::new(&mut body).poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(d) = frame.into_data() {
                        out.extend_from_slice(&d);
                    }
                }
                Poll::Ready(Some(Err(e))) => return Err(e),
                Poll::Ready(None) => return Ok(out.freeze()),
                Poll::Pending => panic!("static inner body never pends"),
            }
        }
    }

    fn signed_body(data: &[u8], chunk_len: usize, declared: u64) -> AwsChunkedBody {
        let inner = Body::from(encode_signed(data, chunk_len));
        AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            declared,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        )
    }

    fn empty_sink() -> TrailerSink {
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()))
    }

    fn dummy_metrics() -> IntCounterVec {
        let registry = antfs_metric::Registry::new();
        antfs_metric::counter_vec(
            &registry,
            "test_s3_chunked_rejected",
            "test only",
            &["reason"],
        )
        .expect("metric registration on a private registry cannot fail")
    }

    fn headers_with(literal: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(l) = literal {
            h.insert("x-amz-content-sha256", l.parse().unwrap());
        }
        h
    }

    #[test]
    fn classify_passes_signed_declaration() {
        let mut h = headers_with(Some(STREAMING_SIGNED));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        h.insert("x-amz-decoded-content-length", "100".parse().unwrap());
        let shape = classify("PUT", &h, false).unwrap().expect("shape");
        assert_eq!(shape.flavor, ChunkFlavor::Signed);
        assert_eq!(shape.declared, 100);
        let info = shape.into_info(SECRET, &info_fixture(), TS);
        assert!(info.sig.is_some());
    }

    #[test]
    fn classify_passes_unsigned_trailer_declaration() {
        let mut h = headers_with(Some(STREAMING_UNSIGNED_TRAILER));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        h.insert("x-amz-decoded-content-length", "7".parse().unwrap());
        let shape = classify("PUT", &h, false).unwrap().expect("shape");
        assert_eq!(shape.flavor, ChunkFlavor::UnsignedTrailer);
        assert!(shape.into_info(SECRET, &info_fixture(), TS).sig.is_none());
    }

    #[test]
    fn classify_without_any_signal_bypasses() {
        let h = headers_with(None);
        assert!(classify("PUT", &h, false).unwrap().is_none());
    }

    #[test]
    fn classify_rejects_torn_and_unsupported_declarations() {
        let mut h = headers_with(None);
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        assert!(classify("PUT", &h, false).is_err());

        let h = headers_with(Some(STREAMING_SIGNED));
        assert!(classify("PUT", &h, false).is_err());

        let mut h = headers_with(Some(STREAMING_SIGNED));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        assert!(classify("PUT", &h, false).is_err());

        let mut h = headers_with(Some(STREAMING_SIGNED));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        h.insert("x-amz-decoded-content-length", "10".parse().unwrap());
        assert!(classify("GET", &h, false).is_err());
        assert!(classify("PUT", &h, true).is_err());

        let mut h = headers_with(Some("STREAMING-WS4-HMAC-SHA256-PAYLOAD"));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        h.insert("x-amz-decoded-content-length", "10".parse().unwrap());
        assert!(classify("PUT", &h, false).is_err());

        let mut h = headers_with(Some(STREAMING_SIGNED));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        h.insert("x-amz-decoded-content-length", "nan".parse().unwrap());
        assert!(classify("PUT", &h, false).is_err());
    }

    fn info_fixture() -> AuthInfo {
        AuthInfo {
            access_key: "AKIDEXAMPLE".into(),
            date: "20261016".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
            signed_headers: vec!["host".into()],
            signature: SEED.into(),
            timestamp: Some(TS.into()),
            expires: None,
            presigned: false,
        }
    }

    #[test]
    fn decodes_multi_chunk_signed_body() {
        let data = b"hello aws-chunked round trip \xe2\x98\x95 twice!".repeat(3);
        let body = signed_body(&data, 5, data.len() as u64);
        assert_eq!(drain(body).unwrap().to_vec(), data.to_vec());
    }

    #[test]
    fn decodes_single_byte_chunks() {
        let data: &[u8] = b"abc";
        let body = signed_body(data, 1, 3);
        assert_eq!(drain(body).unwrap().to_vec(), b"abc");
    }

    #[test]
    fn decodes_unsigned_trailer_body() {
        let data = b"opaque payload".repeat(10);
        let sink = empty_sink();
        let inner = Body::from(encode_unsigned_trailer(&data, 16));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::UnsignedTrailer,
            data.len() as u64,
            None,
            sink.clone(),
            dummy_metrics(),
        );
        assert_eq!(drain(body).unwrap().to_vec(), data.to_vec());
        assert_eq!(
            sink.lock().expect("sink lock").get("x-amz-checksum-crc32c"),
            Some(&"AAAAAA==".to_string()),
            "trailer 行收集进 sink(写路径校验承接)"
        );
    }

    #[test]
    fn tampered_chunk_signature_is_rejected() {
        let data = b"payload-for-tamper";
        let framed = encode_signed(data, 5).to_vec();
        let last_sig_at = framed
            .windows(64)
            .rposition(|w| w.starts_with(b"chunk-signature="))
            .expect("has sigs");
        let mut framed = framed;
        framed[last_sig_at + 16 + 5] ^= 0x01;
        let inner = Body::from(Bytes::from(framed));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            data.len() as u64,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        assert!(matches!(drain(body), Err(S3Error::SignatureDoesNotMatch)));
    }

    #[test]
    fn chunk_size_over_declared_is_rejected() {
        let data = b"toolarge";
        let inner = Body::from(encode_signed(data, 100));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            1,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        let err = drain(body).unwrap_err();
        assert!(matches!(err, S3Error::InvalidArgument(_)));
    }

    #[test]
    fn truncated_body_is_rejected() {
        let data = b"truncation";
        let mut framed = encode_signed(data, 4).to_vec();
        framed.truncate(framed.len() - 3);
        let inner = Body::from(Bytes::from(framed));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            data.len() as u64,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        assert!(matches!(drain(body), Err(S3Error::IncompleteBody)));
    }

    #[test]
    fn trailing_garbage_after_final_chunk_is_rejected() {
        let data = b"tail-garbage";
        let mut framed = encode_signed(data, 100).to_vec();
        framed.extend_from_slice(b"JUNK");
        let inner = Body::from(Bytes::from(framed));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            data.len() as u64,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        assert!(matches!(drain(body), Err(S3Error::InvalidArgument(_))));
    }

    #[test]
    fn unsigned_flavor_rejects_signature_extension() {
        let data = b"no-sig-allowed";
        let framed = encode_signed(data, 100);
        let inner = Body::from(framed);
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::UnsignedTrailer,
            data.len() as u64,
            None,
            empty_sink(),
            dummy_metrics(),
        );
        let err = drain(body).unwrap_err();
        assert!(matches!(err, S3Error::InvalidArgument(_)));
    }

    #[test]
    fn signed_flavor_requires_signature_extension() {
        let data = b"signed-required";
        let framed = encode_unsigned_trailer(data, 100);
        let inner = Body::from(framed);
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::Signed,
            data.len() as u64,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        let err = drain(body).unwrap_err();
        assert!(matches!(err, S3Error::InvalidArgument(_)));
    }

    #[test]
    fn signed_trailer_flavor_with_empty_trailer_section_is_accepted() {
        let data = b"trailer-flavored";
        let key = key();
        let sig1 = chunk_sig(&key, SEED, data);
        let final_sig = chunk_sig(&key, &sig1, b"");
        let mut framed = format!("{:x};{}{sig1}\r\n", data.len(), CHUNK_SIG_EXT).into_bytes();
        framed.extend_from_slice(data);
        framed.extend_from_slice(b"\r\n");
        let tail = format!("0;{}{final_sig}\r\n\r\n", CHUNK_SIG_EXT);
        framed.extend_from_slice(tail.as_bytes());
        let inner = Body::from(Bytes::from(framed));
        let body = AwsChunkedBody::new(
            inner,
            ChunkFlavor::SignedTrailer,
            data.len() as u64,
            Some(sig_ctx()),
            empty_sink(),
            dummy_metrics(),
        );
        assert_eq!(drain(body).unwrap().to_vec(), data.to_vec());
    }
}
