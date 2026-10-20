//! SDK 层透明数据压缩(`router-blob-compression`)。
//!
//! **形态**:自描述帧 = `8B magic \x89ANTFSZ1` + `algo u8` + `orig_len u64 LE` + 编码流。
//! store 集群(记录/索引/复制/compaction/CRC)全程视帧为不透明字节;读侧按帧头自动解压,
//! 未配置或未过闸的对象就是普通 payload —— 集群可混存不同时期、不同算法写的对象。
//!
//! **算法族**(`RouterConfig::compression_algorithm`):zstd / gzip / lz4 / snappy。
//! 算法号进帧头(新算法走新号,老帧永远读得动 —— 对齐 Ambry `Compression.java`
//! "名字存数据、升级不兼容换名"的设计训)。级别语义按算法族有界(zstd 1..=19、gzip 0..=9、
//! lz4/snappy 定档)。

use bytes::{Bytes, BytesMut};

/// 帧魔数:PNG 风格 8 字节;随机数据头碰概率 ~2^-64,误报面可忽略。
pub const FRAME_MAGIC: [u8; 8] = *b"\x89ANTFSZ1";
/// 帧头总长:magic(8) + algo(1) + orig_len(8)。
pub const FRAME_HEADER: usize = 17;
/// 算法号注册表(新算法永远追加,**不复用旧号**)。
pub const ALGO_ZSTD: u8 = 1;
pub const ALGO_GZIP: u8 = 2;
pub const ALGO_LZ4: u8 = 3;
pub const ALGO_SNAPPY: u8 = 4;
/// 支持的算法名(配置校验与帧解码命名用;持久真值是上面的算法号)。
pub const SUPPORTED_ALGOS: [&str; 4] = ["zstd", "gzip", "lz4", "snappy"];

/// put 三闸判定结果。
pub enum Gate {
    /// 保留压缩态:整帧 + 节省字节数(orig − stored)。
    Kept(Bytes, u64),
    /// 原文直写;`&'static str` = 监控 `outcome` 标签(disabled / too_small / low_ratio)。
    Skipped(&'static str),
}

/// 算法名 → 帧算法号(配置校验层拒未知名)。
pub fn algo_id_by_name(name: &str) -> Option<u8> {
    match name {
        "zstd" => Some(ALGO_ZSTD),
        "gzip" => Some(ALGO_GZIP),
        "lz4" => Some(ALGO_LZ4),
        "snappy" => Some(ALGO_SNAPPY),
        _ => None,
    }
}

fn algo_name_by_id(id: u8) -> Option<&'static str> {
    match id {
        ALGO_ZSTD => Some("zstd"),
        ALGO_GZIP => Some("gzip"),
        ALGO_LZ4 => Some("lz4"),
        ALGO_SNAPPY => Some("snappy"),
        _ => None,
    }
}

/// 整块编码(内存态);级别语义按算法。
fn encode(algo: u8, level: i32, data: &[u8]) -> std::io::Result<Bytes> {
    use std::io::Write as _;
    let mut out = Vec::with_capacity(data.len() / 2 + 64);
    match algo {
        // zstd bulk:整块路径(流式口另有 streaming_decoder),容量已知时最省封包。
        ALGO_ZSTD => {
            // Validate 已限 zstd 1..=19;clamp 双保险避免未过校验的旁路写路径。
            let lv: i32 = level.clamp(1, 19);
            out = zstd::bulk::compress(data, lv)?;
        }
        ALGO_GZIP => {
            flate2::read::GzEncoder::new(
                std::io::Cursor::new(data),
                flate2::Compression::new(level.clamp(0, 9) as u32),
            )
            .read_all_into(&mut out)?;
        }
        // lz4 frame:Write 口 + 必须显式 finish()(Drop 不补帧尾,lz4_flex 的纪律)。
        ALGO_LZ4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(&mut out);
            enc.write_all(data)?;
            enc.finish()
                .map_err(|e| std::io::Error::other(format!("lz4 finish: {e}")))?;
        }
        ALGO_SNAPPY => {
            snap::read::FrameEncoder::new(data).read_all_into(&mut out)?;
        }
        _ => return Err(std::io::Error::other("unknown algo id in encode")),
    }
    Ok(Bytes::from(out))
}

/// 对 `read_to_end` 的直白裹路(`use` 域 std::io::Read 常被遮蔽,集中一处)。
trait ReadAllExt: std::io::Read {
    fn read_all_into(&mut self, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.read_to_end(out)?;
        Ok(())
    }
}
impl<R: std::io::Read + ?Sized> ReadAllExt for R {}

/// put 侧三闸 + 帧封装(enabled / min_size 由调用方短路,本函数专注 ratio 与封帧)。
pub fn maybe_compress(
    enabled: bool,
    algo: u8,
    level: i32,
    min_size: usize,
    min_ratio: f64,
    data: &Bytes,
) -> Gate {
    if !enabled {
        return Gate::Skipped("disabled");
    }
    // min_size=0 视为 1:零长/极小体量不试压(17B 帧头本身已吞噬收益)。
    if data.len() < min_size.max(1) {
        return Gate::Skipped("too_small");
    }
    let compressed = match encode(algo, level, data) {
        Ok(c) => c,
        // 编码器失败:回落原文 —— 压缩是优化,不能让压缩器故障变成写入失败。
        Err(_) => return Gate::Skipped("low_ratio"),
    };
    let stored = FRAME_HEADER + compressed.len();
    if (data.len() as f64) < min_ratio * stored as f64 {
        return Gate::Skipped("low_ratio");
    }
    let mut frame = BytesMut::with_capacity(stored);
    frame.extend_from_slice(&FRAME_MAGIC);
    frame.extend_from_slice(&[algo]);
    frame.extend_from_slice(&(data.len() as u64).to_le_bytes());
    frame.extend_from_slice(&compressed);
    let saved = (data.len() as u64).saturating_sub(stored as u64);
    Gate::Kept(frame.freeze(), saved)
}

/// payload 是否帧形态(长度 + 魔数)。
pub fn is_frame(data: &[u8]) -> bool {
    data.len() >= FRAME_HEADER && data[..8] == FRAME_MAGIC
}

/// bulk 解压(bulk 读口):非帧原样透传;帧须产出长度恰等 `orig_len`(截断/谎报即错)。
pub fn decode_frame(data: &Bytes) -> Result<Bytes, String> {
    if !is_frame(data) {
        return Ok(data.clone());
    }
    let algo = data[8];
    let orig_len = u64::from_le_bytes(data[9..17].try_into().expect("slice len 8"));
    let name = algo_name_by_id(algo)
        .ok_or_else(|| format!("unknown compression algo id {algo} in frame"))?;
    let decoded = decode_body(algo, &data[FRAME_HEADER..], orig_len as usize)
        .map_err(|e| format!("{name} decompress: {e}"))?;
    if decoded.len() as u64 != orig_len {
        return Err(format!(
            "frame declares orig_len {orig_len} but decoded {}",
            decoded.len()
        ));
    }
    Ok(decoded)
}

/// 整块解码(容量提示给无 orig 语义的算法)。
fn decode_body(algo: u8, body: &[u8], cap: usize) -> std::io::Result<Bytes> {
    let mut out = Vec::with_capacity(cap.min(16 << 20));
    match algo {
        ALGO_ZSTD => {
            out = zstd::bulk::decompress(body, cap)?;
        }
        ALGO_GZIP => flate2::read::MultiGzDecoder::new(body).read_all_into(&mut out)?,
        ALGO_LZ4 => lz4_flex::frame::FrameDecoder::new(body).read_all_into(&mut out)?,
        ALGO_SNAPPY => snap::read::FrameDecoder::new(body).read_all_into(&mut out)?,
        _ => return Err(std::io::Error::other("unknown algo id in decode")),
    }
    Ok(Bytes::from(out))
}

/// `(解码 Reader, orig_len, 算法名)`。
pub type StreamingDecode = (Box<dyn std::io::Read + Send>, u64, &'static str);

/// 流式解码口(get_file):窥到帧头后,把**编码流源**(桥自网络 chunk)包成解码 Reader。
/// `source` 已含完整编码流(peek 出的 17B 之后);`orig_len` 供收尾校验;非帧返回 `None`。
pub fn streaming_decoder(
    frame_head: &[u8],
    source: Box<dyn std::io::Read + Send>,
) -> Option<StreamingDecode> {
    if !is_frame(frame_head) {
        return None;
    }
    let algo = frame_head[8];
    let orig_len = u64::from_le_bytes(frame_head[9..17].try_into().expect("slice len 8"));
    let name = algo_name_by_id(algo)?;
    let dec: Box<dyn std::io::Read + Send> = match algo {
        ALGO_ZSTD => Box::new(zstd::stream::read::Decoder::new(source).ok()?),
        ALGO_GZIP => Box::new(flate2::read::MultiGzDecoder::new(source)),
        ALGO_LZ4 => Box::new(lz4_flex::frame::FrameDecoder::new(source)),
        ALGO_SNAPPY => Box::new(snap::read::FrameDecoder::new(source)),
        _ => return None,
    };
    Some((dec, orig_len, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(n: usize) -> Bytes {
        // 高重复文本段:任何算法都显著可压。
        let pattern = b"antfs-compressible-pattern-with-redundancy-";
        Bytes::from(
            (0..n)
                .map(|i| pattern[i % pattern.len()])
                .collect::<Vec<_>>(),
        )
    }

    fn entropy(n: usize) -> Bytes {
        // xorshift 组:不碰帧魔数前缀即可(熵流前 8B 碰 magic 概率 ~2^-64)。
        let mut x = 0x9E3779B97F4A7C15u64;
        Bytes::from(
            (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn gate_matrix_all_algos() {
        for name in SUPPORTED_ALGOS {
            let algo = algo_id_by_name(name).expect("known algo");
            // 可压体:kept + roundtrip。
            let data = text(64 * 1024);
            match maybe_compress(true, algo, 1, 4096, 1.05, &data) {
                Gate::Kept(frame, saved) => {
                    assert!(saved > 0, "{name} 应有节省");
                    assert!(frame.len() < data.len());
                    let nm: &str = name;
                    let dec = decode_frame(&frame).unwrap_or_else(|e| {
                        panic!("{nm} roundtrip err: {e}");
                    });
                    assert_eq!(dec, data, "{nm}");
                }
                Gate::Skipped(o) => panic!("{name} 可压体必须过闸,skipped={o}"),
            }
            // 小体量:too_small。
            let small = text(64);
            assert!(matches!(
                maybe_compress(true, algo, 1, 4096, 1.05, &small),
                Gate::Skipped("too_small")
            ));
            // 熵数据:low_ratio。
            let noise = entropy(256 * 1024);
            assert!(
                matches!(
                    maybe_compress(true, algo, 1, 4096, 1.05, &noise),
                    Gate::Skipped("low_ratio")
                ),
                "{name} 熵数据应回落 low_ratio"
            );
            // 关:disabled。
            assert!(matches!(
                maybe_compress(false, algo, 1, 4096, 1.05, &data),
                Gate::Skipped("disabled")
            ));
            // 零长:too_small(帧头即亏)。
            assert!(matches!(
                maybe_compress(true, algo, 1, 0, 1.05, &Bytes::new()),
                Gate::Skipped("too_small")
            ));
        }
    }

    #[test]
    fn nonframe_passthrough_and_frame_checks() {
        // 非帧:原样透传。
        let raw = Bytes::from_static(b"plain bytes not a frame");
        assert_eq!(decode_frame(&raw).expect("passthrough"), raw);
        assert!(!is_frame(&raw));
        // 帧声明 orig_len 谎报 → 错误(截短保护)。
        let data = text(8192);
        match maybe_compress(true, ALGO_ZSTD, 1, 4096, 1.05, &data) {
            Gate::Kept(frame, _) => {
                // 篡改 orig_len(+1)。
                let mut bad_len = frame.to_vec();
                let n = (data.len() as u64 + 1).to_le_bytes();
                bad_len[9..17].copy_from_slice(&n);
                assert!(
                    decode_frame(&Bytes::from(bad_len)).is_err(),
                    "orig_len 谎报须响错"
                );
                // 篡改 algo 号(未知)→ 响错。
                let mut bad_algo = frame.to_vec();
                bad_algo[8] = 0x7F;
                assert!(
                    decode_frame(&Bytes::from(bad_algo)).is_err(),
                    "未知算法号须响错"
                );
            }
            _ => panic!("zstd 可压体必须过闸"),
        }
    }

    #[test]
    fn mixed_algo_frames_stay_readable() {
        // 混存语义:四种算法各自写帧,统一 decode 读回(集群演进不破坏可读性)。
        let data = text(32 * 1024);
        for name in SUPPORTED_ALGOS {
            let algo = algo_id_by_name(name).expect("known");
            match maybe_compress(true, algo, 1, 4096, 1.01, &data) {
                Gate::Kept(frame, _) => {
                    let nm: &str = name;
                    let dec = decode_frame(&frame).unwrap_or_else(|e| {
                        panic!("{nm} roundtrip err: {e}");
                    });
                    assert_eq!(dec, data, "{nm}");
                }
                Gate::Skipped(o) => panic!("{name} 宽比率应过闸,得 {o}"),
            }
        }
    }
}

#[cfg(test)]
mod probe {
    use super::*;
    #[test]
    fn probe_kept_zstd() {
        let pattern = b"antfs-compressible-pattern-with-redundancy-";
        let data = Bytes::from(
            (0..65536)
                .map(|i| pattern[i % pattern.len()])
                .collect::<Vec<_>>(),
        );
        let g = maybe_compress(true, ALGO_ZSTD, 1, 4096, 1.05, &data);
        match g {
            Gate::Kept(frame, saved) => {
                println!("frame len={} saved={}", frame.len(), saved);
                println!("head={:02X?}", &frame[..17.min(frame.len())]);
                let dec = decode_frame(&frame).expect("roundtrip");
                println!("decoded len={}", dec.len());
                assert_eq!(dec, data);
            }
            Gate::Skipped(o) => panic!("skipped {}", o),
        }
    }
}
