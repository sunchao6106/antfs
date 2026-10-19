//! 拼装(stitch)描述符的**字节格式单点**(descriptor-drop-magic 后的形态):编码、
//! 常量与严格解析。
//!
//! 描述符本身是普通 blob 的数据体,由 frontend 读写。类型判定(谁是描述符)**只由
//! 存储层的 `FLAG_COMPOSED` 承载**;本 crate 的 parse 输入域因此恒为"由 encode 写出、
//! 经组合写口落位"的位字节 —— 不存在对无位/不可信数据判型的调用方。格式即契约。
//!
//! 刻意零依赖(不引 StoreKey / 引擎任何类型):`id` 以原始 `[u8; 8]` 载体进出,
//! 调用方在边界转换一次。
//!
//! 格式(小端):
//! ```text
//! version(1) | chunk_count(u32) | session_len(u16) | session |
//! × chunk_count: blobId(8) | size(u64) | expiration_ms(i64)
//! ```

/// 描述符格式版本;读到不认识的版本即判结构损坏(不递推兼容 —— 格式演进走新版本号)。
pub const VERSION: u8 = 1;
/// 单个拼装对象的片数上限(`chunk_count` 合法域为 `1..=MAX_CHUNKS`)。
pub const MAX_CHUNKS: usize = 4096;
/// 描述符字节上限(1 MiB):读路径据此决定“整体读出判定”还是“直接流式”;
/// 描述符按构造恒在其内(两域上限相加 ≈ 163 KiB,这是“永远到不了”的防御性兜底)。
pub const MAX_BYTES: u64 = 1024 * 1024;
/// 固定头长:`version(1) + chunk_count(4) + session_len(2)`。
pub const HEADER_BYTES: usize = 7;
/// 每片条目长:`blobId(8) + size(8) + expiration_ms(8)`。
pub const ENTRY_BYTES: usize = 24;

/// 描述符里的一片(小端落盘的东西,与业务对象无关)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    /// 片的 blobId(原始 8 字节)。
    pub id: [u8; 8],
    /// 拼装提交时记录的片数据长度(读时核对,不一致即上游损坏)。
    pub size: u64,
    /// 拼装提交时记录的过期时刻(epoch 毫秒;`-1` = 不过期)。
    pub expiration_ms: i64,
}

/// 格式层错误:调用方各自映射到自己的错误口径(frontend → 502 上游损坏;
/// 回填 → 跳过该候选)。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DescriptorError {
    #[error("unsupported descriptor version {0}")]
    UnsupportedVersion(u8),
    #[error("chunk count must be in 1..={MAX_CHUNKS}")]
    ChunkCountOutOfBounds,
    #[error("session must be at most 65535 bytes")]
    SessionTooLong,
    #[error("descriptor exceeds {MAX_BYTES} bytes")]
    TooLarge,
    #[error("corrupt descriptor: {0}")]
    Corrupt(&'static str),
}

/// 编码完整描述符。
pub fn encode(session: &[u8], chunks: &[ChunkRef]) -> Result<Vec<u8>, DescriptorError> {
    if chunks.is_empty() || chunks.len() > MAX_CHUNKS {
        return Err(DescriptorError::ChunkCountOutOfBounds);
    }
    let session_len = u16::try_from(session.len()).map_err(|_| DescriptorError::SessionTooLong)?;
    let mut out = Vec::with_capacity(HEADER_BYTES + session.len() + chunks.len() * ENTRY_BYTES);
    out.push(VERSION);
    out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    out.extend_from_slice(&session_len.to_le_bytes());
    out.extend_from_slice(session);
    for c in chunks {
        out.extend_from_slice(&c.id);
        out.extend_from_slice(&c.size.to_le_bytes());
        out.extend_from_slice(&c.expiration_ms.to_le_bytes());
    }
    if out.len() as u64 > MAX_BYTES {
        return Err(DescriptorError::TooLarge);
    }
    Ok(out)
}

/// 严格解析:任何结构性错误(含**尾随字节**)都报错,成功即 `[ChunkRef]`。
/// session 内容只在客户端侧有意义,解析时校验长度自洽后跳过。
pub fn parse(buf: &[u8]) -> Result<Vec<ChunkRef>, DescriptorError> {
    let corrupt = |msg: &'static str| DescriptorError::Corrupt(msg);
    let mut cur = Cursor { buf, pos: 0 };
    let version = cur.u8().ok_or_else(|| corrupt("truncated version"))?;
    if version != VERSION {
        return Err(DescriptorError::UnsupportedVersion(version));
    }
    let chunk_count = u32::from_le_bytes(
        cur.array::<4>()
            .ok_or_else(|| corrupt("truncated chunk_count"))?,
    );
    let chunk_count = usize::try_from(chunk_count).map_err(|_| corrupt("chunk count overflow"))?;
    if chunk_count == 0 || chunk_count > MAX_CHUNKS {
        return Err(DescriptorError::ChunkCountOutOfBounds);
    }
    let session_len = usize::from(u16::from_le_bytes(
        cur.array::<2>()
            .ok_or_else(|| corrupt("truncated session_len"))?,
    ));
    cur.take(session_len)
        .ok_or_else(|| corrupt("truncated session"))?;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        let id: [u8; 8] = cur
            .take(8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| corrupt("truncated chunk id"))?;
        let size = u64::from_le_bytes(
            cur.array::<8>()
                .ok_or_else(|| corrupt("truncated chunk size"))?,
        );
        let expiration_ms = i64::from_le_bytes(
            cur.array::<8>()
                .ok_or_else(|| corrupt("truncated chunk expiration"))?,
        );
        chunks.push(ChunkRef {
            id,
            size,
            expiration_ms,
        });
    }
    if cur.pos != buf.len() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(chunks)
}

/// 描述符的小端游标:越界即 `None`,不做任何索引/切片 panic。
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.pos.checked_add(n)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.array::<1>()?[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: u64, size: u64, exp: i64) -> ChunkRef {
        ChunkRef {
            id: id.to_le_bytes(),
            size,
            expiration_ms: exp,
        }
    }

    #[test]
    fn roundtrip_with_session() {
        let chunks = vec![
            chunk(0x1122_3344_5566_7788, 4096, 1_700_000_000_000),
            chunk(9, 1, -1),
        ];
        let buf = encode("session-甲".as_bytes(), &chunks).expect("encode");
        assert_eq!(
            buf.len(),
            HEADER_BYTES + "session-甲".len() + 2 * ENTRY_BYTES
        );
        let back = parse(&buf).expect("parse");
        assert_eq!(back, chunks);
    }

    #[test]
    fn zero_and_over_max_chunks_rejected() {
        assert_eq!(
            encode(b"", &[]).unwrap_err(),
            DescriptorError::ChunkCountOutOfBounds
        );
        let many: Vec<ChunkRef> = (0..=MAX_CHUNKS).map(|i| chunk(i as u64, 1, -1)).collect();
        assert_eq!(
            encode(b"", &many).unwrap_err(),
            DescriptorError::ChunkCountOutOfBounds
        );
    }

    #[test]
    fn size_bound_holds_at_domain_maxima() {
        // MAX_BYTES 是防御性兜底:两域上限(65 535 B session + 4096×24 B 条目)相加
        // ≈ 163 KiB,恒在 1 MiB 内 —— 这是 GET 靠 size 判断"可整体读出"成立的前提,
        // 也是"合法输入到不了 TooLarge"的不变量,钉死之。
        let session = vec![b'x'; u16::MAX as usize];
        let many: Vec<ChunkRef> = (0..MAX_CHUNKS).map(|i| chunk(i as u64, 1, -1)).collect();
        let buf = encode(&session, &many).expect("极限合法输入必须可编码");
        assert!((buf.len() as u64) < MAX_BYTES);
    }

    /// 历史前缀时代的字节(个人旧盘如遇):首字节 0x41(65)→ 版本位响亮报错,
    /// 绝不静默误拼(descriptor-drop-magic 的 Owner Ruling 承载)。
    #[test]
    fn legacy_prefixed_bytes_fail_loudly_at_the_version_byte() {
        let good = encode(b"s", &[chunk(1, 8, -1)]).unwrap();
        // 带旧 4 字节前缀的陈旧行(字节面:41 53 54 43):
        let mut legacy = [0x41u8, 0x53, 0x54, 0x43].to_vec();
        legacy.extend_from_slice(&good);
        assert_eq!(
            parse(&legacy),
            Err(DescriptorError::UnsupportedVersion(0x41))
        );
    }

    #[test]
    fn bad_version_rejected() {
        let mut v2 = encode(b"", &[chunk(1, 8, -1)]).unwrap();
        let bad_version = VERSION + 7;
        v2[0] = bad_version;
        assert_eq!(
            parse(&v2),
            Err(DescriptorError::UnsupportedVersion(bad_version))
        );
    }

    #[test]
    fn structural_corruption_rejected() {
        let good = encode(b"sess", &[chunk(1, 8, -1)]).unwrap();
        // 截断:每一步都是 Corrupt(不 panic;连首字节都缺时是 "truncated version"):
        for cut in 0..good.len() {
            let r = parse(&good[..cut]);
            assert!(
                matches!(&r, Err(DescriptorError::Corrupt(_))),
                "cut {cut}: {r:?}"
            );
        }
        // 尾随字节:片数/长度全都自洽,但尾部多一个字节 → 拒(严格性)。
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(
            parse(&trailing),
            Err(DescriptorError::Corrupt("trailing bytes"))
        );
        // 坏的 chunk_count(超出 MAX_CHUNKS)→ 计数越界优先于长度发现。
        let mut bad_count = encode(b"", &[chunk(1, 1, -1)]).unwrap();
        bad_count[1..5].copy_from_slice(&(MAX_CHUNKS as u32 + 1).to_le_bytes());
        assert_eq!(
            parse(&bad_count),
            Err(DescriptorError::ChunkCountOutOfBounds)
        );
    }
}
