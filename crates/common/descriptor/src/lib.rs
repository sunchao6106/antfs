//! 拼装(stitch)描述符的**字节格式单点**(store-composed-backfill):编码、常量与严格解析。
//!
//! 描述符本身是普通 blob 的数据体;它的字节格式被两层共同依赖 —— frontend 读写它,
//! store 的存量回填以**同一份 parse** 甄别真描述符。格式即契约(CLAUDE.md §3.4):
//! 本 crate 是唯一真值,两处不得再各自手搓,否则甄别与读写漂移。
//!
//! 刻意零依赖(不引 StoreKey / 引擎任何类型):`id` 以原始 `[u8; 8]` 载体进出,
//! 调用方在边界转换一次。
//!
//! 格式(小端):
//! ```text
//! MAGIC(4) "ASTC" | version(1) | chunk_count(u32) | session_len(u16) | session |
//! × chunk_count: blobId(8) | size(u64) | expiration_ms(i64)
//! ```

/// 描述符 MAGIC(4 字节):读路径见到此前缀即把数据体当描述符解析。
pub const MAGIC: &[u8; 4] = b"ASTC";
/// 描述符格式版本;读到不认识的版本即判结构损坏(不递推兼容 —— 格式演进走新版本号)。
pub const VERSION: u8 = 1;
/// 单个拼装对象的片数上限(`chunk_count` 合法域为 `1..=MAX_CHUNKS`)。
pub const MAX_CHUNKS: usize = 4096;
/// 描述符字节上限(1 MiB):读路径据此决定“整体读出判定”还是“直接流式”;
/// 回填只甄别不超过此尺寸的候选(真描述符按构造恒在其内)。
pub const MAX_BYTES: u64 = 1024 * 1024;
/// 固定头长:`MAGIC(4) + version(1) + chunk_count(4) + session_len(2)`。
pub const HEADER_BYTES: usize = 11;
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
    #[error("bad magic")]
    BadMagic,
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
    out.extend_from_slice(MAGIC);
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
    if !buf.starts_with(MAGIC) {
        return Err(DescriptorError::BadMagic);
    }
    let corrupt = |msg: &'static str| DescriptorError::Corrupt(msg);
    let mut cur = Cursor {
        buf,
        pos: MAGIC.len(),
    };
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
        assert!(buf.starts_with(MAGIC));
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

    #[test]
    fn bad_magic_and_version_rejected() {
        let mut buf = encode(b"", &[chunk(1, 8, -1)]).unwrap();
        assert_eq!(parse(&buf[..3]), Err(DescriptorError::BadMagic));
        buf[0] = b'X';
        assert_eq!(parse(&buf), Err(DescriptorError::BadMagic));
        let mut v2 = encode(b"", &[chunk(1, 8, -1)]).unwrap();
        v2[MAGIC.len()] = 2;
        assert!(matches!(
            parse(&v2),
            Err(DescriptorError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn structural_corruption_rejected() {
        let good = encode(b"sess", &[chunk(1, 8, -1)]).unwrap();
        // 截断:头 4 字节内截断 = BadMagic;之后 = Corrupt(不 panic):
        for cut in 1..good.len() {
            let r = parse(&good[..cut]);
            assert!(
                match &r {
                    Err(e) if cut < MAGIC.len() => *e == DescriptorError::BadMagic,
                    Err(DescriptorError::Corrupt(_)) => true,
                    _ => false,
                },
                "cut {cut}: {r:?}"
            );
        }
        // 尾随字节:片数/长度全都自洽,但尾部多一个字节 → 拒(严格性 = 甄别伪件的底)。
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(
            parse(&trailing),
            Err(DescriptorError::Corrupt("trailing bytes"))
        );
        // 坏的 chunk_count(超出 MAX_CHUNKS 但长度不够)→ 计数越界优先。
        let mut bad_count = encode(b"", &[chunk(1, 1, -1)]).unwrap();
        bad_count[5..9].copy_from_slice(&(MAX_CHUNKS as u32 + 1).to_le_bytes());
        assert_eq!(
            parse(&bad_count),
            Err(DescriptorError::ChunkCountOutOfBounds)
        );
    }
}
