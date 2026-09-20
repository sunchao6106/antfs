//! `LogSegment`:预分配、只追加的段文件,用 monoio + io_uring + O_DIRECT 落盘。
//!
//! 物理布局:
//! ```text
//! [ 第 0 块(ALIGN):Header ] [ 数据块 0 ] [ 数据块 1 ] ... [ 数据块 N ]
//! ```
//! - Header 独占首个 `ALIGN` 块,记录 magic/version/capacity/data_end/crc。
//! - 逻辑数据 offset 0 映射到物理 offset `HEADER_SIZE`。
//!
//! O_DIRECT 要求缓冲地址、文件偏移、长度都按 `ALIGN` 对齐。因此追加时先把
//! 变长记录拷进对齐的暂存块(`tail`),满一块就整块写盘;`flush()` 时把不满
//! 的尾块补零后整块写盘并 fsync。读取时对齐读盘,未落盘的尾部直接从内存 `tail` 取。

use std::path::Path;

use crate::aligned::{align_down, align_up, AlignedBuf, ALIGN};
use crate::error::{Error, Result};

const HEADER_MAGIC: u32 = 0x414D_4253; // "AMBS"
const HEADER_VERSION: u16 = 1;
const HEADER_SIZE: usize = ALIGN;

/// header 内各字段偏移(小端)。
mod hdr {
    pub const MAGIC: usize = 0; // u32
    pub const VERSION: usize = 4; // u16
                                  // 6..8 reserved
    pub const CAPACITY: usize = 8; // u64
    pub const DATA_END: usize = 16; // u64
    pub const CREATED_NS: usize = 24; // u64
    pub const CRC: usize = 32; // u32,覆盖 [0, CRC)
    pub const COVERED: usize = CRC;
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 一个只追加的日志段。非并发安全:同一时刻只应有一个写者(`&mut self`)。
pub struct LogSegment {
    file: monoio::fs::File,
    /// 物理总容量(含 header),`ALIGN` 整数倍。
    capacity: u64,
    /// 可用数据容量 = capacity - HEADER_SIZE。
    data_capacity: u64,
    /// 已完整落盘的数据块数。
    flushed_blocks: u64,
    /// 当前(可能不满的)尾块暂存,长度恒为 `ALIGN`。
    tail: AlignedBuf,
    /// 尾块中有效字节数,范围 `[0, ALIGN)`。
    tail_len: usize,
    created_ns: u64,
}

impl std::fmt::Debug for LogSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSegment")
            .field("capacity", &self.capacity)
            .field("data_end", &self.data_end())
            .field("flushed_blocks", &self.flushed_blocks)
            .field("tail_len", &self.tail_len)
            .finish()
    }
}

impl LogSegment {
    /// 创建新段:std 预分配(posix_fallocate)→ monoio O_DIRECT 打开 → 写 header。
    pub async fn create(path: impl AsRef<Path>, capacity: u64) -> Result<Self> {
        if capacity <= HEADER_SIZE as u64 {
            return Err(Error::BadArgument(format!(
                "capacity {capacity} must exceed header size {HEADER_SIZE}"
            )));
        }
        let capacity = align_up(capacity, ALIGN as u64);
        let path = path.as_ref();

        // 用 std 真实预分配(fallocate),避免碎片(monoio 无 ftruncate API)。
        {
            use std::os::unix::io::AsRawFd;
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            // SAFETY: fd 有效,size > 0。
            let ret = unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, capacity as libc::off_t) };
            if ret != 0 {
                return Err(Error::Io(std::io::Error::from_raw_os_error(ret)));
            }
        }

        let file = open_direct(path).await?;
        let mut seg = Self {
            file,
            capacity,
            data_capacity: capacity - HEADER_SIZE as u64,
            flushed_blocks: 0,
            tail: AlignedBuf::with_capacity(ALIGN),
            tail_len: 0,
            created_ns: now_ns(),
        };
        seg.write_header().await?;
        seg.file.sync_all().await?;
        Ok(seg)
    }

    /// 打开已有段并恢复到上次 `flush` 的状态(data_end 由 header 记录)。
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = open_direct(path.as_ref()).await?;

        let hb = AlignedBuf::with_capacity(HEADER_SIZE);
        let (res, hb) = file.read_exact_at(hb, 0).await;
        res?;
        let s = hb.as_cap_slice();

        let magic = u32::from_le_bytes(s[hdr::MAGIC..hdr::MAGIC + 4].try_into().unwrap());
        if magic != HEADER_MAGIC {
            return Err(Error::Corrupt(format!("bad magic {magic:#x}")));
        }
        let stored_crc = u32::from_le_bytes(s[hdr::CRC..hdr::CRC + 4].try_into().unwrap());
        let crc = crc32fast::hash(&s[0..hdr::COVERED]);
        if crc != stored_crc {
            return Err(Error::Corrupt(format!(
                "header crc mismatch: stored={stored_crc:#x} computed={crc:#x}"
            )));
        }

        let capacity = u64::from_le_bytes(s[hdr::CAPACITY..hdr::CAPACITY + 8].try_into().unwrap());
        let data_end = u64::from_le_bytes(s[hdr::DATA_END..hdr::DATA_END + 8].try_into().unwrap());
        let created_ns =
            u64::from_le_bytes(s[hdr::CREATED_NS..hdr::CREATED_NS + 8].try_into().unwrap());

        let flushed_blocks = data_end / ALIGN as u64;
        let tail_len = (data_end % ALIGN as u64) as usize;

        // 恢复尾块内容(若上次 flush 时尾块不满)。
        let mut tail = AlignedBuf::with_capacity(ALIGN);
        if tail_len > 0 {
            let phys = HEADER_SIZE as u64 + flushed_blocks * ALIGN as u64;
            let rb = AlignedBuf::with_capacity(ALIGN);
            let (res, rb) = file.read_exact_at(rb, phys).await;
            res?;
            tail.as_cap_mut().copy_from_slice(rb.as_cap_slice());
        }

        Ok(Self {
            file,
            capacity,
            data_capacity: capacity - HEADER_SIZE as u64,
            flushed_blocks,
            tail,
            tail_len,
            created_ns,
        })
    }

    /// 当前逻辑数据末尾(字节粒度)。
    #[inline]
    pub fn data_end(&self) -> u64 {
        self.flushed_blocks * ALIGN as u64 + self.tail_len as u64
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// 可用数据容量(= 物理容量 - header)。
    #[inline]
    pub fn data_capacity(&self) -> u64 {
        self.data_capacity
    }

    #[inline]
    pub fn remaining(&self) -> u64 {
        self.data_capacity - self.data_end()
    }

    #[inline]
    pub fn created_ns(&self) -> u64 {
        self.created_ns
    }

    /// 追加一条记录,返回它在数据区的逻辑起始 offset。
    /// 记录先入对齐暂存块,满块即写盘。**未 fsync**,持久化需调用 [`flush`](Self::flush)。
    pub async fn append(&mut self, record: &[u8]) -> Result<u64> {
        let start = self.data_end();
        if start + record.len() as u64 > self.data_capacity {
            return Err(Error::SegmentFull);
        }
        let mut src = record;
        while !src.is_empty() {
            let space = ALIGN - self.tail_len;
            let n = space.min(src.len());
            self.tail.as_cap_mut()[self.tail_len..self.tail_len + n].copy_from_slice(&src[..n]);
            self.tail_len += n;
            src = &src[n..];
            if self.tail_len == ALIGN {
                let idx = self.flushed_blocks;
                self.persist_tail_block(idx).await?;
                self.flushed_blocks += 1;
                self.tail_len = 0;
                self.tail.as_cap_mut().fill(0);
            }
        }
        Ok(start)
    }

    /// 持久化:把不满的尾块补零后整块写盘,更新 header,fsync。
    pub async fn flush(&mut self) -> Result<()> {
        if self.tail_len > 0 {
            let idx = self.flushed_blocks;
            self.persist_tail_block(idx).await?;
            // 不递增 flushed_blocks:尾块可能继续被后续 append 填充/覆盖写。
        }
        self.write_header().await?;
        self.file.sync_all().await?;
        Ok(())
    }

    /// 读取 `[offset, offset+len)` 的数据。已落盘部分对齐读盘,未落盘尾部读内存。
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let end = offset + len as u64;
        if end > self.data_end() {
            return Err(Error::OutOfRange {
                offset,
                len,
                data_end: self.data_end(),
            });
        }
        let mut out = vec![0u8; len];
        let flushed = self.flushed_blocks * ALIGN as u64;

        // 磁盘部分:[offset, min(end, flushed))
        if offset < flushed {
            let disk_end = end.min(flushed);
            let phys_start = HEADER_SIZE as u64 + offset;
            let phys_end = HEADER_SIZE as u64 + disk_end;
            let a_start = align_down(phys_start, ALIGN as u64);
            let a_end = align_up(phys_end, ALIGN as u64);
            let buf = AlignedBuf::with_capacity((a_end - a_start) as usize);
            let (res, buf) = self.file.read_exact_at(buf, a_start).await;
            res?;
            let inner = (phys_start - a_start) as usize;
            let n = (disk_end - offset) as usize;
            out[..n].copy_from_slice(&buf.as_cap_slice()[inner..inner + n]);
        }

        // 内存尾部:[max(offset, flushed), end)
        if end > flushed {
            let t_start = offset.max(flushed);
            let src_off = (t_start - flushed) as usize;
            let n = (end - t_start) as usize;
            let dst_off = (t_start - offset) as usize;
            out[dst_off..dst_off + n].copy_from_slice(&self.tail.as_cap_slice()[src_off..src_off + n]);
        }

        Ok(out)
    }

    /// 关闭底层文件(flush 由调用方负责)。
    pub async fn close(self) -> Result<()> {
        self.file.close().await?;
        Ok(())
    }

    /// 把当前 `tail` 作为一个完整块写到 `block_index` 处(补零到 ALIGN)。
    /// 不改变 `flushed_blocks`/`tail_len`(由调用方决定语义)。
    async fn persist_tail_block(&mut self, block_index: u64) -> Result<()> {
        if self.tail_len < ALIGN {
            self.tail.as_cap_mut()[self.tail_len..ALIGN].fill(0);
        }
        self.tail.set_len(ALIGN);
        let phys = HEADER_SIZE as u64 + block_index * ALIGN as u64;
        // monoio 完成式 IO 需拥有缓冲:临时换出 tail,写完换回。
        let buf = std::mem::replace(&mut self.tail, AlignedBuf::with_capacity(ALIGN));
        let (res, buf) = self.file.write_all_at(buf, phys).await;
        self.tail = buf;
        res?;
        Ok(())
    }

    async fn write_header(&mut self) -> Result<()> {
        let mut hb = AlignedBuf::with_capacity(HEADER_SIZE);
        {
            let s = hb.as_cap_mut();
            s[hdr::MAGIC..hdr::MAGIC + 4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
            s[hdr::VERSION..hdr::VERSION + 2].copy_from_slice(&HEADER_VERSION.to_le_bytes());
            s[hdr::CAPACITY..hdr::CAPACITY + 8].copy_from_slice(&self.capacity.to_le_bytes());
            s[hdr::DATA_END..hdr::DATA_END + 8].copy_from_slice(&self.data_end().to_le_bytes());
            s[hdr::CREATED_NS..hdr::CREATED_NS + 8].copy_from_slice(&self.created_ns.to_le_bytes());
            let crc = crc32fast::hash(&s[0..hdr::COVERED]);
            s[hdr::CRC..hdr::CRC + 4].copy_from_slice(&crc.to_le_bytes());
        }
        hb.set_len(HEADER_SIZE);
        let (res, _hb) = self.file.write_all_at(hb, 0).await;
        res?;
        Ok(())
    }
}

async fn open_direct(path: &Path) -> Result<monoio::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = monoio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .await?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let ns = now_ns();
        std::path::PathBuf::from(format!("/var/tmp/ambry_ls_{tag}_{ns}.seg"))
    }

    fn cap(blocks: u64) -> u64 {
        HEADER_SIZE as u64 + blocks * ALIGN as u64
    }

    #[monoio::test]
    async fn roundtrip_before_and_after_flush() {
        let path = temp_path("rt");
        let mut seg = LogSegment::create(&path, cap(64)).await.unwrap();

        let a = b"hello".to_vec();
        let b = vec![7u8; 100];
        let c = b"the-third-record".to_vec();

        let oa = seg.append(&a).await.unwrap();
        let ob = seg.append(&b).await.unwrap();
        let oc = seg.append(&c).await.unwrap();
        assert_eq!(oa, 0);
        assert_eq!(ob, a.len() as u64);
        assert_eq!(oc, (a.len() + b.len()) as u64);

        // flush 前:从内存 tail 读
        assert_eq!(seg.read(oa, a.len()).await.unwrap(), a);
        assert_eq!(seg.read(ob, b.len()).await.unwrap(), b);
        assert_eq!(seg.read(oc, c.len()).await.unwrap(), c);

        // flush 后:再读一致
        seg.flush().await.unwrap();
        assert_eq!(seg.read(oa, a.len()).await.unwrap(), a);
        assert_eq!(seg.read(ob, b.len()).await.unwrap(), b);
        assert_eq!(seg.read(oc, c.len()).await.unwrap(), c);

        seg.close().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[monoio::test]
    async fn multi_block_record_spans_boundaries() {
        let path = temp_path("multi");
        let mut seg = LogSegment::create(&path, cap(64)).await.unwrap();

        // 记录跨多个 ALIGN 块
        let big: Vec<u8> = (0..(ALIGN * 3 + 123)).map(|i| (i % 251) as u8).collect();
        let off = seg.append(&big).await.unwrap();
        assert_eq!(off, 0);
        seg.flush().await.unwrap();

        // 整体读回
        assert_eq!(seg.read(off, big.len()).await.unwrap(), big);
        // 局部读(跨块中间)
        assert_eq!(
            seg.read(off + 4000, 500).await.unwrap(),
            big[4000..4500].to_vec()
        );

        seg.close().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[monoio::test]
    async fn segment_full_is_reported() {
        let path = temp_path("full");
        let mut seg = LogSegment::create(&path, cap(2)).await.unwrap(); // 数据区仅 2 块
        let chunk = vec![1u8; ALIGN];
        seg.append(&chunk).await.unwrap();
        seg.append(&chunk).await.unwrap();
        // 第三块超出容量
        match seg.append(&chunk).await {
            Err(Error::SegmentFull) => {}
            other => panic!("expected SegmentFull, got {other:?}"),
        }
        seg.close().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[monoio::test]
    async fn reopen_recovers_state_and_data() {
        let path = temp_path("reopen");
        let a = vec![3u8; 5000]; // 跨块 + 尾部不满
        let b = b"tail-record".to_vec();
        let (oa, ob, data_end);
        {
            let mut seg = LogSegment::create(&path, cap(64)).await.unwrap();
            oa = seg.append(&a).await.unwrap();
            ob = seg.append(&b).await.unwrap();
            seg.flush().await.unwrap();
            data_end = seg.data_end();
            seg.close().await.unwrap();
        }
        // 重新打开
        let seg = LogSegment::open(&path).await.unwrap();
        assert_eq!(seg.data_end(), data_end);
        assert_eq!(seg.read(oa, a.len()).await.unwrap(), a);
        assert_eq!(seg.read(ob, b.len()).await.unwrap(), b);

        seg.close().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
