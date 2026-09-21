//! `LogSegment`:预分配、只追加的段文件,同步阻塞 IO(buffered)。
//!
//! 物理布局(无需 O_DIRECT 对齐):
//! ```text
//! [ 第 0 块:Header ] [ 数据区(变长记录顺序打包) ]
//! ```
//! - Header 记录 magic/version/capacity/data_end/crc;数据 offset 0 映射到物理 `HEADER_SIZE`。
//! - 追加走 buffered `write_at`(进 OS page cache);`flush()` 才更新 header.data_end 并 `sync_all`,故
//!   **持久化边界 = flush**。断电时磁盘 header 恒为「上一次 flush 的状态」,恢复严格截断到该边界。

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

const HEADER_MAGIC: u32 = 0x414D_4253; // "AMBS"
const HEADER_VERSION: u16 = 1;
const HEADER_SIZE: usize = 64;

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
    pub const HEADER_LEN: usize = 36;
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 预分配段文件到 `capacity` 字节:Linux `posix_fallocate` 真实占盘、macOS `F_PREALLOCATE`,
/// 失败或其他平台回退 `set_len`(仅设逻辑长度)。保证段文件恒为固定大小,避免碎片。
fn preallocate(file: &std::fs::File, capacity: u64) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, capacity as libc::off_t) };
        if ret == 0 {
            return Ok(());
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: capacity as libc::off_t,
            fst_bytesalloc: 0,
        };
        // F_PREALLOCATE 只预留空间、不改逻辑大小,失败也无妨——下面 set_len 兜底。
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &store) };
    }
    file.set_len(capacity)?;
    Ok(())
}

/// 一个只追加的日志段。非并发安全:单写者(`&mut self`),但 `read` 可并发。
pub struct LogSegment {
    file: std::fs::File,
    capacity: u64,
    data_capacity: u64,
    data_end: AtomicU64,
    created_ns: u64,
}

impl std::fmt::Debug for LogSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSegment")
            .field("capacity", &self.capacity)
            .field("data_end", &self.data_end.load(Ordering::Acquire))
            .finish()
    }
}

impl LogSegment {
    /// 创建新段:预分配容量(跨平台)→ 写 header → fsync。
    pub fn create(path: impl AsRef<Path>, capacity: u64) -> Result<Self> {
        if capacity <= HEADER_SIZE as u64 {
            return Err(Error::BadArgument(format!(
                "capacity {capacity} must exceed header size {HEADER_SIZE}"
            )));
        }
        let path = path.as_ref();

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        preallocate(&file, capacity)?;

        let seg = Self {
            file,
            capacity,
            data_capacity: capacity - HEADER_SIZE as u64,
            data_end: AtomicU64::new(0),
            created_ns: now_ns(),
        };
        seg.write_header()?;
        seg.file.sync_all()?;
        Ok(seg)
    }

    /// 打开已有段,恢复到上次 `flush` 的状态(data_end 由 header 记录)。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let mut hb = [0u8; HEADER_SIZE];
        file.read_exact_at(&mut hb, 0)?;
        let s = &hb;

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

        Ok(Self {
            file,
            capacity,
            data_capacity: capacity - HEADER_SIZE as u64,
            data_end: AtomicU64::new(data_end),
            created_ns,
        })
    }

    /// 当前逻辑数据末尾(字节粒度,含未 flush 的追加)。
    #[inline]
    pub fn data_end(&self) -> u64 {
        self.data_end.load(Ordering::Acquire)
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
        self.data_capacity - self.data_end.load(Ordering::Acquire)
    }

    #[inline]
    pub fn created_ns(&self) -> u64 {
        self.created_ns
    }

    /// 追加一条记录,返回数据区逻辑起始 offset。**未 fsync**,持久化需 [`flush`](Self::flush)。
    ///
    /// `&self` + `AtomicU64` data_end:写者(D4 下由 write_lock 串行)release-store data_end,
    /// 读者 acquire-load 后按 offset<data_end 定位读,配合 `write_at` 系统调用形成 happens-before。
    pub fn append(&self, record: &[u8]) -> Result<u64> {
        let start = self.data_end.load(Ordering::Acquire);
        if start + record.len() as u64 > self.data_capacity {
            return Err(Error::SegmentFull);
        }
        self.file.write_all_at(record, HEADER_SIZE as u64 + start)?;
        self.data_end
            .store(start + record.len() as u64, Ordering::Release);
        Ok(start)
    }

    /// 持久化:写入 header(data_end)→ `sync_all`(fsync)。成为持久化边界。
    pub fn flush(&self) -> Result<()> {
        self.write_header()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// 就地覆盖已写入区间 `[offset, offset+bytes.len())` 的字节(不改 data_end)。
    /// 用于硬删除等原地擦除;越出已写数据末尾则拒绝。**未 fsync**,持久化需 [`flush`](Self::flush)。
    pub fn overwrite_at(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let end = offset + bytes.len() as u64;
        let data_end = self.data_end.load(Ordering::Acquire);
        if end > data_end {
            return Err(Error::OutOfRange {
                offset,
                len: bytes.len(),
                data_end,
            });
        }
        self.file.write_all_at(bytes, HEADER_SIZE as u64 + offset)?;
        Ok(())
    }

    /// 读取 `[offset, offset+len)` 字节。
    pub fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let end = offset + len as u64;
        let data_end = self.data_end.load(Ordering::Acquire);
        if end > data_end {
            return Err(Error::OutOfRange {
                offset,
                len,
                data_end,
            });
        }
        let mut out = vec![0u8; len];
        self.file
            .read_exact_at(&mut out, HEADER_SIZE as u64 + offset)?;
        Ok(out)
    }

    /// 关闭底层文件(持久化由调用方负责)。
    pub fn close(self) -> Result<()> {
        drop(self.file);
        Ok(())
    }

    fn write_header(&self) -> Result<()> {
        let mut s = [0u8; hdr::HEADER_LEN];
        s[hdr::MAGIC..hdr::MAGIC + 4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
        s[hdr::VERSION..hdr::VERSION + 2].copy_from_slice(&HEADER_VERSION.to_le_bytes());
        s[hdr::CAPACITY..hdr::CAPACITY + 8].copy_from_slice(&self.capacity.to_le_bytes());
        s[hdr::DATA_END..hdr::DATA_END + 8]
            .copy_from_slice(&self.data_end.load(Ordering::Acquire).to_le_bytes());
        s[hdr::CREATED_NS..hdr::CREATED_NS + 8].copy_from_slice(&self.created_ns.to_le_bytes());
        let crc = crc32fast::hash(&s[0..hdr::COVERED]);
        s[hdr::CRC..hdr::CRC + 4].copy_from_slice(&crc.to_le_bytes());
        let mut hb = [0u8; HEADER_SIZE];
        hb[..hdr::HEADER_LEN].copy_from_slice(&s);
        self.file.write_all_at(&hb, 0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let ns = now_ns();
        std::path::PathBuf::from(format!("/var/tmp/ambry_ls_{tag}_{ns}.seg"))
    }

    fn cap(blocks: u64) -> u64 {
        HEADER_SIZE as u64 + blocks * 1024
    }

    #[test]
    fn roundtrip_before_and_after_flush() {
        let path = temp_path("rt");
        let seg = LogSegment::create(&path, cap(64)).unwrap();

        let a = b"hello".to_vec();
        let b = vec![7u8; 100];
        let c = b"the-third-record".to_vec();

        let oa = seg.append(&a).unwrap();
        let ob = seg.append(&b).unwrap();
        let oc = seg.append(&c).unwrap();
        assert_eq!(oa, 0);
        assert_eq!(ob, a.len() as u64);
        assert_eq!(oc, (a.len() + b.len()) as u64);

        assert_eq!(seg.read(oa, a.len()).unwrap(), a);
        assert_eq!(seg.read(ob, b.len()).unwrap(), b);
        assert_eq!(seg.read(oc, c.len()).unwrap(), c);

        seg.flush().unwrap();
        assert_eq!(seg.read(oa, a.len()).unwrap(), a);
        seg.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn oversized_record_rejected() {
        let path = temp_path("big");
        let seg = LogSegment::create(&path, cap(2)).unwrap();
        let big = vec![0u8; (cap(2) - HEADER_SIZE as u64) as usize + 1];
        match seg.append(&big) {
            Err(Error::SegmentFull) => {}
            other => panic!("expected SegmentFull, got {other:?}"),
        }
        seg.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_out_of_range_rejected() {
        let path = temp_path("oor");
        let seg = LogSegment::create(&path, cap(4)).unwrap();
        match seg.read(0, 1000) {
            Err(Error::OutOfRange { .. }) => {}
            other => panic!("expected OutOfRange, got {other:?}"),
        }
        seg.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_recovers_flushed_state() {
        let path = temp_path("reopen");
        let a = vec![3u8; 5000];
        let b = b"tail-record".to_vec();
        let (oa, ob, data_end);
        {
            let seg = LogSegment::create(&path, cap(64)).unwrap();
            oa = seg.append(&a).unwrap();
            ob = seg.append(&b).unwrap();
            seg.flush().unwrap();
            data_end = seg.data_end();
            seg.close().unwrap();
        }
        let seg = LogSegment::open(&path).unwrap();
        assert_eq!(seg.data_end(), data_end);
        assert_eq!(seg.read(oa, a.len()).unwrap(), a);
        assert_eq!(seg.read(ob, b.len()).unwrap(), b);
        seg.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unflushed_data_not_in_header_after_reopen() {
        let path = temp_path("unflush");
        let flushed_end;
        {
            let seg = LogSegment::create(&path, cap(64)).unwrap();
            seg.append(b"aaa").unwrap();
            seg.flush().unwrap();
            flushed_end = seg.data_end();
            // 追加但**不 flush**。
            seg.append(b"bbbbbb").unwrap();
            seg.close().unwrap();
        }
        let seg = LogSegment::open(&path).unwrap();
        // 恢复出的 data_end 只反映最后一次 flush 的边界。
        assert_eq!(
            seg.data_end(),
            flushed_end,
            "durability boundary == last flush"
        );
        seg.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
