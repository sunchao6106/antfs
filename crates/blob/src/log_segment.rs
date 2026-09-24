//! `LogSegment`:预分配、只追加的段文件,同步阻塞 IO(buffered)。
//!
//! 物理布局(无需 O_DIRECT 对齐):
//! ```text
//! [ 第 0 块:Header ] [ 数据区(变长记录顺序打包) ]
//! ```
//! - Header 记录 magic/version/capacity/data_end/key_prefix/crc;数据 offset 0 映射到物理 `HEADER_SIZE`。
//! - 追加走 buffered `write_at`(进 OS page cache);`flush()` 才更新 header.data_end 并 `sync_all`,故
//!   **持久化边界 = flush**。断电时磁盘 header 恒为「上一次 flush 的状态」,恢复严格截断到该边界。

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

const HEADER_MAGIC: u32 = 0x414D_4253; // "AMBS"
const HEADER_VERSION: u16 = 3;
const HEADER_SIZE: usize = 64;

/// 段容量硬上界 = **4 GiB**。这不是调优建议,是全引擎的**格式不变量**:
/// - 段内偏移要放进索引条目的 u32 字段(`types::LogOffset::pack`);
/// - "单条记录 ≤ 段 − 64 < `u32::MAX`" 让记录头里的 u32 `data_len` 结构性不会回绕。
///
/// `StoreConfig::validate` 在配置层先拦一道(给运维看得懂的报错),这里再拦一道 ——
/// 绕过配置直接建库(测试、工具)也必须被挡住,否则索引里的 u32 偏移会静默截断。
pub const MAX_CAPACITY: u64 = 4 * 1024 * 1024 * 1024;

/// header 内各字段偏移(小端)。
mod hdr {
    pub const MAGIC: usize = 0; // u32
    pub const VERSION: usize = 4; // u16
                                  // 6..8 reserved
    pub const CAPACITY: usize = 8; // u64
    pub const DATA_END: usize = 16; // u64
    pub const CREATED_NS: usize = 24; // u64
    /// u32:本段所属副本的 key 前缀 = blobId 高 32bit(partitionId)。同段内所有记录共享,
    /// 故记录体里只存低 32bit 的 `objectId` 后缀(见 `blob.rs` 记录布局)。
    pub const KEY_PREFIX: usize = 32; // u32
    /// u32:段的**代次**(见 [`LogSegmentName`])。v3 新增:段名里也有它,写进段头是为了
    /// **自述** —— 物理拷贝/校验时不必信文件名,比对段头即可。
    pub const GENERATION: usize = 36; // u32
    pub const CRC: usize = 40; // u32,覆盖 [0, CRC)
    pub const COVERED: usize = CRC;
    pub const HEADER_LEN: usize = 44;
}

/// 从一段**段文件前缀字节**里读出段头自述的代次(不落盘、不开文件)。
///
/// 物 bootstrap 的接收端用它校验"刚拷来的字节"正是清单里那一段:段头与清单代次不符
/// 说明对端在拷贝期间重写过 log(同位置指向别的记录),这份拷贝必须丢弃。
pub fn peek_generation(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < hdr::HEADER_LEN {
        return None;
    }
    let magic = u32::from_le_bytes(bytes[hdr::MAGIC..hdr::MAGIC + 4].try_into().ok()?);
    if magic != HEADER_MAGIC {
        return None;
    }
    let stored_crc = u32::from_le_bytes(bytes[hdr::CRC..hdr::CRC + 4].try_into().ok()?);
    if crc32fast::hash(&bytes[0..hdr::COVERED]) != stored_crc {
        return None;
    }
    Some(u32::from_le_bytes(
        bytes[hdr::GENERATION..hdr::GENERATION + 4]
            .try_into()
            .ok()?,
    ))
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
    key_prefix: u32,
    generation: u32,
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
    ///
    /// `generation` = 段的代次(见 [`LogSegmentName`]),由上层 [`Log::create`](crate::log::Log::create) /
    /// 滚动时分配;写进段头与文件名两处(自述,便于物理拷贝后校验)。
    pub fn create(
        path: impl AsRef<Path>,
        capacity: u64,
        key_prefix: u32,
        generation: u32,
    ) -> Result<Self> {
        if capacity <= HEADER_SIZE as u64 {
            return Err(Error::BadArgument(format!(
                "capacity {capacity} must exceed header size {HEADER_SIZE}"
            )));
        }
        if capacity > MAX_CAPACITY {
            return Err(Error::BadArgument(format!(
                "capacity {capacity} exceeds the segment ceiling {MAX_CAPACITY}"
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
            key_prefix,
            generation,
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
        let version = u16::from_le_bytes(s[hdr::VERSION..hdr::VERSION + 2].try_into().unwrap());
        if version != HEADER_VERSION {
            return Err(Error::Corrupt(format!(
                "unsupported segment header version {version} (expected {HEADER_VERSION})"
            )));
        }
        let stored_crc = u32::from_le_bytes(s[hdr::CRC..hdr::CRC + 4].try_into().unwrap());
        let crc = crc32fast::hash(&s[0..hdr::COVERED]);
        if crc != stored_crc {
            return Err(Error::Corrupt(format!(
                "header crc mismatch: stored={stored_crc:#x} computed={crc:#x}"
            )));
        }

        let capacity = u64::from_le_bytes(s[hdr::CAPACITY..hdr::CAPACITY + 8].try_into().unwrap());
        // 段头自述的容量也必须 ≤ 上界:否则段内偏移放不进索引的 u32,读到的数据会错位。
        if capacity > MAX_CAPACITY {
            return Err(Error::Corrupt(format!(
                "segment capacity {capacity} exceeds the ceiling {MAX_CAPACITY}"
            )));
        }
        let data_end = u64::from_le_bytes(s[hdr::DATA_END..hdr::DATA_END + 8].try_into().unwrap());
        let created_ns =
            u64::from_le_bytes(s[hdr::CREATED_NS..hdr::CREATED_NS + 8].try_into().unwrap());
        let key_prefix =
            u32::from_le_bytes(s[hdr::KEY_PREFIX..hdr::KEY_PREFIX + 4].try_into().unwrap());
        let generation =
            u32::from_le_bytes(s[hdr::GENERATION..hdr::GENERATION + 4].try_into().unwrap());

        Ok(Self {
            file,
            capacity,
            data_capacity: capacity - HEADER_SIZE as u64,
            data_end: AtomicU64::new(data_end),
            created_ns,
            key_prefix,
            generation,
        })
    }

    /// 本段所属副本的 key 前缀(blobId 高 32bit)。
    #[inline]
    pub fn key_prefix(&self) -> u32 {
        self.key_prefix
    }

    /// 本段的代次(段头自述;应与文件名里的代次一致,见 [`LogSegmentName`])。
    #[inline]
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// 段头字节数(物理固定 64B)。供上层算"一条记录最多能用多少段内空间"。
    #[inline]
    pub const fn header_size() -> u64 {
        HEADER_SIZE as u64
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

    /// 为一条长度已知的记录预留 `total` 字节连续空间,返回其起始 offset。
    ///
    /// **不推进 `data_end`** —— 预留期间该区间对读者与顺序扫描都不可见,须
    /// [`commit_reserved`](Self::commit_reserved) 才可见。写者由上层 `write_lock` 串行,
    /// 故 `data_end` 在预留期间不会被别的追加改动。
    pub fn reserve(&self, total: u64) -> Result<u64> {
        let start = self.data_end.load(Ordering::Acquire);
        if start + total > self.data_capacity {
            return Err(Error::SegmentFull);
        }
        Ok(start)
    }

    /// 写入预留区内相对 `off` 处的字节(可多次调用,分块流式写入)。
    pub fn write_reserved(&self, start: u64, off: u64, buf: &[u8]) -> Result<()> {
        self.file
            .write_all_at(buf, HEADER_SIZE as u64 + start + off)?;
        Ok(())
    }

    /// 发布预留区间:一次性把 `data_end` 推进到 `start + total`,该记录自此对读者可见。
    pub fn commit_reserved(&self, start: u64, total: u64) -> Result<()> {
        let end = start + total;
        if end > self.data_capacity {
            return Err(Error::SegmentFull);
        }
        debug_assert_eq!(
            self.data_end.load(Ordering::Acquire),
            start,
            "reserved range must be committed before any other append"
        );
        self.data_end.store(end, Ordering::Release);
        Ok(())
    }

    /// 持久化:写入 header(data_end)→ `sync_all`(fsync)。成为持久化边界。
    pub fn flush(&self) -> Result<()> {
        self.write_header()?;
        self.file.sync_all()?;
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

    /// 段的原始前缀 = header + `[0, data_end)` 有效数据(省去预分配的尾部空洞)。
    /// 供 S3b 物理拷贝**封存段**:接收方原样落盘后 [`open`](Self::open) 即得等价段。
    pub fn raw_prefix(&self) -> Result<Vec<u8>> {
        let de = self.data_end.load(Ordering::Acquire) as usize;
        let mut buf = vec![0u8; HEADER_SIZE + de];
        self.file.read_exact_at(&mut buf, 0)?;
        Ok(buf)
    }

    fn write_header(&self) -> Result<()> {
        let mut s = [0u8; hdr::HEADER_LEN];
        s[hdr::MAGIC..hdr::MAGIC + 4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
        s[hdr::VERSION..hdr::VERSION + 2].copy_from_slice(&HEADER_VERSION.to_le_bytes());
        s[hdr::CAPACITY..hdr::CAPACITY + 8].copy_from_slice(&self.capacity.to_le_bytes());
        s[hdr::DATA_END..hdr::DATA_END + 8]
            .copy_from_slice(&self.data_end.load(Ordering::Acquire).to_le_bytes());
        s[hdr::CREATED_NS..hdr::CREATED_NS + 8].copy_from_slice(&self.created_ns.to_le_bytes());
        s[hdr::KEY_PREFIX..hdr::KEY_PREFIX + 4].copy_from_slice(&self.key_prefix.to_le_bytes());
        s[hdr::GENERATION..hdr::GENERATION + 4].copy_from_slice(&self.generation.to_le_bytes());
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

    /// 单个测试段文件:落在 RAII 临时目录里(测试结束整目录回收,`$TMPDIR` 下,带 ambry 前缀可辨认)。
    fn temp_path(tag: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::Builder::new()
            .prefix(&format!("ambry_ls_{tag}_"))
            .tempdir()
            .unwrap();
        let path = dir.path().join(format!("{tag}.seg"));
        (dir, path)
    }

    fn cap(blocks: u64) -> u64 {
        HEADER_SIZE as u64 + blocks * 1024
    }

    #[test]
    fn roundtrip_before_and_after_flush() {
        let (_dir, path) = temp_path("rt");
        let seg = LogSegment::create(&path, cap(64), 7, 1).unwrap();

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
    }

    #[test]
    fn oversized_record_rejected() {
        let (_dir, path) = temp_path("big");
        let seg = LogSegment::create(&path, cap(2), 7, 1).unwrap();
        let big = vec![0u8; (cap(2) - HEADER_SIZE as u64) as usize + 1];
        match seg.append(&big) {
            Err(Error::SegmentFull) => {}
            other => panic!("expected SegmentFull, got {other:?}"),
        }
        seg.close().unwrap();
    }

    #[test]
    fn read_out_of_range_rejected() {
        let (_dir, path) = temp_path("oor");
        let seg = LogSegment::create(&path, cap(4), 7, 1).unwrap();
        match seg.read(0, 1000) {
            Err(Error::OutOfRange { .. }) => {}
            other => panic!("expected OutOfRange, got {other:?}"),
        }
        seg.close().unwrap();
    }

    #[test]
    fn reopen_recovers_flushed_state() {
        let (_dir, path) = temp_path("reopen");
        let a = vec![3u8; 5000];
        let b = b"tail-record".to_vec();
        let (oa, ob, data_end);
        {
            let seg = LogSegment::create(&path, cap(64), 7, 1).unwrap();
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
    }

    #[test]
    fn unflushed_data_not_in_header_after_reopen() {
        let (_dir, path) = temp_path("unflush");
        let flushed_end;
        {
            let seg = LogSegment::create(&path, cap(64), 7, 1).unwrap();
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
    }

    #[test]
    fn create_rejects_capacity_above_ceiling() {
        let (_dir, path) = temp_path("ceiling");
        match LogSegment::create(&path, MAX_CAPACITY + 1, 0, 1) {
            Err(Error::BadArgument(m)) => assert!(m.contains("ceiling"), "got: {m}"),
            other => panic!("expected BadArgument, got {other:?}"),
        }
        assert!(!path.exists(), "被拒的段不该留下文件");
    }

    #[test]
    fn open_rejects_header_capacity_above_ceiling() {
        // 手写一个 capacity 超过上界的段头(CRC 自洽,只违反容量不变量)→ open 必须报 Corrupt。
        let (_dir, path) = temp_path("ceiling_open");
        let mut hb = [0u8; HEADER_SIZE];
        hb[hdr::MAGIC..hdr::MAGIC + 4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
        hb[hdr::VERSION..hdr::VERSION + 2].copy_from_slice(&HEADER_VERSION.to_le_bytes());
        hb[hdr::CAPACITY..hdr::CAPACITY + 8].copy_from_slice(&(MAX_CAPACITY + 1).to_le_bytes());
        let crc = crc32fast::hash(&hb[0..hdr::COVERED]);
        hb[hdr::CRC..hdr::CRC + 4].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, hb).unwrap();
        assert!(
            matches!(LogSegment::open(&path), Err(Error::Corrupt(ref m)) if m.contains("ceiling")),
            "超上界的段头必须判 Corrupt"
        );
    }

    #[test]
    fn key_prefix_survives_reopen_and_flush() {
        let (_dir, path) = temp_path("prefix");
        {
            let seg = LogSegment::create(&path, cap(4), 0xA1B2_C3D4, 1).unwrap();
            assert_eq!(seg.key_prefix(), 0xA1B2_C3D4);
            // flush 重写 header 后前缀不变(CRC 覆盖 key_prefix,写坏会被 open 拒掉)。
            seg.append(b"x").unwrap();
            seg.flush().unwrap();
            seg.close().unwrap();
        }
        let seg = LogSegment::open(&path).unwrap();
        assert_eq!(seg.key_prefix(), 0xA1B2_C3D4);
        assert_eq!(seg.read(0, 1).unwrap(), b"x");
        seg.close().unwrap();
    }

    #[test]
    fn corrupt_key_prefix_trips_header_crc() {
        let (_dir, path) = temp_path("prefix_crc");
        LogSegment::create(&path, cap(4), 7, 1)
            .unwrap()
            .close()
            .unwrap();
        // 只翻转 key_prefix 的一个 bit(不动 CRC)→ open 必须报 Corrupt。
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut b = [0u8; 1];
        file.read_exact_at(&mut b, hdr::KEY_PREFIX as u64).unwrap();
        file.write_all_at(&[b[0] ^ 0x01], hdr::KEY_PREFIX as u64)
            .unwrap();
        match LogSegment::open(&path) {
            Err(Error::Corrupt(_)) => {}
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }
}
