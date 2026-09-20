//! 索引段:活跃段封存后的不可变有序段。
//!
//! - 磁盘态:`{seq}.idx`(定长头 + 排序条目)+ `{seq}.fuse`(Binary Fuse 过滤器,
//!   `DmaSerializable` 的 descriptor + fingerprints 原样落盘,避免启动重建)。
//! - 载入态 [`SealedSegment`]:条目以 **mmap**(`memmap2`)映射,不整段载入 RAM;
//!   过滤器用 `BinaryFuse8Ref` 零拷贝引用落盘字节;查找时过滤器命中再对 mmap 区间二分。
//! - 写用 tmp+rename **先 fsync 再 rename**(保证断电下 rename 后数据已持久)。

use std::path::Path;

use memmap2::Mmap;
use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use crate::error::{Error, Result};
use crate::index::value::{IndexValue, VALUE_SIZE};
use crate::types::{StoreKey, KEY_SIZE};

const IDX_MAGIC: u32 = 0x414D_4258; // "AMBX"
const IDX_VERSION: u16 = 1;
const IDX_HEADER_SIZE: usize = 32;
const ENTRY_SIZE: usize = KEY_SIZE + VALUE_SIZE;

/// 从磁盘读回的原始段条目(活跃段层级整段载入内存用)。
pub(crate) struct LoadedSegment {
    pub entries: Vec<(StoreKey, IndexValue)>,
}

/// 不可变的封存段:mmap 条目 + 零拷贝 Binary Fuse 过滤器。
pub(crate) struct SealedSegment {
    seq: u64,
    map: Mmap,
    filter: BinaryFuse8Ref<'static>,
}

// 二进制安全:SealedSegment 在同一线程使用,不暴露多余访问;mmap 后位模式即字节。
unsafe impl Send for SealedSegment {}
unsafe impl Sync for SealedSegment {}

impl SealedSegment {
    /// 从内存条目构造封存段:写 `{seq}.idx` + `{seq}.fuse`,再 mmap 载入。
    pub fn from_entries(dir: &Path, seq: u64, entries: &[(StoreKey, IndexValue)]) -> Result<Self> {
        write_segment_file(&dir.join(format!("{seq}.idx")), entries)?;
        write_filter_file(&dir.join(format!("{seq}.fuse")), &build_filter(entries)?)?;
        Self::open(dir, seq)
    }

    /// 打开已落盘的封存段:mmap `.idx`,零拷贝载入 `.fuse` 过滤器。
    pub fn open(dir: &Path, seq: u64) -> Result<Self> {
        let idx_path = dir.join(format!("{seq}.idx"));
        let file = std::fs::File::open(&idx_path)?;
        // SAFETY: 段封存即不可变,map 只读;由调用方保证文件不被并发改写。
        let map = unsafe { Mmap::map(&file)? };
        let (desc, fps) = load_filter_parts(&dir.join(format!("{seq}.fuse")))?;
        // 'static:过滤器指纹/描述符需与 mmap 同存活期,这里二者都 Box::leak 以绑定 'static。
        // 内存很小(≈1B/key),且封存段生命周期与 PersistentIndex 绑定,泄漏量等于段总数×过滤器大小,可接受。
        let desc: &'static [u8] = Box::leak(desc.into_boxed_slice());
        let fps: &'static [u8] = Box::leak(fps.into_boxed_slice());
        let filter = BinaryFuse8Ref::from_dma(desc, fps);
        Ok(Self { seq, map, filter })
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn find(&self, key: &StoreKey) -> Option<IndexValue> {
        if !self.filter.contains(&key.hash64()) {
            return None;
        }
        // mmap 上二分:entry 按 key 升序,定长 ENTRY_SIZE。
        let bytes = &self.map[IDX_HEADER_SIZE..];
        let count = bytes.len() / ENTRY_SIZE;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = mid * ENTRY_SIZE;
            let k = StoreKey::from_slice(&bytes[off..off + KEY_SIZE]).expect("key size fixed");
            match k.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let v = IndexValue::from_bytes(&bytes[off + KEY_SIZE..off + ENTRY_SIZE])
                        .expect("value size fixed");
                    return Some(v);
                }
            }
        }
        None
    }
}

/// 把 `BinaryFuse8` 过滤器持久化为 `descriptor + fingerprints`。
fn build_filter(entries: &[(StoreKey, IndexValue)]) -> Result<BinaryFuse8> {
    let mut hashes: Vec<u64> = entries.iter().map(|(k, _)| k.hash64()).collect();
    hashes.sort_unstable();
    hashes.dedup();
    BinaryFuse8::try_from(&hashes).map_err(|e| Error::Corrupt(format!("build fuse filter: {e}")))
}

const FUSE_DESCRIPTOR_LEN: usize = <BinaryFuse8 as DmaSerializable>::DESCRIPTOR_LEN;

fn write_filter_file(path: &Path, filter: &BinaryFuse8) -> Result<()> {
    let mut desc = vec![0u8; FUSE_DESCRIPTOR_LEN];
    filter.dma_copy_descriptor_to(&mut desc);
    let fps = filter.dma_fingerprints();
    let mut buf = Vec::with_capacity(FUSE_DESCRIPTOR_LEN + fps.len());
    buf.extend_from_slice(&desc);
    buf.extend_from_slice(fps);
    atomic_write(path, &buf)
}

fn load_filter_parts(path: &Path) -> Result<(Vec<u8>, Vec<u8>)> {
    let buf = std::fs::read(path)?;
    if buf.len() < FUSE_DESCRIPTOR_LEN {
        return Err(Error::Corrupt(format!(
            "filter file too short: {}",
            path.display()
        )));
    }
    let desc = buf[..FUSE_DESCRIPTOR_LEN].to_vec();
    let fps = buf[FUSE_DESCRIPTOR_LEN..].to_vec();
    Ok((desc, fps))
}

/// 把排序条目写入 `{seq}.idx`(先 fsync 再 rename)。
pub(crate) fn write_segment_file(path: &Path, entries: &[(StoreKey, IndexValue)]) -> Result<()> {
    let count = entries.len();
    let body_len = count * ENTRY_SIZE;
    let mut buf = vec![0u8; IDX_HEADER_SIZE + body_len];

    let mut body_crc = crc32fast::Hasher::new();
    for (i, (k, v)) in entries.iter().enumerate() {
        let off = IDX_HEADER_SIZE + i * ENTRY_SIZE;
        buf[off..off + KEY_SIZE].copy_from_slice(k.as_bytes());
        buf[off + KEY_SIZE..off + ENTRY_SIZE].copy_from_slice(&v.to_bytes());
        body_crc.update(&buf[off..off + ENTRY_SIZE]);
    }
    let body_crc = body_crc.finalize();

    buf[0..4].copy_from_slice(&IDX_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&IDX_VERSION.to_le_bytes());
    buf[8..10].copy_from_slice(&(KEY_SIZE as u16).to_le_bytes());
    buf[10..12].copy_from_slice(&(VALUE_SIZE as u16).to_le_bytes());
    buf[12..16].copy_from_slice(&(count as u32).to_le_bytes());
    buf[16..20].copy_from_slice(&body_crc.to_le_bytes());
    let header_crc = crc32fast::hash(&buf[0..20]);
    buf[20..24].copy_from_slice(&header_crc.to_le_bytes());

    atomic_write(path, &buf)
}

/// 读 `.idx`(封存段用 mmap,这里仅活跃段整段载入校验/恢复用)。
pub(crate) fn read_segment_file(path: &Path) -> Result<LoadedSegment> {
    let buf = std::fs::read(path)?;
    if buf.len() < IDX_HEADER_SIZE {
        return Err(Error::Corrupt(format!("index file too short: {}", path.display())));
    }

    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != IDX_MAGIC {
        return Err(Error::Corrupt(format!("bad index magic {magic:#x}")));
    }
    let header_crc = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    if crc32fast::hash(&buf[0..20]) != header_crc {
        return Err(Error::Corrupt("index header crc mismatch".into()));
    }

    let key_size = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
    let value_size = u16::from_le_bytes(buf[10..12].try_into().unwrap()) as usize;
    if key_size != KEY_SIZE || value_size != VALUE_SIZE {
        return Err(Error::Corrupt(format!(
            "index layout mismatch: key={key_size} value={value_size}"
        )));
    }
    let count = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    let body_crc = u32::from_le_bytes(buf[16..20].try_into().unwrap());

    let body_len = count * ENTRY_SIZE;
    if buf.len() < IDX_HEADER_SIZE + body_len {
        return Err(Error::Corrupt(format!("index body truncated: {}", path.display())));
    }
    let body = &buf[IDX_HEADER_SIZE..IDX_HEADER_SIZE + body_len];
    if crc32fast::hash(body) != body_crc {
        return Err(Error::Corrupt("index body crc mismatch".into()));
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * ENTRY_SIZE;
        let key = StoreKey::from_slice(&body[off..off + KEY_SIZE]).expect("key size checked");
        let val = IndexValue::from_bytes(&body[off + KEY_SIZE..off + ENTRY_SIZE])
            .expect("value size checked");
        entries.push((key, val));
    }

    Ok(LoadedSegment { entries })
}

/// 原子写:写 tmp → fsync → rename → fsync 目录,保证断电下 rename 后内容已持久。
fn atomic_write(path: &Path, buf: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = Path::new(&tmp);
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp)?;
        use std::io::Write;
        let mut f = f;
        f.write_all(buf)?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    if let Some(dir) = path.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}