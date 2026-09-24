//! `BlobStore`:把 [`Log`](crate::log::Log) 与 [`PersistentIndex`](crate::index::PersistentIndex)
//! 串成一个对象存储:`put` / `get` / `delete`。
//!
//! 对象**不可变**:写入后不再改数据,`delete` 只追加墓碑;key(blobId)永久占用,
//! 删除不可逆,TTL 只在写入时定。
//!
//! 每次写入都在 Log 追加一条**自描述记录**(含 key 后缀/flags/crc),索引只保存
//! `key → (LogOffset, 记录大小, flags, TTL)`。因此索引损坏时可由扫描 Log 重建。
//!
//! 记录布局(小端,crc 覆盖其前所有字节;总开销 24B):
//! ```text
//! [ magic u16 | version u8 | flags u8 | key_suffix u32 | expiration i64 | data_len u32 | data | crc u32 ]
//! ```
//! key 的**高 32bit(partitionId)不落在记录里** —— 一个 log 段属于唯一一个副本,该前缀是整段
//! 常量,写在段头(`LogSegment::key_prefix`),记录只存低 32bit 的 `objectId`;读时由
//! [`Log::key_prefix`] 补全。`put` 写 `flags=0`;`delete` 写 `flags=FLAG_DELETED`、`data_len=0` 的墓碑。

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::StoreConfig;
use crate::error::{Error, Result};
use crate::index::value::{FLAG_DELETED, NO_EXPIRATION};
use crate::index::{IndexValue, PersistentIndex};
use crate::log::Log;
use crate::log_segment::LogSegment;
use crate::metrics::{StoreMetricHandles, StoreMetrics};
use crate::types::{LogOffset, LogSegmentName, StoreKey};

const REC_MAGIC: u16 = 0x424D; // "MB"
const REC_VERSION: u8 = 4;

mod rec {
    pub const MAGIC: usize = 0;
    pub const VERSION: usize = 2;
    pub const FLAGS: usize = 3;
    /// key 的**低 32bit**(`objectId`);高 32bit 在段头(`LogSegment::key_prefix`)。
    pub const KEY_SUFFIX: usize = 4; // u32
    pub const EXPIRATION: usize = KEY_SUFFIX + 4; // i64
    pub const DATA_LEN: usize = EXPIRATION + 8; // u32
    pub const DATA: usize = DATA_LEN + 4;
    pub const CRC_LEN: usize = 4;
}

/// 记录去掉 data 后的固定开销(头 + 尾部 crc)。
pub const RECORD_OVERHEAD: usize = rec::DATA + rec::CRC_LEN;

/// 记录头字节数(不含 data 与尾部 crc)。
const RECORD_HEADER: usize = rec::DATA;

/// 流式写([`BlobStore::put_from_reader`])的分块大小:单次上传的内存占用 ≈ 这一块。
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 本机当前时刻(纳秒)。用作副本身份的**本地兜底值**(见 [`ensure_incarnation`])。
fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 只写记录头(magic/version/flags/key 后缀/expiration/data_len)。
///
/// 流式写入需要**先落头再落数据体**(长度已知,先写在预留区首部),故与 [`encode_record`] 共用。
/// `key_suffix` 按**大端**落盘,与 [`StoreKey`] 的字节序一致(便于十六进制对读)。
fn encode_header_into(
    buf: &mut [u8],
    key_suffix: u32,
    flags: u8,
    expiration_ms: i64,
    data_len: u32,
) {
    debug_assert!(buf.len() >= RECORD_HEADER);
    buf[rec::MAGIC..rec::MAGIC + 2].copy_from_slice(&REC_MAGIC.to_le_bytes());
    buf[rec::VERSION] = REC_VERSION;
    buf[rec::FLAGS] = flags;
    buf[rec::KEY_SUFFIX..rec::KEY_SUFFIX + 4].copy_from_slice(&key_suffix.to_be_bytes());
    buf[rec::EXPIRATION..rec::EXPIRATION + 8].copy_from_slice(&expiration_ms.to_le_bytes());
    buf[rec::DATA_LEN..rec::DATA_LEN + 4].copy_from_slice(&data_len.to_le_bytes());
}

/// 记录内 `data_len` 是 u32:数据体超过 `u32::MAX` 的记录**不可写** —— 否则长度字段回绕,落盘后任何读取
/// 都会因 `decode_record` 的尺寸校验失败而报 `Corrupt`(持久性红线:宁拒绝,不写坏)。
///
/// 段大小上限 4 GiB(`StoreConfig::validate` 强制)时本检查**必然通过**(单条记录 ≤ 段 − 102
/// < `u32::MAX`),保留它是第二道保险。
fn record_data_len(len: u64) -> Result<u32> {
    u32::try_from(len).map_err(|_| {
        Error::BadArgument(format!(
            "data length {len} exceeds what a single record can encode ({}) — use chunked upload",
            u32::MAX
        ))
    })
}

/// 索引条目里的 `size` 是 u32(段 ≤ 4 GiB ⇒ 整条记录必然放得下):显式转换,不静默截断。
fn record_size_u32(size: u64) -> Result<u32> {
    u32::try_from(size).map_err(|_| {
        Error::BadArgument(format!(
            "record size {size} does not fit the index entry's u32 field"
        ))
    })
}

fn encode_record(key: &StoreKey, flags: u8, expiration_ms: i64, data: &[u8]) -> Result<Vec<u8>> {
    let data_len = record_data_len(data.len() as u64)?;
    let total = RECORD_OVERHEAD + data.len();
    let mut buf = vec![0u8; total];
    encode_header_into(
        &mut buf[..RECORD_HEADER],
        key.object_id(),
        flags,
        expiration_ms,
        data_len,
    );
    buf[rec::DATA..rec::DATA + data.len()].copy_from_slice(data);
    let crc_at = rec::DATA + data.len();
    let crc = crc32fast::hash(&buf[..crc_at]);
    buf[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

struct DecodedRecord {
    flags: u8,
    key: StoreKey,
    expiration_ms: i64,
    data: Vec<u8>,
}

fn decode_record(buf: &[u8], key_prefix: u32) -> Result<DecodedRecord> {
    if buf.len() < RECORD_OVERHEAD {
        return Err(Error::Corrupt(format!("record too short: {}", buf.len())));
    }
    let magic = u16::from_le_bytes(buf[rec::MAGIC..rec::MAGIC + 2].try_into().unwrap());
    if magic != REC_MAGIC {
        return Err(Error::Corrupt(format!("bad record magic {magic:#x}")));
    }
    if buf[rec::VERSION] != REC_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported record version {}",
            buf[rec::VERSION]
        )));
    }
    let flags = buf[rec::FLAGS];
    let key_suffix = u32::from_be_bytes(
        buf[rec::KEY_SUFFIX..rec::KEY_SUFFIX + 4]
            .try_into()
            .unwrap(),
    );
    let key = StoreKey::from_parts(key_prefix, key_suffix);
    let expiration_ms = i64::from_le_bytes(
        buf[rec::EXPIRATION..rec::EXPIRATION + 8]
            .try_into()
            .unwrap(),
    );
    let data_len =
        u32::from_le_bytes(buf[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
    let crc_at = rec::DATA + data_len;
    if buf.len() != crc_at + rec::CRC_LEN {
        return Err(Error::Corrupt(format!(
            "record size mismatch: buf={} expected={}",
            buf.len(),
            crc_at + rec::CRC_LEN
        )));
    }
    let stored = u32::from_le_bytes(buf[crc_at..crc_at + 4].try_into().unwrap());
    let computed = crc32fast::hash(&buf[..crc_at]);
    if stored != computed {
        return Err(Error::Corrupt(format!(
            "record crc mismatch: stored={stored:#x} computed={computed:#x}"
        )));
    }
    Ok(DecodedRecord {
        flags,
        key,
        expiration_ms,
        data: buf[rec::DATA..crc_at].to_vec(),
    })
}

/// 对象存储:单写者(`&mut self` 写路径)。数据走 Log(std::fs 同步阻塞),索引走 std::fs。
/// 可共享的对象存储(D4 并发模型 P2):可 `Arc<BlobStore>` 跨线程,写方法 `&self`。
///
/// - `inner`:`{log, index}` 快照,读者取读锁克隆 `Arc<Inner>`(纳秒级)后无锁读;正常写直接
///   经当前 `Inner` 的内部可变 `log`/`index` 就地改,**仅 compaction** 构建全新 `Inner` 原子换入。
///   Unix 下老读者持有的 `Arc<Inner>` 仍握有旧段 fd,compaction rename/unlink 旧文件不影响其读。
/// - `write_lock`:串行化所有写者与 compaction(单写者不变量)。
/// - `seg_bytes`:每写都变但仅被 stats 读,独立 `Mutex`,不放 `Inner`(否则每写都要换 `Inner`)。
pub struct BlobStore {
    dir: PathBuf,
    total_capacity: u64,
    segment_capacity: u64,
    max_index_entries_per_segment: usize,
    /// 本副本的 key 前缀 = blobId 高 32bit(partitionId)。写入 log 段头,记录体内只存低 32bit;
    /// 写路径据此**拒绝不属于本分区的 key**(否则落盘后读不回来)。
    key_prefix: u32,
    /// 副本级坐标系身份(见 [`BlobStore::incarnation`]);热路径只读,故用原子。
    incarnation: AtomicU64,
    inner: RwLock<Arc<Inner>>,
    write_lock: Mutex<()>,
    seg_bytes: Mutex<BTreeMap<u64, SegBytes>>,
    /// 有效数据字节的增量核算(排除墓碑 + 按过期时刻分桶),使 `valid_data_size` 免于每次全扫 log。
    valid_stats: Mutex<ValidStats>,
    metrics: StoreMetricHandles,
    /// 连续 IO 错误计数(被动磁盘检测,对齐 Ambry `BlobStore.errorCount`);成功 op 清零。
    io_error_count: AtomicU32,
    /// IO 错误自关阈值(0 = 禁用,对齐 Ambry 默认 `MAX_VALUE`);由 `StoreConfig` 注入。
    io_error_threshold: AtomicU32,
    /// 是否可用;连续 IO 错误达阈值自关后置 false,后续 op 拒绝(`Error::Unavailable`)。
    available: AtomicBool,
    /// 自关回调(store-server 设置:标 replica ERROR → 上报 master),仅触发一次。
    io_error_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// 打开时的容错恢复报告(空 = 干净);见 [`OpenDamage`] 与 [`Self::open_damage`]。
    open_damage: OpenDamage,
}

/// 一致的 `{log, index}` 快照;compaction 时整体替换,保证读者不见「新 log + 旧 index」撕裂态。
struct Inner {
    log: Log,
    index: PersistentIndex,
}

/// 单个 log 段的存活/死亡字节。死 = 被更新版本取代/删除的旧记录(非当前索引赢家)。
#[derive(Clone, Copy, Default)]
struct SegBytes {
    live: u64,
    dead: u64,
}

/// 有效数据字节的增量核算:当前索引赢家中**未删除**记录的 `size` 之和(墓碑不计)。
///
/// 简化取舍:**不区分过期**——过期但尚未被 compaction 回收的对象仍占盘,故计入(避免按过期
/// 时刻分桶的复杂度)。赢家变更(put/delete)时增量 add/remove,compaction/open 全量重算。
#[derive(Default)]
struct ValidStats {
    bytes: u64,
}

impl ValidStats {
    fn add(&mut self, v: &IndexValue) {
        if !v.is_deleted() {
            self.bytes += v.size_bytes();
        }
    }

    fn remove(&mut self, v: &IndexValue) {
        if !v.is_deleted() {
            self.bytes = self.bytes.saturating_sub(v.size_bytes());
        }
    }
}

/// compaction 结果统计。
///
/// `dead_bytes` = 本次**未搬入**的字节(被取代的旧记录 + 被回收的过期对象/墓碑),`live` = 搬入的。
/// 后两个字段是回收构成的分项(观测用,已含在 `dead_bytes` 内)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    pub live_bytes: u64,
    pub dead_bytes: u64,
    /// `dead_bytes` 中来自**墓碑**(已过保留期)的字节。
    pub tombstone_reclaimed_bytes: u64,
    /// `dead_bytes` 中来自 **TTL 过期对象**的字节。
    pub expired_reclaimed_bytes: u64,
    pub segments_before: usize,
    pub segments_after: usize,
}

/// compaction 策略(纯参数,由上层按配置构造):判活口径 + 触发阈值。
///
/// 「可回收」= 被取代的旧记录(死字节)+ TTL 已过期对象 + 已过保留期的墓碑(后者还要过
/// [`tombstones_allowed`](Self::tombstones_allowed) 这道闸门)。阈值 `dead_ratio`
/// 同时用于 **partition 级闸门** 与 **段级选择**。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactPolicy {
    /// 触发阈值 (0,1]:可回收字节占比 ≥ 此值才压缩。
    pub dead_ratio: f64,
    /// 判定「现在」的 unix 毫秒(过期与保留期都以它为准)。
    pub now_ms: i64,
    /// 墓碑保留期(毫秒):`delete` 之后满此时长才允许回收墓碑,防副本修复把已删对象复活。
    /// [`i64::MAX`] = 永不回收墓碑。
    pub tombstone_retention_ms: i64,
    /// **墓碑回收闸门**:`false` 时墓碑一律照搬(连已过保留期的也不回收)。
    ///
    /// 保留期只说"删除已足够久",但如果**还有副本没追平**,墓碑一收,那份滞后副本就永远学不到
    /// 这条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。故集群侧由 master 权威下发
    /// ("该分区所有副本都已追平"才放行),store 侧按副本的闸门值填本字段(见 `crate::worker`)。
    ///
    /// [`Self::new`] 给的是 `true`(保留期本身就是判活口径,与改造前的行为一致);单机 / 不关心
    /// 跨副本传播的调用方可不设。
    pub tombstones_allowed: bool,
}

impl CompactPolicy {
    pub fn new(dead_ratio: f64, now_ms: i64, tombstone_retention_ms: i64) -> Self {
        Self {
            dead_ratio,
            now_ms,
            tombstone_retention_ms,
            tombstones_allowed: true,
        }
    }

    /// 设定墓碑回收闸门(`false` = 墓碑一律照搬,见 [`Self::tombstones_allowed`])。
    pub fn with_tombstones_allowed(mut self, tombstones_allowed: bool) -> Self {
        self.tombstones_allowed = tombstones_allowed;
        self
    }

    /// 历史口径:只回收被取代的旧记录,不判定过期、墓碑永留(供不关心新回收口径的调用方/测试用)。
    pub fn preserve_legacy(dead_ratio: f64) -> Self {
        Self {
            dead_ratio,
            now_ms: 0,
            tombstone_retention_ms: i64::MAX,
            tombstones_allowed: false,
        }
    }

    /// 该索引赢家是否可回收(过期对象 / 过保留期墓碑)。
    fn is_reclaimable(&self, v: &IndexValue) -> bool {
        v.is_expired_at(self.now_ms)
            || (self.tombstones_allowed
                && v.is_reclaimable_tombstone(self.now_ms, self.tombstone_retention_ms))
    }
}

/// **打开副本时的容错恢复报告**:`open` 遇到坏记录不再整体失败,而是跳过坏记录、尽力恢复,
/// 并把损伤如实记在这里(见 [`BlobStore::open_damage`]);同时 ERROR 日志 + 指标 + 经 `store` 的
/// `scrub-report` 暴露给运维。
///
/// 设计取舍(CLAUDE.md §3.1 持久性偏执):
/// - **不删、不改写、不截断**损坏字节 —— 保留证据,便于事后分析与人工处置;
/// - 只把**无法解码**的记录从索引里跳过(它们本来就读不出来),其余记录照常可读可写;
/// - 因此"带伤启动"的副本会**少数据但对外可服务**,损伤程度由本报告与 `verify` 如实呈现,
///   绝不静默。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenDamage {
    /// 打开时跳过(无法解码 / CRC 不符)的记录数。
    pub bad_records: u64,
    /// 第一条坏记录的 `(segment, offset)`。
    pub first_bad_offset: Option<(u64, u64)>,
    /// 因**无法重同步**而放弃扫描剩余部分的段数:该段自损坏点之后的内容没有进索引
    /// (数据仍在盘上,未被删除或改写)。
    pub abandoned_segments: u64,
}

impl OpenDamage {
    /// 是否发生过损伤(日志/指标据此判断,避免干净副本也刷日志)。
    pub fn is_clean(&self) -> bool {
        self.bad_records == 0 && self.abandoned_segments == 0
    }
}

/// 本地完整性校验结果(见 [`BlobStore::verify`])。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VerifyReport {
    /// 成功解码且 CRC 校验通过的记录数。
    pub records: u64,
    /// 解码或 CRC 失败的记录数(每条计 1)。
    pub bad_records: u64,
    /// 第一条坏记录的 `(segment, offset)`;全好则 `None`。
    pub first_bad_offset: Option<(u64, u64)>,
    /// 活记录里"索引元数据与 log 记录不符"的条数(size/flags 任一不符)。
    pub index_mismatch: u64,
    /// 扫描未覆盖全部 log:`max_bytes` 到点,或遇到无法重同步的坏记录(段尾截断)时为 true。
    pub truncated: bool,
}

/// 副本摘要(只走索引/mmap,**不读数据体**,开销与 key 数成正比)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexStats {
    /// 活记录数(不含墓碑)。
    pub live_records: u64,
    /// 墓碑(软删记录)数。
    pub tombstones: u64,
    /// 活记录的数据字节(不含记录头与 CRC,不含墓碑)。
    pub live_bytes: u64,
    /// 封存段数(不含活跃段)。
    pub sealed_segments: usize,
}

/// blob 元数据(不读数据体):状态标志 + TTL + 数据长度。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobMeta {
    pub deleted: bool,
    pub expired: bool,
    pub expiration_ms: i64,
    pub data_len: u64,
}

/// 一条记录的复制信息(对齐 Ambry `MessageInfo`):key + 大小 + 当前状态标志。account/container
/// 编码在不透明 `StoreKey` 内故不单列;crc 在 log 记录内故不单列。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageInfo {
    pub key: StoreKey,
    pub size: u64,
    pub expiration_ms: i64,
    pub deleted: bool,
}

/// 复制增量拉取游标(对齐 Ambry `StoreFindToken`:log offset 游标 + **段身份**)。
///
/// 游标记录的是"最后一条已审视记录"所在的**段身份**(位置 + 代次,见 [`LogSegmentName`])与段内偏移。
/// 段身份在副本历史上永不复用(位置会随全库 compact 从 0 重来,但代次接着涨),所以"认不出这个段"
/// 就等价于"这个游标作废了"(对齐 Ambry 按段名校验 `revalidateStoreFindToken`)。
///
/// 副本级身份(incarnation)不放进这里:它属于**副本**而不是段,由复制层单独持有与校验
/// (见 `docs/design/store.md` 的"复制游标重定位")。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FindToken {
    /// 从 log 起点开始。
    #[default]
    Uninitialized,
    /// 上次返回的最后 offset(下次从其之后继续,exclusive)+ 它所属的段身份。
    Offset {
        segment: LogSegmentName,
        offset: u64,
    },
}

impl FindToken {
    pub fn at(segment: LogSegmentName, offset: u64) -> Self {
        Self::Offset { segment, offset }
    }

    /// 游标位置(段位置 + 段内偏移);`Uninitialized` = 无(从起点)。
    pub fn offset(&self) -> Option<LogOffset> {
        match self {
            Self::Uninitialized => None,
            Self::Offset { segment, offset } => {
                Some(LogOffset::new(segment.position as u64, *offset))
            }
        }
    }

    /// 游标所属的段身份;`Uninitialized` = 无游标。
    pub fn segment_name(&self) -> Option<LogSegmentName> {
        match self {
            Self::Uninitialized => None,
            Self::Offset { segment, .. } => Some(*segment),
        }
    }
}

/// 游标为什么被重定位(复制侧据此收紧判据与打点,对齐 Ambry `revalidateStoreFindToken`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenResetReason {
    /// 游标所在段已不在段表里(compaction 删了它,或对方整体重写过 log)。
    SegmentMissing,
    /// 游标位置上有段,但**代次不同** —— 段号被复用(全库 compact 后段位置从 0 重来)。
    SegmentReplaced,
    /// 游标偏移超出该段数据区(段被截断,或该游标根本不属于这个 log)。
    OffsetOutOfRange,
    /// 游标声明的**副本 incarnation** 与服务端不符(副本被重建 / 换盘后重来)。由**服务层**判定并置入
    /// —— 引擎看不到副本级身份。
    IncarnationMismatch,
}

impl TokenResetReason {
    /// 指标/日志用的稳定短名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SegmentMissing => "segment_missing",
            Self::OffsetOutOfRange => "offset_out_of_range",
            Self::SegmentReplaced => "segment_generation_mismatch",
            Self::IncarnationMismatch => "incarnation_mismatch",
        }
    }
}

/// [`find_entries_since`](BlobStore::find_entries_since) 结果:本批条目 + 新游标 + 已读日志字节。
#[derive(Debug, Clone)]
pub struct FindInfo {
    pub entries: Vec<MessageInfo>,
    pub token: FindToken,
    /// 从 log 起点到新游标的累计字节(复制滞后度量,对齐 Ambry `FindInfo` bytesRead)。
    pub bytes_read: u64,
    /// 本轮游标是否被重定位(`Some(reason)`);**重定位那轮不得据此判"已追上"**。
    pub token_reset: Option<TokenResetReason>,
}

/// [`apply_replicated`](BlobStore::apply_replicated) 结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// peer 记录比本地新,已写入。
    Applied,
    /// 本地已 ≥ peer 状态,未改动(幂等)。
    Skipped,
}

/// 从 peer 复制来的一条记录(拉复制 apply 的输入,承 Ambry replication `MessageInfo` + 数据体)。
///
/// `data` 仅在"本地缺失且该记录为存活赢家(非墓碑)"时必需;墓碑无需数据,置 `None` 即可。
#[derive(Debug, Clone)]
pub struct ReplicatedRecord {
    pub key: StoreKey,
    pub flags: u8,
    pub expiration_ms: i64,
    pub data: Option<Vec<u8>>,
}

impl BlobStore {
    /// 在 `dir` 下创建新库(key 前缀 0,供无分区上下文的测试/工具用):数据在 `dir/log`,索引在 `dir/index`。
    ///
    /// 生产路径用 [`create_for_partition`](Self::create_for_partition)。用错前缀不会静默写坏数据:
    /// 写路径按前缀校验 key,不属于本分区的 key 直接 [`BadArgument`](Error::BadArgument)。
    pub fn create(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        Self::create_for_partition(
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            0,
        )
    }

    /// 在 `dir` 下创建新库,并声明本副本所属分区:`partition_id` 即 key 前缀(blobId 高 32bit),
    /// 落进每个 log 段头,记录体内只存低 32bit 后缀。
    pub fn create_for_partition(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
        partition_id: u32,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let (log_dir, index_dir) = sub_dirs(&dir);
        // 新副本:段代次从 1 起(段身份 = 位置 + 代次,见 `LogSegmentName`)。
        let log = Log::create(&log_dir, total_capacity, segment_capacity, partition_id, 1)?;
        let index = PersistentIndex::create(&index_dir, max_index_entries_per_segment)?;
        // 新副本的 incarnation:本地先给一个**本机唯一**的值(当前时刻),master 随后会用它颁发的值
        // 纠正(见 `adopt_incarnation`)。单机模式没有 master,这个本地值就是最终值。
        let incarnation = now_ns().max(1);
        write_incarnation(&dir, incarnation)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            key_prefix: partition_id,
            incarnation: AtomicU64::new(incarnation),
            inner: RwLock::new(Arc::new(Inner { log, index })),
            write_lock: Mutex::new(()),
            seg_bytes: Mutex::new(BTreeMap::new()),
            valid_stats: Mutex::new(ValidStats::default()),
            metrics: StoreMetricHandles::disabled(),
            io_error_count: AtomicU32::new(0),
            io_error_threshold: AtomicU32::new(0),
            available: AtomicBool::new(true),
            io_error_hook: Mutex::new(None),
            open_damage: OpenDamage::default(),
        })
    }

    /// 打开已有库并恢复索引与日志;若有未完成的 compaction 提交标记,先修复。
    ///
    /// 崩溃安全:加载已落盘索引后,按**持久化水位**回放 log 尾部——把「已落盘到 log 但
    /// 上次索引持久化未覆盖」的记录补进索引(对齐 Ambry `recover()`),避免加载到陈旧索引。
    /// 索引文件损坏(`Corrupt`)时自动回退到全量扫 log 重建(数据 fsync 是持久边界,索引可重建)。
    pub fn open(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        recover_compaction(
            &dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
        )?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let (index, open_damage) =
            match load_index_with_replay(&log, &index_dir, max_index_entries_per_segment) {
                Ok(index) => (index, OpenDamage::default()),
                // 索引坏、或回放扫到损坏记录 → 全量**容错**重建(坏记录跳过,而不是让副本起不来)。
                // 注意 `OutOfRange` 也要算损坏:记录头里的长度字段被写坏时,`read_full_record` 读越界
                // 抛的就是它(见 `resync_skip` 的段尾校验),只认 `Corrupt` 会漏掉这类。
                Err(Error::Corrupt(_) | Error::OutOfRange { .. }) => {
                    rebuild_index_tolerant(&log, &index_dir, max_index_entries_per_segment)?
                }
                Err(e) => return Err(e),
            };
        // 段字节核算本身也是**容错扫描**(坏记录跳过而非报错),它同时是"索引完好但 log 已损坏"
        // 这条路径上唯一的损伤发现者(索引可加载时不会走重建)。
        let (seg_bytes, scan_damage) = scan_seg_bytes_tolerant(&log, &index)?;
        let open_damage = if open_damage.is_clean() {
            scan_damage
        } else {
            open_damage
        };
        log_open_damage(&dir, &open_damage);
        let valid_stats = compute_valid_stats(&log, &index)?;
        let incarnation = ensure_incarnation(&dir)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            key_prefix: log.key_prefix(),
            incarnation: AtomicU64::new(incarnation),
            inner: RwLock::new(Arc::new(Inner { log, index })),
            write_lock: Mutex::new(()),
            seg_bytes: Mutex::new(seg_bytes),
            valid_stats: Mutex::new(valid_stats),
            metrics: StoreMetricHandles::disabled(),
            io_error_count: AtomicU32::new(0),
            io_error_threshold: AtomicU32::new(0),
            available: AtomicBool::new(true),
            io_error_hook: Mutex::new(None),
            open_damage,
        })
    }

    /// 索引损坏/丢失时的兜底:打开 Log,丢弃旧索引文件,顺序扫描全部记录重建索引。
    ///
    /// 记录自描述(含 key/flags/expiration),按 Log 追加顺序(旧→新)插入即天然满足 newest-wins,
    /// 重建后 expiration 由记录读回(不丢 TTL)。只重建到最近一次 flush 的持久化边界(未落盘尾部本就不可见,无残记录)。
    pub fn open_rebuild_index(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        segment_capacity: u64,
        max_index_entries_per_segment: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        recover_compaction(
            &dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
        )?;
        let (log_dir, index_dir) = sub_dirs(&dir);
        // 物理 bootstrap 装载的目录只有段文件:身份由 `Log::open` 从首段 `created_ns` 自愈补写。
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let (index, open_damage) =
            rebuild_index_tolerant(&log, &index_dir, max_index_entries_per_segment)?;
        let (seg_bytes, scan_damage) = scan_seg_bytes_tolerant(&log, &index)?;
        let open_damage = if open_damage.is_clean() {
            scan_damage
        } else {
            open_damage
        };
        log_open_damage(&dir, &open_damage);

        let valid_stats = compute_valid_stats(&log, &index)?;
        let incarnation = ensure_incarnation(&dir)?;
        Ok(Self {
            dir,
            total_capacity,
            segment_capacity,
            max_index_entries_per_segment,
            key_prefix: log.key_prefix(),
            incarnation: AtomicU64::new(incarnation),
            inner: RwLock::new(Arc::new(Inner { log, index })),
            write_lock: Mutex::new(()),
            seg_bytes: Mutex::new(seg_bytes),
            valid_stats: Mutex::new(valid_stats),
            metrics: StoreMetricHandles::disabled(),
            io_error_count: AtomicU32::new(0),
            io_error_threshold: AtomicU32::new(0),
            available: AtomicBool::new(true),
            io_error_hook: Mutex::new(None),
            open_damage,
        })
    }

    /// 依赖注入版构造(对齐 Ambry `StorageManager`→`BlobStore` 的 DI 形状):由调用方注入共享的
    /// [`StoreConfig`] 与 [`StoreMetrics`],`store_id` 作为指标的 `store` label。`total_capacity`
    /// 是 per-replica 属性(对齐 Ambry `capacityInBytes`),仍显式传入。
    ///
    /// 本构造用 key 前缀 0(无分区上下文);生产副本走
    /// [`create_with_partition`](Self::create_with_partition)。
    pub fn create_with(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
    ) -> Result<Self> {
        Self::create_with_partition(dir, total_capacity, config, metrics, store_id, 0)
    }

    /// [`create_with`](Self::create_with) + 声明本副本分区(`partition_id` = key 前缀 = blobId 高 32bit)。
    pub fn create_with_partition(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
        partition_id: u32,
    ) -> Result<Self> {
        let store = Self::create_for_partition(
            dir,
            total_capacity,
            config.segment_size_bytes,
            config.index_max_inmem_elements,
            partition_id,
        )?;
        Self::finish_di(store, config, metrics, store_id)
    }

    /// 打开已有库的依赖注入版,参数语义同 [`create_with`](Self::create_with)。
    /// key 前缀取自盘上的段头(副本自描述其分区)。
    pub fn open_with(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
    ) -> Result<Self> {
        Self::open_di(dir, total_capacity, config, metrics, store_id, None)
    }

    /// [`open_with`](Self::open_with) + **校验**盘上副本的分区正是 `partition_id`。
    /// 不符即 [`Corrupt`](Error::Corrupt)(目录放错分区/被替换时大声失败,而不是按错前缀解释数据)。
    pub fn open_with_partition(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
        partition_id: u32,
    ) -> Result<Self> {
        Self::open_di(
            dir,
            total_capacity,
            config,
            metrics,
            store_id,
            Some(partition_id),
        )
    }

    /// `open_with*` 的实现:`expected_partition` 非空时校验段头前缀。
    fn open_di(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
        expected_partition: Option<u32>,
    ) -> Result<Self> {
        let store = Self::open(
            dir,
            total_capacity,
            config.segment_size_bytes,
            config.index_max_inmem_elements,
        )?;
        check_partition(&store, expected_partition)?;
        Self::finish_di(store, config, metrics, store_id)
    }

    /// 依赖注入版的重建打开(装载物理拷贝的段文件后用,S3b):同 [`open_with`](Self::open_with),
    /// 但走 [`open_rebuild_index`](Self::open_rebuild_index) 从段文件全量重建索引。
    pub fn open_rebuild_index_with(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
    ) -> Result<Self> {
        Self::open_rebuild_di(dir, total_capacity, config, metrics, store_id, None)
    }

    /// [`open_rebuild_index_with`](Self::open_rebuild_index_with) + **校验**盘上段头的 key 前缀正是
    /// `partition_id`:物理 bootstrap 收到的封存段必须属于本分区(拷错分区的段会被当场拒掉)。
    pub fn open_rebuild_index_with_partition(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
        partition_id: u32,
    ) -> Result<Self> {
        Self::open_rebuild_di(
            dir,
            total_capacity,
            config,
            metrics,
            store_id,
            Some(partition_id),
        )
    }

    fn open_rebuild_di(
        dir: impl AsRef<Path>,
        total_capacity: u64,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
        expected_partition: Option<u32>,
    ) -> Result<Self> {
        let store = Self::open_rebuild_index(
            dir,
            total_capacity,
            config.segment_size_bytes,
            config.index_max_inmem_elements,
        )?;
        check_partition(&store, expected_partition)?;
        Self::finish_di(store, config, metrics, store_id)
    }

    /// `*_with` 家族的公共收尾:注入 IO-error 阈值与指标 label、上报开库损伤、刷新 gauge。
    fn finish_di(
        mut store: Self,
        config: &StoreConfig,
        metrics: &StoreMetrics,
        store_id: &str,
    ) -> Result<Self> {
        store
            .io_error_threshold
            .store(config.io_error_count_to_trigger_shutdown, Ordering::Relaxed);
        store.metrics = metrics.for_store(store_id);
        store.report_open_damage();
        store.refresh_gauges();
        Ok(store)
    }

    /// 本副本负责的 key 前缀(blobId 高 32bit = partitionId),来自构造参数或盘上段头。
    pub fn key_prefix(&self) -> u32 {
        self.key_prefix
    }

    /// 记录只存 key 的低 32bit,高 32bit 由段头承载 —— 写进来的 key 必须属于本副本分区,
    /// 否则落盘后读不回来。宁可在此拒绝,也不写一条解不出来的记录。
    fn check_key(&self, key: &StoreKey) -> Result<()> {
        if key.partition_id() != self.key_prefix {
            return Err(Error::BadArgument(format!(
                "key {key:?} is in partition {} but this replica stores partition {}",
                key.partition_id(),
                self.key_prefix
            )));
        }
        Ok(())
    }

    /// 封存段(不可变,除活跃段外)的 `(position, data_end)`,S3b 物理拷贝发送方枚举。
    /// 封存段清单(`(段身份, data_end)`,按位置升序;不含活跃段)。S3b 物理拷贝与复制清单用。
    pub fn sealed_segments(&self) -> Vec<(LogSegmentName, u64)> {
        self.inner().log.sealed_segment_names()
    }

    /// 本地 log 在 `after`(段位置 + 段内偏移)**之后还有记录**吗?
    ///
    /// **纯位置扫描,不校验段身份/代次、不触发重定位** —— 这是复制侧"本地是否已 apply 到 peer 游标
    /// 处"的辅助判据:peer 游标里的段身份属于**对端坐标系**(本地段位置与代次跟对端并不一一对应),
    /// 拿它进 [`find_entries_since`](Self::find_entries_since) 会被重定位判定误伤。
    ///
    /// 语义与 [`find_entries_since`](Self::find_entries_since) 的游标一致:游标处那条记录**本身**
    /// 不算"之后的内容"(游标是 exclusive 的)。读失败一律按"还有内容"(= 不判追平,保守)。
    pub fn has_content_after(&self, after: LogOffset) -> bool {
        let inner = self.inner();
        for (position, data_end) in inner.log.segment_spans() {
            if position < after.segment {
                continue;
            }
            let begin = if position == after.segment {
                after.offset
            } else {
                0
            };
            let mut off = begin;
            while off < data_end {
                if position == after.segment && off == after.offset {
                    // 跳过游标处那条记录本身(其长度从记录头读;读不出就保守认为"还有内容")。
                    match read_full_record(&inner.log, LogOffset::new(position, off)) {
                        Ok((raw, _)) => off += raw.len() as u64,
                        Err(_) => return true,
                    }
                    continue;
                }
                return true;
            }
        }
        false
    }

    /// 读某封存段的原始前缀字节(header + 有效数据),S3b 物理拷贝发送方。
    ///
    /// 按**段身份**(位置 + 代次)取:同位置不同代次 → 报"没有这段"(取段方 404 化后丢弃拷贝
    /// 回落纯逻辑拉取),避免把"已经被 compaction 重写过的段"当同一段发出去。
    pub fn read_segment_prefix(&self, name: LogSegmentName) -> Result<Vec<u8>> {
        self.inner().log.read_segment_prefix(name)
    }

    /// 打开时的容错恢复报告(空 = 干净副本)。`store` 经 `scrub-report` 把它转给运维,
    /// 使"带伤启动"在 admin 的一致性巡检里立刻表现为差异。
    pub fn open_damage(&self) -> OpenDamage {
        self.open_damage
    }

    /// 把打开时的损伤推到注入的指标上(`*_with` 构造在注入 metrics 后调用;干净副本零副作用)。
    fn report_open_damage(&self) {
        if self.open_damage.is_clean() {
            return;
        }
        self.metrics
            .open_bad_records
            .inc_by(self.open_damage.bad_records);
        self.metrics
            .open_abandoned_segments
            .inc_by(self.open_damage.abandoned_segments);
    }

    /// 从容量/段容量/gauge 刷新容量类指标(在写/compaction/flush 后调用)。
    fn refresh_gauges(&self) {
        let (mut live, mut dead) = (0u64, 0u64);
        let count = {
            let seg = self.seg_bytes.lock().expect("blob seg_bytes poisoned");
            for v in seg.values() {
                live += v.live;
                dead += v.dead;
            }
            seg.len() as i64
        };
        self.metrics.used_capacity_bytes.set((live + dead) as i64);
        self.metrics.invalid_data_bytes.set(dead as i64);
        self.metrics.segment_count.set(count);
    }

    /// 设置"连续 IO 错误达阈值自关"的回调(store-server 用:标 replica ERROR → 上报 master)。仅触发一次。
    pub fn set_io_error_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.io_error_hook.lock().expect("io_error_hook poisoned") = Some(hook);
    }

    /// store 是否仍可用(连续 IO 错误达阈值自关后为 false)。
    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    /// 当前连续 IO 错误计数(观测/测试用)。
    pub fn io_error_count(&self) -> u32 {
        self.io_error_count.load(Ordering::Relaxed)
    }

    /// 成功 op → 清零连续 IO 错误计数(对齐 Ambry `onSuccess`)。
    fn record_success(&self) {
        self.io_error_count.store(0, Ordering::Relaxed);
    }

    /// IO 错误 op → 递增计数;阈值>0 且达阈值时自关 + 触发一次回调(对齐 Ambry `onError`)。
    fn record_io_error(&self) {
        let threshold = self.io_error_threshold.load(Ordering::Relaxed);
        let count = self.io_error_count.fetch_add(1, Ordering::Relaxed) + 1;
        // swap 保证只在首次跨阈值触发回调(后续 IO 错误 available 已为 false)。
        if threshold > 0 && count >= threshold && self.available.swap(false, Ordering::Relaxed) {
            if let Some(hook) = self
                .io_error_hook
                .lock()
                .expect("io_error_hook poisoned")
                .clone()
            {
                hook();
            }
        }
    }

    /// 按 op 结果更新被动 IO 错误状态:`Ok`→清零;`Err(Io)`→计数(可能自关);其它逻辑错误→不计不清。
    fn track<T>(&self, r: Result<T>) -> Result<T> {
        match &r {
            Ok(_) => self.record_success(),
            Err(Error::Io(_)) => self.record_io_error(),
            Err(_) => {}
        }
        r
    }

    fn inner(&self) -> Arc<Inner> {
        self.inner.read().expect("blob inner poisoned").clone()
    }

    /// 写入一个对象(无过期)。key 已存在(含墓碑)则报 [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。
    pub fn put(&self, key: StoreKey, data: &[u8]) -> Result<()> {
        self.put_ttl(key, data, NO_EXPIRATION)
    }

    /// 写入一个对象并设定过期时间(`expiration_ms`,`NO_EXPIRATION` 表示不过期)。
    ///
    /// 对齐 Ambry:blob id 永久占用——命中任何已存在条目(PUT 或 DELETE 墓碑)一律
    /// [`Error::AlreadyExists`](crate::error::Error::AlreadyExists)。对象不可变且**删除不可逆**,
    /// 故没有"删后重写/复活"这条路。
    pub fn put_ttl(&self, key: StoreKey, data: &[u8], expiration_ms: i64) -> Result<()> {
        if !self.is_available() {
            return Err(Error::Unavailable);
        }
        self.track(self.put_ttl_inner(key, data, expiration_ms))
    }

    /// 流式写入一个对象:`src` 提供 `len` 字节数据体,边读边写,**内存占用恒为一个
    /// [`STREAM_CHUNK_BYTES`] 块**(不随 `len` 增长)——用于大文件上传,避免「整个 blob 缓进内存」
    /// (CLAUDE.md §5.3)。
    ///
    /// 长度须**已知**(记录内 `data_len` 为 u32,故 `len ≤ u32::MAX`):HTTP 层用 `Content-Length`,
    /// 由调用方保证 `src` 恰好能提供 `len` 字节(不足则 `read_exact` 报 IO 错,该次写不发布)。
    /// 语义与 [`put_ttl`](Self::put_ttl) 完全一致(AlreadyExists / StoreFull / 索引与统计更新);
    /// 崩溃语义亦一致 —— 记录在 [`crate::log::Reserved::commit`] 前不对读者可见,恢复只认 header 里
    /// 上一次 flush 的 `data_end`。
    pub fn put_from_reader(
        &self,
        key: StoreKey,
        len: u64,
        src: &mut impl std::io::Read,
        expiration_ms: i64,
    ) -> Result<()> {
        if !self.is_available() {
            return Err(Error::Unavailable);
        }
        self.track(self.put_from_reader_inner(key, len, src, expiration_ms))
    }

    fn put_from_reader_inner(
        &self,
        key: StoreKey,
        len: u64,
        src: &mut impl std::io::Read,
        expiration_ms: i64,
    ) -> Result<()> {
        let _t = self.metrics.op_timer("put");
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        self.check_key(&key)?;
        // 与整块写路径共用同一条长度策略(记录内 data_len 是 u32):超限直接拒,**绝不写回绕的记录**。
        let data_len = record_data_len(len)?;
        let inner = self.inner();
        if inner.index.find(&key).is_some() {
            self.metrics.already_exists.inc();
            return Err(Error::AlreadyExists);
        }
        let mut header = [0u8; RECORD_HEADER];
        encode_header_into(&mut header, key.object_id(), 0, expiration_ms, data_len);
        let total = RECORD_OVERHEAD as u64 + len;
        // 预留整段后分块写入:crc 边写边算,最后补写尾部 crc 再 commit 发布。
        let reserved = inner.log.reserve(total).map_err(to_store_full)?;
        reserved.write(0, &header)?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        let mut buf = vec![0u8; STREAM_CHUNK_BYTES];
        let mut done = 0u64;
        while done < len {
            let want = std::cmp::min(STREAM_CHUNK_BYTES as u64, len - done) as usize;
            src.read_exact(&mut buf[..want])?;
            reserved.write(rec::DATA as u64 + done, &buf[..want])?;
            hasher.update(&buf[..want]);
            done += want as u64;
        }
        reserved.write(rec::DATA as u64 + len, &hasher.finalize().to_le_bytes())?;
        let offset = reserved.commit()?;
        self.publish_put(key, offset, record_size_u32(total)?, expiration_ms)
    }

    /// 记录已落 log 后的统一发布:索引 + 每段存活字节 + 有效数据统计。
    /// 整块写([`put_ttl`](Self::put_ttl))与流式写([`put_from_reader`](Self::put_from_reader))共用,
    /// 保证两条写路径的索引/统计语义逐字一致。
    fn publish_put(
        &self,
        key: StoreKey,
        offset: LogOffset,
        size: u32,
        expiration_ms: i64,
    ) -> Result<()> {
        let inner = self.inner();
        let mut value = IndexValue::put(offset, size);
        value.expiration_ms = expiration_ms;
        inner.index.add(key, value)?;
        self.seg_bytes
            .lock()
            .expect("blob seg_bytes poisoned")
            .entry(offset.segment)
            .or_default()
            .live += u64::from(size);
        self.valid_stats
            .lock()
            .expect("blob valid_stats poisoned")
            .add(&value);
        Ok(())
    }

    fn put_ttl_inner(&self, key: StoreKey, data: &[u8], expiration_ms: i64) -> Result<()> {
        let _t = self.metrics.op_timer("put");
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        self.check_key(&key)?;
        let inner = self.inner();
        if inner.index.find(&key).is_some() {
            self.metrics.already_exists.inc();
            return Err(Error::AlreadyExists);
        }
        let record = encode_record(&key, 0, expiration_ms, data)?;
        let offset = inner.log.append(&record).map_err(to_store_full)?;
        self.publish_put(
            key,
            offset,
            record_size_u32(record.len() as u64)?,
            expiration_ms,
        )
    }

    /// 读取一个对象。不存在/已删除/已过期分别返回对应错误。
    pub fn get(&self, key: &StoreKey) -> Result<Vec<u8>> {
        self.get_with_options(key, false)
    }

    /// `get` 的可选版本:`include_expired` 为真时返回**已过期**对象的数据(复制拉取需要 ——
    /// 过期但尚未被 compaction 回收的对象仍要被对端忠实 apply)。
    ///
    /// 已删除对象**无法**读取:删除不可逆,墓碑不保留原数据位置(对齐"删了就删了")。
    pub fn get_with_options(&self, key: &StoreKey, include_expired: bool) -> Result<Vec<u8>> {
        if !self.is_available() {
            return Err(Error::Unavailable);
        }
        self.track(self.get_with_options_inner(key, include_expired))
    }

    fn get_with_options_inner(&self, key: &StoreKey, include_expired: bool) -> Result<Vec<u8>> {
        let _t = self.metrics.op_timer("get");
        let inner = self.inner();
        let value = inner.index.find(key).ok_or(Error::NotFound)?;
        if value.is_deleted() {
            return Err(Error::Deleted);
        }
        if value.expiration_ms != NO_EXPIRATION
            && now_ms() as i64 >= value.expiration_ms
            && !include_expired
        {
            return Err(Error::Expired);
        }
        let (_, decoded) =
            read_full_record(&inner.log, value.offset).map_err(|_| Error::NotFound)?;
        if &decoded.key != key {
            return Err(Error::Corrupt(format!(
                "record key mismatch at {}",
                value.offset
            )));
        }
        if decoded.flags & FLAG_DELETED != 0 {
            return Err(Error::Corrupt(format!(
                "live index points at a tombstone record at {}",
                value.offset
            )));
        }
        Ok(decoded.data)
    }

    /// 查询 blob 元数据(状态/TTL/数据长度),**不读数据体**(对齐 Ambry `findKey`)。
    /// key 不存在报 [`NotFound`](crate::error::Error::NotFound);已删除/已过期仍返回其元数据。
    ///
    /// TTL 口径走 [`IndexValue::ttl_expiration_ms`]:墓碑的 `expiration_ms` 槽位存的是删除时刻,
    /// 不能当成过期时刻对外汇报。
    pub fn stat(&self, key: &StoreKey) -> Result<BlobMeta> {
        let v = self.inner().index.find(key).ok_or(Error::NotFound)?;
        let now = now_ms() as i64;
        Ok(BlobMeta {
            deleted: v.is_deleted(),
            expired: v.is_expired_at(now),
            expiration_ms: v.ttl_expiration_ms(),
            data_len: v.size_bytes().saturating_sub(RECORD_OVERHEAD as u64),
        })
    }

    /// 已用日志字节(存活 + 死亡),对齐 Ambry `getSizeInBytes`。
    pub fn size_in_bytes(&self) -> u64 {
        self.seg_bytes
            .lock()
            .expect("blob seg_bytes poisoned")
            .values()
            .map(|v| v.live + v.dead)
            .sum()
    }

    /// 库是否为空(尚无任何记录)。
    pub fn is_empty(&self) -> bool {
        self.size_in_bytes() == 0
    }

    /// 本 store(定容 replica)的容量上界(字节),= 构造时传入的 `total_capacity`。
    pub fn total_capacity(&self) -> u64 {
        self.total_capacity
    }

    /// 剩余可写字节的保守估计(总容量 − 已用),供上层判断是否该把新写路由到别处。
    pub fn available_capacity(&self) -> u64 {
        self.total_capacity.saturating_sub(self.size_in_bytes())
    }

    /// store 是否已写满:再放不下「一个段能容纳的最大记录」即视为满(对齐 Ambry 写满即只读、
    /// 由集群层滚动到新 partition)。粗判用于路由;精确容量约束仍由 log 追加时强制。
    pub fn is_full(&self) -> bool {
        self.available_capacity() < self.segment_capacity
    }

    /// key 是否为已删除状态(对齐 Ambry `isKeyDeleted`);不存在报
    /// [`NotFound`](crate::error::Error::NotFound)。
    pub fn is_key_deleted(&self, key: &StoreKey) -> Result<bool> {
        self.inner()
            .index
            .find(key)
            .map(|v| v.is_deleted())
            .ok_or(Error::NotFound)
    }

    /// 从给定 key 集合中返回本库**不存在**的(对齐 Ambry `findMissingKeys`,复制探测用)。
    pub fn find_missing_keys(&self, keys: &[StoreKey]) -> Vec<StoreKey> {
        let inner = self.inner();
        keys.iter()
            .filter(|k| inner.index.find(k).is_none())
            .copied()
            .collect()
    }

    /// 复制增量拉取(对齐 Ambry `findEntriesSince`):返回 `token` 之后、按 log 追加序、每个 key
    /// 取其**当前最新状态**去重的一批 [`MessageInfo`],累计大小达 `max_total_size` 即止,给出新游标。
    ///
    /// 路径:游标落在 journal 覆盖窗口内 → journal 快路径(免扫 log);否则(初次/滞后过久)→ 扫 log。
    /// 已追平 → 空条目 + 原游标。批内按 key 去重;跨批次同一 key 可能重复返回(复制侧幂等应用)。
    ///
    /// **第一步是 revalidate**(对齐 Ambry `PersistentIndex.findEntriesSince` 的
    /// `revalidateStoreFindToken`):游标必须能在**当前**段表里认出来,认不出就**重置为从头重扫**
    /// (方向是多拉,安全)。绝不把失效游标当成"已越过" —— 那会让调用方以为已经追上,静默漏数据。
    ///
    /// 返回的游标**自带段身份**(位置 + 代次,见 [`FindToken`])。副本级 incarnation 不在游标里 ——
    /// 那是**对端**的事实,由服务层/复制会话另外持有与比对(见 `store::server::replica_metadata`
    /// 与 `store::replication::PullSession`)。
    pub fn find_entries_since(&self, token: &FindToken, max_total_size: u64) -> Result<FindInfo> {
        let inner = self.inner();

        // 0) revalidate:段没了 / 段号被复用(代次不同)/ 偏移越界 ⇒ 从头重扫,并如实报告
        //    "本轮发生过重定位"(对齐 Ambry `revalidateStoreFindToken`:段名认不出即 `new StoreFindToken()`)。
        let (token, token_reset) = match token.segment_name() {
            None => (*token, None),
            Some(segment) => {
                let offset = token.offset().expect("Offset token has a position").offset;
                match revalidate_token(&inner.log, segment, offset) {
                    Ok(()) => (*token, None),
                    Err(reason) => (FindToken::Uninitialized, Some(reason)),
                }
            }
        };

        // 1) 取「游标之后的 (offset,key) 候选序列」,按 log 追加序;log 扫描按 max_total_size 限量。
        let candidates: Vec<(LogOffset, StoreKey)> = match token.offset() {
            Some(o) => match inner.index.journal_entries_since(o, false) {
                Some(j) => j,
                None => scan_candidates(&inner.log, Some(o), max_total_size)?,
            },
            None => scan_candidates(&inner.log, None, max_total_size)?,
        };

        // 2) 按序去重到「每 key 当前最新状态」,累计大小达上限即止,推进游标到最后审视的 offset。
        let mut entries = Vec::new();
        let mut seen: HashSet<StoreKey> = HashSet::new();
        let mut total = 0u64;
        let mut last: Option<LogOffset> = None;
        for (off, key) in candidates {
            last = Some(off);
            if !seen.insert(key) {
                continue;
            }
            if let Some(v) = inner.index.find(&key) {
                entries.push(MessageInfo {
                    key,
                    size: v.size_bytes(),
                    expiration_ms: v.expiration_ms,
                    deleted: v.is_deleted(),
                });
                total += v.size_bytes();
                if total >= max_total_size {
                    break;
                }
            }
        }

        // 本轮没有候选(已追平)= 游标停在原处;重定位过(且 log 已空)= Uninitialized,不假装有进展。
        // 游标带回**段身份**(位置 + 代次):代次从段表取,服务层无需再补。
        let new_token = match last.or(token.offset()) {
            Some(o) => {
                let generation = inner
                    .log
                    .generation_of(o.segment)
                    .expect("winning record lives in a segment of this log");
                FindToken::at(
                    LogSegmentName::new(o.segment, u64::from(generation)),
                    o.offset,
                )
            }
            None => FindToken::Uninitialized,
        };
        let bytes_read = bytes_read_to(&inner.log, &new_token);
        Ok(FindInfo {
            entries,
            token: new_token,
            bytes_read,
            token_reset,
        })
    }

    /// 忠实应用一条来自 peer 的复制记录(拉复制 apply,承 Ambry replication)。
    ///
    /// 合并规则(**墓碑恒胜**)—— 对象不可变 + 删除不可逆,故每个 key 的状态单调向前:
    ///
    /// | 本地现状 | peer 记录 | 结果 |
    /// |---|---|---|
    /// | 不存在 | PUT | 写入(数据体取自 peer,缺则 `Corrupt`) |
    /// | 不存在 | 墓碑 | 写入墓碑(确认该 key 已删除) |
    /// | PUT | PUT | `Skipped`(同一 key 内容不可变) |
    /// | PUT | 墓碑 | 写入墓碑(覆盖) |
    /// | 墓碑 | 任意 | `Skipped`(删除不可逆) |
    ///
    /// **只增不删**(幂等):同一批记录重复 apply、或部分失败后重试,都安全。
    pub fn apply_replicated(&self, rec: ReplicatedRecord) -> Result<ApplyOutcome> {
        if !self.is_available() {
            return Err(Error::Unavailable);
        }
        self.track(self.apply_replicated_inner(rec))
    }

    fn apply_replicated_inner(&self, rec: ReplicatedRecord) -> Result<ApplyOutcome> {
        let _t = self.metrics.op_timer("replicate");
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        self.check_key(&rec.key)?;
        let inner = self.inner();
        let cur = inner.index.find(&rec.key);

        let rec_deleted = rec.flags & FLAG_DELETED != 0;
        if let Some(c) = cur {
            // 墓碑恒胜:本地已是墓碑就无物可推进;本地是 PUT 时只有 peer 的墓碑能推进它。
            if c.is_deleted() || !rec_deleted {
                return Ok(ApplyOutcome::Skipped);
            }
        }

        // 墓碑无数据体;存活记录的数据体必须由 peer 带来(本地缺这条 key 时才可能走到这里)。
        let data = if rec_deleted {
            Vec::new()
        } else {
            rec.data.ok_or_else(|| {
                Error::Corrupt(
                    "apply_replicated: alive record missing data for an absent key".to_string(),
                )
            })?
        };

        let record = encode_record(&rec.key, rec.flags, rec.expiration_ms, &data)?;
        let offset = inner.log.append(&record).map_err(to_store_full)?;
        let mut value = IndexValue::put(offset, record_size_u32(record.len() as u64)?);
        value.flags = rec.flags;
        value.expiration_ms = rec.expiration_ms;
        inner.index.add(rec.key, value)?;

        let mut seg_bytes = self.seg_bytes.lock().expect("blob seg_bytes poisoned");
        seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        if let Some(c) = cur {
            let e = seg_bytes.entry(c.offset.segment).or_default();
            e.live = e.live.saturating_sub(c.size_bytes());
            e.dead += c.size_bytes();
        }
        drop(seg_bytes);
        let mut vs = self.valid_stats.lock().expect("blob valid_stats poisoned");
        if let Some(c) = cur {
            vs.remove(&c);
        }
        vs.add(&value);
        Ok(ApplyOutcome::Applied)
    }

    /// 有效数据字节:索引赢家中**未删除**记录的 `size` 之和(墓碑不计)。**不区分过期**(过期但
    /// 未回收对象仍占盘,故计入),故 ≠ `stats` 的 live 字节(后者含墓碑)。走增量计数,`O(1)` 查询;
    /// 正确性由 `#[cfg(test)]` 全扫基准 `valid_data_size_by_scan` 差分对拍守护。
    /// 本地完整性校验:逐记录读 log 并验 CRC,**遇到坏记录记下继续**(与生产读路径"坏即 Err"不同 ——
    /// 这里的目标是"告诉运维坏在哪个段/偏移",而不是让整副本的复制/重建失败)。
    ///
    /// 口径:
    /// - 坏记录后尝试从记录头读出 `data_len` 以重同步到下一条;头也读不出则本段止步并把 `truncated` 置真。
    /// - `index_mismatch`:对**活记录**(索引当前赢家,即 `find(key).offset == 本记录 offset`)比对索引里的
    ///   `size`/`flags` 与记录实际值,不一致即计 1。它只能发现"索引与 log 对不上"的这一类漂移,
    ///   **发现不了**"索引里指向了 log 中并不存在的 offset"(那需要遍历索引反向核对,属后续增强)。
    /// - `max_bytes` 是本次扫描的**读取字节上限**(到点返回部分结果 + `truncated=true`),供深扫限流用。
    pub fn verify(&self, max_bytes: u64) -> Result<VerifyReport> {
        let inner = self.inner();
        let mut report = VerifyReport::default();
        let mut read: u64 = 0;
        'segments: for (position, data_end) in inner.log.segment_spans() {
            let mut off = 0u64;
            while off < data_end {
                if read >= max_bytes {
                    report.truncated = true;
                    break 'segments;
                }
                let at = LogOffset::new(position, off);
                match read_full_record(&inner.log, at) {
                    Ok((raw, decoded)) => {
                        read += raw.len() as u64;
                        off += raw.len() as u64;
                        report.records += 1;
                        if let Some(v) = inner.index.find(&decoded.key) {
                            if v.offset == at
                                && (u64::from(v.size) != raw.len() as u64
                                    || v.flags != decoded.flags)
                            {
                                report.index_mismatch += 1;
                            }
                        }
                    }
                    Err(_) => {
                        report.bad_records += 1;
                        if report.first_bad_offset.is_none() {
                            report.first_bad_offset = Some((position, off));
                        }
                        // 从记录头重同步:读得出 data_len 就跳过这条坏记录,否则本段止步(尾部截断)。
                        match inner.log.read(at, RECORD_OVERHEAD) {
                            Ok(head) => {
                                let data_len = u32::from_le_bytes(
                                    head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap(),
                                ) as u64;
                                let skip = RECORD_OVERHEAD as u64 + data_len;
                                if skip == 0 || off + skip > data_end {
                                    report.truncated = true;
                                    break;
                                }
                                read += skip;
                                off += skip;
                            }
                            Err(_) => {
                                report.truncated = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(report)
    }

    /// 本副本 log 末端 `(segment, data_end)`(含活跃段);空 log → `None`。
    ///
    /// 供 scrub 报告"各副本各自读到哪儿"用:两侧末端一致才说明**可比**(否则是采样时点不同,差异未必是真不一致)。
    pub fn log_end(&self) -> Option<(u64, u64)> {
        self.inner().log.segment_spans().last().copied()
    }

    /// 本 log 用过的最大段代次(诊断/测试用;全库 compact 会把它推高,增量 compaction 只在同目录建新段)。
    pub fn max_segment_generation(&self) -> u64 {
        self.inner().log.max_generation()
    }

    /// 本副本的 **incarnation**(副本级坐标系身份)。
    ///
    /// 由 master 在"建这份副本"时颁发(单调、永不复用;单机模式退化为本地生成),落盘在
    /// `{store_dir}/LOG_INCARNATION`。复制侧用它判定"对面这份副本是不是我游标所在的那一代":
    /// 副本被重建、或换盘后重来 ⇒ 新号 ⇒ 旧游标一律作废。
    pub fn incarnation(&self) -> u64 {
        self.incarnation.load(Ordering::Acquire)
    }

    /// 采纳 master 颁发的 incarnation(与本地不同则改写并**作废此前发出的游标** —— 保守方向)。
    ///
    /// 只在新副本建库时调用;master 是唯一权威(它记得"这份副本该是第几代")。
    pub fn adopt_incarnation(&self, incarnation: u64) -> Result<()> {
        let previous = self.incarnation.swap(incarnation, Ordering::AcqRel);
        if previous != incarnation {
            write_incarnation(&self.dir, incarnation)?;
            tracing::info!(
                previous,
                current = incarnation,
                "replica incarnation adopted"
            );
        }
        Ok(())
    }

    /// 副本摘要(活记录数/墓碑数/活数据字节/封存段数);只走索引与 mmap,不读数据体。
    pub fn index_stats(&self) -> IndexStats {
        let inner = self.inner();
        let mut stats = IndexStats {
            sealed_segments: self.sealed_segments().len(),
            ..Default::default()
        };
        inner.index.for_each(|_k, v| {
            if v.is_deleted() {
                stats.tombstones += 1;
            } else {
                stats.live_records += 1;
                stats.live_bytes += v.size_bytes().saturating_sub(RECORD_OVERHEAD as u64);
            }
        });
        stats
    }

    pub fn valid_data_size(&self) -> u64 {
        self.valid_stats
            .lock()
            .expect("blob valid_stats poisoned")
            .bytes
    }

    /// 全扫 log 的有效数据字节基准(对拍 `valid_data_size` 的增量计数)。
    #[cfg(test)]
    fn valid_data_size_by_scan(&self) -> u64 {
        let inner = self.inner();
        let mut total = 0u64;
        for_each_record(&inner.log, |off, _raw, decoded| {
            if let Some(v) = inner.index.find(&decoded.key) {
                if v.offset == off && !v.is_deleted() {
                    total += v.size_bytes();
                }
            }
            Ok(())
        })
        .unwrap();
        total
    }

    /// 删除一个对象:追加墓碑记录并在索引写入删除条目。
    /// 不存在报 NotFound;已删除(墓碑)幂等成功(不写第二条墓碑),对应 HTTP 204。
    ///
    /// 墓碑记录索引条目与记录头都带**删除时刻**(见 [`IndexValue::deleted_at_ms`]):compaction 只在
    /// 删除满保留期后才回收墓碑,时间戳随复制传给各副本(故各副本的回收判定一致)。
    pub fn delete(&self, key: StoreKey) -> Result<()> {
        if !self.is_available() {
            return Err(Error::Unavailable);
        }
        self.track(self.delete_inner(key))
    }

    fn delete_inner(&self, key: StoreKey) -> Result<()> {
        let _t = self.metrics.op_timer("delete");
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        self.check_key(&key)?;
        let inner = self.inner();
        let old = match inner.index.find(&key) {
            None => return Err(Error::NotFound),
            // 幂等:墓碑已是终态,重复删视为成功(204);不再写第二条墓碑,索引/统计不变。
            Some(v) if v.is_deleted() => return Ok(()),
            Some(v) => v,
        };
        let deleted_at_ms = now_ms() as i64;
        // 墓碑的 `expiration` 槽位存**删除时刻**(非 TTL):保留期判定与复制的删除时间传播都靠它。
        let record = encode_record(&key, FLAG_DELETED, deleted_at_ms, &[])?;
        let offset = inner.log.append(&record).map_err(to_store_full)?;
        let mut value = IndexValue::put(offset, record_size_u32(record.len() as u64)?);
        value.flags = FLAG_DELETED;
        value.expiration_ms = deleted_at_ms;
        inner.index.add(key, value)?;
        // 墓碑成为新赢家(live);被取代的旧 PUT 转为该段的死数据。
        let mut seg_bytes = self.seg_bytes.lock().expect("blob seg_bytes poisoned");
        seg_bytes.entry(offset.segment).or_default().live += record.len() as u64;
        let e = seg_bytes.entry(old.offset.segment).or_default();
        e.live = e.live.saturating_sub(old.size_bytes());
        e.dead += old.size_bytes();
        // 有效数据:旧赢家(未删)移除,新墓碑不计。
        let mut vs = self.valid_stats.lock().expect("blob valid_stats poisoned");
        vs.remove(&old);
        vs.add(&value);
        Ok(())
    }

    /// 持久化:先落数据(fsync)再落索引,最后写持久化水位(= 当前 log 末尾)。
    pub fn flush(&self) -> Result<()> {
        let _t = self.metrics.op_timer("flush");
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        self.flush_inner(&self.inner())?;
        self.refresh_gauges();
        Ok(())
    }

    /// 持久化实现(调用方须已持 `write_lock`);compaction 内部复用,避免重入死锁。
    fn flush_inner(&self, inner: &Inner) -> Result<()> {
        inner.log.flush()?;
        inner.index.flush()?;
        let (_, index_dir) = sub_dirs(&self.dir);
        write_watermark(&index_dir, inner.log.end_offset())?;
        Ok(())
    }

    /// flush 后关闭底层日志文件。
    pub fn close(self) -> Result<()> {
        {
            let _w = self.write_lock.lock().expect("blob write_lock poisoned");
            self.flush_inner(&self.inner())?;
        }
        // 消费 self:取出 Inner;残余 Arc 引用(读者)由其末次释放时关文件。
        let inner = self.inner.into_inner().expect("blob inner poisoned");
        if let Ok(inner) = Arc::try_unwrap(inner) {
            inner.log.close()?;
        }
        Ok(())
    }

    /// 仅持久化数据日志(推进 log header 的 data_end),**不**落索引/水位。用于测试模拟
    /// 「数据已落盘、索引仍停在旧水位」的崩溃前状态。
    #[cfg(test)]
    fn log_flush_only_for_test(&self) {
        self.inner().log.flush().unwrap();
    }

    #[cfg(test)]
    fn log_segment_count_for_test(&self) -> usize {
        self.inner().log.segment_count()
    }

    #[cfg(test)]
    fn expiration_of(&self, key: &StoreKey) -> Option<i64> {
        self.inner().index.find(key).map(|v| v.expiration_ms)
    }

    /// 返回全库存活/死亡字节统计(读增量计数,死 = 被更新版本取代/删除的旧记录)。
    /// `stats` 是**全量口径**(不含 TTL/墓碑的可回收判定),回收构成见各次 compaction 的返回值。
    pub fn stats(&self) -> Result<CompactionStats> {
        let mut live = 0u64;
        let mut dead = 0u64;
        for v in self
            .seg_bytes
            .lock()
            .expect("blob seg_bytes poisoned")
            .values()
        {
            live += v.live;
            dead += v.dead;
        }
        let segments = self.inner().log.segment_count();
        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            tombstone_reclaimed_bytes: 0,
            expired_reclaimed_bytes: 0,
            segments_before: segments,
            segments_after: segments,
        })
    }

    #[cfg(test)]
    fn stats_by_scan(&self) -> CompactionStats {
        let inner = self.inner();
        let m = scan_seg_bytes(&inner.log, &inner.index).unwrap();
        let (mut live, mut dead) = (0u64, 0u64);
        for v in m.values() {
            live += v.live;
            dead += v.dead;
        }
        let segments = inner.log.segment_count();
        CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            tombstone_reclaimed_bytes: 0,
            expired_reclaimed_bytes: 0,
            segments_before: segments,
            segments_after: segments,
        }
    }

    /// 每段的 `(总字节, 可回收字节)` —— compaction 的闸门与段选择共用这一份口径。
    ///
    /// - 总字节 = `seg_bytes` 的 `live + dead`;
    /// - 可回收 = 被更新版本取代的旧记录(`seg_bytes.dead`)+ 该段内**已过期 / 墓碑已过保留期**的
    ///   索引赢家字节(后者仍记在 `seg_bytes.live` 里,故只看死字节计数看不见 TTL 与墓碑的空间)。
    ///
    /// 只走索引(内存 + mmap),**不读数据体**,开销与 key 数成正比。
    fn reclaimable_by_segment(
        &self,
        inner: &Inner,
        policy: &CompactPolicy,
    ) -> BTreeMap<u64, (u64, u64)> {
        let mut m: BTreeMap<u64, (u64, u64)> = self
            .seg_bytes
            .lock()
            .expect("blob seg_bytes poisoned")
            .iter()
            .map(|(&pos, v)| (pos, (v.live + v.dead, v.dead)))
            .collect();
        inner.index.for_each(|_k, v| {
            if policy.is_reclaimable(&v) {
                let e = m.entry(v.offset.segment).or_insert((0, 0));
                e.1 += v.size_bytes();
            }
        });
        m
    }

    /// 全库可回收字节占比(partition 级闸门:低于阈值连段选择都不必做)。
    fn reclaimable_ratio(&self, inner: &Inner, policy: &CompactPolicy) -> f64 {
        let m = self.reclaimable_by_segment(inner, policy);
        let total: u64 = m.values().map(|(t, _)| *t).sum();
        let reclaimable: u64 = m.values().map(|(_, r)| *r).sum();
        if total == 0 {
            0.0
        } else {
            reclaimable as f64 / total as f64
        }
    }

    /// 周期性 GC 入口:先过 **partition 级闸门**(全库可回收占比 ≥ `policy.dead_ratio` 才动手,
    /// 免得小比例回收白跑一趟),再对达阈的**非活跃段**增量压缩(热/末段不动),
    /// 对齐 Ambry StatsBased 的按段触发而非全库 CompactAll。无达阈段返回 `None`。
    ///
    /// 可回收口径 = 被取代的旧记录 + TTL 已过期对象 + 墓碑已过保留期(见 [`CompactPolicy`])。
    /// 需要全库重写时显式调用 [`compact`](Self::compact)。
    pub fn compact_if_needed(&self, policy: &CompactPolicy) -> Result<Option<CompactionStats>> {
        if self.reclaimable_ratio(&self.inner(), policy) < policy.dead_ratio {
            return Ok(None);
        }
        self.compact_incremental(policy)
    }

    /// CompactAll:重写整个 log,只保留存活记录(newest-wins 赢家),重建索引,原子换入。
    ///
    /// 「存活」按 `policy` 判定:被取代的旧记录、TTL 已过期的对象、墓碑已过保留期的条目都**不复入**
    /// 新 log(即真正回收),其余包括保留期内的墓碑照常搬运(它仍是该 key 的赢家)。
    ///
    /// 崩溃安全:先建 `log.new` 并 fsync → 写提交标记 → 目录 rename 换入(log→log.old,log.new→log)
    /// → 重建索引 → 删标记与 log.old。任一步崩溃,`open()` 依据标记修复(见 `recover_compaction`)。
    pub fn compact(&self, policy: &CompactPolicy) -> Result<CompactionStats> {
        let _t = self.metrics.compaction_duration.start_timer();
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        let old = self.inner();
        self.flush_inner(&old)?;

        let before = old.log.segment_count();
        let (log_dir, index_dir) = sub_dirs(&self.dir);
        let log_new = self.dir.join("log.new");
        let log_old = self.dir.join("log.old");
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;

        // COPY:扫旧 log,存活记录(当前赢家且未过期/未过保留期)重写进 log.new。
        //
        // **段代次接着旧 log 往后取**(位置从 0 重来):于是新目录里的段身份与旧目录**绝不重复**,
        // 旧游标必然"认不出自己的段"(见 `docs/design/store.md` 的"复制游标重定位")。
        let next_generation = old.log.max_generation() + 1;
        let new_log = Log::create(
            &log_new,
            self.total_capacity,
            self.segment_capacity,
            self.key_prefix,
            next_generation,
        )?;
        let mut live = 0u64;
        let mut dead = 0u64;
        let mut tombstones = 0u64;
        let mut expired = 0u64;
        for_each_record(&old.log, |off, raw, decoded| {
            let winner = match old.index.find(&decoded.key) {
                Some(v) if v.offset == off => Some(v),
                _ => None,
            };
            match winner {
                // 过期对象 / 过保留期墓碑:不予搬运 → 字节真正释放。
                Some(v) if policy.is_reclaimable(&v) => {
                    if v.is_deleted() {
                        tombstones += raw.len() as u64;
                    } else {
                        expired += raw.len() as u64;
                    }
                    dead += raw.len() as u64;
                }
                Some(_) => {
                    new_log.append(raw)?;
                    live += raw.len() as u64;
                }
                None => dead += raw.len() as u64,
            }
            Ok(())
        })?;
        new_log.flush()?;
        let after = new_log.segment_count();
        new_log.close()?;

        // COMMIT:写标记 → 换入。标记在,open() 会保证 log 是 compacted 的那份并重建索引。
        // Unix 下 rename 目录不受内部已打开 fd 影响,直接换入即可。
        let marker = self.dir.join("COMPACT_COMMIT");
        write_marker(&marker)?;
        std::fs::rename(&log_dir, &log_old)?;
        std::fs::rename(&log_new, &log_dir)?;

        // 重建索引(offset 已变)→ 原子换入新 Inner。老 Inner(读者持有)fd 仍指向已 rename 的
        // 旧文件,Unix 下不受影响;末次释放后 log.old 的 inode 才回收。
        let new_log = Log::open(&log_dir, self.total_capacity, self.segment_capacity)?;
        let new_index = rebuild_index(&new_log, &index_dir, self.max_index_entries_per_segment)?;
        let new_seg_bytes = scan_seg_bytes(&new_log, &new_index)?;
        let new_valid = compute_valid_stats(&new_log, &new_index)?;
        *self.seg_bytes.lock().expect("blob seg_bytes poisoned") = new_seg_bytes;
        *self.valid_stats.lock().expect("blob valid_stats poisoned") = new_valid;
        *self.inner.write().expect("blob inner poisoned") = Arc::new(Inner {
            log: new_log,
            index: new_index,
        });
        let _ = std::fs::remove_file(&marker);
        rm_rf(&log_old)?;

        self.metrics.compaction_reclaimed_bytes.inc_by(dead);
        self.refresh_gauges();
        Ok(CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            tombstone_reclaimed_bytes: tombstones,
            expired_reclaimed_bytes: expired,
            segments_before: before,
            segments_after: after,
        })
    }

    /// 每个封存段(不含活跃/末段)的 `(position, live_bytes, dead_bytes)`,读增量计数。
    pub fn segment_stats(&self) -> Result<Vec<(u64, u64, u64)>> {
        let active_pos = self.inner().log.segment_spans().last().map(|(p, _)| *p);
        let mut out = Vec::new();
        for (&pos, v) in self
            .seg_bytes
            .lock()
            .expect("blob seg_bytes poisoned")
            .iter()
        {
            if Some(pos) == active_pos {
                continue;
            }
            out.push((pos, v.live, v.dead));
        }
        Ok(out)
    }

    /// 增量 compaction:只压缩**可回收占比 ≥ `policy.dead_ratio`** 的**非活跃**段(热/末段不动)。
    /// 执行机制与崩溃安全见 [`compact_segments`](Self::compact_segments)。无达阈段返回 `None`。
    pub fn compact_incremental(&self, policy: &CompactPolicy) -> Result<Option<CompactionStats>> {
        let _t = self.metrics.compaction_duration.start_timer();
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        let old = self.inner();
        self.flush_inner(&old)?;
        let active_pos = old.log.segment_spans().last().map(|(p, _)| *p);
        let selected: Vec<u64> = self
            .reclaimable_by_segment(&old, policy)
            .into_iter()
            .filter(|(pos, (total, reclaimable))| {
                Some(*pos) != active_pos
                    && *total > 0
                    && (*reclaimable as f64 / *total as f64) >= policy.dead_ratio
            })
            .map(|(pos, _)| pos)
            .collect();
        if selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.run_compact_segments(&old, &selected, policy)?))
    }

    /// cost-benefit compaction(对齐 Ambry `StatsBasedCompactionPolicy`):在非活跃段的**连续区间**里,
    /// 按「cost = 需搬走的存活字节、benefit = 回收段数 = 源段数 − ceil(cost/段容量)」选 cost/benefit
    /// 最优且 `benefit ≥ min_reclaim` 的区间压缩。无合格区间返回 `None`。
    ///
    /// `cost` 用**剔除可回收字节后的存活量**:过期对象/过保留期墓碑本就不必搬运,不计入成本。
    pub fn compact_cost_benefit(
        &self,
        policy: &CompactPolicy,
        min_reclaim: usize,
    ) -> Result<Option<CompactionStats>> {
        let _t = self.metrics.compaction_duration.start_timer();
        let _w = self.write_lock.lock().expect("blob write_lock poisoned");
        let old = self.inner();
        self.flush_inner(&old)?;
        let selected = self.best_compaction_range(&old, policy, min_reclaim.max(1));
        if selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.run_compact_segments(&old, &selected, policy)?))
    }

    /// 在非活跃段的连续区间中选 cost/benefit 最优区间(`benefit ≥ min_reclaim`),返回段位置列表。
    fn best_compaction_range(
        &self,
        old: &Inner,
        policy: &CompactPolicy,
        min_reclaim: usize,
    ) -> Vec<u64> {
        let active = old.log.segment_spans().last().map(|(p, _)| *p);
        let segs: Vec<(u64, u64)> = self
            .reclaimable_by_segment(old, policy)
            .into_iter()
            .filter(|(p, _)| Some(*p) != active)
            .map(|(p, (total, reclaimable))| (p, total.saturating_sub(reclaimable)))
            .collect();
        let cap = self.segment_capacity.max(1);
        let mut best: Option<(f64, usize, usize, usize)> = None; // (ratio, benefit, i, j)
        for i in 0..segs.len() {
            let mut cost = 0u64;
            for (j, seg) in segs.iter().enumerate().skip(i) {
                cost += seg.1;
                let source = j - i + 1;
                let target = cost.div_ceil(cap) as usize;
                if source <= target {
                    continue;
                }
                let benefit = source - target;
                if benefit < min_reclaim {
                    continue;
                }
                let ratio = cost as f64 / benefit as f64;
                let better =
                    best.is_none_or(|(br, bb, _, _)| ratio < br || (ratio == br && benefit > bb));
                if better {
                    best = Some((ratio, benefit, i, j));
                }
            }
        }
        match best {
            Some((_, _, i, j)) => segs[i..=j].iter().map(|(p, _)| *p).collect(),
            None => Vec::new(),
        }
    }

    /// 包装 [`compact_segments`](Self::compact_segments):成功后记录回收字节并刷新容量 gauge。
    fn run_compact_segments(
        &self,
        old: &Inner,
        selected: &[u64],
        policy: &CompactPolicy,
    ) -> Result<CompactionStats> {
        let stats = self.compact_segments(old, selected, policy)?;
        self.metrics
            .compaction_reclaimed_bytes
            .inc_by(stats.dead_bytes);
        self.refresh_gauges();
        Ok(stats)
    }

    /// 压缩给定**非活跃段**集合 `selected`(调用方须已持 `write_lock`,`old` 为其快照):
    /// 存活记录写入位置更高的新段(temp→fsync→rename),删旧段,重建索引后原子换入新 Inner。
    /// 存活赢家搬到更高位置仍是其 key 最新记录,newest-wins 不变。崩溃安全:复用 `COMPACT_COMMIT`
    /// 标记 + open 重建索引;残留 `.tmp`/重复记录在重建时被忽略/收敛。
    ///
    /// 「存活」按 `policy` 判定:过期对象与过保留期墓碑**不搬**(回收),保留期内的墓碑照搬。
    fn compact_segments(
        &self,
        old: &Inner,
        selected: &[u64],
        policy: &CompactPolicy,
    ) -> Result<CompactionStats> {
        let before = old.log.segment_count();
        let (log_dir, index_dir) = sub_dirs(&self.dir);
        let marker = self.dir.join("COMPACT_COMMIT");
        write_marker(&marker)?;

        let mut next_pos = old
            .log
            .segment_spans()
            .iter()
            .map(|(p, _)| *p)
            .max()
            .unwrap()
            + 1;
        // 新段代次接着旧 log 的最大值往后 —— 段身份(位置 + 代次)永不复用。
        let mut next_gen = old.log.max_generation() + 1;
        let mut live_bytes = 0u64;
        let mut dead_bytes = 0u64;
        let mut tombstone_bytes = 0u64;
        let mut expired_bytes = 0u64;
        let mut tmp_paths: Vec<(PathBuf, PathBuf)> = Vec::new(); // (tmp, final)
        let mut cur: Option<LogSegment> = None;

        for &pos in selected {
            let data_end = old
                .log
                .segment_spans()
                .iter()
                .find(|(p, _)| *p == pos)
                .map(|(_, e)| *e)
                .unwrap_or(0);
            scan_segment(&old.log, pos, data_end, |off, raw, decoded| {
                let winner = match old.index.find(&decoded.key) {
                    Some(v) if v.offset == off => Some(v),
                    _ => None,
                };
                match winner {
                    // 被取代的旧记录:本就不是赢家。
                    None => {
                        dead_bytes += raw.len() as u64;
                        return Ok(());
                    }
                    // 过期对象 / 过保留期墓碑:不予搬运 → 字节真正释放。
                    Some(v) if policy.is_reclaimable(&v) => {
                        if v.is_deleted() {
                            tombstone_bytes += raw.len() as u64;
                        } else {
                            expired_bytes += raw.len() as u64;
                        }
                        dead_bytes += raw.len() as u64;
                        return Ok(());
                    }
                    Some(_) => {}
                }
                live_bytes += raw.len() as u64;
                let need = raw.len() as u64;
                let fits = cur.as_ref().map(|c| c.remaining() >= need).unwrap_or(false);
                if !fits {
                    if let Some(c) = cur.take() {
                        c.flush()?;
                        c.close()?;
                    }
                    let final_path =
                        log_dir.join(LogSegmentName::new(next_pos, next_gen).filename());
                    let mut tmp = final_path.as_os_str().to_owned();
                    tmp.push(".tmp");
                    let tmp = PathBuf::from(tmp);
                    next_pos += 1;
                    next_gen += 1;
                    cur = Some(LogSegment::create(
                        &tmp,
                        self.segment_capacity,
                        self.key_prefix,
                        u32::try_from(next_gen - 1).expect("generation must fit u32"),
                    )?);
                    tmp_paths.push((tmp, final_path));
                }
                cur.as_ref().unwrap().append(raw)?;
                Ok(())
            })?;
        }
        if let Some(c) = cur.take() {
            c.flush()?;
            c.close()?;
        }

        // temp 新段 → 正式名(atomic rename)。
        for (tmp, final_path) in &tmp_paths {
            std::fs::rename(tmp, final_path)?;
        }
        // 删被压缩的旧段(按 (位置, 代次) 定位文件)。老 Inner 读者的 fd 仍有效(Unix),
        // 末次释放后 inode 才回收。
        for &pos in selected {
            let Some(generation) = old.log.generation_of(pos) else {
                continue;
            };
            let _ = std::fs::remove_file(
                log_dir.join(LogSegmentName::new(pos, u64::from(generation)).filename()),
            );
        }

        let new_log = Log::open(&log_dir, self.total_capacity, self.segment_capacity)?;
        let new_index = rebuild_index(&new_log, &index_dir, self.max_index_entries_per_segment)?;
        let new_seg_bytes = scan_seg_bytes(&new_log, &new_index)?;
        let new_valid = compute_valid_stats(&new_log, &new_index)?;
        let segments_after = new_log.segment_count();
        *self.seg_bytes.lock().expect("blob seg_bytes poisoned") = new_seg_bytes;
        *self.valid_stats.lock().expect("blob valid_stats poisoned") = new_valid;
        *self.inner.write().expect("blob inner poisoned") = Arc::new(Inner {
            log: new_log,
            index: new_index,
        });
        let _ = std::fs::remove_file(&marker);

        Ok(CompactionStats {
            live_bytes,
            dead_bytes,
            tombstone_reclaimed_bytes: tombstone_bytes,
            expired_reclaimed_bytes: expired_bytes,
            segments_before: before,
            segments_after,
        })
    }
}

fn sub_dirs(dir: &Path) -> (PathBuf, PathBuf) {
    (dir.join("log"), dir.join("index"))
}

/// 副本 incarnation 文件名(内容 = u64 文本)。见 [`BlobStore::incarnation`]。
pub const LOG_INCARNATION_FILE: &str = "LOG_INCARNATION";

/// 读 `{store_dir}/LOG_INCARNATION`;缺失/不可解析 → `None`。
fn read_incarnation(dir: &Path) -> Option<u64> {
    std::fs::read_to_string(dir.join(LOG_INCARNATION_FILE))
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// 写 `{store_dir}/LOG_INCARNATION` 并 fsync。
fn write_incarnation(dir: &Path, incarnation: u64) -> Result<()> {
    use std::io::Write;
    let path = dir.join(LOG_INCARNATION_FILE);
    let mut f = std::fs::File::create(&path)?;
    f.write_all(incarnation.to_string().as_bytes())?;
    f.sync_all()?;
    Ok(())
}

/// 打开副本目录时确定 incarnation:优先读文件;文件缺失(物理 bootstrap 只拷段、或换盘后重来)时
/// 用**本机当前时刻**兜底并落盘,master 随后可用 [`BlobStore::adopt_incarnation`] 纠正为权威值。
///
/// **绝不用段头里的 `created_ns` 兜底**(曾经如此):物理 bootstrap 拷来的段诞生在**对端**,那个时刻
/// 正是对端自己的兜底身份 —— 拿它当本副本的身份,重建两次就可能与对端**撞号**,于是"重建 ⇒ 身份必换"
/// 这条保证在"申领没成功"时失效(对端手里的旧游标会看似合法)。用本机时钟则身份永远是**本机独有**的:
/// 重建必得新号,重启重开因文件已落盘而保持不变。
fn ensure_incarnation(store_dir: &Path) -> Result<u64> {
    if let Some(v) = read_incarnation(store_dir) {
        return Ok(v);
    }
    let derived = now_ns().max(1);
    write_incarnation(store_dir, derived)?;
    Ok(derived)
}

/// `expected` 非空时校验盘上副本的 key 前缀正是它 —— 不符即 [`Corrupt`](Error::Corrupt)。
///
/// 用在"打开一个已存在的副本"的三条路径上(`open_with_partition` / `open_rebuild_index_with_partition`):
/// 分区目录放错位置或被替换时,必须大声失败,而不是按错误的前缀去解释盘上的记录。
fn check_partition(store: &BlobStore, expected: Option<u32>) -> Result<()> {
    if let Some(want) = expected {
        if store.key_prefix != want {
            return Err(Error::Corrupt(format!(
                "replica on disk holds partition {:#x} but {want:#x} was expected",
                store.key_prefix
            )));
        }
    }
    Ok(())
}

/// 写路径把「log 追加因段用尽/无法滚动」的 `SegmentFull` 归一为面向调用方的 `StoreFull`;其余原样。
fn to_store_full(e: Error) -> Error {
    match e {
        Error::SegmentFull => Error::StoreFull,
        other => other,
    }
}

/// 顺序遍历 Log 全部记录,回调 `(offset, 完整记录字节, 解码结果)`。
fn for_each_record(
    log: &Log,
    mut f: impl FnMut(LogOffset, &[u8], &DecodedRecord) -> Result<()>,
) -> Result<()> {
    for (position, data_end) in log.segment_spans() {
        scan_segment(log, position, data_end, &mut f)?;
    }
    Ok(())
}

/// 读取 `at` 处一条完整记录(先读头拿 data_len,再读整条并校验 crc)。
///
/// key 的前缀来自段头(`log.key_prefix()`),记录体里只有低 32bit 后缀。
fn read_full_record(log: &Log, at: LogOffset) -> Result<(Vec<u8>, DecodedRecord)> {
    let head = log.read(at, rec::DATA)?;
    let data_len =
        u32::from_le_bytes(head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().unwrap()) as usize;
    let total = RECORD_OVERHEAD + data_len;
    let raw = log.read(at, total)?;
    let decoded = decode_record(&raw, log.key_prefix())?;
    Ok((raw, decoded))
}

/// 对齐 Ambry `revalidateStoreFindToken`:游标必须能在**当前**段表里认出来。
///
/// - 段不在表里(compaction 删了它 / 对方整体重写过 log)→ [`TokenResetReason::SegmentMissing`];
/// - 偏移越过该段数据区 → [`TokenResetReason::OffsetOutOfRange`]。
///
/// 认不出即"重置为从头重扫",绝不把失效游标当成"已越过"(那会静默漏数据)。
fn revalidate_token(
    log: &Log,
    segment: LogSegmentName,
    offset: u64,
) -> std::result::Result<(), TokenResetReason> {
    match log
        .segment_spans()
        .iter()
        .find(|(pos, _)| *pos == u64::from(segment.position))
    {
        None => Err(TokenResetReason::SegmentMissing),
        // 位置还在、代次不同 ⇒ 段号被复用(全库 compact 后位置从 0 重来)⇒ 游标作废。
        Some(_) if log.generation_of(u64::from(segment.position)) != Some(segment.generation) => {
            Err(TokenResetReason::SegmentReplaced)
        }
        Some((_, data_end)) if offset > *data_end => Err(TokenResetReason::OffsetOutOfRange),
        Some(_) => Ok(()),
    }
}

/// 扫 log 收集 `start` 之后(exclusive;`None`=从头 inclusive)按追加序的 `(offset, key)`,
/// 累计记录字节达 `max_total_size` 即止(上界近似,批内去重在调用方)。避免滞后拉取每次全扫 log。
///
/// **调用方必须先 [`revalidate_token`]**:`start` 所在段必须存在且偏移不越界(同一 `Inner` 快照内段表
/// 不会变)。这里**不再**把"段不存在"静默当成"已越过"——那样会让失效游标看起来已经追上。
fn scan_candidates(
    log: &Log,
    start: Option<LogOffset>,
    max_total_size: u64,
) -> Result<Vec<(LogOffset, StoreKey)>> {
    let mut out = Vec::new();
    let mut acc = 0u64;
    let spans = log.segment_spans();
    let begin_at = match start {
        None => 0,
        Some(s) => spans
            .iter()
            .position(|(pos, _)| *pos == s.segment)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "scan start segment {} is not in the log (cursor not revalidated)",
                    s.segment
                ))
            })?,
    };
    for (i, (pos, data_end)) in spans.iter().enumerate().skip(begin_at) {
        let begin = match start {
            Some(s) if i == begin_at => s.offset,
            _ => 0,
        };
        let mut off = begin;
        while off < *data_end {
            let at = LogOffset::new(*pos, off);
            let (raw, decoded) = read_full_record(log, at)?;
            off += raw.len() as u64;
            if start != Some(at) {
                out.push((at, decoded.key));
                acc += raw.len() as u64;
                if acc >= max_total_size {
                    return Ok(out);
                }
            }
        }
    }
    Ok(out)
}

/// 从 log 起点到 `token` 游标的累计字节(复制滞后度量):游标段之前的段全计 + 段内到游标 offset。
fn bytes_read_to(log: &Log, token: &FindToken) -> u64 {
    let Some(o) = token.offset() else {
        return 0;
    };
    log.segment_spans()
        .iter()
        .map(|(pos, data_end)| {
            if *pos < o.segment {
                *data_end
            } else if *pos == o.segment {
                o.offset
            } else {
                0
            }
        })
        .sum()
}

/// 遍历单个段 `position` 内 `[0, data_end)` 的全部记录。
fn scan_segment(
    log: &Log,
    position: u64,
    data_end: u64,
    mut f: impl FnMut(LogOffset, &[u8], &DecodedRecord) -> Result<()>,
) -> Result<()> {
    let mut off = 0u64;
    while off < data_end {
        let at = LogOffset::new(position, off);
        let (raw, decoded) = read_full_record(log, at)?;
        off += raw.len() as u64;
        f(at, &raw, &decoded)?;
    }
    Ok(())
}

/// 从一条记录构造索引值(记录自描述 flags/expiration)。
///
/// `raw_len` 是整条记录的字节数;段 ≤ 4 GiB ⇒ 它必然放得进索引的 u32 `size` 字段。
fn index_value_from_record(off: LogOffset, raw_len: usize, decoded: &DecodedRecord) -> IndexValue {
    let mut value = IndexValue::put(
        off,
        u32::try_from(raw_len).expect("record length must fit u32 (segment ≤ 4 GiB)"),
    );
    value.flags = decoded.flags;
    value.expiration_ms = decoded.expiration_ms;
    value
}

/// 全量扫 log 计算每段 `(live, dead)` 字节。冷路径:open/compact/rebuild 后重算,及测试对拍基准。
fn scan_seg_bytes(log: &Log, index: &PersistentIndex) -> Result<BTreeMap<u64, SegBytes>> {
    Ok(scan_seg_bytes_tolerant(log, index)?.0)
}

/// 容错版段字节核算:**坏记录跳过**(既不记 live 也不记 dead —— 其字节归属未知),
/// 并把它作为 [`OpenDamage`] 返回给调用方去大声报错。
fn scan_seg_bytes_tolerant(
    log: &Log,
    index: &PersistentIndex,
) -> Result<(BTreeMap<u64, SegBytes>, OpenDamage)> {
    let mut m: BTreeMap<u64, SegBytes> = BTreeMap::new();
    for (pos, _) in log.segment_spans() {
        m.entry(pos).or_default();
    }
    let mut damage = OpenDamage::default();
    for (position, data_end) in log.segment_spans() {
        let mut off = 0u64;
        while off < data_end {
            let at = LogOffset::new(position, off);
            match read_full_record(log, at) {
                Ok((raw, decoded)) => {
                    let e = m.entry(position).or_default();
                    if matches!(index.find(&decoded.key), Some(v) if v.offset == at) {
                        e.live += raw.len() as u64;
                    } else {
                        e.dead += raw.len() as u64;
                    }
                    off += raw.len() as u64;
                }
                Err(_) => {
                    damage.bad_records += 1;
                    if damage.first_bad_offset.is_none() {
                        damage.first_bad_offset = Some((position, off));
                    }
                    match resync_skip(log, at, data_end) {
                        Some(skip) => off += skip,
                        None => {
                            damage.abandoned_segments += 1;
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok((m, damage))
}

/// 坏记录后按记录头重同步:读得出**可信的** `data_len` 就返回跳过长度,否则 `None`(本段剩余部分放弃)。
///
/// "可信" 必须同时满足两条,否则长度字段本身就不可信、继续往下扫只会扫出垃圾:
/// 1. 跳过长度大于记录头(即 `data_len > 0` 之外还要求长度非零);
/// 2. **跳过后不越过段尾**(`off + skip <= data_end`)—— 少了这条,一个被写坏的长度字段会把
///    "本段剩余内容"静默带走却不算"放弃",等于把损伤瞒下来。
fn resync_skip(log: &Log, at: LogOffset, data_end: u64) -> Option<u64> {
    let head = log.read(at, RECORD_OVERHEAD).ok()?;
    let data_len = u32::from_le_bytes(head[rec::DATA_LEN..rec::DATA_LEN + 4].try_into().ok()?);
    let skip = RECORD_OVERHEAD as u64 + data_len as u64;
    (skip > RECORD_OVERHEAD as u64 && at.offset + skip <= data_end).then_some(skip)
}

/// 全量扫 log 重算有效数据核算([`ValidStats`]):对每个索引赢家记录累加。冷路径:open/compact/
/// rebuild 后重建,及测试对拍基准。
fn compute_valid_stats(log: &Log, index: &PersistentIndex) -> Result<ValidStats> {
    let mut stats = ValidStats::default();
    for (position, data_end) in log.segment_spans() {
        let mut off = 0u64;
        while off < data_end {
            let at = LogOffset::new(position, off);
            match read_full_record(log, at) {
                Ok((raw, decoded)) => {
                    if let Some(v) = index.find(&decoded.key) {
                        if v.offset == at {
                            stats.add(&v);
                        }
                    }
                    off += raw.len() as u64;
                }
                // 坏记录:其字节归属未知,跳过(损伤由 `scan_seg_bytes_tolerant` 统一上报)。
                Err(_) => match resync_skip(log, at, data_end) {
                    Some(skip) => off += skip,
                    None => break,
                },
            }
        }
    }
    Ok(stats)
}

/// 清空索引目录并顺序扫描 Log 重建索引(旧→新,天然 newest-wins)。
/// 容错版索引重建:`open` 的恢复路径用它 —— **坏记录跳过并记录,而不是让整副本起不来**。
///
/// 每条坏记录后尝试从记录头读出 `data_len` 重同步到下一条;读不出(头也坏了)则放弃该段剩余部分
/// (记 `abandoned_segments`)并继续下**一段**。不删不改写任何盘上字节。
fn rebuild_index_tolerant(
    log: &Log,
    index_dir: &Path,
    max_entries: usize,
) -> Result<(PersistentIndex, OpenDamage)> {
    std::fs::create_dir_all(index_dir)?;
    for ent in std::fs::read_dir(index_dir)? {
        let ent = ent?;
        if let Some(n) = ent.file_name().to_str() {
            if n.ends_with(".idx") || n.ends_with(".fuse") {
                std::fs::remove_file(ent.path())?;
            }
        }
    }
    let index = PersistentIndex::create(index_dir, max_entries)?;
    let mut damage = OpenDamage::default();
    for (position, data_end) in log.segment_spans() {
        let mut off = 0u64;
        while off < data_end {
            let at = LogOffset::new(position, off);
            match read_full_record(log, at) {
                Ok((raw, decoded)) => {
                    let value = index_value_from_record(at, raw.len(), &decoded);
                    index.add(decoded.key, value)?;
                    off += raw.len() as u64;
                }
                Err(e) => {
                    damage.bad_records += 1;
                    if damage.first_bad_offset.is_none() {
                        damage.first_bad_offset = Some((position, off));
                    }
                    // 尝试按记录头重同步;头读不出/长度不可信 → 放弃本段剩余部分(不改盘)。
                    let resync = resync_skip(log, at, data_end);
                    match resync {
                        Some(skip) => {
                            tracing::error!(
                                segment = position,
                                offset = off,
                                error = %e,
                                "damaged record skipped during index rebuild"
                            );
                            off += skip;
                        }
                        None => {
                            damage.abandoned_segments += 1;
                            tracing::error!(
                                segment = position,
                                offset = off,
                                error = %e,
                                "damaged record is not resyncable; abandoning the rest of this segment"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }
    index.flush()?;
    // 重建覆盖了整个 log(除被放弃的段),水位推进到当前末尾。
    write_watermark(index_dir, log.end_offset())?;
    Ok((index, damage))
}

/// 打开时若发生损伤,打一条汇总 ERROR(逐条坏记录的明细已在重建过程中打好)。
///
/// **大声报错**是这条容错路径的硬要求:宁可日志刺眼,也不能让"带伤启动"悄悄发生。
fn log_open_damage(dir: &Path, damage: &OpenDamage) {
    if damage.is_clean() {
        return;
    }
    tracing::error!(
        dir = %dir.display(),
        bad_records = damage.bad_records,
        abandoned_segments = damage.abandoned_segments,
        first_bad = ?damage.first_bad_offset,
        "opened store with damaged records: unusable records were skipped (bytes left untouched on disk); \
         run `GET /partitions/{{pid}}/verify` for details and consider rebuilding this replica"
    );
}

fn rebuild_index(log: &Log, index_dir: &Path, max_entries: usize) -> Result<PersistentIndex> {
    std::fs::create_dir_all(index_dir)?;
    for ent in std::fs::read_dir(index_dir)? {
        let ent = ent?;
        if let Some(n) = ent.file_name().to_str() {
            if n.ends_with(".idx") || n.ends_with(".fuse") {
                std::fs::remove_file(ent.path())?;
            }
        }
    }
    let index = PersistentIndex::create(index_dir, max_entries)?;
    for_each_record(log, |off, raw, decoded| {
        let value = index_value_from_record(off, raw.len(), decoded);
        index.add(decoded.key, value)?;
        Ok(())
    })?;
    index.flush()?;
    // 重建覆盖了整个 log,水位推进到当前末尾。
    write_watermark(index_dir, log.end_offset())?;
    Ok(index)
}

/// 加载已落盘索引并按水位回放 log 尾部(增量恢复,非全量重建);`Corrupt` 由调用方回退重建。
fn load_index_with_replay(
    log: &Log,
    index_dir: &Path,
    max_entries: usize,
) -> Result<PersistentIndex> {
    let index = PersistentIndex::open(index_dir, max_entries)?;
    let watermark = read_watermark(index_dir).unwrap_or(LogOffset::new(0, 0));
    replay_from(log, &index, watermark)?;
    Ok(index)
}

/// 按持久化水位回放 log 尾部:把 offset ≥ watermark 的记录补进索引(旧→新,保 newest-wins)。
fn replay_from(log: &Log, index: &PersistentIndex, watermark: LogOffset) -> Result<()> {
    for_each_record(log, |off, raw, decoded| {
        if off >= watermark {
            let value = index_value_from_record(off, raw.len(), decoded);
            index.add(decoded.key, value)?;
        }
        Ok(())
    })
}

const WATERMARK_FILE: &str = "WATERMARK";

/// 写持久化水位(segment u64 + offset u64 + crc u32,tmp→fsync→rename)。
fn write_watermark(index_dir: &Path, off: LogOffset) -> Result<()> {
    std::fs::create_dir_all(index_dir)?;
    let mut buf = [0u8; 20];
    buf[0..8].copy_from_slice(&off.segment.to_le_bytes());
    buf[8..16].copy_from_slice(&off.offset.to_le_bytes());
    let crc = crc32fast::hash(&buf[0..16]);
    buf[16..20].copy_from_slice(&crc.to_le_bytes());
    let path = index_dir.join(WATERMARK_FILE);
    let tmp = index_dir.join("WATERMARK.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// 读持久化水位;文件缺失或损坏返回 None(调用方回退到从 0 回放)。
fn read_watermark(index_dir: &Path) -> Option<LogOffset> {
    let buf = std::fs::read(index_dir.join(WATERMARK_FILE)).ok()?;
    if buf.len() != 20 {
        return None;
    }
    let crc = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    if crc32fast::hash(&buf[0..16]) != crc {
        return None;
    }
    let segment = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    Some(LogOffset::new(segment, offset))
}

/// 打开前修复未完成的 compaction:依据 `COMPACT_COMMIT` 标记与 `log.new`/`log.old` 收敛到一致状态。
fn recover_compaction(
    dir: &Path,
    total_capacity: u64,
    segment_capacity: u64,
    max_entries: usize,
) -> Result<()> {
    let (log_dir, index_dir) = sub_dirs(dir);
    let log_new = dir.join("log.new");
    let log_old = dir.join("log.old");
    let marker = dir.join("COMPACT_COMMIT");

    if marker.exists() {
        // 提交进行中:保证 log 是 compacted 的那份。
        if !log_dir.exists() && log_new.exists() {
            // 崩在两次 rename 之间(CompactAll):log.new 尚未换入。
            std::fs::rename(&log_new, &log_dir)?;
        }
        // 其余情况 log_dir 已是新 log(或 log.old 存旧份,丢弃)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
        clean_tmp_segments(&log_dir)?; // 增量 compaction 崩溃遗留的 .tmp 新段
                                       // 索引可能是旧/半建的,统一重建。
        let log = Log::open(&log_dir, total_capacity, segment_capacity)?;
        let _ = rebuild_index(&log, &index_dir, max_entries)?;
        std::fs::remove_file(&marker)?;
    } else {
        // 无标记:COPY 阶段崩溃遗留的 log.new / .tmp 直接丢弃(旧 log 完好)。
        rm_rf(&log_new)?;
        rm_rf(&log_old)?;
        clean_tmp_segments(&log_dir)?;
    }
    Ok(())
}

/// 删除 log 目录下残留的 `*.tmp`(崩溃遗留的增量 compaction 半成品新段)。
fn clean_tmp_segments(log_dir: &Path) -> Result<()> {
    if !log_dir.exists() {
        return Ok(());
    }
    for ent in std::fs::read_dir(log_dir)? {
        let ent = ent?;
        if ent
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with(".tmp"))
        {
            let _ = std::fs::remove_file(ent.path());
        }
    }
    Ok(())
}

fn write_marker(path: &Path) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    f.write_all(b"COMPACTING")?;
    f.sync_all()?;
    Ok(())
}

fn rm_rf(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: u64 = 64;
    const BLOCK: u64 = 1024;

    /// RAII 临时目录(测试结束整目录回收,`$TMPDIR` 下,带 ambry 前缀可辨认)。
    fn temp_dir(tag: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("ambry_blob_{tag}_"))
            .tempdir()
            .unwrap()
    }

    fn seg_cap(blocks: u64) -> u64 {
        HEADER + blocks * BLOCK
    }

    fn key(n: u64) -> StoreKey {
        StoreKey::new(n.to_be_bytes())
    }

    /// 历史口径策略:只回收被取代的旧记录(不判 TTL 过期、墓碑永留)。
    /// 既有断言的语义基准都在这个口径上(墓碑回收与 TTL 回收由专门的新测试覆盖)。
    fn legacy_policy(dead_ratio: f64) -> CompactPolicy {
        CompactPolicy::preserve_legacy(dead_ratio)
    }

    #[test]
    fn io_error_threshold_trips_shutdown_and_hook() {
        let dir = temp_dir("ioerr");
        let seg = seg_cap(64);
        let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        store.io_error_threshold.store(3, Ordering::Relaxed);

        let fired = Arc::new(AtomicBool::new(false));
        let f2 = fired.clone();
        store.set_io_error_hook(Arc::new(move || f2.store(true, Ordering::Relaxed)));

        // 逻辑错误不计数、不清零。
        let _ = store.track::<()>(Err(Error::NotFound));
        assert_eq!(store.io_error_count(), 0);
        // 两次 IO 错误未达阈值。
        let _ = store.track::<()>(Err(Error::Io(std::io::Error::other("x"))));
        let _ = store.track::<()>(Err(Error::Io(std::io::Error::other("x"))));
        assert_eq!(store.io_error_count(), 2);
        assert!(store.is_available());
        assert!(!fired.load(Ordering::Relaxed));
        // 成功 op 清零。
        let _ = store.track::<()>(Ok(()));
        assert_eq!(store.io_error_count(), 0);
        // 连续 3 次 IO 错误 → 自关 + 触发 hook。
        for _ in 0..3 {
            let _ = store.track::<()>(Err(Error::Io(std::io::Error::other("x"))));
        }
        assert!(!store.is_available());
        assert!(fired.load(Ordering::Relaxed));
        // 自关后拒绝服务。
        assert!(matches!(
            store.put(key(1), b"data"),
            Err(Error::Unavailable)
        ));
    }

    #[test]
    fn threshold_zero_never_trips() {
        let dir = temp_dir("ioerr0");
        let seg = seg_cap(64);
        let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        for _ in 0..100 {
            store.record_io_error();
        }
        assert!(
            store.is_available(),
            "threshold 0 (disabled) must never trip"
        );
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = temp_dir("rt");
        let seg = seg_cap(64);
        let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();

        let data = b"hello ambry blob".to_vec();
        store.put(key(1), &data).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), data);

        let big: Vec<u8> = (0..(BLOCK * 3 + 77) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        store.put(key(2), &big).unwrap();
        assert_eq!(store.get(&key(2)).unwrap(), big);

        assert!(matches!(store.get(&key(999)), Err(Error::NotFound)));
        store.close().unwrap();
    }

    #[test]
    fn put_existing_rejected() {
        let dir = temp_dir("dup");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        store.put(key(1), b"a").unwrap();
        assert!(matches!(store.put(key(1), b"b"), Err(Error::AlreadyExists)));
        store.close().unwrap();
    }

    #[test]
    fn delete_then_reput_rejected() {
        let dir = temp_dir("del");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        store.put(key(1), b"v1").unwrap();
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        // 重复删幂等成功(204);不存在仍报 NotFound。
        store.delete(key(1)).unwrap();
        assert!(matches!(store.delete(key(2)), Err(Error::NotFound)));

        // blob id 永久占用:删除后普通 put 仍报 AlreadyExists,墓碑不可复活。
        assert!(matches!(
            store.put(key(1), b"v2"),
            Err(Error::AlreadyExists)
        ));
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        store.close().unwrap();
    }

    #[test]
    fn reopen_recovers_blobs() {
        let dir = temp_dir("reopen");
        let seg = seg_cap(16);
        {
            let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
            for i in 0..20u64 {
                store.put(key(i), format!("blob-{i}").as_bytes()).unwrap();
            }
            store.delete(key(3)).unwrap();
            store.close().unwrap();
        }
        let store = BlobStore::open(dir.path(), seg * 8, seg, 4).unwrap();
        for i in 0..20u64 {
            if i == 3 {
                assert!(matches!(store.get(&key(i)), Err(Error::Deleted)));
            } else {
                assert_eq!(
                    store.get(&key(i)).unwrap(),
                    format!("blob-{i}").into_bytes(),
                    "key {i} lost after reopen"
                );
            }
        }
    }

    #[test]
    fn rebuild_index_from_log_scan() {
        let dir = temp_dir("rebuild");
        let seg = seg_cap(16);
        {
            let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
            for i in 0..25u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            store.delete(key(7)).unwrap(); // key 7:PUT + DELETE 墓碑 → newest-wins 应为 Deleted
            store.close().unwrap();
        }

        let index_dir = dir.path().join("index");
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().is_some_and(|e| e == "idx") {
                std::fs::remove_file(p).unwrap();
            }
        }

        let store = BlobStore::open_rebuild_index(dir.path(), seg * 8, seg, 4).unwrap();
        for i in 0..25u64 {
            if i == 7 {
                assert!(
                    matches!(store.get(&key(7)), Err(Error::Deleted)),
                    "key 7 tombstone after rebuild"
                );
                continue;
            }
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("v-{i}").into_bytes(),
                "key {i} after rebuild"
            );
        }
        assert!(matches!(store.get(&key(999)), Err(Error::NotFound)));

        drop(store);
        let store2 = BlobStore::open(dir.path(), seg * 8, seg, 4).unwrap();
        assert!(matches!(store2.get(&key(7)), Err(Error::Deleted)));
        drop(store2);
    }

    #[test]
    fn open_falls_back_to_rebuild_on_corrupt_index() {
        let dir = temp_dir("corrupt_idx");
        let seg = seg_cap(16);
        {
            let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
            for i in 0..20u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            store.delete(key(3)).unwrap();
            store.close().unwrap();
        }

        // 损坏活跃索引段(`{seq}.idx` 且无配套 `{seq}.fuse`,open 时走 crc 校验的整段载入)。
        let index_dir = dir.path().join("index");
        let mut active_idx = None;
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            let is_idx = p.extension().is_some_and(|e| e == "idx");
            let has_fuse = p.with_extension("fuse").exists();
            if is_idx && !has_fuse {
                active_idx = Some(p);
                break;
            }
        }
        std::fs::write(active_idx.expect("active idx exists"), b"garbage").unwrap();

        // open 检测到 Corrupt → 自动扫 log 重建;逻辑状态仍正确。
        let store = BlobStore::open(dir.path(), seg * 8, seg, 4).unwrap();
        for i in 0..20u64 {
            if i == 3 {
                assert!(
                    matches!(store.get(&key(3)), Err(Error::Deleted)),
                    "key 3 stays deleted"
                );
                continue;
            }
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("v-{i}").into_bytes(),
                "key {i} recovered"
            );
        }
        store.close().unwrap();
    }

    #[test]
    fn record_roundtrip_layout_and_crc_guard() {
        // 记录头 24B = magic2 + version1 + flags1 + key_suffix4 + expiration8 + data_len4 + crc4。
        assert_eq!(RECORD_OVERHEAD, 24);
        assert_eq!(rec::KEY_SUFFIX, 4, "key 只存低 32bit 后缀");
        assert_eq!(rec::DATA, 20);

        let k = StoreKey::from_parts(7, 42);
        let data = b"payload".to_vec();
        let buf = encode_record(&k, 0, 1_700_000_000_000, &data).unwrap();
        assert_eq!(buf.len(), RECORD_OVERHEAD + data.len());
        // 记录体里**不含** partition 前缀:只有 4B 后缀(高 32bit 在段头)。
        assert_eq!(
            &buf[rec::KEY_SUFFIX..rec::KEY_SUFFIX + 4],
            &42u32.to_be_bytes()
        );

        // 前缀来自段头:同一段字节按不同前缀解出不同的完整 key。
        let d = decode_record(&buf, 7).unwrap();
        assert_eq!(d.flags, 0);
        assert_eq!(d.key, k);
        assert_eq!(d.data, data);
        assert_eq!(d.expiration_ms, 1_700_000_000_000);
        assert_eq!(
            decode_record(&buf, 9).unwrap().key,
            StoreKey::from_parts(9, 42)
        );

        // 版本不符 → Corrupt(换版后旧字节绝不会被按新布局静默解释)。
        let mut bad_version = buf.clone();
        bad_version[rec::VERSION] = REC_VERSION - 1;
        assert!(matches!(
            decode_record(&bad_version, 7),
            Err(Error::Corrupt(_))
        ));

        // 翻转一个数据字节,crc 必须失败。
        let mut flipped = buf.clone();
        flipped[rec::DATA] ^= 0xFF;
        assert!(matches!(decode_record(&flipped, 7), Err(Error::Corrupt(_))));
    }

    #[test]
    fn ttl_survives_rebuild() {
        let dir = temp_dir("ttl");
        let seg = seg_cap(16);
        let exp = now_ms() as i64 + 3_600_000; // 1h 后过期
        {
            let store = BlobStore::create(dir.path(), seg * 4, seg, 100).unwrap();
            store.put_ttl(key(1), b"has-ttl", exp).unwrap();
            store.put(key(2), b"no-ttl").unwrap();
            store.close().unwrap();
        }
        // 抹掉索引,从 log 重建:expiration 应被保留。
        let index_dir = dir.path().join("index");
        for ent in std::fs::read_dir(&index_dir).unwrap() {
            let p = ent.unwrap().path();
            let _ = std::fs::remove_file(p);
        }
        let store = BlobStore::open_rebuild_index(dir.path(), seg * 4, seg, 100).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"has-ttl".to_vec());
        assert_eq!(store.get(&key(2)).unwrap(), b"no-ttl".to_vec());
        // 用一个已过期的 TTL 再验证过期路径。
        drop(store);
        {
            let s2 = BlobStore::open(dir.path(), seg * 4, seg, 100).unwrap();
            s2.put_ttl(key(3), b"expired", 1).unwrap(); // expiration=1ms,早已过期
            assert!(matches!(s2.get(&key(3)), Err(Error::Expired)));
            s2.close().unwrap();
        }
    }

    #[test]
    fn compact_reclaims_dead_space_and_preserves_state() {
        let dir = temp_dir("compact");
        let seg = seg_cap(8);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();

        // 死数据来源(Ambry 语义下无覆盖/无删后重写):delete 使原 PUT 变死,墓碑成为新赢家。
        for i in 0..10u64 {
            store
                .put(key(i), format!("original-value-{i}").as_bytes())
                .unwrap();
        }
        for i in 0..5u64 {
            store.delete(key(i)).unwrap(); // key 0..4 变墓碑(原 PUT 变死数据)
        }

        let before = store.stats().unwrap();
        assert!(before.dead_bytes > 0, "should have dead data");

        let st = store.compact(&legacy_policy(0.5)).unwrap();
        assert!(st.dead_bytes > 0);
        assert_eq!(
            st.tombstone_reclaimed_bytes, 0,
            "历史口径不回收墓碑(保留期内的墓碑照搬)"
        );
        // compact 后死数据应清零(全库只剩赢家:5..9 的 PUT + 0..4 的墓碑)。
        let after = store.stats().unwrap();
        assert_eq!(after.dead_bytes, 0, "no dead data after compaction");

        // 逻辑状态不变:0..4 已删,5..9 是原值。
        for i in 0..5u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::Deleted)),
                "key {i} must stay deleted"
            );
        }
        for i in 5..10u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("original-value-{i}").into_bytes(),
                "key {i} value must survive compaction"
            );
        }

        // 重开(会经过 recover_compaction 无标记路径)仍一致。
        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        for i in 5..10u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                format!("original-value-{i}").into_bytes()
            );
        }
        assert!(matches!(store.get(&key(0)), Err(Error::Deleted)));
    }

    #[test]
    fn compact_if_needed_respects_threshold() {
        let dir = temp_dir("compact_thr");
        let seg = seg_cap(8);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 100).unwrap();
        for i in 0..5u64 {
            store.put(key(i), b"v").unwrap();
        }
        // 全是存活数据,可回收占比为 0 → 高阈值不触发。
        assert!(store
            .compact_if_needed(&legacy_policy(0.5))
            .unwrap()
            .is_none());
    }

    /// 判定用的"现在":远在未来,确保短 TTL 对象都已过期,且不会与真实时钟打架。
    const NOW_FAR_FUTURE: i64 = 10_000_000_000_000;

    #[test]
    fn compaction_reclaims_expired_objects() {
        let dir = temp_dir("compact_ttl");
        let seg = seg_cap(4);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
        // 三条短 TTL(1ms,出生即过期)+ 两条不过期。载荷 ~2KB、段容量 ~4KB ⇒ 过期数据必然
        // 落在**封存段**里(活跃段不参与增量压缩,闸门放行也不代表有段可选)。
        let expired = vec![0xE1u8; 2000];
        let live = vec![0x11u8; 2000];
        for i in 0..3u64 {
            store.put_ttl(key(i), &expired, 1).unwrap();
        }
        for i in 3..5u64 {
            store.put(key(i), &live).unwrap();
        }
        let used_before = store.valid_data_size();
        let size_before = store.size_in_bytes();

        let st = store
            .compact_if_needed(&CompactPolicy::new(0.3, NOW_FAR_FUTURE, 86_400_000))
            .unwrap()
            .expect("过半字节已过期,闸门应放行");
        assert!(
            st.expired_reclaimed_bytes >= 3 * expired.len() as u64,
            "过期对象的数据体应被回收: {st:?}"
        );
        // 索引条目一并消失:过期对象从「Expired」变为「NotFound」(盘上已无此记录)。
        for i in 0..3u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::NotFound)),
                "key {i} 的过期记录应已被回收"
            );
        }
        for i in 3..5u64 {
            assert_eq!(store.get(&key(i)).unwrap(), live);
        }
        assert!(
            store.valid_data_size() <= used_before,
            "有效数据口径只减不增"
        );
        assert!(store.size_in_bytes() < size_before, "盘上占用应真正下降");
        assert_eq!(store.stats().unwrap(), store.stats_by_scan());

        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        assert!(matches!(store.get(&key(0)), Err(Error::NotFound)));
    }

    #[test]
    fn tombstones_are_reclaimed_only_after_retention() {
        let dir = temp_dir("compact_tomb");
        let seg = seg_cap(8);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
        for i in 0..6u64 {
            store.put(key(i), b"payload-to-be-deleted").unwrap();
        }
        for i in 0..3u64 {
            store.delete(key(i)).unwrap();
        }
        let deleted_at = store.expiration_of(&key(0)).expect("墓碑留下删除时刻");
        let retention = 86_400_000;
        // 各墓碑的删除时刻略有先后(同一测试内逐条写),判定"满保留期"时留 1 分钟余量,
        // 免得卡在边界上(这正是保留期判定必须用真实时间戳的体现)。
        let slack = 60_000;

        // 未满保留期:墓碑照搬,已删对象仍是 Deleted(而非 NotFound)。
        let st = store
            .compact(&CompactPolicy::new(
                0.3,
                deleted_at + retention - slack,
                retention,
            ))
            .unwrap();
        assert_eq!(st.tombstone_reclaimed_bytes, 0, "保留期内不得回收墓碑");
        for i in 0..3u64 {
            assert!(matches!(store.get(&key(i)), Err(Error::Deleted)));
        }

        // 满保留期:墓碑连同其索引条目一起消失。
        let st = store
            .compact(&CompactPolicy::new(
                0.3,
                deleted_at + retention + slack,
                retention,
            ))
            .unwrap();
        assert!(st.tombstone_reclaimed_bytes > 0, "{st:?}");
        for i in 0..3u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::NotFound)),
                "key {i} 的墓碑应已被回收"
            );
        }
        for i in 3..6u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                b"payload-to-be-deleted".to_vec()
            );
        }
        assert_eq!(store.stats().unwrap(), store.stats_by_scan());

        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        assert!(matches!(store.get(&key(0)), Err(Error::NotFound)));
    }

    /// 墓碑的删除时刻随记录落盘:重开后保留期判定不变(否则重启就会提前回收墓碑)。
    #[test]
    fn tombstone_delete_time_survives_reopen() {
        let dir = temp_dir("tomb_reopen");
        let seg = seg_cap(8);
        let deleted_at;
        {
            let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
            store.put(key(1), b"v").unwrap();
            store.delete(key(1)).unwrap();
            deleted_at = store.expiration_of(&key(1)).unwrap();
            store.close().unwrap();
        }
        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        assert_eq!(store.expiration_of(&key(1)), Some(deleted_at));
        // 元数据口径:墓碑的 TTL 仍是 -1,不当成过期时刻对外汇报。
        let stat = store.stat(&key(1)).unwrap();
        assert!(stat.deleted);
        assert_eq!(stat.expiration_ms, NO_EXPIRATION);
        assert!(!stat.expired);
    }

    /// partition 级闸门用「可回收占比」(含 TTL/墓碑空间),不是只看死字节计数。
    #[test]
    fn compact_if_needed_gate_counts_expired_bytes() {
        let dir = temp_dir("compact_gate");
        let seg = seg_cap(4);
        let store = BlobStore::create(dir.path(), seg * 32, seg, 4).unwrap();
        let payload = vec![0xABu8; 512];
        for i in 0..4u64 {
            store.put_ttl(key(i), &payload, 1).unwrap(); // 过期
        }
        for i in 4..10u64 {
            store.put(key(i), &payload).unwrap(); // 存活
        }
        let policy = |ratio: f64| CompactPolicy::new(ratio, NOW_FAR_FUTURE, 86_400_000);

        assert!(
            store.compact_if_needed(&policy(0.6)).unwrap().is_none(),
            "4/10 已过期,0.6 的闸门不该放行"
        );
        let st = store
            .compact_if_needed(&policy(0.3))
            .unwrap()
            .expect("0.3 的闸门应放行");
        assert!(st.expired_reclaimed_bytes > 0);
    }

    #[test]
    fn recover_finishes_interrupted_commit() {
        // 模拟:compaction 提交阶段崩溃(留下 COMPACT_COMMIT 标记 + log.old),open 应修复。
        let dir = temp_dir("compact_crash");
        let seg = seg_cap(8);
        {
            let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
            for i in 0..8u64 {
                store.put(key(i), format!("v-{i}").as_bytes()).unwrap();
            }
            for i in 0..4u64 {
                store.delete(key(i)).unwrap();
            }
            store.compact(&legacy_policy(0.5)).unwrap();
            store.close().unwrap();
        }
        // 人为伪造"提交中崩溃":放一个标记 + 残留 log.old / log.new,open 必须收敛。
        let marker = dir.path().join("COMPACT_COMMIT");
        std::fs::write(&marker, b"COMPACTING").unwrap();
        // 残留一个空 log.new(无标记语义下会被丢弃;有标记时若 log 存在则清理)。
        std::fs::create_dir_all(dir.path().join("log.new")).unwrap();
        std::fs::create_dir_all(dir.path().join("log.old")).unwrap();

        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        assert!(!marker.exists(), "marker cleared after recovery");
        assert!(!dir.path().join("log.new").exists(), "log.new cleaned");
        assert!(!dir.path().join("log.old").exists(), "log.old cleaned");
        for i in 4..8u64 {
            assert_eq!(store.get(&key(i)).unwrap(), format!("v-{i}").into_bytes());
        }
        for i in 0..4u64 {
            assert!(matches!(store.get(&key(i)), Err(Error::Deleted)));
        }
    }

    /// **磁盘格式前后兼容**(CLAUDE.md §3.4):改造前的墓碑 `expiration = -1`(删除时间未知),
    /// 新代码必须"读得进、且不回收" —— 否则升级瞬间会抹掉老副本上的删除标记,滞后副本一修复就把
    /// 已删对象复活回来。
    #[test]
    fn legacy_tombstone_on_disk_is_readable_and_never_reclaimed() {
        let dir = temp_dir("legacy_tomb");
        let seg = seg_cap(8);
        {
            let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
            store.put(key(7), b"v").unwrap();
            // 手工造一条"旧格式"墓碑:记录头与索引条目的 expiration 都是 -1。
            let record = encode_record(&key(7), FLAG_DELETED, NO_EXPIRATION, &[]).unwrap();
            let inner = store.inner();
            let off = inner.log.append(&record).unwrap();
            let mut value = IndexValue::put(off, record_size_u32(record.len() as u64).unwrap());
            value.flags = FLAG_DELETED; // expiration 保持 NO_EXPIRATION
            inner.index.add(key(7), value).unwrap();
            store.flush().unwrap();
            store.close().unwrap();
        }

        // 重开走"由 log 重建索引"的路径:删除时间从记录头还原,仍是 -1。
        let store = BlobStore::open(dir.path(), seg * 16, seg, 4).unwrap();
        assert_eq!(store.expiration_of(&key(7)), Some(NO_EXPIRATION));
        assert!(matches!(store.get(&key(7)), Err(Error::Deleted)));

        // 保留期给 0(最激进的回收口径)也不得回收删除时间未知的墓碑。
        let st = store
            .compact(&CompactPolicy::new(0.0, NOW_FAR_FUTURE, 0))
            .unwrap();
        assert_eq!(
            st.tombstone_reclaimed_bytes, 0,
            "旧格式墓碑(删除时间未知)永不自动回收"
        );
        assert!(matches!(store.get(&key(7)), Err(Error::Deleted)));
    }

    #[test]
    fn watermark_replays_log_tail_beyond_persisted_index() {
        // 模拟「索引持久化后又写了新数据但未再持久化,然后崩溃」:
        // log 有尾部记录、索引文件停在旧水位。open 应按水位回放 log 尾部补回。
        let dir = temp_dir("wm");
        let seg = seg_cap(64);
        {
            let store = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
            store.put(key(1), b"a").unwrap();
            store.flush().unwrap(); // 索引 + 水位落到含 key(1)
                                    // 之后再写 key(2)/key(3),只 flush 数据日志(不落索引、不推进水位),模拟崩溃前状态。
            store.put(key(2), b"b").unwrap();
            store.put(key(3), b"c").unwrap();
            store.log_flush_only_for_test();
            // 直接丢弃内存态(不走 close 的 flush),模拟进程崩溃。
            std::mem::forget(store);
        }
        // 重开:水位=含 key(1) 处,回放 log 尾部应补回 key(2)/key(3)。
        let store = BlobStore::open(dir.path(), seg * 4, seg, 1000).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), b"a".to_vec());
        assert_eq!(
            store.get(&key(2)).unwrap(),
            b"b".to_vec(),
            "tail key(2) recovered via replay"
        );
        assert_eq!(
            store.get(&key(3)).unwrap(),
            b"c".to_vec(),
            "tail key(3) recovered via replay"
        );
    }

    #[test]
    fn incremental_compaction_reclaims_only_dead_segments() {
        let dir = temp_dir("incr");
        let seg = seg_cap(1); // 每段数据容量 ~1KiB → 大记录会跨多段
        let store = BlobStore::create(dir.path(), seg * 128, seg, 1000).unwrap();

        let big = |tag: &str| -> Vec<u8> {
            let mut v = vec![b'x'; 400];
            v[..tag.len()].copy_from_slice(tag.as_bytes());
            v
        };

        // 写 40 个大 key(跨很多段),再删前 30 个 → 老段变高死数据。
        for i in 0..40u64 {
            store.put(key(i), &big(&format!("v{i}"))).unwrap();
        }
        for i in 0..30u64 {
            store.delete(key(i)).unwrap();
        }
        store.flush().unwrap();

        let before_segs = store.log_segment_count_for_test();
        assert!(before_segs > 2, "should span multiple segments");
        let st = store.compact_incremental(&legacy_policy(0.5)).unwrap();
        assert!(st.is_some(), "high-dead old segments should be compacted");
        let st = st.unwrap();
        assert!(st.dead_bytes > 0);
        assert!(
            st.segments_after <= before_segs,
            "segment count should not grow"
        );

        // 逻辑状态不变:0..29 已删,30..39 存活。
        for i in 0..30u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::Deleted)),
                "key {i} deleted"
            );
        }
        for i in 30..40u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                big(&format!("v{i}")),
                "key {i} live"
            );
        }

        // 重开一致。
        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 128, seg, 1000).unwrap();
        for i in 30..40u64 {
            assert_eq!(store.get(&key(i)).unwrap(), big(&format!("v{i}")));
        }
        assert!(matches!(store.get(&key(5)), Err(Error::Deleted)));
    }

    #[test]
    fn cost_benefit_compaction_reclaims_and_preserves_state() {
        let dir = temp_dir("costben");
        let seg = seg_cap(1); // ~1KiB 数据容量,400B 记录 → 每段约 2 条,跨很多段
        let store = BlobStore::create(dir.path(), seg * 128, seg, 1000).unwrap();
        let big = |tag: &str| -> Vec<u8> {
            let mut v = vec![b'y'; 400];
            v[..tag.len()].copy_from_slice(tag.as_bytes());
            v
        };
        for i in 0..40u64 {
            store.put(key(i), &big(&format!("v{i}"))).unwrap();
        }
        for i in 0..35u64 {
            store.delete(key(i)).unwrap(); // 大量高死老段
        }
        store.flush().unwrap();

        let before = store.log_segment_count_for_test();
        let st = store
            .compact_cost_benefit(&legacy_policy(0.3), 1)
            .unwrap()
            .expect("high-dead range should be picked");
        assert!(
            st.segments_after < before,
            "cost-benefit compaction reclaims ≥1 segment"
        );
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "stats consistent after compaction"
        );

        for i in 0..35u64 {
            assert!(
                matches!(store.get(&key(i)), Err(Error::Deleted)),
                "key {i} stays deleted"
            );
        }
        for i in 35..40u64 {
            assert_eq!(
                store.get(&key(i)).unwrap(),
                big(&format!("v{i}")),
                "key {i} live"
            );
        }

        // 全存活库无可回收区间 → None。
        let dir2 = temp_dir("costben2");
        let fresh = BlobStore::create(dir2.path(), seg * 128, seg, 1000).unwrap();
        for i in 0..6u64 {
            fresh.put(key(i), &big(&format!("v{i}"))).unwrap();
        }
        assert!(
            fresh
                .compact_cost_benefit(&legacy_policy(0.3), 1)
                .unwrap()
                .is_none(),
            "no dead data → no candidate"
        );
        fresh.close().unwrap();

        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 128, seg, 1000).unwrap();
        for i in 35..40u64 {
            assert_eq!(store.get(&key(i)).unwrap(), big(&format!("v{i}")));
        }
    }

    #[test]
    fn incremental_stats_match_full_scan() {
        let dir = temp_dir("stats_incr");
        let seg = seg_cap(2);
        let store = BlobStore::create(dir.path(), seg * 64, seg, 8).unwrap();
        for i in 0..40u64 {
            store.put(key(i), format!("value-{i}").as_bytes()).unwrap();
        }
        for i in 0..15u64 {
            store.delete(key(i)).unwrap();
        }
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after put/delete"
        );

        store.compact_if_needed(&legacy_policy(0.3)).unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after incremental compaction"
        );

        store.compact(&legacy_policy(0.3)).unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after CompactAll"
        );

        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 64, seg, 8).unwrap();
        assert_eq!(
            store.stats().unwrap(),
            store.stats_by_scan(),
            "after reopen recompute"
        );
    }

    #[test]
    fn delete_is_irreversible_and_key_stays_occupied() {
        let dir = temp_dir("irreversible");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
        store.put(key(1), b"hello").unwrap();

        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        let m = store.stat(&key(1)).unwrap();
        assert!(m.deleted && m.data_len == 0, "墓碑无数据长度");

        // 墓碑是终态:重复删幂等成功(204),且不写第二条墓碑;重写(等价"复活")报 AlreadyExists。
        let size_before = store.size_in_bytes();
        let tombs_before = store.index_stats().tombstones;
        store.delete(key(1)).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert!(store.stat(&key(1)).unwrap().deleted);
        assert_eq!(
            store.index_stats().tombstones,
            tombs_before,
            "幂等 delete 不得新增墓碑条目"
        );
        assert_eq!(
            store.size_in_bytes(),
            size_before,
            "幂等 delete 不得追加 log 记录"
        );
        assert!(matches!(
            store.put(key(1), b"hello"),
            Err(Error::AlreadyExists)
        ));
        let mut src = &b"hello"[..];
        assert!(matches!(
            store.put_from_reader(key(1), 5, &mut src, NO_EXPIRATION),
            Err(Error::AlreadyExists)
        ));

        // 墓碑跨重开与扫 log 重建仍是终态。
        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 8, seg, 4).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        store.close().unwrap();
        let store = BlobStore::open_rebuild_index(dir.path(), seg * 8, seg, 4).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert!(store.stat(&key(1)).unwrap().deleted);
    }

    #[test]
    fn ttl_is_fixed_at_write_time() {
        let dir = temp_dir("ttl_write_only");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
        // 过期时刻设在过去 → 立即过期。
        store.put_ttl(key(1), b"payload", 1).unwrap();
        assert!(matches!(store.get(&key(1)), Err(Error::Expired)));
        assert!(store.stat(&key(1)).unwrap().expired);
        // 没有"改期"这条路:过期对象仍是已占用 key。
        assert!(matches!(
            store.put_ttl(key(1), b"payload", NO_EXPIRATION),
            Err(Error::AlreadyExists)
        ));
        // 复制读可以看到过期数据(接收端据此忠实 apply)。
        assert_eq!(
            store.get_with_options(&key(1), true).unwrap(),
            b"payload".to_vec()
        );
        // TTL 落记录体内,故扫 log 全量重建后仍过期(不只活在索引里)。
        store.close().unwrap();
        let store = BlobStore::open_rebuild_index(dir.path(), seg * 8, seg, 4).unwrap();
        assert_eq!(store.expiration_of(&key(1)), Some(1));
        assert!(matches!(store.get(&key(1)), Err(Error::Expired)));
    }

    #[test]
    fn foreign_partition_key_is_rejected() {
        let dir = temp_dir("prefix_reject");
        let seg = seg_cap(16);
        let store = BlobStore::create_for_partition(dir.path(), seg * 8, seg, 4, 7).unwrap();
        assert_eq!(store.key_prefix(), 7);
        let mine = StoreKey::from_parts(7, 1);
        let other = StoreKey::from_parts(8, 1);
        store.put(mine, b"ok").unwrap();
        assert_eq!(store.get(&mine).unwrap(), b"ok".to_vec());

        // 记录只存 key 的低 32bit:别的 partition 的 key 写进去就再也读不回来,必须当场拒。
        assert!(matches!(store.put(other, b"x"), Err(Error::BadArgument(_))));
        let mut src = &b"x"[..];
        assert!(matches!(
            store.put_from_reader(other, 1, &mut src, NO_EXPIRATION),
            Err(Error::BadArgument(_))
        ));
        assert!(matches!(store.delete(other), Err(Error::BadArgument(_))));
        assert!(matches!(
            store.apply_replicated(ReplicatedRecord {
                key: other,
                flags: 0,
                expiration_ms: NO_EXPIRATION,
                data: Some(b"x".to_vec()),
            }),
            Err(Error::BadArgument(_))
        ));

        // 前缀从段头读回(重开后仍知道自己是 partition 7)。
        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 8, seg, 4).unwrap();
        assert_eq!(store.key_prefix(), 7);
        assert_eq!(store.get(&mine).unwrap(), b"ok".to_vec());
        store.close().unwrap();

        // `open_with_partition` 校验盘上分区,不符即拒(目录放错分区必须大声失败)。
        let registry = crate::metrics::StoreMetrics::new(&antfs_metric::Registry::new()).unwrap();
        let cfg = crate::config::StoreConfig::default();
        assert!(matches!(
            BlobStore::open_with_partition(dir.path(), seg * 8, &cfg, &registry, "p7", 8),
            Err(Error::Corrupt(_))
        ));
        let ok =
            BlobStore::open_with_partition(dir.path(), seg * 8, &cfg, &registry, "p7", 7).unwrap();
        assert_eq!(ok.key_prefix(), 7);
    }

    #[test]
    fn stat_get_options_and_introspection() {
        let dir = temp_dir("stat");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
        assert!(store.is_empty());
        assert!(matches!(store.stat(&key(1)), Err(Error::NotFound)));

        store.put(key(1), b"hello").unwrap();
        let m = store.stat(&key(1)).unwrap();
        assert!(!m.deleted && !m.expired);
        assert_eq!(
            m.data_len, 5,
            "payload length reported without reading data"
        );
        assert!(!store.is_empty() && store.size_in_bytes() > 0);

        // 删除后 stat 仍可查状态;数据不再可取 —— 删除不可逆,没有 include_deleted 这条路。
        store.delete(key(1)).unwrap();
        assert!(store.stat(&key(1)).unwrap().deleted);
        assert!(matches!(store.get(&key(1)), Err(Error::Deleted)));
        assert!(matches!(
            store.get_with_options(&key(1), true),
            Err(Error::Deleted)
        ));

        // 过期对象:stat 标记 expired;include_expired 可读(复制拉取需要)。
        store.put_ttl(key(2), b"temp", 1).unwrap();
        assert!(store.stat(&key(2)).unwrap().expired);
        assert!(matches!(store.get(&key(2)), Err(Error::Expired)));
        assert_eq!(
            store.get_with_options(&key(2), true).unwrap(),
            b"temp".to_vec()
        );
        store.close().unwrap();
    }

    /// 差分对拍:纯内存模型记录每个 key 的期望状态,随机穿插 put/delete 与
    /// flush/reopen/compact/compact_incremental,每步全量比对 `get()` 与模型。覆盖
    /// newest-wins、墓碑、删后重写、跨段封存、compaction offset 重写、水位回放重开、
    /// 扫 log 全量重建——把已实现的读写/持久化/压缩路径拧在一起验证一致性。
    #[derive(Clone, PartialEq, Eq)]
    enum ModelState {
        Absent,
        Live(Vec<u8>),
        Deleted,
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn data_for(k: u64, ver: u64) -> Vec<u8> {
        let len = 4 + (ver % 13) as usize;
        let mut v = vec![0u8; len];
        for (i, b) in v.iter_mut().enumerate() {
            *b = (k.wrapping_mul(31).wrapping_add(ver).wrapping_add(i as u64) & 0xff) as u8;
        }
        v
    }

    fn verify_all(store: &BlobStore, model: &[ModelState], ctx: &str) {
        for (k, state) in model.iter().enumerate() {
            let got = store.get(&key(k as u64));
            match state {
                ModelState::Absent => {
                    assert!(
                        matches!(got, Err(Error::NotFound)),
                        "{ctx}: key {k} expect NotFound, got {got:?}"
                    );
                }
                ModelState::Deleted => {
                    assert!(
                        matches!(got, Err(Error::Deleted)),
                        "{ctx}: key {k} expect Deleted, got {got:?}"
                    );
                }
                ModelState::Live(data) => {
                    let got =
                        got.unwrap_or_else(|e| panic!("{ctx}: key {k} expect Live, got {e:?}"));
                    assert_eq!(&got, data, "{ctx}: key {k} data mismatch");
                }
            }
        }
    }

    #[test]
    fn reference_model_full_crosscheck() {
        const KEYS: usize = 24;
        let seg = seg_cap(2); // 小段 + 小封存阈值 → 频繁滚段/封存,吃满 mmap+fuse 封存路径与多段扫描
        let total = seg * 256;
        let max_idx = 8usize;

        for &seed in &[0x1234_5678u64, 0xdead_beefu64, 0x0BAD_F00Du64] {
            let dir = temp_dir(&format!("model_{seed:x}"));
            let mut store = BlobStore::create(dir.path(), total, seg, max_idx).unwrap();
            let mut model = vec![ModelState::Absent; KEYS];
            let mut rng = Rng(seed);
            let mut ver = 0u64;

            for step in 0..400u64 {
                match rng.below(100) {
                    0..=44 => {
                        let k = rng.below(KEYS as u64);
                        ver += 1;
                        let data = data_for(k, ver);
                        let res = store.put(key(k), &data);
                        match &model[k as usize] {
                            ModelState::Absent => {
                                res.unwrap_or_else(|e| {
                                    panic!("step {step}: put absent key {k} failed: {e:?}")
                                });
                                model[k as usize] = ModelState::Live(data);
                            }
                            _ => {
                                assert!(
                                    matches!(res, Err(Error::AlreadyExists)),
                                    "step {step}: put existing key {k} must reject"
                                );
                            }
                        }
                    }
                    45..=64 => {
                        let k = rng.below(KEYS as u64);
                        let res = store.delete(key(k));
                        match &model[k as usize] {
                            ModelState::Absent => assert!(
                                matches!(res, Err(Error::NotFound)),
                                "step {step}: del absent key {k}"
                            ),
                            ModelState::Deleted => assert!(
                                res.is_ok(),
                                "step {step}: delete on tombstone key {k} must be idempotent Ok, got {res:?}"
                            ),
                            ModelState::Live(_) => {
                                res.unwrap_or_else(|e| {
                                    panic!("step {step}: delete key {k} failed: {e:?}")
                                });
                                model[k as usize] = ModelState::Deleted;
                            }
                        }
                    }
                    65..=76 => store.flush().unwrap(),
                    77..=86 => {
                        store.close().unwrap();
                        store = BlobStore::open(dir.path(), total, seg, max_idx).unwrap();
                        verify_all(&store, &model, &format!("step {step} after reopen"));
                    }
                    87..=92 => {
                        store.compact(&legacy_policy(0.3)).unwrap();
                        verify_all(&store, &model, &format!("step {step} after compact"));
                    }
                    _ => {
                        store.compact_incremental(&legacy_policy(0.2)).unwrap();
                        verify_all(
                            &store,
                            &model,
                            &format!("step {step} after compact_incremental"),
                        );
                    }
                }
                verify_all(&store, &model, &format!("step {step}"));
            }

            verify_all(&store, &model, "final live");
            assert_eq!(
                store.stats().unwrap(),
                store.stats_by_scan(),
                "seg_bytes drift vs full scan"
            );
            store.close().unwrap();

            let reopened = BlobStore::open(dir.path(), total, seg, max_idx).unwrap();
            verify_all(&reopened, &model, "final reopen");
            reopened.close().unwrap();

            let rebuilt = BlobStore::open_rebuild_index(dir.path(), total, seg, max_idx).unwrap();
            verify_all(&rebuilt, &model, "final rebuild-from-log");
            rebuilt.close().unwrap();
        }
    }

    // D4 P2:验证 `Arc<BlobStore>` 跨线程共享(方法 `&self`)——多写者并发写(写锁串行),
    // 无锁读者在「并发删除 + compaction 换 Inner」全程读到的 KEPT 键数据恒精确(RCU:老读者
    // 经其持有的 Inner 旧段 fd 读旧文件,Unix 下 rename/unlink 不影响;新读者读换入的新 Inner)。
    #[test]
    fn concurrent_shared_access_stays_consistent_through_writes_and_compaction() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Barrier};

        let dir = temp_dir("concurrent");
        let seg = seg_cap(8);
        let store = Arc::new(BlobStore::create(dir.path(), seg * 256, seg, 1000).unwrap());

        const KEPT: u64 = 200;
        const VICTIMS: u64 = 200;
        let total_keys = KEPT + VICTIMS;

        // Phase A:多写者并发 put(键区间交错、不重叠),Barrier 同步起跑压出并发。
        let writers = 4u64;
        let barrier = Arc::new(Barrier::new(writers as usize));
        std::thread::scope(|s| {
            for w in 0..writers {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                s.spawn(move || {
                    barrier.wait();
                    let mut k = w;
                    while k < total_keys {
                        store.put(key(k), &data_for(k, 0)).unwrap();
                        k += writers;
                    }
                });
            }
        });
        for k in 0..total_keys {
            assert_eq!(
                store.get(&key(k)).unwrap(),
                data_for(k, 0),
                "post-write key {k}"
            );
        }

        // Phase B:无锁读者 并发于 删除者(制造死数据)+ 压缩者(反复 CompactAll 换 Inner)。
        let done = Arc::new(AtomicBool::new(false));
        std::thread::scope(|s| {
            for _ in 0..4 {
                let store = Arc::clone(&store);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    let mut rounds = 0u64;
                    while !done.load(Ordering::Acquire) {
                        for k in 0..KEPT {
                            assert_eq!(
                                store.get(&key(k)).unwrap(),
                                data_for(k, 0),
                                "concurrent read kept key {k}"
                            );
                        }
                        rounds += 1;
                    }
                    for k in 0..KEPT {
                        assert_eq!(
                            store.get(&key(k)).unwrap(),
                            data_for(k, 0),
                            "post-compaction read kept key {k}"
                        );
                    }
                    assert!(rounds > 0, "reader made no round before done");
                });
            }
            {
                let store = Arc::clone(&store);
                s.spawn(move || {
                    for k in KEPT..total_keys {
                        store.delete(key(k)).unwrap();
                    }
                });
            }
            {
                let store = Arc::clone(&store);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    for _ in 0..3 {
                        store.compact(&legacy_policy(0.3)).unwrap();
                    }
                    done.store(true, Ordering::Release);
                });
            }
        });

        for k in 0..KEPT {
            assert_eq!(store.get(&key(k)).unwrap(), data_for(k, 0), "end kept {k}");
        }
        for k in KEPT..total_keys {
            assert!(
                matches!(store.get(&key(k)), Err(Error::Deleted)),
                "end victim {k} must be deleted"
            );
        }

        Arc::into_inner(store).unwrap().close().unwrap();
    }

    // 汇总分批拉取的全部条目到「key → 最新 MessageInfo」(find 恒返回最新态,跨批同 key 合并)。
    #[cfg(test)]
    fn drain_all(
        store: &BlobStore,
        max_size: u64,
    ) -> std::collections::HashMap<StoreKey, MessageInfo> {
        let mut map = std::collections::HashMap::new();
        let mut token = FindToken::Uninitialized;
        loop {
            let info = store.find_entries_since(&token, max_size).unwrap();
            if info.entries.is_empty() {
                break;
            }
            for e in &info.entries {
                map.insert(e.key, *e);
            }
            token = info.token;
        }
        map
    }

    #[test]
    fn find_entries_since_covers_all_and_catches_up() {
        let dir = temp_dir("fes_basic");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 1000).unwrap();
        for k in 0..20u64 {
            store.put(key(k), &data_for(k, 0)).unwrap();
        }
        // 大 maxSize:一批取尽。
        let info = store
            .find_entries_since(&FindToken::Uninitialized, u64::MAX)
            .unwrap();
        assert_eq!(info.entries.len(), 20, "all 20 keys in one batch");
        assert!(info.bytes_read > 0);
        // 从返回游标继续 → 已追平,空。
        let next = store.find_entries_since(&info.token, u64::MAX).unwrap();
        assert!(next.entries.is_empty(), "caught up → empty");
        assert_eq!(next.token, info.token, "token unchanged when caught up");
        store.close().unwrap();
    }

    #[test]
    fn find_entries_since_paginates_and_reflects_latest_state() {
        let dir = temp_dir("fes_page");
        let seg = seg_cap(16);
        // 小段 → 小 journal(4*2=8),配合从头分批拉取,强制走扫 log 慢路径。
        let store = BlobStore::create(dir.path(), seg * 32, seg, 2).unwrap();
        for k in 0..15u64 {
            store.put(key(k), &data_for(k, 0)).unwrap();
        }
        store.delete(key(3)).unwrap();
        store.delete(key(7)).unwrap();

        // 小 maxSize 分批拉取,合并后覆盖全部 15 个 key,且状态为最新(3/7 已删)。
        let all = drain_all(&store, 1);
        assert_eq!(all.len(), 15, "pagination covers all keys");
        assert!(all[&key(3)].deleted, "key 3 latest = deleted");
        assert!(all[&key(7)].deleted, "key 7 latest = deleted");
        assert!(!all[&key(4)].deleted, "key 4 live");
        assert_eq!(all[&key(4)].size, all[&key(0)].size, "sizes populated");

        // journal 快路径:追平后再写一个,从上次游标继续应取到新 key。
        let caught = store
            .find_entries_since(&FindToken::Uninitialized, u64::MAX)
            .unwrap();
        store.put(key(999), &data_for(999, 0)).unwrap();
        let delta = store.find_entries_since(&caught.token, u64::MAX).unwrap();
        let keys: Vec<_> = delta.entries.iter().map(|e| e.key).collect();
        assert!(
            keys.contains(&key(999)),
            "journal fast-path returns new key"
        );
        store.close().unwrap();
    }

    #[test]
    fn find_missing_keys_and_is_key_deleted() {
        let dir = temp_dir("fmk");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 100).unwrap();
        store.put(key(1), b"a").unwrap();
        store.put(key(2), b"b").unwrap();
        store.delete(key(2)).unwrap();

        let missing = store.find_missing_keys(&[key(1), key(2), key(3), key(4)]);
        assert_eq!(
            missing,
            vec![key(3), key(4)],
            "only absent keys reported missing"
        );

        assert!(!store.is_key_deleted(&key(1)).unwrap(), "live");
        assert!(store.is_key_deleted(&key(2)).unwrap(), "deleted");
        assert!(
            matches!(store.is_key_deleted(&key(3)), Err(Error::NotFound)),
            "absent"
        );
        store.close().unwrap();
    }

    #[test]
    fn valid_data_size_excludes_deleted_but_counts_unexpired_and_expired() {
        let dir = temp_dir("valid");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 100).unwrap();
        // 永久 blob。
        store.put(key(1), b"keep-1").unwrap();
        let live1 = store.stat(&key(1)).unwrap();
        // 带过期时刻的 blob:未删除 → 计入(不区分过期)。
        store.put_ttl(key(2), b"expiring", 1000).unwrap();
        // put 后删除 → 墓碑赢家 + 死 PUT,均不计有效。
        store.put(key(3), b"gone").unwrap();
        store.delete(key(3)).unwrap();

        let rec1 = live1.data_len + RECORD_OVERHEAD as u64;
        let rec2 = store.stat(&key(2)).unwrap().data_len + RECORD_OVERHEAD as u64;

        // 未删除的 key1 + key2 计入(含带过期的 key2);已删除 key3 不计。
        assert_eq!(store.valid_data_size(), rec1 + rec2);
        assert_eq!(store.valid_data_size_by_scan(), rec1 + rec2, "scan agrees");
        store.close().unwrap();
    }

    #[test]
    fn valid_data_size_incremental_matches_scan_under_mixed_ops() {
        let dir = temp_dir("valid_diff");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 64, seg, 8).unwrap();
        let mut rng = Rng(0x51ED_C0DE);

        // 混合 put(部分带 TTL)/delete,穿插 compaction,每步后增量计数须与全扫基准相等。
        for step in 0..300u64 {
            let k = rng.below(40);
            match rng.below(4) {
                0 => {
                    let ttl = if k.is_multiple_of(2) {
                        1000
                    } else {
                        NO_EXPIRATION
                    };
                    let _ = store.put_ttl(key(k), &data_for(k, step), ttl);
                }
                1 => {
                    let _ = store.delete(key(k));
                }
                _ => {
                    if step % 40 == 39 {
                        store.compact(&legacy_policy(0.3)).unwrap();
                    }
                }
            }
            assert_eq!(
                store.valid_data_size(),
                store.valid_data_size_by_scan(),
                "step {step}: incremental != scan"
            );
        }
        // 重开后(全量重算)仍一致。
        store.close().unwrap();
        let store = BlobStore::open(dir.path(), seg * 64, seg, 8).unwrap();
        assert_eq!(
            store.valid_data_size(),
            store.valid_data_size_by_scan(),
            "reopen"
        );
        store.close().unwrap();
    }

    #[test]
    fn put_ttl_records_arbitrary_expiration() {
        let dir = temp_dir("ttl_set");
        let seg = seg_cap(16);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 100).unwrap();
        // 未来的过期时刻(非永久)—— 写入时一次定下,没有后续"改期"。
        let future = now_ms() as i64 + 1_000_000;
        store.put_ttl(key(1), b"payload", future).unwrap();
        let m = store.stat(&key(1)).unwrap();
        assert_eq!(m.expiration_ms, future, "expiration comes from the write");
        assert!(!m.expired, "not yet expired");
        assert!(!m.deleted);
        store.close().unwrap();
    }

    #[test]
    fn store_full_when_capacity_exhausted() {
        let dir = temp_dir("store_full");
        let seg = seg_cap(2); // 小段:数据区约 2KiB
        let store = BlobStore::create(dir.path(), seg * 2, seg, 1000).unwrap(); // 仅 2 段
        assert_eq!(store.total_capacity(), seg * 2);

        let value = vec![0u8; 500];
        let mut n = 0u64;
        let full_err = loop {
            match store.put(key(n), &value) {
                Ok(()) => n += 1,
                Err(e) => break e,
            }
            assert!(n < 1000, "should hit StoreFull well before 1000 puts");
        };
        assert!(matches!(full_err, Error::StoreFull), "got {full_err:?}");
        assert!(n > 0, "some puts succeeded before full");
        assert!(store.is_full(), "is_full() true once StoreFull hit");
        assert!(store.available_capacity() < seg, "under one segment free");
        // 已写入的对象仍可读。
        assert_eq!(store.get(&key(0)).unwrap(), value);
        store.close().unwrap();
    }

    /// 拉复制收敛驱动:把 `peer` 的全部记录按 log 序忠实 apply 到 `local`(镜像 store-server 复制器)。
    fn replicate_all(peer: &BlobStore, local: &BlobStore) {
        let mut token = FindToken::Uninitialized;
        loop {
            let info = peer.find_entries_since(&token, u64::MAX).unwrap();
            if info.entries.is_empty() {
                break;
            }
            for m in &info.entries {
                let flags = if m.deleted { FLAG_DELETED } else { 0 };
                let data = if m.deleted {
                    None
                } else {
                    Some(peer.get_with_options(&m.key, true).unwrap())
                };
                local
                    .apply_replicated(ReplicatedRecord {
                        key: m.key,
                        flags,
                        expiration_ms: m.expiration_ms,
                        data,
                    })
                    .unwrap();
            }
            token = info.token;
        }
    }

    #[test]
    fn apply_replicated_propagates_put_and_is_idempotent() {
        let (dp, dl) = (temp_dir("repl_put_peer"), temp_dir("repl_put_local"));
        let seg = seg_cap(64);
        let peer = BlobStore::create(dp.path(), seg * 4, seg, 1000).unwrap();
        let local = BlobStore::create(dl.path(), seg * 4, seg, 1000).unwrap();
        let payload = vec![9u8; 128];
        peer.put(key(1), &payload).unwrap();

        replicate_all(&peer, &local);
        assert_eq!(local.get(&key(1)).unwrap(), payload, "PUT propagated");
        assert!(!local.stat(&key(1)).unwrap().deleted);

        let bytes = local.size_in_bytes();
        replicate_all(&peer, &local); // 再来一轮
        assert_eq!(local.size_in_bytes(), bytes, "重复 apply 幂等:字节不增");

        peer.close().unwrap();
        local.close().unwrap();
    }

    /// 墓碑恒胜:对象不可变 + 删除不可逆 ⇒ 每个 key 的状态单调向前。
    /// 本地已有的 PUT 不会被 peer 的 PUT 覆盖(内容相同),被 peer 的墓碑推进;墓碑不再后退。
    #[test]
    fn apply_replicated_is_tombstone_wins_and_never_regresses() {
        let (dp, dl) = (temp_dir("repl_tw_peer"), temp_dir("repl_tw_local"));
        let seg = seg_cap(64);
        let peer = BlobStore::create(dp.path(), seg * 4, seg, 1000).unwrap();
        let local = BlobStore::create(dl.path(), seg * 4, seg, 1000).unwrap();

        // 1) peer 的 PUT 进本地(PUT + 本地不存在 → Applied)。
        peer.put(key(1), &[1, 2, 3]).unwrap();
        replicate_all(&peer, &local);
        assert_eq!(local.get(&key(1)).unwrap(), vec![1, 2, 3]);

        // 2) 本地已有同名 key 时,peer 的 PUT 一律 Skipped(内容不可变,无需覆盖)。
        let before = local.size_in_bytes();
        let out = local
            .apply_replicated(ReplicatedRecord {
                key: key(1),
                flags: 0,
                expiration_ms: NO_EXPIRATION,
                data: Some(vec![9, 9, 9]),
            })
            .unwrap();
        assert_eq!(out, ApplyOutcome::Skipped, "PUT over PUT skips");
        assert_eq!(local.size_in_bytes(), before, "内容未被改写");

        // 3) peer 的墓碑覆盖本地 PUT(tombstone wins)。
        peer.delete(key(1)).unwrap();
        replicate_all(&peer, &local);
        assert!(matches!(local.get(&key(1)), Err(Error::Deleted)));

        // 4) 本地已是墓碑 → peer 的 PUT 不再生效(删除不可逆,不回归)。
        let out = local
            .apply_replicated(ReplicatedRecord {
                key: key(1),
                flags: 0,
                expiration_ms: NO_EXPIRATION,
                data: Some(vec![4, 5, 6]),
            })
            .unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Skipped,
            "tombstone never regresses to PUT"
        );
        assert!(matches!(local.get(&key(1)), Err(Error::Deleted)));
        // peer 的墓碑再 apply 也 Skipped(幂等)。
        let out = local
            .apply_replicated(ReplicatedRecord {
                key: key(1),
                flags: FLAG_DELETED,
                expiration_ms: NO_EXPIRATION,
                data: None,
            })
            .unwrap();
        assert_eq!(out, ApplyOutcome::Skipped, "tombstone over tombstone skips");

        peer.close().unwrap();
        local.close().unwrap();
    }

    /// 存活记录缺数据体且本地也缺该 key → `Corrupt`(不能凭空造一条没有数据的活记录)。
    #[test]
    fn apply_replicated_alive_without_data_is_corrupt() {
        let dir = temp_dir("repl_no_data");
        let seg = seg_cap(64);
        let local = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        let r = local.apply_replicated(ReplicatedRecord {
            key: key(1),
            flags: 0,
            expiration_ms: NO_EXPIRATION,
            data: None,
        });
        assert!(matches!(r, Err(Error::Corrupt(_))), "got {r:?}");
        // 墓碑则不需要数据体,照常落盘。
        let ok = local.apply_replicated(ReplicatedRecord {
            key: key(1),
            flags: FLAG_DELETED,
            expiration_ms: NO_EXPIRATION,
            data: None,
        });
        assert_eq!(ok.unwrap(), ApplyOutcome::Applied);
        assert!(matches!(local.get(&key(1)), Err(Error::Deleted)));
        local.close().unwrap();
    }

    #[test]
    fn apply_replicated_propagates_tombstone() {
        let (dp, dl) = (temp_dir("repl_del_peer"), temp_dir("repl_del_local"));
        let seg = seg_cap(64);
        let peer = BlobStore::create(dp.path(), seg * 4, seg, 1000).unwrap();
        let local = BlobStore::create(dl.path(), seg * 4, seg, 1000).unwrap();
        peer.put(key(3), &[7u8; 32]).unwrap();
        replicate_all(&peer, &local);
        assert!(local.get(&key(3)).is_ok(), "先收到 PUT");

        peer.delete(key(3)).unwrap();
        replicate_all(&peer, &local);
        assert!(
            matches!(local.get(&key(3)), Err(Error::Deleted)),
            "墓碑传播后本地不可读"
        );

        peer.close().unwrap();
        local.close().unwrap();
    }

    #[test]
    fn apply_replicated_rejected_when_unavailable() {
        let dir = temp_dir("repl_unavail");
        let seg = seg_cap(64);
        let local = BlobStore::create(dir.path(), seg * 4, seg, 1000).unwrap();
        local.available.store(false, Ordering::Relaxed);
        let r = local.apply_replicated(ReplicatedRecord {
            key: key(4),
            flags: 0,
            expiration_ms: NO_EXPIRATION,
            data: Some(vec![1]),
        });
        assert!(matches!(r, Err(Error::Unavailable)), "自关后 apply 拒绝");
        local.close().unwrap();
    }

    #[test]
    fn sealed_segments_exclude_active_and_copy_rebuilds() {
        // 造多段:小段容量,写到滚动出封存段。
        let src = temp_dir("s3b_src");
        let seg = seg_cap(2); // 每段 2 块数据
        let store = BlobStore::create(src.path(), seg * 8, seg, 1000).unwrap();
        let rec = vec![7u8; (BLOCK + 100) as usize]; // 每条 > 1 块,2 条即滚动
        for n in 0..6u64 {
            store.put(key(n), &rec).unwrap();
        }
        store.flush().unwrap();
        let sealed = store.sealed_segments();
        assert!(sealed.len() >= 2, "应有多个封存段, got {}", sealed.len());
        let spans = store.inner().log.segment_spans();
        let active_pos = spans.last().unwrap().0;
        assert!(
            sealed
                .iter()
                .all(|(name, _)| u64::from(name.position) != active_pos),
            "封存段不含活跃段 {active_pos}"
        );

        // 物理拷贝封存段到新目录 → open_rebuild_index_with → 封存段里的对象可读。
        let dst = temp_dir("s3b_dst");
        let (log_dir, _) = sub_dirs(dst.path());
        std::fs::create_dir_all(&log_dir).unwrap();
        for (name, _) in &sealed {
            let bytes = store.read_segment_prefix(*name).unwrap();
            std::fs::write(log_dir.join(name.filename()), &bytes).unwrap();
        }
        let cfg = StoreConfig {
            segment_size_bytes: seg,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let reg = crate::StoreMetrics::new(&antfs_metric::Registry::new()).unwrap();
        let copied =
            BlobStore::open_rebuild_index_with(dst.path(), seg * 8, &cfg, &reg, "s3b").unwrap();
        // 封存段覆盖的 key(排除落在活跃段的)应可读。
        let mut readable = 0;
        for n in 0..6u64 {
            if copied.get(&key(n)).is_ok() {
                readable += 1;
            }
        }
        assert!(readable >= 1, "物理拷贝的封存段里应有可读对象");

        store.close().unwrap();
        copied.close().unwrap();
    }
    // ── 段身份(代次)+ 副本 incarnation + 游标重定位(对齐 Ambry `revalidateStoreFindToken`)──────

    /// incarnation:建库即落盘、重开读到同一个值;`adopt_incarnation`(master 权威)会改写它。
    #[test]
    fn incarnation_is_persisted_and_adoptable() {
        let dir = temp_dir("incarnation");
        let seg = seg_cap(4);
        let created;
        {
            let s = BlobStore::create(dir.path(), seg * 4, seg, 4).unwrap();
            created = s.incarnation();
            assert!(created >= 1);
            assert_eq!(
                std::fs::read_to_string(dir.path().join(LOG_INCARNATION_FILE))
                    .unwrap()
                    .trim()
                    .parse::<u64>()
                    .unwrap(),
                created,
                "建库即落盘"
            );
            s.close().unwrap();
        }
        let again = BlobStore::open(dir.path(), seg * 4, seg, 4).unwrap();
        assert_eq!(again.incarnation(), created, "重开必须读到同一号");
        again.adopt_incarnation(created + 100).unwrap();
        assert_eq!(again.incarnation(), created + 100);
        again.close().unwrap();
        let third = BlobStore::open(dir.path(), seg * 4, seg, 4).unwrap();
        assert_eq!(third.incarnation(), created + 100, "采纳后必须持久");
        third.close().unwrap();
    }

    /// 副本目录被删后重建(等价 `DROPPED`,或**换盘后重来**):身份文件随之消失,重建时会写一个新号
    /// (master 随后可用 `adopt_incarnation` 纠正为权威值)。
    #[test]
    fn recreated_replica_gets_a_new_incarnation_file() {
        let root = temp_dir("incarnation_recreate");
        let store_dir = root.path().join("partition_0");
        let seg = seg_cap(4);
        let first = BlobStore::create(&store_dir, seg * 4, seg, 4).unwrap();
        first.close().unwrap();
        std::fs::remove_dir_all(&store_dir).unwrap();
        assert!(read_incarnation(&store_dir).is_none());

        let second = BlobStore::create(&store_dir, seg * 4, seg, 4).unwrap();
        assert!(second.incarnation() >= 1);
        assert_eq!(read_incarnation(&store_dir), Some(second.incarnation()));
        second.close().unwrap();
    }

    /// 身份文件缺失(物理 bootstrap 只拷段):`open` 用**本机当前时刻**兜底并落盘。
    #[test]
    fn missing_incarnation_file_is_healed() {
        let dir = temp_dir("incarnation_heal");
        let seg = seg_cap(4);
        {
            let s = BlobStore::create(dir.path(), seg * 4, seg, 4).unwrap();
            s.put(key(1), b"v").unwrap();
            s.close().unwrap();
        }
        std::fs::remove_file(dir.path().join(LOG_INCARNATION_FILE)).unwrap();
        let store = BlobStore::open(dir.path(), seg * 4, seg, 4).unwrap();
        assert!(store.incarnation() >= 1, "缺失 → 用本机时刻兜底补写");
        assert_eq!(read_incarnation(dir.path()), Some(store.incarnation()));
        assert_eq!(store.get(&key(1)).unwrap(), b"v");
        store.close().unwrap();
    }

    /// **兜底值不得取自段头**:物理 bootstrap 拷来的段诞生在**对端**,其 `created_ns` 正是对端自己的
    /// 兜底身份 —— 若拿它当本副本的身份,重建两次就可能与对端撞号,于是"重建 ⇒ 身份必换"在"申领没
    /// 成功"时失效:对端手里的旧游标会看似合法 → 误判追平 → master 删源 → 丢数据(CLAUDE.md §3.1)。
    ///
    /// 本用例构造"逐字节拷来的段 + 没有身份文件"这一路(物理 bootstrap 的落盘形态),断言新副本的身份
    /// **不等于对端的身份**:在"用首段 `created_ns` 兜底"的旧实现上,这里必然相等(红)。
    #[test]
    fn bootstrapped_replica_identity_is_not_inherited_from_the_peer() {
        let src = temp_dir("incarnation_peer_src");
        let dst = temp_dir("incarnation_peer_dst");
        let seg = seg_cap(2);
        let peer = BlobStore::create(src.path(), seg * 4, seg, 4).unwrap();
        for i in 0..3u64 {
            peer.put(key(i), &[9u8; 700]).unwrap();
        }
        peer.flush().unwrap();
        let sealed = peer.sealed_segments();
        assert!(!sealed.is_empty(), "前置:对端要有封存段");
        let peer_incarnation = peer.incarnation();

        // 物理拷贝:段文件原样落盘(对端时刻原封不动带过来),不带 LOG_INCARNATION。
        let (log_dir, _) = sub_dirs(dst.path());
        std::fs::create_dir_all(&log_dir).unwrap();
        for (name, _) in &sealed {
            let bytes = peer.read_segment_prefix(*name).unwrap();
            std::fs::write(log_dir.join(name.filename()), &bytes).unwrap();
        }
        assert!(
            read_incarnation(dst.path()).is_none(),
            "bootstrap 不带身份文件"
        );
        let local = BlobStore::open_rebuild_index(dst.path(), seg * 8, seg, 4).unwrap();
        assert!(
            local.incarnation() >= 1 && local.incarnation() != peer_incarnation,
            "本副本身份必须从本机取,绝不复用对端的:local={} peer={peer_incarnation}",
            local.incarnation()
        );
        // 重建(换盘)后会再取一个新号:身份永不与"上一次"相同。
        local.close().unwrap();
        std::fs::remove_dir_all(&dst).unwrap();
        std::fs::create_dir_all(&log_dir).unwrap();
        for (name, _) in &sealed {
            let bytes = peer.read_segment_prefix(*name).unwrap();
            std::fs::write(log_dir.join(name.filename()), &bytes).unwrap();
        }
        let again = BlobStore::open_rebuild_index(dst.path(), seg * 8, seg, 4).unwrap();
        assert_ne!(
            again.incarnation(),
            peer_incarnation,
            "第二次 bootstrap 也不得撞上对端"
        );

        peer.close().unwrap();
        again.close().unwrap();
    }

    /// 全库 compact 必须换身份,且旧游标被**重定位**(段已不存在)而不是"看似已追上"。
    #[test]
    fn full_compact_changes_identity_and_revalidates_old_cursor() {
        let dir = temp_dir("epoch_compact");
        let seg = seg_cap(2);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
        let payload = vec![7u8; 600];
        for i in 0..15u64 {
            store.put(key(i), &payload).unwrap();
        }
        // 取一个落在**深处**的游标(旧坐标系里的段 ≥ 1)。
        let info = store
            .find_entries_since(&FindToken::Uninitialized, 8 * 700)
            .unwrap();
        let old_token = info.token;
        assert!(
            old_token.offset().unwrap().segment >= 1,
            "前置:数据必须跨段,游标才够深: {old_token:?}"
        );
        let max_gen_before = store.inner().log.max_generation();
        let incarnation_before = store.incarnation();

        // 删掉大部分数据 → compact 后只剩一小段,旧游标指向的段必然消失。
        for i in 0..12u64 {
            store.delete(key(i)).unwrap();
        }
        let st = store.compact(&legacy_policy(0.3)).unwrap();
        assert!(st.live_bytes > 0);
        assert!(
            store.inner().log.max_generation() > max_gen_before,
            "全库重写后段代次必须接着旧 log 往后取(位置会从 0 重来)"
        );
        assert_eq!(
            store.incarnation(),
            incarnation_before,
            "全库 compact 是**同一份副本**的重写,副本身份(incarnation)不变"
        );

        let after = store.find_entries_since(&old_token, u64::MAX).unwrap();
        assert!(
            matches!(
                after.token_reset,
                Some(TokenResetReason::SegmentMissing | TokenResetReason::SegmentReplaced)
            ),
            "旧游标必须被重定位(段没了 / 段号被复用),而不是返回空批: {:?}",
            after.token_reset
        );
        let got: Vec<StoreKey> = after.entries.iter().map(|e| e.key).collect();
        for i in 12..15u64 {
            assert!(got.contains(&key(i)), "重定位后从起点重扫必须覆盖活数据");
        }
        store.close().unwrap();
    }

    /// **代次正是用来识破"段号被复用"的**:全库 compact 后新 log 的段位置从 0 重来,但代次接着涨,
    /// 于是"同位置、不同代次"必须判重定位(否则位置校验会静默放行,复制漏数据)。
    ///
    /// 这条与 [`LogSegmentName`] 的存在理由一一对应;谁把代次从段身份里去掉,它会立刻变红。
    #[test]
    fn reused_segment_positions_are_detected_by_generation() {
        let dir = temp_dir("gen_reuse");
        let seg = seg_cap(4);
        let store = BlobStore::create(dir.path(), seg * 16, seg, 4).unwrap();
        let payload = vec![9u8; 900];
        for i in 0..8u64 {
            store.put(key(i), &payload).unwrap();
        }
        let old_gen = store.inner().log.generation_of(1).expect("段 1 存在");
        let stale = FindToken::at(LogSegmentName::new(1, u64::from(old_gen)), 0);
        assert!(
            revalidate_token(
                &store.inner().log,
                LogSegmentName::new(1, old_gen.into()),
                0
            )
            .is_ok(),
            "前置:代次一致时放行"
        );

        store.compact(&legacy_policy(0.3)).unwrap();

        // compact 后新 log 仍可能有"位置 1",但代次一定不同 ⇒ 必须判重定位。
        let new_gen = store.inner().log.generation_of(1);
        assert!(new_gen.is_some(), "前置:重写后位置 1 仍有段");
        assert_ne!(new_gen, Some(old_gen), "同位置的代次必须换成新的");
        let info = store.find_entries_since(&stale, u64::MAX).unwrap();
        assert_eq!(
            info.token_reset,
            Some(TokenResetReason::SegmentReplaced),
            "同位置、不同代次 ⇒ 段号被复用 ⇒ 游标作废"
        );
        store.close().unwrap();
    }

    /// 重定位的三种原因逐个盯死;合法游标**不得**被误重置(否则每次拉取都从头重扫)。
    #[test]
    fn revalidate_reports_reasons_and_keeps_valid_cursors() {
        let dir = temp_dir("revalidate");
        let seg = seg_cap(4);
        let store = BlobStore::create(dir.path(), seg * 8, seg, 4).unwrap();
        for i in 0..8u64 {
            store.put(key(i), &data_for(i, 0)).unwrap();
        }

        // 段不存在(位置 999 没有段)。
        let missing = store
            .find_entries_since(&FindToken::at(LogSegmentName::new(999, 1), 0), u64::MAX)
            .unwrap();
        assert_eq!(
            missing.token_reset,
            Some(TokenResetReason::SegmentMissing),
            "段不存在 ⇒ 重定位"
        );
        assert_eq!(missing.entries.len(), 8, "重定位后从头重扫:全都在");

        // 偏移越界(位置 0 有段、代次也对,但偏移越过数据区)。
        let gen0 = store.inner().log.generation_of(0).expect("段 0 存在");
        let beyond = store
            .find_entries_since(
                &FindToken::at(LogSegmentName::new(0, u64::from(gen0)), u64::MAX),
                u64::MAX,
            )
            .unwrap();
        assert_eq!(
            beyond.token_reset,
            Some(TokenResetReason::OffsetOutOfRange),
            "偏移越过段数据区 ⇒ 重定位"
        );

        // 合法游标(批量拉取中途)⇒ 不重定位,且只给增量。
        let mut token = FindToken::Uninitialized;
        let mut reset_seen = false;
        for _ in 0..8 {
            let info = store.find_entries_since(&token, 1).unwrap();
            reset_seen |= info.token_reset.is_some();
            token = info.token;
        }
        assert!(!reset_seen, "合法游标一律不得被重置");
        let tail = store.find_entries_since(&token, u64::MAX).unwrap();
        assert!(tail.entries.is_empty(), "已到末端");

        // 身份相关的原因由**服务层**置入(引擎看不到跨库身份):这里只钉住短名稳定。
        assert_eq!(
            TokenResetReason::IncarnationMismatch.as_str(),
            "incarnation_mismatch"
        );
        assert_eq!(
            TokenResetReason::SegmentReplaced.as_str(),
            "segment_generation_mismatch"
        );
        assert_eq!(TokenResetReason::SegmentMissing.as_str(), "segment_missing");
        assert_eq!(
            TokenResetReason::OffsetOutOfRange.as_str(),
            "offset_out_of_range"
        );
        store.close().unwrap();
    }
}

#[cfg(test)]
mod record_len_tests {
    use super::*;

    /// 记录内 `data_len` 是 u32:恰好 `u32::MAX` 放行,再多一个字节必须**拒绝**
    /// (静默回绕会写出长度自相矛盾的记录,之后任何读取都 `Corrupt` —— 持久性红线)。
    ///
    /// 这条测试刻意只测**长度策略本身**(不需要真分配 4 GiB):
    /// 走 HTTP 的两条写路径(inline ≤1 MiB / 流式)都汇到 `record_data_len`。
    #[test]
    fn record_data_len_accepts_up_to_u32_max_and_rejects_more() {
        assert_eq!(record_data_len(0).unwrap(), 0);
        assert_eq!(record_data_len(u32::MAX as u64).unwrap(), u32::MAX);
        let err = record_data_len(u32::MAX as u64 + 1).unwrap_err();
        assert!(matches!(err, Error::BadArgument(_)), "got {err:?}");
        assert!(
            err.to_string().contains("exceeds"),
            "报错要说清是长度超限: {err}"
        );
    }

    /// 64 位平台才存在"超过 u32 的 `usize`";16/32 位平台该分支不可达,故不测。
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn bulk_put_path_rejects_oversized_slice_via_policy() {
        // 整块写路径无法在不分配 4 GiB 的前提下构造真实入参,故这里断言它**调用的那条策略**。
        // 结构性的保证在类型层:`encode_record` 现在返回 `Result<Vec<u8>>`、内部用 `record_data_len`
        // 做 `u32::try_from`(截断所需的 `as u32` 已从代码里删除),而 6 个调用点都写了 `?` ——
        // 任何人把它改回"静默截断"都会**编不过**,不依赖这条测试去发现。
        assert!(record_data_len(u32::MAX as u64 + 1).is_err(), "策略必须拒");
        assert_eq!(
            record_data_len((1 << 20) as u64).unwrap(),
            1 << 20,
            "常规大小照常放行"
        );
    }
}
