//! store:Ambry 存储引擎的 Rust 翻译(对标 Ambry `ambry-store` 模块)。
//!
//! IO 层:同步阻塞 IO(buffered),数据靠 OS page cache;索引走 std::fs / mmap。
//! 已实现:`LogSegment` / `Log` / `PersistentIndex` / `BlobStore`。

pub mod blob;
pub mod config;
pub mod error;
pub mod index;
pub mod log;
pub mod log_segment;
pub mod metrics;
pub mod types;

pub use blob::{
    ApplyOutcome, BlobStore, CompactPolicy, CompactionStats, FindInfo, FindToken, MessageInfo,
    ReplicatedRecord, TokenResetReason, VerifyReport, LOG_INCARNATION_FILE,
};
pub use config::StoreConfig;
pub use error::{Error, Result};
/// "不过期"哨兵(`i64::MAX` 语义见 value 模块):`put_from_reader` / HTTP 层的 `expiration_ms` 缺省值。
pub use index::value::NO_EXPIRATION;
pub use index::{IndexValue, PersistentIndex};
pub use log::Log;
pub use log_segment::{peek_generation, LogSegment};
pub use metrics::{StoreMetricHandles, StoreMetrics};
pub use types::{LogOffset, LogSegmentName, StoreKey, KEY_SIZE};
