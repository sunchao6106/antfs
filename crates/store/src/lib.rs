//! ambry-store: Rust 重写的 Ambry 存储引擎。
//!
//! IO 层:同步阻塞 IO(buffered),数据靠 OS page cache;索引走 std::fs / mmap。
//! 已实现:`LogSegment` / `Log` / `PersistentIndex` / `BlobStore`。

pub mod blob;
pub mod error;
pub mod index;
pub mod log;
pub mod log_segment;
pub mod types;

pub use blob::BlobStore;
pub use error::{Error, Result};
pub use index::{IndexValue, PersistentIndex};
pub use log::Log;
pub use log_segment::LogSegment;
pub use types::{LogOffset, LogSegmentName, StoreKey, KEY_SIZE};
