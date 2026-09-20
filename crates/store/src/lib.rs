//! ambry-store: Rust 重写的 Ambry 存储引擎(局部替换为业界最新实现)。
//!
//! IO 层:monoio + io_uring + O_DIRECT(仅 Linux)。
//! 已实现:`LogSegment` / `Log`;后续补 `PersistentIndex`。

pub mod aligned;
pub mod blob;
pub mod error;
pub mod index;
pub mod log;
pub mod log_segment;
pub mod types;

pub use aligned::{AlignedBuf, ALIGN};
pub use blob::BlobStore;
pub use error::{Error, Result};
pub use index::{IndexValue, PersistentIndex};
pub use log::Log;
pub use log_segment::LogSegment;
pub use types::{LogOffset, LogSegmentName, StoreKey, KEY_SIZE};
