use thiserror::Error;

/// 存储引擎错误。
#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 追加时容量不足。
    #[error("log segment is full")]
    SegmentFull,

    /// 读取范围超出已写入数据。
    #[error("read out of range: offset={offset} len={len} data_end={data_end}")]
    OutOfRange {
        offset: u64,
        len: usize,
        data_end: u64,
    },

    /// 段文件损坏(magic/crc 校验失败等)。
    #[error("corrupt segment: {0}")]
    Corrupt(String),

    /// 参数非法。
    #[error("bad argument: {0}")]
    BadArgument(String),

    /// 查找的 key 不存在。
    #[error("blob not found")]
    NotFound,

    /// key 存在但已被删除(tombstone)。
    #[error("blob deleted")]
    Deleted,

    /// undelete 目标当前未处于已删除态(存活/已复活),不可复活。
    #[error("blob not deleted")]
    NotDeleted,

    /// key 存在但已过期(TTL)。
    #[error("blob expired")]
    Expired,

    /// put 的 key 已存在(BlobId 冲突,与 Ambry 一致不覆盖)。
    #[error("blob already exists")]
    AlreadyExists,
}

pub type Result<T> = std::result::Result<T, Error>;
