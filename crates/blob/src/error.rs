use thiserror::Error;

/// 存储引擎错误。
#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 追加时容量不足。
    #[error("log segment is full")]
    SegmentFull,

    /// 整个 store(定容 replica)已写满,无法再接纳新记录(对齐 Ambry `Store_Full`)。
    /// 上层据此把新写路由到其他 partition(滚动 partition 属集群层职责,不在 store)。
    #[error("store is full")]
    StoreFull,

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

    /// key 存在但已过期(TTL)。
    #[error("blob expired")]
    Expired,

    /// put 的 key 已存在(BlobId 冲突,与 Ambry 一致不覆盖)。
    #[error("blob already exists")]
    AlreadyExists,

    /// store 因连续 IO 错误达阈值自关,不再服务(对齐 Ambry IO-error 自关;需重启/重开恢复)。
    #[error("store unavailable (shut down after repeated io errors)")]
    Unavailable,
}

pub type Result<T> = std::result::Result<T, Error>;
