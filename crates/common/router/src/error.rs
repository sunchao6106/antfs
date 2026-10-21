//! client(router)错误类型(`thiserror` + `?` 传播,CLAUDE.md §2)。
//!
//! [`Error::kind`] 是**稳定契约**:既作 Prometheus label,也作压测的错误分类口径,改动须同步文档。

use antfs_master::PartitionId;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// 配置非法。
    #[error("config: {0}")]
    Config(#[from] antfs_config::ConfigError),

    /// 连 master gRPC 失败。
    #[error("connect master: {0}")]
    Connect(#[from] tonic::transport::Error),

    /// 指标注册失败。
    #[error("metric: {0}")]
    Metric(#[from] antfs_metric::Error),

    /// 数据面 HTTP 客户端构造失败(TLS/超时配置非法等,正常配置不会发生)。
    #[error("build http client: {0}")]
    HttpClient(reqwest::Error),

    /// master gRPC 调用失败(传输/状态)。
    #[error("master rpc: {0}")]
    Master(#[from] tonic::Status),

    /// master 明确拒绝(业务性 `ok=false`,非传输错误)。
    #[error("master rejected: {0}")]
    Rejected(String),

    /// master 是 follower 而本路由把请求打向了它(发号等写路径)。`leader_addr` 是 follower
    /// 回填的 leader gRPC 地址(节点自报注册的唯一真值;注册窗口期为 `None` 调用方重试)。
    /// router 内建自愈:发号路径遇带提示的 NotLeader 会单飞切换 master 通道并重试一次
    ///(`master-leader-address`);其余路径原样上抛供上层处置。
    #[error("master is not the leader ({detail}); leader: {leader_addr:?}")]
    NotLeader {
        detail: String,
        leader_addr: Option<String>,
    },

    /// 没有可写分区(集群未建分区 / 全是 RO/SEALED / 无可达副本)。
    #[error("no writable partition")]
    NoWritablePartition,

    /// 该分区的 objectId 空间耗尽(应封存该分区;绝不复用)。
    #[error("blob id space exhausted for partition {0}; seal it")]
    IdExhausted(PartitionId),

    /// 数据面 HTTP 传输失败(连不上/超时)。
    #[error("http to replica {addr}: {source}")]
    Http {
        addr: String,
        #[source]
        source: reqwest::Error,
    },

    /// 副本返回了非预期的状态码。
    #[error("replica {addr} returned {status}: {detail}")]
    ReplicaStatus {
        addr: String,
        status: u16,
        detail: String,
    },

    /// 该 blobId 在副本上已存在 —— 新发的号段**不可能**这样,出现即说明号被复用(严重)。
    #[error("blob already exists on replica {addr}: blob id was reused")]
    AlreadyExists { addr: String },

    /// 分区写满(HTTP 507):该加分区了。
    #[error("replica {addr} is out of capacity (partition full)")]
    StoreFull { addr: String },

    /// 该节点没有此分区的副本(HTTP 404):路由到了错节点。
    #[error("replica {addr} has no replica for partition {pid}")]
    NoReplica { addr: String, pid: PartitionId },

    /// 操作被该节点**请求静默**(HTTP 503 + `Retry-After`):运维止血,不是故障。
    #[error("replica {addr} has silenced this operation (retry after {retry_after_secs:?}s)")]
    Silenced {
        addr: String,
        retry_after_secs: Option<u64>,
    },

    /// 对象不存在(所有副本都报 404)。
    #[error("blob not found on any replica of partition {pid}")]
    NotFound { pid: PartitionId },

    /// 写未达多数派。
    #[error("write quorum not reached: {acks}/{required} acks of {total} replicas ({detail})")]
    Quorum {
        acks: usize,
        required: usize,
        total: usize,
        detail: String,
    },

    /// blobId 自带的 partition 与目标 partition 不一致(拼错了,会导致索引与 URL 对不上)。
    #[error("blob id partition {key_pid} does not match target partition {expected}")]
    PartitionMismatch {
        key_pid: PartitionId,
        expected: PartitionId,
    },

    /// 本地文件 IO(读写上传/下载的文件)。
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// 恒切管线容量拒绝:body 超过 `put_bytes_chunk_bytes × MAX_CHUNKS`
    /// (描述符片数上限);一字节未写,调用方可缩小重试或走流式口。
    #[error("blob of {len} bytes exceeds the chunked-put capacity of {max} bytes")]
    TooLarge { len: u64, max: u64 },

    /// 拼装对象读回时描述符/片不一致(上游损坏类,不静默忽略 —— CLAUDE.md §3.3)。
    #[error("bad composed blob: {0}")]
    BadComposed(&'static str),
}

impl Error {
    /// 稳定的错误分类(指标 label / 压测错误分桶的唯一真值)。
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Config(_) => "config",
            Error::Connect(_) => "connect",
            Error::Metric(_) => "metric",
            Error::HttpClient(_) => "http_client",
            Error::Master(_) => "rpc",
            Error::Rejected(_) => "rejected",
            Error::NotLeader { .. } => "not_leader",
            Error::NoWritablePartition => "no_writable_partition",
            Error::IdExhausted(_) => "id_exhausted",
            Error::Http { .. } => "http",
            Error::ReplicaStatus { .. } => "status",
            Error::AlreadyExists { .. } => "already_exists",
            Error::StoreFull { .. } => "store_full",
            Error::NoReplica { .. } => "no_replica",
            Error::Silenced { .. } => "silenced",
            Error::NotFound { .. } => "not_found",
            Error::Quorum { .. } => "quorum",
            Error::PartitionMismatch { .. } => "partition_mismatch",
            Error::Io(_) => "io",
            Error::TooLarge { .. } => "too_large",
            Error::BadComposed(_) => "bad_composed",
        }
    }

    /// 失败是否"值得重试"(传输类/被静默/暂时无副本),供上层决定是否退避重试。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Http { .. }
                | Error::Silenced { .. }
                | Error::NoReplica { .. }
                | Error::Master(_)
                | Error::IdExhausted(_)
        )
    }
}
