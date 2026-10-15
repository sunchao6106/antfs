use antfs_blob::StoreKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type BlobId = StoreKey;
pub type UserMeta = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct PutObjectMeta {
    pub bucket: String,
    pub key: String,
    pub blob_id: BlobId,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub user_meta: UserMeta,
    pub parts_manifest: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ObjectVersion {
    pub version: i64,
}

/// `put_object` / `complete_multipart` 的结果:新版本号 + 同语句序列里**被遮蔽**的旧版本
/// blob_id 集(state 置 0)。handler 须对每个 shadowed blob 调 `router.delete` 兜底删除
/// (overwrite 无版本化 = 旧数据立即可弃;引擎 delete 幂等,重复删安全)。
#[derive(Debug, Clone)]
pub struct PutObjectOutcome {
    pub version: i64,
    pub shadowed_blobs: Vec<BlobId>,
}

#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub blob_id: BlobId,
    pub version: i64,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub created_at: i64,
    pub user_meta: UserMeta,
    pub parts_manifest: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeletedObject {
    pub blob_id: BlobId,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct DeleteResult {
    pub key: String,
    pub deleted: bool,
    pub error: Option<String>,
}

/// 桶级存储配额与占用记账的交接视图(字节;quota 0 = 不限)。守卫/扣减在 put/delete/
/// complete 的记账事务里维护,GC purge 不触碰(state=0 行不在计数集合)。
#[derive(Debug, Clone)]
pub struct BucketQuota {
    pub name: String,
    pub quota_bytes: u64,
    pub used_bytes: u64,
}

/// 桶级 canned ACL(s3-canned-acl 一期档):`private` = owner 独占(默认,首轮 ACL 语义);
/// `public-read` = 读面(GET/HEAD object + ListObjectsV2)对任何人开放(含匿名),
/// 写面仍 owner 独占。object ACL / 桶策略 JSON / 跨 owner 授权不在一期(见 change 档)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketAcl {
    Private,
    PublicRead,
}

impl BucketAcl {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
        }
    }

    /// 白名单解析;未知值 `None`(调用方决定 400/兜底口径,见 `BucketMeta::acl` 读入兜底)。
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "private" => Some(Self::Private),
            "public-read" => Some(Self::PublicRead),
            _ => None,
        }
    }
}

/// 桶元数据。`acl` 从 DB 读出;未知值兜底 `Private`(写路径白名单保证不出现,兜底取严侧)。
#[derive(Debug, Clone)]
pub struct BucketMeta {
    pub name: String,
    pub owner: String,
    pub created_at: i64,
    pub acl: BucketAcl,
}

#[derive(Debug, Clone)]
pub struct PartMeta {
    pub part_number: i32,
    pub blob_id: BlobId,
    pub size: u64,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct CompletePart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartResult {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub blob_id: BlobId,
    pub size: u64,
    pub parts_manifest: String,
    /// 同 key 被 complete 遮蔽的旧版本 blob(handler 需逐一 `router.delete`,见 `PutObjectOutcome`)。
    pub shadowed_blobs: Vec<BlobId>,
    /// 已上传但**未列入 manifest** 的 part blob(S3 语义:complete 即丢弃)——会话行删除竞态下
    /// 被顶替换下的旧 part blob 不在此列(put_part 已把 old 恰一次交还给上传方回收)。
    /// handler 需逐一 `router.delete`,否则这些 blob 无墓碑、GC 永不收。
    pub discarded_part_blobs: Vec<BlobId>,
}

#[derive(Debug, Clone)]
pub struct S3Credential {
    pub access_key: String,
    pub secret_key: String,
    pub owner: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct ListObjectsReq {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: i32,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: i64,
}

#[derive(Debug, Clone)]
pub struct ListObjectsResp {
    pub entries: Vec<ObjectEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub key_count: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ListPartsOpts {
    pub max_parts: i32,
    pub part_number_marker: i32,
}

#[derive(Debug, Clone)]
pub struct PartEntry {
    pub part_number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct ListPartsResp {
    pub parts: Vec<PartEntry>,
    pub is_truncated: bool,
    pub next_part_number_marker: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ListUploadsOpts {
    pub prefix: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub max_uploads: i32,
}

#[derive(Debug, Clone)]
pub struct UploadEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated: i64,
}

#[derive(Debug, Clone)]
pub struct ListUploadsResp {
    pub uploads: Vec<UploadEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestPart {
    pub blob_id: [u8; 8],
    pub size: u64,
}
