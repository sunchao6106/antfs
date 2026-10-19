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
    /// `s3-sse` 描述符(JSON `{"alg","key_id"}`);None = 明文(历史行 NULL 列同此)。
    pub sse: Option<String>,
    /// flexible checksum(`s3-checksums`):最强一份(算法+base64 值,明文口径);
    /// None = 未随写声明(multipart 装配行/拷贝产物/历史行同此)。
    pub checksum: Option<StoredChecksum>,
}

/// 行内持久化的 flexible checksum(migration 005 两可空列的读侧形态)。
#[derive(Debug, Clone)]
pub struct StoredChecksum {
    pub algo: crate::checksum::ChecksumAlgo,
    pub value: String,
    /// 值口径(migration 007 列;历史行读侧归 FULL_OBJECT)。
    pub kind: crate::checksum::ChecksumKind,
}

impl StoredChecksum {
    /// 双可空列 → 形态(半行 = 脏数据,按无值处理并告警,不 500:回显缺省属
    /// 优雅降级,读写路径都不该被历史半写打穿)。
    /// 对象行三列读(named_objects checksum_algo/b64/type);type NULL = FULL_OBJECT
    /// (migration 005 历史行),野值按缺值告警宽容。
    pub fn from_row(
        algo: Option<String>,
        value: Option<String>,
        kind: Option<String>,
    ) -> Option<Self> {
        let kind = match kind.as_deref() {
            None | Some("FULL_OBJECT") => crate::checksum::ChecksumKind::FullObject,
            Some("COMPOSITE") => crate::checksum::ChecksumKind::Composite,
            Some(other) => {
                tracing::warn!(kind = other, "unknown checksum_type, treating as absent");
                return None;
            }
        };
        match (algo, value) {
            (Some(a), Some(v)) => {
                let algo = crate::checksum::ChecksumAlgo::from_algo_name(&a)?;
                Some(StoredChecksum {
                    algo,
                    value: v,
                    kind,
                })
            }
            (None, None) => None,
            _ => {
                tracing::warn!(
                    "named_objects row has half-written checksum columns, treating as absent"
                );
                None
            }
        }
    }

    /// part 行两列读(multipart_parts;part 值恒为单片 FULL_OBJECT)。
    pub fn from_pair(algo: Option<String>, value: Option<String>) -> Option<Self> {
        Self::from_row(algo, value, None)
    }
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
    /// `s3-sse` 描述符;Some → blob 为 SSE 帧(读侧须解帧),None = 明文。
    pub sse: Option<String>,
    /// 行内 flexible checksum(None → 回显面缺省)。
    pub checksum: Option<StoredChecksum>,
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
    /// UploadPart 已验证的最强声明(migration 007;None = 该片未带校验上传)。
    pub checksum: Option<StoredChecksum>,
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
    /// complete 级裁决后的行存校验(composite 推导产物/FULL_OBJECT 验证产物;None = 无)。
    pub checksum: Option<StoredChecksum>,
}

/// CompleteMultipartUpload 的 complete 级校验声明(handler 裁决后传入 metastore;
/// composite 的部件族性/同族性比对在会话锁内终判)。
#[derive(Debug, Clone)]
pub enum CompleteChecksum {
    /// 无声明(若 manifest 片全员同族 CRC 有值,仍自动推导 composite 落行)。
    None,
    /// FULL_OBJECT:handler 已流式全验的最强值。
    FullObject(StoredChecksum),
    /// COMPOSITE:claimed = SDK 供值(Some)或纯推导(None)。
    Composite { claimed: Option<String> },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_row_decodes_kinds_and_tolerates_legacy() {
        // 007 前历史行:两列有值、type NULL → FULL_OBJECT。
        let legacy = StoredChecksum::from_row(Some("SHA256".into()), Some("QUJD".into()), None)
            .expect("legacy row decodes");
        assert_eq!(legacy.kind, crate::checksum::ChecksumKind::FullObject);

        let composite = StoredChecksum::from_row(
            Some("CRC32".into()),
            Some("QUJD".into()),
            Some("COMPOSITE".into()),
        )
        .expect("composite kind decodes");
        assert_eq!(composite.kind, crate::checksum::ChecksumKind::Composite);

        // 野值 kind:告警宽容(按缺值处理)。
        assert!(StoredChecksum::from_row(
            Some("CRC32".into()),
            Some("QUJD".into()),
            Some("BASE64".into())
        )
        .is_none());

        // 半写行(既有 005 契约):缺一列 → None。
        assert!(StoredChecksum::from_row(Some("CRC32".into()), None, None).is_none());
        assert!(StoredChecksum::from_row(None, Some("QUJD".into()), None).is_none());

        // part 行两列读:恒 FULL_OBJECT。
        let part = StoredChecksum::from_pair(Some("CRC32".into()), Some("QUJD".into()))
            .expect("part row decodes");
        assert_eq!(part.kind, crate::checksum::ChecksumKind::FullObject);
    }
}
