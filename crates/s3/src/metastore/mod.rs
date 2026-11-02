pub mod types;

use async_trait::async_trait;
use std::sync::Arc;
use types::*;

use crate::config::MetaStoreConfig;
use crate::error::S3Error;

pub type Result<T> = std::result::Result<T, S3Error>;

/// ListObjects/ListUploads 前缀的**字面上界**:任何以 `prefix` 开头的键都严格小于它
/// (末位字符进一并截断;逐位顶格到 Unicode 顶 = 无上界)。前缀查询借此做**纯范围扫**
/// `key >= prefix AND key < upper`,替代 LIKE —— 前缀中获得**字面语义**(`_`/`%`/`\`
/// 不再碰通配符,对齐 seaweedfs#11737 的教训),范围谓词也让索引直接收窄扫描区间。
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(c) = chars.pop() {
        let mut next = u32::from(c) + 1;
        while next <= u32::from(char::MAX) && char::from_u32(next).is_none() {
            // 跳过 UTF-16 代理区等无效码位。
            next += 1;
        }
        if let Some(nc) = char::from_u32(next) {
            chars.push(nc);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// 级联回收(H3,2026-10-26 裁决):对象行 `parts_manifest` JSON → 全部片 BlobId。
/// 解析失败(损坏/异构)→ 空集 + warn —— 级联退化为旧行为(只回收主 blob),
/// 不让一行坏 manifest 阻塞删除/覆盖的主语义。
pub(crate) fn manifest_part_ids(manifest: Option<&str>) -> Vec<BlobId> {
    let Some(json) = manifest else {
        return Vec::new();
    };
    match serde_json::from_str::<Vec<ManifestPart>>(json) {
        Ok(parts) => parts
            .into_iter()
            .filter_map(|p| BlobId::from_slice(&p.blob_id))
            .collect(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "cascade reclaim: parts_manifest unparseable, primary-only fallback"
            );
            Vec::new()
        }
    }
}

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    // Object CRUD
    async fn put_object(&self, req: PutObjectMeta) -> Result<PutObjectOutcome>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<DeletedObject>>;
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<Vec<DeleteResult>>;
    async fn list_objects(&self, req: ListObjectsReq) -> Result<ListObjectsResp>;

    // Bucket CRUD
    async fn create_bucket(&self, name: &str, owner: &str) -> Result<()>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn head_bucket(&self, name: &str) -> Result<Option<BucketMeta>>;
    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>>;
    async fn get_bucket_quota(&self, bucket: &str) -> Result<Option<BucketQuota>>;
    async fn set_bucket_quota(&self, bucket: &str, quota_bytes: u64) -> Result<()>;
    async fn set_bucket_acl(&self, bucket: &str, acl: BucketAcl) -> Result<()>;
    /// 桶策略 upsert(`s3-bucket-policy`):doc = 调用方已全验的 JSON;None = 撤(幂等)。
    /// 桶不存在 → NoSuchBucket;行不动(仅 policy 列)。
    async fn set_bucket_policy(&self, bucket: &str, policy: Option<&str>) -> Result<()>;
    /// 桶 ACL 授予集 upsert(`s3-acl-xml`;None = 撤,幂等)。
    async fn set_bucket_acl_grants(&self, bucket: &str, grants: Option<&str>) -> Result<()>;
    /// 对象 ACL 授予集 upsert(就地 UPDATE state=1 当前行,不 bump 版本;无活行 →
    /// NoSuchKey)。
    async fn set_object_acl_grants(
        &self,
        bucket: &str,
        key: &str,
        grants: Option<&str>,
    ) -> Result<()>;

    // Multipart
    async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: &UserMeta,
    ) -> Result<String>;
    async fn put_part(&self, upload_id: &str, part: PartMeta) -> Result<Option<BlobId>>;
    async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletePart],
        checksum: CompleteChecksum,
    ) -> Result<CompleteMultipartResult>;
    async fn abort_multipart(&self, upload_id: &str) -> Result<Vec<BlobId>>;
    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> Result<ListPartsResp>;
    /// 会话全部 part 行(按 part_number 升序,含 migration 007 校验两列)——complete 级
    /// FULL_OBJECT 预检按 manifest 映射片 blob 用(handler 侧只读)。
    async fn session_parts(&self, upload_id: &str) -> Result<Vec<PartMeta>>;
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        opts: ListUploadsOpts,
    ) -> Result<ListUploadsResp>;

    // Auth
    async fn lookup_credential(&self, access_key: &str) -> Result<Option<S3Credential>>;

    // Credential management
    async fn create_credential(&self, owner: &str) -> Result<S3Credential>;
    async fn delete_credential(&self, access_key: &str) -> Result<bool>;

    // Garbage collection
    /// 清理已过保留期的遮蔽/软删行(state=0 且 `deleted_at < cutoff`),返回清掉的**行数**。
    /// blob 已在删除/遮蔽时被 handler 删掉,此处不碰 blob。单语句原子取件(并发实例不双删)。
    async fn purge_expired_deletions(&self, cutoff_ms: i64, limit: u64) -> Result<u64>;
    /// 取走过期未 complete 的 multipart session(`created_at < cutoff`),返回 upload_id 集。
    /// 调用方拿到者对这些 session 做孤儿清理(parts 行删 + 逐 blob 墓碑)。
    async fn take_expired_uploads(&self, cutoff_ms: i64, limit: u64) -> Result<Vec<String>>;
    /// 删除某 session 的全部 parts 行,返回 blob_id 集(孤儿回收专用;`take_expired_uploads`
    /// 已删 session 行,`abort_multipart` 的存在性前置不成立,故给独立原语)。
    async fn delete_parts_of_session(&self, upload_id: &str) -> Result<Vec<BlobId>>;
    /// 回收 WAL:批读待删 blob(非破坏;GC 兜底第 3 面用,行只在 resolve 时消失)。
    async fn pending_reclaims_batch(&self, limit: u64) -> Result<Vec<BlobId>>;
    /// 回收 WAL:墓碑成功后消账(删行);返回删掉行数。
    async fn resolve_pending_reclaims(&self, blob_ids: &[BlobId]) -> Result<u64>;
    /// 回收 WAL:当前积压行数(gauge 采样用)。
    async fn pending_reclaims_count(&self) -> Result<u64>;

    /// SSE 行扫描(主钥轮换离线工具用):`state=1 且 sse 非空`的行按 `(bucket, key)`
    /// 升序整 `ObjectMeta` 吐回;`bucket` = 域(None = 全库;按桶金丝雀轮换/测试隔离);
    /// `after` = 排他游标(上一页末行),`limit` = 页大小。只读无事务副作用;就地重写
    /// 已扫过的行落在游标左侧,分页无幻读环。
    async fn scan_sse_objects(
        &self,
        bucket: Option<&str>,
        after: Option<(String, String)>,
        limit: u64,
    ) -> Result<Vec<ObjectMeta>>;
}

/// complete 级 composite 的部件值形态:manifest 片**全员有值且同族** → 返回该算法与
/// 值表(manifest 序);否则 None(自动推导静默放弃)或 Err(显式 composite 声明时
/// 响亮报违约形态)。
pub(crate) fn uniform_part_values(
    listed: &[Option<StoredChecksum>],
) -> std::result::Result<(crate::checksum::ChecksumAlgo, Vec<String>), S3Error> {
    let mut values: Vec<String> = Vec::with_capacity(listed.len());
    for (idx, ck) in listed.iter().enumerate() {
        match ck {
            Some(c) => values.push(c.value.clone()),
            None => {
                return Err(S3Error::InvalidRequest(format!(
                    "part {} was uploaded without a checksum; a composite complete-level \
                    value cannot be derived or verified",
                    idx + 1
                )))
            }
        }
    }
    let Some(first) = listed.first().and_then(|c| c.as_ref()) else {
        return Err(S3Error::InvalidRequest(
            "no part carries a checksum; composite cannot be derived".into(),
        ));
    };
    let algo = first.algo;
    if listed
        .iter()
        .any(|c| c.as_ref().map(|x| x.algo) != Some(algo))
    {
        return Err(S3Error::InvalidRequest(
            "parts carry mixed checksum algorithms; composite requires one family".into(),
        ));
    }
    if !matches!(
        algo,
        crate::checksum::ChecksumAlgo::Crc32 | crate::checksum::ChecksumAlgo::Crc32c
    ) {
        return Err(S3Error::InvalidRequest(
            "composite complete-level checksums are only defined for the CRC family".into(),
        ));
    }
    Ok((algo, values))
}

/// 自动推导(None 声明路径):只有全员同族 CRC 有值才出 composite,否则放弃(行校验
/// 为空)——静默放弃面与显式声明的响亮拒面严格分开。
pub(crate) fn derive_composite_if_uniform(
    listed: &[Option<StoredChecksum>],
) -> Option<StoredChecksum> {
    if listed.iter().any(|c| c.is_none()) {
        return None;
    }
    let outcome = uniform_part_values(listed).ok()?;
    let algo = outcome.0;
    let value = crate::checksum::derive_composite(algo, &outcome.1)?;
    Some(StoredChecksum {
        algo,
        value,
        kind: crate::checksum::ChecksumKind::Composite,
    })
}

pub async fn connect(cfg: &MetaStoreConfig) -> std::result::Result<Arc<dyn MetaStore>, S3Error> {
    match cfg.driver.as_str() {
        "postgres" => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(cfg.max_connections)
                .connect(&cfg.url)
                .await?;
            sqlx::migrate!("src/metastore/migrations/postgres")
                .run(&pool)
                .await
                .map_err(|e| S3Error::InternalError(format!("pg migration: {e}")))?;
            Ok(Arc::new(pg::PgMetaStore::new(pool)))
        }
        "mysql" => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(cfg.max_connections)
                .connect(&cfg.url)
                .await?;
            sqlx::migrate!("src/metastore/migrations/mysql")
                .run(&pool)
                .await
                .map_err(|e| S3Error::InternalError(format!("mysql migration: {e}")))?;
            Ok(Arc::new(mysql::MysqlMetaStore::new(pool)))
        }
        other => Err(S3Error::InternalError(format!(
            "unsupported metastore driver: {other}"
        ))),
    }
}

pub mod cache;
pub mod mysql;
pub mod pg;

#[cfg(test)]
mod tests {
    use super::prefix_upper_bound;

    /// 字面上界的进位/截断/顶格三态(比直接手写口算更稳的钉子)。
    #[test]
    fn prefix_upper_bound_semantics() {
        assert_eq!(prefix_upper_bound("ab").as_deref(), Some("ac"));
        assert_eq!(prefix_upper_bound("az").as_deref(), Some("a{"));
        assert_eq!(prefix_upper_bound("logs/").as_deref(), Some("logs0"));
        assert_eq!(
            prefix_upper_bound("call_03_K").as_deref(),
            Some("call_03_L")
        );
        // 代理区跳过:0xFFFF 的下一个有效字符是 U+10000。
        assert_eq!(
            prefix_upper_bound("ab\u{FFFF}").as_deref(),
            Some("ab\u{10000}")
        );
        // 逐位顶格:末位顶格即弃位进位("a+顶格" 的上界是 "b" —— 一切 "a…" 都 < "b");
        // 整串顶格才无上界(调用方退化为仅下界)。
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
        // 不变式:任何以 prefix 开头的串都严格小于上界。
        // 不变式只覆盖真正的前缀延伸(以 prefix 开头者);非延伸串不在上界的义务内。
        let ub = prefix_upper_bound("abz").expect("上界");
        for cand in ["abz", "abzz", "abz\u{10FFFF}", "abzz\u{10FFFF}{"] {
            assert!(cand < ub.as_str(), "{cand} 必须小于上界 {ub}");
        }
    }
}
