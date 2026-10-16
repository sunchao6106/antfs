use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

use antfs_blob::StoreKey;

use super::types::*;
use super::{MetaStore, Result};
use crate::error::S3Error;

pub struct PgMetaStore {
    pool: PgPool,
}

impl PgMetaStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn gen_version() -> i64 {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let random_part = (uuid::Uuid::new_v4().as_u128() % 1000) as i64;
        now / 1000 + random_part
    }

    /// 回收 WAL 入账(幂等;调用点都在语义事务里,失败随主事务回滚)。
    async fn queue_reclaims(tx: &mut sqlx::PgTransaction<'_>, blob_ids: &[StoreKey]) -> Result<()> {
        for bid in blob_ids {
            sqlx::query(
                "INSERT INTO pending_reclaims (blob_id, queued_at) VALUES ($1, $2)
                 ON CONFLICT (blob_id) DO NOTHING",
            )
            .bind(bid.as_bytes().as_slice())
            .bind(Self::now_ms())
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl MetaStore for PgMetaStore {
    async fn put_object(&self, req: PutObjectMeta) -> Result<PutObjectOutcome> {
        let (stored_algo_string, stored_value) = match &req.checksum {
            Some(c) => (Some(c.algo.algo_name().to_string()), Some(c.value.clone())),
            None => (None, None),
        };
        let version = Self::gen_version();
        let now = Self::now_ms();
        let meta_json = if req.user_meta.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&req.user_meta).unwrap_or_default())
        };
        // 记账事务:遮蔽(先圈出字节)→ 配额净增量守卫(单语句原子)→ INSERT,一并提交。
        let mut tx = self.pool.begin().await?;

        // 遮蔽同 key 旧活行(overwrite 语义:旧数据立即可弃)。紧邻 INSERT;
        // 并发 overwrite 自然满足 last-writer-wins(version 单调)。
        let shadowed: Vec<(Vec<u8>, i64)> = sqlx::query_as(
            "UPDATE named_objects SET state = 0, deleted_at = $3
             WHERE bucket = $1 AND key = $2 AND state = 1 AND deleted_at IS NULL AND version < $4
             RETURNING blob_id, size",
        )
        .bind(&req.bucket)
        .bind(&req.key)
        .bind(now)
        .bind(version)
        .fetch_all(&mut *tx)
        .await?;
        let shadow_bytes: i64 = shadowed.iter().map(|(_, s)| *s).sum();

        // 配额守卫,净增量口径(used + 新写 − 遮蔽 ≤ quota;quota=0 不限)。
        let new_size = req.size as i64;
        let guard = sqlx::query(
            "UPDATE buckets SET used_bytes = used_bytes + $2 - $3
             WHERE name = $1 AND (quota_bytes = 0 OR used_bytes + $2 - $3 <= quota_bytes)",
        )
        .bind(&req.bucket)
        .bind(new_size)
        .bind(shadow_bytes)
        .execute(&mut *tx)
        .await?;
        if guard.rows_affected() == 0 {
            tx.rollback().await?;
            let quota: Option<(i64, i64)> =
                sqlx::query_as("SELECT quota_bytes, used_bytes FROM buckets WHERE name = $1")
                    .bind(&req.bucket)
                    .fetch_optional(&self.pool)
                    .await?;
            return match quota {
                None => Err(S3Error::NoSuchBucket),
                Some((q, u)) => Err(S3Error::QuotaExceeded {
                    bucket: req.bucket.clone(),
                    quota: q.max(0) as u64,
                    used: u.max(0) as u64,
                }),
            };
        }

        sqlx::query(
            "INSERT INTO named_objects (bucket, key, version, blob_id, state, size, etag, content_type, created_at, user_meta, parts_manifest, sse, checksum_algo, checksum_b64)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(&req.bucket)
        .bind(&req.key)
        .bind(version)
        .bind(req.blob_id.as_bytes().as_slice())
        .bind(new_size)
        .bind(&req.etag)
        .bind(&req.content_type)
        .bind(now)
        .bind(&meta_json)
        .bind(&req.parts_manifest)
        .bind(&req.sse)
        .bind(stored_algo_string.as_deref())
        .bind(stored_value.as_deref())
        .execute(&mut *tx)
        .await?;
        let shadowed_ids: Vec<StoreKey> = shadowed
            .iter()
            .map(|(b, _)| blob_id_from_bytes(b))
            .collect();
        Self::queue_reclaims(&mut tx, &shadowed_ids).await?;
        tx.commit().await?;

        Ok(PutObjectOutcome {
            version,
            shadowed_blobs: shadowed
                .into_iter()
                .map(|(b, _)| blob_id_from_bytes(&b))
                .collect(),
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let row: Option<PgObjectRow> = sqlx::query_as(
            "SELECT bucket, key, version, blob_id, size, etag, content_type, created_at, user_meta, parts_manifest, sse, checksum_algo, checksum_b64
             FROM named_objects
             WHERE bucket = $1 AND key = $2 AND state = 1 AND deleted_at IS NULL
             ORDER BY version DESC LIMIT 1",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.into_meta()))
    }

    async fn scan_sse_objects(
        &self,
        bucket: Option<&str>,
        after: Option<(String, String)>,
        limit: u64,
    ) -> Result<Vec<ObjectMeta>> {
        let (after_bucket, after_key) = match after {
            Some((b, k)) => (Some(b), Some(k)),
            None => (None, None),
        };
        let rows: Vec<PgObjectRow> = sqlx::query_as(
            "SELECT bucket, key, version, blob_id, size, etag, content_type, created_at, user_meta, parts_manifest, sse, checksum_algo, checksum_b64
             FROM named_objects
             WHERE sse IS NOT NULL AND state = 1 AND deleted_at IS NULL
               AND ($1::text IS NULL OR (bucket, key) > ($1::text, $2::text))
               AND ($4::text IS NULL OR bucket = $4)
             ORDER BY bucket, key
             LIMIT $3",
        )
        .bind(after_bucket)
        .bind(after_key)
        .bind(limit as i64)
        .bind(bucket)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.into_meta()).collect())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<DeletedObject>> {
        let now = Self::now_ms();
        // 记账事务:墓碑与 used_bytes 扣减同事务(不变量:used = Σ state=1 行 size)。
        let mut tx = self.pool.begin().await?;
        let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "UPDATE named_objects SET state = 0, deleted_at = $3
             WHERE bucket = $1 AND key = $2 AND state = 1 AND deleted_at IS NULL
             AND version = (
                 SELECT version FROM named_objects
                 WHERE bucket = $1 AND key = $2 AND state = 1 AND deleted_at IS NULL
                 ORDER BY version DESC LIMIT 1
             )
             RETURNING blob_id, size",
        )
        .bind(bucket)
        .bind(key)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((blob_id, _)) = &row {
            let bid = blob_id_from_bytes(blob_id);
            Self::queue_reclaims(&mut tx, std::slice::from_ref(&bid)).await?;
        }
        if let Some((_, size)) = &row {
            sqlx::query("UPDATE buckets SET used_bytes = used_bytes - $2 WHERE name = $1")
                .bind(bucket)
                .bind(*size)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(row.map(|(blob_id, _)| DeletedObject {
            blob_id: blob_id_from_bytes(&blob_id),
            key: key.to_string(),
        }))
    }

    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<Vec<DeleteResult>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let deleted = self.delete_object(bucket, key).await?;
            results.push(DeleteResult {
                key: key.clone(),
                deleted: deleted.is_some(),
                error: None,
            });
        }
        Ok(results)
    }

    async fn get_bucket_quota(&self, bucket: &str) -> Result<Option<BucketQuota>> {
        let row: Option<(i64, i64)> =
            sqlx::query_as("SELECT quota_bytes, used_bytes FROM buckets WHERE name = $1")
                .bind(bucket)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(q, u)| BucketQuota {
            name: bucket.to_string(),
            quota_bytes: q.max(0) as u64,
            used_bytes: u.max(0) as u64,
        }))
    }

    async fn set_bucket_quota(&self, bucket: &str, quota_bytes: u64) -> Result<()> {
        let res = sqlx::query("UPDATE buckets SET quota_bytes = $2 WHERE name = $1")
            .bind(bucket)
            .bind(quota_bytes as i64)
            .execute(&self.pool)
            .await?;
        if res.rows_affected() == 0 {
            return Err(S3Error::NoSuchBucket);
        }
        Ok(())
    }

    async fn list_objects(&self, req: ListObjectsReq) -> Result<ListObjectsResp> {
        let limit = req.max_keys.max(1) as i64 + 1;
        let start_key = req
            .continuation_token
            .as_deref()
            .or(req.start_after.as_deref())
            .unwrap_or("");

        let prefix_pattern = req.prefix.as_deref().unwrap_or("");
        let like_pattern = format!(
            "{}%",
            prefix_pattern.replace('%', "\\%").replace('_', "\\_")
        );

        let rows: Vec<PgListRow> = sqlx::query_as(
            "SELECT DISTINCT ON (key) key, size, etag, created_at
             FROM named_objects
             WHERE bucket = $1 AND key > $2 AND key LIKE $3
               AND state = 1 AND deleted_at IS NULL
             ORDER BY key, version DESC
             LIMIT $4",
        )
        .bind(&req.bucket)
        .bind(start_key)
        .bind(&like_pattern)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let is_truncated = rows.len() as i64 > req.max_keys as i64;
        let entries_len = (rows.len() as i32).min(req.max_keys);
        let entries: Vec<ObjectEntry> = rows
            .into_iter()
            .take(entries_len as usize)
            .map(|r| ObjectEntry {
                key: r.key,
                size: r.size as u64,
                etag: r.etag.unwrap_or_default(),
                last_modified: r.created_at,
            })
            .collect();

        let next_token = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        let mut common_prefixes = Vec::new();
        let mut filtered_entries = Vec::new();
        if let Some(delimiter) = &req.delimiter {
            let prefix_len = prefix_pattern.len();
            let mut seen_prefixes = std::collections::HashSet::new();
            for entry in entries {
                if let Some(pos) = entry.key[prefix_len..].find(delimiter.as_str()) {
                    let cp = &entry.key[..prefix_len + pos + delimiter.len()];
                    if seen_prefixes.insert(cp.to_string()) {
                        common_prefixes.push(cp.to_string());
                    }
                } else {
                    filtered_entries.push(entry);
                }
            }
        } else {
            filtered_entries = entries;
        }

        let key_count = filtered_entries.len() as i32 + common_prefixes.len() as i32;
        Ok(ListObjectsResp {
            entries: filtered_entries,
            common_prefixes,
            is_truncated,
            next_continuation_token: next_token,
            key_count,
        })
    }

    async fn create_bucket(&self, name: &str, owner: &str) -> Result<()> {
        let now = Self::now_ms();
        let result = sqlx::query(
            "INSERT INTO buckets (name, owner, created_at) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING",
        )
        .bind(name)
        .bind(owner)
        .bind(now)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let existing = self.head_bucket(name).await?;
            if let Some(b) = existing {
                if b.owner == owner {
                    return Ok(());
                }
                return Err(S3Error::BucketAlreadyExists);
            }
        }
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let has_objects: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM named_objects WHERE bucket = $1 AND state = 1 AND deleted_at IS NULL LIMIT 1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        if has_objects.is_some() {
            return Err(S3Error::BucketNotEmpty);
        }
        sqlx::query("DELETE FROM buckets WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn head_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        let row: Option<(String, String, i64, String)> =
            sqlx::query_as("SELECT name, owner, created_at, acl FROM buckets WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(n, o, c, a)| BucketMeta {
            name: n,
            owner: o,
            created_at: c,
            acl: BucketAcl::parse(&a).unwrap_or(BucketAcl::Private),
        }))
    }

    async fn set_bucket_acl(&self, bucket: &str, acl: BucketAcl) -> Result<()> {
        let result = sqlx::query("UPDATE buckets SET acl = $1 WHERE name = $2")
            .bind(acl.as_str())
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(S3Error::NoSuchBucket);
        }
        Ok(())
    }

    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>> {
        let rows: Vec<(String, String, i64, String)> = sqlx::query_as(
            "SELECT name, owner, created_at, acl FROM buckets WHERE owner = $1 ORDER BY name",
        )
        .bind(owner)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(n, o, c, a)| BucketMeta {
                name: n,
                owner: o,
                created_at: c,
                acl: BucketAcl::parse(&a).unwrap_or(BucketAcl::Private),
            })
            .collect())
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: &UserMeta,
    ) -> Result<String> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_ms();
        let meta_json = if user_meta.is_empty() {
            None
        } else {
            Some(serde_json::to_string(user_meta).unwrap_or_default())
        };
        sqlx::query(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, created_at, content_type, user_meta)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&upload_id)
        .bind(bucket)
        .bind(key)
        .bind(now)
        .bind(content_type)
        .bind(&meta_json)
        .execute(&self.pool)
        .await?;
        Ok(upload_id)
    }

    async fn put_part(&self, upload_id: &str, part: PartMeta) -> Result<Option<BlobId>> {
        let mut tx = self.pool.begin().await?;

        // 会话行锁:与会话生命周期操作(abort/complete)全序 —— 不会"先验后插队"
        // (否则 part 行会落在 abort/complete 的 DELETE 之后,成为无人回收的孤儿行)。
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = $1 FOR UPDATE")
                .bind(upload_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        // part 行锁读(锁读 = latest committed,且阻到在途持有者提交):并发重传同一 part 时,
        // 后到者拿到的必须是**前驱提交后的**行,否则返回的 old 是过时快照——被换下的中间
        // blob 无人删、无墓碑,GC 永不收(重传孤儿泄漏)。
        let old: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT blob_id FROM multipart_parts WHERE upload_id = $1 AND part_number = $2 FOR UPDATE",
        )
        .bind(upload_id)
        .bind(part.part_number)
        .fetch_optional(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO multipart_parts (upload_id, part_number, blob_id, size, etag)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (upload_id, part_number)
             DO UPDATE SET blob_id = EXCLUDED.blob_id, size = EXCLUDED.size, etag = EXCLUDED.etag",
        )
        .bind(upload_id)
        .bind(part.part_number)
        .bind(part.blob_id.as_bytes().as_slice())
        .bind(part.size as i64)
        .bind(&part.etag)
        .execute(&mut *tx)
        .await?;
        if let Some((old_bytes,)) = &old {
            let bid = blob_id_from_bytes(old_bytes);
            Self::queue_reclaims(&mut tx, std::slice::from_ref(&bid)).await?;
        }

        tx.commit().await?;

        Ok(old.map(|(b,)| blob_id_from_bytes(&b)))
    }

    async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletePart],
    ) -> Result<CompleteMultipartResult> {
        let mut tx = self.pool.begin().await?;

        // 会话行锁:与 put_part / abort 全序( 会话所有权不变量;锁内快照即终版)。
        let upload: Option<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT bucket, key, content_type, user_meta FROM multipart_uploads
             WHERE upload_id = $1 FOR UPDATE",
        )
        .bind(upload_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (bucket, key, content_type, user_meta_json) = upload.ok_or(S3Error::NoSuchUpload)?;

        let stored: Vec<(i32, Vec<u8>, i64, String)> = sqlx::query_as(
            "SELECT part_number, blob_id, size, etag FROM multipart_parts
             WHERE upload_id = $1 ORDER BY part_number",
        )
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;

        // 已上传但未列入 manifest 的 part(S3 语义:complete 即丢弃)——上报给 handler 回收,
        // 否则这些 blob 无墓碑、GC 永不收(确定性泄漏)。
        let listed: HashSet<i32> = parts.iter().map(|cp| cp.part_number).collect();
        let discarded_part_blobs: Vec<BlobId> = stored
            .iter()
            .filter(|(pn, ..)| !listed.contains(pn))
            .map(|(_, bid, ..)| blob_id_from_bytes(bid))
            .collect();

        let stored_map: HashMap<i32, (Vec<u8>, i64, String)> = stored
            .into_iter()
            .map(|(pn, bid, sz, et)| (pn, (bid, sz, et)))
            .collect();

        let mut manifest_parts = Vec::with_capacity(parts.len());
        let mut total_size: u64 = 0;
        let mut etag_parts = Vec::new();
        let mut prev_part_num = 0i32;

        for cp in parts {
            if cp.part_number <= prev_part_num {
                return Err(S3Error::InvalidPartOrder);
            }
            prev_part_num = cp.part_number;

            let (blob_bytes, size, stored_etag) = stored_map
                .get(&cp.part_number)
                .ok_or(S3Error::InvalidPart)?;

            let normalized_etag = cp.etag.trim_matches('"');
            if normalized_etag != stored_etag.trim_matches('"') {
                return Err(S3Error::InvalidPart);
            }

            let mut bid = [0u8; 8];
            bid.copy_from_slice(blob_bytes);
            manifest_parts.push(ManifestPart {
                blob_id: bid,
                size: *size as u64,
            });
            total_size += *size as u64;
            etag_parts.push(stored_etag.clone());
        }

        let composite_etag = if parts.len() == 1 {
            etag_parts[0].clone()
        } else {
            use md5::{Digest, Md5};
            let mut hasher = Md5::new();
            for et in &etag_parts {
                if let Ok(bytes) = hex::decode(et.trim_matches('"')) {
                    hasher.update(&bytes);
                }
            }
            format!("{:x}-{}", hasher.finalize(), parts.len())
        };

        let manifest_json = serde_json::to_string(&manifest_parts)
            .map_err(|e| S3Error::InternalError(format!("serialize manifest: {e}")))?;

        let primary_blob_id = blob_id_from_bytes(&manifest_parts[0].blob_id);

        let user_meta: UserMeta = user_meta_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let put_req = PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id: primary_blob_id,
            size: total_size,
            etag: composite_etag.clone(),
            content_type,
            user_meta,
            parts_manifest: Some(manifest_json.clone()),
            sse: None,
            checksum: None,
        };
        // put_object 自持事务(objects/buckets 行)与本会话锁互不相交、加锁单向
        // (会话锁 ↔ 对象行,无环),无死锁;会话锁握过整个 put_object 窗口,期间新的
        // put_part/abort 无法插队落孤儿行。
        let outcome = self.put_object(put_req).await?;

        Self::queue_reclaims(&mut tx, &discarded_part_blobs).await?;
        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = $1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = $1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(CompleteMultipartResult {
            bucket,
            key,
            etag: composite_etag,
            blob_id: primary_blob_id,
            size: total_size,
            parts_manifest: manifest_json,
            shadowed_blobs: outcome.shadowed_blobs,
            discarded_part_blobs,
        })
    }

    async fn abort_multipart(&self, upload_id: &str) -> Result<Vec<BlobId>> {
        let mut tx = self.pool.begin().await?;

        // 会话行锁:与 put_part / complete 全序 —— 锁内读到的一定是终版 part 集,
        // 不会出现"读完后 put 插队、其 part 行被删而其 blob 无人负责"。
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = $1 FOR UPDATE")
                .bind(upload_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        let parts: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT blob_id FROM multipart_parts WHERE upload_id = $1")
                .bind(upload_id)
                .fetch_all(&mut *tx)
                .await?;
        let part_ids: Vec<StoreKey> = parts.iter().map(|(b,)| blob_id_from_bytes(b)).collect();
        Self::queue_reclaims(&mut tx, &part_ids).await?;

        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = $1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = $1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(part_ids)
    }

    async fn pending_reclaims_batch(&self, limit: u64) -> Result<Vec<BlobId>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT blob_id FROM pending_reclaims ORDER BY queued_at LIMIT $1")
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.iter().map(|(b,)| blob_id_from_bytes(b)).collect())
    }

    async fn resolve_pending_reclaims(&self, blob_ids: &[BlobId]) -> Result<u64> {
        if blob_ids.is_empty() {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        let mut removed = 0u64;
        for bid in blob_ids {
            let res = sqlx::query("DELETE FROM pending_reclaims WHERE blob_id = $1")
                .bind(bid.as_bytes().as_slice())
                .execute(&mut *tx)
                .await?;
            removed += res.rows_affected();
        }
        tx.commit().await?;
        Ok(removed)
    }

    async fn pending_reclaims_count(&self) -> Result<u64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_reclaims")
            .fetch_one(&self.pool)
            .await?;
        Ok(n.max(0) as u64)
    }

    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> Result<ListPartsResp> {
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = $1")
                .bind(upload_id)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        let limit = opts.max_parts.max(1) as i64 + 1;
        let rows: Vec<(i32, i64, String)> = sqlx::query_as(
            "SELECT part_number, size, etag FROM multipart_parts
             WHERE upload_id = $1 AND part_number > $2
             ORDER BY part_number LIMIT $3",
        )
        .bind(upload_id)
        .bind(opts.part_number_marker)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let is_truncated = rows.len() as i64 > opts.max_parts as i64;
        let parts: Vec<PartEntry> = rows
            .into_iter()
            .take(opts.max_parts.max(1) as usize)
            .map(|(pn, sz, et)| PartEntry {
                part_number: pn,
                size: sz as u64,
                etag: et,
            })
            .collect();

        let next_marker = if is_truncated {
            parts.last().map(|p| p.part_number).unwrap_or(0)
        } else {
            0
        };

        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker: next_marker,
        })
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        opts: ListUploadsOpts,
    ) -> Result<ListUploadsResp> {
        let limit = opts.max_uploads.max(1) as i64 + 1;
        let prefix_pattern = opts.prefix.as_deref().unwrap_or("");
        let like_pattern = format!(
            "{}%",
            prefix_pattern.replace('%', "\\%").replace('_', "\\_")
        );
        let key_marker = opts.key_marker.as_deref().unwrap_or("");
        let id_marker = opts.upload_id_marker.as_deref().unwrap_or("");

        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT key, upload_id, created_at FROM multipart_uploads
             WHERE bucket = $1 AND key LIKE $2 AND (key > $3 OR (key = $3 AND upload_id > $4))
             ORDER BY key, upload_id LIMIT $5",
        )
        .bind(bucket)
        .bind(&like_pattern)
        .bind(key_marker)
        .bind(id_marker)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let is_truncated = rows.len() as i64 > opts.max_uploads as i64;
        let uploads: Vec<UploadEntry> = rows
            .into_iter()
            .take(opts.max_uploads.max(1) as usize)
            .map(|(k, uid, c)| UploadEntry {
                key: k,
                upload_id: uid,
                initiated: c,
            })
            .collect();

        let (next_key, next_id) = if is_truncated {
            uploads
                .last()
                .map(|u| (Some(u.key.clone()), Some(u.upload_id.clone())))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        Ok(ListUploadsResp {
            uploads,
            is_truncated,
            next_key_marker: next_key,
            next_upload_id_marker: next_id,
        })
    }

    async fn lookup_credential(&self, access_key: &str) -> Result<Option<S3Credential>> {
        let row: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT access_key, secret_key, owner, created_at FROM s3_credentials WHERE access_key = $1",
        )
        .bind(access_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(ak, sk, o, c)| S3Credential {
            access_key: ak,
            secret_key: sk,
            owner: o,
            created_at: c,
        }))
    }

    async fn create_credential(&self, owner: &str) -> Result<S3Credential> {
        let access_key = format!(
            "AKIA{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..16]
        );
        let secret_key = uuid::Uuid::new_v4().to_string().replace('-', "")
            + &uuid::Uuid::new_v4().to_string().replace('-', "")[..8];
        let now = Self::now_ms();
        sqlx::query(
            "INSERT INTO s3_credentials (access_key, secret_key, owner, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(&access_key)
        .bind(&secret_key)
        .bind(owner)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(S3Credential {
            access_key,
            secret_key,
            owner: owner.to_string(),
            created_at: now,
        })
    }

    async fn delete_credential(&self, access_key: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM s3_credentials WHERE access_key = $1")
            .bind(access_key)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn purge_expired_deletions(&self, cutoff_ms: i64, limit: u64) -> Result<u64> {
        // ctid 子查询限批;单语句原子取件 —— 并发实例互不重复计数(行锁)。
        let result = sqlx::query(
            "DELETE FROM named_objects
             WHERE ctid IN (
                 SELECT ctid FROM named_objects
                 WHERE state = 0 AND deleted_at IS NOT NULL AND deleted_at < $1
                 ORDER BY deleted_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )",
        )
        .bind(cutoff_ms)
        .bind(limit as i64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn take_expired_uploads(&self, cutoff_ms: i64, limit: u64) -> Result<Vec<String>> {
        // 取件即删除(原子):拿到 upload_id 的实例独占该 session 的孤儿清理。
        let rows: Vec<(String,)> = sqlx::query_as(
            "DELETE FROM multipart_uploads
             WHERE ctid IN (
                 SELECT ctid FROM multipart_uploads
                 WHERE created_at < $1
                 ORDER BY created_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING upload_id",
        )
        .bind(cutoff_ms)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn delete_parts_of_session(&self, upload_id: &str) -> Result<Vec<BlobId>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("DELETE FROM multipart_parts WHERE upload_id = $1 RETURNING blob_id")
                .bind(upload_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|(b,)| blob_id_from_bytes(&b))
            .collect())
    }
}

fn blob_id_from_bytes(bytes: &[u8]) -> StoreKey {
    StoreKey::from_slice(bytes).unwrap_or_else(|| StoreKey::from_parts(0, 0))
}

#[derive(sqlx::FromRow)]
struct PgObjectRow {
    bucket: String,
    key: String,
    version: i64,
    blob_id: Vec<u8>,
    size: i64,
    etag: Option<String>,
    content_type: Option<String>,
    created_at: i64,
    user_meta: Option<String>,
    parts_manifest: Option<String>,
    sse: Option<String>,
    checksum_algo: Option<String>,
    checksum_b64: Option<String>,
}

impl PgObjectRow {
    fn into_meta(self) -> ObjectMeta {
        let user_meta: UserMeta = self
            .user_meta
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        ObjectMeta {
            bucket: self.bucket,
            key: self.key,
            blob_id: blob_id_from_bytes(&self.blob_id),
            version: self.version,
            size: self.size as u64,
            etag: self.etag.unwrap_or_default(),
            content_type: self.content_type,
            created_at: self.created_at,
            user_meta,
            parts_manifest: self.parts_manifest,
            sse: self.sse,
            checksum: StoredChecksum::from_pair(self.checksum_algo, self.checksum_b64),
        }
    }
}

#[derive(sqlx::FromRow)]
struct PgListRow {
    key: String,
    size: i64,
    etag: Option<String>,
    created_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use antfs_blob::StoreKey;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    async fn test_pool() -> Option<PgPool> {
        let url = std::env::var("TEST_S3_POSTGRES_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .ok()?;
        sqlx::migrate!("src/metastore/migrations/postgres")
            .run(&pool)
            .await
            .ok()?;
        Some(pool)
    }

    async fn wal_wipe(pool: &PgPool) {
        sqlx::query("DELETE FROM pending_reclaims")
            .execute(pool)
            .await
            .expect("wal wipe");
    }

    #[tokio::test]
    #[ignore]
    async fn put_get_roundtrip() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let blob_id = StoreKey::from_parts(1, 123);
        let user_meta = UserMeta::from([("k".to_string(), "v".to_string())]);
        let req = PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id,
            size: 42,
            etag: "etag123".to_string(),
            content_type: Some("text/plain".to_string()),
            user_meta: user_meta.clone(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        let _ver = store.put_object(req).await.unwrap();
        let got = store.get_object(&bucket, &key).await.unwrap();
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got.bucket, bucket);
        assert_eq!(got.key, key);
        assert_eq!(got.blob_id, blob_id);
        assert_eq!(got.size, 42);
        assert_eq!(got.etag, "etag123");
        assert_eq!(got.content_type, Some("text/plain".to_string()));
        assert_eq!(got.user_meta, user_meta);
        assert!(got.parts_manifest.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn get_nonexistent_returns_none() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let key = format!("key-{}", Uuid::new_v4());
        let got = store.get_object(&bucket, &key).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn overwrite_returns_latest() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let req1 = PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id: StoreKey::from_parts(1, 1),
            size: 1,
            etag: "e1".to_string(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        store.put_object(req1).await.unwrap();
        let req2 = PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id: StoreKey::from_parts(1, 2),
            size: 2,
            etag: "e2".to_string(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        store.put_object(req2).await.unwrap();
        let got = store.get_object(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(got.blob_id, StoreKey::from_parts(1, 2));
        assert_eq!(got.size, 2);
        assert_eq!(got.etag, "e2");

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn delete_makes_invisible() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let req = PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id: StoreKey::from_parts(1, 10),
            size: 10,
            etag: "e".to_string(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        store.put_object(req).await.unwrap();
        let del = store.delete_object(&bucket, &key).await.unwrap();
        assert!(del.is_some());
        let got = store.get_object(&bucket, &key).await.unwrap();
        assert!(got.is_none());

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn delete_nonexistent_returns_none() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let key = format!("key-{}", Uuid::new_v4());
        let del = store.delete_object(&bucket, &key).await.unwrap();
        assert!(del.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn delete_objects_batch() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let keys: Vec<String> = (0..3).map(|i| format!("k{}", i)).collect();
        for (i, k) in keys.iter().enumerate() {
            let req = PutObjectMeta {
                bucket: bucket.clone(),
                key: k.clone(),
                blob_id: StoreKey::from_parts(1, i as u32),
                size: i as u64,
                etag: format!("e{}", i),
                content_type: None,
                user_meta: UserMeta::default(),
                sse: None,
                parts_manifest: None,
                checksum: None,
            };
            store.put_object(req).await.unwrap();
        }
        let res = store.delete_objects(&bucket, &keys).await.unwrap();
        assert_eq!(res.len(), 3);
        for r in &res {
            assert!(r.deleted);
            assert!(r.error.is_none());
        }
        for k in &keys {
            let got = store.get_object(&bucket, k).await.unwrap();
            assert!(got.is_none());
        }

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn list_prefix() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let keys = [
            "logs/a".to_string(),
            "logs/b".to_string(),
            "data/c".to_string(),
        ];
        for (i, k) in keys.iter().enumerate() {
            let req = PutObjectMeta {
                bucket: bucket.clone(),
                key: k.clone(),
                blob_id: StoreKey::from_parts(1, i as u32),
                size: 1,
                etag: "e".to_string(),
                content_type: None,
                user_meta: UserMeta::default(),
                sse: None,
                parts_manifest: None,
                checksum: None,
            };
            store.put_object(req).await.unwrap();
        }
        let req = ListObjectsReq {
            bucket: bucket.clone(),
            prefix: Some("logs/".to_string()),
            delimiter: None,
            start_after: None,
            continuation_token: None,
            max_keys: 1000,
        };
        let resp = store.list_objects(req).await.unwrap();
        assert_eq!(resp.entries.len(), 2);
        assert!(resp.common_prefixes.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn list_delimiter_common_prefixes() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let keys = [
            "logs/2024/a".to_string(),
            "logs/2025/b".to_string(),
            "data/c".to_string(),
        ];
        for (i, k) in keys.iter().enumerate() {
            let req = PutObjectMeta {
                bucket: bucket.clone(),
                key: k.clone(),
                blob_id: StoreKey::from_parts(1, i as u32),
                size: 1,
                etag: "e".to_string(),
                content_type: None,
                user_meta: UserMeta::default(),
                sse: None,
                parts_manifest: None,
                checksum: None,
            };
            store.put_object(req).await.unwrap();
        }
        let req = ListObjectsReq {
            bucket: bucket.clone(),
            prefix: Some("logs/".to_string()),
            delimiter: Some("/".to_string()),
            start_after: None,
            continuation_token: None,
            max_keys: 1000,
        };
        let resp = store.list_objects(req).await.unwrap();
        assert_eq!(resp.common_prefixes.len(), 2);
        assert!(resp.common_prefixes.contains(&"logs/2024/".to_string()));
        assert!(resp.common_prefixes.contains(&"logs/2025/".to_string()));
        assert!(resp.entries.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn list_pagination() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        for i in 0..5 {
            let req = PutObjectMeta {
                bucket: bucket.clone(),
                key: format!("k{}", i),
                blob_id: StoreKey::from_parts(1, i),
                size: 1,
                etag: "e".to_string(),
                content_type: None,
                user_meta: UserMeta::default(),
                sse: None,
                parts_manifest: None,
                checksum: None,
            };
            store.put_object(req).await.unwrap();
        }
        let req = ListObjectsReq {
            bucket: bucket.clone(),
            prefix: None,
            delimiter: None,
            start_after: None,
            continuation_token: None,
            max_keys: 2,
        };
        let resp = store.list_objects(req).await.unwrap();
        assert!(resp.is_truncated);
        assert_eq!(resp.entries.len(), 2);
        assert!(resp.next_continuation_token.is_some());
        let token = resp.next_continuation_token.clone().unwrap();
        let req2 = ListObjectsReq {
            bucket: bucket.clone(),
            prefix: None,
            delimiter: None,
            start_after: None,
            continuation_token: Some(token),
            max_keys: 2,
        };
        let resp2 = store.list_objects(req2).await.unwrap();
        assert_eq!(resp2.entries.len(), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn bucket_crud() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner1").await.unwrap();
        let head = store.head_bucket(&bucket).await.unwrap();
        assert!(head.is_some());
        let list = store.list_buckets("owner1").await.unwrap();
        assert!(list.iter().any(|b| b.name == bucket));
        store.delete_bucket(&bucket).await.unwrap();
        let head = store.head_bucket(&bucket).await.unwrap();
        assert!(head.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn bucket_acl_set_and_head() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        // 新桶默认 private(首轮 ACL 语义逐字保留)。
        assert_eq!(
            store.head_bucket(&bucket).await.unwrap().unwrap().acl,
            BucketAcl::Private
        );
        // 开公开 → 立即可见;ListBuckets 同样携带真值(不静默回默认)。
        store
            .set_bucket_acl(&bucket, BucketAcl::PublicRead)
            .await
            .unwrap();
        assert_eq!(
            store.head_bucket(&bucket).await.unwrap().unwrap().acl,
            BucketAcl::PublicRead
        );
        let listed = store.list_buckets("owner").await.unwrap();
        assert_eq!(
            listed
                .iter()
                .find(|b| b.name == bucket)
                .expect("bucket must be listed")
                .acl,
            BucketAcl::PublicRead
        );
        // 未知桶 set → NoSuchBucket(UPDATE 0 行命中)。
        let err = store
            .set_bucket_acl("no-such-bucket-xyz", BucketAcl::PublicRead)
            .await
            .unwrap_err();
        assert!(matches!(err, S3Error::NoSuchBucket));
    }

    #[tokio::test]
    #[ignore]
    async fn delete_nonempty_bucket_fails() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let req = PutObjectMeta {
            bucket: bucket.clone(),
            key: "k".to_string(),
            blob_id: StoreKey::from_parts(1, 1),
            size: 1,
            etag: "e".to_string(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        store.put_object(req).await.unwrap();
        let res = store.delete_bucket(&bucket).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn multipart_roundtrip() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, Some("text/plain"), &UserMeta::default())
            .await
            .unwrap();
        let part1 = PartMeta {
            part_number: 1,
            blob_id: StoreKey::from_parts(1, 1),
            size: 5,
            etag: "11111111111111111111111111111111".to_string(),
        };
        let part2 = PartMeta {
            part_number: 2,
            blob_id: StoreKey::from_parts(1, 2),
            size: 3,
            etag: "22222222222222222222222222222222".to_string(),
        };
        store.put_part(&upload_id, part1).await.unwrap();
        store.put_part(&upload_id, part2).await.unwrap();
        let parts = vec![
            CompletePart {
                part_number: 1,
                etag: "11111111111111111111111111111111".to_string(),
            },
            CompletePart {
                part_number: 2,
                etag: "22222222222222222222222222222222".to_string(),
            },
        ];
        let res = store.complete_multipart(&upload_id, &parts).await.unwrap();
        assert_eq!(res.bucket, bucket);
        assert_eq!(res.key, key);
        assert!(!res.parts_manifest.is_empty());
        let got = store.get_object(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(got.parts_manifest, Some(res.parts_manifest.clone()));
    }

    // ── multipart 会话生命周期:所有权恰一次(重传孤儿/abort 插队/unlisted 泄漏)──

    #[tokio::test]
    #[ignore]
    async fn reclaim_wal_transactional_recording() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());

        let mk = |blob: StoreKey| PutObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            blob_id: blob,
            size: 1,
            etag: "e".into(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        let v1 = StoreKey::from_parts(1, 91);
        let v2 = StoreKey::from_parts(1, 92);
        store.put_object(mk(v1)).await.unwrap();
        store.put_object(mk(v2)).await.unwrap();

        let pending = store.pending_reclaims_batch(50).await.unwrap();
        assert!(pending.contains(&v1), "{pending:?}");
        assert!(!pending.contains(&v2));
        assert_eq!(store.resolve_pending_reclaims(&[v1]).await.unwrap(), 1);

        assert!(store.delete_object(&bucket, &key).await.unwrap().is_some());
        assert!(store
            .pending_reclaims_batch(50)
            .await
            .unwrap()
            .contains(&v2));

        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();
        let p_old = StoreKey::from_parts(1, 93);
        let p_new = StoreKey::from_parts(1, 94);
        store
            .put_part(
                &upload_id,
                PartMeta {
                    part_number: 1,
                    blob_id: p_old,
                    size: 1,
                    etag: "a".into(),
                },
            )
            .await
            .unwrap();
        store
            .put_part(
                &upload_id,
                PartMeta {
                    part_number: 1,
                    blob_id: p_new,
                    size: 1,
                    etag: "b".into(),
                },
            )
            .await
            .unwrap();
        assert!(store
            .pending_reclaims_batch(50)
            .await
            .unwrap()
            .contains(&p_old));
        assert_eq!(
            store.abort_multipart(&upload_id).await.unwrap(),
            vec![p_new]
        );
        let pending = store.pending_reclaims_batch(50).await.unwrap();
        assert!(
            pending.contains(&p_new) && pending.contains(&v2),
            "{pending:?}"
        );

        // dedupe:对已在 WAL 的 v2 再造一次遮蔽(自我覆盖同 blob),行数不变。
        let before = store.pending_reclaims_count().await.unwrap();
        let key2 = format!("key-{}", Uuid::new_v4());
        let mk2 = |blob: StoreKey| PutObjectMeta {
            bucket: bucket.clone(),
            key: key2.clone(),
            blob_id: blob,
            size: 1,
            etag: "e".into(),
            content_type: None,
            user_meta: UserMeta::default(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        };
        store.put_object(mk2(v2)).await.unwrap();
        store.put_object(mk2(v2)).await.unwrap();
        let after = store.pending_reclaims_count().await.unwrap();
        assert_eq!(before, after, "同 blob 重入账被主键幂等去重");

        // 自洁:本测产生的 WAL 行全消(共享库不给后续批次留残留)。
        for bid in [v1, v2, p_old, p_new] {
            let _ = store.resolve_pending_reclaims(&[bid]).await.unwrap();
        }
        let rest = store.pending_reclaims_batch(50).await.unwrap();
        assert!(!rest.contains(&v1) && !rest.contains(&v2), "{rest:?}");
        assert!(!rest.contains(&p_old) && !rest.contains(&p_new), "{rest:?}");
    }

    #[tokio::test]
    #[ignore]
    async fn put_part_concurrent_retransmit_exactly_once() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();

        // 8 并发重传同一 part。不变量:每个上传的 blob 恰一人负责——要么成为终版行,
        // 要么恰一次作为 old 交还给上传方(handler 据此 router.delete)。漏一人 = 孤儿
        // (无墓碑,GC 永不收);重一人 = 二次删(幂等但说明语义破了)。
        const N: u32 = 8;
        let mut handles = Vec::new();
        for i in 1..=N {
            let pool_i = pool.clone();
            let uid = upload_id.clone();
            handles.push(tokio::spawn(async move {
                let store = PgMetaStore::new(pool_i);
                store
                    .put_part(
                        &uid,
                        PartMeta {
                            part_number: 1,
                            blob_id: StoreKey::from_parts(9, i),
                            size: 1,
                            etag: format!("{i:032}"),
                        },
                    )
                    .await
            }));
        }
        let mut owned: Vec<Vec<u8>> = Vec::new();
        for h in handles {
            if let Some(old) = h.await.unwrap().unwrap() {
                owned.push(old.as_bytes().to_vec());
            }
        }

        let final_blob: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT blob_id FROM multipart_parts WHERE upload_id = $1 AND part_number = 1",
        )
        .bind(&upload_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        owned.push(final_blob.expect("终版 part 行必须存在").0);

        let mut expected: Vec<Vec<u8>> = (1..=N)
            .map(|i| StoreKey::from_parts(9, i).as_bytes().to_vec())
            .collect();
        owned.sort();
        expected.sort();
        assert_eq!(owned, expected, "每个 blob 的回收所有权必须恰一次");

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn complete_multipart_discards_unlisted_parts() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();
        for (pn, oid) in [(1, 1), (2, 2), (3, 3)] {
            store
                .put_part(
                    &upload_id,
                    PartMeta {
                        part_number: pn,
                        blob_id: StoreKey::from_parts(4, oid),
                        size: 2,
                        etag: format!("{oid:032}"),
                    },
                )
                .await
                .unwrap();
        }
        // 只 finalize part 1/2:part 3 未列入 manifest(S3 语义:complete 即丢弃,须上报回收)。
        let parts = vec![
            CompletePart {
                part_number: 1,
                etag: format!("{:032}", 1),
            },
            CompletePart {
                part_number: 2,
                etag: format!("{:032}", 2),
            },
        ];
        let res = store.complete_multipart(&upload_id, &parts).await.unwrap();
        assert_eq!(res.discarded_part_blobs.len(), 1);
        assert_eq!(
            res.discarded_part_blobs[0].as_bytes().to_vec(),
            StoreKey::from_parts(4, 3).as_bytes().to_vec()
        );
        assert!(store
            .list_parts(&upload_id, ListPartsOpts::default())
            .await
            .is_err());

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn abort_vs_put_part_never_leaves_orphan_row() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();

        let pool_i = pool.clone();
        let uid = upload_id.clone();
        let putter = tokio::spawn(async move {
            let store = PgMetaStore::new(pool_i);
            store
                .put_part(
                    &uid,
                    PartMeta {
                        part_number: 1,
                        blob_id: StoreKey::from_parts(5, 7),
                        size: 1,
                        etag: "etag".to_string(),
                    },
                )
                .await
        });
        let aborted = store.abort_multipart(&upload_id).await;
        let put_res = putter.await.unwrap();

        // 两种合法终局:abort 先 → put 拿 NoSuchUpload;put 先 → abort 在锁内看到该
        // part 并交还其 blob。任一终局,终态必须"会话行不存在、part 行不存在"。
        if let Ok(()) = put_res.as_ref().map(|_| ()) {
            // put 成功 ⇒ abort 必在其后执行且恰带回该 part 的 blob。
            let blobs = aborted.as_ref().expect("put 先行 ⇒ abort 必成功");
            assert!(blobs
                .iter()
                .any(|b| b.as_bytes().to_vec() == StoreKey::from_parts(5, 7).as_bytes().to_vec()));
        } else {
            aborted.expect("put 失败 ⇒ abort 必已先清理");
        }
        assert!(store
            .list_parts(&upload_id, ListPartsOpts::default())
            .await
            .is_err());

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn multipart_abort() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();
        let part = PartMeta {
            part_number: 1,
            blob_id: StoreKey::from_parts(1, 5),
            size: 2,
            etag: "etag".to_string(),
        };
        store.put_part(&upload_id, part).await.unwrap();
        let blob_ids = store.abort_multipart(&upload_id).await.unwrap();
        assert_eq!(blob_ids.len(), 1);
        let list_res = store.list_parts(&upload_id, ListPartsOpts::default()).await;
        assert!(list_res.is_err());

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn put_part_overwrite() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        store.create_bucket(&bucket, "owner").await.unwrap();
        let key = format!("key-{}", Uuid::new_v4());
        let upload_id = store
            .create_multipart(&bucket, &key, None, &UserMeta::default())
            .await
            .unwrap();
        let part1 = PartMeta {
            part_number: 1,
            blob_id: StoreKey::from_parts(1, 1),
            size: 1,
            etag: "e1".to_string(),
        };
        let old = store.put_part(&upload_id, part1).await.unwrap();
        assert!(old.is_none());
        let part2 = PartMeta {
            part_number: 1,
            blob_id: StoreKey::from_parts(1, 2),
            size: 2,
            etag: "e2".to_string(),
        };
        let old = store.put_part(&upload_id, part2).await.unwrap();
        assert!(old.is_some());
        let parts = vec![CompletePart {
            part_number: 1,
            etag: "e2".to_string(),
        }];
        let res = store.complete_multipart(&upload_id, &parts).await.unwrap();
        assert_eq!(res.blob_id, StoreKey::from_parts(1, 2));

        wal_wipe(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn credential_crud() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let cred = store.create_credential("owner").await.unwrap();
        let looked = store.lookup_credential(&cred.access_key).await.unwrap();
        assert!(looked.is_some());
        let deleted = store.delete_credential(&cred.access_key).await.unwrap();
        assert!(deleted);
        let looked = store.lookup_credential(&cred.access_key).await.unwrap();
        assert!(looked.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn list_multipart_uploads_prefix() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = PgMetaStore::new(pool.clone());
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let _u1 = store
            .create_multipart(&bucket, "logs/a", None, &UserMeta::default())
            .await
            .unwrap();
        let _u2 = store
            .create_multipart(&bucket, "logs/b", None, &UserMeta::default())
            .await
            .unwrap();
        let _u3 = store
            .create_multipart(&bucket, "data/c", None, &UserMeta::default())
            .await
            .unwrap();
        let opts = ListUploadsOpts {
            prefix: Some("logs/".to_string()),
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        };
        let resp = store.list_multipart_uploads(&bucket, opts).await.unwrap();
        assert_eq!(resp.uploads.len(), 2);
    }
}
