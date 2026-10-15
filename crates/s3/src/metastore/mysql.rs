use async_trait::async_trait;
use sqlx::MySqlPool;
use std::collections::HashMap;

use antfs_blob::StoreKey;

use super::types::*;
use super::{MetaStore, Result};
use crate::error::S3Error;

pub struct MysqlMetaStore {
    pool: MySqlPool,
}

impl MysqlMetaStore {
    pub fn new(pool: MySqlPool) -> Self {
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
}

#[async_trait]
impl MetaStore for MysqlMetaStore {
    async fn put_object(&self, req: PutObjectMeta) -> Result<PutObjectOutcome> {
        let version = Self::gen_version();
        let now = Self::now_ms();
        let meta_json = if req.user_meta.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&req.user_meta).unwrap_or_default())
        };
        sqlx::query(
            "INSERT INTO named_objects (bucket, `key`, version, blob_id, state, size, etag, content_type, created_at, user_meta, parts_manifest)
             VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&req.bucket)
        .bind(&req.key)
        .bind(version)
        .bind(req.blob_id.as_bytes().as_slice())
        .bind(req.size as i64)
        .bind(&req.etag)
        .bind(&req.content_type)
        .bind(now)
        .bind(&meta_json)
        .bind(&req.parts_manifest)
        .execute(&self.pool)
        .await?;
        // 遮蔽同 key 旧活行(overwrite 语义,PG 版同段注释)。MySQL 无 RETURNING,两步:
        // 先圈定将被遮蔽的 blob_id,再同条件 UPDATE。两步间隙被并发遮蔽的行会重复置位
        // (幂等,deleted_at 重写无语义损伤);被拿到的 blob 重复删除由引擎墓碑兜底 —— 收敛正确。
        let shadowed: Vec<(Vec<u8>,)> = sqlx::query_as(
            "SELECT blob_id FROM named_objects
             WHERE bucket = ? AND `key` = ? AND state = 1 AND deleted_at IS NULL AND version < ?",
        )
        .bind(&req.bucket)
        .bind(&req.key)
        .bind(version)
        .fetch_all(&self.pool)
        .await?;
        sqlx::query(
            "UPDATE named_objects SET state = 0, deleted_at = ?
             WHERE bucket = ? AND `key` = ? AND state = 1 AND deleted_at IS NULL AND version < ?",
        )
        .bind(now)
        .bind(&req.bucket)
        .bind(&req.key)
        .bind(version)
        .execute(&self.pool)
        .await?;
        Ok(PutObjectOutcome {
            version,
            shadowed_blobs: shadowed
                .into_iter()
                .map(|(b,)| blob_id_from_bytes(&b))
                .collect(),
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let row: Option<MysqlObjectRow> = sqlx::query_as(
            "SELECT bucket, `key`, version, blob_id, size, etag, content_type, created_at, user_meta, parts_manifest
             FROM named_objects
             WHERE bucket = ? AND `key` = ? AND state = 1 AND deleted_at IS NULL
             ORDER BY version DESC LIMIT 1",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.into_meta()))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<DeletedObject>> {
        let now = Self::now_ms();
        // MySQL has no `UPDATE ... RETURNING`; select the current latest version and its
        // blob_id first, then soft-delete exactly that version.
        let latest: Option<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT version, blob_id FROM named_objects
             WHERE bucket = ? AND `key` = ? AND state = 1 AND deleted_at IS NULL
             ORDER BY version DESC LIMIT 1",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        let Some((version, blob_id)) = latest else {
            return Ok(None);
        };

        let result = sqlx::query(
            "UPDATE named_objects SET state = 0, deleted_at = ?
             WHERE bucket = ? AND `key` = ? AND version = ? AND state = 1 AND deleted_at IS NULL",
        )
        .bind(now)
        .bind(bucket)
        .bind(key)
        .bind(version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }

        Ok(Some(DeletedObject {
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

        // MySQL has no `DISTINCT ON`; join each key against its latest version via a
        // grouped subquery, mirroring the PostgreSQL `DISTINCT ON (key) ... ORDER BY key, version DESC`.
        let rows: Vec<MysqlListRow> = sqlx::query_as(
            "SELECT n.`key`, n.size, n.etag, n.created_at
             FROM named_objects n
             JOIN (
                 SELECT bucket, `key` AS k, MAX(version) AS max_version
                 FROM named_objects
                 WHERE bucket = ? AND `key` > ? AND `key` LIKE ?
                   AND state = 1 AND deleted_at IS NULL
                 GROUP BY bucket, `key`
             ) latest ON n.bucket = latest.bucket AND n.`key` = latest.k AND n.version = latest.max_version
             WHERE n.state = 1 AND n.deleted_at IS NULL
             ORDER BY n.`key`
             LIMIT ?",
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
        // `name = name` makes a duplicate a no-op, matching PostgreSQL `ON CONFLICT DO NOTHING`
        // (rows_affected() == 0 on the duplicate branch).
        let result = sqlx::query(
            "INSERT INTO buckets (name, owner, created_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = name",
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
            "SELECT 1 FROM named_objects WHERE bucket = ? AND state = 1 AND deleted_at IS NULL LIMIT 1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        if has_objects.is_some() {
            return Err(S3Error::BucketNotEmpty);
        }
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn head_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        let row: Option<(String, String, i64)> =
            sqlx::query_as("SELECT name, owner, created_at FROM buckets WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(n, o, c)| BucketMeta {
            name: n,
            owner: o,
            created_at: c,
        }))
    }

    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>> {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT name, owner, created_at FROM buckets WHERE owner = ? ORDER BY name",
        )
        .bind(owner)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(n, o, c)| BucketMeta {
                name: n,
                owner: o,
                created_at: c,
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
            "INSERT INTO multipart_uploads (upload_id, bucket, `key`, created_at, content_type, user_meta)
             VALUES (?, ?, ?, ?, ?, ?)",
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
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = ?")
                .bind(upload_id)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        let old: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
        )
        .bind(upload_id)
        .bind(part.part_number)
        .fetch_optional(&self.pool)
        .await?;

        sqlx::query(
            "INSERT INTO multipart_parts (upload_id, part_number, blob_id, size, etag)
             VALUES (?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE blob_id = VALUES(blob_id), size = VALUES(size), etag = VALUES(etag)",
        )
        .bind(upload_id)
        .bind(part.part_number)
        .bind(part.blob_id.as_bytes().as_slice())
        .bind(part.size as i64)
        .bind(&part.etag)
        .execute(&self.pool)
        .await?;

        Ok(old.map(|(b,)| blob_id_from_bytes(&b)))
    }

    async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletePart],
    ) -> Result<CompleteMultipartResult> {
        let upload: Option<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT bucket, `key`, content_type, user_meta FROM multipart_uploads WHERE upload_id = ?",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        let (bucket, key, content_type, user_meta_json) = upload.ok_or(S3Error::NoSuchUpload)?;

        let stored: Vec<(i32, Vec<u8>, i64, String)> = sqlx::query_as(
            "SELECT part_number, blob_id, size, etag FROM multipart_parts
             WHERE upload_id = ? ORDER BY part_number",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;

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
        };
        let outcome = self.put_object(put_req).await?;

        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;

        Ok(CompleteMultipartResult {
            bucket,
            key,
            etag: composite_etag,
            blob_id: primary_blob_id,
            shadowed_blobs: outcome.shadowed_blobs,
            size: total_size,
            parts_manifest: manifest_json,
        })
    }

    async fn abort_multipart(&self, upload_id: &str) -> Result<Vec<BlobId>> {
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = ?")
                .bind(upload_id)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        let parts: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT blob_id FROM multipart_parts WHERE upload_id = ?")
                .bind(upload_id)
                .fetch_all(&self.pool)
                .await?;

        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;

        Ok(parts.iter().map(|(b,)| blob_id_from_bytes(b)).collect())
    }

    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> Result<ListPartsResp> {
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM multipart_uploads WHERE upload_id = ?")
                .bind(upload_id)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_none() {
            return Err(S3Error::NoSuchUpload);
        }

        let limit = opts.max_parts.max(1) as i64 + 1;
        let rows: Vec<(i32, i64, String)> = sqlx::query_as(
            "SELECT part_number, size, etag FROM multipart_parts
             WHERE upload_id = ? AND part_number > ?
             ORDER BY part_number LIMIT ?",
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
            "SELECT `key`, upload_id, created_at FROM multipart_uploads
             WHERE bucket = ? AND `key` LIKE ? AND (`key` > ? OR (`key` = ? AND upload_id > ?))
             ORDER BY `key`, upload_id LIMIT ?",
        )
        .bind(bucket)
        .bind(&like_pattern)
        .bind(key_marker)
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
            "SELECT access_key, secret_key, owner, created_at FROM s3_credentials WHERE access_key = ?",
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
            "INSERT INTO s3_credentials (access_key, secret_key, owner, created_at) VALUES (?, ?, ?, ?)",
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
        let result = sqlx::query("DELETE FROM s3_credentials WHERE access_key = ?")
            .bind(access_key)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn purge_expired_deletions(&self, cutoff_ms: i64, limit: u64) -> Result<u64> {
        // MySQL 支持 DELETE ... ORDER BY ... LIMIT,单语句原子取件(行锁下并发实例不重复)。
        // 无 RETURNING:GC 清行不需要拿回内容,rows_affected 即计数。
        let result = sqlx::query(
            "DELETE FROM named_objects
             WHERE state = 0 AND deleted_at IS NOT NULL AND deleted_at < ?
             ORDER BY deleted_at
             LIMIT ?",
        )
        .bind(cutoff_ms)
        .bind(limit as i64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn take_expired_uploads(&self, cutoff_ms: i64, limit: u64) -> Result<Vec<String>> {
        // MySQL 无 RETURNING:两步 —— 圈定候选再按主键删除,删除语句的 affected 集即"实际取到"。
        // 交叠窗下另一实例可能先取,主键 DELETE 只影响现存行;漏取者下一轮再来(循环语义收敛)。
        let candidates: Vec<(String,)> = sqlx::query_as(
            "SELECT upload_id FROM multipart_uploads WHERE created_at < ? ORDER BY created_at LIMIT ?",
        )
        .bind(cutoff_ms)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        let mut taken = Vec::with_capacity(candidates.len());
        for (upload_id,) in candidates {
            let res = sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
                .bind(&upload_id)
                .execute(&self.pool)
                .await?;
            if res.rows_affected() > 0 {
                taken.push(upload_id);
            }
        }
        Ok(taken)
    }

    async fn delete_parts_of_session(&self, upload_id: &str) -> Result<Vec<BlobId>> {
        // MySQL 无 RETURNING:先圈后删(同仓方言先例;交叠窗由删除幂等收敛)。
        let blobs: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT blob_id FROM multipart_parts WHERE upload_id = ?")
                .bind(upload_id)
                .fetch_all(&self.pool)
                .await?;
        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        Ok(blobs
            .into_iter()
            .map(|(b,)| blob_id_from_bytes(&b))
            .collect())
    }
}

fn blob_id_from_bytes(bytes: &[u8]) -> StoreKey {
    StoreKey::from_slice(bytes).unwrap_or_else(|| StoreKey::from_parts(0, 0))
}

#[derive(sqlx::FromRow)]
struct MysqlObjectRow {
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
}

impl MysqlObjectRow {
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
        }
    }
}

#[derive(sqlx::FromRow)]
struct MysqlListRow {
    key: String,
    size: i64,
    etag: Option<String>,
    created_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use antfs_blob::StoreKey;
    use sqlx::mysql::MySqlPoolOptions;
    use uuid::Uuid;

    async fn test_pool() -> Option<MySqlPool> {
        let url = std::env::var("TEST_S3_MYSQL_URL").ok()?;
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .ok()?;
        sqlx::migrate!("src/metastore/migrations/mysql")
            .run(&pool)
            .await
            .ok()?;
        Some(pool)
    }

    #[tokio::test]
    #[ignore]
    async fn put_get_roundtrip() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
        let store = MysqlMetaStore::new(pool);
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
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
        };
        store.put_object(req2).await.unwrap();
        let got = store.get_object(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(got.blob_id, StoreKey::from_parts(1, 2));
        assert_eq!(got.size, 2);
        assert_eq!(got.etag, "e2");
    }

    #[tokio::test]
    #[ignore]
    async fn delete_makes_invisible() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
        };
        store.put_object(req).await.unwrap();
        let del = store.delete_object(&bucket, &key).await.unwrap();
        assert!(del.is_some());
        let got = store.get_object(&bucket, &key).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn delete_nonexistent_returns_none() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
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
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
                parts_manifest: None,
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
    }

    #[tokio::test]
    #[ignore]
    async fn list_prefix() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
                parts_manifest: None,
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
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
                parts_manifest: None,
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
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
        for i in 0..5 {
            let req = PutObjectMeta {
                bucket: bucket.clone(),
                key: format!("k{}", i),
                blob_id: StoreKey::from_parts(1, i),
                size: 1,
                etag: "e".to_string(),
                content_type: None,
                user_meta: UserMeta::default(),
                parts_manifest: None,
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
        let store = MysqlMetaStore::new(pool);
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
    async fn delete_nonempty_bucket_fails() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
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
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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

    #[tokio::test]
    #[ignore]
    async fn multipart_abort() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
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
    }

    #[tokio::test]
    #[ignore]
    async fn put_part_overwrite() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
        let bucket = format!("bucket-{}", Uuid::new_v4());
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
    }

    #[tokio::test]
    #[ignore]
    async fn credential_crud() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let store = MysqlMetaStore::new(pool);
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
        let store = MysqlMetaStore::new(pool);
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
