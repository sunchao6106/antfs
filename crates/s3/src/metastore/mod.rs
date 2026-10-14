pub mod types;

use async_trait::async_trait;
use std::sync::Arc;
use types::*;

use crate::config::MetaStoreConfig;
use crate::error::S3Error;

pub type Result<T> = std::result::Result<T, S3Error>;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    // Object CRUD
    async fn put_object(&self, req: PutObjectMeta) -> Result<ObjectVersion>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<DeletedObject>>;
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<Vec<DeleteResult>>;
    async fn list_objects(&self, req: ListObjectsReq) -> Result<ListObjectsResp>;

    // Bucket CRUD
    async fn create_bucket(&self, name: &str, owner: &str) -> Result<()>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn head_bucket(&self, name: &str) -> Result<Option<BucketMeta>>;
    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>>;

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
    ) -> Result<CompleteMultipartResult>;
    async fn abort_multipart(&self, upload_id: &str) -> Result<Vec<BlobId>>;
    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> Result<ListPartsResp>;
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

pub mod mysql;
pub mod pg;
