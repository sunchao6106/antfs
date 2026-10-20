//! S3 网关每请求双查缓存(`s3-meta-cache`):每个数据面请求在路由分发前要打两次
//! metastore —— `lookup_credential`(SigV4 验签,auth 中间件)与 `head_bucket`
//! (authorize 授权门)。`CachingMetaStore` 装饰器对这两条热路径加 TTL 缓存。
//!
//! 语义边界(权衡序:安全 > 可用 > 性能):
//! - **本进程写路径即时失效**(create/delete credential、桶四 set_* + create/delete):
//!   同实例管理面改完立即对数据面生效;
//! - **多实例共享同一 metastore 时**,跨实例变更 ≤ TTL 收敛(与 S3 自身最终一致语义
//!   同角:凭证撤销跨实例最长一个 TTL 窗口,单实例即时);
//! - **正负条目都缓存**(负 = 查无):未知 access_key 高频轰炸不逐次打库;负条目
//!   固定 5s TTL(防"刚建好的凭证"被旧负缓存挡太久);
//! - 错误**不缓存**(fail-open 到数据库真相,只计数);
//! - 容量 4096/图:插入时先扫过期,仍超整图清空(兜底,桶/凭证数量正常远不及);
//! - `ttl = 0`:不装配(`wrap` 原对象直返)—— 零行为零开销逃生口。
//! - 对象面(put/get/list/multipart)一律穿透:**S3 读写一致性契约优先于吞吐**。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::types::{
    BlobId, BucketAcl, BucketMeta, BucketQuota, CompleteChecksum, CompleteMultipartResult,
    CompletePart, DeleteResult, DeletedObject, ListObjectsReq, ListObjectsResp, ListPartsOpts,
    ListPartsResp, ListUploadsOpts, ListUploadsResp, ObjectMeta, PartMeta, PutObjectMeta,
    PutObjectOutcome, S3Credential, UserMeta,
};
use super::{MetaStore, Result as MetaResult};
use crate::metrics::S3Metrics;

/// 单图容量上限:超出先扫过期,仍超整图清空(重建风暴记档兜底)。
const MAX_ENTRIES: usize = 4096;
/// 负条目(查无)TTL:固定 5s —— 防刚建凭证/桶被旧负缓存长时间挡住。
const NEG_TTL: Duration = Duration::from_secs(5);

struct Entry<T> {
    value: Option<T>,
    expires: Instant,
}

pub struct CachingMetaStore {
    inner: Arc<dyn MetaStore>,
    metrics: Arc<S3Metrics>,
    ttl: Duration,
    creds: RwLock<HashMap<String, Entry<S3Credential>>>,
    buckets: RwLock<HashMap<String, Entry<BucketMeta>>>,
}

/// 装配点:`ttl_secs = 0` → 原对象直返(不包装);> 0 → 装饰器包一层。
/// 挂在 `metastore::connect()` 之后、`AppState` 组装之前。
pub fn wrap(
    inner: Arc<dyn MetaStore>,
    ttl_secs: u64,
    metrics: Arc<S3Metrics>,
) -> Arc<dyn MetaStore> {
    if ttl_secs == 0 {
        return inner;
    }
    Arc::new(CachingMetaStore {
        inner,
        metrics,
        ttl: Duration::from_secs(ttl_secs),
        creds: RwLock::new(HashMap::new()),
        buckets: RwLock::new(HashMap::new()),
    })
}

impl CachingMetaStore {
    /// 命中(未过期)返回 `Some(cached)` — `cached` 本身可为 `None`(负条目)。
    fn cached<T: Clone>(map: &RwLock<HashMap<String, Entry<T>>>, key: &str) -> Option<Option<T>> {
        let now = Instant::now();
        let guard = map.read().expect("meta cache lock poisoned");
        guard.get(key).and_then(|e| {
            if now < e.expires {
                Some(e.value.clone())
            } else {
                None
            }
        })
    }

    fn insert_bounded<T>(
        map: &RwLock<HashMap<String, Entry<T>>>,
        key: &str,
        value: Option<T>,
        ttl: Duration,
    ) {
        let mut guard = map.write().expect("meta cache lock poisoned");
        if guard.len() >= MAX_ENTRIES {
            let now = Instant::now();
            guard.retain(|_, e| now < e.expires);
            if guard.len() >= MAX_ENTRIES {
                guard.clear();
            }
        }
        guard.insert(
            key.to_string(),
            Entry {
                value,
                expires: Instant::now() + ttl,
            },
        );
    }

    fn invalidate<T>(map: &RwLock<HashMap<String, Entry<T>>>, key: &str) {
        map.write().expect("meta cache lock poisoned").remove(key);
    }

    fn neg_ttl(&self) -> Duration {
        self.ttl.min(NEG_TTL)
    }
}

#[async_trait]
impl MetaStore for CachingMetaStore {
    // ── 缓存面(两条热路径)─────────────────────────────────────────────────

    async fn lookup_credential(&self, access_key: &str) -> MetaResult<Option<S3Credential>> {
        if let Some(hit) = Self::cached(&self.creds, access_key) {
            self.metrics
                .meta_cache_lookups_total
                .with_label_values(&["credential", "hit"])
                .inc();
            return Ok(hit);
        }
        let r = self.inner.lookup_credential(access_key).await;
        match &r {
            Ok(v) => {
                let ttl = if v.is_some() {
                    self.ttl
                } else {
                    self.neg_ttl()
                };
                Self::insert_bounded(&self.creds, access_key, v.clone(), ttl);
                self.metrics
                    .meta_cache_lookups_total
                    .with_label_values(&["credential", "miss"])
                    .inc();
            }
            Err(_) => {
                self.metrics
                    .meta_cache_lookups_total
                    .with_label_values(&["credential", "error"])
                    .inc();
            }
        }
        r
    }

    async fn head_bucket(&self, name: &str) -> MetaResult<Option<BucketMeta>> {
        if let Some(hit) = Self::cached(&self.buckets, name) {
            self.metrics
                .meta_cache_lookups_total
                .with_label_values(&["bucket", "hit"])
                .inc();
            return Ok(hit);
        }
        let r = self.inner.head_bucket(name).await;
        match &r {
            Ok(v) => {
                let ttl = if v.is_some() {
                    self.ttl
                } else {
                    self.neg_ttl()
                };
                Self::insert_bounded(&self.buckets, name, v.clone(), ttl);
                self.metrics
                    .meta_cache_lookups_total
                    .with_label_values(&["bucket", "miss"])
                    .inc();
            }
            Err(_) => {
                self.metrics
                    .meta_cache_lookups_total
                    .with_label_values(&["bucket", "error"])
                    .inc();
            }
        }
        r
    }

    // ── 失效面(本进程写路径,await 后无条件摘除 —— Err 也无害,代价一次重查)────

    async fn create_credential(&self, owner: &str) -> MetaResult<S3Credential> {
        let r = self.inner.create_credential(owner).await;
        if let Ok(cred) = &r {
            Self::invalidate(&self.creds, &cred.access_key);
        }
        r
    }

    async fn delete_credential(&self, access_key: &str) -> MetaResult<bool> {
        let r = self.inner.delete_credential(access_key).await;
        Self::invalidate(&self.creds, access_key);
        r
    }

    async fn create_bucket(&self, name: &str, owner: &str) -> MetaResult<()> {
        let r = self.inner.create_bucket(name, owner).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, name);
        }
        r
    }

    async fn delete_bucket(&self, name: &str) -> MetaResult<()> {
        let r = self.inner.delete_bucket(name).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, name);
        }
        r
    }

    async fn set_bucket_quota(&self, bucket: &str, quota_bytes: u64) -> MetaResult<()> {
        let r = self.inner.set_bucket_quota(bucket, quota_bytes).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, bucket);
        }
        r
    }

    async fn set_bucket_acl(&self, bucket: &str, acl: BucketAcl) -> MetaResult<()> {
        let r = self.inner.set_bucket_acl(bucket, acl).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, bucket);
        }
        r
    }

    async fn set_bucket_policy(&self, bucket: &str, policy: Option<&str>) -> MetaResult<()> {
        let r = self.inner.set_bucket_policy(bucket, policy).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, bucket);
        }
        r
    }

    async fn set_bucket_acl_grants(&self, bucket: &str, grants: Option<&str>) -> MetaResult<()> {
        let r = self.inner.set_bucket_acl_grants(bucket, grants).await;
        if r.is_ok() {
            Self::invalidate(&self.buckets, bucket);
        }
        r
    }

    // ── 纯委托面(对象/分片/GC/SSE 一律穿透;行为与收益无关,保持一致语义)────

    async fn put_object(&self, req: PutObjectMeta) -> MetaResult<PutObjectOutcome> {
        self.inner.put_object(req).await
    }
    async fn get_object(&self, bucket: &str, key: &str) -> MetaResult<Option<ObjectMeta>> {
        self.inner.get_object(bucket, key).await
    }
    async fn delete_object(&self, bucket: &str, key: &str) -> MetaResult<Option<DeletedObject>> {
        self.inner.delete_object(bucket, key).await
    }
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> MetaResult<Vec<DeleteResult>> {
        self.inner.delete_objects(bucket, keys).await
    }
    async fn list_objects(&self, req: ListObjectsReq) -> MetaResult<ListObjectsResp> {
        self.inner.list_objects(req).await
    }
    async fn list_buckets(&self, owner: &str) -> MetaResult<Vec<BucketMeta>> {
        self.inner.list_buckets(owner).await
    }
    async fn get_bucket_quota(&self, bucket: &str) -> MetaResult<Option<BucketQuota>> {
        self.inner.get_bucket_quota(bucket).await
    }
    async fn set_object_acl_grants(
        &self,
        bucket: &str,
        key: &str,
        grants: Option<&str>,
    ) -> MetaResult<()> {
        self.inner.set_object_acl_grants(bucket, key, grants).await
    }
    async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: &UserMeta,
    ) -> MetaResult<String> {
        self.inner
            .create_multipart(bucket, key, content_type, user_meta)
            .await
    }
    async fn put_part(&self, upload_id: &str, part: PartMeta) -> MetaResult<Option<BlobId>> {
        self.inner.put_part(upload_id, part).await
    }
    async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletePart],
        checksum: CompleteChecksum,
    ) -> MetaResult<CompleteMultipartResult> {
        self.inner
            .complete_multipart(upload_id, parts, checksum)
            .await
    }
    async fn abort_multipart(&self, upload_id: &str) -> MetaResult<Vec<BlobId>> {
        self.inner.abort_multipart(upload_id).await
    }
    async fn list_parts(&self, upload_id: &str, opts: ListPartsOpts) -> MetaResult<ListPartsResp> {
        self.inner.list_parts(upload_id, opts).await
    }
    async fn session_parts(&self, upload_id: &str) -> MetaResult<Vec<PartMeta>> {
        self.inner.session_parts(upload_id).await
    }
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        opts: ListUploadsOpts,
    ) -> MetaResult<ListUploadsResp> {
        self.inner.list_multipart_uploads(bucket, opts).await
    }
    async fn purge_expired_deletions(&self, cutoff_ms: i64, limit: u64) -> MetaResult<u64> {
        self.inner.purge_expired_deletions(cutoff_ms, limit).await
    }
    async fn take_expired_uploads(&self, cutoff_ms: i64, limit: u64) -> MetaResult<Vec<String>> {
        self.inner.take_expired_uploads(cutoff_ms, limit).await
    }
    async fn delete_parts_of_session(&self, upload_id: &str) -> MetaResult<Vec<BlobId>> {
        self.inner.delete_parts_of_session(upload_id).await
    }
    async fn pending_reclaims_batch(&self, limit: u64) -> MetaResult<Vec<BlobId>> {
        self.inner.pending_reclaims_batch(limit).await
    }
    async fn resolve_pending_reclaims(&self, blob_ids: &[BlobId]) -> MetaResult<u64> {
        self.inner.resolve_pending_reclaims(blob_ids).await
    }
    async fn pending_reclaims_count(&self) -> MetaResult<u64> {
        self.inner.pending_reclaims_count().await
    }
    async fn scan_sse_objects(
        &self,
        bucket: Option<&str>,
        after: Option<(String, String)>,
        limit: u64,
    ) -> MetaResult<Vec<ObjectMeta>> {
        self.inner.scan_sse_objects(bucket, after, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::S3Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Counts {
        lookup_credential: AtomicUsize,
        head_bucket: AtomicUsize,
        create_credential: AtomicUsize,
        delete_credential: AtomicUsize,
        create_bucket: AtomicUsize,
        delete_bucket: AtomicUsize,
        set_bucket_policy: AtomicUsize,
    }

    /// 只实现缓存面会触碰的原语,其余一律 InternalError(装饰器纯转发的路径不进测试)。
    struct MockMeta {
        counts: Arc<Counts>,
        creds: std::sync::Mutex<HashMap<String, S3Credential>>,
        buckets: std::sync::Mutex<HashMap<String, BucketMeta>>,
    }

    fn unimplemented<T>() -> MetaResult<T> {
        Err(S3Error::InternalError("mock: not implemented".into()))
    }

    fn cred(key: &str) -> S3Credential {
        S3Credential {
            access_key: key.to_string(),
            secret_key: format!("secret-{key}"),
            owner: "default".to_string(),
            created_at: 0,
        }
    }

    fn bucket(name: &str) -> BucketMeta {
        BucketMeta {
            name: name.to_string(),
            owner: "default".to_string(),
            created_at: 0,
            acl: BucketAcl::Private,
            policy: None,
            acl_grants: None,
        }
    }

    impl MockMeta {
        fn with(cred_keys: &[&str], bucket_names: &[&str]) -> (Arc<Self>, Arc<Counts>) {
            let counts = Arc::new(Counts::default());
            let mut creds = HashMap::new();
            for k in cred_keys {
                creds.insert(k.to_string(), cred(k));
            }
            let mut buckets = HashMap::new();
            for b in bucket_names {
                buckets.insert(b.to_string(), bucket(b));
            }
            let m = Arc::new(MockMeta {
                counts: Arc::clone(&counts),
                creds: std::sync::Mutex::new(creds),
                buckets: std::sync::Mutex::new(buckets),
            });
            (m, counts)
        }
    }

    fn cache_ttl_mocks(
        ttl_secs: u64,
        cred_keys: &[&str],
        bucket_names: &[&str],
    ) -> (
        Arc<dyn MetaStore>,
        Arc<MockMeta>,
        Arc<Counts>,
        Arc<S3Metrics>,
    ) {
        let registry = antfs_metric::Registry::new();
        let metrics = Arc::new(S3Metrics::new(&registry).expect("register test metrics"));
        let (inner, counts) = MockMeta::with(cred_keys, bucket_names);
        let mock = Arc::clone(&inner);
        (
            wrap(inner, ttl_secs, Arc::clone(&metrics)),
            mock,
            counts,
            metrics,
        )
    }

    fn n(c: &AtomicUsize) -> usize {
        c.load(Ordering::SeqCst)
    }

    #[async_trait]
    impl MetaStore for MockMeta {
        async fn put_object(&self, _req: PutObjectMeta) -> MetaResult<PutObjectOutcome> {
            unimplemented()
        }
        async fn get_object(&self, _bucket: &str, _key: &str) -> MetaResult<Option<ObjectMeta>> {
            unimplemented()
        }
        async fn delete_object(
            &self,
            _bucket: &str,
            _key: &str,
        ) -> MetaResult<Option<DeletedObject>> {
            unimplemented()
        }
        async fn delete_objects(
            &self,
            _bucket: &str,
            _keys: &[String],
        ) -> MetaResult<Vec<DeleteResult>> {
            unimplemented()
        }
        async fn list_objects(&self, _req: ListObjectsReq) -> MetaResult<ListObjectsResp> {
            unimplemented()
        }
        async fn create_bucket(&self, name: &str, _owner: &str) -> MetaResult<()> {
            self.counts.create_bucket.fetch_add(1, Ordering::SeqCst);
            self.buckets
                .lock()
                .expect("mock lock")
                .insert(name.to_string(), bucket(name));
            Ok(())
        }
        async fn delete_bucket(&self, name: &str) -> MetaResult<()> {
            self.counts.delete_bucket.fetch_add(1, Ordering::SeqCst);
            if self
                .buckets
                .lock()
                .expect("mock lock")
                .remove(name)
                .is_some()
            {
                Ok(())
            } else {
                Err(S3Error::NoSuchBucket)
            }
        }
        async fn head_bucket(&self, name: &str) -> MetaResult<Option<BucketMeta>> {
            self.counts.head_bucket.fetch_add(1, Ordering::SeqCst);
            Ok(self.buckets.lock().expect("mock lock").get(name).cloned())
        }
        async fn list_buckets(&self, _owner: &str) -> MetaResult<Vec<BucketMeta>> {
            unimplemented()
        }
        async fn get_bucket_quota(&self, _bucket: &str) -> MetaResult<Option<BucketQuota>> {
            unimplemented()
        }
        async fn set_bucket_quota(&self, _bucket: &str, _quota_bytes: u64) -> MetaResult<()> {
            unimplemented()
        }
        async fn set_bucket_acl(&self, _bucket: &str, _acl: BucketAcl) -> MetaResult<()> {
            unimplemented()
        }
        async fn set_bucket_policy(&self, bucket: &str, policy: Option<&str>) -> MetaResult<()> {
            self.counts.set_bucket_policy.fetch_add(1, Ordering::SeqCst);
            self.buckets
                .lock()
                .expect("mock lock")
                .get_mut(bucket)
                .expect("bucket exists")
                .policy = policy.map(str::to_string);
            Ok(())
        }
        async fn set_bucket_acl_grants(
            &self,
            _bucket: &str,
            _grants: Option<&str>,
        ) -> MetaResult<()> {
            unimplemented()
        }
        async fn set_object_acl_grants(
            &self,
            _bucket: &str,
            _key: &str,
            _grants: Option<&str>,
        ) -> MetaResult<()> {
            unimplemented()
        }
        async fn create_multipart(
            &self,
            _bucket: &str,
            _key: &str,
            _content_type: Option<&str>,
            _user_meta: &UserMeta,
        ) -> MetaResult<String> {
            unimplemented()
        }
        async fn put_part(&self, _upload_id: &str, _part: PartMeta) -> MetaResult<Option<BlobId>> {
            unimplemented()
        }
        async fn complete_multipart(
            &self,
            _upload_id: &str,
            _parts: &[CompletePart],
            _checksum: CompleteChecksum,
        ) -> MetaResult<CompleteMultipartResult> {
            unimplemented()
        }
        async fn abort_multipart(&self, _upload_id: &str) -> MetaResult<Vec<BlobId>> {
            unimplemented()
        }
        async fn list_parts(
            &self,
            _upload_id: &str,
            _opts: ListPartsOpts,
        ) -> MetaResult<ListPartsResp> {
            unimplemented()
        }
        async fn session_parts(&self, _upload_id: &str) -> MetaResult<Vec<PartMeta>> {
            unimplemented()
        }
        async fn list_multipart_uploads(
            &self,
            _bucket: &str,
            _opts: ListUploadsOpts,
        ) -> MetaResult<ListUploadsResp> {
            unimplemented()
        }
        async fn lookup_credential(&self, access_key: &str) -> MetaResult<Option<S3Credential>> {
            self.counts.lookup_credential.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .creds
                .lock()
                .expect("mock lock")
                .get(access_key)
                .cloned())
        }
        async fn create_credential(&self, _owner: &str) -> MetaResult<S3Credential> {
            self.counts.create_credential.fetch_add(1, Ordering::SeqCst);
            let c = cred("k-new");
            self.creds
                .lock()
                .expect("mock lock")
                .insert(c.access_key.clone(), c.clone());
            Ok(c)
        }
        async fn delete_credential(&self, access_key: &str) -> MetaResult<bool> {
            self.counts.delete_credential.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .creds
                .lock()
                .expect("mock lock")
                .remove(access_key)
                .is_some())
        }
        async fn purge_expired_deletions(&self, _cutoff_ms: i64, _limit: u64) -> MetaResult<u64> {
            unimplemented()
        }
        async fn take_expired_uploads(
            &self,
            _cutoff_ms: i64,
            _limit: u64,
        ) -> MetaResult<Vec<String>> {
            unimplemented()
        }
        async fn delete_parts_of_session(&self, _upload_id: &str) -> MetaResult<Vec<BlobId>> {
            unimplemented()
        }
        async fn pending_reclaims_batch(&self, _limit: u64) -> MetaResult<Vec<BlobId>> {
            unimplemented()
        }
        async fn resolve_pending_reclaims(&self, _blob_ids: &[BlobId]) -> MetaResult<u64> {
            unimplemented()
        }
        async fn pending_reclaims_count(&self) -> MetaResult<u64> {
            unimplemented()
        }
        async fn scan_sse_objects(
            &self,
            _bucket: Option<&str>,
            _after: Option<(String, String)>,
            _limit: u64,
        ) -> MetaResult<Vec<ObjectMeta>> {
            unimplemented()
        }
    }

    #[tokio::test]
    async fn credential_lookup_second_hits_cache() {
        let (store, _, counts, metrics) = cache_ttl_mocks(30, &["k-a"], &[]);
        assert!(store.lookup_credential("k-a").await.unwrap().is_some());
        assert!(store.lookup_credential("k-a").await.unwrap().is_some());
        assert_eq!(n(&counts.lookup_credential), 1, "第二次须命中缓存");
        assert_eq!(
            metrics
                .meta_cache_lookups_total
                .with_label_values(&["credential", "miss"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .meta_cache_lookups_total
                .with_label_values(&["credential", "hit"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn unknown_access_key_is_negatively_cached() {
        let (store, mock, counts, metrics) = cache_ttl_mocks(30, &["k-a"], &[]);
        assert!(store.lookup_credential("nope").await.unwrap().is_none());
        assert!(store.lookup_credential("nope").await.unwrap().is_none());
        assert_eq!(n(&counts.lookup_credential), 1, "负条目 5s 内只打一次库");
        assert!(mock.creds.lock().unwrap().contains_key("k-a"));
        assert_eq!(
            metrics
                .meta_cache_lookups_total
                .with_label_values(&["credential", "hit"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn delete_credential_invalidates_cache() {
        let (store, _, counts, _) = cache_ttl_mocks(30, &["k-a"], &[]);
        assert!(store.lookup_credential("k-a").await.unwrap().is_some());
        assert!(store.delete_credential("k-a").await.unwrap());
        // 失效后重查:打库且见删除后的真相(None 被负缓存)。
        assert!(store.lookup_credential("k-a").await.unwrap().is_none());
        assert!(store.lookup_credential("k-a").await.unwrap().is_none());
        assert_eq!(n(&counts.lookup_credential), 2);
    }

    #[tokio::test]
    async fn create_credential_clears_stale_negative_entry() {
        let (store, _, counts, _) = cache_ttl_mocks(30, &[], &[]);
        // 建前先踩一发负缓存(旧 5s 负条目)。
        assert!(store.lookup_credential("k-new").await.unwrap().is_none());
        let c = store.create_credential("default").await.unwrap();
        assert_eq!(c.access_key, "k-new");
        // 同进程建号即失效:立即可见,不再被负缓存挡。
        assert!(store.lookup_credential("k-new").await.unwrap().is_some());
        assert_eq!(n(&counts.lookup_credential), 2);
    }

    #[tokio::test]
    async fn bucket_meta_hit_and_write_invalidation() {
        let (store, _, counts, _) = cache_ttl_mocks(30, &[], &["bkt"]);
        assert_eq!(
            store
                .head_bucket("bkt")
                .await
                .unwrap()
                .expect("exists")
                .policy,
            None
        );
        assert!(store.head_bucket("bkt").await.unwrap().is_some());
        assert_eq!(n(&counts.head_bucket), 1, "第二次命中");
        // 桶策略写在本进程 ⇒ 即时失效 ⇒ 重查见到新值。
        store
            .set_bucket_policy("bkt", Some("{\"v\":1}"))
            .await
            .unwrap();
        assert_eq!(
            store
                .head_bucket("bkt")
                .await
                .unwrap()
                .expect("exists")
                .policy,
            Some("{\"v\":1}".to_string())
        );
        assert_eq!(n(&counts.head_bucket), 2);
        // 延迟删除然后判断删除后的重查
        store.delete_bucket("bkt").await.unwrap();
        assert!(store.head_bucket("bkt").await.unwrap().is_none());
        assert_eq!(n(&counts.head_bucket), 3);
    }

    #[tokio::test]
    async fn create_bucket_clears_stale_negative_bucket_entry() {
        let (store, _, counts, _) = cache_ttl_mocks(30, &[], &[]);
        assert!(store.head_bucket("fresh").await.unwrap().is_none());
        store.create_bucket("fresh", "default").await.unwrap();
        assert!(store.head_bucket("fresh").await.unwrap().is_some());
        assert_eq!(n(&counts.head_bucket), 2);
    }

    #[tokio::test]
    async fn expired_entry_misses_deterministically() {
        let registry = antfs_metric::Registry::new();
        let metrics = Arc::new(S3Metrics::new(&registry).unwrap());
        let (inner, counts) = MockMeta::with(&["k-a"], &[]);
        // 直构装饰器(测试与被测同模块,可入私有图倒拨到期时刻 —— 免 sleep 的确定性时效,
        // CLAUDE.md §5.6)。
        let caching = Arc::new(CachingMetaStore {
            inner,
            metrics,
            ttl: Duration::from_secs(30),
            creds: RwLock::new(HashMap::new()),
            buckets: RwLock::new(HashMap::new()),
        });
        let store: Arc<dyn MetaStore> = caching.clone();
        assert!(store.lookup_credential("k-a").await.unwrap().is_some());
        if let Some(e) = caching.creds.write().expect("cache lock").get_mut("k-a") {
            e.expires = Instant::now() - Duration::from_secs(1);
        }
        assert!(store.lookup_credential("k-a").await.unwrap().is_some());
        assert_eq!(n(&counts.lookup_credential), 2, "过期条目须穿透重查");
    }

    #[tokio::test]
    async fn ttl_zero_disables_cache_entirely() {
        let registry = antfs_metric::Registry::new();
        let metrics = Arc::new(S3Metrics::new(&registry).unwrap());
        let (inner, _) = MockMeta::with(&["k-a"], &[]);
        let inner_dyn: Arc<dyn MetaStore> = inner;
        let out = wrap(inner_dyn.clone(), 0, metrics);
        // 0 = 关:wrap 不装配,原 Arc 原样归还(零行为零开销逃生口)。
        assert!(Arc::ptr_eq(&inner_dyn, &out));
        // 行为旁证:关闭态两次访问各打各的库(无缓存)。
        assert!(out.lookup_credential("k-a").await.unwrap().is_some());
        assert!(out.lookup_credential("k-a").await.unwrap().is_some());
    }
}
