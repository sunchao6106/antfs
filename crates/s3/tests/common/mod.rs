//! Shared harness for the S3 gateway end-to-end tests.
//!
//! Follows the layout of `crates/frontend/tests/rest.rs`: a real in-process cluster (1 Raft master +
//! 3 store nodes) plus a real S3 gateway bound to a real port, driven over real HTTP with a full
//! AWS SigV4 signature on every request (no auth bypass). The blob data path goes through the real
//! router SDK; only the per-object metadata lives in a relational MetaStore —— PostgreSQL by
//! default (`TEST_S3_POSTGRES_URL`), or MySQL with `TEST_S3_DRIVER=mysql` (`TEST_S3_MYSQL_URL`).
//!
//! The cluster is bootstrapped purely through Raft commits and the store data-plane admin endpoint,
//! so there is no heartbeat wait and no `sleep` (CLAUDE.md §5.6).
//!
//! Every test is `#[ignore]`d because it needs a live database: run with
//! `TEST_S3_POSTGRES_URL=postgres://... cargo test -p antfs-s3 -- --ignored`
//! or `TEST_S3_DRIVER=mysql TEST_S3_MYSQL_URL=mysql://... cargo test -p antfs-s3 -- --ignored`.

// Each integration-test binary includes this module but only touches a subset of it, so the
// shared scaffolding legitimately has items unused by some binaries. `dead_code` would fire
// per-binary otherwise.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::ServiceExt;

use antfs_config::Validate;
use antfs_master::config::MasterConfig;
use antfs_master::types::{DiskSpec, MasterCommand, MasterResponse, NodeInfo, ReplicaPlacement};
use antfs_master::MasterNode;
use antfs_router::RouterConfig;
use antfs_s3::config::{MetaStoreConfig, S3GatewayConfig};
use antfs_s3::metrics::S3Metrics;
use antfs_s3::server::{build_app, AppState};
use antfs_store::{StoreServer, StoreServerConfig};
use chrono::Utc;
use hmac::{Hmac, Mac};
use openraft::BasicNode;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use sha2::{Digest, Sha256};

const SECRET: &str = "test-secret";
/// Every data node owns a single disk with this id (matches the placement instruction).
const DISK: &str = "d0";
/// Per-request body cap, aligned with `StoreServerConfig::default` (enough for the 2 MiB object test).
const MAX_BODY_BYTES: u64 = 8 << 20;
/// The region the gateway signs and verifies in.
pub const REGION: &str = "us-east-1";
/// The owner every fixture credential is created under. `CreateBucket` hardcodes `"default"` as the
/// bucket owner, so `ListBuckets` (filtered by the credential owner) only sees those buckets when
/// the caller owns the `"default"` identity too.
const DEFAULT_OWNER: &str = "default";
/// Marker for a payload that is not part of the signature (`X-Amz-Content-Sha256`).
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// AWS URI-encoding set: unreserved characters (`A-Za-z0-9-_.~`) stay literal.
const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

// ── cluster bootstrap (mirrors crates/frontend/tests/rest.rs) ────────────────────────────────

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

fn addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Start a single-node master (bootstrap → wait until it elects itself leader).
async fn solo_master(dir: &std::path::Path) -> (MasterNode, String) {
    let raft_addr = addr(free_port());
    let grpc_addr = addr(free_port());
    let cfg = MasterConfig {
        node_id: 1,
        raft_addr: raft_addr.clone(),
        grpc_addr: grpc_addr.clone(),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        partition_capacity_bytes: 64 << 20,
        // 与 store 段一致(段 ≥ 容量会被守卫拒启动;503 偶发根因,见 DEVLOG 步骤 48)
        segment_capacity_bytes: 8 << 20,
        id_alloc_segment_size: 4,
        id_alloc_raft_batch_multiple: 2,
        reconcile_startup_grace_secs: 0,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg, &registry)
        .await
        .expect("start master");
    node.serve().expect("serve master");
    node.initialize(BTreeMap::from([(1u64, BasicNode::new(raft_addr))]))
        .await
        .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "leader elected")
        .await
        .expect("leader elected");
    (node, grpc_addr)
}

/// Start a real data node (deliberately not pointed at the master: registration goes through a
/// direct Raft write so there is no heartbeat noise to wait on).
async fn start_store(node_id: u64, dir: &std::path::Path) -> (StoreServer, String) {
    let http_addr = addr(free_port());
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: http_addr.clone(),
        master_addr: String::new(),
        partition_capacity_bytes: 64 << 20,
        max_body_bytes: MAX_BODY_BYTES,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(
        cfg,
        antfs_blob::StoreConfig {
            segment_size_bytes: 8 << 20,
            index_max_inmem_elements: 100_000,
            io_error_count_to_trigger_shutdown: 0,
            defer_seal_flush: true,
        },
        &registry,
    )
    .expect("start store");
    srv.serve().await.expect("serve store");
    (srv, http_addr)
}

/// Persist a data node's identity through Raft (the test knows the address).
async fn register_node(master: &MasterNode, node_id: u64, http_addr: &str) {
    let info = NodeInfo {
        dc: "dc1".into(),
        rack: "r1".into(),
        addr: http_addr.to_string(),
        disks: BTreeMap::from([(
            DISK.to_string(),
            DiskSpec {
                capacity_bytes: 1 << 40,
            },
        )]),
        registered_at_ms: 1,
    };
    master
        .raft
        .client_write(MasterCommand::RegisterNode { node_id, info })
        .await
        .expect("register node");
}

fn placement(node_id: u64) -> ReplicaPlacement {
    ReplicaPlacement {
        node_id,
        disk_id: DISK.to_string(),
        target_state: "ACTIVE".to_string(),
        // Placeholder: the real incarnation is assigned by the master's apply path.
        incarnation: 0,
    }
}

/// Create a partition through Raft with an exact replica set; returns the partition id.
async fn create_partition(master: &MasterNode, replicas: Vec<ReplicaPlacement>) -> u32 {
    let rf = u8::try_from(replicas.len()).expect("rf fits u8");
    let resp = master
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: rf,
            replicas,
            partition_capacity: 0,
        })
        .await
        .expect("create partition");
    match resp.data {
        MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected apply response: {other:?}"),
    }
}

/// Create the replica for `pid` on a store (data-plane admin endpoint).
async fn ensure_replica(store_addr: &str, pid: u32) {
    let resp = reqwest::Client::new()
        .post(format!("http://{store_addr}/admin/partitions/{pid}"))
        .send()
        .await
        .expect("admin create replica");
    assert_eq!(
        resp.status().as_u16(),
        201,
        "create replica on {store_addr}"
    );
}

// ── SigV4 signing (client side) ──────────────────────────────────────────────────────────────
//
// Mirrors the server verifier in `crates/s3/src/auth/sigv4.rs`: same canonical request, same
// `AWS4` HMAC key chain. Keeping it independent (rather than calling the crate's own signer)
// means the signature path is actually exercised end to end.

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

/// Derive the SigV4 signing key: `AWS4<secret>` → date → region → `s3` → `aws4_request`.
fn signing_key(secret_key: &str, date: &str, region: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn uri_encode(input: &str) -> String {
    utf8_percent_encode(input, URI_ENCODE_SET).to_string()
}

fn split_uri(uri: &str) -> (&str, &str) {
    match uri.split_once('?') {
        Some((path, query)) => (path, query),
        None => (uri, ""),
    }
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let decoded = percent_decode_str(path).decode_utf8_lossy();
    decoded
        .split('/')
        .map(uri_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn canonical_querystring(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        let key = percent_decode_str(key).decode_utf8_lossy();
        let value = percent_decode_str(value).decode_utf8_lossy();
        pairs.push((uri_encode(&key), uri_encode(&value)));
    }
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn normalize_header_value(value: &str) -> String {
    let trimmed = value.trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for ch in trimmed.chars() {
        if ch == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out
}

fn canonical_request(method: &str, uri: &str, headers: &HeaderMap, payload_hash: &str) -> String {
    let (path, query) = split_uri(uri);
    let canonical_uri = canonical_uri(path);
    let canonical_query = canonical_querystring(query);

    let signed_headers = ["host", "x-amz-content-sha256", "x-amz-date"];
    let canonical_headers = signed_headers
        .iter()
        .map(|name| {
            let value = headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .expect("signed header is always set before canonicalization");
            format!("{name}:{}\n", normalize_header_value(value))
        })
        .collect::<String>();
    let signed = signed_headers.join(";");

    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload_hash}"
    )
}

fn sign_with_hash(
    method: &str,
    uri: &str,
    headers: &mut HeaderMap,
    payload_hash: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> String {
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_str(payload_hash).expect("valid payload hash header"),
    );
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    headers.insert(
        "x-amz-date",
        HeaderValue::from_str(&timestamp).expect("valid date header"),
    );

    let date = &timestamp[..8];
    let canonical = canonical_request(method, uri, headers, payload_hash);
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = signing_key(secret_key, date, region);
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    )
}

/// Sign a request with the SHA-256 of `body` as the payload hash; returns the `Authorization` value.
/// `headers` must already carry `host`; `x-amz-content-sha256` and `x-amz-date` are inserted here.
pub fn sign_request(
    method: &str,
    uri: &str,
    headers: &mut HeaderMap,
    body: &[u8],
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> String {
    let payload_hash = hex::encode(Sha256::digest(body));
    sign_with_hash(
        method,
        uri,
        headers,
        &payload_hash,
        access_key,
        secret_key,
        region,
    )
}

/// Like [`sign_request`] but declares `UNSIGNED-PAYLOAD` (the body is not part of the signature).
pub fn sign_request_unsigned(
    method: &str,
    uri: &str,
    headers: &mut HeaderMap,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> String {
    sign_with_hash(
        method,
        uri,
        headers,
        UNSIGNED_PAYLOAD,
        access_key,
        secret_key,
        region,
    )
}

/// Build a fully signed header map for `host` (payload hash = SHA-256 of `body`).
pub fn signed_headers_for(
    host: &str,
    access_key: &str,
    secret_key: &str,
    method: &str,
    uri: &str,
    body: &[u8],
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "host",
        HeaderValue::from_str(host).expect("valid host header"),
    );
    let auth = sign_request(
        method,
        uri,
        &mut headers,
        body,
        access_key,
        secret_key,
        REGION,
    );
    headers.insert(
        "authorization",
        HeaderValue::from_str(&auth).expect("valid authorization header"),
    );
    headers
}

/// Like [`signed_headers_for`] but with an `UNSIGNED-PAYLOAD` signature.
pub fn unsigned_headers_for(
    host: &str,
    access_key: &str,
    secret_key: &str,
    method: &str,
    uri: &str,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "host",
        HeaderValue::from_str(host).expect("valid host header"),
    );
    let auth = sign_request_unsigned(method, uri, &mut headers, access_key, secret_key, REGION);
    headers.insert(
        "authorization",
        HeaderValue::from_str(&auth).expect("valid authorization header"),
    );
    headers
}

// ── fixture ──────────────────────────────────────────────────────────────────────────────────

/// One in-process cluster: 1 master + 3 stores + 1 S3 gateway, plus a credential created through
/// the (public) admin endpoint and kept for signing.
pub struct S3Fixture {
    /// Kept alive for the duration of the test.
    pub _master: MasterNode,
    pub _stores: Vec<StoreServer>,
    /// S3 gateway listen address.
    pub s3_addr: String,
    pub access_key: String,
    pub secret_key: String,
    pub http: reqwest::Client,
    /// Short random suffix used to keep bucket names unique across runs (the metastore persists).
    pub run_id: String,
    /// MetaStore dialect of this fixture("postgres" | "mysql")—— 行级断言助手按它分言。
    pub driver: String,
    /// The MetaStore URL the fixture connected to(row-level assertion helper 直连用)。
    pub metastore_url: String,
    /// The gateway's live `AppState` — lets tests drive metastore / router / GC directly.
    pub state: antfs_s3::server::AppState,
    /// Kept last so the data directories outlive master/store on drop.
    pub _tmp: tempfile::TempDir,
}

impl S3Fixture {
    /// 本 fixture 桶内遮蔽行(`state=0`)的行数 —— 方言中性行级断言(PG `$1` / MySQL `?`)。
    /// 共享测试库:GC 是全库语义,断言一律限本 fixture 的 bucket 域。
    pub async fn shadowed_rows_in_bucket(&self, bucket: &str) -> i64 {
        match self.driver.as_str() {
            "mysql" => {
                let pool = sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(2)
                    .connect(&self.metastore_url)
                    .await
                    .expect("direct mysql connect");
                let r: (i64,) = sqlx::query_as(
                    "SELECT count(*) FROM named_objects WHERE bucket = ? AND state = 0",
                )
                .bind(bucket)
                .fetch_one(&pool)
                .await
                .expect("count query");
                r.0
            }
            _ => {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&self.metastore_url)
                    .await
                    .expect("direct pg connect");
                let r: (i64,) = sqlx::query_as(
                    "SELECT count(*) FROM named_objects WHERE bucket = $1 AND state = 0",
                )
                .bind(bucket)
                .fetch_one(&pool)
                .await
                .expect("count query");
                r.0
            }
        }
    }

    pub async fn start() -> S3Fixture {
        Self::start_with_sse(true).await
    }

    /// `sse` = false builds the fixture with NO SSE master key (the 501 regime).
    pub async fn start_with_sse(sse: bool) -> S3Fixture {
        let driver = std::env::var("TEST_S3_DRIVER").unwrap_or_else(|_| "postgres".to_string());
        let db_url = match driver.as_str() {
            "postgres" => std::env::var("TEST_S3_POSTGRES_URL").expect(
                "TEST_S3_POSTGRES_URL must be set to run S3 integration tests \
                 (they are #[ignore]d by default)",
            ),
            "mysql" => std::env::var("TEST_S3_MYSQL_URL").expect(
                "TEST_S3_MYSQL_URL must be set to run S3 integration tests \
                 (they are #[ignore]d by default)",
            ),
            other => panic!("unknown TEST_S3_DRIVER {other:?} (expected postgres|mysql)"),
        };

        let tmp = tempfile::tempdir().expect("tempdir");
        let (master, grpc_addr) = solo_master(&tmp.path().join("master")).await;

        let mut stores = Vec::new();
        let mut store_addrs: BTreeMap<u64, String> = BTreeMap::new();
        for i in 0..3u64 {
            let node_id = 101 + i;
            let dir = tmp.path().join(format!("store-{node_id}"));
            std::fs::create_dir_all(&dir).expect("mkdir store dir");
            let (srv, http_addr) = start_store(node_id, &dir).await;
            register_node(&master, node_id, &http_addr).await;
            store_addrs.insert(node_id, http_addr);
            stores.push(srv);
        }

        // Place two replicas (RF=2 → write quorum = 2 = all replicas, so read-after-write is
        // deterministic within a partition).
        let pid = create_partition(&master, vec![placement(101), placement(102)]).await;
        for node_id in [101u64, 102] {
            if let Some(store_addr) = store_addrs.get(&node_id) {
                ensure_replica(store_addr, pid).await;
            }
        }

        let registry = antfs_metric::Registry::new();
        let metrics = S3Metrics::new(&registry).expect("register s3 metrics");

        let router_cfg = RouterConfig {
            master_addr: grpc_addr,
            cluster_secret: SECRET.to_string(),
            request_timeout_secs: 10,
            id_segment_size: 4,
            partition_refresh_secs: 30,
            ..Default::default()
        };
        let router = antfs_router::Router::connect(router_cfg.clone(), &registry)
            .await
            .expect("connect router");

        let metastore_cfg = MetaStoreConfig {
            driver: driver.clone(),
            url: db_url.clone(),
            max_connections: 5,
        };
        let metastore = antfs_s3::metastore::connect(&metastore_cfg)
            .await
            .expect("connect metastore");

        // `s3-sse` 夹具钥:tmp 目录随机 64 hex;sse=false分支不生成(None = 501 面)。
        let sse_material = sse.then(|| {
            format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            )
        });
        let key_path = tmp.path().join("sse-master.key");
        if let Some(material) = &sse_material {
            std::fs::write(&key_path, material).expect("write fixture sse master key");
        }
        let master_key = if sse_material.is_some() {
            Some(std::sync::Arc::new(
                antfs_s3::sse::MasterKey::load(key_path.to_string_lossy().as_ref())
                    .expect("fixture master key just written loads"),
            ))
        } else {
            None
        };
        let s3_addr = addr(free_port());
        let cfg = S3GatewayConfig {
            listen_addr: s3_addr.clone(),
            region: REGION.to_string(),
            operator_token: String::new(),
            metastore: metastore_cfg,
            router: router_cfg,
            // vhost e2e(tests/vhost.rs)设 TEST_S3_VHOST_DOMAIN 起「后缀开」夹具;
            // 其余二进制不设 → 默认空 = 关 —— 它们就是「默认关零变化」的回归。
            virtual_host_domain: std::env::var("TEST_S3_VHOST_DOMAIN").unwrap_or_default(),
            sse_master_key_file: if sse {
                key_path.to_string_lossy().into_owned()
            } else {
                String::new()
            },
            ..Default::default()
        };
        cfg.validate().expect("s3 gateway config");

        let state = AppState {
            router: Arc::new(router),
            metastore,
            metrics: Arc::new(metrics),
            registry,
            config: Arc::new(cfg),
            sse: master_key,
        };
        let listener = tokio::net::TcpListener::bind(&s3_addr)
            .await
            .expect("bind s3 gateway");
        let app = build_app(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });

        // Create the fixture credential through the public admin endpoint (no signing needed).
        let http = reqwest::Client::new();
        let resp = http
            .post(format!("http://{s3_addr}/admin/credentials"))
            .json(&serde_json::json!({ "owner": DEFAULT_OWNER }))
            .send()
            .await
            .expect("create credential request");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "admin create credential must return 200"
        );
        let body: serde_json::Value = resp.json().await.expect("credential json");
        let access_key = body["access_key"]
            .as_str()
            .expect("credential access_key")
            .to_string();
        let secret_key = body["secret_key"]
            .as_str()
            .expect("credential secret_key")
            .to_string();

        let run_id: String = uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect();

        S3Fixture {
            _master: master,
            _stores: stores,
            s3_addr,
            access_key,
            secret_key,
            http,
            run_id,
            driver,
            metastore_url: db_url,
            state,
            _tmp: tmp,
        }
    }

    pub fn url(&self, uri: &str) -> String {
        format!("http://{}{uri}", self.s3_addr)
    }

    /// A bucket name unique to this fixture run (the metastore persists between runs).
    pub fn unique(&self, tag: &str) -> String {
        format!("{tag}-{}", self.run_id)
    }

    /// Create a uniquely named bucket and assert it succeeded; returns the name.
    pub async fn create_unique_bucket(&self, tag: &str) -> String {
        let name = self.unique(tag);
        let resp = self.create_bucket(&name).await;
        assert_eq!(resp.status().as_u16(), 200, "create bucket {name}");
        name
    }

    /// Build signed headers (payload hash = SHA-256 of `body`) for a request to this gateway.
    pub fn signed_headers(&self, method: &str, uri: &str, body: &[u8]) -> HeaderMap {
        signed_headers_for(
            &self.s3_addr,
            &self.access_key,
            &self.secret_key,
            method,
            uri,
            body,
        )
    }

    /// Send an already-signed request. `uri` is the path + optional query.
    pub async fn send(
        &self,
        method: Method,
        uri: &str,
        headers: HeaderMap,
        body: Vec<u8>,
    ) -> reqwest::Response {
        self.http
            .request(method, self.url(uri))
            .headers(headers)
            .body(body)
            .send()
            .await
            .expect("send request")
    }

    /// Sign and send a request (payload hash = SHA-256 of `body`).
    pub async fn request(&self, method: Method, uri: &str, body: Vec<u8>) -> reqwest::Response {
        let headers = self.signed_headers(method.as_str(), uri, &body);
        self.send(method, uri, headers, body).await
    }

    // ── bucket operations ──────────────────────────────────────────────────────────────────

    pub async fn create_bucket(&self, bucket: &str) -> reqwest::Response {
        self.request(Method::PUT, &format!("/{bucket}"), Vec::new())
            .await
    }

    pub async fn delete_bucket(&self, bucket: &str) -> reqwest::Response {
        self.request(Method::DELETE, &format!("/{bucket}"), Vec::new())
            .await
    }

    pub async fn head_bucket(&self, bucket: &str) -> reqwest::Response {
        self.request(Method::HEAD, &format!("/{bucket}"), Vec::new())
            .await
    }

    pub async fn list_buckets(&self) -> reqwest::Response {
        self.request(Method::GET, "/", Vec::new()).await
    }

    // ── object operations ──────────────────────────────────────────────────────────────────

    pub async fn put_object(&self, bucket: &str, key: &str, body: &[u8]) -> reqwest::Response {
        self.request(Method::PUT, &format!("/{bucket}/{key}"), body.to_vec())
            .await
    }

    /// PUT carrying extra request headers unsigned. SigV4 only signs host/x-amz-*, so
    /// additional headers (Content-Type, x-amz-meta-*) travel unsigned by design.
    pub async fn put_object_with_headers(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}");
        let mut headers = self.signed_headers("PUT", &uri, body);
        self.insert_extra_headers(&mut headers, extra);
        self.send(Method::PUT, &uri, headers, body.to_vec()).await
    }

    /// POST `?uploads` carrying extra request headers unsigned (CreateMultipartUpload with metadata).
    pub async fn create_multipart_with_headers(
        &self,
        bucket: &str,
        key: &str,
        extra: &[(&str, &str)],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploads=");
        let mut headers = self.signed_headers("POST", &uri, &[]);
        self.insert_extra_headers(&mut headers, extra);
        self.send(Method::POST, &uri, headers, Vec::new()).await
    }

    fn insert_extra_headers(&self, headers: &mut HeaderMap, extra: &[(&str, &str)]) {
        for (k, v) in extra {
            let name = HeaderName::from_bytes(k.as_bytes()).expect("valid header name");
            let value = HeaderValue::from_str(v).expect("valid header value");
            headers.insert(name, value);
        }
    }

    /// PUT with an `UNSIGNED-PAYLOAD` signature.
    pub async fn put_object_unsigned(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}");
        let headers = unsigned_headers_for(
            &self.s3_addr,
            &self.access_key,
            &self.secret_key,
            "PUT",
            &uri,
        );
        self.send(Method::PUT, &uri, headers, body.to_vec()).await
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> reqwest::Response {
        self.request(Method::GET, &format!("/{bucket}/{key}"), Vec::new())
            .await
    }

    /// GET carrying extra request headers unsigned(SigV4 只签 host/x-amz-*,
    /// `Range` 等头按约定不参签,与 put_object_with_headers 同型)。
    pub async fn get_object_with_headers(
        &self,
        bucket: &str,
        key: &str,
        extra: &[(&str, &str)],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}");
        let mut headers = self.signed_headers("GET", &uri, &[]);
        self.insert_extra_headers(&mut headers, extra);
        self.send(Method::GET, &uri, headers, Vec::new()).await
    }

    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: &str,
    ) -> reqwest::Response {
        self.get_object_with_headers(bucket, key, &[("range", range)])
            .await
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> reqwest::Response {
        self.request(Method::HEAD, &format!("/{bucket}/{key}"), Vec::new())
            .await
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> reqwest::Response {
        self.request(Method::DELETE, &format!("/{bucket}/{key}"), Vec::new())
            .await
    }

    pub async fn list_objects_v2(&self, bucket: &str, query: &str) -> reqwest::Response {
        self.request(Method::GET, &format!("/{bucket}?{query}"), Vec::new())
            .await
    }

    // ── multipart operations ───────────────────────────────────────────────────────────────

    pub async fn create_multipart(&self, bucket: &str, key: &str) -> reqwest::Response {
        self.request(
            Method::POST,
            &format!("/{bucket}/{key}?uploads="),
            Vec::new(),
        )
        .await
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber={part_number}");
        self.request(Method::PUT, &uri, body.to_vec()).await
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        xml_body: &[u8],
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}");
        self.request(Method::POST, &uri, xml_body.to_vec()).await
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}");
        self.request(Method::DELETE, &uri, Vec::new()).await
    }

    pub async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}");
        self.request(Method::GET, &uri, Vec::new()).await
    }

    pub async fn list_multipart_uploads(&self, bucket: &str) -> reqwest::Response {
        self.request(Method::GET, &format!("/{bucket}?uploads="), Vec::new())
            .await
    }

    // ── admin: bucket quota(/admin 路径无 SigV4;fixture 未配 operator_token = 开放)──

    /// Set the bucket's storage quota (bytes; 0 = unlimited) via the admin endpoint.
    pub async fn set_bucket_quota(&self, bucket: &str, quota_bytes: u64) {
        let resp = self
            .http
            .put(format!(
                "http://{}/admin/buckets/{bucket}/quota",
                self.s3_addr
            ))
            .json(&serde_json::json!({ "quota_bytes": quota_bytes }))
            .send()
            .await
            .expect("set quota request");
        assert_eq!(resp.status().as_u16(), 200, "set quota on {bucket}");
    }

    /// Read quota accounting via the admin endpoint → (quota_bytes, used_bytes)。
    pub async fn bucket_quota(&self, bucket: &str) -> (u64, u64) {
        let resp = self
            .http
            .get(format!(
                "http://{}/admin/buckets/{bucket}/quota",
                self.s3_addr
            ))
            .send()
            .await
            .expect("get quota request");
        assert_eq!(resp.status().as_u16(), 200, "quota on {bucket}");
        let v: serde_json::Value = resp.json().await.expect("quota json");
        (
            v["quota_bytes"].as_u64().expect("quota_bytes field"),
            v["used_bytes"].as_u64().expect("used_bytes field"),
        )
    }

    /// 签发第二租户凭证(经 admin 端点;跨租户 ACL 测试用),返回 (access_key, secret_key)。
    pub async fn issue_credential(&self, owner: &str) -> (String, String) {
        let resp = self
            .http
            .post(format!("http://{}/admin/credentials", self.s3_addr))
            .json(&serde_json::json!({ "owner": owner }))
            .send()
            .await
            .expect("issue credential request");
        assert_eq!(resp.status().as_u16(), 200, "issue credential for {owner}");
        let v: serde_json::Value = resp.json().await.expect("credential json");
        (
            v["access_key"].as_str().expect("access_key").to_string(),
            v["secret_key"].as_str().expect("secret_key").to_string(),
        )
    }

    /// 以指定凭证签发一次签名请求(默认凭证之外的租户;跨租户 ACL 测试)。
    pub async fn request_as(
        &self,
        access_key: &str,
        secret_key: &str,
        method: &str,
        uri: &str,
        body: &[u8],
    ) -> reqwest::Response {
        let headers = signed_headers_for(&self.s3_addr, access_key, secret_key, method, uri, body);
        let method: reqwest::Method = method.parse().expect("valid method");
        self.http
            .request(method, self.url(uri))
            .headers(headers)
            .body(body.to_vec())
            .send()
            .await
            .expect("signed request send")
    }

    // ── aws-chunked framed PUT / UploadPart(s3-chunked-payload e2e)────────────────

    fn streaming_headers(
        &self,
        method: &str,
        uri: &str,
        literal: &str,
        declared: u64,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            HeaderValue::from_str(&self.s3_addr).expect("valid host header"),
        );
        let (authorization, _seed) = sign_streaming_request(
            method,
            uri,
            &mut headers,
            literal,
            &self.access_key,
            &self.secret_key,
            REGION,
        );
        headers.insert(
            "authorization",
            HeaderValue::from_str(&authorization).expect("valid authorization header"),
        );
        headers.insert(
            "content-encoding",
            HeaderValue::from_str("aws-chunked").expect("valid encoding header"),
        );
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_str(&declared.to_string()).expect("valid length header"),
        );
        headers
    }

    /// PUT with a pre-encoded frame stream and an explicit declared decoded
    /// length(negative tests control both knobs independently).
    pub async fn put_object_chunked_raw(
        &self,
        bucket: &str,
        key: &str,
        literal: &str,
        declared: u64,
        framed: Vec<u8>,
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}");
        let headers = self.streaming_headers("PUT", &uri, literal, declared);
        self.send(Method::PUT, &uri, headers, framed).await
    }

    /// One signing pass → framed payload + headers(块长可调)。负例测试拿走两件
    /// 后自行篡改再经 [`Self::send`] 出线。
    pub fn chunked_sign_and_frame(
        &self,
        method: &str,
        uri: &str,
        data: &[u8],
        chunk_len: usize,
    ) -> (HeaderMap, Vec<u8>) {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            HeaderValue::from_str(&self.s3_addr).expect("valid host header"),
        );
        let (authorization, seed) = sign_streaming_request(
            method,
            uri,
            &mut headers,
            STREAMING_SIGNED,
            &self.access_key,
            &self.secret_key,
            REGION,
        );
        headers.insert(
            "authorization",
            HeaderValue::from_str(&authorization).expect("valid authorization header"),
        );
        headers.insert(
            "content-encoding",
            HeaderValue::from_str("aws-chunked").expect("valid encoding header"),
        );
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_str(&data.len().to_string()).expect("valid length header"),
        );
        let framed = aws_chunked_signed_body(&self.secret_key, &seed, data, chunk_len);
        (headers, framed)
    }

    /// Signed-flavor framed PUT(申报长度 = 实长)。
    pub async fn put_object_chunked(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        chunk_len: usize,
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}");
        let (headers, framed) = self.chunked_sign_and_frame("PUT", &uri, data, chunk_len);
        self.send(Method::PUT, &uri, headers, framed).await
    }

    /// Signed-flavor framed UploadPart(申报长度 = 实长)。
    pub async fn upload_part_chunked(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: &[u8],
        chunk_len: usize,
    ) -> reqwest::Response {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber={part_number}");
        let (headers, framed) = self.chunked_sign_and_frame("PUT", &uri, data, chunk_len);
        self.send(Method::PUT, &uri, headers, framed).await
    }
}

// ── small helpers ────────────────────────────────────────────────────────────────────────────

/// Read a response header as a string (panics with the header name when missing).
pub fn header(resp: &reqwest::Response, name: &str) -> String {
    resp.headers()
        .get(name)
        .unwrap_or_else(|| panic!("response missing header {name}"))
        .to_str()
        .expect("header is utf8")
        .to_string()
}

/// Deterministic pseudo-random bytes (no `rand`: contents just need to be non-trivial and
/// byte-checkable).
pub fn pseudo_random(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 8);
    let mut x: u64 = 0x243F_6A88_85A3_08D3;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Lowercase MD5 hex of a buffer (the S3 single-part ETag).
pub fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest as _, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Extract the text between `<tag>` and `</tag>` from a response XML body.
pub fn xml_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("response XML missing <{tag}>: {xml}"))
        + open.len();
    let rest = &xml[start..];
    let end = rest
        .find(&close)
        .unwrap_or_else(|| panic!("response XML missing </{tag}>: {xml}"));
    rest[..end].to_string()
}

// ── aws-chunked client-side framing (s3-chunked-payload) ───────────────────────────────────
//
// Mirrors the server verifier in `crates/s3/src/auth/chunked.rs`: same chunk
// signature chain (seed = request signature from the Authorization header).

/// STREAMING payload-hash literals the gateway accepts.
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

/// Material the chunk signature chain needs: the request signature is the seed.
#[derive(Clone)]
pub struct ChunkSeed {
    pub timestamp: String,
    pub scope: String,
    pub signature: String,
}

/// Sign a request whose payload hash is a STREAMING-* literal; returns the
/// Authorization header value plus the seed for chunk signature chaining.
/// `headers` must already carry `host`; the `x-amz-*` pair is inserted here.
pub fn sign_streaming_request(
    method: &str,
    uri: &str,
    headers: &mut HeaderMap,
    literal: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> (String, ChunkSeed) {
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_str(literal).expect("valid literal header"),
    );
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    headers.insert(
        "x-amz-date",
        HeaderValue::from_str(&timestamp).expect("valid date header"),
    );

    let date = &timestamp[..8];
    let canonical = canonical_request(method, uri, headers, literal);
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = signing_key(secret_key, date, region);
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    (
        authorization,
        ChunkSeed {
            timestamp,
            scope,
            signature,
        },
    )
}

fn chunk_sig(key: &[u8], seed: &ChunkSeed, prev: &str, data: &[u8]) -> String {
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{prev}\n{}",
        seed.timestamp,
        seed.scope,
        hex::encode(Sha256::digest(data))
    );
    hex::encode(hmac_sha256(key, string_to_sign.as_bytes()))
}

/// Encode `data` as a signed aws-chunked frame stream (pieces of `chunk_len`,
/// bare final chunk; the trailing spare CRLF is tolerated server-side).
pub fn aws_chunked_signed_body(
    secret_key: &str,
    seed: &ChunkSeed,
    data: &[u8],
    chunk_len: usize,
) -> Vec<u8> {
    let key = signing_key(secret_key, &seed.timestamp[..8], REGION);
    let mut prev = seed.signature.clone();
    let mut out = Vec::new();
    for piece in data.chunks(chunk_len.max(1)) {
        let sig = chunk_sig(&key, seed, &prev, piece);
        out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", piece.len()).as_bytes());
        out.extend_from_slice(piece);
        out.extend_from_slice(b"\r\n");
        prev = sig;
    }
    let final_sig = chunk_sig(&key, seed, &prev, b"");
    out.extend_from_slice(format!("0;chunk-signature={final_sig}\r\n\r\n").as_bytes());
    out
}

/// Encode `data` as an unsigned trailer-flavored frame stream; the trailing
/// checksum header is sent but not verified by the gateway (checksums are a
/// separate feature; absence of verification is declared behavior).
pub fn aws_chunked_unsigned_trailer_body(data: &[u8], chunk_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for piece in data.chunks(chunk_len.max(1)) {
        out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
        out.extend_from_slice(piece);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\nx-amz-checksum-crc32c:AAAAAA==\r\n\r\n");
    out
}
