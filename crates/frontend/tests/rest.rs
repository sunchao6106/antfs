//! frontend REST 端到端测试:**进程内 1 master + 3 store + 1 frontend,全用真端口**(无 mock)。
//!
//! 夹具刻意**不依赖心跳周期**:节点注册与分区建立直接经 master 的 Raft 提交,副本在 store 上经数据面
//! admin 端点同步创建 —— 全部同步等待,不用 `sleep`(CLAUDE.md §5.6)。
//!
//! 覆盖:上传(3 MiB,证明不被 body 上限挡掉)/ 下载逐字节一致 / HEAD 元数据 / 删除 / 复活 / TTL /
//! 分片拼装 / 边界校验(400 / 413)/ 副本故障降级(2 真 + 1 死仍可写可读)/ `/metrics`。
//! 指标断言打在 frontend **实际接入的那个 `Registry`** 上(CLAUDE.md §6)。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_config::Validate;
use antfs_frontend::config::FrontendConfig;
use antfs_frontend::server::{build_router, AppState};
use antfs_master::config::MasterConfig;
use antfs_master::types::{DiskSpec, MasterCommand, MasterResponse, NodeInfo, ReplicaPlacement};
use antfs_master::MasterNode;
use antfs_router::RouterConfig;
use antfs_store::{StoreServer, StoreServerConfig};
use openraft::BasicNode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "test-secret";
/// 每个数据节点上唯一的盘 id(与放置指令里的 disk_id 一致)。
const DISK: &str = "d0";
/// 默认单请求 body 上限(与 `StoreServerConfig::default` 的同名项对齐,够 3 MiB 走通流式上传)。
const MAX_BODY_BYTES: u64 = 8 << 20;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

fn addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// 起单节点 master(自举 → 等自己当选 leader)。
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
        id_alloc_segment_size: 4,
        id_alloc_raft_batch_multiple: 2,
        // 测试不要失败驱动的再复制(会引入噪声指令),关掉选主宽限。
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

/// 起一个真数据节点(刻意**不接 master**:注册走测试直连 Raft,避免心跳噪声与等待)。
async fn start_store(node_id: u64, dir: &std::path::Path) -> (StoreServer, String) {
    let http_addr = addr(free_port());
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
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
        },
        &registry,
    )
    .expect("start store");
    srv.serve().await.expect("serve store");
    (srv, http_addr)
}

/// 经 Raft 固化数据节点身份(测试自己知道地址)。
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
    }
}

/// 经 Raft 建分区(测试精确控制副本集合),返回 partition id。
async fn create_partition(master: &MasterNode, replicas: Vec<ReplicaPlacement>) -> u32 {
    let rf = u8::try_from(replicas.len()).expect("rf fits u8");
    let resp = master
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: rf,
            replicas,
        })
        .await
        .expect("create partition");
    match resp.data {
        MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected apply response: {other:?}"),
    }
}

/// 在 store 上同步建出该分区的副本(数据面 admin 端点,已存在)。
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

/// 一套进程内集群:1 master + 3 store(+ 可选一个无监听的假副本)+ 1 frontend。
struct Fixture {
    /// 只为活到测试结束(注册/建分区都在 [`Fixture::start`] 里做完)。
    _master: MasterNode,
    _stores: Vec<StoreServer>,
    /// 已建分区的 id(测试造"合法但不存在的 blobId"用)。
    pid: u32,
    /// `node_id → 数据面地址`(测试直连 store 做停机/静默等运维动作)。
    store_addrs: BTreeMap<u64, String>,
    /// frontend 监听地址。
    frontend: String,
    /// frontend 实际接入的注册表(指标断言用)。
    registry: antfs_metric::Registry,
    http: reqwest::Client,
    /// 放在最后:保证数据目录在 master/store 之后才被删。
    _tmp: tempfile::TempDir,
}

impl Fixture {
    /// 起集群并建一个分区:`replica_nodes` 是放置的节点清单,其中**真实存在**的节点会建出本地副本;
    /// `max_body_bytes` 为 frontend 的 body 上限;`cors_origins` 空 = 不挂 CORS 层。
    async fn start(
        max_body_bytes: u64,
        replica_nodes: &[u64],
        cors_origins: Vec<String>,
    ) -> Fixture {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (master, grpc_addr) = solo_master(&tmp.path().join("master")).await;

        // 3 个真数据节点:101/102/103。真副本的地址按 node_id 记下,便于建副本。
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

        // 副本清单里出现、但不在 101..=103 的节点 = 假节点(无监听端口),模拟宕机副本。
        for node_id in replica_nodes {
            if !store_addrs.contains_key(node_id) {
                register_node(&master, *node_id, &addr(free_port())).await;
            }
        }
        let pid = create_partition(
            &master,
            replica_nodes.iter().copied().map(placement).collect(),
        )
        .await;
        for node_id in replica_nodes {
            if let Some(store_addr) = store_addrs.get(node_id) {
                ensure_replica(store_addr, pid).await;
            }
        }

        // ---- frontend(真端口 + 真 SDK)----
        let registry = antfs_metric::Registry::new();
        let frontend_addr = addr(free_port());
        let cfg = FrontendConfig {
            listen_addr: frontend_addr.clone(),
            max_body_bytes,
            cors_allowed_origins: cors_origins,
            router: RouterConfig {
                master_addr: grpc_addr,
                cluster_secret: SECRET.to_string(),
                request_timeout_secs: 10,
                id_segment_size: 4,
            },
        };
        cfg.validate().expect("frontend config");
        let state = AppState::from_config(&cfg, &registry)
            .await
            .expect("frontend state");
        let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
            .await
            .expect("bind frontend");
        let router = build_router(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Fixture {
            _master: master,
            _stores: stores,
            pid,
            store_addrs,
            frontend: frontend_addr,
            registry,
            http: reqwest::Client::new(),
            _tmp: tmp,
        }
    }

    /// 常规集群:3 个真数据节点,分区放 **2 个副本**(RF=2 → `W = 2/2+1 = 2` = 该分区全部副本)。
    ///
    /// RF 取 2 而非 3 是**刻意的**:RF=3 时 W=2 < 3,`collect_majority` 收满 2 个 ack 即返回并取消第三个
    /// 请求 —— 该副本可能根本没拿到这次变更,紧接着的 R=1 读就有概率读到旧态
    /// (cluster.md §10 明说的"变更未追上的副本短暂可能返回旧态"最终一致窗口)。
    /// 读后写类的**确定性**断言必须让"多数派 = 全部副本";跨副本的降级场景见
    /// [`write_and_read_survive_a_dead_replica`](它只断言掉一个副本时仍写得进、读得到,不依赖读后写可见性)。
    async fn quorum_cluster() -> Fixture {
        Fixture::start(MAX_BODY_BYTES, &[101, 102], Vec::new()).await
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.frontend)
    }

    /// 该分区内一个**合法但从未写过**的 blobId(用于"区分 404 与 400"的断言)。
    fn missing_blob_id(&self) -> String {
        format!("{:08x}{:08x}", self.pid, 0xdead_beef_u32)
    }

    /// 上传一个对象,断言 201 与三个头,返回 blobId。
    async fn upload(&self, data: &[u8]) -> String {
        let resp = self
            .http
            .post(self.url("/blobs"))
            .body(data.to_vec())
            .send()
            .await
            .expect("upload request");
        assert_eq!(resp.status().as_u16(), 201, "上传必须返回 201");
        let blob_id = header(&resp, "x-ambry-blob-id");
        assert_eq!(
            header(&resp, "location"),
            format!("/blobs/{blob_id}"),
            "Location 必须指向新对象"
        );
        assert_eq!(
            header(&resp, "x-ambry-blob-size"),
            data.len().to_string(),
            "x-ambry-blob-size 必须是写入字节数"
        );
        blob_id
    }

    /// GET 一个对象。
    async fn get(&self, blob_id: &str) -> reqwest::Response {
        self.http
            .get(self.url(&format!("/blobs/{blob_id}")))
            .send()
            .await
            .expect("get request")
    }

    /// 下载并断言 200,返回数据体。
    async fn get_ok(&self, blob_id: &str) -> Vec<u8> {
        let resp = self.get(blob_id).await;
        assert_eq!(resp.status().as_u16(), 200, "下载必须返回 200");
        resp.bytes().await.expect("read body").to_vec()
    }
}

fn header(resp: &reqwest::Response, name: &str) -> String {
    resp.headers()
        .get(name)
        .unwrap_or_else(|| panic!("响应缺少头 {name}"))
        .to_str()
        .expect("header is utf8")
        .to_string()
}

/// 确定性伪随机数据(不引 `rand`:只要"内容不重复且可逐字节校验")。
fn pseudo_random(len: usize) -> Vec<u8> {
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

// ── 1 / 2:上传 + 下载 + HEAD ───────────────────────────────────────────────────────────────

/// 3 MiB 上传(超过 body 默认上限 2 MiB 的大对象)往返必须逐字节一致 —— 证明流式上传没被上限挡掉,
/// 且大对象走的是"落暂存 + 流式回"的路径。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_and_downloads_three_mib_roundtrip() {
    let fx = Fixture::quorum_cluster().await;
    let data = pseudo_random(3 * 1024 * 1024);
    let blob_id = fx.upload(&data).await;
    assert_eq!(blob_id.len(), 16, "blobId 必须是 16 hex");
    assert_eq!(fx.get_ok(&blob_id).await, data, "下载必须逐字节一致");
}

/// HEAD 只取元数据:200 + 无 body + `x-ambry-blob-size` 正确。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn head_reports_size_without_body() {
    let fx = Fixture::quorum_cluster().await;
    let data = pseudo_random(4096);
    let blob_id = fx.upload(&data).await;

    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{blob_id}")))
        .send()
        .await
        .expect("head request");
    assert_eq!(resp.status().as_u16(), 200, "HEAD 必须 200");
    assert_eq!(header(&resp, "x-ambry-blob-size"), "4096");
    assert_eq!(header(&resp, "x-ambry-blob-deleted"), "false");
    assert_eq!(header(&resp, "x-ambry-blob-expired"), "false");
    assert_eq!(header(&resp, "x-ambry-blob-expiration-ms"), "-1");
    let body = resp.bytes().await.expect("head body");
    assert!(body.is_empty(), "HEAD 不得带 body");
}

// ── 3:删除 ────────────────────────────────────────────────────────────────────────────────

/// 删除**不可逆**:DELETE → 204 → GET 404 → HEAD 仍 200 且 `deleted: true`;
/// 复活端点已不存在(404);重复删**幂等**(再回 204,墓碑是终态)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_is_irreversible() {
    let fx = Fixture::quorum_cluster().await;
    let data = pencil(100 * 1024);
    let blob_id = fx.upload(&data).await;

    let resp = fx
        .http
        .delete(fx.url(&format!("/blobs/{blob_id}")))
        .send()
        .await
        .expect("delete request");
    assert_eq!(resp.status().as_u16(), 204, "删除必须 204");

    assert_eq!(
        fx.get(&blob_id).await.status().as_u16(),
        404,
        "删除后 GET 必须 404"
    );

    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{blob_id}")))
        .send()
        .await
        .expect("head after delete");
    assert_eq!(resp.status().as_u16(), 200, "墓碑的元数据仍可读(HEAD 200)");
    assert_eq!(header(&resp, "x-ambry-blob-deleted"), "true");

    // 复活端点已删掉:路由不存在 → 404(而不是"以为能复活")。
    let resp = fx
        .http
        .post(fx.url(&format!("/blobs/{blob_id}/undelete")))
        .send()
        .await
        .expect("undelete request");
    assert_eq!(resp.status().as_u16(), 404, "undelete 端点必须不存在");

    // 重复删幂等:副本对墓碑 key 不再写第二条墓碑、回 204 → 多数派达成 → 204。
    let resp = fx
        .http
        .delete(fx.url(&format!("/blobs/{blob_id}")))
        .send()
        .await
        .expect("delete again");
    assert_eq!(resp.status().as_u16(), 204, "重复删必须幂等 204");

    // 数据仍然取不回来(没有 include_deleted 这条路)。
    assert_eq!(fx.get(&blob_id).await.status().as_u16(), 404);
}

/// 一个可读的小负载(不是拼装描述符:开头不是 `ASTC`)。
fn pencil(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

// ── 4:TTL ────────────────────────────────────────────────────────────────────────────────

/// TTL 只在**上传时**定(`x-ambry-ttl-ms` → `PUT ?expiration_ms=`,一次多数派写):
/// 已过去的时刻 → 201 且随即 404 / HEAD `expired: true`;非法值 → 400;
/// "改期"端点不存在(对象不可变)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_is_set_at_upload_time_only() {
    let fx = Fixture::quorum_cluster().await;

    // 1970 年的过期时刻 = 早已过期。
    let resp = fx
        .http
        .post(fx.url("/blobs"))
        .header("x-ambry-ttl-ms", "1000")
        .body(pencil(512))
        .send()
        .await
        .expect("upload with ttl");
    assert_eq!(resp.status().as_u16(), 201, "带 TTL 的上传必须 201");
    let blob_id = header(&resp, "x-ambry-blob-id");
    assert_eq!(
        fx.get(&blob_id).await.status().as_u16(),
        404,
        "带 TTL 上传的对象应已过期"
    );
    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{blob_id}")))
        .send()
        .await
        .expect("head expired");
    assert_eq!(resp.status().as_u16(), 200, "过期对象的元数据仍可读");
    assert_eq!(header(&resp, "x-ambry-blob-expired"), "true");
    assert_eq!(header(&resp, "x-ambry-blob-expiration-ms"), "1000");

    // 非法 TTL → 400(不静默当成"不过期")。
    let resp = fx
        .http
        .post(fx.url("/blobs"))
        .header("x-ambry-ttl-ms", "not-a-number")
        .body(pencil(8))
        .send()
        .await
        .expect("bad ttl");
    assert_eq!(resp.status().as_u16(), 400, "非法 x-ambry-ttl-ms 必须 400");

    // 改 TTL 的端点不存在(带不带参数都不该有这条路)。
    let live = fx.upload(&pencil(64)).await;
    for path in [
        format!("/blobs/{live}/ttl"),
        format!("/blobs/{live}/ttl?expiration_ms=1000"),
    ] {
        let resp = fx
            .http
            .post(fx.url(&path))
            .send()
            .await
            .expect("ttl endpoint");
        assert_eq!(
            resp.status().as_u16(),
            404,
            "TTL 更新端点必须不存在: {path}"
        );
    }
    // 未带 TTL 上传的对象是永久的。
    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{live}")))
        .send()
        .await
        .expect("head live");
    assert_eq!(header(&resp, "x-ambry-blob-expiration-ms"), "-1");
}

// ── 5:分片拼装 ────────────────────────────────────────────────────────────────────────────

/// 把一段数据切成 3 片分别上传(同一 session),`POST /stitch` 提交片 id → 201;
/// `GET` 拼装结果等于原始数据。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stitch_concatenates_chunks_in_order() {
    let fx = Fixture::quorum_cluster().await;
    let session = "session-stitch-test";
    let parts: Vec<Vec<u8>> = vec![
        pseudo_random(100 * 1024),
        pencil(70 * 1024),
        pseudo_random(33),
    ];
    let mut chunk_ids = Vec::new();
    for part in &parts {
        let resp = fx
            .http
            .post(fx.url("/blobs"))
            .header("x-ambry-session", session)
            .body(part.clone())
            .send()
            .await
            .expect("upload chunk");
        assert_eq!(resp.status().as_u16(), 201, "片就是普通 blob");
        chunk_ids.push(header(&resp, "x-ambry-blob-id"));
    }

    let body = serde_json::json!({ "signedChunkIds": chunk_ids });
    let resp = fx
        .http
        .post(fx.url("/stitch"))
        .header("x-ambry-session", session)
        .json(&body)
        .send()
        .await
        .expect("stitch request");
    assert_eq!(resp.status().as_u16(), 201, "拼装必须 201");
    let stitched = header(&resp, "x-ambry-blob-id");
    assert_eq!(
        header(&resp, "location"),
        format!("/blobs/{stitched}"),
        "Location 指向拼装结果"
    );

    let expected: Vec<u8> = parts.concat();
    assert_eq!(
        fx.get_ok(&stitched).await,
        expected,
        "拼装结果必须等于原始数据"
    );

    // HEAD 必须与 GET 一致:报**逻辑大小**(各片之和)+ 标记为拼装对象。
    // (曾报描述符自身那几百字节,导致 "HEAD 785 / GET 3145728" 的错配 —— 已修。)
    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{stitched}")))
        .send()
        .await
        .expect("head stitched");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        header(&resp, "x-ambry-blob-size")
            .parse::<u64>()
            .expect("number"),
        expected.len() as u64,
        "HEAD 的 size 必须是逻辑大小,与 GET 返回的字节数一致"
    );
    assert_eq!(header(&resp, "x-ambry-blob-stitched"), "true");
    assert_eq!(header(&resp, "x-ambry-blob-stitch-chunks"), "3", "三片");

    // 普通对象不受影响:HEAD 报自身大小,且不带 stitched 标记。
    let plain = fx.upload(&pseudo_random(4096)).await;
    let resp = fx
        .http
        .head(fx.url(&format!("/blobs/{plain}")))
        .send()
        .await
        .expect("head plain");
    assert_eq!(
        header(&resp, "x-ambry-blob-size")
            .parse::<u64>()
            .expect("number"),
        4096
    );
    assert!(
        resp.headers().get("x-ambry-blob-stitched").is_none(),
        "普通对象不带 stitched"
    );
}

// ── 6:边界校验 ────────────────────────────────────────────────────────────────────────────

/// 非法 blobId(非 hex / 不是 8 字节)、`/stitch` 缺 session / 空片 / 非法片 id
/// 一律 400,且 blobId 文案稳定。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_requests_are_rejected_with_400() {
    let fx = Fixture::quorum_cluster().await;

    for bad in ["zz", "0f", "000000010000000200", "0000000100000002ff"] {
        let path = format!("/blobs/{bad}");
        let resp = fx.http.get(fx.url(&path)).send().await.expect("request");
        assert_eq!(resp.status().as_u16(), 400, "非法 blobId {bad} 必须 400");
        let text = resp.text().await.expect("body");
        assert!(
            text.contains("blob id must be 8 bytes (16 hex)"),
            "文案必须稳定,实际: {text}"
        );
    }
    // 合法但不存在:400 之外应是 404(证明 400 与 404 没混)。
    let missing_id = fx.missing_blob_id();
    let missing = fx.get(&missing_id).await;
    assert_eq!(missing.status().as_u16(), 404, "合法 id 找不到必须 404");
    assert_eq!(
        fx.http
            .head(fx.url(&format!("/blobs/{missing_id}")))
            .send()
            .await
            .expect("head missing")
            .status()
            .as_u16(),
        404,
        "HEAD 找不到也必须 404"
    );

    // /stitch:缺 session。
    let resp = fx
        .http
        .post(fx.url("/stitch"))
        .json(&serde_json::json!({ "signedChunkIds": [missing_id] }))
        .send()
        .await
        .expect("stitch request");
    assert_eq!(resp.status().as_u16(), 400, "缺 x-ambry-session 必须 400");

    // /stitch:空片列表(含字段缺失)。
    for body in [
        serde_json::json!({ "signedChunkIds": [] }),
        serde_json::json!({}),
    ] {
        let resp = fx
            .http
            .post(fx.url("/stitch"))
            .header("x-ambry-session", "s")
            .json(&body)
            .send()
            .await
            .expect("stitch request");
        assert_eq!(resp.status().as_u16(), 400, "空片列表必须 400: {body}");
    }

    // /stitch:非法片 id。
    let resp = fx
        .http
        .post(fx.url("/stitch"))
        .header("x-ambry-session", "s")
        .json(&serde_json::json!({ "signedChunkIds": ["zz"] }))
        .send()
        .await
        .expect("stitch request");
    assert_eq!(resp.status().as_u16(), 400, "非法片 id 必须 400");

    // /stitch:片可读但不存在 → 400(文案说明是哪个片)。
    let resp = fx
        .http
        .post(fx.url("/stitch"))
        .header("x-ambry-session", "s")
        .json(&serde_json::json!({ "signedChunkIds": [missing_id] }))
        .send()
        .await
        .expect("stitch request");
    assert_eq!(resp.status().as_u16(), 400, "片不可读必须 400");
    let text = resp.text().await.expect("body");
    assert!(
        text.contains(&missing_id) && text.contains("not readable"),
        "报错必须点明哪个片不可读,实际: {text}"
    );
}

// ── 7:body 上限(413)──────────────────────────────────────────────────────────────────────

/// 超过 `frontend.max_body_bytes` 的 body → 413:
/// ① 有 `Content-Length` 时在读 body **之前**就拒(裸 socket 只发头,判定与客户端是否继续发体无关);
/// ② chunked(无 `Content-Length`)时边收边计数,超限即停。
/// 同时验证**恰好等于上限**能正常上传(边界不加不减)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_limit_is_enforced_with_413() {
    const LIMIT: u64 = 1 << 20;
    let fx = Fixture::start(LIMIT, &[101, 102], Vec::new()).await;

    // ① 声明长度超限:server 不读 body 就回 413。
    assert_eq!(
        declared_length_status(&fx.frontend, LIMIT + 1).await,
        413,
        "Content-Length 超限必须 413"
    );
    // ② chunked 超限:边收边计数,同样 413。
    assert_eq!(
        chunked_over_limit_status(&fx.frontend, LIMIT).await,
        413,
        "chunked 超限必须 413"
    );
    // ③ 恰好等于上限:正常写入。
    let data = pseudo_random(LIMIT as usize);
    let blob_id = fx.upload(&data).await;
    assert_eq!(
        fx.get_ok(&blob_id).await,
        data,
        "上限内的上传必须成功且一致"
    );
}

/// 直连 frontend,只发请求头(声明超大 `Content-Length`)、不写 body,返回响应状态码。
///
/// 用裸 socket 而不是 HTTP 客户端:413 的判定发生在读 body **之前**,HTTP 客户端可能在"服务端早退
/// 不读 body"时因写失败而报错,拿不到响应。
async fn declared_length_status(frontend_addr: &str, declared: u64) -> u16 {
    let mut sock = tokio::net::TcpStream::connect(frontend_addr)
        .await
        .expect("connect frontend");
    sock.write_all(
        format!(
            "POST /blobs HTTP/1.1\r\nHost: {frontend_addr}\r\nContent-Length: {declared}\r\n\
             Connection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .expect("write request head");
    read_status(&mut sock).await
}

/// 用 chunked(body 无 `Content-Length`)发到超过上限,返回响应状态码。
async fn chunked_over_limit_status(frontend_addr: &str, limit: u64) -> u16 {
    let mut sock = tokio::net::TcpStream::connect(frontend_addr)
        .await
        .expect("connect frontend");
    sock.write_all(
        format!(
            "POST /blobs HTTP/1.1\r\nHost: {frontend_addr}\r\nTransfer-Encoding: chunked\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .expect("write request head");
    let piece = vec![0u8; 64 * 1024];
    let mut sent = 0u64;
    while sent <= limit {
        sock.write_all(format!("{:x}\r\n", piece.len()).as_bytes())
            .await
            .expect("write chunk header");
        sock.write_all(&piece).await.expect("write chunk");
        sock.write_all(b"\r\n").await.expect("write chunk tail");
        sent += piece.len() as u64;
    }
    read_status(&mut sock).await
}

/// 读一次响应并解析状态行(响应很小,一次读就能拿到头;不必等连接关闭)。
async fn read_status(sock: &mut tokio::net::TcpStream) -> u16 {
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf))
        .await
        .expect("read response within timeout")
        .expect("read response");
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    let line = text.lines().next().unwrap_or_default().to_string();
    line.split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no HTTP status in {line:?}"))
}

// ── 8:副本故障降级 ───────────────────────────────────────────────────────────────────────

/// 3 副本里有 1 个是**无监听的假节点**:W = 多数派 = 2 仍能写成功(201),R=1 仍能读到。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_and_read_survive_a_dead_replica() {
    // 104 不在真节点清单里 → 注册成无监听地址,只作为"宕机副本"参与放置。
    let fx = Fixture::start(MAX_BODY_BYTES, &[101, 102, 104], Vec::new()).await;
    let data = pseudo_random(64 * 1024);
    let blob_id = fx.upload(&data).await;
    assert_eq!(fx.get_ok(&blob_id).await, data, "掉一个副本仍必须可写可读");
}

// ── 9:指标 ───────────────────────────────────────────────────────────────────────────────

/// `/metrics` 是 Prometheus 文本,且 frontend 的计数器真被计入(断言打在 frontend 实际接入的
/// `Registry` 上,CLAUDE.md §6)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_exposes_counters() {
    let fx = Fixture::quorum_cluster().await;
    let data = pseudo_random(2048);
    let blob_id = fx.upload(&data).await;
    assert_eq!(fx.get_ok(&blob_id).await, data);

    let resp = fx
        .http
        .get(fx.url("/metrics"))
        .send()
        .await
        .expect("metrics request");
    assert_eq!(resp.status().as_u16(), 200);
    let text = resp.text().await.expect("metrics text");
    assert!(
        text.contains("frontend_upload_total{outcome=\"ok\"} 1"),
        "/metrics 必须导出上传计数:\n{text}"
    );
    assert!(
        text.contains("frontend_download_bytes_total 2048"),
        "下载字节数:\n{text}"
    );
    assert!(
        text.contains("frontend_request_duration_seconds_count{op=\"upload\"} 1"),
        "每个 op 都要有延迟观测:\n{text}"
    );

    // 同一个 Registry 直接 gather 也应一致(frontend 接入的就是它)。
    let dumped = antfs_metric::gather(&fx.registry).expect("gather frontend registry");
    assert!(dumped.contains("frontend_upload_total"), "{dumped}");
    assert!(dumped.contains("frontend_in_flight_requests"), "{dumped}");
}

// ── 10:CORS(白名单才挂)────────────────────────────────────────────────────────────────────

/// `frontend.cors_allowed_origins` 非空才挂 CORS 层,且只放行**白名单内**的来源(不用 permissive)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cors_allows_listed_origins_only() {
    const ALLOWED: &str = "https://app.example";
    let fx = Fixture::start(MAX_BODY_BYTES, &[101, 102], vec![ALLOWED.to_string()]).await;

    // 白名单内:回 `access-control-allow-origin`,并暴露 `x-ambry-*` 响应头与 `location`
    // (暴露头只在**非预检**响应上出现)。
    let resp = fx
        .http
        .get(fx.url("/metrics"))
        .header("origin", ALLOWED)
        .send()
        .await
        .expect("cors request");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some(ALLOWED)
    );
    let exposed = resp
        .headers()
        .get("access-control-expose-headers")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        exposed.contains("x-ambry-blob-id") && exposed.contains("location"),
        "必须暴露 x-ambry-* 与 location: {exposed}"
    );

    // 白名单外:**不**放行(没有该头),也绝不回 `*`。
    let resp = fx
        .http
        .get(fx.url("/metrics"))
        .header("origin", "https://evil.example")
        .send()
        .await
        .expect("cors request");
    assert!(
        resp.headers().get("access-control-allow-origin").is_none(),
        "白名单外的来源不得放行"
    );

    // 预检:暴露方法 + 暴露头都要有(浏览器据此决定能否读 `x-ambry-*`)。
    let resp = fx
        .http
        .request(reqwest::Method::OPTIONS, fx.url("/blobs"))
        .header("origin", ALLOWED)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "x-ambry-ttl-ms")
        .send()
        .await
        .expect("preflight request");
    assert_eq!(resp.status().as_u16(), 200, "预检必须 200");
    let methods = resp
        .headers()
        .get("access-control-allow-methods")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        methods.contains("POST") && methods.contains("DELETE"),
        "{methods}"
    );
    assert!(
        resp.headers()
            .get("access-control-allow-headers")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .contains("x-ambry-session"),
        "允许的请求头必须含 x-ambry-session"
    );
}

// ── 11:MAGIC 嗅探的已知取舍(钉住行为)───────────────────────────────────────────────────
/// 契约的已知取舍:`GET` 只按"前 4 字节 == `ASTC`"判定拼装描述符,故一个**恰好以 `ASTC` 开头**的
/// 普通小 blob 会被当作描述符解析,解析失败即 **502**(而不是静默返回原字节)。
/// 这里把该行为钉住 —— 它是有意为之(MAGIC 嗅探),改动必须同步改契约与文档。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_blob_starting_with_magic_is_reported_as_broken_stitch() {
    let fx = Fixture::quorum_cluster().await;
    let mut data = b"ASTC".to_vec();
    data.extend_from_slice(b"ordinary bytes that are not a descriptor");
    let blob_id = fx.upload(&data).await;
    let resp = fx.get(&blob_id).await;
    assert_eq!(
        resp.status().as_u16(),
        502,
        "MAGIC 命中但结构不合法 → 上游数据不一致"
    );
}

// ── 12:请求静默的透传(503 + Retry-After)───────────────────────────────────────────────

/// 副本被**请求静默**(store 的 `/admin/request-control`)时,frontend 必须原样透出 503 并把剩余时长
/// 透传成 `Retry-After`(对齐契约:静默是可运维动作,不是故障,别吞成 500)。
///
/// 用 RF=1 是刻意的:读路径只有一个副本,它的静默就是这次读的结论;RF>1 时 SDK 会把"未达多数派"
/// 收敛成 `Quorum`,重试提示无从取出(那是 SDK 的取舍,不是本层的)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silenced_replica_is_passed_through_with_retry_after() {
    let fx = Fixture::start(MAX_BODY_BYTES, &[101], Vec::new()).await;
    let blob_id = fx.upload(&pencil(256)).await;
    assert_eq!(
        fx.get(&blob_id).await.status().as_u16(),
        200,
        "静默前必须可读"
    );

    let store = fx.store_addrs.get(&101).expect("store 101 已启动").clone();
    let resp = fx
        .http
        .post(format!("http://{store}/admin/request-control"))
        .json(&serde_json::json!({
            "op": "get",
            "partition_id": fx.pid,
            "enable": true,
            "ttl_secs": 600,
        }))
        .send()
        .await
        .expect("silence request");
    assert_eq!(resp.status().as_u16(), 200, "静默指令必须被 store 接受");

    let resp = fx.get(&blob_id).await;
    assert_eq!(resp.status().as_u16(), 503, "副本被静默时读必须 503");
    let retry: u64 = header(&resp, "retry-after")
        .parse()
        .expect("Retry-After 必须是秒数");
    assert!(
        (590..=600).contains(&retry),
        "剩余静默时长应≈600 秒并透传(`Retry-After`),实际 {retry}"
    );
}
