//! Virtual-host style routing(`s3-vhost-routing`):`Host = {bucket}.{suffix}` 的请求
//! 按等价路径风格请求路由;签名验证发生在改写之前(外层 auth 先跑),vhost 风格签名
//! (路径不含桶、Host 含桶)天然成立。
//!
//! 本二进制设 `TEST_S3_VHOST_DOMAIN` 起夹具;**其余 e2e 二进制不设(默认空 = 关),
//! 它们整体就是「关态行为零变化」的回归**。All tests are `#[ignore]`d — they need
//! a live database via `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL`(同其余 e2e)。

mod common;

use common::{signed_headers_for, S3Fixture};

const SUFFIX: &str = "s3.example.test";

fn vhost_host(bucket: &str) -> String {
    format!("{bucket}.{SUFFIX}")
}

/// vhost 形态的 **authority**(host:port)—— reqwest/hyper 对显式 Host 头仍会带上
/// URL 的端口;SigV4 的 Host 是逐字匹配,签名口径必须与线上字节一致(诊断双探针
/// 实证:portless/portful 各签各发均过,reqwest 出线为 portful)。vhost 匹配剥端
/// 口、两种形态皆通,故仅签名口径需对齐。
fn vhost_authority(fx: &S3Fixture, bucket: &str) -> String {
    let port = fx
        .s3_addr
        .rsplit_once(':')
        .map(|(_, p)| p.to_string())
        .unwrap_or_else(|| "80".to_string());
    format!("{}:{port}", vhost_host(bucket))
}

/// vhost 风格的签名请求(默认凭证):签名 host = vhost authority(host:port,与出线一致)、uri = **原路径**
/// (验签在改写前,canonical 与客户端视角一致)。发往夹具地址但覆盖 Host 头。
async fn vhost_send(
    fx: &S3Fixture,
    method: &str,
    bucket: &str,
    uri: &str,
    body: &[u8],
) -> reqwest::Response {
    let host = vhost_authority(fx, bucket);
    let headers = signed_headers_for(&host, &fx.access_key, &fx.secret_key, method, uri, body);
    let url = format!("http://{}{uri}", fx.s3_addr);
    let m: reqwest::Method = method.parse().expect("valid method");
    fx.http
        .request(m, &url)
        .headers(headers)
        .body(body.to_vec())
        .send()
        .await
        .expect("vhost request send")
}

/// vhost 全语义:对象读写 head/列举同路径风格;404/403 族照旧;私有/公开(canned
/// ACL)与匿名/跨租户正交组合;Host 带端口;vhost 建桶;**双风格互证**(vhost 写·
/// 路径读;路径写·vhost 读)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn virtual_host_style_routes_like_path_style() {
    std::env::set_var("TEST_S3_VHOST_DOMAIN", SUFFIX);
    let fx = S3Fixture::start().await;
    // env 只在 start() 起夹具时读一次;本二进制即 vhost 专用,无需清除。

    let bucket = fx.create_unique_bucket("vh").await;
    let body = vec![0xA5u8; 128];

    // 对象写读改 vhost(签名 host/原路径):PUT/GET/HEAD roundtrip。
    let resp = vhost_send(&fx, "PUT", &bucket, "/vh-k.bin", &body).await;
    let status = resp.status().as_u16();
    if status != 200 {
        let text = resp.text().await.unwrap_or_default();
        panic!("vhost PUT status {status}:\n{text}");
    }
    let resp = vhost_send(&fx, "GET", &bucket, "/vh-k.bin", &[]).await;
    assert_eq!(resp.status().as_u16(), 200, "vhost GET");
    assert_eq!(resp.bytes().await.unwrap().to_vec(), body, "vhost GET body");
    let resp = vhost_send(&fx, "HEAD", &bucket, "/vh-k.bin", &[]).await;
    assert_eq!(resp.status().as_u16(), 200, "vhost HEAD");

    // vhost 未知 key → 404(同路径风格)。
    let resp = vhost_send(&fx, "GET", &bucket, "/no-such.bin", &[]).await;
    assert_eq!(resp.status().as_u16(), 404, "vhost GET missing key");

    // 桶级列举:根路径 + ListV2(根去尾斜杠 + query 原样重挂)。
    let resp = vhost_send(&fx, "GET", &bucket, "/?list-type=2", &[]).await;
    assert_eq!(resp.status().as_u16(), 200, "vhost ListObjectsV2");
    let xml = resp.text().await.unwrap();
    assert!(xml.contains("<Key>vh-k.bin</Key>"), "vhost listing: {xml}");

    // vhost 未知桶 → 404 NoSuchBucket。
    let resp = vhost_send(&fx, "GET", "no-such-vhost-bucket", "/k", &[]).await;
    assert_eq!(resp.status().as_u16(), 404, "vhost unknown bucket");

    // 私有桶:匿名(无凭证)vhost → 403;跨租户签名 vhost → 403(ACL 语义随路由继承)。
    let resp = fx
        .http
        .get(format!("http://{}/vh-k.bin", fx.s3_addr))
        .header("host", vhost_host(&bucket))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        403,
        "anonymous vhost GET on private bucket"
    );
    let (bob_ak, bob_sk) = fx.issue_credential("bob").await;
    let host = vhost_authority(&fx, &bucket);
    let hdrs = signed_headers_for(&host, &bob_ak, &bob_sk, "GET", "/vh-k.bin", &[]);
    let resp = fx
        .http
        .get(format!("http://{}/vh-k.bin", fx.s3_addr))
        .headers(hdrs)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        403,
        "cross-tenant vhost GET on private bucket"
    );

    // 桶级公开:canned ACL 走 vhost 语义(根路径 `?acl`;x-amz-acl 不在签名集,
    // SigV4 只验已列头)→ 匿名 vhost GET 变 200 —— 证明 vhost 与 ACL 正交组合。
    let host = vhost_authority(&fx, &bucket);
    let mut hdrs = signed_headers_for(&host, &fx.access_key, &fx.secret_key, "PUT", "/?acl", &[]);
    hdrs.insert("x-amz-acl", "public-read".parse().unwrap());
    let resp = fx
        .http
        .put(format!("http://{}/?acl", fx.s3_addr))
        .headers(hdrs)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "vhost PutBucketAcl public-read"
    );
    let resp = fx
        .http
        .get(format!("http://{}/vh-k.bin", fx.s3_addr))
        .header("host", vhost_host(&bucket))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "anonymous vhost GET on public-read bucket"
    );

    // Host 带端口:签名 host 与实发 Host 同串(记端口),剥离后匹配如常。
    let port = fx.s3_addr.rsplit_once(':').map(|(_, p)| p).unwrap_or("");
    let host_with_port = format!("{}:{port}", vhost_host(&bucket));
    let hdrs = signed_headers_for(
        &host_with_port,
        &fx.access_key,
        &fx.secret_key,
        "GET",
        "/vh-k.bin",
        &[],
    );
    let resp = fx
        .http
        .get(format!("http://{}/vh-k.bin", fx.s3_addr))
        .headers(hdrs)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "vhost with port in Host");

    // vhost 建桶(根路径 PUT = CreateBucket):创建即按凭证 owner 认领。
    let bucket2 = fx.unique("vhost-create");
    let resp = vhost_send(&fx, "PUT", &bucket2, "/", &[]).await;
    assert_eq!(resp.status().as_u16(), 200, "vhost bucket create at root");

    // 双风格互证(同一集群、同一桶语义):路径风格 写 → vhost 读。
    let resp = fx.put_object(&bucket2, "half.bin", b"path-style put").await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "path-style PUT into vhost bucket"
    );
    let resp = vhost_send(&fx, "GET", &bucket2, "/half.bin", &[]).await;
    assert_eq!(resp.status().as_u16(), 200, "path-style-put XOR vhost-read");
    assert_eq!(
        resp.bytes().await.unwrap(),
        &b"path-style put"[..],
        "dual-style body"
    );
    // vhost 写 → 路径风格 读(vh-k.bin 最初由 vhost PUT)。
    let resp = fx.get_object(&bucket, "vh-k.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "vhost-put XOR path-style-read");
    assert_eq!(resp.bytes().await.unwrap().to_vec(), body);
}

/// 免鉴权探针:路径风格开公开(canned ACL)→ 匿名 vhost GET(裸 Host 覆盖、零签名)。
/// 三态切因:200 = Host 覆盖生效 ∧ 改写命中;404 NoSuchBucket = 覆盖/改写未生效
/// (请求落回路径风格 `/pub.bin` 单段桶路由);403 = 覆盖与改写都通但公开未生效。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn anonymous_vhost_get_on_public_read_bucket() {
    std::env::set_var("TEST_S3_VHOST_DOMAIN", SUFFIX);
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("vh-probe").await;
    fx.put_object(&bucket, "pub.bin", b"pub data").await;

    let mut headers = fx.signed_headers("PUT", &format!("/{bucket}?acl"), b"");
    headers.insert("x-amz-acl", "public-read".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "path-style PutBucketAcl");

    let resp = fx
        .http
        .get(format!("http://{}/pub.bin", fx.s3_addr))
        .header("host", vhost_host(&bucket))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    if status != 200 {
        panic!("anonymous vhost probe status {status}:\n{text}");
    }
}

/// 裸 TCP 探针(绕开 reqwest,字节级控制 Host 头):匿名 GET 公开桶的 vhost 形态。
/// 切分因:reqwest 的 Host 覆盖被吞掉(客户端因)vs 服务端改写链路断(服务端因)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn raw_tcp_vhost_probe() {
    use std::io::{Read, Write};

    std::env::set_var("TEST_S3_VHOST_DOMAIN", SUFFIX);
    let fx = S3Fixture::start().await;
    // 变因一:夹具是否真的拿到后缀(env → config)。
    assert_eq!(
        fx.state.config.virtual_host_domain, SUFFIX,
        "fixture must carry the vhost suffix"
    );

    let bucket = fx.create_unique_bucket("vh-raw").await;
    fx.put_object(&bucket, "pub.bin", b"raw probe").await;
    let mut headers = fx.signed_headers("PUT", &format!("/{bucket}?acl"), b"");
    headers.insert("x-amz-acl", "public-read".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "path-style PutBucketAcl");

    // 裸 HTTP/1.1:Host = vhost 形态,匿名(无签名)。
    let req = format!(
        "GET /pub.bin HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
        vhost_host(&bucket)
    );
    let mut stream = std::net::TcpStream::connect(&fx.s3_addr).expect("tcp connect");
    stream.write_all(req.as_bytes()).expect("write raw request");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read raw response");
    assert!(
        raw.starts_with("HTTP/1.1 200"),
        "raw vhost GET must be 200 (public read, anonymous):\n=====RAW=====\n{raw}\n=====/RAW====="
    );
    assert!(raw.contains("raw probe"), "raw body: {raw}");

    // 签名口径双探针(字节级):portless / portful Host 各签各发 —— 切分「SigV4
    // vhost-host canonical 正确性」vs「reqwest 对 Host 头的改写(加端口?)」。
    // 诊断段:打印两态首行,不断言(由人判读后再定 vhost_send 的签名口径)。
    let probe_port = fx
        .s3_addr
        .rsplit_once(':')
        .map(|(_, p)| p.to_string())
        .unwrap_or_else(|| "80".to_string());
    for host in [
        vhost_host(&bucket),
        format!("{}:{}", vhost_host(&bucket), probe_port),
    ] {
        let hdrs = signed_headers_for(
            &host,
            &fx.access_key,
            &fx.secret_key,
            "GET",
            "/pub.bin",
            &[],
        );
        let mut signed_req = String::from("GET /pub.bin HTTP/1.1\r\n");
        for (name, value) in hdrs.iter() {
            signed_req.push_str(&format!(
                "{}: {}\r\n",
                name.as_str(),
                value.to_str().unwrap_or("")
            ));
        }
        signed_req.push_str("Connection: close\r\n\r\n");
        let mut stream = std::net::TcpStream::connect(&fx.s3_addr).expect("tcp connect");
        stream
            .write_all(signed_req.as_bytes())
            .expect("write signed raw");
        let mut raw_out = String::new();
        stream
            .read_to_string(&mut raw_out)
            .expect("read signed raw");
        assert!(
            raw_out.starts_with("HTTP/1.1 200"),
            "signed raw vhost GET (host={host}) must verify: {raw_out}"
        );
        println!(
            "SIGNED-RAW host={host} status_line={}",
            raw_out.lines().next().unwrap_or("?")
        );
    }
}
