//! ACL XML 授予面(`s3-acl-xml`):桶/对象两域的 AccessControlPolicy body 形态、
//! 与策略/兜底的评估序(授予只抬兜底,压不赢显式 Deny)、canned 头路径零回归。
//!
//! All tests are `#[ignore]`d — they need a live database via
//! `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL`.

mod common;

use common::{signed_headers_for, xml_tag, S3Fixture};
use reqwest::Method;

async fn tenant_connector(fx: &S3Fixture, owner: &str) -> (String, String) {
    let resp = fx
        .http
        .post(fx.url("/admin/credentials"))
        .json(&serde_json::json!({ "owner": owner }))
        .send()
        .await
        .expect("credential");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    (
        body["access_key"].as_str().expect("ak").to_string(),
        body["secret_key"].as_str().expect("sk").to_string(),
    )
}

fn acl_xml(owner: &str, grants: &[(Option<&str>, &str)]) -> String {
    let mut xml = format!(
        "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>{owner}</ID></Owner><AccessControlList>"
    );
    for (id, perm) in grants {
        match id {
            None => xml.push_str(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
                 xsi:type=\"Group\">\
                 <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>",
            ),
            Some(id) => xml.push_str(&format!(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
                 xsi:type=\"CanonicalUser\"><ID>{id}</ID></Grantee>"
            )),
        }
        xml.push_str(&format!("<Permission>{perm}</Permission></Grant>"));
    }
    xml.push_str("</AccessControlList></AccessControlPolicy>");
    xml
}

async fn put_bucket_acl_xml(fx: &S3Fixture, bucket: &str, body: &str) -> reqwest::Response {
    let uri = format!("/{bucket}?acl");
    let headers = fx.signed_headers("PUT", &uri, body.as_bytes());
    fx.send(Method::PUT, &uri, headers, body.as_bytes().to_vec())
        .await
}

async fn put_object_acl_xml(
    fx: &S3Fixture,
    bucket: &str,
    key: &str,
    body: &str,
) -> reqwest::Response {
    let uri = format!("/{bucket}/{key}?acl");
    let headers = fx.signed_headers("PUT", &uri, body.as_bytes());
    fx.send(Method::PUT, &uri, headers, body.as_bytes().to_vec())
        .await
}

async fn tenant_get(
    fx: &S3Fixture,
    cred: &(String, String),
    bucket: &str,
    key: &str,
) -> reqwest::Response {
    let uri = format!("/{bucket}/{key}");
    let headers = signed_headers_for(&fx.s3_addr, &cred.0, &cred.1, "GET", &uri, b"");
    fx.send(Method::GET, &uri, headers, Vec::new()).await
}

async fn tenant_list(fx: &S3Fixture, cred: &(String, String), bucket: &str) -> reqwest::Response {
    let uri = format!("/{bucket}?list-type=2");
    let headers = signed_headers_for(&fx.s3_addr, &cred.0, &cred.1, "GET", &uri, b"");
    fx.send(Method::GET, &uri, headers, Vec::new()).await
}

async fn owner_id_of(fx: &S3Fixture, bucket: &str) -> String {
    let uri = format!("/{bucket}?acl");
    let headers = fx.signed_headers("GET", &uri, b"");
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 200);
    xml_tag(&resp.text().await.unwrap(), "ID")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bucket_acl_grants_lift_listing_only() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxb").await;
    let tenant_b = tenant_connector(&fx, "tenant-b").await;
    let owner = owner_id_of(&fx, &bucket).await;

    let data = b"payload".to_vec();
    let resp = fx.put_object(&bucket, "doc.bin", &data).await;
    assert_eq!(resp.status().as_u16(), 200);

    assert_eq!(
        tenant_list(&fx, &tenant_b, &bucket).await.status().as_u16(),
        403,
        "private bucket blocks other tenant listing"
    );

    let body = acl_xml(&owner, &[(Some("tenant-b"), "READ")]);
    let resp = put_bucket_acl_xml(&fx, &bucket, &body).await;
    assert_eq!(resp.status().as_u16(), 200, "XML PutBucketAcl accepted");

    assert_eq!(
        tenant_list(&fx, &tenant_b, &bucket).await.status().as_u16(),
        200,
        "bucket READ grant lifts listing"
    );
    assert_eq!(
        tenant_get(&fx, &tenant_b, &bucket, "doc.bin")
            .await
            .status()
            .as_u16(),
        403,
        "bucket READ grant does NOT lift object reads (AWS geometry)"
    );

    // GET ?acl 回吐 grants 形态(含 B 的 READ grant)。
    let uri = format!("/{bucket}?acl");
    let headers = fx.signed_headers("GET", &uri, b"");
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    let xml = resp.text().await.unwrap();
    assert!(xml.contains("<ID>tenant-b</ID>"), "{xml}");
    assert!(xml.contains("<Permission>READ</Permission>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn object_acl_grants_lift_single_read() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxo").await;
    let tenant_b = tenant_connector(&fx, "tenant-b").await;
    let owner = owner_id_of(&fx, &bucket).await;

    let data = b"shared".to_vec();
    let resp = fx.put_object(&bucket, "share.bin", &data).await;
    assert_eq!(resp.status().as_u16(), 200);
    let resp = fx
        .put_object(&bucket, "sibling.bin", b"sibling".to_vec().as_slice())
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    let body = acl_xml(&owner, &[(Some("tenant-b"), "READ")]);
    let resp = put_object_acl_xml(&fx, &bucket, "share.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "object ACL PUT");

    let resp = tenant_get(&fx, &tenant_b, &bucket, "share.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "granted object readable");
    assert_eq!(resp.bytes().await.unwrap().to_vec(), data);
    assert_eq!(
        tenant_get(&fx, &tenant_b, &bucket, "sibling.bin")
            .await
            .status()
            .as_u16(),
        403,
        "grant is per object, siblings stay private"
    );

    // AllUsers 形("Group" URI)放开匿名单对象。
    let body = acl_xml(&owner, &[(None, "READ")]);
    let resp = put_object_acl_xml(&fx, &bucket, "share.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200);
    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}/share.bin")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "AllUsers object grant opens anonymous read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_deny_beats_object_grant() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxdeny").await;
    let owner = owner_id_of(&fx, &bucket).await;

    let resp = fx
        .put_object(&bucket, "k.bin", b"data".to_vec().as_slice())
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    // 策略:显式 Deny 全体 GetObject(资源 = 整桶)。
    let deny = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:antfs:s3:::{bucket}/*"}}]}}"#
    );
    let uri = format!("/{bucket}?policy");
    let headers = fx.signed_headers("PUT", &uri, deny.as_bytes());
    let resp = fx.send(Method::PUT, &uri, headers, deny.into_bytes()).await;
    assert_eq!(resp.status().as_u16(), 200);

    // 对象 grants tenant-b READ(若授予腿真被评估,它会想放行)。
    let body = acl_xml(&owner, &[(Some("tenant-b"), "READ")]);
    let resp = put_object_acl_xml(&fx, &bucket, "k.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200);

    let tenant_b = tenant_connector(&fx, "tenant-b").await;
    let resp = tenant_get(&fx, &tenant_b, &bucket, "k.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "explicit policy Deny outranks ACL grants"
    );
    // owner 自身也被 Deny(授予腿更轮不到)。
    let resp = fx.get_object(&bucket, "k.bin").await;
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bucket_acl_xml_allusers_maps_to_public_read() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxall").await;
    let owner = owner_id_of(&fx, &bucket).await;

    let data = b"open".to_vec();
    let resp = fx.put_object(&bucket, "open.bin", &data).await;
    assert_eq!(resp.status().as_u16(), 200);

    let body = acl_xml(&owner, &[(None, "READ")]);
    let resp = put_bucket_acl_xml(&fx, &bucket, &body).await;
    assert_eq!(resp.status().as_u16(), 200);

    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}/open.bin")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "AllUsers bucket grant = public-read"
    );
    assert_eq!(resp.bytes().await.unwrap().to_vec(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn acl_xml_rejections_are_loud() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxbad").await;
    let owner = owner_id_of(&fx, &bucket).await;
    let resp = fx
        .put_object(&bucket, "k.bin", b"data".to_vec().as_slice())
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    // WRITE 家族:不支持即拒。
    let body = acl_xml(&owner, &[(Some("tenant-b"), "WRITE")]);
    let resp = put_bucket_acl_xml(&fx, &bucket, &body).await;
    assert_eq!(resp.status().as_u16(), 400, "WRITE permission rejected");

    // FULL_CONTROL 给他人:不支持即拒。
    let body = acl_xml(&owner, &[(Some("tenant-b"), "FULL_CONTROL")]);
    let resp = put_object_acl_xml(&fx, &bucket, "k.bin", &body).await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "FULL_CONTROL to others rejected"
    );

    // Owner 不是桶主:403。
    let body = acl_xml("impostor", &[(Some("tenant-b"), "READ")]);
    let resp = put_bucket_acl_xml(&fx, &bucket, &body).await;
    assert_eq!(resp.status().as_u16(), 403, "Owner mismatch rejected");

    // 坏 XML:MalformedXML 400。
    let resp = put_bucket_acl_xml(&fx, &bucket, " ACE not xml ").await;
    assert_eq!(resp.status().as_u16(), 400, "malformed XML body rejected");

    // 对象不存在:acls 面对幽灵对象 → 404 NoSuchKey。
    let body = acl_xml(&owner, &[(Some("tenant-b"), "READ")]);
    let resp = put_object_acl_xml(&fx, &bucket, "ghost.bin", &body).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "ACL PUT on missing object is 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn canned_header_replace_semantics_and_revert() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("aclxcanned").await;
    let owner = owner_id_of(&fx, &bucket).await;
    let tenant_b = tenant_connector(&fx, "tenant-b").await;

    let resp = fx
        .put_object(&bucket, "k.bin", b"data".to_vec().as_slice())
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    // XML 授予 B。
    let body = acl_xml(&owner, &[(Some("tenant-b"), "READ")]);
    let resp = put_bucket_acl_xml(&fx, &bucket, &body).await;
    assert_eq!(resp.status().as_u16(), 200);
    // AllUsers READ 形态对象授予(展示对象域 AllUsers——用对象域)已覆盖他测;此处仅断言 B 已抬。
    assert_eq!(
        tenant_list(&fx, &tenant_b, &bucket).await.status().as_u16(),
        200
    );

    // canned 头换置 = 全量替换(grants 清空,回 pure private)。
    let uri = format!("/{bucket}?acl");
    let mut headers = fx.signed_headers("PUT", &uri, b"");
    headers.insert("x-amz-acl", "private".parse().unwrap());
    let resp = fx.send(Method::PUT, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 200, "canned HEAD path untouched");
    assert_eq!(
        tenant_list(&fx, &tenant_b, &bucket).await.status().as_u16(),
        403,
        "canned PUT replaces the grant set wholesale"
    );
}
