//! 桶策略(`s3-bucket-policy`):JSON 策略文档的 put 校验、跨租户/匿名授权、
//! Deny 优先、管理面恒 owner、坏政策 fail-closed。
//!
//! All tests are `#[ignore]`d — they need a live database via
//! `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL`.

mod common;

use common::{signed_headers_for, S3Fixture};
use reqwest::Method;

/// 以指名 owner 造一张新凭证(admin 面,operator token 空 = 放行)。
async fn tenant_connector(fx: &S3Fixture, owner: &str) -> (String, String) {
    let resp = fx
        .http
        .post(fx.url("/admin/credentials"))
        .json(&serde_json::json!({ "owner": owner }))
        .send()
        .await
        .expect("create credential");
    assert_eq!(resp.status().as_u16(), 200, "credential must be created");
    let body: serde_json::Value = resp.json().await.expect("json");
    (
        body["access_key"].as_str().expect("ak").to_string(),
        body["secret_key"].as_str().expect("sk").to_string(),
    )
}

async fn put_policy(fx: &S3Fixture, bucket: &str, doc: &str) -> reqwest::Response {
    let uri = format!("/{bucket}?policy");
    let headers = fx.signed_headers("PUT", &uri, doc.as_bytes());
    fx.send(Method::PUT, &uri, headers, doc.as_bytes().to_vec())
        .await
}

async fn get_policy(fx: &S3Fixture, bucket: &str) -> reqwest::Response {
    let uri = format!("/{bucket}?policy");
    let headers = fx.signed_headers("GET", &uri, b"");
    fx.send(Method::GET, &uri, headers, Vec::new()).await
}

async fn delete_policy(fx: &S3Fixture, bucket: &str) -> reqwest::Response {
    let uri = format!("/{bucket}?policy");
    let headers = fx.signed_headers("DELETE", &uri, b"");
    fx.send(Method::DELETE, &uri, headers, Vec::new()).await
}

/// 以他租户签名取对象(GET)。
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

async fn tenant_put(
    fx: &S3Fixture,
    cred: &(String, String),
    bucket: &str,
    key: &str,
    body: &[u8],
) -> reqwest::Response {
    let uri = format!("/{bucket}/{key}");
    let headers = signed_headers_for(&fx.s3_addr, &cred.0, &cred.1, "PUT", &uri, body);
    fx.send(Method::PUT, &uri, headers, body.to_vec()).await
}

fn doc_allow_reports_for(owner: &str, bucket: &str) -> String {
    format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{owner}"}},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::{bucket}/Reports/*"}}]}}"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_grants_cross_tenant_prefix_reads() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("polgrant").await;
    let tenant_b = tenant_connector(&fx, "tenant-b").await;

    let q1 = b"q1 report bytes".to_vec();
    let secret = b"secret bytes".to_vec();
    for (key, data) in [("Reports/q1.bin", &q1), ("Secrets/x.bin", &secret)] {
        let resp = fx.put_object(&bucket, key, data).await;
        assert_eq!(resp.status().as_u16(), 200, "owner PUT {key}");
    }

    // 授权前:他租户读 403(私有兜底)。
    let resp = tenant_get(&fx, &tenant_b, &bucket, "Reports/q1.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "private bucket blocks tenant-b"
    );

    let resp = put_policy(&fx, &bucket, &doc_allow_reports_for("tenant-b", &bucket)).await;
    assert_eq!(resp.status().as_u16(), 200, "PutBucketPolicy must 200");

    // 授权后:前缀内放行、界外 403、列举未授予 403、匿名未授予 403。
    let resp = tenant_get(&fx, &tenant_b, &bucket, "Reports/q1.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "policy grants prefix read");
    assert_eq!(resp.bytes().await.unwrap().to_vec(), q1);

    let resp = tenant_get(&fx, &tenant_b, &bucket, "Secrets/x.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "outside policy prefix stays private"
    );

    let uri = format!("/{bucket}?list-type=2");
    let headers = signed_headers_for(&fx.s3_addr, &tenant_b.0, &tenant_b.1, "GET", &uri, b"");
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 403, "ListBucket not granted");

    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}/Reports/q1.bin")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403, "anonymous not granted");

    // GET ?policy 回原文;删除后回私有(零变化闭环)。
    let resp = get_policy(&fx, &bucket).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.text().await.unwrap(),
        doc_allow_reports_for("tenant-b", &bucket)
    );

    let resp = delete_policy(&fx, &bucket).await;
    assert_eq!(resp.status().as_u16(), 204, "DeleteBucketPolicy");
    let resp = get_policy(&fx, &bucket).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "policy gone → NoSuchBucketPolicy"
    );
    let resp = tenant_get(&fx, &tenant_b, &bucket, "Reports/q1.bin").await;
    assert_eq!(resp.status().as_u16(), 403, "back to private after delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_denies_owner_but_never_policy_management() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("poldeny").await;

    let deny_doc = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":"s3:PutObject","Resource":"arn:antfs:s3:::{bucket}/*"}}]}}"#
    );
    let resp = put_policy(&fx, &bucket, &deny_doc).await;
    assert_eq!(resp.status().as_u16(), 200);

    // 显式 Deny 压 owner 蕴含:owner 自己写被拒。
    let resp = fx.put_object(&bucket, "k.bin", b"data").await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "explicit Deny beats owner implicit allow"
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("AccessDenied"), "{body}");

    // 管理面恒 owner:被 Deny 的 owner 仍可换策略、仍可删策略。
    let allow_doc = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":"*","Resource":"arn:antfs:s3:::{bucket}/*"}}]}}"#
    );
    let resp = put_policy(&fx, &bucket, &allow_doc).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "owner never locked out of policy PUT"
    );

    let data = b"after replacement".to_vec();
    let resp = fx.put_object(&bucket, "k.bin", &data).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "allow-* policy unblocks writes"
    );

    let resp = delete_policy(&fx, &bucket).await;
    assert_eq!(
        resp.status().as_u16(),
        204,
        "owner never locked out of policy DELETE"
    );
    let data = b"back to normal".to_vec();
    let resp = fx.put_object(&bucket, "k2.bin", &data).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "no policy = owner semantics restored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_grants_anonymous_reads_on_private_bucket() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("polanon").await;

    let data = b"public bytes".to_vec();
    let resp = fx.put_object(&bucket, "pub/anything.bin", &data).await;
    assert_eq!(resp.status().as_u16(), 200);

    let doc = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:antfs:s3:::{bucket}/pub/*"}}]}}"#
    );
    let resp = put_policy(&fx, &bucket, &doc).await;
    assert_eq!(resp.status().as_u16(), 200);

    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}/pub/anything.bin")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "anonymous GET granted by policy"
    );
    assert_eq!(resp.bytes().await.unwrap().to_vec(), data);

    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}?list-type=2")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        403,
        "listing still not granted (private fallback)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_put_rejects_unsupported_documents() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("polbad").await;
    let bucket2 = fx.create_unique_bucket("opolbad").await;

    let base = r#"{"Version":"2012-10-17","Statement":[{"Effect":"ALLOW","Principal":{"AWS":"x"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::{HERE}/*"}]}"#;
    let bad_docs = [
        "not json at all".to_string(),
        r#"{"Version":"2008-10-17","Statement":[]}"#.to_string(),
        base.replace("{HERE}", &bucket).replace("ALLOW", "Allow").replace(
            "s3:GetObject",
            "s3:ListObjects",
        ),
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1:user/u"},"Action":"s3:GetObject","Resource":"arn:antfs:s3:::POL/*"}]}"#
            .replace("POL", &bucket),
        base.replace("{HERE}", &bucket)
            .replace("\"Effect\":\"Allow\"", "\"Effect\":\"Allow\",\"Condition\":{\"k\":\"v\"}"),
        base.replace("{HERE}", &bucket2),
        format!(
            "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\"Principal\":{{\"AWS\":\"o\"}},\"Action\":\"s3:GetObject\",\"Resource\":\"arn:antfs:s3:::{bucket}/{}\"}}]}}",
            "a".repeat(21 * 1024)
        ),
    ];
    for (idx, doc) in bad_docs.iter().enumerate() {
        let resp = put_policy(&fx, &bucket, doc).await;
        assert_eq!(
            resp.status().as_u16(),
            400,
            "doc #{idx} must be rejected loudly"
        );
    }
    // 无一存库。
    let resp = get_policy(&fx, &bucket).await;
    assert_eq!(resp.status().as_u16(), 404, "no policy stored after 400s");
    // 非法 type 语义面拒绝同族。
    let resp = delete_policy(&fx, &bucket).await;
    assert_eq!(resp.status().as_u16(), 204, "delete stays idempotent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn corrupt_stored_policy_fails_closed() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("polcorrupt").await;
    let data = b"bytes".to_vec();
    let resp = fx.put_object(&bucket, "k.bin", &data).await;
    assert_eq!(resp.status().as_u16(), 200);

    // 直改库注入坏策略(理论不可达,人工破坏模拟)→ 鉴权面 fail-closed 500。
    update_bucket_policy_row(&fx, Some("}not-json{"), &bucket).await;
    let resp = fx.get_object(&bucket, "k.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        500,
        "corrupt stored policy must fail closed (no silent weakening)"
    );

    // 复原:共享测试库不受污染,刚改的桶恢复原服务面。
    update_bucket_policy_row(&fx, None, &bucket).await;
    let resp = fx.get_object(&bucket, "k.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "restored row serves normally");
}

/// 双方言直改 `buckets.policy`(fail-closed 用例的破坏注入口)。
async fn update_bucket_policy_row(fx: &S3Fixture, policy: Option<&str>, bucket: &str) {
    match fx.driver.as_str() {
        "mysql" => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(2)
                .connect(&fx.metastore_url)
                .await
                .expect("connect");
            sqlx::query("UPDATE buckets SET policy = ? WHERE name = ?")
                .bind(policy)
                .bind(bucket)
                .execute(&pool)
                .await
                .expect("update policy");
        }
        _ => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .connect(&fx.metastore_url)
                .await
                .expect("connect");
            sqlx::query("UPDATE buckets SET policy = $1 WHERE name = $2")
                .bind(policy)
                .bind(bucket)
                .execute(&pool)
                .await
                .expect("update policy");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn policy_grants_cross_tenant_writes_within_resource() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("polwrite").await;
    let tenant_b = tenant_connector(&fx, "tenant-b").await;

    let doc = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":"s3:PutObject","Resource":"arn:antfs:s3:::{bucket}/inbox/*"}}]}}"#
    );
    let resp = put_policy(&fx, &bucket, &doc).await;
    assert_eq!(resp.status().as_u16(), 200);

    let payload = b"drop-off".to_vec();
    let resp = tenant_put(&fx, &tenant_b, &bucket, "inbox/drop.bin", &payload).await;
    assert_eq!(resp.status().as_u16(), 200, "policy grants scoped write");
    let get = fx.get_object(&bucket, "inbox/drop.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(get.bytes().await.unwrap().to_vec(), payload);

    let resp = tenant_put(&fx, &tenant_b, &bucket, "outbox/drop.bin", &payload).await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "write outside the granted resource stays private"
    );
    let resp = tenant_get(&fx, &tenant_b, &bucket, "inbox/drop.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "PutObject grant does not imply reads"
    );
}
