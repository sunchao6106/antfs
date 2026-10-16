//! Bucket management and ListObjectsV2 (task 13.3).
//!
//! All tests are `#[ignore]`d — they need a live PostgreSQL via `TEST_S3_POSTGRES_URL`.

mod common;

use common::{pseudo_random, xml_tag, S3Fixture};

fn count_contents(xml: &str) -> usize {
    xml.matches("<Contents>").count()
}

/// `prefix` filters the listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_objects_prefix() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("list").await;

    for key in ["logs/a", "logs/b", "data/c"] {
        let resp = fx.put_object(&bucket, key, b"x").await;
        assert_eq!(resp.status().as_u16(), 200, "PUT {key} must 200");
    }

    let resp = fx
        .list_objects_v2(&bucket, "list-type=2&prefix=logs/")
        .await;
    assert_eq!(resp.status().as_u16(), 200, "ListObjectsV2 must 200");
    let xml = resp.text().await.expect("list body");
    assert_eq!(
        count_contents(&xml),
        2,
        "only the logs/ prefix matches: {xml}"
    );
    assert!(xml.contains("<Key>logs/a</Key>"), "logs/a listed: {xml}");
    assert!(xml.contains("<Key>logs/b</Key>"), "logs/b listed: {xml}");
    assert!(!xml.contains("<Key>data/c</Key>"), "data/c excluded: {xml}");
}

/// A `delimiter` rolls deeper keys into `CommonPrefixes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_objects_delimiter() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("list").await;

    for key in ["logs/2024/a", "logs/2025/b"] {
        let resp = fx.put_object(&bucket, key, b"x").await;
        assert_eq!(resp.status().as_u16(), 200, "PUT {key} must 200");
    }

    let resp = fx
        .list_objects_v2(&bucket, "list-type=2&prefix=logs/&delimiter=/")
        .await;
    assert_eq!(resp.status().as_u16(), 200, "ListObjectsV2 must 200");
    let xml = resp.text().await.expect("list body");
    assert!(
        xml.contains("<CommonPrefixes><Prefix>logs/2024/</Prefix></CommonPrefixes>"),
        "logs/2024/ must be a common prefix: {xml}"
    );
    assert!(
        xml.contains("<CommonPrefixes><Prefix>logs/2025/</Prefix></CommonPrefixes>"),
        "logs/2025/ must be a common prefix: {xml}"
    );
    assert_eq!(
        count_contents(&xml),
        0,
        "no direct contents under the delimiter: {xml}"
    );
}

/// `max-keys` truncates the page and hands back a continuation token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_objects_pagination() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("list").await;

    for key in ["k1", "k2", "k3", "k4", "k5"] {
        let resp = fx.put_object(&bucket, key, b"x").await;
        assert_eq!(resp.status().as_u16(), 200, "PUT {key} must 200");
    }

    let resp = fx.list_objects_v2(&bucket, "list-type=2&max-keys=2").await;
    assert_eq!(resp.status().as_u16(), 200, "first page must 200");
    let page1 = resp.text().await.expect("first page body");
    assert_eq!(
        count_contents(&page1),
        2,
        "first page holds max-keys entries"
    );
    assert!(
        page1.contains("<IsTruncated>true</IsTruncated>"),
        "first page must be truncated: {page1}"
    );
    let token = xml_tag(&page1, "NextContinuationToken");
    assert!(!token.is_empty(), "a continuation token must be present");

    let resp = fx
        .list_objects_v2(
            &bucket,
            &format!("list-type=2&max-keys=2&continuation-token={token}"),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "second page must 200");
    let page2 = resp.text().await.expect("second page body");
    assert_eq!(
        count_contents(&page2),
        2,
        "second page holds the next entries"
    );
}

/// CreateBucket → HeadBucket 200 → DeleteBucket 204 → HeadBucket 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn create_delete_bucket() {
    let fx = S3Fixture::start().await;
    let bucket = fx.unique("bkt");

    assert_eq!(
        fx.create_bucket(&bucket).await.status().as_u16(),
        200,
        "CreateBucket must 200"
    );
    assert_eq!(
        fx.head_bucket(&bucket).await.status().as_u16(),
        200,
        "HeadBucket on an existing bucket must 200"
    );
    assert_eq!(
        fx.delete_bucket(&bucket).await.status().as_u16(),
        204,
        "DeleteBucket must 204"
    );
    assert_eq!(
        fx.head_bucket(&bucket).await.status().as_u16(),
        404,
        "HeadBucket after DeleteBucket must 404"
    );
}

/// Deleting a bucket that still holds objects is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn delete_nonempty_bucket_fails() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("full").await;

    let resp = fx.put_object(&bucket, "obj", &pseudo_random(16)).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must 200");

    assert_eq!(
        fx.delete_bucket(&bucket).await.status().as_u16(),
        409,
        "DeleteBucket on a non-empty bucket must 409"
    );
}

/// ListBuckets shows every bucket owned by the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_buckets() {
    let fx = S3Fixture::start().await;
    let first = fx.create_unique_bucket("bkt-a").await;
    let second = fx.create_unique_bucket("bkt-b").await;

    let resp = fx.list_buckets().await;
    assert_eq!(resp.status().as_u16(), 200, "ListBuckets must 200");
    let xml = resp.text().await.expect("list buckets body");
    assert!(
        xml.contains(&format!("<Name>{first}</Name>")),
        "first bucket listed: {xml}"
    );
    assert!(
        xml.contains(&format!("<Name>{second}</Name>")),
        "second bucket listed: {xml}"
    );
}

// ── ListObjectsV1 + GetBucketLocation(s3-read-compat)────────────────────────

/// 裸 `GET /{bucket}`(无 list-type)V1 形态:Marker 回显 / 无 KeyCount /
/// Contents StorageClass / max-keys 截断翻页(union 无缺漏)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_objects_v1_shape_and_pagination() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("v1").await;

    for key in ["k1", "k2", "k3", "k4", "k5"] {
        let resp = fx.put_object(&bucket, key, b"x").await;
        assert_eq!(resp.status().as_u16(), 200, "PUT {key} must 200");
    }

    let resp = fx
        .request(reqwest::Method::GET, &format!("/{bucket}"), Vec::new())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "bare GET is V1");
    let xml = resp.text().await.expect("v1 body");
    assert_eq!(count_contents(&xml), 5, "full listing: {xml}");
    assert!(xml.contains("<Name>") && xml.contains("<Prefix></Prefix>"));
    assert!(
        xml.contains("<Marker></Marker>"),
        "Marker always present: {xml}"
    );
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
    assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
    assert!(!xml.contains("<KeyCount>"), "KeyCount is V2-only");
    assert!(!xml.contains("<NextContinuationToken>"));

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?max-keys=2"),
            Vec::new(),
        )
        .await;
    let page1 = resp.text().await.expect("page 1");
    assert_eq!(count_contents(&page1), 2);
    assert!(page1.contains("<IsTruncated>true</IsTruncated>"));
    assert!(
        !page1.contains("<NextMarker>"),
        "no delimiter → no NextMarker: {page1}"
    );
    assert!(page1.contains("<Marker></Marker>"));

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?max-keys=2&marker=k2"),
            Vec::new(),
        )
        .await;
    let page2 = resp.text().await.expect("page 2");
    assert_eq!(count_contents(&page2), 2, "marker is exclusive: {page2}");
    assert!(page2.contains("<Marker>k2</Marker>"), "marker echoed back");
    assert!(page2.contains("<Key>k3</Key>") && page2.contains("<Key>k4</Key>"));

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?marker=k4"),
            Vec::new(),
        )
        .await;
    let page3 = resp.text().await.expect("page 3");
    assert_eq!(count_contents(&page3), 1);
    assert!(page3.contains("<Key>k5</Key>"));
}

/// V1 + delimiter:CommonPrefixes + NextMarker(仅 delimiter 存在且截断时)——
/// 翻页走 NextMarker,分页 union 无缺漏。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_objects_v1_delimiter_next_marker() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("v1d").await;

    for key in ["data/c", "logs/2024/a", "logs/2024/b", "logs/2025/c"] {
        let resp = fx.put_object(&bucket, key, b"x").await;
        assert_eq!(resp.status().as_u16(), 200, "PUT {key} must 200");
    }

    let mut entire_xml = String::new();

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?prefix=logs/&delimiter=/&max-keys=2"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "page 1 must 200");
    let page1 = resp.text().await.expect("page 1");
    assert!(page1.contains("<IsTruncated>true</IsTruncated>"), "{page1}");
    assert!(
        page1.contains("<CommonPrefixes><Prefix>logs/2024/</Prefix></CommonPrefixes>"),
        "page 1 groups logs/2024/*: {page1}"
    );
    assert!(
        page1.contains("<NextMarker>logs/2024/b</NextMarker>"),
        "NextMarker = 排他 token(末个已扫 raw key,吞于分组): {page1}"
    );
    assert!(!page1.contains("<KeyCount>"));
    entire_xml.push_str(&page1);

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?prefix=logs/&delimiter=/&marker=logs/2024/b"),
            Vec::new(),
        )
        .await;
    let page2 = resp.text().await.expect("page 2");
    assert!(
        page2.contains("<CommonPrefixes><Prefix>logs/2025/</Prefix></CommonPrefixes>"),
        "next page holds the rest: {page2}"
    );
    assert!(
        page2.contains("<Marker>logs/2024/b</Marker>"),
        "marker echoed back"
    );
    assert!(
        page2.contains("<IsTruncated>false</IsTruncated>"),
        "{page2}"
    );
    assert!(!page2.contains("<NextMarker>"));
    entire_xml.push_str(&page2);

    assert!(entire_xml.contains("<Prefix>logs/2024/</Prefix>"));
    assert!(entire_xml.contains("<Prefix>logs/2025/</Prefix>"));
}

/// `GET /{bucket}?location`:owner 专属;region=us-east-1(测试默认)回空元素。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn get_bucket_location_us_east_1_empty() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("loc").await;

    let resp = fx
        .request(
            reqwest::Method::GET,
            &format!("/{bucket}?location"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "owner GET ?location must 200");
    let xml = resp.text().await.expect("location body");
    assert!(
        xml.contains("<LocationConstraint"),
        "LocationConstraint present: {xml}"
    );
    assert!(
        !xml.contains("us-east-1"),
        "classic region renders as an empty element: {xml}"
    );
    assert!(
        xml.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"),
        "{xml}"
    );
}
