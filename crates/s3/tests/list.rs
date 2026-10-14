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
