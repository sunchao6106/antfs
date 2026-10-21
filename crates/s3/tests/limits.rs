//! 公共单文件上限(`[upload]` 表,REST 与 S3 共用一处)在 **S3 面**的双闸 e2e:
//! 本二进制设 `TEST_S3_MAX_OBJECT_BYTES` 起「小上限」夹具(其余 e2e 二进制不设 →
//! 默认 16 GiB = 零变化;同 `tests/vhost.rs` 的「专用二进制 + 夹具建时读 env」模式)。
//!
//! 全部 `#[ignore]`(活库依赖,同 object/multipart 惯例:`TEST_S3_DRIVER` /
//! `TEST_S3_POSTGRES_URL`)。

mod common;

use common::{header, xml_tag, S3Fixture};

/// 公共上限 = 1 MiB:远小于 multipart 片的 body 界(4 MiB),令「合计超限」「单件合法」
/// 两条闸都能在小体量下扎实地测。
const CAP: u64 = 1024 * 1024;

/// 单次 PutObject 声明超公共上限:413 `EntityTooLarge`(读 body 之前的声明闸);
/// 上限内的单传照常 200。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn put_object_over_shared_cap_is_rejected() {
    std::env::set_var("TEST_S3_MAX_OBJECT_BYTES", CAP.to_string());
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("lim").await;

    // 2 MiB > 1 MiB:必须 413。
    let resp = fx
        .put_object(&bucket, "over.bin", &vec![7u8; (CAP * 2) as usize])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        413,
        "超过公共单文件上限必须 EntityTooLarge"
    );
    let text = resp.text().await.expect("read xml");
    assert!(text.contains("EntityTooLarge"), "S3 错误码面:\n{text}");

    // 上限内的单传照常成立。
    let resp = fx
        .put_object(&bucket, "ok.bin", &vec![7u8; (CAP as usize) - 1024])
        .await;
    assert_eq!(resp.status().as_u16(), 200, "上限内必须 200");
}

/// Initiate an upload and return its upload id(同 tests/multipart.rs 的助手形态)。
async fn initiate(fx: &S3Fixture, bucket: &str, key: &str) -> String {
    let resp = fx.create_multipart(bucket, key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "CreateMultipartUpload must 200"
    );
    let xml = resp.text().await.expect("initiate body");
    xml_tag(&xml, "UploadId")
}

/// Build the `<CompleteMultipartUpload>` XML body from `(part_number, etag)` pairs.
fn complete_xml(parts: &[(i32, String)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// multipart 完成时按 manifest 所列各片尺寸**合计**对账:超过公共上限 → 413;session
/// 保留(ListParts 200、abort 204),且不产生对象行(GET 404)。片单件 ≤ body 界不受影响。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_total_over_shared_cap_is_rejected() {
    std::env::set_var("TEST_S3_MAX_OBJECT_BYTES", CAP.to_string());
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "over-total.bin";

    let upload_id = initiate(&fx, &bucket, key).await;

    // 两片各 640 KiB(单片 ≤ 4 MiB body 界,本身合法),合计 1.25 MiB > 1 MiB 上限。
    let mut etags = Vec::new();
    for number in [1i32, 2] {
        let resp = fx
            .upload_part(&bucket, key, &upload_id, number, &vec![7u8; 640 * 1024])
            .await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "UploadPart {number} (单片合法)必须 200"
        );
        etags.push((number, header(&resp, "etag")));
    }

    // complete 列入两片:合计超限 → 413。
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        413,
        "合计超公共单文件上限必须 EntityTooLarge"
    );
    let text = resp.text().await.expect("read xml");
    assert!(text.contains("EntityTooLarge"), "错误码面:\n{text}");

    // 拒绝不扫会话:ListParts 仍 200,客户端可显式 abort。
    let resp = fx.list_parts(&bucket, key, &upload_id).await;
    assert_eq!(resp.status().as_u16(), 200, "被拒后 session 必须保留");
    let resp = fx.abort_multipart(&bucket, key, &upload_id).await;
    assert_eq!(resp.status().as_u16(), 204, "abort 必须 204");

    // 没有对象行被写下。
    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 404, "被拒后不得存在对象");
}
