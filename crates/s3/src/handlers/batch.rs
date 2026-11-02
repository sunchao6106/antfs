use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::S3Error;
use crate::server::AppState;

pub async fn delete_objects(st: AppState, bucket: &str, req: Request) -> Result<Response, S3Error> {
    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .map_err(|_| S3Error::MalformedXML("body too large".into()))?;

    let xml_str =
        std::str::from_utf8(&body).map_err(|_| S3Error::MalformedXML("invalid utf-8".into()))?;

    let (keys, quiet) = crate::xml::parse_delete_objects(xml_str)?;

    let results = st.metastore.delete_objects(bucket, &keys).await?;

    // blob 侧零随手动作:delete_objects 事务内已把各 key 的主 blob + manifest 片
    // 全部送 pending_reclaims(H3 级联),GC 每日墓碑化;旧行为"删后再 get_object"
    // 是死代码且有并发误删新对象的竞态,已随级联一并摘除。

    let xml = crate::xml::serialize_delete_result(&results, quiet);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}
