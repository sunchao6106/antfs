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

    for r in &results {
        if r.deleted {
            if let Ok(Some(obj)) = st.metastore.get_object(bucket, &r.key).await {
                let _ = st.router.delete(&obj.blob_id).await;
            }
        }
    }

    let xml = crate::xml::serialize_delete_result(&results, quiet);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}
