pub mod admin;
pub mod batch;
pub mod bucket;
pub mod multipart;
pub mod object;
pub mod range;

use axum::body::Bytes;
use axum::extract::Request;
use axum::http::header;

use crate::error::S3Error;
use crate::server::AppState;

/// 读取有界请求体(put_object / upload_part 共用):解码器产出的 `S3Error`
/// 经 `axum::Error::downcast` 精确回放;其余读体错误保持既有 EntityTooLarge
/// 宽语义。分帧请求长度对账 `x-amz-decoded-content-length`(分帧线上
/// Content-Length = 帧字号,不用于对账);非分帧保持逐字节既有语义。
pub(crate) async fn read_bounded_body(st: &AppState, req: Request) -> Result<Bytes, S3Error> {
    let (parts, body) = req.into_parts();
    let decoded_header = parts
        .headers
        .get("x-amz-decoded-content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    let content_length = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let bytes = axum::body::to_bytes(body, st.config.max_body_bytes as usize)
        .await
        .map_err(map_body_error)?;

    if let Some(declared) = decoded_header {
        if bytes.len() as u64 != declared {
            return Err(S3Error::IncompleteBody);
        }
    } else if let Some(cl) = content_length {
        if bytes.len() as u64 != cl {
            return Err(S3Error::InternalError(format!(
                "body size {} != Content-Length {cl}",
                bytes.len()
            )));
        }
    }
    Ok(bytes)
}

/// 分帧解码错误原样回放(错误链三层:axum 包装 ×2 → 生源 `S3Error`);
/// 溯源失败 = 疑似超限/传输断,保持既有 EntityTooLarge 宽语义(非分帧路径)。
fn map_body_error(error: axum::Error) -> S3Error {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(err) = source {
        if let Some(s3) = err.downcast_ref::<S3Error>() {
            return s3.clone();
        }
        source = err.source();
    }
    S3Error::EntityTooLarge
}
