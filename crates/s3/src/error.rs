use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone)]
pub enum S3Error {
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidBucketName(String),
    InvalidPart,
    InvalidPartOrder,
    AccessDenied(String),
    SignatureDoesNotMatch,
    InvalidAccessKeyId,
    RequestTimeTooSkewed,
    InternalError(String),
    MalformedXML(String),
    EntityTooLarge,
    MissingContentLength,
    /// `aws-chunked` 流式分帧的解码长度对账失配/截断(aws-chunked 轮起)。
    IncompleteBody,
    MetadataTooLarge,
    QuotaExceeded {
        bucket: String,
        quota: u64,
        used: u64,
    },
    InvalidArgument(String),
    /// 参数各自合法但组合非法(如自拷贝且 COPY 指令)——区别于 InvalidArgument 的单值坏。
    InvalidRequest(String),
    /// 条件拷贝/读的前置条件不满足(412 家族)。
    PreconditionFailed,
    /// 能力显式出界(未实现的面,如 PutBucketAcl 的 XML body 形式)——不静默也不 400 误导。
    NotImplemented(String),
    InvalidRange {
        requested: String,
        actual_size: u64,
    },
    /// `x-amz-checksum-*`(头或 trailer)与网关对明文的计算值失配(400,报文点名载体)。
    BadDigest {
        checksum_form: String,
    },
}

impl S3Error {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InvalidBucketName(_) => "InvalidBucketName",
            Self::InvalidPart => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::AccessDenied(_) => "AccessDenied",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::RequestTimeTooSkewed => "RequestTimeTooSkewed",
            Self::InternalError(_) => "InternalError",
            Self::MalformedXML(_) => "MalformedXML",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::MissingContentLength => "MissingContentLength",
            Self::IncompleteBody => "IncompleteBody",
            Self::MetadataTooLarge => "MetadataTooLarge",
            Self::QuotaExceeded { .. } => "QuotaExceeded",
            Self::InvalidArgument(_) => "InvalidArgument",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotImplemented(_) => "NotImplemented",
            Self::InvalidRange { .. } => "InvalidRange",
            Self::BadDigest { .. } => "BadDigest",
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::NoSuchBucket | Self::NoSuchKey | Self::NoSuchUpload => StatusCode::NOT_FOUND,
            Self::BucketAlreadyExists | Self::BucketAlreadyOwnedByYou => StatusCode::CONFLICT,
            Self::BucketNotEmpty => StatusCode::CONFLICT,
            Self::InvalidBucketName(_) | Self::InvalidPart | Self::InvalidPartOrder => {
                StatusCode::BAD_REQUEST
            }
            Self::MalformedXML(_) | Self::MissingContentLength => StatusCode::BAD_REQUEST,
            Self::IncompleteBody => StatusCode::BAD_REQUEST,
            Self::MetadataTooLarge | Self::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            Self::BadDigest { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::InvalidRange { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::QuotaExceeded { .. } => StatusCode::from_u16(507).unwrap(),
            Self::AccessDenied(_)
            | Self::SignatureDoesNotMatch
            | Self::InvalidAccessKeyId
            | Self::RequestTimeTooSkewed => StatusCode::FORBIDDEN,
            Self::EntityTooLarge => StatusCode::from_u16(413).unwrap(),
            Self::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::NoSuchBucket => "The specified bucket does not exist.".into(),
            Self::NoSuchKey => "The specified key does not exist.".into(),
            Self::NoSuchUpload => "The specified multipart upload does not exist.".into(),
            Self::BucketAlreadyExists => "The requested bucket name is not available.".into(),
            Self::BucketAlreadyOwnedByYou => {
                "The bucket you tried to create already exists, and you own it.".into()
            }
            Self::BucketNotEmpty => {
                "The bucket you tried to delete is not empty.".into()
            }
            Self::InvalidBucketName(n) => format!("The specified bucket is not valid: {n}"),
            Self::InvalidPart => {
                "One or more of the specified parts could not be found.".into()
            }
            Self::InvalidPartOrder => "The list of parts was not in ascending order.".into(),
            Self::AccessDenied(msg) => {
                if msg.is_empty() {
                    "Access Denied".into()
                } else {
                    msg.clone()
                }
            }
            Self::SignatureDoesNotMatch => {
                "The request signature we calculated does not match the signature you provided."
                    .into()
            }
            Self::InvalidAccessKeyId => {
                "The AWS Access Key Id you provided does not exist in our records.".into()
            }
            Self::RequestTimeTooSkewed => {
                "The difference between the request time and the server's time is too large.".into()
            }
            Self::InternalError(msg) => {
                if msg.is_empty() {
                    "We encountered an internal error. Please try again.".into()
                } else {
                    msg.clone()
                }
            }
            Self::MalformedXML(detail) => format!("The XML you provided was not well-formed or did not validate against our published schema. {detail}"),
            Self::EntityTooLarge => "Your proposed upload exceeds the maximum allowed size.".into(),
            Self::MissingContentLength => "You must provide the Content-Length HTTP header.".into(),
            Self::IncompleteBody => {
                "The decoded body length does not match the declared x-amz-decoded-content-length."
                    .into()
            }
            Self::MetadataTooLarge => {
                "Your metadata headers exceed the maximum allowed metadata size.".into()
            }
            Self::QuotaExceeded { bucket, quota, used } => format!(
                "The bucket storage quota is exceeded (bucket: {bucket}, quota: {quota} bytes, in use: {used} bytes)."
            ),
            Self::InvalidArgument(detail) => {
                format!("Invalid argument: {detail}")
            }
            Self::InvalidRequest(detail) => format!("Invalid request: {detail}"),
            Self::PreconditionFailed => {
                "The requested precondition(s) did not hold for the source object.".into()
            }
            Self::NotImplemented(detail) => {
                format!(
                    "A header or query you provided implies functionality that is not implemented: {detail}"
                )
            }
            Self::InvalidRange { requested, actual_size } => format!(
                "The requested range is not satisfiable (requested: {requested}, actual object size: {actual_size} bytes)."
            ),
            Self::BadDigest { checksum_form } => format!(
                "The {checksum_form} value you specified did not match the calculated checksum."
            ),
        }
    }

    pub fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code(),
            quick_xml::escape::escape(self.message()),
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = self.to_xml();
        (self.status(), [("content-type", "application/xml")], body).into_response()
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for S3Error {}

impl From<antfs_router::Error> for S3Error {
    fn from(e: antfs_router::Error) -> Self {
        match &e {
            antfs_router::Error::NotFound { .. } => Self::NoSuchKey,
            antfs_router::Error::StoreFull { .. } => {
                Self::InternalError("storage capacity exhausted".into())
            }
            _ => Self::InternalError(e.to_string()),
        }
    }
}

impl From<sqlx::Error> for S3Error {
    fn from(e: sqlx::Error) -> Self {
        Self::InternalError(format!("metastore: {e}"))
    }
}
