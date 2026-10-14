use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
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
