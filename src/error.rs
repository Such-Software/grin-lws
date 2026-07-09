//! Error type mapped to HTTP responses.
//!
//! Hostile-upstream hygiene (carried over from smirk-backend-core's LWS/grin
//! clients): a node/DB error NEVER interpolates an untrusted upstream response
//! body into the client-facing message.

use axum::{http::StatusCode, response::IntoResponse, Json};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("validation: {0}")]
    Validation(String),

    #[error("not found")]
    NotFound,

    #[error("unauthorized")]
    Unauthorized,

    /// Upstream grin node failure. The `&'static str` label is safe to surface;
    /// an untrusted node body is never included.
    #[error("node error: {0}")]
    Node(&'static str),

    #[error("database error")]
    Db(#[from] sqlx::Error),

    /// A route that is deliberately stubbed in this scaffold.
    #[error("{0} not implemented (scaffold)")]
    NotImplemented(&'static str),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::Validation(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::Unauthorized => StatusCode::UNAUTHORIZED,
            Error::Node(_) => StatusCode::BAD_GATEWAY,
            Error::Db(e) => {
                // Log the detail privately; never surface DB internals.
                tracing::error!(error = %e, "database error");
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
        };
        let message = match &self {
            Error::Db(_) => "internal error".to_string(),
            other => other.to_string(),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Validate a 64-char hex string (a `rewind_hash`). Rejects anything else.
pub fn validate_hex64(s: &str, field: &str) -> Result<()> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(Error::Validation(format!("{field} must be 64 hex chars")))
    }
}
