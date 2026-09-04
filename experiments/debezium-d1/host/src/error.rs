use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum HostError {
    #[error("{0}")]
    Usage(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Debezium runtime error: {0}")]
    Debezium(#[from] dogpaddle_debezium::Error),
}
