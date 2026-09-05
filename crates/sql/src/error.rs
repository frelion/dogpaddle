use std::{io, path::PathBuf};

use datafusion_common::DataFusionError;
use datafusion_sql::sqlparser::parser::ParserError;
use dogpaddle_flow::FlowError;
use thiserror::Error;

/// Failure while parsing, building, or opening a SQL program.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqlError {
    /// The SQL text is syntactically invalid.
    #[error(transparent)]
    Parse(#[from] ParserError),
    /// The file containing the SQL program could not be read.
    #[error("failed to read SQL file {path:?}: {source}")]
    Read {
        /// Path supplied by the caller.
        path: PathBuf,
        /// Underlying file-system error.
        #[source]
        source: io::Error,
    },
    /// The statement or one of its endpoint declarations is invalid.
    #[error("invalid SQL program: {0}")]
    Invalid(String),
    /// An environment-backed endpoint parameter is unavailable.
    #[error("environment variable {name:?} is unavailable or is not valid UTF-8")]
    Environment {
        /// Referenced environment variable.
        name: String,
    },
    /// `DataFusion` could not plan or type-check the query.
    #[error("SQL query planning failed: {0}")]
    Planning(#[from] DataFusionError),
    /// An endpoint could not be discovered or represented by an Operation.
    #[error("SQL endpoint setup failed: {0}")]
    Endpoint(String),
    /// `DataFusion` produced a logical node outside the streaming SQL subset.
    #[error("unsupported SQL query: {0}")]
    Unsupported(String),
    /// The underlying Flow could not be built or opened.
    #[error(transparent)]
    Flow(#[from] FlowError),
}

impl SqlError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub(crate) fn endpoint(error: impl std::fmt::Display) -> Self {
        Self::Endpoint(error.to_string())
    }
}
