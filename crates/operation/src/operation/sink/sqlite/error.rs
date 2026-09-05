use thiserror::Error;

use super::row::RowError;

/// `SQLite`-specific failure while creating, reading, or writing a relation target.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteSinkError {
    /// A fixed batch contains an invalid physical row reference.
    #[error("SQLite sink batch is invalid: {message}")]
    InvalidBatch {
        /// Diagnostic for the rejected batch.
        message: String,
    },
    /// A logical row could not be encoded exactly.
    #[error("SQLite sink row processing failed: {message}")]
    Row {
        /// Diagnostic from the row codec.
        message: String,
    },
    /// The target table or its reserved index already exists on first use.
    #[error("SQLite sink target object {name:?} already exists")]
    TargetExists {
        /// Existing `SQLite` object name.
        name: String,
    },
    /// A previously initialized target table or index is missing.
    #[error("SQLite sink target object {name:?} is missing")]
    TargetMissing {
        /// Missing `SQLite` object name.
        name: String,
    },
    /// A target object differs from the exact layout created by this sink.
    #[error("SQLite sink target object {name:?} has an incompatible layout")]
    TargetLayoutMismatch {
        /// Incompatible `SQLite` object name.
        name: String,
    },
    /// Initialization replay found rows in a table that should still be empty.
    #[error("SQLite sink target table {table:?} is not empty during initialization")]
    TargetNotEmpty {
        /// Target table name.
        table: String,
    },
    /// The target contains an ID outside the sink-owned positive ID range.
    #[error("SQLite sink target contains invalid technical ID {id}")]
    InvalidStoredTechnicalId {
        /// Invalid stored ID.
        id: i64,
    },
    /// `SQLite` rejected connection setup, schema inspection, or row mutation.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub(super) fn invalid_batch(message: impl Into<String>) -> SqliteSinkError {
    SqliteSinkError::InvalidBatch {
        message: message.into(),
    }
}

impl From<RowError> for SqliteSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
