use arrow_schema::SchemaRef;
use thiserror::Error;

use super::{row::RowError, state::PendingStateCodecError};

/// SQLiteSink-specific failure during one operation turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteSinkError {
    /// The sink was called without an input Change.
    #[error("SQLite sink requires one input Change")]
    MissingInput,
    /// `SQLiteSink` only accepts its definition's first input port.
    #[error("SQLite sink does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// A direct caller supplied a Schema different from the bound Schema.
    #[error("SQLite sink input Schema differs from its bound Schema")]
    InputSchemaMismatch {
        /// Exact Schema fixed during binding.
        expected: SchemaRef,
        /// Schema supplied by this turn.
        actual: SchemaRef,
    },
    /// The persistent next-ID and pending-state cells contradict each other.
    #[error("SQLite sink persistent state is invalid: {message}")]
    InvalidState {
        /// Stable diagnostic for the rejected state.
        message: String,
    },
    /// A retained Change does not agree with its persistent batch position.
    #[error("SQLite sink pending batch does not match its retained Change: {message}")]
    PendingInputMismatch {
        /// Stable diagnostic for the mismatch.
        message: String,
    },
    /// A logical row could not be encoded exactly.
    #[error("SQLite sink row processing failed: {message}")]
    Row {
        /// Stable diagnostic from the private row codec.
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
    /// The target contains an ID not below the durable next-ID frontier.
    #[error("SQLite sink target technical ID {id} is not below next ID {next_id}")]
    TechnicalIdFrontierMismatch {
        /// Observed target ID.
        id: u64,
        /// Durable next unallocated ID.
        next_id: u64,
    },
    /// A positive multiplicity cannot fit in the remaining technical-ID space.
    #[error("SQLite sink technical IDs are exhausted at {next_id}; {needed} IDs are required")]
    TechnicalIdExhausted {
        /// Current next unallocated ID.
        next_id: u64,
        /// Complete remaining multiplicity that must be admitted atomically.
        needed: u64,
    },
    /// A negative multiplicity has fewer exact physical rows than required.
    #[error(
        "SQLite sink cannot retract row {row_index}: {needed} instances are required but only {available} exist"
    )]
    MissingRetraction {
        /// Change row containing the invalid negative difference.
        row_index: u64,
        /// Complete remaining multiplicity requested by the event.
        needed: u64,
        /// Exact matching instances visible after earlier mutations in the batch.
        available: u64,
    },
    /// An idempotent insert found the same ID attached to another logical row.
    #[error("SQLite sink technical ID {id} belongs to a different logical row")]
    TechnicalIdConflict {
        /// Conflicting technical ID.
        id: u64,
    },
    /// A prepared delete found its ID attached to another logical row.
    #[error("SQLite sink delete ID {id} belongs to a different logical row")]
    DeleteRowMismatch {
        /// Mismatched technical ID.
        id: u64,
    },
    /// `SQLite` returned a mutation count impossible for a primary-key operation.
    #[error("SQLite sink {operation} for ID {id} changed {actual} rows, expected {expected}")]
    UnexpectedMutationCount {
        /// Operation being checked.
        operation: &'static str,
        /// Technical ID used by the mutation.
        id: u64,
        /// Required affected-row count.
        expected: usize,
        /// Reported affected-row count.
        actual: usize,
    },
    /// `SQLite` rejected connection setup, schema inspection, or row mutation.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub(super) fn invalid_state(message: impl Into<String>) -> SqliteSinkError {
    SqliteSinkError::InvalidState {
        message: message.into(),
    }
}

pub(super) fn pending_mismatch(message: impl Into<String>) -> SqliteSinkError {
    SqliteSinkError::PendingInputMismatch {
        message: message.into(),
    }
}

impl From<PendingStateCodecError> for SqliteSinkError {
    fn from(error: PendingStateCodecError) -> Self {
        invalid_state(error.to_string())
    }
}

impl From<RowError> for SqliteSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
