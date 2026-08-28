use std::path::PathBuf;

use thiserror::Error;

use crate::CodecError;

/// Store declaration, open, and data-access failure.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A data object name is invalid.
    #[error("invalid data object name {name:?}: {reason}")]
    InvalidName { name: String, reason: &'static str },

    /// A data object already uses this name.
    #[error("data object {0:?} already exists")]
    DataAlreadyExists(String),

    /// Creation requires an unused path.
    #[error("store path already exists: {0}")]
    PathExists(PathBuf),

    /// Opening requires an existing store directory.
    #[error("store path does not exist or is not a directory: {0}")]
    StoreNotFound(PathBuf),

    /// A data object is not present in the durable catalog.
    #[error("data object {0:?} does not exist")]
    DataNotFound(String),

    /// A typed open requested a different durable size than the catalog records.
    #[error(
        "data object {name:?} has size {actual}, but the requested data class requires {expected}"
    )]
    DataSizeMismatch {
        /// Durable data object name.
        name: String,
        /// Size required by the requested data class.
        expected: &'static str,
        /// Size recorded by the durable catalog.
        actual: &'static str,
    },

    /// The store marker or catalog metadata is invalid.
    #[error("store marker or catalog metadata is invalid")]
    InvalidStore,

    /// No more durable data object identifiers are available.
    #[error("store has exhausted its data object identifiers")]
    DataIdExhausted,

    /// No more physical tables are available for large data objects.
    #[error("store has exhausted its large-data capacity")]
    LargeDataCapacityExhausted,

    /// A data object belongs to another store.
    #[error("data object belongs to another store")]
    WrongStore,

    /// Transaction-bound values from different transactions were mixed.
    #[error("data access belongs to another transaction")]
    WrongTransaction,

    /// A scan limit must reserve at least one item and one byte.
    #[error("scan limits must have non-zero item and byte bounds")]
    InvalidScanLimit,

    /// A single encoded item cannot fit in the requested scan batch.
    #[error("encoded item requires {size} bytes but the scan allows {limit}")]
    ItemTooLarge { size: usize, limit: usize },

    /// An append-log cursor or truncation target is outside its valid range.
    #[error("append-log offset {offset} is outside valid range [{head}, {tail}]")]
    LogOffsetOutOfRange {
        /// Requested offset.
        offset: u64,
        /// First retained offset.
        head: u64,
        /// Next append offset.
        tail: u64,
    },

    /// An append log has consumed every representable offset.
    #[error("append log has exhausted its offset space")]
    LogOffsetExhausted,

    /// An append log cannot represent its retained encoded-byte count.
    #[error("append log has exhausted its retained-byte counter")]
    LogRetainedBytesExhausted,

    /// Persisted append-log metadata and entries violate the log invariants.
    #[error("append log is corrupt: {reason}")]
    CorruptAppendLog {
        /// Violated invariant.
        reason: &'static str,
    },

    /// Typed encoding or decoding failed.
    #[error("codec failure: {0}")]
    Codec(#[from] CodecError),

    /// The underlying storage engine failed.
    #[error("store failed during {operation}: {message}")]
    Storage {
        operation: &'static str,
        message: String,
    },

    /// This transaction previously encountered a hard failure.
    #[error("transaction is poisoned after a prior hard failure")]
    TransactionPoisoned,
}

impl StoreError {
    pub(crate) fn storage(operation: &'static str, error: impl std::fmt::Display) -> Self {
        Self::Storage {
            operation,
            message: error.to_string(),
        }
    }

    pub(crate) fn poisons_transaction(&self) -> bool {
        !matches!(self, Self::ItemTooLarge { .. })
    }
}
