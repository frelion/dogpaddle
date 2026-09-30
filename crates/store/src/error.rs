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

    /// A typed open requested a different collection kind than the catalog records.
    #[error("data object {name:?} is a {actual}, but the requested data class is a {expected}")]
    DataKindMismatch {
        /// Durable data object name.
        name: String,
        /// Kind required by the requested data class.
        expected: &'static str,
        /// Kind recorded by the durable catalog.
        actual: &'static str,
    },

    /// The store marker or catalog metadata is invalid.
    #[error("store marker or catalog metadata is invalid")]
    InvalidStore,

    /// No more durable data object identifiers are available.
    #[error("store has exhausted its data object identifiers while declaring {name:?}")]
    DataIdExhausted {
        /// Full catalog name of the attempted declaration.
        name: String,
    },

    /// A data object belongs to another store.
    #[error("data object belongs to another store")]
    WrongStore,

    /// A scan limit must reserve at least one item and one byte.
    #[error("scan limits must have non-zero item and byte bounds")]
    InvalidScanLimit,

    /// A single encoded item cannot fit in the requested scan batch.
    #[error("encoded item requires {size} bytes but the scan allows {limit}")]
    ItemTooLarge { size: usize, limit: usize },

    /// A multiset adjustment would make a key's multiplicity negative.
    #[error("multiset multiplicity cannot become negative")]
    MultiplicityUnderflow,

    /// A multiset adjustment would exceed the representable multiplicity.
    #[error("multiset multiplicity overflow")]
    MultiplicityOverflow,

    /// A non-empty queue has consumed every representable private sequence number.
    #[error("queue has exhausted its sequence number space")]
    QueueSequenceExhausted,

    /// A queue cannot represent its retained logical byte count.
    #[error("queue has exhausted its queued-byte counter")]
    QueueByteCountExhausted,

    /// Persisted queue metadata and entries violate the FIFO invariants.
    #[error("queue is corrupt: {reason}")]
    CorruptQueue {
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
