use std::path::PathBuf;

use thiserror::Error;

use crate::CodecError;

/// Store declaration, open, and data-access failure.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A data namespace name is invalid.
    #[error("invalid data namespace name {name:?}: {reason}")]
    InvalidName { name: String, reason: &'static str },

    /// A data namespace already uses this name.
    #[error("data namespace {0:?} already exists")]
    DataAlreadyExists(String),

    /// Creation requires an unused path.
    #[error("store path already exists: {0}")]
    PathExists(PathBuf),

    /// Opening requires an existing store directory.
    #[error("store path does not exist or is not a directory: {0}")]
    StoreNotFound(PathBuf),

    /// A data namespace is not present in the durable catalog.
    #[error("data namespace {0:?} does not exist")]
    DataNotFound(String),

    /// The durable store format or catalog metadata is invalid or incompatible.
    #[error("durable store format or catalog metadata is invalid or incompatible")]
    InvalidStore,

    /// No more durable data namespace identifiers are available.
    #[error("store has exhausted its data namespace identifiers")]
    DataIdExhausted,

    /// No more dedicated physical tables are available.
    #[error("store has exhausted its dedicated data tables")]
    DedicatedCapacityExhausted,

    /// A handle belongs to another store.
    #[error("data handle belongs to another store")]
    WrongStore,

    /// A scan limit must reserve at least one item and one byte.
    #[error("scan limits must have non-zero item and byte bounds")]
    InvalidScanLimit,

    /// A single encoded item cannot fit in the requested scan batch.
    #[error("encoded item requires {size} bytes but the scan allows {limit}")]
    ItemTooLarge { size: usize, limit: usize },

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
