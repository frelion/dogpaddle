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
    #[error("store has exhausted its data object identifiers")]
    DataIdExhausted,

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

    /// Persisted multiset bytes violate its positive-multiplicity invariant.
    #[error("multiset is corrupt: {reason}")]
    CorruptMultiset {
        /// Violated invariant.
        reason: &'static str,
    },

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

    /// A persisted fixed subscriber count differs from its owning definition.
    #[error("subscribed log has {actual} subscribers, but {expected} were expected")]
    SubscriberCountMismatch {
        /// Count derived from the owning definition.
        expected: u64,
        /// Count stored with the log.
        actual: u64,
    },

    /// A requested subscriber is outside a log's fixed subscriber set.
    #[error(
        "subscriber {subscriber} is outside subscribed log subscriber count {subscriber_count}"
    )]
    SubscriberOutOfRange {
        /// Requested subscriber identity.
        subscriber: u64,
        /// Fixed number of subscribers in the log.
        subscriber_count: u64,
    },

    /// An acknowledgement does not identify the subscriber's current entry.
    #[error("subscriber {subscriber} expected offset {actual}, not acknowledged offset {expected}")]
    SubscriptionPositionMismatch {
        /// Subscriber being acknowledged.
        subscriber: u64,
        /// Offset supplied by the caller.
        expected: u64,
        /// Subscriber's durable current position.
        actual: u64,
    },

    /// A caught-up subscriber has no entry to acknowledge.
    #[error("subscriber {subscriber} is already caught up at log tail {tail}")]
    SubscriptionAtTail {
        /// Subscriber being acknowledged.
        subscriber: u64,
        /// Current exclusive log tail.
        tail: u64,
    },

    /// A subscribed log has consumed every representable stable offset.
    #[error("subscribed log has exhausted its offset space")]
    SubscribedLogOffsetExhausted,

    /// A subscribed log cannot represent its retained logical byte count.
    #[error("subscribed log has exhausted its retained-byte counter")]
    SubscribedLogRetainedBytesExhausted,

    /// Persisted subscribed-log metadata, positions, and entries disagree.
    #[error("subscribed log is corrupt: {reason}")]
    CorruptSubscribedLog {
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
