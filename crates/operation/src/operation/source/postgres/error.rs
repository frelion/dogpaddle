use thiserror::Error;

/// Failure in `PostgreSQL` source planning, conversion, recovery, or execution.
///
/// External connection failures report the failed stage, not credentials or
/// raw connection properties. Record failures never include complete row data.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresSourceError {
    /// The non-sensitive source definition is invalid.
    #[error("invalid PostgreSQL source definition: {0}")]
    InvalidDefinition(String),
    /// A record does not satisfy the fixed source contract.
    #[error("invalid PostgreSQL source record: {0}")]
    InvalidRecord(String),
    /// An external resource or live catalog does not satisfy the source contract.
    #[error("PostgreSQL source runtime failed: {0}")]
    InvalidRuntime(String),
    /// Persisted source state is invalid or exceeds its explicit bound.
    #[error("invalid PostgreSQL source state: {0}")]
    InvalidState(&'static str),
    /// A typed Arrow array or batch could not be constructed.
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    /// A converted record batch violates the Change contract.
    #[error(transparent)]
    Change(#[from] dogpaddle_change::ChangeError),
    /// Source state could not be accessed transactionally.
    #[error(transparent)]
    Store(#[from] dogpaddle_store::StoreError),
}

impl PostgresSourceError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self::InvalidRuntime(message.into())
    }
}
