use thiserror::Error;

/// Failure in `PostgreSQL` CDC Scan planning, conversion, recovery, or execution.
///
/// External connection failures report the failed stage, not credentials or
/// raw connection properties. Record failures never include complete row data.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresCdcScanError {
    /// The non-sensitive Scan definition is invalid.
    #[error("invalid PostgreSQL CDC scan definition: {0}")]
    InvalidDefinition(String),
    /// A record does not satisfy the fixed Scan contract.
    #[error("invalid PostgreSQL CDC scan record: {0}")]
    InvalidRecord(String),
    /// An external resource or live catalog does not satisfy the Scan contract.
    #[error("PostgreSQL CDC scan runtime failed: {0}")]
    InvalidRuntime(String),
    /// Persisted Scan state is invalid or exceeds its explicit bound.
    #[error("invalid PostgreSQL CDC scan state: {0}")]
    InvalidState(&'static str),
    /// The complete initial snapshot cannot fit its configured private spool.
    #[error("PostgreSQL initial snapshot exceeds bootstrap_spool_bytes")]
    BootstrapSpoolFull,
    /// A typed Arrow array or batch could not be constructed.
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    /// A converted record batch violates the Change contract.
    #[error(transparent)]
    Change(#[from] dogpaddle_change::ChangeError),
    /// A private snapshot Change could not be encoded or decoded.
    #[error(transparent)]
    ChangeCodec(#[from] dogpaddle_change::CodecError),
    /// Scan state could not be accessed transactionally.
    #[error(transparent)]
    Store(#[from] dogpaddle_store::StoreError),
}

impl PostgresCdcScanError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self::InvalidRuntime(message.into())
    }
}
