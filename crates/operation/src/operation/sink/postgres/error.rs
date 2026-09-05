use arrow_schema::DataType;
use thiserror::Error;

/// Failure while validating a `PostgreSQL` relation sink's bound Arrow Schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum PostgresSinkSchemaError {
    /// The logical columns plus two sink-owned columns exceed `PostgreSQL`'s limit.
    #[error(
        "PostgreSQL sink input has {actual} logical columns, exceeding the maximum of {maximum}"
    )]
    TooManyColumns {
        /// Number of top-level logical columns supplied by the input Schema.
        actual: usize,
        /// Maximum number of supported logical columns.
        maximum: usize,
    },
    /// A field name cannot be represented as a `PostgreSQL` identifier.
    #[error("PostgreSQL sink field {field} has an invalid identifier {name:?}")]
    InvalidFieldName {
        /// Zero-based index of the rejected top-level field.
        field: usize,
        /// Rejected field name.
        name: String,
    },
    /// A logical field collides with a sink-owned technical column.
    #[error("PostgreSQL sink field {field} name {name:?} conflicts with a technical column")]
    TechnicalColumnCollision {
        /// Zero-based index of the rejected top-level field.
        field: usize,
        /// Rejected field name.
        name: String,
    },
    /// A logical field uses an exact reserved `PostgreSQL` system-column name.
    #[error("PostgreSQL sink field {field} name {name:?} conflicts with a system column")]
    SystemColumnCollision {
        /// Zero-based index of the rejected top-level field.
        field: usize,
        /// Reserved system-column name.
        name: String,
    },
    /// Two quoted `PostgreSQL` columns have the same exact name.
    #[error("PostgreSQL sink fields {first} and {second} have the same name {name:?}")]
    DuplicateFieldName {
        /// Zero-based index of the first field.
        first: usize,
        /// Zero-based index of the duplicate field.
        second: usize,
        /// Duplicate name.
        name: String,
    },
    /// A future `DogPaddle` type reached the sink before it had a storage mapping.
    #[error("PostgreSQL sink has no storage mapping for field {field:?} with type {data_type}")]
    UnsupportedType {
        /// Name of the unsupported top-level field.
        field: String,
        /// Unsupported Arrow type.
        data_type: DataType,
    },
}

/// Failure while connecting to or mutating a `PostgreSQL` relation target.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresSinkError {
    /// Runtime connection configuration is malformed.
    #[error("invalid PostgreSQL sink configuration: {message}")]
    InvalidConfig {
        /// Stable validation diagnostic.
        message: String,
    },
    /// A non-secret persistent target specification is malformed.
    #[error("invalid PostgreSQL sink target specification: {message}")]
    InvalidSpec {
        /// Stable validation diagnostic.
        message: String,
    },
    /// Runtime configuration points at another database than the persistent spec.
    #[error("PostgreSQL sink runtime database differs from its persistent target specification")]
    DatabaseMismatch,
    /// The connected `PostgreSQL` cluster or database identity changed.
    #[error("PostgreSQL sink target cluster or database identity changed")]
    TargetIdentityChanged,
    /// The target server cannot provide durable commits.
    #[error("PostgreSQL sink requires fsync and synchronous_commit")]
    DurabilityDisabled,
    /// The target database cannot preserve UTF-8 identifiers exactly.
    #[error("PostgreSQL sink requires a UTF8 server encoding")]
    UnsupportedServerEncoding,
    /// A target object expected to be absent already exists.
    #[error("PostgreSQL sink target object {name:?} already exists")]
    TargetExists {
        /// Existing schema-local object name.
        name: String,
    },
    /// A previously initialized target object is missing.
    #[error("PostgreSQL sink target object {name:?} is missing")]
    TargetMissing {
        /// Missing schema-local object name.
        name: String,
    },
    /// A sink-owned target object fails its ownership or basic layout checks.
    #[error("PostgreSQL sink target object {name:?} has an incompatible layout")]
    TargetLayoutMismatch {
        /// Incompatible schema-local object name.
        name: String,
    },
    /// Initialization replay found target data that cannot belong to initialization.
    #[error("PostgreSQL sink target is not empty during initialization replay")]
    TargetNotEmpty,
    /// A batch is structurally invalid.
    #[error("invalid PostgreSQL sink batch: {message}")]
    InvalidBatch {
        /// Stable validation diagnostic.
        message: String,
    },
    /// A logical Arrow row could not be encoded exactly.
    #[error("PostgreSQL sink row processing failed: {message}")]
    Row {
        /// Stable row diagnostic.
        message: String,
    },
    /// `PostgreSQL` rejected a connection, catalog read, or target transaction.
    #[error("PostgreSQL sink {stage} failed (SQLSTATE {sqlstate})")]
    Database {
        /// Stable operation stage without server-provided detail or credentials.
        stage: &'static str,
        /// Five-character `PostgreSQL` error code, when available.
        sqlstate: String,
    },
    /// A complete connection or database operation exceeded its client deadline.
    #[error("PostgreSQL sink {stage} timed out")]
    Timeout {
        /// Stable operation stage that exceeded its deadline.
        stage: &'static str,
    },
}

pub(super) fn invalid_config(message: impl Into<String>) -> PostgresSinkError {
    PostgresSinkError::InvalidConfig {
        message: message.into(),
    }
}

pub(super) fn invalid_spec(message: impl Into<String>) -> PostgresSinkError {
    PostgresSinkError::InvalidSpec {
        message: message.into(),
    }
}

pub(super) fn invalid_batch(message: impl Into<String>) -> PostgresSinkError {
    PostgresSinkError::InvalidBatch {
        message: message.into(),
    }
}

pub(super) fn database_error(
    stage: &'static str,
    error: &tokio_postgres::Error,
) -> PostgresSinkError {
    PostgresSinkError::Database {
        stage,
        sqlstate: error
            .code()
            .map_or_else(|| "unavailable".to_owned(), |code| code.code().to_owned()),
    }
}

pub(super) const fn timeout(stage: &'static str) -> PostgresSinkError {
    PostgresSinkError::Timeout { stage }
}
