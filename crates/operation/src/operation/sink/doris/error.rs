use arrow_schema::DataType;
use thiserror::Error;

/// Failure while validating an Apache Doris sink's bound Arrow Schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DorisSinkSchemaError {
    /// The input exceeds the conservative Doris column boundary.
    #[error("Doris sink input has {actual} logical columns, exceeding the maximum of {maximum}")]
    TooManyColumns {
        /// Number of logical columns.
        actual: usize,
        /// Maximum supported logical columns.
        maximum: usize,
    },
    /// A field name cannot be represented exactly as a Doris identifier.
    #[error("Doris sink field {field} has an invalid identifier {name:?}")]
    InvalidFieldName {
        /// Zero-based field index.
        field: usize,
        /// Rejected field name.
        name: String,
    },
    /// A field collides with a sink-owned column.
    #[error("Doris sink field {field} name {name:?} conflicts with a technical column")]
    TechnicalColumnCollision {
        /// Zero-based field index.
        field: usize,
        /// Rejected field name.
        name: String,
    },
    /// Two fields have the same exact name.
    #[error("Doris sink fields {first} and {second} have the same name {name:?}")]
    DuplicateFieldName {
        /// First field index.
        first: usize,
        /// Duplicate field index.
        second: usize,
        /// Duplicate name.
        name: String,
    },
    /// A future `DogPaddle` type has no Doris representation.
    #[error("Doris sink has no storage mapping for field {field:?} with type {data_type}")]
    UnsupportedType {
        /// Field name.
        field: String,
        /// Unsupported Arrow type.
        data_type: DataType,
    },
}

/// Failure while connecting to or mutating an Apache Doris target.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DorisSinkError {
    /// Runtime connection configuration is malformed.
    #[error("invalid Doris sink configuration: {message}")]
    InvalidConfig {
        /// Stable validation diagnostic.
        message: String,
    },
    /// Persistent target identity is malformed.
    #[error("invalid Doris sink target specification: {message}")]
    InvalidSpec {
        /// Stable validation diagnostic.
        message: String,
    },
    /// Runtime database differs from the definition.
    #[error("Doris sink runtime database differs from its persistent target specification")]
    DatabaseMismatch,
    /// The connected Doris cluster changed.
    #[error("Doris sink target cluster identity changed")]
    TargetIdentityChanged,
    /// A target object expected to be absent exists.
    #[error("Doris sink target object {name:?} already exists")]
    TargetExists {
        /// Existing database-local object.
        name: String,
    },
    /// A target object is missing.
    #[error("Doris sink target object {name:?} is missing")]
    TargetMissing {
        /// Missing database-local object.
        name: String,
    },
    /// A sink-owned object has an incompatible layout.
    #[error("Doris sink target object {name:?} has an incompatible layout")]
    TargetLayoutMismatch {
        /// Incompatible object.
        name: String,
    },
    /// Initialization replay found target data.
    #[error("Doris sink target is not empty during initialization replay")]
    TargetNotEmpty,
    /// A relation batch is malformed.
    #[error("invalid Doris sink batch: {message}")]
    InvalidBatch {
        /// Stable validation diagnostic.
        message: String,
    },
    /// An Arrow row cannot be encoded exactly.
    #[error("Doris sink row processing failed: {message}")]
    Row {
        /// Stable row diagnostic.
        message: String,
    },
    /// Doris rejected a database operation. Server text is deliberately omitted.
    #[error("Doris sink {stage} failed")]
    Database {
        /// Stable operation stage.
        stage: &'static str,
    },
}

pub(super) fn invalid_config(message: impl Into<String>) -> DorisSinkError {
    DorisSinkError::InvalidConfig {
        message: message.into(),
    }
}

pub(super) fn invalid_spec(message: impl Into<String>) -> DorisSinkError {
    DorisSinkError::InvalidSpec {
        message: message.into(),
    }
}

pub(super) fn invalid_batch(message: impl Into<String>) -> DorisSinkError {
    DorisSinkError::InvalidBatch {
        message: message.into(),
    }
}

pub(super) const fn database(stage: &'static str) -> DorisSinkError {
    DorisSinkError::Database { stage }
}
