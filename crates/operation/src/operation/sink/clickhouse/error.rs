use arrow_schema::DataType;
use thiserror::Error;

/// Failure while validating a `ClickHouse` sink's bound Arrow Schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ClickHouseSinkSchemaError {
    /// The input exceeds the conservative column boundary.
    #[error(
        "ClickHouse sink input has {actual} logical columns, exceeding the maximum of {maximum}"
    )]
    TooManyColumns {
        /// Number of logical columns.
        actual: usize,
        /// Maximum supported columns.
        maximum: usize,
    },
    /// A field name is not representable.
    #[error("ClickHouse sink field {field} has an invalid identifier {name:?}")]
    InvalidFieldName {
        /// Zero-based field index.
        field: usize,
        /// Rejected name.
        name: String,
    },
    /// A field collides with a technical column.
    #[error("ClickHouse sink field {field} name {name:?} conflicts with a technical column")]
    TechnicalColumnCollision {
        /// Zero-based field index.
        field: usize,
        /// Rejected name.
        name: String,
    },
    /// Two fields have the same name.
    #[error("ClickHouse sink fields {first} and {second} have the same name {name:?}")]
    DuplicateFieldName {
        /// First index.
        first: usize,
        /// Duplicate index.
        second: usize,
        /// Duplicate name.
        name: String,
    },
    /// A future type has no target mapping.
    #[error("ClickHouse sink has no storage mapping for field {field:?} with type {data_type}")]
    UnsupportedType {
        /// Field name.
        field: String,
        /// Unsupported type.
        data_type: DataType,
    },
}

/// Failure while connecting to or mutating a `ClickHouse` target.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClickHouseSinkError {
    /// Runtime configuration is malformed.
    #[error("invalid ClickHouse sink configuration: {message}")]
    InvalidConfig {
        /// Stable diagnostic.
        message: String,
    },
    /// Persistent target specification is malformed.
    #[error("invalid ClickHouse sink target specification: {message}")]
    InvalidSpec {
        /// Stable diagnostic.
        message: String,
    },
    /// Runtime database differs from the definition.
    #[error("ClickHouse sink runtime database differs from its persistent target specification")]
    DatabaseMismatch,
    /// The database identity changed.
    #[error("ClickHouse sink target database identity changed")]
    TargetIdentityChanged,
    /// An object expected absent exists.
    #[error("ClickHouse sink target object {name:?} already exists")]
    TargetExists {
        /// Existing object.
        name: String,
    },
    /// An expected object is missing.
    #[error("ClickHouse sink target object {name:?} is missing")]
    TargetMissing {
        /// Missing object.
        name: String,
    },
    /// An owned object has an incompatible layout.
    #[error("ClickHouse sink target object {name:?} has an incompatible layout")]
    TargetLayoutMismatch {
        /// Incompatible object.
        name: String,
    },
    /// Initialization replay found data.
    #[error("ClickHouse sink target is not empty during initialization replay")]
    TargetNotEmpty,
    /// A relation batch is malformed.
    #[error("invalid ClickHouse sink batch: {message}")]
    InvalidBatch {
        /// Stable diagnostic.
        message: String,
    },
    /// A logical row cannot be encoded exactly.
    #[error("ClickHouse sink row processing failed: {message}")]
    Row {
        /// Stable diagnostic.
        message: String,
    },
    /// HTTP or database execution failed; remote text is deliberately omitted.
    #[error("ClickHouse sink {stage} failed")]
    Database {
        /// Stable stage.
        stage: &'static str,
    },
    /// A bounded response was invalid or too large.
    #[error("ClickHouse sink {stage} returned an invalid response")]
    InvalidResponse {
        /// Stable stage.
        stage: &'static str,
    },
}

pub(super) fn invalid_config(message: impl Into<String>) -> ClickHouseSinkError {
    ClickHouseSinkError::InvalidConfig {
        message: message.into(),
    }
}

pub(super) fn invalid_spec(message: impl Into<String>) -> ClickHouseSinkError {
    ClickHouseSinkError::InvalidSpec {
        message: message.into(),
    }
}

pub(super) fn invalid_batch(message: impl Into<String>) -> ClickHouseSinkError {
    ClickHouseSinkError::InvalidBatch {
        message: message.into(),
    }
}

pub(super) const fn database(stage: &'static str) -> ClickHouseSinkError {
    ClickHouseSinkError::Database { stage }
}

pub(super) const fn invalid_response(stage: &'static str) -> ClickHouseSinkError {
    ClickHouseSinkError::InvalidResponse { stage }
}
