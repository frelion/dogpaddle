use arrow_schema::{ArrowError, DataType};
use datafusion_common::DataFusionError;
use dogpaddle_change::ChangeError;
use dogpaddle_store::StoreError;
use thiserror::Error;

use crate::{
    ExpressionBindError, ExpressionDefinitionError, ExpressionError, operation::OperationError,
};

mod definition;
mod runtime;
mod state;

pub use definition::InnerEquiJoinDefinition;
pub(crate) use definition::{TAG, decode_definition};
pub use runtime::InnerEquiJoinOperation;

/// Failure while constructing a persistent [`InnerEquiJoinDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InnerEquiJoinDefinitionError {
    /// At least one equality key pair is required.
    #[error("inner equi-join requires at least one key pair")]
    EmptyKeys,
    /// A stable Definition count cannot represent all supplied values.
    #[error("inner equi-join definition has too many {kind}")]
    TooMany {
        /// The collection whose count overflowed the stable format.
        kind: &'static str,
    },
    /// An output name cannot fit the stable Definition format.
    #[error("inner equi-join output name {output} is too long")]
    OutputNameTooLong {
        /// Zero-based output name.
        output: usize,
    },
    /// One key expression cannot be persisted canonically.
    #[error("inner equi-join {side} key expression {key} cannot be persisted")]
    KeyExpression {
        /// Zero-based key pair.
        key: usize,
        /// Side containing the rejected expression.
        side: &'static str,
        /// Expression persistence failure.
        #[source]
        source: ExpressionDefinitionError,
    },
    /// Join keys must be immutable because a paged turn evaluates them again after reopen.
    #[error("inner equi-join {side} key expression {key} is not immutable")]
    NonImmutableKey {
        /// Zero-based key pair.
        key: usize,
        /// Side containing the rejected expression.
        side: &'static str,
    },
}

/// Inner equi-join rejection while binding two exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InnerEquiJoinSchemaError {
    /// One key expression cannot bind to its own input Schema.
    #[error("inner equi-join {side} key expression {key} cannot bind")]
    KeyExpression {
        /// Zero-based key pair.
        key: usize,
        /// Side containing the rejected expression.
        side: &'static str,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// Both expressions in one equality pair must have the same exact type.
    #[error("inner equi-join key {key} has different types: left {left}, right {right}")]
    KeyTypeMismatch {
        /// Zero-based key pair.
        key: usize,
        /// Bound left type.
        left: DataType,
        /// Bound right type.
        right: DataType,
    },
    /// v1 admits only flat non-floating equality keys.
    #[error("inner equi-join key {key} has unsupported type {data_type}")]
    UnsupportedKeyType {
        /// Zero-based key pair.
        key: usize,
        /// Rejected exact type.
        data_type: DataType,
    },
    /// One stable name is required for every left field followed by every right field.
    #[error("inner equi-join requires {expected} output names but received {actual}")]
    OutputNameCount {
        /// Exact number of source fields.
        expected: usize,
        /// Supplied name count.
        actual: usize,
    },
}

/// Failure during one [`InnerEquiJoinOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InnerEquiJoinError {
    /// The runtime requires one complete input Change.
    #[error("inner equi-join requires input")]
    MissingInput,
    /// Only the two bound input ports are valid.
    #[error("inner equi-join does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port.
        port: usize,
    },
    /// Runtime input differs from the exact Schema bound for its port.
    #[error("inner equi-join input {port} Schema differs from its bound Schema")]
    InputSchemaMismatch {
        /// Input port whose Schema drifted.
        port: usize,
    },
    /// One bound key expression failed while preparing the Claim.
    #[error("inner equi-join key expression {key} failed for input {port}")]
    KeyExpression {
        /// Offered input port.
        port: usize,
        /// Zero-based key pair.
        key: usize,
        /// Evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// Applying a difference would make one exact input row negative.
    #[error("inner equi-join input would make an exact row weight negative")]
    NegativeWeight,
    /// An exact input row's durable multiplicity cannot represent an adjustment.
    #[error("inner equi-join row weight overflow")]
    WeightOverflow,
    /// Multiplying an input difference by an opposite multiplicity exceeds `i64`.
    #[error("inner equi-join output difference overflow")]
    OutputDifferenceOverflow,
    /// Durable continuation is inconsistent with the pinned input Claim.
    #[error("inner equi-join continuation is invalid: {0}")]
    InvalidContinuation(&'static str),
    /// Exact canonical row encoding or decoding failed.
    #[error("inner equi-join canonical row processing failed")]
    CanonicalRow {
        /// Concrete private row-codec failure.
        #[source]
        source: OperationError,
    },
    /// Durable state could not be accessed or decoded.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A scalar could not be converted to or from Arrow.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    /// Arrow could not construct a Join output batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Join output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

pub(super) const fn key_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}
