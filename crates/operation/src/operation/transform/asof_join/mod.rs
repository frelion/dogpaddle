//! Dynamic ASOF joins over equality partitions and ordered right candidates.

use arrow_schema::{ArrowError, DataType};
use datafusion_common::DataFusionError;
use dogpaddle_change::ChangeError;
use dogpaddle_store::{DataScope, StoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ExpressionBindError, ExpressionDefinitionError, ExpressionError, operation::OperationError,
};

mod definition;
mod index;
mod runtime;
pub(crate) mod state;

pub(crate) use definition::AsOfJoinLayout;
pub use definition::{AsOfEqualityKey, AsOfJoinDefinition, AsOfOrderKey};
pub(crate) use runtime::AsOfJoinOperation;

use crate::{OperationSetupError, operation::Operation};

fn construct(
    layout: AsOfJoinLayout,
    scope: &mut DataScope<'_>,
) -> Result<Operation, OperationSetupError> {
    let left_rows = scope.data::<state::Rows>(definition::LEFT_ROWS)?;
    let right_rows = scope.data::<state::Rows>(definition::RIGHT_ROWS)?;
    Ok(Operation::Paged(Box::new(AsOfJoinOperation {
        layout,
        left_rows,
        right_rows,
    })))
}

/// Ordered SQL ASOF candidate search direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum AsOfDirection {
    /// Selects the greatest eligible right value before the left value.
    Backward {
        /// Whether equal order values are eligible.
        allow_exact: bool,
    },
    /// Selects the least eligible right value after the left value.
    Forward {
        /// Whether equal order values are eligible.
        allow_exact: bool,
    },
}
impl AsOfDirection {
    /// Returns whether equal values are eligible.
    #[must_use]
    pub const fn allow_exact(self) -> bool {
        match self {
            Self::Backward { allow_exact } | Self::Forward { allow_exact } => allow_exact,
        }
    }
}

/// Failure while constructing a persistent [`AsOfJoinDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AsOfJoinDefinitionError {
    /// A stable Definition count cannot represent all supplied values.
    #[error("ASOF join definition has too many {kind}")]
    TooMany {
        /// Collection whose count overflowed the stable format.
        kind: &'static str,
    },
    /// An output name cannot fit the stable Definition format.
    #[error("ASOF join output name {output} is too long")]
    OutputNameTooLong {
        /// Zero-based output name.
        output: usize,
    },
    /// One expression cannot be persisted canonically.
    #[error("ASOF join {role} expression {index} cannot be persisted")]
    Expression {
        /// Expression collection.
        role: &'static str,
        /// Zero-based expression or pair index.
        index: usize,
        /// Expression persistence failure.
        #[source]
        source: ExpressionDefinitionError,
    },
}

/// ASOF join rejection while binding two exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AsOfJoinSchemaError {
    /// One expression cannot bind to its input Schema.
    #[error("ASOF join {side} {role} expression {index} cannot bind")]
    Expression {
        /// Expression collection.
        role: &'static str,
        /// Zero-based expression or pair index.
        index: usize,
        /// Input containing the expression.
        side: &'static str,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// Both expressions in one pair must have the same exact type.
    #[error("ASOF join {role} key {index} has different types: left {left}, right {right}")]
    TypeMismatch {
        /// Expression collection.
        role: &'static str,
        /// Zero-based pair index.
        index: usize,
        /// Bound left type.
        left: DataType,
        /// Bound right type.
        right: DataType,
    },
    /// The bound scalar cannot be represented in the stable ordered index.
    #[error("ASOF join {role} key {index} has unsupported type {data_type}")]
    UnsupportedType {
        /// Expression collection.
        role: &'static str,
        /// Zero-based key index.
        index: usize,
        /// Rejected exact type.
        data_type: DataType,
    },
    /// One stable name is required for every emitted field.
    #[error("ASOF join requires {expected} output names but received {actual}")]
    OutputNameCount {
        /// Exact field count emitted by this join kind.
        expected: usize,
        /// Supplied name count.
        actual: usize,
    },
}

/// Failure during one `AsOfJoinOperation` step.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AsOfJoinError {
    /// Only the two bound input ports are valid.
    #[error("ASOF join does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port.
        port: usize,
    },
    /// Runtime input differs from its exact bound Schema.
    #[error("ASOF join input {port} Schema differs from its bound Schema")]
    InputSchemaMismatch {
        /// Input port whose Schema drifted.
        port: usize,
    },
    /// A bound expression failed while preparing the input.
    #[error("ASOF join {role} expression {index} failed for input {port}")]
    Expression {
        /// Expression collection.
        role: &'static str,
        /// Zero-based expression index.
        index: usize,
        /// Offered input port.
        port: usize,
        /// Expression evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// Applying a difference would make an exact input row negative.
    #[error("ASOF join input would make an exact row weight negative")]
    NegativeWeight,
    /// An exact input row multiplicity cannot represent an adjustment.
    #[error("ASOF join row weight overflow")]
    WeightOverflow,
    /// An output difference exceeds `i64`.
    #[error("ASOF join output difference overflow")]
    OutputDifferenceOverflow,
    /// More than one distinct row shares an exposed winning order value.
    #[error("ASOF join winning right order value is ambiguous")]
    AmbiguousTie,
    /// Durable index state is malformed or inconsistent.
    #[error("ASOF join index is invalid: {0}")]
    InvalidIndex(&'static str),
    /// Opaque resume is inconsistent with the immutable input.
    #[error("ASOF join resume is invalid: {0}")]
    InvalidResume(&'static str),
    /// Canonical row processing failed.
    #[error("ASOF join canonical row processing failed")]
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
    /// Arrow could not construct an output batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Join output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}
