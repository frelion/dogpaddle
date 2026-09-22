//! Dynamic ASOF joins over equality partitions and ordered right candidates.

use arrow_schema::{ArrowError, DataType};
use datafusion_common::DataFusionError;
use dogpaddle_change::ChangeError;
use dogpaddle_store::{DataScope, StoreError};
use thiserror::Error;

use crate::{
    ExpressionBindError, ExpressionDefinitionError, ExpressionError, operation::OperationError,
};

mod definition;
mod index;
mod runtime;
mod state;

pub use definition::{AsOfEqualityKey, AsOfJoinDefinition, AsOfOrderKey, AsOfTieBreak};
pub(crate) use definition::{AsOfJoinLayout, TAG, decode_definition};
pub(crate) use runtime::AsOfJoinOperation;

use crate::{OperationSetupError, operation::Operation};

fn construct(
    layout: AsOfJoinLayout,
    scope: &mut DataScope<'_>,
) -> Result<Operation, OperationSetupError> {
    let left_rows = scope.data::<state::Rows>(definition::LEFT_ROWS)?;
    let right_rows = scope.data::<state::Rows>(definition::RIGHT_ROWS)?;
    let continuation = scope.data::<state::Continuation>(definition::CONTINUATION)?;
    Ok(Operation::Turn(Box::new(AsOfJoinOperation {
        kind: layout.kind,
        direction: layout.direction,
        tie_fallback: layout.tie_fallback,
        tolerance: layout.tolerance,
        input_schemas: layout.input_schemas,
        candidate_schema: layout.candidate_schema,
        output_schema: layout.output_schema,
        equalities: layout.equalities,
        orders: layout.orders,
        ties: layout.ties,
        right_nulls: layout.right_nulls,
        residual: layout.residual,
        left_rows,
        right_rows,
        continuation,
        prepared: None,
    })))
}

/// Relational output semantics of an ASOF join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfJoinKind {
    /// Emits each left row with its selected right row, when one exists.
    Inner,
    /// Retains an unmatched left row and fills every right field with NULL.
    LeftOuter,
    /// Emits a left row exactly when it selects a right row.
    LeftSemi,
    /// Emits a left row exactly when it does not select a right row.
    LeftAnti,
}

impl AsOfJoinKind {
    pub(super) const fn left_only(self) -> bool {
        matches!(self, Self::LeftSemi | Self::LeftAnti)
    }

    pub(super) const fn code(self) -> u8 {
        match self {
            Self::Inner => 0,
            Self::LeftOuter => 1,
            Self::LeftSemi => 2,
            Self::LeftAnti => 3,
        }
    }

    pub(super) const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Inner),
            1 => Some(Self::LeftOuter),
            2 => Some(Self::LeftSemi),
            3 => Some(Self::LeftAnti),
            _ => None,
        }
    }
}

/// Which candidate wins when nearest neighbors are equally distant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfEquidistantPreference {
    /// Selects the candidate before the left order value.
    Backward,
    /// Selects the candidate after the left order value.
    Forward,
}

/// Ordered candidate-search strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfDirection {
    /// Selects the greatest eligible right value before the left value.
    Backward {
        /// Whether an equal order value is eligible.
        allow_exact: bool,
    },
    /// Selects the least eligible right value after the left value.
    Forward {
        /// Whether an equal order value is eligible.
        allow_exact: bool,
    },
    /// Selects the right order value with the smallest absolute distance.
    Nearest {
        /// Whether an equal order value is eligible.
        allow_exact: bool,
        /// Deterministic winner for equal predecessor and successor distances.
        equidistant: AsOfEquidistantPreference,
    },
}

impl AsOfDirection {
    /// Returns whether an exactly equal order value is eligible.
    #[must_use]
    pub const fn allow_exact(self) -> bool {
        match self {
            Self::Backward { allow_exact }
            | Self::Forward { allow_exact }
            | Self::Nearest { allow_exact, .. } => allow_exact,
        }
    }

    pub(super) const fn code(self) -> u8 {
        match self {
            Self::Backward { .. } => 0,
            Self::Forward { .. } => 1,
            Self::Nearest {
                equidistant: AsOfEquidistantPreference::Backward,
                ..
            } => 2,
            Self::Nearest {
                equidistant: AsOfEquidistantPreference::Forward,
                ..
            } => 3,
        }
    }

    pub(super) const fn from_code(code: u8, allow_exact: bool) -> Option<Self> {
        match code {
            0 => Some(Self::Backward { allow_exact }),
            1 => Some(Self::Forward { allow_exact }),
            2 => Some(Self::Nearest {
                allow_exact,
                equidistant: AsOfEquidistantPreference::Backward,
            }),
            3 => Some(Self::Nearest {
                allow_exact,
                equidistant: AsOfEquidistantPreference::Forward,
            }),
            _ => None,
        }
    }

    pub(super) const fn nearest(self) -> bool {
        matches!(self, Self::Nearest { .. })
    }
}

/// NULL comparison semantics for one equality partition key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfEqualityMode {
    /// SQL equality: a NULL on either side makes the key ineligible.
    Equal,
    /// SQL `IS NOT DISTINCT FROM`: two NULL values are equal.
    NotDistinct,
}

impl AsOfEqualityMode {
    pub(super) const fn code(self) -> u8 {
        match self {
            Self::Equal => 0,
            Self::NotDistinct => 1,
        }
    }

    pub(super) const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Equal),
            1 => Some(Self::NotDistinct),
            _ => None,
        }
    }
}

/// Behavior when explicit right tie-break expressions do not identify one row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfTieFallback {
    /// Rejects an ambiguous right candidate set at runtime.
    Reject,
    /// Uses the exact canonical right row in ascending byte order.
    CanonicalAscending,
    /// Uses the exact canonical right row in descending byte order.
    CanonicalDescending,
}

impl AsOfTieFallback {
    pub(super) const fn code(self) -> u8 {
        match self {
            Self::Reject => 0,
            Self::CanonicalAscending => 1,
            Self::CanonicalDescending => 2,
        }
    }

    pub(super) const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Reject),
            1 => Some(Self::CanonicalAscending),
            2 => Some(Self::CanonicalDescending),
            _ => None,
        }
    }
}

/// Failure while constructing a persistent [`AsOfJoinDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AsOfJoinDefinitionError {
    /// At least one ordered key pair is required.
    #[error("ASOF join requires at least one order key pair")]
    EmptyOrderKeys,
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
    /// The candidate-eligibility predicate cannot be persisted canonically.
    #[error("ASOF join residual predicate cannot be persisted")]
    ResidualExpression {
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
    /// The persistent residual cannot bind to the exact candidate-pair Schema.
    #[error("ASOF join residual predicate cannot bind")]
    ResidualExpression {
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// A residual predicate must produce Boolean.
    #[error("ASOF join residual predicate must produce Boolean, found {actual}")]
    ResidualType {
        /// Actual expression result type.
        actual: DataType,
    },
    /// Nearest search and bounded tolerance require one distance-capable order key.
    #[error("ASOF join {feature} requires exactly one distance-capable order key")]
    DistanceOrder {
        /// Feature requiring a scalar distance.
        feature: &'static str,
    },
    /// One stable name is required for every emitted field.
    #[error("ASOF join requires {expected} output names but received {actual}")]
    OutputNameCount {
        /// Exact field count emitted by this join kind.
        expected: usize,
        /// Supplied name count.
        actual: usize,
    },
    /// A right input field cannot be represented as typed NULL padding.
    #[error("ASOF join cannot construct NULL padding")]
    NullPadding(#[source] DataFusionError),
}

/// Failure during one `AsOfJoinOperation` turn.
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
    /// The exact candidate-pair residual failed during evaluation.
    #[error("ASOF join residual predicate evaluation failed")]
    ResidualExpression {
        /// Predicate evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// The residual reported Boolean but did not produce Arrow's canonical Boolean array.
    #[error("ASOF join residual predicate did not produce a canonical Boolean Arrow array")]
    ResidualArray,
    /// Applying a difference would make an exact input row negative.
    #[error("ASOF join input would make an exact row weight negative")]
    NegativeWeight,
    /// An exact input row multiplicity cannot represent an adjustment.
    #[error("ASOF join row weight overflow")]
    WeightOverflow,
    /// An output difference exceeds `i64`.
    #[error("ASOF join output difference overflow")]
    OutputDifferenceOverflow,
    /// Explicit tie-breaks do not identify one right row.
    #[error("ASOF join right candidates remain ambiguous after explicit tie-breaks")]
    AmbiguousTie,
    /// Preparing the pinned input would retain an excessive index working set.
    #[error("ASOF join prepared Claim exceeds its {max_bytes}-byte limit")]
    PreparedClaimTooLarge {
        /// Maximum retained preparation working set.
        max_bytes: usize,
    },
    /// Durable index state is malformed or inconsistent.
    #[error("ASOF join index is invalid: {0}")]
    InvalidIndex(&'static str),
    /// Durable continuation is inconsistent with the pinned input Claim.
    #[error("ASOF join continuation is invalid: {0}")]
    InvalidContinuation(&'static str),
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
