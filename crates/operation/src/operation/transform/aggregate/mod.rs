use arrow_schema::{ArrowError, DataType};
use datafusion_common::DataFusionError;
use dogpaddle_change::ChangeError;
use dogpaddle_store::{DataScope, StoreError};
use thiserror::Error;

use crate::{ExpressionBindError, ExpressionDefinitionError, ExpressionError};

mod definition;
mod functions;
mod runtime;
mod state;
mod value;

#[cfg(test)]
mod tests;

pub use definition::{AggregateCall, AggregateDefinition};
pub(crate) use definition::{AggregateLayout, TAG, decode_definition};
pub(crate) use runtime::AggregateOperation;

use crate::{OperationSetupError, definition::data, operation::Operation};

fn construct(
    layout: AggregateLayout,
    scope: &mut DataScope<'_>,
    prefix: &str,
) -> Result<Operation, OperationSetupError> {
    let groups = data::<state::Groups>(scope, prefix, definition::GROUPS)?;
    let entries = data::<state::Entries>(scope, prefix, definition::ENTRIES)?;
    let control = data::<state::Control>(scope, prefix, definition::CONTROL)?;
    Ok(Operation::Atomic(Box::new(AggregateOperation {
        input_schema: layout.input_schema,
        output_schema: layout.output_schema,
        group_expressions: layout.group_expressions,
        calls: layout.calls,
        layouts: layout.layouts,
        slots: layout.slots,
        groups,
        entries,
        control,
    })))
}

/// Failure while constructing a persistent [`AggregateDefinition`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AggregateDefinitionError {
    /// Aggregate currently requires at least one grouping expression.
    #[error("aggregate requires at least one GROUP BY expression")]
    EmptyGroupBy,
    /// A stable definition count cannot represent the supplied fields.
    #[error("aggregate definition has too many fields")]
    TooManyFields,
    /// A field name cannot fit the stable definition format.
    #[error("aggregate output field name is too long")]
    FieldNameTooLong,
    /// A grouping expression cannot be persisted canonically.
    #[error("aggregate group expression {group} cannot be persisted")]
    GroupExpression {
        /// Zero-based grouping field.
        group: usize,
        /// Expression persistence failure.
        #[source]
        source: ExpressionDefinitionError,
    },
    /// An aggregate argument cannot be persisted canonically.
    #[error("aggregate call {aggregate} argument cannot be persisted")]
    AggregateExpression {
        /// Zero-based aggregate output field.
        aggregate: usize,
        /// Expression persistence failure.
        #[source]
        source: ExpressionDefinitionError,
    },
}

/// Aggregate-specific rejection while binding an exact input Schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AggregateSchemaError {
    /// One grouping expression cannot bind to the input Schema.
    #[error("aggregate group expression {group} cannot bind")]
    GroupExpression {
        /// Zero-based grouping field.
        group: usize,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// One aggregate argument cannot bind to the input Schema.
    #[error("aggregate call {aggregate} argument cannot bind")]
    AggregateExpression {
        /// Zero-based aggregate output field.
        aggregate: usize,
        /// Expression binding failure.
        #[source]
        source: ExpressionBindError,
    },
    /// Float grouping is deliberately absent until SQL equality is fixed.
    #[error("aggregate GROUP BY does not support floating-point field {group}")]
    FloatGroupKey {
        /// Zero-based grouping field.
        group: usize,
    },
    /// The selected built-in does not support the bound argument type.
    #[error("{function} does not support aggregate argument type {data_type}")]
    UnsupportedArgument {
        /// Stable SQL function name.
        function: &'static str,
        /// Rejected bound type.
        data_type: DataType,
    },
}

/// Failure during one `AggregateOperation` turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AggregateError {
    /// Aggregate only accepts its first input port.
    #[error("aggregate does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port.
        port: usize,
    },
    /// Runtime input differs from the exact bound Schema.
    #[error("aggregate input Schema differs from its bound Schema")]
    InputSchemaMismatch,
    /// A grouping expression failed to evaluate.
    #[error("aggregate group expression {group} evaluation failed")]
    GroupExpression {
        /// Zero-based grouping expression.
        group: usize,
        /// Evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// An aggregate argument failed to evaluate.
    #[error("aggregate call {aggregate} expression evaluation failed")]
    AggregateExpression {
        /// Zero-based aggregate call.
        aggregate: usize,
        /// Evaluation failure.
        #[source]
        source: ExpressionError,
    },
    /// Applying an input difference would make the group's row count negative.
    #[error("aggregate input would make a group row count negative")]
    GroupWeightUnderflow,
    /// Applying an input difference would make one call's non-null count negative.
    #[error("aggregate input would make an aggregate call's non-null count negative")]
    CallWeightUnderflow,
    /// Applying an input difference would make an extrema argument negative.
    #[error("aggregate input would make an extrema argument weight negative")]
    ExtremaWeightUnderflow,
    /// A group identifier cannot be allocated.
    #[error("aggregate group identifiers are exhausted")]
    GroupIdExhausted,
    /// An aggregate result or weight cannot be represented.
    #[error("aggregate arithmetic overflow")]
    ArithmeticOverflow,
    /// Durable aggregate state does not match the bound definition.
    #[error("aggregate durable state is invalid")]
    InvalidState,
    /// Durable state could not be accessed or decoded.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A scalar value could not be converted to or from Arrow.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    /// Arrow could not construct aggregate output.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Aggregate output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}
