use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchOptions, make_array};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use arrow_select::filter::FilterBuilder;
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::TransactionAccess;
use thiserror::Error;

use crate::{
    DataDeclaration, DefinitionCodecError, Expr, ExpressionBindError, ExpressionDefinitionError,
    ExpressionError, OperationBinding, OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::Sealed as SealedDefinition,
    expression::{BoundExpression, StoredExpression},
    operation::{Action, OperationError, OperationInput, TransactionalOperation},
};

pub(crate) const TAG: u16 = 5;
const DATA: &[DataDeclaration] = &[];

/// Pure definition of an order-preserving row filter.
///
/// The predicate is bound to the exact logical input Schema and must produce a
/// Boolean. A row is retained only when its predicate is non-null `true`;
/// `false` and null both remove the row. Filtering never changes differences.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterDefinition {
    predicate: StoredExpression,
}

/// Materialized exact-Schema-bound row filter.
///
/// This value owns only its compiled predicate and no persistent Store data.
pub struct FilterOperation {
    predicate: BoundExpression,
}

/// Filter-specific failure while binding exact input Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FilterSchemaError {
    /// The persistent predicate cannot bind to the input Schema.
    #[error(transparent)]
    Expression(#[from] ExpressionBindError),
    /// A filter predicate must produce a Boolean value.
    #[error("filter predicate must produce Boolean, found {actual}")]
    PredicateType {
        /// Actual expression result type.
        actual: DataType,
    },
}

/// Filter-specific failure during one [`FilterOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FilterError {
    /// The input Operation was called without a Change.
    #[error("filter requires one input Change")]
    MissingInput,
    /// Filter only accepts its Definition's first input port.
    #[error("filter does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// Predicate evaluation failed.
    #[error(transparent)]
    Expression(#[from] ExpressionError),
    /// The predicate reported Boolean but did not use Arrow's canonical Boolean array.
    #[error("filter predicate did not produce a canonical Boolean Arrow array")]
    PredicateArray,
    /// Arrow could not construct or filter a canonical record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Arrow returned a non-Int64 difference array after filtering Int64 differences.
    #[error("filter produced a non-Int64 difference array")]
    DifferenceArray,
    /// The filtered output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

impl FilterDefinition {
    /// Admits a `DataFusion` scalar expression as a persistent row predicate.
    ///
    /// The expression is immediately round-tripped through `DataFusion`'s
    /// protobuf codec so Definition encoding cannot fail later.
    ///
    /// # Errors
    ///
    /// Returns [`ExpressionDefinitionError`] when `DataFusion` cannot encode and
    /// decode the expression exactly and canonically.
    pub fn try_new(predicate: Expr) -> Result<Self, ExpressionDefinitionError> {
        StoredExpression::try_new(predicate).map(|predicate| Self { predicate })
    }

    /// Returns the canonical `DataFusion` predicate admitted by this definition.
    #[must_use]
    pub fn predicate(&self) -> &Expr {
        self.predicate.expression()
    }
}

impl SealedDefinition for FilterDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Filter input arity");
        let predicate = self.predicate.bind(Arc::clone(input_schema)).map_err(
            |source| -> OperationSchemaError { Box::new(FilterSchemaError::Expression(source)) },
        )?;
        if predicate.output_type() != &DataType::Boolean {
            return Err(Box::new(FilterSchemaError::PredicateType {
                actual: predicate.output_type().clone(),
            }));
        }
        Ok(OperationBinding::without_data(
            Some(Arc::clone(input_schema)),
            FilterOperation { predicate },
        ))
    }
}

impl OperationDefinition for FilterDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Transform(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        self.predicate.encode(output);
    }
}

impl TransactionalOperation for FilterOperation {
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(FilterError::MissingInput)?;
        if input.port != 0 {
            return Err(FilterError::InvalidInputPort { port: input.port }.into());
        }

        let predicate = self
            .predicate
            .evaluate(input.change.records())
            .map_err(FilterError::Expression)?;
        let predicate = predicate
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or(FilterError::PredicateArray)?;
        let selected = predicate.true_count();
        if selected == 0 {
            return Ok(Action::Complete(None));
        }
        if selected == input.change.num_rows() {
            return Ok(Action::Complete(Some(input.change.clone())));
        }

        let filter = FilterBuilder::new(predicate).optimize().build();
        let canonical =
            canonical_record_batch(input.change.records()).map_err(FilterError::Arrow)?;
        let records = filter
            .filter_record_batch(&canonical)
            .map_err(FilterError::Arrow)?;
        let diffs = filter
            .filter(input.change.diffs())
            .map_err(FilterError::Arrow)?;
        let diffs = diffs
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(FilterError::DifferenceArray)?
            .clone();
        let output = Change::try_new(records, diffs).map_err(FilterError::Change)?;
        Ok(Action::Complete(Some(output)))
    }
}

fn canonical_record_batch(records: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let columns = records
        .columns()
        .iter()
        .map(|column| make_array(column.to_data()))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(records.num_rows()));
    RecordBatch::try_new_with_options(records.schema(), columns, &options)
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let predicate = StoredExpression::decode(&mut cursor)?;
    cursor.finish()?;
    Ok(Box::new(FilterDefinition { predicate }))
}
