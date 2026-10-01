use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::filter::filter_record_batch;
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::{OrderedMap, OrderedMapAccess, StoreError, TransactionAccess};
use thiserror::Error;

use crate::{
    definition::ConstructedOperation,
    operation::{
        AtomicOperation, BudgetExceeded, OperationError, OperationInput, StepBudget,
        logical_change_bytes,
        relation::{RowError, canonical_row_bounded, canonical_row_size_bounded},
    },
};

const WEIGHTS: &str = "distinct.weights";

/// Pure definition of an exact, order-preserving distinct operation.
///
/// The operation tracks each complete logical row's positive multiplicity. It
/// emits an insertion when a row first becomes present and a retraction when
/// its multiplicity returns to zero. Intermediate multiplicity changes emit
/// nothing, and input event order is never consolidated or reordered.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DistinctDefinition {}

/// Materialized exact-row distinct operation.
///
/// The runtime owns only its bound input Schema and durable row weights. It
/// does not retain its Definition or begin, commit, or store a transaction.
pub(crate) struct DistinctOperation {
    input_schema: SchemaRef,
    weights: OrderedMap<Vec<u8>, std::num::NonZeroU64>,
}

/// One contiguous run; only its current weight needs to stay in memory.
struct PendingWeight {
    key: Vec<u8>,
    persisted: u64,
    current: u64,
}

/// Distinct-specific failure during one `DistinctOperation` page.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DistinctError {
    /// Distinct only accepts its Definition's first input port.
    #[error("distinct does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// Runtime input differs from the exact Schema used during binding.
    #[error("distinct input Schema differs from its bound Schema")]
    InputSchemaMismatch,
    /// Applying an input difference would make the row's multiplicity negative.
    #[error("distinct input would make a row weight negative")]
    NegativeWeight,
    /// A row's durable multiplicity cannot represent the applied input difference.
    #[error("distinct row weight overflow")]
    WeightOverflow,
    /// Durable Distinct state could not be accessed or decoded.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Arrow could not construct or filter the selected record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// The selected output violates the Change invariant.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl DistinctDefinition {
    /// Creates a distinct definition.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

impl DistinctDefinition {
    pub(crate) fn output_schema_unchecked(input_schemas: &[SchemaRef]) -> SchemaRef {
        Arc::clone(&input_schemas[0])
    }

    pub(crate) fn construct_unchecked(
        input_schemas: &[SchemaRef],
        scope: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Distinct input arity");
        let weights = scope.data::<OrderedMap<Vec<u8>, std::num::NonZeroU64>>(WEIGHTS)?;
        Ok(ConstructedOperation::atomic(
            Arc::clone(input_schema),
            DistinctOperation {
                input_schema: Arc::clone(input_schema),
                weights,
            },
        ))
    }
}

impl AtomicOperation for DistinctOperation {
    fn apply(
        &self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Option<Change>, OperationError> {
        if input.port != 0 {
            return Err(DistinctError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(DistinctError::InputSchemaMismatch.into());
        }

        budget.charge(logical_change_bytes(input.change))?;
        budget.charge(input.change.num_rows().saturating_mul(9))?;
        let mut selected = Vec::with_capacity(input.change.num_rows());
        let mut output_diffs = Vec::with_capacity(input.change.num_rows());
        let mut weights = self.weights.access(access).map_err(DistinctError::Store)?;
        let mut pending: Option<PendingWeight> = None;
        for row_index in 0..input.change.num_rows() {
            let row_bytes = canonical_row_size_bounded(
                input.change.records(),
                row_index,
                budget.remaining_bytes(),
            )
            .map_err(|error| {
                if matches!(
                    error.downcast_ref::<RowError>(),
                    Some(RowError::SizeLimit { .. })
                ) {
                    Box::new(BudgetExceeded) as OperationError
                } else {
                    error
                }
            })?;
            budget.charge(row_bytes)?;
            let row = canonical_row_bounded(input.change.records(), row_index, row_bytes)?;
            if pending.as_ref().is_none_or(|pending| pending.key != row) {
                if let Some(previous) = pending.take() {
                    flush_weight(&mut weights, &previous, budget)?;
                }
                budget.charge(row.len().saturating_add(8))?;
                // Store admits exactly eight encoded bytes before copying;
                // malformed positive weights still poison the transaction.
                let persisted = weights.multiplicity(&row).map_err(map_weight_error)?;
                pending = Some(PendingWeight {
                    key: row,
                    persisted,
                    current: persisted,
                });
            }
            let pending = pending.as_mut().expect("the current row was loaded above");
            let difference = input.change.diffs().value(row_index);
            let before = pending.current;
            let after = if difference > 0 {
                before.checked_add(difference.unsigned_abs())
            } else {
                before.checked_sub(difference.unsigned_abs())
            };
            let Some(after) = after else {
                // Preserve Store's transaction-poisoning rule on an invalid
                // prefix, even though earlier events in the run were cached.
                flush_weight(&mut weights, pending, budget)?;
                budget.charge(pending.key.len().saturating_add(8))?;
                let error = weights
                    .adjust(&pending.key, difference)
                    .expect_err("the checked prefix rejected this adjustment");
                return Err(map_weight_error(error).into());
            };
            pending.current = after;
            let output_difference = match (before, after) {
                (0, after) if after > 0 => Some(1),
                (before, 0) if before > 0 => Some(-1),
                _ => None,
            };
            if let Some(difference) = output_difference {
                selected.push(true);
                output_diffs.push(difference);
            } else {
                selected.push(false);
            }
        }
        if let Some(pending) = pending {
            flush_weight(&mut weights, &pending, budget)?;
        }

        if output_diffs.is_empty() {
            return Ok(None);
        }
        let records = if output_diffs.len() == input.change.num_rows() {
            input.change.records().clone()
        } else {
            budget.charge(logical_change_bytes(input.change))?;
            let selected = BooleanArray::from(selected);
            filter_record_batch(input.change.records(), &selected).map_err(DistinctError::Arrow)?
        };
        let output = Change::try_new(records, Int64Array::from(output_diffs))
            .map_err(DistinctError::Change)?;
        Ok(Some(output))
    }
}

fn flush_weight(
    weights: &mut OrderedMapAccess<'_, Vec<u8>, std::num::NonZeroU64>,
    pending: &PendingWeight,
    budget: &mut StepBudget,
) -> Result<(), OperationError> {
    if pending.current != pending.persisted {
        budget.charge(pending.key.len().saturating_add(8))?;
        weights
            .set_multiplicity(&pending.key, pending.current)
            .map_err(map_weight_error)?;
    }
    Ok(())
}

fn map_weight_error(error: StoreError) -> DistinctError {
    match error {
        StoreError::MultiplicityUnderflow => DistinctError::NegativeWeight,
        StoreError::MultiplicityOverflow => DistinctError::WeightOverflow,
        source => DistinctError::Store(source),
    }
}
