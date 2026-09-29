use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::filter::filter_record_batch;
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::{OrderedMultiset, OrderedMultisetAccess, StoreError, TransactionAccess};
use thiserror::Error;

use crate::{
    DefinitionCodecError,
    codec::decode_json_payload,
    definition::ConstructedOperation,
    operation::{AtomicOperation, OperationError, OperationInput, relation::canonical_row},
};

pub(crate) const TAG: u16 = 13;
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
    weights: OrderedMultiset<Vec<u8>>,
}

/// One contiguous run; only its current weight needs to stay in memory.
struct PendingWeight {
    key: Vec<u8>,
    persisted: u64,
    current: u64,
}

/// Distinct-specific failure during one `DistinctOperation` turn.
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
        let weights = scope.data::<OrderedMultiset<Vec<u8>>>(WEIGHTS)?;
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
        &mut self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError> {
        if input.port != 0 {
            return Err(DistinctError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(DistinctError::InputSchemaMismatch.into());
        }

        let mut selected = Vec::with_capacity(input.change.num_rows());
        let mut output_diffs = Vec::new();
        let mut weights = self.weights.access(access).map_err(DistinctError::Store)?;
        let mut pending: Option<PendingWeight> = None;
        for row_index in 0..input.change.num_rows() {
            let row = canonical_row(input.change.records(), row_index)?;
            if pending.as_ref().is_none_or(|pending| pending.key != row) {
                if let Some(previous) = pending.take() {
                    flush_weight(&mut weights, &previous).map_err(map_weight_error)?;
                }
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
                flush_weight(&mut weights, pending).map_err(map_weight_error)?;
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
            flush_weight(&mut weights, &pending).map_err(map_weight_error)?;
        }

        if output_diffs.is_empty() {
            return Ok(None);
        }
        let records = if output_diffs.len() == input.change.num_rows() {
            input.change.records().clone()
        } else {
            let selected = BooleanArray::from(selected);
            filter_record_batch(input.change.records(), &selected).map_err(DistinctError::Arrow)?
        };
        let output = Change::try_new(records, Int64Array::from(output_diffs))
            .map_err(DistinctError::Change)?;
        Ok(Some(output))
    }
}

fn flush_weight(
    weights: &mut OrderedMultisetAccess<'_, Vec<u8>>,
    pending: &PendingWeight,
) -> Result<(), StoreError> {
    if pending.current != pending.persisted {
        weights.set_multiplicity(&pending.key, pending.current)?;
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

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<DistinctDefinition>, DefinitionCodecError> {
    let definition: DistinctDefinition = decode_json_payload(payload, "invalid Distinct payload")?;
    Ok(Box::new(definition))
}
