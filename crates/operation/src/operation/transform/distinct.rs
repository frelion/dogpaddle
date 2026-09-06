use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{BooleanArray, Int64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::filter::filter_record_batch;
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::{StoreError, TransactionAccess};
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    definition::{DataName, Sealed as SealedDefinition},
    operation::{
        Action, Operation, OperationError, OperationInput, TransactionalOperation,
        relation::{RowWeightError, RowWeights, apply_weight, canonical_row},
    },
};

pub(crate) const TAG: u16 = 13;
const WEIGHTS: DataName<RowWeights> = DataName::new("distinct.weights");
const DATA: &[DataDeclaration] = &[WEIGHTS.declaration()];

/// Pure definition of an exact, order-preserving distinct operation.
///
/// The operation tracks each complete logical row's positive multiplicity. It
/// emits an insertion when a row first becomes present and a retraction when
/// its multiplicity returns to zero. Intermediate multiplicity changes emit
/// nothing, and input event order is never consolidated or reordered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DistinctDefinition {
    _private: (),
}

/// Materialized exact-row distinct operation.
///
/// The runtime owns only its bound input Schema and durable row weights. It
/// does not retain its Definition or begin, commit, or store a transaction.
pub struct DistinctOperation {
    input_schema: SchemaRef,
    weights: RowWeights,
}

/// Distinct-specific failure during one [`DistinctOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DistinctError {
    /// The input Operation was called without a Change.
    #[error("distinct requires one input Change")]
    MissingInput,
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

impl From<RowWeightError> for DistinctError {
    fn from(error: RowWeightError) -> Self {
        match error {
            RowWeightError::Store(source) => Self::Store(source),
            RowWeightError::Negative => Self::NegativeWeight,
            RowWeightError::Overflow => Self::WeightOverflow,
        }
    }
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl DistinctDefinition {
    /// Creates a distinct definition.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl SealedDefinition for DistinctDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Distinct input arity");
        let runtime_schema = Arc::clone(input_schema);
        Ok(OperationBinding::new(
            Some(Arc::clone(input_schema)),
            move |data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                let weights = data.take(&WEIGHTS)?;
                Ok(Box::new(DistinctOperation {
                    input_schema: runtime_schema,
                    weights,
                }))
            },
        ))
    }
}

impl OperationDefinition for DistinctDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Transform(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl TransactionalOperation for DistinctOperation {
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(DistinctError::MissingInput)?;
        if input.port != 0 {
            return Err(DistinctError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(DistinctError::InputSchemaMismatch.into());
        }

        let mut selected = Vec::with_capacity(input.change.num_rows());
        let mut output_diffs = Vec::new();
        let mut weights = self.weights.access(access).map_err(DistinctError::Store)?;
        for row_index in 0..input.change.num_rows() {
            let row = canonical_row(input.change.records(), row_index)?;
            let output_difference =
                apply_weight(&mut weights, row, input.change.diffs().value(row_index))
                    .map_err(DistinctError::from)?;
            if let Some(difference) = output_difference {
                selected.push(true);
                output_diffs.push(difference);
            } else {
                selected.push(false);
            }
        }

        if output_diffs.is_empty() {
            return Ok(Action::Complete(None));
        }
        let records = if output_diffs.len() == input.change.num_rows() {
            input.change.records().clone()
        } else {
            let selected = BooleanArray::from(selected);
            filter_record_batch(input.change.records(), &selected).map_err(DistinctError::Arrow)?
        };
        let output = Change::try_new(records, Int64Array::from(output_diffs))
            .map_err(DistinctError::Change)?;
        Ok(Action::Complete(Some(output)))
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    if payload.is_empty() {
        Ok(Box::new(DistinctDefinition::new()))
    } else {
        Err(DefinitionCodecError::TrailingBytes)
    }
}
