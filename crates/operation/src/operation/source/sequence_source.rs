use std::sync::{Arc, OnceLock};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, ChangeError};
use dogpaddle_store::{Cell, StoreError, TransactionAccess};
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationDefinition,
    definition::{DataName, Sealed as SealedDefinition},
    operation::{Operation, OperationError, OperationInput, Sealed as SealedOperation},
};

pub(crate) const TAG: u16 = 1;
const POSITION: DataName<Cell<u64>> = DataName::new("sequence_source.position");
const DATA: &[DataDeclaration] = &[POSITION.declaration()];

/// Pure definition of a monotonically increasing source.
///
/// The source accepts no inputs and emits `u64` values beginning at `start`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceSourceDefinition {
    start: u64,
}

/// Materialized monotonically increasing source operation.
///
/// This value owns its pure definition and persistent position, but never
/// begins, commits, or stores a transaction.
pub struct SequenceSourceOperation {
    definition: SequenceSourceDefinition,
    position: Cell<u64>,
}

/// Failure during one [`SequenceSourceOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SequenceSourceError {
    /// A source was incorrectly supplied an input Change.
    #[error("sequence source does not accept input")]
    UnexpectedInput,
    /// Persistent position access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The source has already emitted [`u64::MAX`].
    #[error("sequence exhausted after emitting u64::MAX")]
    Exhausted,
    /// Arrow rejected the fixed source output batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Change validation rejected the fixed source output.
    #[error(transparent)]
    Change(#[from] ChangeError),
}

impl SequenceSourceDefinition {
    /// Creates a source whose first emitted value is `start`.
    #[must_use]
    pub const fn new(start: u64) -> Self {
        Self { start }
    }

    /// Returns the first value emitted by a new source.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }
}

impl SealedDefinition for SequenceSourceDefinition {}

impl OperationDefinition for SequenceSourceDefinition {
    fn input_count(&self) -> usize {
        0
    }

    fn produces_output(&self) -> bool {
        true
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn materialize(
        &self,
        data: &mut DataInstances,
    ) -> Result<Box<dyn Operation>, MaterializeError> {
        let position = data.take(&POSITION)?;
        Ok(Box::new(SequenceSourceOperation::new(*self, position)))
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.start.to_be_bytes());
    }
}

impl SequenceSourceOperation {
    /// Materializes a Sequence source from its pure definition and durable data.
    #[must_use]
    pub const fn new(definition: SequenceSourceDefinition, position: Cell<u64>) -> Self {
        Self {
            definition,
            position,
        }
    }

    /// Returns the pure definition used to materialize this source.
    #[must_use]
    pub const fn definition(&self) -> &SequenceSourceDefinition {
        &self.definition
    }

    fn execute_turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, SequenceSourceError> {
        if input.is_some() {
            return Err(SequenceSourceError::UnexpectedInput);
        }

        let mut position = self.position.access(access)?;
        let next = match position.get()? {
            Some(previous) => previous
                .checked_add(1)
                .ok_or(SequenceSourceError::Exhausted)?,
            None => self.definition.start,
        };
        let output = uint64_change(vec![next])?;

        position.set(&next)?;
        Ok(Some(output))
    }
}

impl SealedOperation for SequenceSourceOperation {}

impl Operation for SequenceSourceOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError> {
        self.execute_turn(input, access)
            .map_err(|source| Box::new(source) as OperationError)
    }
}

fn uint64_change(values: Vec<u64>) -> Result<Change, SequenceSourceError> {
    let row_count = values.len();
    let records = RecordBatch::try_new(output_schema(), vec![Arc::new(UInt64Array::from(values))])?;
    let diffs = Int64Array::from(vec![1_i64; row_count]);
    Ok(Change::try_new(records, diffs)?)
}

fn output_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )]))
    }))
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let start = match <[u8; 8]>::try_from(payload) {
        Ok(bytes) => u64::from_be_bytes(bytes),
        Err(_) if payload.len() < size_of::<u64>() => {
            return Err(DefinitionCodecError::Truncated);
        }
        Err(_) => return Err(DefinitionCodecError::TrailingBytes),
    };
    Ok(Box::new(SequenceSourceDefinition::new(start)))
}
