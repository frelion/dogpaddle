use std::{
    num::NonZeroU32,
    sync::{Arc, OnceLock},
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_store::{Cell, TransactionAccess};
use thiserror::Error;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    definition::{DataName, Sealed as SealedDefinition},
    operation::{Action, Operation, OperationError, OperationInput},
};

pub(crate) const TAG: u16 = 2;
const COUNT: DataName<Cell<u64>> = DataName::new("count");
const DATA: &[DataDeclaration] = &[COUNT.declaration()];

/// Pure definition of a running count operation.
///
/// The operation counts ordered input rows independently of their diff values
/// and emits each updated count as an insertion event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CountDefinition {
    _private: (),
}

/// Materialized running count operation.
///
/// This value stores only its persistent count. It never retains its definition
/// or begins, commits, or stores a transaction.
pub struct CountOperation {
    count: Cell<u64>,
}

/// Count-specific failure during one [`CountOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CountError {
    /// The input Operation was called without a Change.
    #[error("count requires one input Change")]
    MissingInput,
    /// Count only accepts its definition's first input port.
    #[error("count does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// The durable count plus the input row count cannot be represented by [`u64`].
    #[error("count overflow")]
    Overflow,
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl CountDefinition {
    /// Creates a running count definition.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl SealedDefinition for CountDefinition {
    fn bind_schemas(
        &self,
        _input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        Ok(OperationBinding::new(
            Some(output_schema()),
            |data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                let count = data.take(&COUNT)?;
                Ok(Box::new(CountOperation::new(count)))
            },
        ))
    }
}

impl OperationDefinition for CountDefinition {
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

impl CountOperation {
    /// Creates a Count operation from its durable count.
    #[must_use]
    pub const fn new(count: Cell<u64>) -> Self {
        Self { count }
    }
}

impl Operation for CountOperation {
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(CountError::MissingInput)?;
        if input.port != 0 {
            return Err(CountError::InvalidInputPort { port: input.port }.into());
        }

        let mut count = self.count.access(access)?;
        let current = count.get()?.unwrap_or_default();
        let rows = u64::try_from(input.change.num_rows()).map_err(|_| CountError::Overflow)?;
        let final_count = current.checked_add(rows).ok_or(CountError::Overflow)?;
        let first = current
            .checked_add(1)
            .expect("nonempty Change fitting the count has a first value");
        let values = (first..=final_count).collect::<Vec<_>>();
        let output = uint64_change(values)?;

        count.set(&final_count)?;
        Ok(Action::Complete(Some(output)))
    }
}

fn uint64_change(values: Vec<u64>) -> Result<Change, OperationError> {
    let row_count = values.len();
    let records = RecordBatch::try_new(output_schema(), vec![Arc::new(UInt64Array::from(values))])?;
    let diffs = Int64Array::from(vec![1_i64; row_count]);
    Ok(Change::try_new(records, diffs)?)
}

fn output_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| {
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }))
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    if payload.is_empty() {
        Ok(Box::new(CountDefinition::new()))
    } else {
        Err(DefinitionCodecError::TrailingBytes)
    }
}
