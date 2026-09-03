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
const COUNT: DataName<Cell<u64>> = DataName::new("running_event_count.count");
const DATA: &[DataDeclaration] = &[COUNT.declaration()];

/// Pure definition of a running event-count operation.
///
/// The operation counts ordered input rows independently of their diff values
/// and emits each updated count as an insertion event. It is an observation
/// transform, not a relational cardinality aggregate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunningEventCountDefinition {
    _private: (),
}

/// Materialized running event-count operation.
///
/// This value stores only its persistent count. It never retains its definition
/// or begins, commits, or stores a transaction.
pub struct RunningEventCountOperation {
    count: Cell<u64>,
}

/// Running-event-count failure during one [`RunningEventCountOperation`] turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RunningEventCountError {
    /// The input Operation was called without a Change.
    #[error("running event count requires one input Change")]
    MissingInput,
    /// `RunningEventCount` only accepts its definition's first input port.
    #[error("running event count does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// The durable count plus the input row count cannot be represented by [`u64`].
    #[error("running event count overflow")]
    Overflow,
}

#[expect(
    clippy::new_without_default,
    reason = "definitions keep one explicit construction path"
)]
impl RunningEventCountDefinition {
    /// Creates a running event-count definition.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl SealedDefinition for RunningEventCountDefinition {
    fn bind_schemas(
        &self,
        _input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        Ok(OperationBinding::new(
            Some(output_schema()),
            |data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                let count = data.take(&COUNT)?;
                Ok(Box::new(RunningEventCountOperation::new(count)))
            },
        ))
    }
}

impl OperationDefinition for RunningEventCountDefinition {
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

impl RunningEventCountOperation {
    /// Creates a running event-count operation from its durable count.
    #[must_use]
    pub const fn new(count: Cell<u64>) -> Self {
        Self { count }
    }
}

impl Operation for RunningEventCountOperation {
    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(RunningEventCountError::MissingInput)?;
        if input.port != 0 {
            return Err(RunningEventCountError::InvalidInputPort { port: input.port }.into());
        }

        let mut count = self.count.access(access)?;
        let current = count.get()?.unwrap_or_default();
        let rows =
            u64::try_from(input.change.num_rows()).map_err(|_| RunningEventCountError::Overflow)?;
        let final_count = current
            .checked_add(rows)
            .ok_or(RunningEventCountError::Overflow)?;
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
        Ok(Box::new(RunningEventCountDefinition::new()))
    } else {
        Err(DefinitionCodecError::TrailingBytes)
    }
}
