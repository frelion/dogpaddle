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
    DefinitionCodecError, OperationDefinition, OperationKind,
    definition::{ConstructedOperation, Sealed as SealedDefinition},
    operation::{AtomicOperation, OperationError, OperationInput},
};

pub(crate) const TAG: u16 = 2;
const COUNT: &str = "running_event_count.count";

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
pub(crate) struct RunningEventCountOperation {
    input_schema: SchemaRef,
    count: Cell<u64>,
}

/// Running-event-count failure during one `RunningEventCountOperation` turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RunningEventCountError {
    /// `RunningEventCount` only accepts its definition's first input port.
    #[error("running event count does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// Runtime input differs from the exact Schema used during binding.
    #[error("running event count input schema differs from its bound schema")]
    InputSchemaMismatch,
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
    fn output_schema_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        _input_schemas: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        Ok(Some(output_schema()))
    }

    fn construct_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        input_schemas: &[SchemaRef],
        scope: &mut dogpaddle_store::DataScope<'_>,
        _resource: crate::RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let count = scope.data::<Cell<u64>>(COUNT)?;
        Ok(ConstructedOperation::atomic(
            output_schema(),
            RunningEventCountOperation::new(Arc::clone(&input_schemas[0]), count),
        ))
    }
}

impl OperationDefinition for RunningEventCountDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::AtomicTransform(NonZeroU32::MIN)
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

impl RunningEventCountOperation {
    /// Creates a running event-count operation from its durable count.
    #[must_use]
    const fn new(input_schema: SchemaRef, count: Cell<u64>) -> Self {
        Self {
            input_schema,
            count,
        }
    }
}

impl AtomicOperation for RunningEventCountOperation {
    fn apply(
        &mut self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError> {
        if input.port != 0 {
            return Err(RunningEventCountError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(RunningEventCountError::InputSchemaMismatch.into());
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
        Ok(Some(output))
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
