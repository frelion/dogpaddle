use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};

use super::cdc_runtime::{DeliveryKind, SourceDelivery};
use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_store::{Cell, Queue, ReadTransactionAccess, TransactionAccess};
use std::num::NonZeroU64;
use thiserror::Error;

use crate::{
    definition::{ConstructedOperation, schema_error},
    operation::{OperationError, SourceOperation},
};

const PUBLISHED: &str = "sequence_scan.published";
const POSITION: &str = "sequence_scan.position";
const PUBLISHED_BYTES: u64 = 64 * 1024 * 1024;

/// Pure definition of a monotonically increasing Scan.
///
/// The Scan accepts no inputs and emits `u64` values beginning at `start`.
/// After committing [`u64::MAX`], subsequent polls return
/// an idle poll without changing persistent state or producing output.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SequenceScanDefinition {
    start: u64,
}

/// Materialized monotonically increasing Scan operation.
///
/// This value stores the first value, published queue and persistent position needed at
/// execution time. It never retains its definition or begins, commits, or
/// stores a transaction.
pub(crate) struct SequenceScanOperation {
    start: u64,
    codec: SchemaBoundChangeCodec,
    position: Cell<u64>,
    published: Queue<Vec<u8>>,
    next: Option<u64>,
}

/// Sequence-specific source protocol or durable state failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SequenceScanError {
    /// A Scan was supplied a delivery from another source kind.
    #[error("sequence scan received another source kind")]
    UnexpectedDelivery,
    /// The committed position precedes the configured first value or queue exceeds capacity.
    #[error("sequence scan durable position or published capacity is invalid")]
    InvalidState,
}

impl SequenceScanDefinition {
    /// Creates a Scan whose first emitted value is `start`.
    #[must_use]
    pub const fn new(start: u64) -> Self {
        Self { start }
    }

    /// Returns the first value emitted by a new Scan.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }
}

impl SequenceScanDefinition {
    pub(crate) fn output_schema_unchecked() -> SchemaRef {
        output_schema()
    }

    pub(crate) fn construct_unchecked(
        self,
        scope: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let position = scope.data::<Cell<u64>>(POSITION)?;
        let published = scope.data::<Queue<Vec<u8>>>(PUBLISHED)?;
        let codec = SchemaBoundChangeCodec::try_new(output_schema()).map_err(schema_error)?;
        Ok(ConstructedOperation::source(
            Some(output_schema()),
            SequenceScanOperation {
                start: self.start,
                codec,
                position,
                published,
                next: Some(self.start),
            },
        ))
    }
}

impl SourceOperation for SequenceScanOperation {
    fn restore(&mut self, access: ReadTransactionAccess<'_>) -> Result<(), OperationError> {
        let position = self.position.read(access)?.get_bounded(8)?;
        if position.is_some_and(|value| value < self.start)
            || self.published.read(access)?.queued_bytes()? > PUBLISHED_BYTES
        {
            return Err(SequenceScanError::InvalidState.into());
        }
        self.next = position.map_or(Some(self.start), |value| value.checked_add(1));
        Ok(())
    }
    fn poll(&mut self) -> Result<Option<SourceDelivery>, OperationError> {
        let Some(next) = self.next else {
            return Ok(None);
        };
        let records = RecordBatch::try_new(
            self.codec.schema(),
            vec![Arc::new(UInt64Array::from(vec![next]))],
        )?;
        let change = Change::try_new(records, Int64Array::from(vec![1_i64]))?;
        let encoded = self.codec.encode(&change)?;
        Ok(Some(SourceDelivery::sequence(next, encoded)))
    }
    fn record(
        &self,
        access: TransactionAccess<'_>,
        delivery: &mut SourceDelivery,
    ) -> Result<bool, OperationError> {
        let DeliveryKind::Sequence { next, encoded } = &delivery.kind else {
            return Err(SequenceScanError::UnexpectedDelivery.into());
        };
        if !self.published.access(access)?.try_push(
            encoded,
            NonZeroU64::new(PUBLISHED_BYTES).expect("capacity is nonzero"),
        )? {
            return Ok(false);
        }
        self.position.access(access)?.set(next)?;
        Ok(true)
    }
    fn ack(&mut self, delivery: SourceDelivery) -> Result<(), OperationError> {
        let DeliveryKind::Sequence { next, .. } = delivery.kind else {
            return Err(SequenceScanError::UnexpectedDelivery.into());
        };
        self.next = next.checked_add(1);
        Ok(())
    }
    fn published(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<Vec<u8>>, OperationError> {
        Ok(self
            .published
            .read(access)?
            .front_bounded(8 * 1024 * 1024)?)
    }
    fn consume_published(&self, access: TransactionAccess<'_>) -> Result<(), OperationError> {
        self.published.access(access)?.discard_front(1)?;
        Ok(())
    }
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
