//! A small, replayable queue used by the runnable example and protocol tests.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, Turn,
};
use dogpaddle_store::Cell;

const RECORDS: &[u64] = &[10, 20, 30];

/// Only the checkpoint is durable; reopening starts with no client.
pub(super) struct QueueSource {
    checkpoint: Cell<u64>,
    client: Option<QueueClient>,
}

impl QueueSource {
    pub(super) fn new(checkpoint: Cell<u64>) -> Self {
        Self {
            checkpoint,
            client: None,
        }
    }
}

impl Operation for QueueSource {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err("queue source does not accept input".into());
        }

        // First turn: recover from Store. Publish the memory cache only after commit.
        if self.client.is_none() {
            return Ok(Turn::ready(move |access| {
                let checkpoint = self.checkpoint.access(access)?.get()?.unwrap_or(0);
                let position = usize::try_from(checkpoint)?;
                if position > RECORDS.len() {
                    return Err("queue checkpoint is beyond the available records".into());
                }
                Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        self.client = Some(QueueClient { position });
                        Ok(())
                    }),
                ))
            }));
        }

        // Later turns: poll outside the Store transaction, without acknowledging.
        let client = self.client.as_mut().expect("client was initialized above");
        let Some(delivery) = client.poll() else {
            return Ok(Turn::Idle);
        };
        let change = one_row(delivery.value)?;
        let next_position = u64::try_from(delivery.next_position)?;
        let checkpoint = &self.checkpoint;

        Ok(Turn::ready(move |access| {
            // The caller persists this output and checkpoint in the same transaction.
            checkpoint.access(access)?.set(&next_position)?;
            Ok((
                Action::Commit(Some(change)),
                AfterCommit::new(move || {
                    // Dropping this closure leaves the delivery available for replay.
                    delivery.ack();
                    Ok(())
                }),
            ))
        }))
    }
}

// The fixed queue stands in for an external service with checkpoint-based resume.
// Its position advances only on ACK; constructing a new client can resume even
// when the previous process committed its checkpoint but exited before ACK.
struct QueueClient {
    position: usize,
}

impl QueueClient {
    fn poll(&mut self) -> Option<Delivery<'_>> {
        let value = *RECORDS.get(self.position)?;
        let next_position = self.position + 1;
        Some(Delivery {
            client: self,
            value,
            next_position,
        })
    }
}

struct Delivery<'client> {
    client: &'client mut QueueClient,
    value: u64,
    next_position: usize,
}

impl Delivery<'_> {
    fn ack(self) {
        self.client.position = self.next_position;
    }
}

fn one_row(value: u64) -> Result<Change, OperationError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))])?;
    Ok(Change::try_new(records, Int64Array::from(vec![1]))?)
}
