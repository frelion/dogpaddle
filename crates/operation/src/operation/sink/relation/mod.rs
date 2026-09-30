//! Fixed-ID relation planning shared by SQL database sink adapters.

mod plan;

pub(crate) use plan::prepare;

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::{Field, SchemaRef};
use dogpaddle_change::Change;
use thiserror::Error;

use crate::operation::{
    OperationError,
    sink::buffered::{DeliveryBatch, MAX_TARGET_BATCH_BYTES},
};

pub(crate) use crate::operation::relation::{
    RowError, canonical_row_bounded, canonical_row_size_bounded, encode_canonical, row_hash,
};

pub(crate) const MAX_MUTATIONS_PER_BATCH: usize = 1024;
pub(crate) const FIRST_TECHNICAL_ID: u64 = 1;
pub(crate) const MAX_TECHNICAL_ID: u64 = i64::MAX.unsigned_abs();
const EXHAUSTED_ID: u64 = MAX_TECHNICAL_ID + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Insert {
    pub row_index: u64,
    pub technical_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Delete {
    pub row_index: u64,
    pub technical_id: u64,
}

/// Immutable work: insert these IDs, then delete these IDs, atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Batch {
    pub inserts: Vec<Insert>,
    pub deletes: Vec<Delete>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct MutationGroup {
    pub(super) row_index: u64,
    pub(super) insert_ids: Vec<u64>,
    pub(super) mutation_ids: Vec<u64>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct MutationGroups {
    pub(super) rows: Vec<MutationGroup>,
    pub(super) delete_ids: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TerminalMutation {
    pub(super) row_index: u64,
    pub(super) technical_id: u64,
    pub(super) deleted: bool,
}

/// Groups a validated plan by logical input row without applying backend rules.
pub(super) fn group_mutations(batch: &Batch) -> MutationGroups {
    #[derive(Default)]
    struct Ids {
        inserts: Vec<u64>,
        mutations: Vec<u64>,
    }

    let mut by_row = BTreeMap::<u64, Ids>::new();
    for insert in &batch.inserts {
        let group = by_row.entry(insert.row_index).or_default();
        group.inserts.push(insert.technical_id);
        group.mutations.push(insert.technical_id);
    }
    let mut delete_ids = Vec::with_capacity(batch.deletes.len());
    for delete in &batch.deletes {
        by_row
            .entry(delete.row_index)
            .or_default()
            .mutations
            .push(delete.technical_id);
        delete_ids.push(delete.technical_id);
    }
    MutationGroups {
        rows: by_row
            .into_iter()
            .map(|(row_index, ids)| MutationGroup {
                row_index,
                insert_ids: ids.inserts,
                mutation_ids: ids.mutations,
            })
            .collect(),
        delete_ids,
    }
}

/// Returns the final mutation for each technical ID in a validated plan.
pub(super) fn terminal_mutations(batch: &Batch) -> Vec<TerminalMutation> {
    let mut by_id = BTreeMap::<u64, TerminalMutation>::new();
    for insert in &batch.inserts {
        let replaced = by_id.insert(
            insert.technical_id,
            TerminalMutation {
                row_index: insert.row_index,
                technical_id: insert.technical_id,
                deleted: false,
            },
        );
        debug_assert!(replaced.is_none(), "validated insert IDs are unique");
    }
    for delete in &batch.deletes {
        by_id
            .entry(delete.technical_id)
            .and_modify(|mutation| {
                // Equal canonical rows can occur at different input indexes.
                mutation.row_index = delete.row_index;
                mutation.deleted = true;
            })
            .or_insert(TerminalMutation {
                row_index: delete.row_index,
                technical_id: delete.technical_id,
                deleted: true,
            });
    }
    by_id.into_values().collect()
}

/// One bounded request per distinct canonical row in the current slice.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Lookup {
    pub row_index: usize,
    pub take: usize,
}

#[derive(Debug)]
pub(crate) struct Matches {
    pub ids: Vec<u64>,
}

/// Target layout and I/O for the single buffered relation sink protocol.
/// Buffering, fixed-ID planning, durable state and replay belong to shared code.
pub(crate) trait RelationTarget: Send + 'static {
    /// Deterministic, nonzero byte charge for one target mutation.
    /// This must be pure and perform no target I/O: admission and delivery slicing
    /// call it before a Change is acknowledged or a target transaction starts.
    fn event_bytes(&self, input: &Change, row_index: usize) -> Result<u64, OperationError> {
        relation_event_bytes(input, row_index)
    }

    /// Fresh construction must reject existing targets before publishing intent.
    fn require_absent(&mut self) -> Result<(), OperationError>;
    /// Creates or verifies the owned empty layout after initialization is durable.
    /// Reopen may repeat this call after an uncertain result, so it must be
    /// idempotent and reject incompatible ownership or layout.
    fn initialize(&mut self) -> Result<(), OperationError>;
    /// Read-only exact matches in request order; return at most `take` ascending
    /// IDs per request. No full-remaining cardinality query is permitted.
    /// A failed read must leave the target session ready for a retry of the same
    /// loaded batch, resetting a poisoned connection before returning the error.
    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError>;
    /// Atomically deliver one durably prepared plan in one target transaction.
    /// Reopen may repeat the exact plan after process exit or an uncertain commit:
    /// matching duplicate IDs and already deleted IDs are successful replay,
    /// while an ID bound to a different complete logical row must fail.
    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError>;
}

/// Encodes each field once and projects its fresh canonical bytes to a target value.
pub(crate) fn encode_target_values<V>(
    schema: &SchemaRef,
    batch: &RecordBatch,
    row_index: usize,
    mut value: impl FnMut(usize, &Field, &[u8]) -> V,
) -> Result<(Vec<u8>, Vec<V>), RowError> {
    if batch.schema_ref().as_ref() != schema.as_ref() {
        return Err(RowError::SchemaMismatch);
    }
    if row_index >= batch.num_rows() {
        return Err(RowError::RowOutOfBounds {
            row_index,
            rows: batch.num_rows(),
        });
    }

    let mut canonical = Vec::new();
    let mut values = Vec::with_capacity(schema.fields().len());
    for (index, (field, array)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        let start = canonical.len();
        encode_canonical(
            field,
            array.as_ref(),
            row_index,
            field.name(),
            &mut canonical,
        )?;
        values.push(value(index, field, &canonical[start..]));
    }
    Ok((canonical, values))
}

pub(super) fn relation_event_bytes(
    input: &Change,
    row_index: usize,
) -> Result<u64, OperationError> {
    const TECHNICAL_VALUE_BYTES: usize = size_of::<u64>() + 16;
    const PARAMETER_FRAMING_BYTES: usize = 8;

    let canonical_bytes = canonical_row_size_bounded(
        input.records(),
        row_index,
        usize::try_from(MAX_TARGET_BATCH_BYTES).expect("the target byte limit fits usize"),
    )?;
    let columns = input
        .records()
        .num_columns()
        .checked_add(2)
        .ok_or_else(|| invalid("target mutation column count exceeds usize"))?;
    canonical_bytes
        .checked_add(TECHNICAL_VALUE_BYTES)
        .and_then(|bytes| {
            columns
                .checked_mul(PARAMETER_FRAMING_BYTES)
                .and_then(|framing| bytes.checked_add(framing))
        })
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid("target mutation byte charge exceeds u64"))
}

pub(crate) fn validate_admission(
    input: &Change,
    checkpoint: u64,
    buffered_events: u64,
) -> Result<(), OperationError> {
    validate_next_id(checkpoint)?;
    let positive_events = input
        .diffs()
        .values()
        .iter()
        .filter(|diff| **diff > 0)
        .try_fold(0_u64, |total, diff| {
            total
                .checked_add(diff.unsigned_abs())
                .ok_or_else(|| invalid("positive event count exceeds u64"))
        })?;
    if positive_events == 0 {
        return Ok(());
    }
    let reserved = buffered_events
        .checked_add(positive_events)
        .ok_or_else(|| invalid("technical ID reservation exceeds u64"))?;
    if reserved > EXHAUSTED_ID - checkpoint {
        Err(invalid("technical ID capacity is exhausted"))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_recovery(
    checkpoint: u64,
    remaining_positive_events: u64,
) -> Result<(), OperationError> {
    validate_next_id(checkpoint)?;
    if remaining_positive_events > EXHAUSTED_ID - checkpoint {
        Err(invalid(
            "buffered positive events exceed the remaining technical ID capacity",
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn encode_checkpoint(checkpoint: u64, output: &mut Vec<u8>) {
    output.extend(checkpoint.to_be_bytes());
}

pub(crate) fn decode_checkpoint(input: &mut &[u8]) -> Result<u64, OperationError> {
    let checkpoint = u64::from_be_bytes(read(input)?);
    validate_next_id(checkpoint)?;
    Ok(checkpoint)
}

pub(crate) fn encode_plan(plan: &Batch, output: &mut Vec<u8>) {
    output.push(1);
    output.extend(
        u16::try_from(plan.inserts.len())
            .expect("the relation plan is bounded")
            .to_be_bytes(),
    );
    output.extend(
        u16::try_from(plan.deletes.len())
            .expect("the relation plan is bounded")
            .to_be_bytes(),
    );
    for insert in &plan.inserts {
        output.extend(insert.row_index.to_be_bytes());
        output.extend(insert.technical_id.to_be_bytes());
    }
    for delete in &plan.deletes {
        output.extend(delete.row_index.to_be_bytes());
        output.extend(delete.technical_id.to_be_bytes());
    }
}

pub(crate) fn decode_plan(
    input: &mut &[u8],
    batch: &DeliveryBatch,
    checkpoint: u64,
) -> Result<Batch, OperationError> {
    if read::<1>(input)? != [1] {
        return Err(invalid("unknown relation-plan version"));
    }
    let inserts = usize::from(u16::from_be_bytes(read(input)?));
    let deletes = usize::from(u16::from_be_bytes(read(input)?));
    let count = inserts
        .checked_add(deletes)
        .ok_or_else(|| invalid("relation-plan mutation count overflows usize"))?;
    if count == 0 || count > MAX_MUTATIONS_PER_BATCH {
        return Err(invalid("invalid relation-plan mutation count"));
    }
    let mut plan = Batch {
        inserts: Vec::with_capacity(inserts),
        deletes: Vec::with_capacity(deletes),
    };
    for _ in 0..inserts {
        plan.inserts.push(Insert {
            row_index: u64::from_be_bytes(read(input)?),
            technical_id: u64::from_be_bytes(read(input)?),
        });
    }
    for _ in 0..deletes {
        plan.deletes.push(Delete {
            row_index: u64::from_be_bytes(read(input)?),
            technical_id: u64::from_be_bytes(read(input)?),
        });
    }
    plan::validate(&plan, checkpoint, batch.change())?;
    Ok(plan)
}

#[derive(Debug, Error)]
#[error("relation sink: {0}")]
struct RelationError(String);

fn invalid(message: impl Into<String>) -> OperationError {
    Box::new(RelationError(message.into()))
}

fn validate_next_id(next_id: u64) -> Result<(), OperationError> {
    if (FIRST_TECHNICAL_ID..=EXHAUSTED_ID).contains(&next_id) {
        Ok(())
    } else {
        Err(invalid("next ID is outside 1..=i64::MAX+1"))
    }
}

fn read<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], OperationError> {
    let (value, rest) = input
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated relation sink state"))?;
    *input = rest;
    Ok(value
        .try_into()
        .expect("the split has the requested length"))
}

#[cfg(test)]
mod tests;
