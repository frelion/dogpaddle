//! Fixed-ID relation planning shared by SQL database sink adapters.

mod plan;

pub(crate) use plan::{prepare, recover};

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::{Field, SchemaRef};
use dogpaddle_change::Change;
use thiserror::Error;

use crate::operation::{OperationError, sink::buffered::MAX_TARGET_BATCH_BYTES};

pub(crate) use crate::operation::relation::{
    RowError, canonical_row_bounded, canonical_row_size_bounded, encode_canonical, row_hash,
};

pub(crate) const MAX_MUTATIONS_PER_BATCH: usize = 1024;
pub(crate) const FIRST_TECHNICAL_ID: u64 = 1;
pub(crate) const MAX_TECHNICAL_ID: u64 = u64::MAX - 1;

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

impl Batch {
    pub(crate) fn negative_ids(&self) -> Vec<u64> {
        self.deletes
            .iter()
            .map(|delete| delete.technical_id)
            .collect()
    }
}

/// Maps unsigned event positions to SQL BIGINT values while preserving order.
pub(crate) fn encode_signed_id(id: u64) -> i64 {
    i64::from_ne_bytes((id ^ (1_u64 << 63)).to_ne_bytes())
}

/// Decodes a SQL BIGINT event position and rejects the two domain sentinels.
pub(crate) fn decode_signed_id(value: i64) -> Result<u64, OperationError> {
    let id = u64::from_ne_bytes(value.to_ne_bytes()) ^ (1_u64 << 63);
    validate_technical_id(id)?;
    Ok(id)
}

pub(crate) fn validate_technical_id(id: u64) -> Result<(), OperationError> {
    if (FIRST_TECHNICAL_ID..=MAX_TECHNICAL_ID).contains(&id) {
        Ok(())
    } else {
        Err(invalid("technical ID is outside 1..u64::MAX"))
    }
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
    mut value: impl FnMut(&Field, &[u8]) -> V,
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
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let start = canonical.len();
        encode_canonical(
            field,
            array.as_ref(),
            row_index,
            field.name(),
            &mut canonical,
        )?;
        values.push(value(field, &canonical[start..]));
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

#[derive(Debug, Error)]
#[error("relation sink: {0}")]
struct RelationError(String);

fn invalid(message: impl Into<String>) -> OperationError {
    Box::new(RelationError(message.into()))
}

#[cfg(test)]
mod tests;
