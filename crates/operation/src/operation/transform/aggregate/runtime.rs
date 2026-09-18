use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{
    MultisetPartition, PartitionedMultisetAccess, StoreError, TransactionAccess,
};

use crate::{
    expression::BoundExpression,
    operation::{
        AtomicOperation, OperationError, OperationInput,
        relation::{OrderError, encode_canonical, order_key, ordered_value},
    },
};

use super::{
    AggregateError,
    functions::{ExtremaDirection, Fold, TrackedWeight, apply_weight},
    state::{Control, Entries, EntryPartition, GroupState, Groups},
    value::null,
};

/// Materialized grouped aggregate over an ordered difference stream.
pub struct AggregateOperation {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[BoundExpression]>,
    pub(super) calls: Box<[BoundCall]>,
    pub(super) layouts: Box<[BoundLayout]>,
    pub(super) slots: Box<[ExtremaSlot]>,
    pub(super) groups: Groups,
    pub(super) entries: Entries,
    pub(super) control: Control,
}

/// One bound aggregate call.
pub(super) enum BoundCall {
    Fold {
        state: usize,
        arguments: Box<[BoundExpression]>,
        reduction: Box<dyn Fold>,
    },
    Extrema {
        slot: usize,
    },
}

/// One distinct ordered-argument expression of the bound aggregate.
///
/// Layouts are dense and 0-based, so a layout's position in
/// [`AggregateOperation::layouts`] is also the partition id it owns in
/// `aggregate.entries` and the layout index its [`ExtremaSlot`]s reference.
pub(super) struct BoundLayout {
    pub(super) owner: usize,
    pub(super) field: Arc<Field>,
    pub(super) expression: BoundExpression,
}

/// One distinct (layout, direction) pair whose extreme is cached per group.
///
/// `MIN(x), MAX(x)` share one layout and need two slots; repeating the same
/// call needs one. Caching only the directions that are bound keeps the cached
/// state proportional to what the definition actually reads.
pub(super) struct ExtremaSlot {
    pub(super) layout: usize,
    pub(super) direction: ExtremaDirection,
}

/// Bound calls, layouts and extrema slots produced by one Schema binding.
pub(super) struct BoundAggregate {
    pub(super) calls: Box<[BoundCall]>,
    pub(super) layouts: Box<[BoundLayout]>,
    pub(super) slots: Box<[ExtremaSlot]>,
}

struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    diffs: Vec<i64>,
}

impl BoundCall {
    pub(super) fn fold(
        state: usize,
        arguments: Box<[BoundExpression]>,
        reduction: Box<dyn Fold>,
    ) -> Self {
        Self::Fold {
            state,
            arguments,
            reduction,
        }
    }

    pub(super) const fn extrema(slot: usize) -> Self {
        Self::Extrema { slot }
    }

    fn initial_fold_state(&self) -> Option<Vec<u8>> {
        match self {
            Self::Fold { reduction, .. } => Some(reduction.empty()),
            Self::Extrema { .. } => None,
        }
    }
}

impl AtomicOperation for AggregateOperation {
    #[expect(
        clippy::too_many_lines,
        reason = "one loop keeps each ordered input event and its atomic state transition together"
    )]
    fn apply(
        &mut self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError> {
        if input.port != 0 {
            return Err(AggregateError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.schema().as_ref() != self.input_schema.as_ref() {
            return Err(AggregateError::InputSchemaMismatch.into());
        }

        let records = input.change.records();
        let group_columns = self
            .group_expressions
            .iter()
            .enumerate()
            .map(|(group, expression)| {
                expression
                    .evaluate(records)
                    .map_err(|source| AggregateError::GroupExpression { group, source })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let call_columns = self
            .calls
            .iter()
            .enumerate()
            .map(|(aggregate, call)| match call {
                BoundCall::Fold { arguments, .. } => arguments
                    .iter()
                    .map(|expression| {
                        expression.evaluate(records).map_err(|source| {
                            AggregateError::AggregateExpression { aggregate, source }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>(),
                BoundCall::Extrema { .. } => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let layout_columns = self
            .layouts
            .iter()
            .map(|layout| {
                layout.expression.evaluate(records).map_err(|source| {
                    AggregateError::AggregateExpression {
                        aggregate: layout.owner,
                        source,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let group_fields = &self.output_schema.fields()[..self.group_expressions.len()];
        let fold_count = self
            .calls
            .iter()
            .filter(|call| matches!(call, BoundCall::Fold { .. }))
            .count();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        let mut groups = self.groups.access(access)?;
        let mut entries = self.entries.access(access)?;
        let mut control = self.control.access(access)?;

        for row in 0..input.change.num_rows() {
            let difference = input.change.diffs().value(row);
            let group = encode_tuple(group_fields, &group_columns, row)?;
            let (existed, mut state) = if let Some(state) = groups.get(&group)? {
                (true, state)
            } else {
                if difference < 0 {
                    return Err(AggregateError::GroupWeightUnderflow.into());
                }
                let id = control.get()?.unwrap_or(0);
                let next = id.checked_add(1).ok_or(AggregateError::GroupIdExhausted)?;
                control.set(&next)?;
                (
                    false,
                    GroupState {
                        id,
                        weight: 0,
                        folds: self
                            .calls
                            .iter()
                            .filter_map(BoundCall::initial_fold_state)
                            .collect(),
                        extremes: vec![None; self.slots.len()].into_boxed_slice(),
                    },
                )
            };
            if state.folds.len() != fold_count || state.extremes.len() != self.slots.len() {
                return Err(AggregateError::InvalidState.into());
            }

            let old_output = existed
                .then(|| {
                    call_output(
                        &self.calls,
                        &self.layouts,
                        &self.slots,
                        &state.folds,
                        &state.extremes,
                        state.weight,
                    )
                })
                .transpose()?;

            state.weight = apply_weight(state.weight, difference, TrackedWeight::Group)?;

            for (layout_index, (layout, column)) in
                self.layouts.iter().zip(&layout_columns).enumerate()
            {
                let value = ScalarValue::try_from_array(column.as_ref(), row)?;
                // A NULL argument never enters the ordered partition, so it can
                // neither become nor retract an extreme.
                let Some(key) = order_key(&layout.field, &value).map_err(map_order_error)? else {
                    continue;
                };
                let partition_id = u32::try_from(layout_index)
                    .expect("the layout count is bounded by the aggregate call count");
                let change = entries
                    .partition(&EntryPartition::new(partition_id, state.id))?
                    .adjust(&key, difference)
                    .map_err(map_weight_error)?;
                // `Change` rejects zero differences, so an absent key here means
                // the key just entered the partition.
                if change.before() == 0 {
                    promote_cached_extreme(&self.slots, &mut state.extremes, layout_index, &key);
                }
                // A group that loses its last row is removed below, so a
                // partition re-read here would only be discarded.
                if change.after() == 0
                    && state.weight != 0
                    && caches_key(&self.slots, &state.extremes, layout_index, &key)
                {
                    let partition =
                        entries.partition(&EntryPartition::new(partition_id, state.id))?;
                    refresh_cached_extreme(
                        &self.slots,
                        &mut state.extremes,
                        layout_index,
                        &key,
                        &partition,
                    )?;
                }
            }

            for (aggregate, call) in self.calls.iter().enumerate() {
                if let BoundCall::Fold {
                    state: state_index,
                    reduction,
                    ..
                } = call
                {
                    // The persisted count was checked above; binding assigns dense indices.
                    let call_state = &mut state.folds[*state_index];
                    let values = scalar_tuple(&call_columns[aggregate], row)?;
                    reduction.apply(call_state, &values, difference, state.weight)?;
                }
            }

            if state.weight == 0 {
                output.push(
                    &group_columns,
                    row,
                    old_output.expect("an existing group has positive weight"),
                    -1,
                )?;
                drain_group_entries(self.layouts.len(), &mut entries, state.id)?;
                groups.remove(&group)?;
            } else {
                let new_output = call_output(
                    &self.calls,
                    &self.layouts,
                    &self.slots,
                    &state.folds,
                    &state.extremes,
                    state.weight,
                )?;
                match old_output {
                    None => output.push(&group_columns, row, new_output, 1)?,
                    Some(old_output) if old_output != new_output => {
                        output.push(&group_columns, row, old_output, -1)?;
                        output.push(&group_columns, row, new_output, 1)?;
                    }
                    Some(_) => {}
                }
                groups.put(&group, &state)?;
            }
        }

        Ok(output.finish(&self.output_schema)?)
    }
}

fn encode_tuple(
    fields: &[Arc<Field>],
    columns: &[ArrayRef],
    row: usize,
) -> Result<Vec<u8>, OperationError> {
    let mut encoded = Vec::new();
    for (field, column) in fields.iter().zip(columns) {
        encode_canonical(field, column.as_ref(), row, field.name(), &mut encoded)?;
    }
    Ok(encoded)
}

fn scalar_tuple(columns: &[ArrayRef], row: usize) -> Result<Vec<ScalarValue>, AggregateError> {
    columns
        .iter()
        .map(|column| {
            ScalarValue::try_from_array(column.as_ref(), row).map_err(AggregateError::from)
        })
        .collect()
}

fn call_output(
    calls: &[BoundCall],
    layouts: &[BoundLayout],
    slots: &[ExtremaSlot],
    fold_states: &[Vec<u8>],
    extremes: &[Option<Vec<u8>>],
    group_weight: u64,
) -> Result<Vec<ScalarValue>, AggregateError> {
    calls
        .iter()
        .map(|call| match call {
            BoundCall::Fold {
                state, reduction, ..
            } => reduction.output(&fold_states[*state], group_weight),
            BoundCall::Extrema { slot } => {
                let target = &slots[*slot];
                let layout = &layouts[target.layout];
                match extremes[*slot].as_deref() {
                    None => null(layout.field.data_type()),
                    Some(key) => ordered_value(&layout.field, key).map_err(map_order_error),
                }
            }
        })
        .collect()
}

/// Removes every key a dying group still owns.
///
/// The relaxed validation accepts a stream that drives a group's row count to
/// zero while one of its ordered partitions still holds keys (retracting a row
/// whose argument is NULL touches no partition). Group IDs are never reused, so
/// those keys would be unreachable and permanent. A stream that retracts the
/// rows it inserted empties each partition through its own adjustments, so this
/// scan normally finds nothing.
pub(super) fn drain_group_entries(
    layout_count: usize,
    entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
    group: u64,
) -> Result<(), AggregateError> {
    for layout_index in 0..layout_count {
        let partition_id = u32::try_from(layout_index)
            .expect("the layout count is bounded by the aggregate call count");
        let mut partition = entries.partition(&EntryPartition::new(partition_id, group))?;
        while let Some(entry) = partition.first()? {
            let removal = i64::try_from(entry.multiplicity).unwrap_or(i64::MAX);
            partition.adjust(&entry.key, -removal)?;
        }
    }
    Ok(())
}

/// Reports whether any slot of one layout currently caches the removed key.
///
/// A key that never was an extreme leaves every cache untouched, so the
/// partition does not need to be opened at all.
fn caches_key(
    slots: &[ExtremaSlot],
    extremes: &[Option<Vec<u8>>],
    layout: usize,
    key: &[u8],
) -> bool {
    slots
        .iter()
        .enumerate()
        .any(|(index, slot)| slot.layout == layout && extremes[index].as_deref() == Some(key))
}

/// Promotes a key that just entered its partition when it beats a cached extreme.
fn promote_cached_extreme(
    slots: &[ExtremaSlot],
    extremes: &mut [Option<Vec<u8>>],
    layout: usize,
    key: &[u8],
) {
    for (index, slot) in slots.iter().enumerate() {
        if slot.layout != layout {
            continue;
        }
        let better = match &extremes[index] {
            None => true,
            Some(current) => match slot.direction {
                ExtremaDirection::Min => key < current.as_slice(),
                ExtremaDirection::Max => key > current.as_slice(),
            },
        };
        if better {
            extremes[index] = Some(key.to_vec());
        }
    }
}

/// Re-reads a cached extreme whose key just left the partition.
///
/// The partition is the source of truth, so the retraction of the cached
/// extreme is the only case that pays for an ordered read.
fn refresh_cached_extreme(
    slots: &[ExtremaSlot],
    extremes: &mut [Option<Vec<u8>>],
    layout: usize,
    key: &[u8],
    partition: &MultisetPartition<'_, '_, Vec<u8>>,
) -> Result<(), AggregateError> {
    for (index, slot) in slots.iter().enumerate() {
        if slot.layout != layout || extremes[index].as_deref() != Some(key) {
            continue;
        }
        let entry = match slot.direction {
            ExtremaDirection::Min => partition.first()?,
            ExtremaDirection::Max => partition.last()?,
        };
        extremes[index] = entry.map(|entry| entry.key);
    }
    Ok(())
}

fn map_weight_error(error: StoreError) -> AggregateError {
    match error {
        StoreError::MultiplicityUnderflow => AggregateError::ExtremaWeightUnderflow,
        StoreError::MultiplicityOverflow => AggregateError::ArithmeticOverflow,
        source => AggregateError::Store(source),
    }
}

const fn map_order_error(_error: OrderError) -> AggregateError {
    AggregateError::InvalidState
}

impl OutputRows {
    fn new(column_count: usize) -> Self {
        Self {
            columns: (0..column_count).map(|_| Vec::new()).collect(),
            diffs: Vec::new(),
        }
    }

    fn push(
        &mut self,
        groups: &[ArrayRef],
        row: usize,
        calls: Vec<ScalarValue>,
        difference: i64,
    ) -> Result<(), AggregateError> {
        for (column, group) in self.columns.iter_mut().zip(groups) {
            column.push(ScalarValue::try_from_array(group.as_ref(), row)?);
        }
        for (column, value) in self.columns[groups.len()..].iter_mut().zip(calls) {
            column.push(value);
        }
        self.diffs.push(difference);
        Ok(())
    }

    fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, AggregateError> {
        if self.diffs.is_empty() {
            return Ok(None);
        }
        let columns = self
            .columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<_>, _>>()?;
        let records = RecordBatch::try_new(Arc::clone(schema), columns)?;
        Ok(Some(Change::try_new(
            records,
            Int64Array::from(self.diffs),
        )?))
    }
}
