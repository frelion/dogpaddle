use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{
    CellAccess, MultisetPartition, OrderedMapAccess, PartitionedMultisetAccess, StoreError,
    TransactionAccess,
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
    functions::{Fold, TrackedWeight, apply_weight},
    state::{Control, Entries, EntryPartition, GroupState, Groups},
    value::null,
};

/// Materialized grouped aggregate over an ordered difference stream.
pub(crate) struct AggregateOperation {
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
    pub(super) min_slot: Option<usize>,
    pub(super) max_slot: Option<usize>,
}

/// One distinct (layout, direction) pair whose extreme is cached per group.
///
/// `MIN(x), MAX(x)` share one layout and need two slots; repeating the same
/// call needs one. Caching only the directions that are bound keeps the cached
/// state proportional to what the definition actually reads.
pub(super) struct ExtremaSlot {
    pub(super) layout: usize,
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

/// Expression results materialized before any durable state is accessed.
struct EvaluatedColumns {
    groups: Vec<ArrayRef>,
    calls: Vec<Vec<ArrayRef>>,
    layouts: Vec<ArrayRef>,
}

/// State for one contiguous run of the same canonical group key.
struct PendingGroup {
    key: Vec<u8>,
    was_present: bool,
    /// Original state cloned only when a multi-row run may end unchanged.
    comparison_baseline: Option<GroupState>,
    state: Option<GroupState>,
}

/// One key per layout is enough to collapse repeated adjacent arguments while
/// keeping memory independent of the number of rows in a Change.
struct PendingExtrema {
    keys: Vec<Option<PendingExtreme>>,
}

struct PendingExtreme {
    group: u64,
    key: Vec<u8>,
    stored: u64,
    current: u64,
}

impl EvaluatedColumns {
    fn evaluate(
        operation: &AggregateOperation,
        records: &RecordBatch,
    ) -> Result<Self, AggregateError> {
        let groups = operation
            .group_expressions
            .iter()
            .enumerate()
            .map(|(group, expression)| {
                expression
                    .evaluate(records)
                    .map_err(|source| AggregateError::GroupExpression { group, source })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let calls = operation
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
        let layouts = operation
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
        Ok(Self {
            groups,
            calls,
            layouts,
        })
    }
}

impl PendingGroup {
    fn load(
        groups: &OrderedMapAccess<'_, Vec<u8>, GroupState>,
        key: Vec<u8>,
        continues: bool,
    ) -> Result<Self, StoreError> {
        let state = groups.get(&key)?;
        Ok(Self {
            key,
            was_present: state.is_some(),
            comparison_baseline: if continues { state.clone() } else { None },
            state,
        })
    }
}

impl PendingExtrema {
    fn new(layouts: usize) -> Self {
        Self {
            keys: (0..layouts).map(|_| None).collect(),
        }
    }

    fn adjust(
        &mut self,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
        layout: u32,
        group: u64,
        key: &Vec<u8>,
        difference: i64,
    ) -> Result<(u64, u64), AggregateError> {
        let slot = &mut self.keys[layout as usize];
        if slot
            .as_ref()
            .is_some_and(|pending| pending.group == group && pending.key == *key)
        {
            let pending = slot.as_mut().expect("the matching key is present");
            let before = pending.current;
            if let Some(after) = checked_extrema_weight(before, difference) {
                pending.current = after;
                return Ok((before, after));
            }
            // Materialize the earlier valid prefix before asking Store to
            // reject this event. Its checked adjustment poisons the transaction
            // exactly as it did on the per-row path.
            Self::flush_slot(slot, entries, layout)?;
            return match entries
                .partition(&EntryPartition::new(layout, group))?
                .adjust(key, difference)
            {
                Err(error) => Err(map_weight_error(error)),
                Ok(_) => Err(AggregateError::InvalidState),
            };
        }

        Self::flush_slot(slot, entries, layout)?;
        let mut partition = entries.partition(&EntryPartition::new(layout, group))?;
        let before = partition.multiplicity(key)?;
        let Some(after) = checked_extrema_weight(before, difference) else {
            // The read validates persisted bytes, while Store still owns the
            // transaction poison on an invalid signed adjustment.
            return match partition.adjust(key, difference) {
                Err(error) => Err(map_weight_error(error)),
                Ok(_) => Err(AggregateError::InvalidState),
            };
        };
        *slot = Some(PendingExtreme {
            group,
            key: key.clone(),
            stored: before,
            current: after,
        });
        Ok((before, after))
    }

    fn flush_layout(
        &mut self,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
        layout: u32,
    ) -> Result<(), AggregateError> {
        Self::flush_slot(&mut self.keys[layout as usize], entries, layout)
    }

    fn flush_all(
        &mut self,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
    ) -> Result<(), AggregateError> {
        for (layout, slot) in self.keys.iter_mut().enumerate() {
            Self::flush_slot(
                slot,
                entries,
                u32::try_from(layout).expect("layout count fits the persistent partition id"),
            )?;
            *slot = None;
        }
        Ok(())
    }

    fn discard_group(
        &mut self,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
        group: u64,
    ) -> Result<(), AggregateError> {
        for (layout, slot) in self.keys.iter_mut().enumerate() {
            let Some(pending) = slot.take() else {
                continue;
            };
            debug_assert_eq!(pending.group, group);
            if pending.stored == 0 {
                continue;
            }
            let layout =
                u32::try_from(layout).expect("layout count fits the persistent partition id");
            entries
                .partition(&EntryPartition::new(layout, group))?
                .set_multiplicity(&pending.key, 0)?;
        }
        Ok(())
    }

    fn flush_slot(
        slot: &mut Option<PendingExtreme>,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
        layout: u32,
    ) -> Result<(), AggregateError> {
        let Some(pending) = slot.as_mut() else {
            return Ok(());
        };
        if pending.current == pending.stored {
            return Ok(());
        }
        entries
            .partition(&EntryPartition::new(layout, pending.group))?
            .set_multiplicity(&pending.key, pending.current)?;
        pending.stored = pending.current;
        Ok(())
    }
}

fn checked_extrema_weight(before: u64, difference: i64) -> Option<u64> {
    if difference > 0 {
        before.checked_add(difference.unsigned_abs())
    } else {
        before.checked_sub(difference.unsigned_abs())
    }
}

impl BoundCall {
    fn initial_fold_state(&self) -> Option<Vec<u8>> {
        match self {
            Self::Fold { reduction, .. } => Some(reduction.empty()),
            Self::Extrema { .. } => None,
        }
    }
}

impl AggregateOperation {
    fn new_group_state(
        &self,
        control: &CellAccess<'_, u64>,
        next_group_id: &mut Option<u64>,
    ) -> Result<GroupState, AggregateError> {
        let id = match *next_group_id {
            Some(id) => id,
            None => control.get()?.unwrap_or(0),
        };
        *next_group_id = Some(id.checked_add(1).ok_or(AggregateError::GroupIdExhausted)?);
        Ok(GroupState {
            id,
            weight: 0,
            folds: self
                .calls
                .iter()
                .filter_map(BoundCall::initial_fold_state)
                .collect(),
            extremes: vec![None; self.slots.len()].into_boxed_slice(),
        })
    }

    fn group_output(&self, state: &GroupState) -> Result<Vec<ScalarValue>, AggregateError> {
        self.calls
            .iter()
            .map(|call| match call {
                BoundCall::Fold {
                    state: state_index,
                    reduction,
                    ..
                } => reduction.output(&state.folds[*state_index], state.weight),
                BoundCall::Extrema { slot } => {
                    let target = &self.slots[*slot];
                    let layout = &self.layouts[target.layout];
                    match state.extremes[*slot].as_deref() {
                        None => null(layout.field.data_type()),
                        Some(key) => ordered_value(&layout.field, key).map_err(map_order_error),
                    }
                }
            })
            .collect()
    }

    fn apply_extrema_row(
        &self,
        state: &mut GroupState,
        columns: &[ArrayRef],
        row: usize,
        difference: i64,
        pending: &mut PendingExtrema,
        entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
    ) -> Result<(), AggregateError> {
        for (layout_index, (layout, column)) in self.layouts.iter().zip(columns).enumerate() {
            let value = ScalarValue::try_from_array(column.as_ref(), row)?;
            // A NULL argument never enters the ordered partition, so it can
            // neither become nor retract an extreme.
            let Some(key) = order_key(&layout.field, &value).map_err(map_order_error)? else {
                continue;
            };
            let partition_id = u32::try_from(layout_index)
                .expect("the layout count is bounded by the aggregate call count");
            let (before, after) =
                pending.adjust(entries, partition_id, state.id, &key, difference)?;
            // `Change` rejects zero differences, so an absent key here means
            // the key just entered the partition.
            if before == 0 {
                promote_cached_extreme(layout, &mut state.extremes, &key);
            }
            // A group that loses its last row is removed by the caller, so a
            // partition re-read here would only be discarded.
            if after == 0 && state.weight != 0 && caches_key(layout, &state.extremes, &key) {
                pending.flush_layout(entries, partition_id)?;
                let partition = entries.partition(&EntryPartition::new(partition_id, state.id))?;
                refresh_cached_extreme(layout, &mut state.extremes, &key, &partition)?;
            }
        }
        Ok(())
    }

    fn apply_fold_row(
        &self,
        state: &mut GroupState,
        columns: &[Vec<ArrayRef>],
        row: usize,
        difference: i64,
    ) -> Result<(), AggregateError> {
        for (aggregate, call) in self.calls.iter().enumerate() {
            let BoundCall::Fold {
                state: state_index,
                reduction,
                ..
            } = call
            else {
                continue;
            };
            // Binding assigns dense indices after persisted shape validation.
            let call_state = &mut state.folds[*state_index];
            let values = scalar_tuple(&columns[aggregate], row)?;
            reduction.apply(call_state, &values, difference, state.weight)?;
        }
        Ok(())
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

        let columns = EvaluatedColumns::evaluate(self, input.change.records())?;

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
        let mut next_group_id = None;
        let mut pending_group: Option<PendingGroup> = None;
        let mut pending_extrema = PendingExtrema::new(self.layouts.len());
        let row_count = input.change.num_rows();
        let mut next_group = if row_count == 0 {
            None
        } else {
            Some(encode_tuple(group_fields, &columns.groups, 0))
        };

        for row in 0..row_count {
            let difference = input.change.diffs().value(row);
            let group = next_group
                .take()
                .expect("the current row key was encoded")?;
            let starts_run = pending_group
                .as_ref()
                .is_none_or(|pending| pending.key != group);
            if starts_run {
                pending_extrema.flush_all(&mut entries)?;
                if let Some(pending) = pending_group.take() {
                    flush_pending_group(&mut groups, pending)?;
                }
            }
            next_group = if row + 1 < row_count {
                Some(encode_tuple(group_fields, &columns.groups, row + 1))
            } else {
                None
            };
            if starts_run {
                let continues = matches!(
                    next_group.as_ref(),
                    Some(Ok(next)) if next.as_slice() == group.as_slice()
                );
                pending_group = Some(PendingGroup::load(&groups, group, continues)?);
            }
            let pending = pending_group
                .as_mut()
                .expect("the current group was loaded above");
            let existed = pending.state.is_some();
            let mut state = match pending.state.take() {
                Some(state) => state,
                None if difference < 0 => {
                    return Err(AggregateError::GroupWeightUnderflow.into());
                }
                None => self.new_group_state(&control, &mut next_group_id)?,
            };
            if state.folds.len() != fold_count || state.extremes.len() != self.slots.len() {
                return Err(AggregateError::InvalidState.into());
            }

            let old_output = if existed {
                Some(self.group_output(&state)?)
            } else {
                None
            };

            state.weight = apply_weight(state.weight, difference, TrackedWeight::Group)?;
            self.apply_extrema_row(
                &mut state,
                &columns.layouts,
                row,
                difference,
                &mut pending_extrema,
                &mut entries,
            )?;
            self.apply_fold_row(&mut state, &columns.calls, row, difference)?;

            if state.weight == 0 {
                output.push(
                    &columns.groups,
                    row,
                    old_output.expect("an existing group has positive weight"),
                    -1,
                )?;
                // Delete known persisted keys directly and drop keys that only
                // existed in the pending cache. The drain handles older keys.
                pending_extrema.discard_group(&mut entries, state.id)?;
                drain_group_entries(self.layouts.len(), &mut entries, state.id)?;
            } else {
                let new_output = self.group_output(&state)?;
                match old_output {
                    None => output.push(&columns.groups, row, new_output, 1)?,
                    Some(old_output) if old_output != new_output => {
                        output.push(&columns.groups, row, old_output, -1)?;
                        output.push(&columns.groups, row, new_output, 1)?;
                    }
                    Some(_) => {}
                }
                pending.state = Some(state);
            }
        }
        pending_extrema.flush_all(&mut entries)?;
        if let Some(pending) = pending_group {
            flush_pending_group(&mut groups, pending)?;
        }
        if let Some(next_group_id) = next_group_id {
            control.set(&next_group_id)?;
        }

        Ok(output.finish(&self.output_schema)?)
    }
}

fn flush_pending_group(
    groups: &mut OrderedMapAccess<'_, Vec<u8>, GroupState>,
    pending: PendingGroup,
) -> Result<(), AggregateError> {
    if pending
        .comparison_baseline
        .as_ref()
        .is_some_and(|baseline| pending.state.as_ref() == Some(baseline))
    {
        return Ok(());
    }
    match pending.state {
        Some(state) => groups.put(&pending.key, &state)?,
        None if pending.was_present => {
            groups.erase(&pending.key)?;
        }
        None => {}
    }
    Ok(())
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
            partition.set_multiplicity(&entry.key, 0)?;
        }
    }
    Ok(())
}

/// Reports whether either slot of one layout currently caches the removed key.
///
/// A key that never was an extreme leaves both caches untouched, so the
/// partition does not need to be opened at all.
fn caches_key(layout: &BoundLayout, extremes: &[Option<Vec<u8>>], key: &[u8]) -> bool {
    [layout.min_slot, layout.max_slot]
        .into_iter()
        .flatten()
        .any(|slot| extremes[slot].as_deref() == Some(key))
}

/// Promotes a key that just entered its partition when it beats a cached extreme.
fn promote_cached_extreme(layout: &BoundLayout, extremes: &mut [Option<Vec<u8>>], key: &[u8]) {
    if let Some(slot) = layout.min_slot
        && extremes[slot]
            .as_deref()
            .is_none_or(|current| key < current)
    {
        extremes[slot] = Some(key.to_vec());
    }
    if let Some(slot) = layout.max_slot
        && extremes[slot]
            .as_deref()
            .is_none_or(|current| key > current)
    {
        extremes[slot] = Some(key.to_vec());
    }
}

/// Re-reads a cached extreme whose key just left the partition.
///
/// The partition is the source of truth, so the retraction of the cached
/// extreme is the only case that pays for an ordered read.
fn refresh_cached_extreme(
    layout: &BoundLayout,
    extremes: &mut [Option<Vec<u8>>],
    key: &[u8],
    partition: &MultisetPartition<'_, '_, Vec<u8>>,
) -> Result<(), AggregateError> {
    if let Some(slot) = layout.min_slot
        && extremes[slot].as_deref() == Some(key)
    {
        extremes[slot] = partition.first()?.map(|entry| entry.key);
    }
    if let Some(slot) = layout.max_slot
        && extremes[slot].as_deref() == Some(key)
    {
        extremes[slot] = partition.last()?.map(|entry| entry.key);
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
