use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{
    CellAccess, MapPartition, OrderedMapAccess, PartitionKey, StoreError, TransactionAccess,
};

use crate::{
    expression::BoundExpression,
    operation::{
        AtomicOperation, BudgetExceeded, OperationError, OperationInput, StepBudget,
        logical_array_bytes,
        relation::{OrderError, RowError, encode_canonical_bounded, order_key, ordered_value},
    },
};

use super::{
    AggregateCall, AggregateError,
    functions::{StatisticKind, TrackedWeight, apply_weight},
    state::{Control, Entries, EntryPartition, GroupState, Groups, Statistic},
    value::null,
};

/// Materialized grouped aggregate over an ordered difference stream.
pub(crate) struct AggregateOperation {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[BoundExpression]>,
    pub(super) calls: Box<[AggregateCall<usize>]>,
    pub(super) arguments: Box<[BoundArgument]>,
    pub(super) statistic_count: usize,
    pub(super) layout_count: usize,
    pub(super) extrema_count: usize,
    pub(super) groups: Groups,
    pub(super) entries: Entries,
    pub(super) control: Control,
}

pub(super) struct BoundArgument {
    pub(super) owner: usize,
    pub(super) expression: BoundExpression,
    pub(super) field: Arc<Field>,
    pub(super) statistic: Option<BoundStatistic>,
    pub(super) extrema: Option<BoundExtrema>,
}

pub(super) struct BoundStatistic {
    pub(super) index: usize,
    pub(super) kind: StatisticKind,
    pub(super) count_output: bool,
    pub(super) sum_output: bool,
}

/// Addresses assigned when an argument first needs an ordered partition or direction.
pub(super) struct BoundExtrema {
    pub(super) partition: usize,
    pub(super) min_slot: Option<usize>,
    pub(super) max_slot: Option<usize>,
}

struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    diffs: Vec<i64>,
}

/// Expression results materialized before any durable state is accessed.
struct EvaluatedColumns {
    groups: Vec<ArrayRef>,
    arguments: Vec<ArrayRef>,
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
        let arguments = operation
            .arguments
            .iter()
            .map(|argument| {
                argument.expression.evaluate(records).map_err(|source| {
                    AggregateError::AggregateExpression {
                        aggregate: argument.owner,
                        source,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { groups, arguments })
    }
}

impl PendingGroup {
    fn load(
        groups: &OrderedMapAccess<'_, Vec<u8>, GroupState>,
        key: Vec<u8>,
        continues: bool,
        budget: &mut StepBudget,
    ) -> Result<Self, OperationError> {
        let state = groups
            .get_bounded(&key, budget.remaining_bytes())
            .map_err(read_budget_error)?;
        if let Some(state) = &state {
            budget.charge(state.logical_bytes())?;
            if continues {
                budget.charge(state.logical_bytes())?;
            }
        }
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
        entries: &mut OrderedMapAccess<
            '_,
            PartitionKey<EntryPartition, Vec<u8>>,
            std::num::NonZeroU64,
        >,
        layout: u32,
        group: u64,
        key: &Vec<u8>,
        difference: i64,
        budget: &mut StepBudget,
    ) -> Result<(u64, u64), OperationError> {
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
            Self::flush_slot(slot, entries, layout, budget)?;
            return match entries
                .partition(&EntryPartition::new(layout, group))?
                .adjust(key, difference)
            {
                Err(error) => Err(map_weight_error(error).into()),
                Ok(_) => Err(AggregateError::InvalidState.into()),
            };
        }

        Self::flush_slot(slot, entries, layout, budget)?;
        let mut partition = entries.partition(&EntryPartition::new(layout, group))?;
        budget.charge(key.len().saturating_add(34))?;
        let before = partition.multiplicity(key)?;
        let Some(after) = checked_extrema_weight(before, difference) else {
            // The read validates persisted bytes, while Store still owns the
            // transaction poison on an invalid signed adjustment.
            return match partition.adjust(key, difference) {
                Err(error) => Err(map_weight_error(error).into()),
                Ok(_) => Err(AggregateError::InvalidState.into()),
            };
        };
        budget.charge(key.len())?;
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
        entries: &mut OrderedMapAccess<
            '_,
            PartitionKey<EntryPartition, Vec<u8>>,
            std::num::NonZeroU64,
        >,
        layout: u32,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        Self::flush_slot(&mut self.keys[layout as usize], entries, layout, budget)
    }

    fn flush_all(
        &mut self,
        entries: &mut OrderedMapAccess<
            '_,
            PartitionKey<EntryPartition, Vec<u8>>,
            std::num::NonZeroU64,
        >,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        for (layout, slot) in self.keys.iter_mut().enumerate() {
            Self::flush_slot(
                slot,
                entries,
                u32::try_from(layout).expect("layout count fits the persistent partition id"),
                budget,
            )?;
            *slot = None;
        }
        Ok(())
    }

    fn flush_slot(
        slot: &mut Option<PendingExtreme>,
        entries: &mut OrderedMapAccess<
            '_,
            PartitionKey<EntryPartition, Vec<u8>>,
            std::num::NonZeroU64,
        >,
        layout: u32,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        let Some(pending) = slot.as_mut() else {
            return Ok(());
        };
        if pending.current == pending.stored {
            return Ok(());
        }
        budget.charge(pending.key.len().saturating_add(34))?;
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

impl AggregateOperation {
    fn new_group_state(
        &self,
        control: &CellAccess<'_, u64>,
        next_group_id: &mut Option<u64>,
    ) -> Result<GroupState, AggregateError> {
        let id = match *next_group_id {
            Some(id) => id,
            None => control.get_bounded(size_of::<u64>())?.unwrap_or(0),
        };
        *next_group_id = Some(id.checked_add(1).ok_or(AggregateError::GroupIdExhausted)?);
        let mut statistics = vec![Statistic::Count(0); self.statistic_count];
        if self.statistic_count > 0 {
            for argument in &self.arguments {
                if let Some(statistic) = &argument.statistic {
                    statistics[statistic.index] = statistic.kind.empty();
                }
            }
        }
        Ok(GroupState {
            id,
            weight: 0,
            statistics,
            extremes: vec![None; self.extrema_count].into_boxed_slice(),
        })
    }

    fn group_output(&self, state: &GroupState) -> Result<Vec<ScalarValue>, AggregateError> {
        let statistic = |argument: usize| {
            let bound = self.arguments[argument]
                .statistic
                .as_ref()
                .expect("a statistical call assigned its argument a state slot");
            &state.statistics[bound.index]
        };
        self.calls
            .iter()
            .map(|call| match call {
                AggregateCall::CountAll => count_value(state.weight),
                AggregateCall::Count(argument) => count_value(statistic(*argument).count()),
                AggregateCall::Sum(argument) => statistic(*argument).sum(),
                AggregateCall::Avg(argument) => statistic(*argument).average(),
                AggregateCall::Min(argument) | AggregateCall::Max(argument) => {
                    let bound = &self.arguments[*argument];
                    let extrema = bound
                        .extrema
                        .as_ref()
                        .expect("an extrema call assigned its argument an ordered partition");
                    let slot = if matches!(call, AggregateCall::Min(_)) {
                        extrema.min_slot
                    } else {
                        extrema.max_slot
                    }
                    .expect("an extrema call assigned its direction a cache slot");
                    match state.extremes[slot].as_deref() {
                        None => null(bound.field.data_type()),
                        Some(key) => ordered_value(&bound.field, key).map_err(map_order_error),
                    }
                }
            })
            .collect()
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one event updates its state, pending keys and transaction using the shared budget"
    )]
    fn apply_extrema_row(
        &self,
        state: &mut GroupState,
        columns: &[ArrayRef],
        row: usize,
        difference: i64,
        pending: &mut PendingExtrema,
        entries: &mut OrderedMapAccess<
            '_,
            PartitionKey<EntryPartition, Vec<u8>>,
            std::num::NonZeroU64,
        >,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        for (argument_index, argument) in self.arguments.iter().enumerate() {
            let Some(extrema) = &argument.extrema else {
                continue;
            };
            // MIN/MAX accept flat scalars. Admit both the owned value and
            // its ordered key before either payload can be copied.
            budget.charge(
                logical_array_bytes(columns[argument_index].slice(row, 1).as_ref())
                    .saturating_mul(2)
                    .saturating_add(size_of::<ScalarValue>()),
            )?;
            let value = ScalarValue::try_from_array(columns[argument_index].as_ref(), row)?;
            // A NULL argument never enters the ordered partition, so it can
            // neither become nor retract an extreme.
            let Some(key) = order_key(&argument.field, &value).map_err(map_order_error)? else {
                continue;
            };
            let partition_id = u32::try_from(extrema.partition)
                .expect("the layout count is bounded by the aggregate call count");
            let (before, after) =
                pending.adjust(entries, partition_id, state.id, &key, difference, budget)?;
            // `Change` rejects zero differences, so an absent key here means
            // the key just entered the partition.
            if before == 0 {
                budget.charge(key.len().saturating_mul(2))?;
                promote_cached_extreme(extrema, &mut state.extremes, &key);
            }
            if after == 0 && caches_key(extrema, &state.extremes, &key) {
                pending.flush_layout(entries, partition_id, budget)?;
                let partition = entries.partition(&EntryPartition::new(partition_id, state.id))?;
                refresh_cached_extreme(extrema, &mut state.extremes, &key, &partition, budget)?;
            }
        }
        Ok(())
    }

    fn apply_statistics_row(
        &self,
        state: &mut GroupState,
        columns: &[ArrayRef],
        row: usize,
        difference: i64,
    ) -> Result<(), AggregateError> {
        for (argument_index, argument) in self.arguments.iter().enumerate() {
            let Some(bound) = &argument.statistic else {
                continue;
            };
            let statistic = &mut state.statistics[bound.index];
            if !matches!(
                (bound.kind, &*statistic),
                (StatisticKind::Count, Statistic::Count(_))
                    | (StatisticKind::Signed, Statistic::Signed { .. })
                    | (StatisticKind::Unsigned, Statistic::Unsigned { .. })
            ) {
                return Err(AggregateError::InvalidState);
            }
            let value = ScalarValue::try_from_array(columns[argument_index].as_ref(), row)?;
            statistic.apply(&value, difference)?;
            if bound.count_output {
                count_value(statistic.count())?;
            }
            if bound.sum_output {
                match statistic {
                    Statistic::Signed { sum, .. } => {
                        i64::try_from(*sum).map_err(|_| AggregateError::ArithmeticOverflow)?;
                    }
                    Statistic::Unsigned { sum, .. } => {
                        u64::try_from(*sum).map_err(|_| AggregateError::ArithmeticOverflow)?;
                    }
                    Statistic::Count(_) => return Err(AggregateError::InvalidState),
                }
            }
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
        &self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Option<Change>, OperationError> {
        if input.port != 0 {
            return Err(AggregateError::InvalidInputPort { port: input.port }.into());
        }
        if input.change.records().schema_ref() != &self.input_schema {
            return Err(AggregateError::InputSchemaMismatch.into());
        }

        let columns = EvaluatedColumns::evaluate(self, input.change.records())?;
        for column in columns.groups.iter().chain(&columns.arguments) {
            budget.charge(logical_array_bytes(column.as_ref()))?;
        }

        let group_fields = &self.output_schema.fields()[..self.group_expressions.len()];
        let mut output = OutputRows::new(self.output_schema.fields().len());
        let mut groups = self.groups.access(access)?;
        let mut entries = self.entries.access(access)?;
        let mut control = self.control.access(access)?;
        let mut next_group_id = None;
        let mut pending_group: Option<PendingGroup> = None;
        let mut pending_extrema = PendingExtrema::new(self.layout_count);
        let row_count = input.change.num_rows();
        let mut next_group = if row_count == 0 {
            None
        } else {
            Some(encode_tuple(group_fields, &columns.groups, 0, budget))
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
                pending_extrema.flush_all(&mut entries, budget)?;
                if let Some(pending) = pending_group.take() {
                    flush_pending_group(&mut groups, pending, budget)?;
                }
            }
            next_group = if row + 1 < row_count {
                Some(encode_tuple(group_fields, &columns.groups, row + 1, budget))
            } else {
                None
            };
            if starts_run {
                let continues = matches!(
                    next_group.as_ref(),
                    Some(Ok(next)) if next.as_slice() == group.as_slice()
                );
                pending_group = Some(PendingGroup::load(&groups, group, continues, budget)?);
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
                None => {
                    budget.charge(
                        self.statistic_count
                            .saturating_mul(32)
                            .saturating_add(self.extrema_count * 8 + 24),
                    )?;
                    self.new_group_state(&control, &mut next_group_id)?
                }
            };
            if state.statistics.len() != self.statistic_count
                || state.extremes.len() != self.extrema_count
            {
                return Err(AggregateError::InvalidState.into());
            }

            let old_output = if existed {
                budget.charge(state.logical_bytes().saturating_add(self.calls.len() * 64))?;
                Some(self.group_output(&state)?)
            } else {
                None
            };

            state.weight = apply_weight(state.weight, difference, TrackedWeight::Group)?;
            if self.layout_count > 0 {
                self.apply_extrema_row(
                    &mut state,
                    &columns.arguments,
                    row,
                    difference,
                    &mut pending_extrema,
                    &mut entries,
                    budget,
                )?;
            }
            if self.statistic_count > 0 {
                self.apply_statistics_row(&mut state, &columns.arguments, row, difference)?;
            }

            if state.weight == 0 {
                output.push(
                    &columns.groups,
                    row,
                    old_output.expect("an existing group has positive weight"),
                    -1,
                    budget,
                )?;
                if state
                    .statistics
                    .iter()
                    .any(|statistic| !statistic.is_empty())
                    || state.extremes.iter().any(Option::is_some)
                {
                    return Err(AggregateError::InvalidState.into());
                }
                pending_extrema.flush_all(&mut entries, budget)?;
            } else {
                budget.charge(state.logical_bytes().saturating_add(self.calls.len() * 64))?;
                let new_output = self.group_output(&state)?;
                match old_output {
                    None => output.push(&columns.groups, row, new_output, 1, budget)?,
                    Some(old_output) if old_output != new_output => {
                        output.push(&columns.groups, row, old_output, -1, budget)?;
                        output.push(&columns.groups, row, new_output, 1, budget)?;
                    }
                    Some(_) => {}
                }
                pending.state = Some(state);
            }
        }
        pending_extrema.flush_all(&mut entries, budget)?;
        if let Some(pending) = pending_group {
            flush_pending_group(&mut groups, pending, budget)?;
        }
        if let Some(next_group_id) = next_group_id {
            budget.charge(8)?;
            control.set(&next_group_id)?;
        }

        output.finish(&self.output_schema, budget)
    }
}

fn flush_pending_group(
    groups: &mut OrderedMapAccess<'_, Vec<u8>, GroupState>,
    pending: PendingGroup,
    budget: &mut StepBudget,
) -> Result<(), OperationError> {
    if pending
        .comparison_baseline
        .as_ref()
        .is_some_and(|baseline| pending.state.as_ref() == Some(baseline))
    {
        return Ok(());
    }
    match pending.state {
        Some(state) => {
            budget.charge(pending.key.len().saturating_add(state.logical_bytes()))?;
            groups.put(&pending.key, &state)?;
        }
        None if pending.was_present => {
            budget.charge(pending.key.len())?;
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
    budget: &mut StepBudget,
) -> Result<Vec<u8>, OperationError> {
    let mut encoded = Vec::new();
    for (field, column) in fields.iter().zip(columns) {
        let before = encoded.len();
        let result = encode_canonical_bounded(
            field,
            column.as_ref(),
            row,
            field.name(),
            &mut encoded,
            before.saturating_add(budget.remaining_bytes()),
        );
        budget.charge(encoded.len() - before)?;
        result.map_err(|error| -> OperationError {
            match error {
                RowError::SizeLimit { .. } => Box::new(BudgetExceeded),
                error => Box::new(error),
            }
        })?;
    }
    Ok(encoded)
}

fn count_value(count: u64) -> Result<ScalarValue, AggregateError> {
    Ok(ScalarValue::Int64(Some(
        i64::try_from(count).map_err(|_| AggregateError::ArithmeticOverflow)?,
    )))
}

/// Reports whether either slot of one layout currently caches the removed key.
///
/// A key that never was an extreme leaves both caches untouched, so the
/// partition does not need to be opened at all.
fn caches_key(layout: &BoundExtrema, extremes: &[Option<Vec<u8>>], key: &[u8]) -> bool {
    [layout.min_slot, layout.max_slot]
        .into_iter()
        .flatten()
        .any(|slot| extremes[slot].as_deref() == Some(key))
}

/// Promotes a key that just entered its partition when it beats a cached extreme.
fn promote_cached_extreme(layout: &BoundExtrema, extremes: &mut [Option<Vec<u8>>], key: &[u8]) {
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
    layout: &BoundExtrema,
    extremes: &mut [Option<Vec<u8>>],
    key: &[u8],
    partition: &MapPartition<'_, '_, Vec<u8>, std::num::NonZeroU64>,
    budget: &mut StepBudget,
) -> Result<(), OperationError> {
    if let Some(slot) = layout.min_slot
        && extremes[slot].as_deref() == Some(key)
    {
        extremes[slot] = read_extreme(partition.first_bounded(budget.remaining_bytes()), budget)?;
    }
    if let Some(slot) = layout.max_slot
        && extremes[slot].as_deref() == Some(key)
    {
        extremes[slot] = read_extreme(partition.last_bounded(budget.remaining_bytes()), budget)?;
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
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        budget.charge(
            calls
                .iter()
                .map(ScalarValue::size)
                .sum::<usize>()
                .saturating_add(8),
        )?;
        for (column, group) in self.columns.iter_mut().zip(groups) {
            let value = ScalarValue::try_from_array(group.as_ref(), row)?;
            budget.charge(value.size())?;
            column.push(value);
        }
        for (column, value) in self.columns[groups.len()..].iter_mut().zip(calls) {
            column.push(value);
        }
        self.diffs.push(difference);
        Ok(())
    }

    fn finish(
        self,
        schema: &SchemaRef,
        budget: &mut StepBudget,
    ) -> Result<Option<Change>, OperationError> {
        if self.diffs.is_empty() {
            return Ok(None);
        }
        budget.charge(
            self.columns
                .iter()
                .flatten()
                .map(ScalarValue::size)
                .sum::<usize>()
                .saturating_add(self.diffs.len() * 8),
        )?;
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

fn read_budget_error(error: StoreError) -> OperationError {
    match error {
        StoreError::ItemTooLarge { .. } | StoreError::InvalidScanLimit => Box::new(BudgetExceeded),
        error => Box::new(error),
    }
}

fn read_extreme(
    result: Result<Option<(Vec<u8>, std::num::NonZeroU64)>, StoreError>,
    budget: &mut StepBudget,
) -> Result<Option<Vec<u8>>, OperationError> {
    let entry = result.map_err(read_budget_error)?;
    if let Some(entry) = &entry {
        budget.charge(entry.0.len().saturating_add(34))?;
    }
    Ok(entry.map(|entry| entry.0))
}

#[cfg(test)]
mod encoding_tests {
    use super::*;
    use arrow_array::BinaryArray;
    use arrow_schema::DataType;

    #[test]
    fn rejected_group_encoding_charges_the_prefix_already_copied() {
        let fields =
            ["first", "second"].map(|name| Arc::new(Field::new(name, DataType::Binary, false)));
        let columns: [ArrayRef; 2] = [
            Arc::new(BinaryArray::from(vec![vec![1; 64].as_slice()])),
            Arc::new(BinaryArray::from(vec![vec![2; 64].as_slice()])),
        ];
        let mut budget = StepBudget::new(1, 100);
        let error = encode_tuple(&fields, &columns, 0, &mut budget).unwrap_err();
        assert!(error.is::<BudgetExceeded>());
        // First value is complete; the second has its null marker and length.
        assert_eq!(budget.remaining_bytes(), 100 - (1 + 8 + 64) - (1 + 8));
    }
}
