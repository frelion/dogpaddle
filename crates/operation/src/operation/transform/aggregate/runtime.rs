use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{PartitionedMultisetAccess, StoreError, TransactionAccess};

use crate::{
    expression::BoundExpression,
    operation::{
        AtomicOperation, OperationError, OperationInput,
        relation::{canonical_row, encode_canonical},
    },
};

use super::{
    AggregateError,
    functions::{ExtremaDirection, Fold, apply_weight},
    state::{Control, Entries, EntryPartition, GroupState, Groups},
    value::{null, order_key, ordered_value},
};

const ADMISSION_LAYOUT: u32 = 0;

/// Materialized exact grouped aggregate.
pub struct AggregateOperation {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[BoundExpression]>,
    pub(super) calls: Box<[BoundCall]>,
    pub(super) layouts: Box<[BoundLayout]>,
    pub(super) groups: Groups,
    pub(super) entries: Entries,
    pub(super) control: Control,
}

pub(super) enum BoundCall {
    Fold {
        state: usize,
        arguments: Box<[BoundExpression]>,
        reduction: Box<dyn Fold>,
    },
    Extrema {
        layout: usize,
        direction: ExtremaDirection,
    },
}

pub(super) struct BoundLayout {
    pub(super) id: u32,
    pub(super) owner: usize,
    pub(super) field: Arc<Field>,
    pub(super) expression: BoundExpression,
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

    pub(super) const fn extrema(layout: usize, direction: ExtremaDirection) -> Self {
        Self::Extrema { layout, direction }
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
                    return Err(AggregateError::NegativeWeight.into());
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
                    },
                )
            };
            if state.folds.len() != fold_count {
                return Err(AggregateError::InvalidState.into());
            }

            let old_output = existed
                .then(|| {
                    call_output(
                        &self.calls,
                        &self.layouts,
                        &state.folds,
                        state.weight,
                        &mut entries,
                        state.id,
                    )
                })
                .transpose()?;

            let input_row = canonical_row(records, row)?;
            entries
                .partition(&EntryPartition::new(ADMISSION_LAYOUT, state.id))?
                .adjust(&input_row, difference)
                .map(|_| ())
                .map_err(map_weight_error)?;
            state.weight = apply_weight(state.weight, difference)?;

            for (layout, column) in self.layouts.iter().zip(&layout_columns) {
                let value = ScalarValue::try_from_array(column.as_ref(), row)?;
                if let Some(key) = order_key(&layout.field, &value)? {
                    entries
                        .partition(&EntryPartition::new(layout.id, state.id))?
                        .adjust(&key, difference)
                        .map(|_| ())
                        .map_err(map_weight_error)?;
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
                groups.remove(&group)?;
            } else {
                let new_output = call_output(
                    &self.calls,
                    &self.layouts,
                    &state.folds,
                    state.weight,
                    &mut entries,
                    state.id,
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
    fold_states: &[Vec<u8>],
    group_weight: u64,
    entries: &mut PartitionedMultisetAccess<'_, EntryPartition, Vec<u8>>,
    group: u64,
) -> Result<Vec<ScalarValue>, AggregateError> {
    calls
        .iter()
        .map(|call| match call {
            BoundCall::Fold {
                state, reduction, ..
            } => reduction.output(&fold_states[*state], group_weight),
            BoundCall::Extrema { layout, direction } => {
                let layout = &layouts[*layout];
                let partition = entries.partition(&EntryPartition::new(layout.id, group))?;
                let entry = match direction {
                    ExtremaDirection::Min => partition.first()?,
                    ExtremaDirection::Max => partition.last()?,
                };
                entry.map_or_else(
                    || null(layout.field.data_type()),
                    |entry| ordered_value(&layout.field, &entry.key),
                )
            }
        })
        .collect()
}

fn map_weight_error(error: StoreError) -> AggregateError {
    match error {
        StoreError::MultiplicityUnderflow => AggregateError::NegativeWeight,
        StoreError::MultiplicityOverflow => AggregateError::ArithmeticOverflow,
        source => AggregateError::Store(source),
    }
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
