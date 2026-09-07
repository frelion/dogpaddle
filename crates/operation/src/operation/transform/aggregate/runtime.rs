use std::{ops::RangeInclusive, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{OrderedMapAccess, ScanDirection, ScanLimit, TransactionAccess};

use crate::{
    expression::BoundExpression,
    operation::{
        Action, OperationError, OperationInput, TransactionalOperation,
        relation::{
            CollisionBucket, RowWeightError, canonical_row, encode_canonical, row_digest,
            update_bucket,
        },
    },
};

use super::{
    AggregateError,
    functions::{Fold, Indexed, apply_weight},
    state::{Control, Entries, EntryKey, GroupBucket, GroupEntry, Groups},
    value::scalars,
};

const ADMISSION_LAYOUT: u32 = 0;

/// Materialized exact grouped aggregate.
///
/// The runtime owns one group map, one unified entry map, and one group-ID
/// cell. Function implementations receive bounded argument tuples and never
/// receive Store access.
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
        arguments: Box<[BoundExpression]>,
        reduction: Box<dyn Fold>,
    },
    Indexed {
        layout: usize,
        reduction: Box<dyn Indexed>,
    },
}

pub(super) struct BoundLayout {
    pub(super) id: u32,
    pub(super) owner: usize,
    pub(super) fields: Box<[Arc<Field>]>,
    pub(super) expressions: Box<[BoundExpression]>,
}

struct IndexChange {
    values: Vec<ScalarValue>,
    encoded: Vec<u8>,
    presence: Option<i64>,
}

struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    diffs: Vec<i64>,
}

impl BoundCall {
    pub(super) fn fold(arguments: Box<[BoundExpression]>, reduction: Box<dyn Fold>) -> Self {
        Self::Fold {
            arguments,
            reduction,
        }
    }

    pub(super) fn indexed(layout: usize, reduction: Box<dyn Indexed>) -> Self {
        Self::Indexed { layout, reduction }
    }

    fn empty(&self) -> Vec<u8> {
        match self {
            Self::Fold { reduction, .. } => reduction.empty(),
            Self::Indexed { reduction, .. } => reduction.empty(),
        }
    }

    fn output(&self, state: &[u8], group_weight: u64) -> Result<ScalarValue, AggregateError> {
        match self {
            Self::Fold { reduction, .. } => reduction.output(state, group_weight),
            Self::Indexed { reduction, .. } => reduction.output(state),
        }
    }
}

impl TransactionalOperation for AggregateOperation {
    #[expect(
        clippy::too_many_lines,
        reason = "one loop keeps each ordered input event and its atomic state transition together"
    )]
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(AggregateError::MissingInput)?;
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
                BoundCall::Indexed { .. } => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let layout_columns = self
            .layouts
            .iter()
            .map(|layout| {
                layout
                    .expressions
                    .iter()
                    .map(|expression| {
                        expression.evaluate(records).map_err(|source| {
                            AggregateError::AggregateExpression {
                                aggregate: layout.owner,
                                source,
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let group_fields = &self.output_schema.fields()[..self.group_expressions.len()];
        let mut output = OutputRows::new(self.output_schema.fields().len());
        let mut groups = self.groups.access(access)?;
        let mut entries = self.entries.access(access)?;
        let mut control = self.control.access(access)?;

        for row in 0..input.change.num_rows() {
            let difference = input.change.diffs().value(row);
            let group = encode_tuple(group_fields, &group_columns, row)?;
            let digest = row_digest(&group);
            let mut bucket = groups.get(&digest)?;
            let existed = bucket
                .as_mut()
                .and_then(|bucket| bucket.get_mut(&group))
                .is_some();
            if !existed {
                if difference < 0 {
                    return Err(AggregateError::NegativeWeight.into());
                }
                let id = control.get()?.unwrap_or(0);
                let next = id.checked_add(1).ok_or(AggregateError::GroupIdExhausted)?;
                control.set(&next)?;
                let entry = GroupEntry {
                    group: group.clone(),
                    id,
                    weight: 0,
                    calls: self.calls.iter().map(BoundCall::empty).collect(),
                };
                if let Some(bucket) = bucket.as_mut() {
                    bucket.insert(entry);
                } else {
                    bucket = Some(GroupBucket::one(entry));
                }
            }

            let remove_group;
            {
                let entry = bucket
                    .as_mut()
                    .and_then(|bucket| bucket.get_mut(&group))
                    .expect("the current group was found or inserted");
                if entry.calls.len() != self.calls.len() {
                    return Err(AggregateError::InvalidState.into());
                }
                let old_weight = entry.weight;
                let old_output = existed
                    .then(|| call_output(&self.calls, &entry.calls, old_weight))
                    .transpose()?;

                let input_row = canonical_row(records, row)?;
                update_entry(
                    &mut entries,
                    ADMISSION_LAYOUT,
                    entry.id,
                    input_row,
                    difference,
                )?;
                entry.weight = apply_weight(entry.weight, difference)?;

                let mut index_changes = Vec::with_capacity(self.layouts.len());
                for (layout, columns) in self.layouts.iter().zip(&layout_columns) {
                    let values = scalar_tuple(columns, row)?;
                    let encoded = encode_tuple(&layout.fields, columns, row)?;
                    let presence = update_entry(
                        &mut entries,
                        layout.id,
                        entry.id,
                        encoded.clone(),
                        difference,
                    )?;
                    index_changes.push(IndexChange {
                        values,
                        encoded,
                        presence,
                    });
                }

                for (aggregate, (call, state)) in
                    self.calls.iter().zip(&mut entry.calls).enumerate()
                {
                    match call {
                        BoundCall::Fold { reduction, .. } => {
                            let values = scalar_tuple(&call_columns[aggregate], row)?;
                            reduction.apply(state, &values, difference, entry.weight)?;
                        }
                        BoundCall::Indexed { layout, reduction } => {
                            let change = &index_changes[*layout];
                            if reduction.change(
                                state,
                                &change.values,
                                &change.encoded,
                                change.presence,
                            )? {
                                scan_layout(
                                    &entries,
                                    &self.layouts[*layout],
                                    entry.id,
                                    reduction.as_ref(),
                                    state,
                                )?;
                            }
                        }
                    }
                }

                if entry.weight == 0 {
                    output.push(
                        &group_columns,
                        row,
                        old_output.expect("an inserted group has positive weight"),
                        -1,
                    )?;
                } else {
                    let new_output = call_output(&self.calls, &entry.calls, entry.weight)?;
                    match old_output {
                        None => {
                            output.push(&group_columns, row, new_output, 1)?;
                        }
                        Some(old_output) if old_output != new_output => {
                            output.push(&group_columns, row, old_output, -1)?;
                            output.push(&group_columns, row, new_output, 1)?;
                        }
                        Some(_) => {}
                    }
                }
                remove_group = entry.weight == 0;
            }

            if remove_group {
                let bucket = bucket.as_mut().expect("the current digest bucket exists");
                bucket.remove(&group);
                if bucket.is_empty() {
                    groups.remove(&digest)?;
                } else {
                    groups.put(&digest, bucket)?;
                }
            } else {
                groups.put(
                    &digest,
                    bucket.as_ref().expect("the current digest bucket exists"),
                )?;
            }
        }

        Ok(Action::Complete(output.finish(&self.output_schema)?))
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
    states: &[Vec<u8>],
    group_weight: u64,
) -> Result<Vec<ScalarValue>, AggregateError> {
    calls
        .iter()
        .zip(states)
        .map(|(call, state)| call.output(state, group_weight))
        .collect()
}

fn update_entry(
    entries: &mut OrderedMapAccess<'_, EntryKey, CollisionBucket>,
    layout: u32,
    group: u64,
    value: Vec<u8>,
    difference: i64,
) -> Result<Option<i64>, AggregateError> {
    let key = EntryKey::new(layout, group, row_digest(&value));
    let mut bucket = entries.get(&key)?;
    let presence = update_bucket(&mut bucket, value, difference).map_err(map_weight_error)?;
    if let Some(bucket) = bucket {
        entries.put(&key, &bucket)?;
    } else {
        entries.remove(&key)?;
    }
    Ok(presence)
}

fn map_weight_error(error: RowWeightError) -> AggregateError {
    match error {
        RowWeightError::Store(source) => AggregateError::Store(source),
        RowWeightError::Negative => AggregateError::NegativeWeight,
        RowWeightError::Overflow => AggregateError::ArithmeticOverflow,
    }
}

fn scan_layout(
    entries: &OrderedMapAccess<'_, EntryKey, CollisionBucket>,
    layout: &BoundLayout,
    group: u64,
    reduction: &dyn Indexed,
    state: &mut Vec<u8>,
) -> Result<(), AggregateError> {
    let range: RangeInclusive<EntryKey> =
        EntryKey::first(layout.id, group)..=EntryKey::last(layout.id, group);
    let limit = ScanLimit::new(1, usize::MAX)?;
    let mut resume = None;
    let mut scan = reduction.begin_scan();
    loop {
        let next = entries.scan(
            range.clone(),
            ScanDirection::Ascending,
            resume.as_ref(),
            limit,
            |entry| {
                let (_, bucket) = entry.decode_owned()?;
                for (encoded, weight) in bucket.rows() {
                    let values = scalars(&layout.fields, encoded)?;
                    reduction.push(&mut scan, &values, encoded, weight)?;
                }
                Ok::<(), AggregateError>(())
            },
        )?;
        let Some(next) = next else {
            break;
        };
        resume = Some(next);
    }
    reduction.finish_scan(state, scan);
    Ok(())
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
