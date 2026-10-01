use std::{num::NonZeroU64, ops::Bound, sync::Arc};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::Field;
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{OrderedMapAccess, ScanDirection, ScanLimit, StoreError, TransactionAccess};

use super::{
    AsOfDirection, AsOfJoinError, AsOfJoinLayout,
    index::{
        matchable_partition_prefix, order_prefix, parse_row_key, prefix_successor, push_component,
        row_key,
    },
    state::{AsOfCursor, Rows},
};
use crate::{
    expression::BoundExpression,
    operation::{
        BudgetExceeded, Cursor, OperationError, OperationInput, PagedOperation, Progress, Resume,
        Step, StepBudget,
        relation::{
            RowError, canonical_row_bounded, canonical_row_size_bounded,
            decode_canonical_row_bounded, encode_canonical_bounded, order_key,
        },
    },
};

pub(super) struct BoundScalar {
    pub(super) expression: BoundExpression,
    pub(super) field: Arc<Field>,
}
pub(super) struct BoundPair {
    pub(super) left: BoundScalar,
    pub(super) right: BoundScalar,
}
impl BoundPair {
    fn for_port(&self, port: usize) -> &BoundScalar {
        if port == 0 { &self.left } else { &self.right }
    }
}

/// Indexed SQL ASOF kernel; all progress belongs to the caller's Resume.
pub(crate) struct AsOfJoinOperation {
    pub(super) layout: AsOfJoinLayout,
    pub(super) left_rows: Rows,
    pub(super) right_rows: Rows,
}
struct PreparedRow {
    key: Vec<u8>,
    partition: Vec<u8>,
    order: Vec<u8>,
    matchable: bool,
    difference: i64,
}
struct Winner {
    key: Vec<u8>,
    row_start: usize,
}
impl Winner {
    fn row(&self) -> &[u8] {
        &self.key[self.row_start..]
    }
}
#[derive(Default)]
struct Candidates {
    winner: Option<Winner>,
    ambiguous: bool,
}
impl Candidates {
    fn checked(&self) -> Result<Option<&Winner>, AsOfJoinError> {
        if self.ambiguous {
            Err(AsOfJoinError::AmbiguousTie)
        } else {
            Ok(self.winner.as_ref())
        }
    }
}
struct Output {
    columns: Vec<Vec<ScalarValue>>,
    differences: Vec<i64>,
}
type Range = (Bound<Vec<u8>>, Bound<Vec<u8>>);

impl AsOfJoinOperation {
    fn validate_input(&self, input: OperationInput<'_>) -> Result<(), AsOfJoinError> {
        if input.port > 1 {
            return Err(AsOfJoinError::InvalidInputPort { port: input.port });
        }
        if input.change.schema() != self.layout.input_schemas[input.port] {
            return Err(AsOfJoinError::InputSchemaMismatch { port: input.port });
        }
        Ok(())
    }
    fn prepare(
        &self,
        input: OperationInput<'_>,
        budget: &mut StepBudget,
    ) -> Result<Vec<PreparedRow>, OperationError> {
        let records = input.change.records();
        let structural = input
            .change
            .num_rows()
            .saturating_mul(std::mem::size_of::<PreparedRow>());
        budget.charge(structural)?;
        let mut rows = (0..input.change.num_rows())
            .map(|index| PreparedRow {
                key: Vec::new(),
                partition: Vec::new(),
                order: vec![0],
                matchable: true,
                difference: input.change.diffs().value(index),
            })
            .collect::<Vec<_>>();
        for (index, pair) in self.layout.equalities.iter().enumerate() {
            let scalar = pair.for_port(input.port);
            let column = scalar.expression.evaluate(records).map_err(|source| {
                AsOfJoinError::Expression {
                    role: "equality",
                    index,
                    port: input.port,
                    source,
                }
            })?;
            budget.charge(crate::operation::logical_array_bytes(column.as_ref()))?;
            for (index, row) in rows.iter_mut().enumerate() {
                row.matchable &= !column.is_null(index);
                let before = row.partition.len();
                let encoded = encode_canonical_bounded(
                    &scalar.field,
                    column.as_ref(),
                    index,
                    "ASOF equality",
                    &mut row.partition,
                    before.saturating_add(budget.remaining_bytes()),
                );
                budget.charge(row.partition.len() - before)?;
                encoded.map_err(|error| {
                    if matches!(error, RowError::SizeLimit { .. }) {
                        Box::new(BudgetExceeded) as OperationError
                    } else {
                        Box::new(error)
                    }
                })?;
            }
        }
        let scalar = self.layout.order.for_port(input.port);
        let column =
            scalar
                .expression
                .evaluate(records)
                .map_err(|source| AsOfJoinError::Expression {
                    role: "order",
                    index: 0,
                    port: input.port,
                    source,
                })?;
        budget.charge(crate::operation::logical_array_bytes(column.as_ref()))?;
        for (index, row) in rows.iter_mut().enumerate() {
            // All ordered types are flat: admit the owned scalar, component
            // and worst-case escaped bytes before constructing any of them.
            budget.charge(
                crate::operation::logical_array_bytes(column.slice(index, 1).as_ref())
                    .saturating_mul(4)
                    .saturating_add(2),
            )?;
            let value = ScalarValue::try_from_array(column.as_ref(), index)?;
            if let Some(component) = order_key(&scalar.field, &value)? {
                row.order[0] = 1;
                push_component(&mut row.order, &component);
            } else {
                row.matchable = false;
            }
            let exact_size = canonical_row_size_bounded(records, index, budget.remaining_bytes())
                .map_err(|error| {
                if matches!(
                    error.downcast_ref::<RowError>(),
                    Some(RowError::SizeLimit { .. })
                ) {
                    Box::new(BudgetExceeded) as OperationError
                } else {
                    error
                }
            })?;
            let estimated = row
                .partition
                .len()
                .saturating_add(row.order.len())
                .saturating_add(exact_size)
                .saturating_mul(2)
                .saturating_add(8);
            budget.charge(exact_size.saturating_add(estimated))?;
            let exact = canonical_row_bounded(records, index, exact_size)?;
            row.key = row_key(&row.partition, &row.order, &exact);
        }
        Ok(rows)
    }
    fn cursor<'a>(
        &self,
        input: OperationInput<'_>,
        resume: &'a Resume,
    ) -> Result<&'a AsOfCursor, OperationError> {
        self.validate_input(input)?;
        let Cursor::AsOf(cursor) = &resume.cursor else {
            return Err(AsOfJoinError::InvalidResume("cursor belongs to another kernel").into());
        };
        if resume.ordinal >= u64::try_from(input.change.num_rows())?
            || (input.port == 0 && cursor.left_resume_after.is_some())
        {
            return Err(
                AsOfJoinError::InvalidResume("cursor is outside input or wrong port").into(),
            );
        }
        Ok(cursor)
    }
    fn validate_cursor_binding(
        &self,
        cursor: &AsOfCursor,
        driving: &PreparedRow,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        if let Some(key) = &cursor.left_resume_after {
            budget.charge(key.len().saturating_mul(2))?;
            let parsed = parse_row_key(key)
                .map_err(|_| AsOfJoinError::InvalidResume("left key framing is invalid"))?;
            let eligible = match self.layout.direction {
                AsOfDirection::Backward { allow_exact } => {
                    parsed.order > driving.order || (allow_exact && parsed.order == driving.order)
                }
                AsOfDirection::Forward { allow_exact } => {
                    parsed.order < driving.order || (allow_exact && parsed.order == driving.order)
                }
            };
            if !driving.matchable
                || parsed.partition != driving.partition
                || parsed.order.first() != Some(&1)
                || !eligible
            {
                return Err(AsOfJoinError::InvalidResume(
                    "left cursor is outside the driving influence interval",
                )
                .into());
            }
            decode_canonical_row_bounded(&self.layout.input_schemas[0], parsed.row, budget)?;
        }
        Ok(())
    }
    fn scan(
        rows: &OrderedMapAccess<'_, Vec<u8>, NonZeroU64>,
        range: Range,
        direction: ScanDirection,
        resume: Option<&Vec<u8>>,
        items: usize,
        budget: &mut StepBudget,
    ) -> Result<dogpaddle_store::OrderedMapPage<Vec<u8>, NonZeroU64>, OperationError> {
        let limit = ScanLimit::new(items, budget.remaining_bytes().max(1))?;
        let page = rows
            .scan(range, direction, resume, limit)
            .map_err(|error| match error {
                StoreError::ItemTooLarge { .. } => Box::new(BudgetExceeded) as OperationError,
                error => Box::new(error),
            })?;
        budget.charge(
            page.entries
                .iter()
                .map(|entry| entry.0.len().saturating_add(8))
                .sum(),
        )?;
        Ok(page)
    }
    fn range_for_partition(partition: &[u8]) -> Range {
        let prefix = matchable_partition_prefix(partition);
        (
            Bound::Included(prefix.clone()),
            prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded),
        )
    }
    fn bucket(
        &self,
        partition: &[u8],
        order: &[u8],
        overlay: Option<(&PreparedRow, bool)>,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Candidates, OperationError> {
        let prefix = order_prefix(partition, order);
        let range = (
            Bound::Included(prefix.clone()),
            prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded),
        );
        let rows = self.right_rows.access(access)?;
        let page = Self::scan(&rows, range, ScanDirection::Ascending, None, 3, budget)?;
        if page.continuation.is_some() && page.entries.len() < 3 {
            return Err(BudgetExceeded.into());
        }
        let mut keys = page
            .entries
            .into_iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>();
        if let Some((row, present)) = overlay {
            keys.retain(|key| key != &row.key);
            if present {
                keys.push(row.key.clone());
            }
        }
        let ambiguous = keys.len() > 1;
        let winner = keys
            .into_iter()
            .next()
            .map(|key| {
                let row_start = parse_row_key(&key).map(|parsed| key.len() - parsed.row.len());
                row_start.map(|row_start| Winner { key, row_start })
            })
            .transpose()?;
        Ok(Candidates { winner, ambiguous })
    }
    fn neighbor(
        &self,
        row: &PreparedRow,
        forward: bool,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Vec<u8>>, OperationError> {
        let prefix = order_prefix(&row.partition, &row.order);
        let mut range = Self::range_for_partition(&row.partition);
        if forward {
            range.0 = prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Included);
        } else {
            range.1 = Bound::Excluded(prefix);
        }
        let rows = self.right_rows.access(access)?;
        let page = Self::scan(
            &rows,
            range,
            if forward {
                ScanDirection::Ascending
            } else {
                ScanDirection::Descending
            },
            None,
            1,
            budget,
        )?;
        page.entries
            .into_iter()
            .next()
            .map(|entry| parse_row_key(&entry.0).map(|parsed| parsed.order))
            .transpose()
            .map_err(Into::into)
    }
    fn select(
        &self,
        left: &PreparedRow,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Candidates, OperationError> {
        if !left.matchable {
            return Ok(Candidates::default());
        }
        let mut range = Self::range_for_partition(&left.partition);
        let prefix = order_prefix(&left.partition, &left.order);
        let forward = matches!(self.layout.direction, AsOfDirection::Forward { .. });
        let inclusive = self.layout.direction.allow_exact();
        if forward {
            range.0 = if inclusive {
                Bound::Included(prefix)
            } else {
                prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Included)
            };
        } else {
            range.1 = if inclusive {
                prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded)
            } else {
                Bound::Excluded(prefix)
            };
        }
        let rows = self.right_rows.access(access)?;
        let page = Self::scan(
            &rows,
            range,
            if forward {
                ScanDirection::Ascending
            } else {
                ScanDirection::Descending
            },
            None,
            2,
            budget,
        )?;
        if page.continuation.is_some() && page.entries.len() < 2 {
            return Err(BudgetExceeded.into());
        }
        let mut entries = page.entries.into_iter();
        let Some(first) = entries.next() else {
            return Ok(Candidates::default());
        };
        let parsed = parse_row_key(&first.0)?;
        let ambiguous = entries
            .next()
            .map(|entry| parse_row_key(&entry.0).map(|second| second.order == parsed.order))
            .transpose()?
            .unwrap_or(false);
        Ok(Candidates {
            winner: Some(Winner {
                row_start: first.0.len() - parsed.row.len(),
                key: first.0,
            }),
            ambiguous,
        })
    }
    fn winner_values(
        &self,
        winner: Option<&Winner>,
        budget: &mut StepBudget,
    ) -> Result<Option<Vec<ScalarValue>>, OperationError> {
        winner
            .map(|winner| {
                decode_canonical_row_bounded(&self.layout.input_schemas[1], winner.row(), budget)
            })
            .transpose()
    }
    fn apply_weight(
        &self,
        port: usize,
        row: &PreparedRow,
        after: Option<NonZeroU64>,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        budget.charge(row.key.len().saturating_add(8))?;
        let mut rows = if port == 0 {
            self.left_rows.access(access)?
        } else {
            self.right_rows.access(access)?
        };
        if let Some(weight) = after {
            rows.put(&row.key, &weight)?;
        } else {
            rows.erase(&row.key)?;
        }
        Ok(())
    }
    fn affected_range(&self, right: &PreparedRow, outer_neighbor: Option<&[u8]>) -> Range {
        let prefix = order_prefix(&right.partition, &right.order);
        let inclusive = self.layout.direction.allow_exact();
        let mut range = Self::range_for_partition(&right.partition);
        if matches!(self.layout.direction, AsOfDirection::Forward { .. }) {
            range.1 = if inclusive {
                prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded)
            } else {
                Bound::Excluded(prefix)
            };
            if let Some(order) = outer_neighbor {
                let prefix = order_prefix(&right.partition, order);
                range.0 = if inclusive {
                    prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Included)
                } else {
                    Bound::Included(prefix)
                };
            }
        } else {
            range.0 = if inclusive {
                Bound::Included(prefix)
            } else {
                prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Included)
            };
            if let Some(order) = outer_neighbor {
                let prefix = order_prefix(&right.partition, order);
                range.1 = if inclusive {
                    Bound::Excluded(prefix)
                } else {
                    prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded)
                };
            }
        }
        range
    }
    fn right_page(
        &self,
        right: &PreparedRow,
        weights: (Option<NonZeroU64>, Option<NonZeroU64>),
        cursor: &mut AsOfCursor,
        output: &mut Output,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<bool, OperationError> {
        let (before, after) = weights;
        if !right.matchable || before.is_some() == after.is_some() {
            if cursor.left_resume_after.is_some() {
                return Err(
                    AsOfJoinError::InvalidResume("non-presence event has a left cursor").into(),
                );
            }
            budget.consume_head(1)?;
            self.apply_weight(1, right, after, budget, access)?;
            return Ok(true);
        }
        let forward = matches!(self.layout.direction, AsOfDirection::Forward { .. });
        let outer_neighbor = self.neighbor(right, !forward, budget, access)?;
        let replacement_order = self.neighbor(right, forward, budget, access)?;
        let range = self.affected_range(right, outer_neighbor.as_deref());
        let rows = self.left_rows.access(access)?;
        if let Some(key) = &cursor.left_resume_after {
            if !std::ops::RangeBounds::contains(&range, key) {
                return Err(AsOfJoinError::InvalidResume(
                    "left cursor exceeds the adjacent influence interval",
                )
                .into());
            }
            budget.charge(key.len().saturating_add(8))?;
            if rows.get_bounded(key, 8)?.is_none() {
                return Err(AsOfJoinError::InvalidResume("left cursor row is absent").into());
            }
        }
        let page = Self::scan(
            &rows,
            range,
            ScanDirection::Ascending,
            cursor.left_resume_after.as_ref(),
            budget.head_remaining().max(1),
            budget,
        )?;
        budget.consume_head(page.entries.len().max(1))?;
        if !page.entries.is_empty() {
            let old_bucket = self.bucket(&right.partition, &right.order, None, budget, access)?;
            let new_bucket = self.bucket(
                &right.partition,
                &right.order,
                Some((right, after.is_some())),
                budget,
                access,
            )?;
            let replacement = if (old_bucket.winner.is_none() || new_bucket.winner.is_none())
                && let Some(order) = replacement_order
            {
                self.bucket(&right.partition, &order, None, budget, access)?
            } else {
                Candidates::default()
            };
            let replacement = replacement.checked()?;
            let old = old_bucket.checked()?.or(replacement);
            let new = new_bucket.checked()?.or(replacement);
            if old.map(|winner| &winner.key) != new.map(|winner| &winner.key) {
                let before_decode = budget.remaining_bytes();
                let old_values = self.winner_values(old, budget)?;
                let new_values = self.winner_values(new, budget)?;
                // Every output still reconstructs Arrow; only decoding is shared.
                let winner_work = before_decode - budget.remaining_bytes();
                budget.charge(winner_work.saturating_mul(page.entries.len() - 1))?;
                for entry in &page.entries {
                    let parsed = parse_row_key(&entry.0)?;
                    let before_decode = budget.remaining_bytes();
                    let left = decode_canonical_row_bounded(
                        &self.layout.input_schemas[0],
                        parsed.row,
                        budget,
                    )?;
                    budget.charge(before_decode - budget.remaining_bytes())?;
                    let positive = i64::try_from(entry.1.get())
                        .map_err(|_| AsOfJoinError::OutputDifferenceOverflow)?;
                    for (winner, values, difference) in [
                        (old, old_values.as_deref(), -positive),
                        (new, new_values.as_deref(), positive),
                    ] {
                        output.push(
                            &left,
                            values.unwrap_or(&self.layout.right_nulls),
                            difference,
                            parsed
                                .row
                                .len()
                                .saturating_add(winner.map_or(0, |winner| winner.row().len())),
                            budget,
                        )?;
                    }
                }
            }
        }
        if let Some(key) = page.continuation {
            cursor.left_resume_after = Some(key);
            Ok(false)
        } else {
            self.apply_weight(1, right, after, budget, access)?;
            cursor.left_resume_after = None;
            Ok(true)
        }
    }
}
impl PagedOperation for AsOfJoinOperation {
    fn initial_resume(&self) -> Resume {
        Resume {
            ordinal: 0,
            cursor: Cursor::AsOf(AsOfCursor {
                left_resume_after: None,
            }),
        }
    }
    fn validate_resume(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
    ) -> Result<(), OperationError> {
        let cursor = self.cursor(input, resume)?;
        if cursor.left_resume_after.is_some() {
            let slice = input
                .change
                .try_slice(usize::try_from(resume.ordinal)?, 1)?;
            let mut budget = StepBudget::new(1, 4 * 1024 * 1024);
            let driving = self.prepare(
                OperationInput {
                    port: input.port,
                    change: &slice,
                },
                &mut budget,
            )?;
            self.validate_cursor_binding(cursor, &driving[0], &mut budget)?;
        }
        Ok(())
    }
    fn step(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
        access: TransactionAccess<'_>,
        budget: &mut StepBudget,
    ) -> Result<Step, OperationError> {
        let cursor = self.cursor(input, resume)?;
        let start = usize::try_from(resume.ordinal)?;
        let length = budget.head_remaining().min(input.change.num_rows() - start);
        if length == 0 {
            return Err(BudgetExceeded.into());
        }
        let slice = input.change.try_slice(start, length)?;
        let rows = self.prepare(
            OperationInput {
                port: input.port,
                change: &slice,
            },
            budget,
        )?;
        self.validate_cursor_binding(cursor, &rows[0], budget)?;
        let mut cursor = cursor.clone();
        let mut next = start;
        let mut output = Output::new(self.layout.output_schema.fields().len());
        for row in &rows {
            if budget.head_remaining() == 0 {
                break;
            }
            budget.charge(row.key.len().saturating_add(8))?;
            let before = if input.port == 0 {
                self.left_rows.access(access)?.get_bounded(&row.key, 8)?
            } else {
                self.right_rows.access(access)?.get_bounded(&row.key, 8)?
            };
            let after =
                dogpaddle_store::checked_weight(before.map_or(0, NonZeroU64::get), row.difference)
                    .map(NonZeroU64::new)
                    .map_err(|error| match error {
                        StoreError::MultiplicityUnderflow => AsOfJoinError::NegativeWeight,
                        StoreError::MultiplicityOverflow => AsOfJoinError::WeightOverflow,
                        error => AsOfJoinError::Store(error),
                    })?;
            if input.port == 0 {
                let winner = self.select(row, budget, access)?;
                let winner = winner.checked()?;
                let values = self.winner_values(winner, budget)?;
                let parsed = parse_row_key(&row.key)?;
                let left = decode_canonical_row_bounded(
                    &self.layout.input_schemas[0],
                    parsed.row,
                    budget,
                )?;
                output.push(
                    &left,
                    values.as_deref().unwrap_or(&self.layout.right_nulls),
                    row.difference,
                    parsed
                        .row
                        .len()
                        .saturating_add(winner.map_or(0, |winner| winner.row().len())),
                    budget,
                )?;
                self.apply_weight(0, row, after, budget, access)?;
                budget.consume_head(1)?;
            } else if !self.right_page(
                row,
                (before, after),
                &mut cursor,
                &mut output,
                budget,
                access,
            )? {
                break;
            }
            next += 1;
        }
        let progress = if next == input.change.num_rows() {
            Progress::Done
        } else {
            Progress::More(Resume {
                ordinal: u64::try_from(next)?,
                cursor: Cursor::AsOf(cursor),
            })
        };
        if matches!(&progress,Progress::More(next)if next==resume) {
            return Err(BudgetExceeded.into());
        }
        Ok(Step {
            output: output.finish(&self.layout.output_schema)?,
            progress,
        })
    }
}
impl Output {
    fn new(fields: usize) -> Self {
        Self {
            columns: (0..fields).map(|_| Vec::new()).collect(),
            differences: Vec::new(),
        }
    }
    fn push(
        &mut self,
        left: &[ScalarValue],
        right: &[ScalarValue],
        diff: i64,
        encoded_bytes: usize,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        let row_bytes = self
            .columns
            .len()
            .saturating_mul(std::mem::size_of::<ScalarValue>())
            .saturating_add(8);
        budget.charge(encoded_bytes.saturating_add(row_bytes))?;
        for (column, value) in self.columns.iter_mut().zip(left.iter().chain(right)) {
            column.push(value.clone());
        }
        self.differences.push(diff);
        Ok(())
    }
    fn finish(self, schema: &arrow_schema::SchemaRef) -> Result<Option<Change>, OperationError> {
        if self.differences.is_empty() {
            return Ok(None);
        }
        let count = self.differences.len();
        let arrays = self
            .columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let records = RecordBatch::try_new_with_options(
            Arc::clone(schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?;
        Ok(Some(Change::try_new(
            records,
            Int64Array::from(self.differences),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, expression::StoredExpression};
    use arrow_schema::{DataType, Schema};
    use dogpaddle_store::StoreSetup;

    #[test]
    fn rejected_key_encoding_accounts_for_copied_bytes_and_admits_order_payload_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("at", DataType::Int64, false),
        ]));
        let scalar = |name: &str, data_type| BoundScalar {
            expression: StoredExpression::try_new(col(name))
                .unwrap()
                .bind(Arc::clone(&schema))
                .unwrap(),
            field: Arc::new(Field::new(name, data_type, false)),
        };
        let mut setup = StoreSetup::new();
        let mut operation = AsOfJoinOperation {
            layout: AsOfJoinLayout {
                direction: AsOfDirection::Backward { allow_exact: true },
                input_schemas: [Arc::clone(&schema), Arc::clone(&schema)],
                output_schema: Arc::clone(&schema),
                equalities: Box::new([BoundPair {
                    left: scalar("key", DataType::Binary),
                    right: scalar("key", DataType::Binary),
                }]),
                order: BoundPair {
                    left: scalar("at", DataType::Int64),
                    right: scalar("at", DataType::Int64),
                },
                right_nulls: vec![],
            },
            left_rows: setup.data_scope().data("left").unwrap(),
            right_rows: setup.data_scope().data("right").unwrap(),
        };
        let change = Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(arrow_array::BinaryArray::from(vec![
                        vec![1; 1024].as_slice(),
                    ])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap(),
            Int64Array::from(vec![1]),
        )
        .unwrap();
        let mut budget = StepBudget::new(1, 1800);
        let error = operation
            .prepare(
                OperationInput {
                    port: 0,
                    change: &change,
                },
                &mut budget,
            )
            .err()
            .unwrap();
        assert!(error.is::<BudgetExceeded>());
        assert_eq!(
            budget.remaining_bytes(),
            1800 - size_of::<PreparedRow>() - (1024 + 8) - (1 + 8)
        );
        operation.layout.equalities = Box::new([]);
        operation.layout.order = BoundPair {
            left: scalar("key", DataType::Binary),
            right: scalar("key", DataType::Binary),
        };
        let mut budget = StepBudget::new(1, 4200);
        let error = operation
            .prepare(
                OperationInput {
                    port: 0,
                    change: &change,
                },
                &mut budget,
            )
            .err()
            .unwrap();
        assert!(error.is::<BudgetExceeded>());
        // The evaluated array is charged, but the next owned copies are
        // rejected together before any partial order component is built.
        assert_eq!(
            budget.remaining_bytes(),
            4200 - size_of::<PreparedRow>() - (1024 + 8)
        );
    }

    #[test]
    fn byte_truncated_bucket_cannot_infer_uniqueness_or_overlay_absence() {
        let root = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("at", DataType::Int64, false)]));
        let scalar = || BoundScalar {
            expression: StoredExpression::try_new(col("at"))
                .unwrap()
                .bind(Arc::clone(&schema))
                .unwrap(),
            field: Arc::new(Field::new("at", DataType::Int64, false)),
        };
        let mut setup = StoreSetup::new();
        let operation = AsOfJoinOperation {
            layout: AsOfJoinLayout {
                direction: AsOfDirection::Forward { allow_exact: true },
                input_schemas: [Arc::clone(&schema), Arc::clone(&schema)],
                output_schema: Arc::clone(&schema),
                equalities: Box::new([]),
                order: BoundPair {
                    left: scalar(),
                    right: scalar(),
                },
                right_nulls: vec![],
            },
            left_rows: setup.data_scope().data("left").unwrap(),
            right_rows: setup.data_scope().data("right").unwrap(),
        };
        let mut transactions = setup.commit(root.path().join("store"), |_| Ok(())).unwrap();
        let partition = vec![1];
        let order = vec![1];
        let small = row_key(&partition, &order, &[1]);
        let large = row_key(&partition, &order, &vec![2; 64 * 1024]);
        {
            let transaction = transactions.begin();
            let mut rows = operation.right_rows.access(transaction.access()).unwrap();
            for key in [&small, &large] {
                rows.put(key, &NonZeroU64::new(1).unwrap()).unwrap();
            }
            transaction.commit().unwrap();
        }
        let removed = PreparedRow {
            key: small,
            partition: partition.clone(),
            order: order.clone(),
            matchable: true,
            difference: -1,
        };
        let transaction = transactions.begin();
        for overlay in [None, Some((&removed, false))] {
            let error = operation
                .bucket(
                    &partition,
                    &order,
                    overlay,
                    &mut StepBudget::new(1, 16 * 1024),
                    transaction.access(),
                )
                .err()
                .unwrap();
            assert!(error.is::<BudgetExceeded>());
        }
        let before = operation
            .bucket(
                &partition,
                &order,
                None,
                &mut StepBudget::new(1, 4 * 1024 * 1024),
                transaction.access(),
            )
            .unwrap();
        assert!(before.ambiguous);
        let after = operation
            .bucket(
                &partition,
                &order,
                Some((&removed, false)),
                &mut StepBudget::new(1, 4 * 1024 * 1024),
                transaction.access(),
            )
            .unwrap();
        assert!(!after.ambiguous);
        assert_eq!(after.winner.unwrap().key, large);
    }
}
