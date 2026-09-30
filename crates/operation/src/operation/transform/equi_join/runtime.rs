// Input progress and durable writes stay here. Candidate evaluation and
// output construction are private children and share this runtime's state.
mod matches;
mod output;

use std::{cell::OnceCell, mem::size_of, num::NonZeroU64, ops::Deref, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_store::{OrderedMapAccess, StoreError, TransactionAccess};

use crate::{
    expression::BoundExpression,
    operation::{
        Cursor, OperationError, OperationInput, PagedOperation, Progress, Resume, Step, StepBudget,
        relation::{
            RowError, canonical_row_bounded, canonical_row_size_bounded,
            decode_canonical_row_bounded, encode_canonical_bounded,
        },
    },
};

use super::{
    EquiJoinError, EquiJoinKind,
    state::{Counts, JoinCursor, MatchCounts, Rows, actual_match_key},
};

use output::OutputRows;

fn canonical_error(source: OperationError) -> EquiJoinError {
    if source.is::<crate::operation::BudgetExceeded>() {
        EquiJoinError::Budget(crate::operation::BudgetExceeded)
    } else {
        EquiJoinError::CanonicalRow { source }
    }
}

fn partition_bytes(key: &[u8]) -> usize {
    key.len()
        .saturating_add(key.iter().fold(0_usize, |count, byte| {
            count.saturating_add(usize::from(*byte == 0))
        }))
        .saturating_add(2)
}

pub(super) struct BoundKey {
    expression: BoundExpression,
    field: Arc<Field>,
}

pub(super) struct BoundKeyPair {
    pub(super) left: BoundKey,
    pub(super) right: BoundKey,
}

/// Materialized, exact-Schema equality join.
///
/// The runtime keeps both input relations in private ordered weight maps. The caller-owned resume advances through bounded output pages after exact-row
/// admission. Each page commits atomically; a later page failure leaves earlier
/// committed pages and their progress intact.
pub(crate) struct EquiJoinOperation {
    pub(super) kind: EquiJoinKind,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) candidate_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) keys: Box<[BoundKeyPair]>,
    pub(super) residual: Option<BoundExpression>,
    pub(super) nulls: [Vec<ScalarValue>; 2],
    pub(super) left_rows: Rows,
    pub(super) right_rows: Rows,
    pub(super) key_counts: Option<Counts>,
    pub(super) match_counts: Option<MatchCounts>,
}

#[derive(Clone, Copy, Default)]
enum KeyTransition {
    #[default]
    None,
    First,
    Last,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum MatchTransition {
    #[default]
    None,
    BecameMatched,
    BecameUnmatched,
}

#[derive(Clone, Copy, Default)]
struct RowEffect {
    matched: bool,
    transition: KeyTransition,
}

struct PreparedRow {
    row: Vec<u8>,
    key: Vec<u8>,
    matchable: bool,
    difference: i64,
}

struct ActiveRow<'row> {
    prepared: &'row PreparedRow,
    records: &'row RecordBatch,
    index: usize,
    values: OnceCell<Vec<ScalarValue>>,
}

struct PreparedMatch {
    row: Vec<u8>,
    values: Vec<ScalarValue>,
    multiplicity: u64,
}

struct ResidualPage {
    matches: Vec<PreparedMatch>,
    qualifying: usize,
    continuation: Option<Vec<u8>>,
    items: usize,
}

impl BoundKey {
    pub(super) fn new(expression: BoundExpression) -> Self {
        let field = Arc::new(Field::new(
            "key",
            expression.output_type().clone(),
            expression.output_nullable(),
        ));
        Self { expression, field }
    }
}

impl BoundKeyPair {
    fn for_port(&self, port: usize) -> &BoundKey {
        match port {
            0 => &self.left,
            1 => &self.right,
            _ => unreachable!("a prepared input has a validated port"),
        }
    }
}

impl<'row> ActiveRow<'row> {
    fn new(prepared: &'row PreparedRow, records: &'row RecordBatch, index: usize) -> Self {
        Self {
            prepared,
            records,
            index,
            values: OnceCell::new(),
        }
    }

    fn values(&self) -> Result<&[ScalarValue], EquiJoinError> {
        if self.values.get().is_none() {
            let values = self
                .records
                .columns()
                .iter()
                .map(|column| ScalarValue::try_from_array(column.as_ref(), self.index))
                .collect::<Result<Vec<_>, _>>()?;
            self.values
                .set(values)
                .expect("a step-local row is initialized by only one thread");
        }
        Ok(self
            .values
            .get()
            .expect("the row values were initialized above"))
    }
}

impl Deref for ActiveRow<'_> {
    type Target = PreparedRow;

    fn deref(&self) -> &Self::Target {
        self.prepared
    }
}

impl EquiJoinOperation {
    fn validate_input(&self, input: OperationInput<'_>) -> Result<(), EquiJoinError> {
        if input.port >= self.input_schemas.len() {
            return Err(EquiJoinError::InvalidInputPort { port: input.port });
        }
        if input.change.schema().as_ref() != self.input_schemas[input.port].as_ref() {
            return Err(EquiJoinError::InputSchemaMismatch { port: input.port });
        }
        Ok(())
    }

    fn cursor<'a>(
        &self,
        input: OperationInput<'_>,
        resume: &'a Resume,
    ) -> Result<&'a JoinCursor, OperationError> {
        self.validate_input(input)?;
        let Cursor::EquiJoin(cursor) = &resume.cursor else {
            return Err(EquiJoinError::InvalidResume("cursor belongs to another kernel").into());
        };
        if resume.ordinal >= u64::try_from(input.change.num_rows())?
            || (cursor.found_match && cursor.resume_after.is_none())
            || (self.residual.is_none() && cursor.found_match)
        {
            return Err(EquiJoinError::InvalidResume(
                "cursor is outside input or invalid for kernel",
            )
            .into());
        }
        Ok(cursor)
    }
    fn validate_cursor_binding(
        &self,
        input: OperationInput<'_>,
        cursor: &JoinCursor,
        driving: &PreparedRow,
        budget: &mut StepBudget,
    ) -> Result<(), OperationError> {
        if let Some(key) = &cursor.resume_after {
            if !driving.matchable
                || (self.residual.is_none() && self.kind.left_only() && input.port == 0)
            {
                return Err(
                    EquiJoinError::InvalidResume("row cannot have an opposite cursor").into(),
                );
            }
            budget.charge(key.len().saturating_mul(2))?;
            let values =
                decode_canonical_row_bounded(&self.input_schemas[1 - input.port], key, budget)?;
            let arrays = values
                .into_iter()
                .map(|value| value.to_array_of_size(1))
                .collect::<Result<Vec<_>, _>>()?;
            let records = RecordBatch::try_new_with_options(
                Arc::clone(&self.input_schemas[1 - input.port]),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?;
            let candidate = dogpaddle_change::Change::try_new(records, Int64Array::from(vec![1]))?;
            let candidate = self.prepare_window(
                OperationInput {
                    port: 1 - input.port,
                    change: &candidate,
                },
                budget,
            )?;
            if !candidate[0].matchable || candidate[0].key != driving.key {
                return Err(EquiJoinError::InvalidResume(
                    "candidate cursor is outside the driving partition",
                )
                .into());
            }
        }
        Ok(())
    }
    fn prepare_window(
        &self,
        input: OperationInput<'_>,
        budget: &mut StepBudget,
    ) -> Result<Vec<PreparedRow>, OperationError> {
        budget.charge(
            input
                .change
                .num_rows()
                .saturating_mul(size_of::<PreparedRow>()),
        )?;
        let records = input.change.records();
        let mut rows = (0..input.change.num_rows())
            .map(|index| PreparedRow {
                row: Vec::new(),
                key: Vec::new(),
                matchable: true,
                difference: input.change.diffs().value(index),
            })
            .collect::<Vec<_>>();
        // Retain one evaluated key array at a time, then encode exact rows.
        // Relation admission still occurs separately for each driving event.
        for (key, pair) in self.keys.iter().enumerate() {
            let bound = pair.for_port(input.port);
            let column = bound.expression.evaluate(records).map_err(|source| {
                EquiJoinError::KeyExpression {
                    port: input.port,
                    key,
                    source,
                }
            })?;
            budget.charge(crate::operation::logical_array_bytes(column.as_ref()))?;
            for (index, row) in rows.iter_mut().enumerate() {
                row.matchable &= !column.is_null(index);
                let before = row.key.len();
                let result = encode_canonical_bounded(
                    &bound.field,
                    column.as_ref(),
                    index,
                    "key",
                    &mut row.key,
                    before.saturating_add(budget.remaining_bytes()),
                );
                budget.charge(row.key.len() - before)?;
                result.map_err(|source| match source {
                    RowError::SizeLimit { .. } => {
                        Box::new(crate::operation::BudgetExceeded) as OperationError
                    }
                    source => Box::new(EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }),
                })?;
            }
        }
        for (index, row) in rows.iter_mut().enumerate() {
            let size = canonical_row_size_bounded(records, index, budget.remaining_bytes())
                .map_err(|source| {
                    if matches!(
                        source.downcast_ref::<RowError>(),
                        Some(RowError::SizeLimit { .. })
                    ) {
                        Box::new(crate::operation::BudgetExceeded) as OperationError
                    } else {
                        Box::new(EquiJoinError::CanonicalRow { source })
                    }
                })?;
            budget.charge(size)?;
            row.row = canonical_row_bounded(records, index, size).map_err(canonical_error)?;
        }
        Ok(rows)
    }

    fn event_effect(
        &self,
        port: usize,
        row: &PreparedRow,
        access: TransactionAccess<'_>,
    ) -> Result<RowEffect, EquiJoinError> {
        let mut own = self.rows(port).access(access)?;
        let before = own.partition(&row.key)?.multiplicity(&row.row)?;
        let after = adjusted_weight(before, row.difference)?;
        let mut effect = RowEffect {
            matched: row.matchable,
            transition: KeyTransition::None,
        };
        if self.residual.is_some() {
            effect.transition = match (before == 0, after == 0) {
                (true, false) => KeyTransition::First,
                (false, true) => KeyTransition::Last,
                _ => KeyTransition::None,
            };
        } else if row.matchable
            && let Some(counts) = &self.key_counts
        {
            let mut counts = counts
                .access(access)?
                .get_bounded(&row.key, 16)?
                .unwrap_or_default();
            let key_before = counts.0[port];
            effect.matched = counts.0[1 - port] > 0;
            counts.adjust(port, before, after)?;
            effect.transition = match (key_before == 0, counts.0[port] == 0) {
                (true, false) => KeyTransition::First,
                (false, true) => KeyTransition::Last,
                _ => KeyTransition::None,
            };
        }
        Ok(effect)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "One bounded driver keeps pure and qualified join progress in the same transaction."
    )]
    fn compute_window(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
        access: TransactionAccess<'_>,
        shared: &mut StepBudget,
    ) -> Result<Step, OperationError> {
        let cursor = self.cursor(input, resume)?;
        let start = usize::try_from(resume.ordinal)?;
        let length = shared.head_remaining().min(input.change.num_rows() - start);
        if length == 0 {
            return Err(crate::operation::BudgetExceeded.into());
        }
        let slice = input.change.try_slice(start, length)?;
        let window = self.prepare_window(
            OperationInput {
                port: input.port,
                change: &slice,
            },
            shared,
        )?;
        self.validate_cursor_binding(input, cursor, &window[0], shared)?;
        let mut state = JoinCursor {
            found_match: cursor.found_match,
            resume_after: cursor.resume_after.clone(),
        };
        let budget = shared;
        let mut output = OutputRows::new(self.output_schema.fields().len());
        let mut next = start;
        for (index, prepared) in window.iter().enumerate() {
            if !budget.can_start(prepared) {
                break;
            }
            // Admission reads, final adjustment read/write, and optional
            // support-count reads/writes all share the page allowance.
            budget.charge(StepBudget::stored_row_bytes(prepared).saturating_mul(3))?;
            if self.key_counts.is_some() {
                budget.charge(prepared.key.len().saturating_add(16).saturating_mul(3))?;
            }
            if self.match_counts.is_some() {
                budget.charge(prepared.row.len().saturating_add(9).saturating_mul(3))?;
            }
            let effect = self.event_effect(input.port, prepared, access)?;
            let row = ActiveRow::new(prepared, slice.records(), index);
            if self.residual.is_some() {
                let found_match =
                    if self.kind.left_only() && matches!(effect.transition, KeyTransition::None) {
                        if state.resume_after.is_some() {
                            return Err(EquiJoinError::InvalidResume(
                                "stable row has candidate cursor",
                            )
                            .into());
                        }
                        self.stable_left_only_found_match(input.port, &row, access)?
                    } else {
                        state.found_match
                    };
                let Some(page) = self.scan_residual_matches(
                    input.port,
                    &row,
                    effect,
                    state.resume_after.as_ref(),
                    budget,
                    access,
                )?
                else {
                    break;
                };
                budget.consume_head(page.items)?;
                state.found_match = found_match;
                self.emit_residual_page(
                    input.port,
                    &row,
                    effect,
                    &page.matches,
                    page.qualifying,
                    &mut state,
                    &mut output,
                    access,
                )?;
                if let Some(key) = page.continuation {
                    state.resume_after = Some(key);
                    break;
                }
                self.append_residual_current(input.port, &row, state.found_match, &mut output)?;
                self.validate_current_match_count(
                    input.port,
                    &row,
                    effect,
                    state.found_match,
                    access,
                )?;
            } else {
                let Some(page) = self.scan_matches(
                    input.port,
                    &row,
                    effect,
                    state.resume_after.as_ref(),
                    budget,
                    access,
                )?
                else {
                    break;
                };
                let work = self.output_work(input.port, &row, effect, &page.entries);
                if !budget.can_accept(work) {
                    break;
                }
                budget.consume_head(work.0)?;
                budget.charge(work.1)?;
                self.append_output_page(
                    input.port,
                    &row,
                    effect,
                    &page.entries,
                    &mut output,
                    budget,
                )?;
                if let Some(key) = page.continuation {
                    state.resume_after = Some(key);
                    break;
                }
            }
            self.adjust_own_row(input.port, &row, access)?;
            next += 1;
            state.found_match = false;
            state.resume_after = None;
            if budget.exhausted() {
                break;
            }
        }
        let progress = if next == input.change.num_rows() {
            Progress::Done
        } else {
            Progress::More(Resume {
                ordinal: u64::try_from(next)?,
                cursor: Cursor::EquiJoin(JoinCursor {
                    found_match: state.found_match,
                    resume_after: state.resume_after,
                }),
            })
        };
        if matches!(&progress, Progress::More(next) if next == resume) {
            return Err(crate::operation::BudgetExceeded.into());
        }
        Ok(Step {
            output: output.finish(&self.output_schema)?,
            progress,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the page transition explicitly receives its pinned row, effect, continuation, output, and transaction"
    )]
    fn emit_residual_page(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        effect: RowEffect,
        matches: &[PreparedMatch],
        qualifying: usize,
        state: &mut JoinCursor,
        output: &mut OutputRows,
        access: TransactionAccess<'_>,
    ) -> Result<(), EquiJoinError> {
        let mut counts = self
            .match_counts
            .as_ref()
            .map(|counts| counts.access(access))
            .transpose()?;
        state.found_match |= qualifying != 0;
        if self.tracks_match_count(port)
            && !matches!(effect.transition, KeyTransition::None)
            && qualifying != 0
        {
            Self::adjust_actual_match_count(
                counts.as_mut().ok_or(EquiJoinError::InvalidMatchCount(
                    "tracked residual join has no match-count state",
                ))?,
                port,
                &input.row,
                effect.transition,
                u64::try_from(qualifying).map_err(|_| EquiJoinError::MatchCountOverflow)?,
            )?;
        }
        for matched in matches {
            let transition = if self.tracks_match_count(1 - port)
                && !matches!(effect.transition, KeyTransition::None)
            {
                Self::adjust_actual_match_count(
                    counts.as_mut().ok_or(EquiJoinError::InvalidMatchCount(
                        "tracked residual join has no match-count state",
                    ))?,
                    1 - port,
                    &matched.row,
                    effect.transition,
                    1,
                )?
            } else {
                MatchTransition::None
            };
            self.append_residual_match_output(port, input, effect, matched, transition, output)?;
        }
        Ok(())
    }

    fn validate_current_match_count(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        found_match: bool,
        access: TransactionAccess<'_>,
    ) -> Result<(), EquiJoinError> {
        if !self.tracks_match_count(port) {
            return Ok(());
        }
        let counts = self
            .match_counts
            .as_ref()
            .ok_or(EquiJoinError::InvalidMatchCount(
                "tracked residual join has no match-count state",
            ))?
            .access(access)?;
        let count = Self::actual_match_count(&counts, port, &input.row)?;
        let valid = match effect.transition {
            KeyTransition::Last => count == 0,
            KeyTransition::First | KeyTransition::None => (count > 0) == found_match,
        };
        if !valid {
            return Err(EquiJoinError::InvalidMatchCount(
                "current row support disagrees with its qualifying candidates",
            ));
        }
        Ok(())
    }

    fn adjust_actual_match_count(
        counts: &mut OrderedMapAccess<'_, Vec<u8>, u64>,
        port: usize,
        row: &[u8],
        transition: KeyTransition,
        amount: u64,
    ) -> Result<MatchTransition, EquiJoinError> {
        let key = actual_match_key(port, row);
        let before = match counts.get_bounded(&key, 8)? {
            Some(0) => {
                return Err(EquiJoinError::InvalidMatchCount(
                    "committed match count is zero instead of absent",
                ));
            }
            Some(value) => value,
            None => 0,
        };
        let after = adjust_match_count(before, transition, amount)?;
        if after == 0 {
            counts.erase(&key)?;
        } else {
            counts.put(&key, &after)?;
        }
        Ok(match_transition(before, after))
    }

    fn actual_match_count(
        counts: &OrderedMapAccess<'_, Vec<u8>, u64>,
        port: usize,
        row: &[u8],
    ) -> Result<u64, EquiJoinError> {
        match counts.get_bounded(&actual_match_key(port, row), 8)? {
            Some(0) => Err(EquiJoinError::InvalidMatchCount(
                "committed match count is zero instead of absent",
            )),
            Some(value) => Ok(value),
            None => Ok(0),
        }
    }

    fn tracks_match_count(&self, port: usize) -> bool {
        match self.kind {
            EquiJoinKind::Inner => false,
            EquiJoinKind::FullOuter => true,
            EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti | EquiJoinKind::LeftOuter => port == 0,
        }
    }

    fn stable_left_only_found_match(
        &self,
        port: usize,
        input: &PreparedRow,
        access: TransactionAccess<'_>,
    ) -> Result<bool, EquiJoinError> {
        if port == 1 || !input.matchable {
            return Ok(false);
        }
        let counts = self
            .match_counts
            .as_ref()
            .ok_or(EquiJoinError::InvalidMatchCount(
                "residual left-only join has no match-count state",
            ))?
            .access(access)?;
        let actual = Self::actual_match_count(&counts, port, &input.row)?;
        Ok(actual > 0)
    }

    fn adjust_own_row(
        &self,
        port: usize,
        row: &PreparedRow,
        access: TransactionAccess<'_>,
    ) -> Result<(), EquiJoinError> {
        let change = self
            .rows(port)
            .access(access)?
            .partition(&row.key)?
            .adjust(&row.row, row.difference)
            .map_err(map_weight_error)?;
        if row.matchable
            && let Some(counts) = &self.key_counts
            && (change.before() == 0) != (change.after() == 0)
        {
            let mut counts = counts.access(access)?;
            let mut value = counts.get_bounded(&row.key, 16)?.unwrap_or_default();
            value.adjust(port, change.before(), change.after())?;
            if value.is_empty() {
                counts.erase(&row.key)?;
            } else {
                counts.put(&row.key, &value)?;
            }
        }
        Ok(())
    }

    fn output_work(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        matches: &[(Vec<u8>, NonZeroU64)],
    ) -> (usize, usize) {
        let repeats_input = !(self.kind.left_only() && port == 1);
        let (items, mut bytes) = StepBudget::work(row, matches, repeats_input);
        if matches.is_empty() && self.kind.preserves(port) && !effect.matched {
            bytes = bytes.saturating_add(
                self.nulls[1 - port]
                    .len()
                    .saturating_mul(size_of::<ScalarValue>()),
            );
        } else if self.kind.preserves(1 - port) && !matches!(effect.transition, KeyTransition::None)
        {
            for matched in matches {
                bytes = bytes.saturating_add(matched.0.len()).saturating_add(
                    self.nulls[port]
                        .len()
                        .saturating_mul(size_of::<ScalarValue>()),
                );
            }
        }
        let scalar_items = if self.kind.left_only() {
            1
        } else {
            matches.len().max(1).saturating_mul(2)
        };
        bytes = bytes.saturating_add(
            scalar_items
                .saturating_mul(self.output_schema.fields().len())
                .saturating_mul(size_of::<ScalarValue>()),
        );
        (items, bytes)
    }

    fn rows(&self, port: usize) -> &Rows {
        match port {
            0 => &self.left_rows,
            1 => &self.right_rows,
            _ => unreachable!("a prepared input has a validated port"),
        }
    }
}

impl StepBudget {
    fn can_start(&self, row: &PreparedRow) -> bool {
        self.head_remaining() > 0 && Self::stored_row_bytes(row) <= self.remaining_bytes()
    }
    fn max_scan_items(&self, row: &PreparedRow, _expanded: bool, repeats_input: bool) -> usize {
        let by_row = if repeats_input {
            self.own_row_bytes() / row.row.len().max(1)
        } else {
            usize::MAX
        };
        self.head_remaining().max(1).min(by_row.max(1))
    }
    fn scan_bytes(&self) -> usize {
        (self.remaining_bytes() / 2).max(1)
    }
    fn own_row_bytes(&self) -> usize {
        self.remaining_bytes() / 2
    }
    fn can_accept(&self, (items, bytes): (usize, usize)) -> bool {
        items <= self.head_remaining() && bytes <= self.remaining_bytes()
    }
    fn work(
        row: &PreparedRow,
        matches: &[(Vec<u8>, NonZeroU64)],
        repeats_input: bool,
    ) -> (usize, usize) {
        let bytes = Self::stored_row_bytes(row).saturating_add(if repeats_input {
            matches.len().saturating_mul(row.row.len())
        } else {
            0
        });
        (matches.len().max(1), bytes.max(1))
    }
    fn scanned_bytes(row: &PreparedRow, matches: &[(Vec<u8>, NonZeroU64)]) -> usize {
        matches.iter().fold(0_usize, |bytes, matched| {
            bytes
                .saturating_add(partition_bytes(&row.key))
                .saturating_add(matched.0.len())
                .saturating_add(8)
        })
    }
    fn stored_row_bytes(row: &PreparedRow) -> usize {
        partition_bytes(&row.key)
            .saturating_add(8)
            .saturating_add(row.row.len())
    }
    fn exhausted(&self) -> bool {
        self.head_remaining() == 0 || self.remaining_bytes() == 0
    }
}

impl PagedOperation for EquiJoinOperation {
    fn initial_resume(&self) -> Resume {
        Resume {
            ordinal: 0,
            cursor: Cursor::EquiJoin(JoinCursor {
                found_match: false,
                resume_after: None,
            }),
        }
    }
    fn validate_resume(
        &self,
        input: OperationInput<'_>,
        resume: &Resume,
    ) -> Result<(), OperationError> {
        let cursor = self.cursor(input, resume)?;
        if cursor.resume_after.is_some() {
            let slice = input
                .change
                .try_slice(usize::try_from(resume.ordinal)?, 1)?;
            let mut budget = StepBudget::new(1, 4 * 1024 * 1024);
            let driving = self.prepare_window(
                OperationInput {
                    port: input.port,
                    change: &slice,
                },
                &mut budget,
            )?;
            self.validate_cursor_binding(input, cursor, &driving[0], &mut budget)?;
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
        self.compute_window(input, resume, access, budget)
    }
}

fn adjusted_weight(weight: u64, difference: i64) -> Result<u64, EquiJoinError> {
    if difference > 0 {
        weight
            .checked_add(difference.unsigned_abs())
            .ok_or(EquiJoinError::WeightOverflow)
    } else {
        weight
            .checked_sub(difference.unsigned_abs())
            .ok_or(EquiJoinError::NegativeWeight)
    }
}

fn adjust_match_count(
    count: u64,
    transition: KeyTransition,
    amount: u64,
) -> Result<u64, EquiJoinError> {
    match transition {
        KeyTransition::None => Ok(count),
        KeyTransition::First => count
            .checked_add(amount)
            .ok_or(EquiJoinError::MatchCountOverflow),
        KeyTransition::Last => count
            .checked_sub(amount)
            .ok_or(EquiJoinError::MatchCountUnderflow),
    }
}

fn match_transition(before: u64, after: u64) -> MatchTransition {
    match (before == 0, after == 0) {
        (true, false) => MatchTransition::BecameMatched,
        (false, true) => MatchTransition::BecameUnmatched,
        _ => MatchTransition::None,
    }
}

fn map_weight_error(error: StoreError) -> EquiJoinError {
    match error {
        StoreError::MultiplicityUnderflow => EquiJoinError::NegativeWeight,
        StoreError::MultiplicityOverflow => EquiJoinError::WeightOverflow,
        source => EquiJoinError::Store(source),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::num::NonZeroU64;

    use super::{ActiveRow, PreparedRow, StepBudget, partition_bytes};

    #[test]
    fn active_row_materializes_values_lazily_once() {
        let records = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["wide payload"]))],
        )
        .unwrap();
        let prepared = PreparedRow {
            row: vec![0],
            key: vec![1],
            matchable: true,
            difference: 1,
        };
        let row = ActiveRow::new(&prepared, &records, 0);

        assert!(row.values.get().is_none());
        let first = row.values().unwrap().as_ptr();
        assert!(row.values.get().is_some());
        assert_eq!(row.values().unwrap().as_ptr(), first);
    }

    #[test]
    fn page_work_counts_partition_framing_and_key_for_every_candidate() {
        let row = PreparedRow {
            row: vec![0; 5],
            key: vec![0; 100],
            matchable: true,
            difference: 1,
        };
        let matches = [
            (vec![0; 3], NonZeroU64::new(1).unwrap()),
            (vec![0; 4], NonZeroU64::new(1).unwrap()),
        ];

        assert_eq!(
            StepBudget::work(&row, &[], false),
            (1, partition_bytes(&row.key) + 8 + row.row.len())
        );
        assert_eq!(
            (
                StepBudget::work(&row, &matches, true).0,
                StepBudget::scanned_bytes(&row, &matches)
                    + StepBudget::work(&row, &matches, true).1
            ),
            (
                2,
                partition_bytes(&row.key)
                    + 8
                    + row.row.len()
                    + 2 * (partition_bytes(&row.key) + 8 + row.row.len())
                    + 3
                    + 4
            )
        );
        assert_eq!(
            (
                StepBudget::work(&row, &matches, false).0,
                StepBudget::scanned_bytes(&row, &matches)
                    + StepBudget::work(&row, &matches, false).1
            ),
            (
                2,
                partition_bytes(&row.key)
                    + 8
                    + row.row.len()
                    + 2 * (partition_bytes(&row.key) + 8)
                    + 3
                    + 4
            )
        );
    }

    #[test]
    fn failed_key_encoding_charges_the_materialized_prefix() {
        use super::{BoundKey, BoundKeyPair, EquiJoinOperation};
        use crate::operation::transform::EquiJoinKind;
        use crate::{
            col,
            expression::StoredExpression,
            operation::{BudgetExceeded, OperationInput},
        };
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["x".repeat(128)]))],
        )
        .unwrap();
        let change =
            dogpaddle_change::Change::try_new(records, arrow_array::Int64Array::from(vec![1]))
                .unwrap();
        let expression = StoredExpression::try_new(col("key")).unwrap();
        let mut setup = dogpaddle_store::StoreSetup::new();
        let mut scope = setup.data_scope();
        let operation = EquiJoinOperation {
            kind: EquiJoinKind::Inner,
            input_schemas: [Arc::clone(&schema), Arc::clone(&schema)],
            candidate_schema: Arc::clone(&schema),
            output_schema: Arc::clone(&schema),
            keys: vec![BoundKeyPair {
                left: BoundKey::new(expression.bind(Arc::clone(&schema)).unwrap()),
                right: BoundKey::new(expression.bind(schema).unwrap()),
            }]
            .into_boxed_slice(),
            residual: None,
            nulls: [Vec::new(), Vec::new()],
            left_rows: scope.data("left").unwrap(),
            right_rows: scope.data("right").unwrap(),
            key_counts: None,
            match_counts: None,
        };
        // Utf8 canonical framing writes its marker and u64 length before the
        // payload can be rejected. That allocated prefix belongs to this attempt.
        let mut budget = StepBudget::new(
            1,
            std::mem::size_of::<PreparedRow>()
                + crate::operation::logical_array_bytes(change.records().column(0).as_ref())
                + 9,
        );
        let error = operation
            .prepare_window(
                OperationInput {
                    port: 0,
                    change: &change,
                },
                &mut budget,
            )
            .err()
            .unwrap();
        assert!(error.is::<BudgetExceeded>());
        assert_eq!(budget.remaining_bytes(), 0);
    }
}
