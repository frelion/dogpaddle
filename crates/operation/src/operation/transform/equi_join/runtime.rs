use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{
    MultisetEntry, MultisetPage, ScanDirection, ScanLimit, StoreError, TransactionAccess,
};

use crate::{
    expression::BoundExpression,
    operation::{
        Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation,
        relation::{canonical_row, decode_canonical_row, encode_canonical},
    },
};

use super::{
    EquiJoinError, EquiJoinKind,
    state::{Continuation, Counts, JoinContinuation, KeyCounts, Phase, Rows},
};

const TURN_ITEMS: usize = 256;
const TURN_BYTES: usize = 4 * 1024 * 1024;

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
/// The runtime keeps both input relations in private durable multisets. A
/// durable continuation first validates every match for the pinned input
/// Change, then emits bounded pages. This makes output-difference overflow and
/// corrupt stored rows fail before any page from that Change is published.
pub struct EquiJoinOperation {
    pub(super) kind: EquiJoinKind,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) output_schema: SchemaRef,
    pub(super) keys: Box<[BoundKeyPair]>,
    pub(super) nulls: [Vec<ScalarValue>; 2],
    pub(super) left_rows: Rows,
    pub(super) right_rows: Rows,
    pub(super) continuation: Continuation,
    pub(super) key_counts: Option<Counts>,
    pub(super) prepared: Option<PreparedClaim>,
}

pub(super) struct PreparedClaim {
    port: usize,
    rows: Vec<PreparedRow>,
    effects: Option<Vec<RowEffect>>,
}

#[derive(Clone, Copy, Default)]
enum KeyTransition {
    #[default]
    None,
    First,
    Last,
}

#[derive(Clone, Copy, Default)]
struct RowEffect {
    matched: bool,
    transition: KeyTransition,
}

struct PreparedRow {
    row: Vec<u8>,
    values: Vec<ScalarValue>,
    key: Vec<u8>,
    matchable: bool,
    difference: i64,
}

struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    differences: Vec<i64>,
}

struct TurnBudget {
    items: usize,
    bytes: usize,
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
            _ => unreachable!("a prepared Join claim has a validated port"),
        }
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

    fn prepare_claim(&self, input: OperationInput<'_>) -> Result<PreparedClaim, OperationError> {
        let records = input.change.records();
        let key_columns = self
            .keys
            .iter()
            .enumerate()
            .map(|(key, pair)| {
                pair.for_port(input.port)
                    .expression
                    .evaluate(records)
                    .map_err(|source| EquiJoinError::KeyExpression {
                        port: input.port,
                        key,
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows = Vec::with_capacity(input.change.num_rows());
        for index in 0..input.change.num_rows() {
            let row = canonical_row(records, index)
                .map_err(|source| EquiJoinError::CanonicalRow { source })?;
            let values = records
                .columns()
                .iter()
                .map(|column| ScalarValue::try_from_array(column.as_ref(), index))
                .collect::<Result<Vec<_>, _>>()?;
            let mut key = Vec::new();
            let mut matchable = true;
            for (pair, column) in self.keys.iter().zip(&key_columns) {
                let bound = pair.for_port(input.port);
                matchable &= !column.is_null(index);
                encode_canonical(&bound.field, column.as_ref(), index, "key", &mut key).map_err(
                    |source| EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    },
                )?;
            }
            rows.push(PreparedRow {
                row,
                values,
                key,
                matchable,
                difference: input.change.diffs().value(index),
            });
        }
        Ok(PreparedClaim {
            port: input.port,
            rows,
            effects: None,
        })
    }

    fn apply_claim(
        &self,
        claim: &mut PreparedClaim,
        access: TransactionAccess<'_>,
    ) -> Result<Action, EquiJoinError> {
        let mut continuation = self.continuation.access(access)?;
        let mut state = if let Some(state) = continuation.get()? {
            Self::validate_continuation(claim, &state)?;
            state
        } else {
            JoinContinuation {
                port: u8::try_from(claim.port).expect("the two validated Join ports fit in a byte"),
                phase: Phase::Probe,
                row: 0,
                resume_after: None,
            }
        };
        if claim.effects.is_none() {
            // Probe has not changed either relation. Emit has committed only
            // earlier rows, so reopen simulates the still-unapplied suffix.
            let start = match state.phase {
                Phase::Probe => 0,
                Phase::Emit => usize::try_from(state.row)
                    .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?,
            };
            claim.effects = Some(self.preflight_admission(claim, start, access)?);
        }

        let mut budget = TurnBudget::new();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        loop {
            let row_index = usize::try_from(state.row)
                .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?;
            let row = &claim.rows[row_index];
            let effect = claim
                .effects
                .as_ref()
                .expect("the Claim has been preflighted")[row_index];
            if !budget.can_start(row) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            let Some(page) = self.scan_matches(
                claim.port,
                row,
                effect,
                state.resume_after.as_ref(),
                &budget,
                access,
            )?
            else {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            };
            let work = self.output_work(claim.port, row, effect, &page.entries);
            if !budget.can_accept(work) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            match state.phase {
                Phase::Probe => {
                    self.validate_output_page(claim.port, row, effect, &page.entries)?;
                }
                Phase::Emit => {
                    self.append_output_page(claim.port, row, effect, &page.entries, &mut output)?;
                }
            }
            budget.charge(work);

            if let Some(resume_after) = page.continuation {
                state.resume_after = Some(resume_after);
                continuation.set(&state)?;
                return Ok(match state.phase {
                    Phase::Probe => Action::Commit(None),
                    Phase::Emit => Action::Commit(output.finish(&self.output_schema)?),
                });
            }

            match state.phase {
                Phase::Probe => {
                    let next = row_index + 1;
                    if next == claim.rows.len() {
                        state.phase = Phase::Emit;
                        state.row = 0;
                    } else {
                        state.row = persistent_row(next)?;
                    }
                    state.resume_after = None;
                    if budget.exhausted() {
                        continuation.set(&state)?;
                        return Ok(Action::Commit(None));
                    }
                }
                Phase::Emit => {
                    self.adjust_own_row(claim.port, row, access)?;
                    let next = row_index + 1;
                    if next == claim.rows.len() {
                        continuation.clear()?;
                        return Ok(Action::Complete(output.finish(&self.output_schema)?));
                    }
                    state.row = persistent_row(next)?;
                    state.resume_after = None;
                    if budget.exhausted() {
                        continuation.set(&state)?;
                        return Ok(Action::Commit(output.finish(&self.output_schema)?));
                    }
                }
            }
        }
    }

    fn preflight_admission(
        &self,
        claim: &PreparedClaim,
        start: usize,
        access: TransactionAccess<'_>,
    ) -> Result<Vec<RowEffect>, EquiJoinError> {
        let mut shadow = HashMap::<(&[u8], &[u8]), u64>::new();
        let mut key_shadow = HashMap::<&[u8], KeyCounts>::new();
        let counts = self
            .key_counts
            .as_ref()
            .map(|counts| counts.access(access))
            .transpose()?;
        let mut own_rows = self.rows(claim.port).access(access)?;
        let mut effects = vec![RowEffect::default(); claim.rows.len()];
        for (row, effect) in claim.rows[start..].iter().zip(&mut effects[start..]) {
            let identity = (row.key.as_slice(), row.row.as_slice());
            let weight = match shadow.entry(identity) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let current = own_rows.partition(&row.key)?.multiplicity(&row.row)?;
                    entry.insert(current)
                }
            };
            let before = *weight;
            *weight = adjusted_weight(before, row.difference)?;
            effect.matched = row.matchable;
            if row.matchable
                && let Some(counts) = &counts
            {
                let counts = match key_shadow.entry(&row.key) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(counts.get(&row.key)?.unwrap_or_default())
                    }
                };
                let key_before = counts.0[claim.port];
                effect.matched &= counts.0[1 - claim.port] > 0;
                counts.adjust(claim.port, before, *weight)?;
                effect.transition = match (key_before == 0, counts.0[claim.port] == 0) {
                    (true, false) => KeyTransition::First,
                    (false, true) => KeyTransition::Last,
                    _ => KeyTransition::None,
                };
            }
        }
        Ok(effects)
    }

    fn validate_continuation(
        claim: &PreparedClaim,
        state: &JoinContinuation,
    ) -> Result<(), EquiJoinError> {
        if usize::from(state.port) != claim.port {
            return Err(EquiJoinError::InvalidContinuation(
                "port differs from the pinned input",
            ));
        }
        let row = usize::try_from(state.row)
            .ok()
            .filter(|row| *row < claim.rows.len())
            .ok_or(EquiJoinError::InvalidContinuation(
                "row is outside the pinned input",
            ))?;
        if state.resume_after.is_none() {
            return Ok(());
        }
        if !claim.rows[row].matchable {
            return Err(EquiJoinError::InvalidContinuation(
                "NULL key has an opposite-row cursor",
            ));
        }
        Ok(())
    }

    fn scan_matches(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        resume_after: Option<&Vec<u8>>,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<MultisetPage<Vec<u8>>>, EquiJoinError> {
        if !effect.matched
            || (self.kind.left_only()
                && (port == 0 || matches!(effect.transition, KeyTransition::None)))
        {
            return Ok(Some(MultisetPage {
                entries: Vec::new(),
                continuation: None,
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        let expanded =
            self.kind.preserves(1 - port) && !matches!(effect.transition, KeyTransition::None);
        let repeats_input = !(self.kind.left_only() && port == 1);
        let max_items = budget.max_scan_items(row, expanded, repeats_input);
        let max_bytes = budget.scan_bytes();
        let limit =
            ScanLimit::new(max_items, max_bytes).expect("positive Join page limits are valid");
        match partition.scan(ScanDirection::Ascending, resume_after, limit) {
            Ok(page) => Ok(Some(page)),
            Err(StoreError::ItemTooLarge { .. }) if !budget.is_empty() => Ok(None),
            Err(StoreError::ItemTooLarge { size, .. }) => {
                let limit = ScanLimit::new(1, size.max(1))
                    .expect("one item and a positive observed byte size are valid");
                Ok(Some(partition.scan(
                    ScanDirection::Ascending,
                    resume_after,
                    limit,
                )?))
            }
            Err(source) => Err(source.into()),
        }
    }

    fn validate_output_page(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
    ) -> Result<(), EquiJoinError> {
        let mut output = OutputRows::new(self.output_schema.fields().len());
        self.append_output_page(port, input, effect, matches, &mut output)?;
        output.finish(&self.output_schema).map(|_| ())
    }

    fn append_output_page(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        if self.kind.left_only() {
            return self.append_existence_output(port, input, effect, matches, output);
        }
        if matches.is_empty() && !effect.matched && self.kind.preserves(port) {
            self.append_padded(port, &input.values, input.difference, output);
        }
        let opposite_schema = &self.input_schemas[1 - port];
        for matched in matches {
            let opposite =
                decode_canonical_row(opposite_schema, &matched.key).map_err(|source| {
                    EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }
                })?;
            let difference =
                output_difference(i128::from(input.difference) * i128::from(matched.multiplicity))?;
            // A match and its NULL-row correction share one cursor position
            // and transaction, including when both have identical values.
            if self.kind.preserves(1 - port) && matches!(effect.transition, KeyTransition::First) {
                self.append_padded(
                    1 - port,
                    &opposite,
                    output_difference(-i128::from(matched.multiplicity))?,
                    output,
                );
            }
            if port == 0 {
                output.push(&input.values, &opposite, difference);
            } else {
                output.push(&opposite, &input.values, difference);
            }
            if self.kind.preserves(1 - port) && matches!(effect.transition, KeyTransition::Last) {
                self.append_padded(
                    1 - port,
                    &opposite,
                    output_difference(i128::from(matched.multiplicity))?,
                    output,
                );
            }
        }
        Ok(())
    }

    fn append_existence_output(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        let semi = self.kind == EquiJoinKind::LeftSemi;
        if port == 0 {
            if effect.matched == semi {
                output.push(&input.values, &[], input.difference);
            }
            return Ok(());
        }
        let sign = match effect.transition {
            KeyTransition::First => 1_i128,
            KeyTransition::Last => -1,
            KeyTransition::None => return Ok(()),
        } * if semi { 1 } else { -1 };
        for matched in matches {
            let left =
                decode_canonical_row(&self.input_schemas[0], &matched.key).map_err(|source| {
                    EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }
                })?;
            output.push(
                &left,
                &[],
                output_difference(sign * i128::from(matched.multiplicity))?,
            );
        }
        Ok(())
    }

    fn append_padded(
        &self,
        port: usize,
        values: &[ScalarValue],
        difference: i64,
        output: &mut OutputRows,
    ) {
        if port == 0 {
            output.push(values, &self.nulls[1], difference);
        } else {
            output.push(&self.nulls[0], values, difference);
        }
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
            let mut value = counts.get(&row.key)?.unwrap_or_default();
            value.adjust(port, change.before(), change.after())?;
            if value.is_empty() {
                counts.remove(&row.key)?;
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
        matches: &[MultisetEntry<Vec<u8>>],
    ) -> (usize, usize) {
        let repeats_input = !(self.kind.left_only() && port == 1);
        let (mut items, mut bytes) = TurnBudget::work(row, matches, repeats_input);
        if matches.is_empty() && self.kind.preserves(port) && !effect.matched {
            bytes = bytes.saturating_add(self.nulls[1 - port].len());
        } else if self.kind.preserves(1 - port) && !matches!(effect.transition, KeyTransition::None)
        {
            items = items.saturating_add(matches.len());
            for matched in matches {
                bytes = bytes
                    .saturating_add(matched.key.len())
                    .saturating_add(self.nulls[port].len());
            }
        }
        (items, bytes)
    }

    fn rows(&self, port: usize) -> &Rows {
        match port {
            0 => &self.left_rows,
            1 => &self.right_rows,
            _ => unreachable!("a prepared Join claim has a validated port"),
        }
    }
}

impl TurnBudget {
    const fn new() -> Self {
        Self { items: 0, bytes: 0 }
    }

    fn can_start(&self, row: &PreparedRow) -> bool {
        if self.items == 0 {
            return true;
        }
        self.items < TURN_ITEMS
            && self.bytes < TURN_BYTES
            && row.row.len().saturating_add(row.key.len()).max(1) <= self.remaining_bytes()
    }

    fn max_scan_items(&self, row: &PreparedRow, expanded: bool, repeats_input: bool) -> usize {
        let by_own_row = if repeats_input {
            self.own_row_bytes() / row.row.len().max(1)
        } else {
            usize::MAX
        };
        let per_match = if expanded { 2 } else { 1 };
        (self.remaining_items() / per_match)
            .max(1)
            .min(by_own_row.max(1))
    }

    fn scan_bytes(&self) -> usize {
        (self.remaining_bytes() / 2).max(1)
    }

    fn own_row_bytes(&self) -> usize {
        self.remaining_bytes() / 2
    }

    fn remaining_items(&self) -> usize {
        TURN_ITEMS.saturating_sub(self.items).max(1)
    }

    fn remaining_bytes(&self) -> usize {
        TURN_BYTES.saturating_sub(self.bytes)
    }

    const fn is_empty(&self) -> bool {
        self.items == 0
    }

    fn charge(&mut self, (items, bytes): (usize, usize)) {
        self.items = self.items.saturating_add(items);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn can_accept(&self, (items, bytes): (usize, usize)) -> bool {
        if self.is_empty() {
            return true;
        }
        self.items.saturating_add(items) <= TURN_ITEMS
            && self.bytes.saturating_add(bytes) <= TURN_BYTES
    }

    fn work(
        row: &PreparedRow,
        matches: &[MultisetEntry<Vec<u8>>],
        repeats_input: bool,
    ) -> (usize, usize) {
        let items = matches.len().max(1);
        let bytes = if matches.is_empty() {
            row.row.len().saturating_add(row.key.len())
        } else {
            matches.iter().fold(row.key.len(), |bytes, matched| {
                bytes
                    .saturating_add(if repeats_input { row.row.len() } else { 0 })
                    .saturating_add(matched.key.len())
            })
        };
        (items, bytes.max(1))
    }

    fn exhausted(&self) -> bool {
        self.items >= TURN_ITEMS || self.bytes >= TURN_BYTES
    }
}

impl TurnOperation for EquiJoinOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let input = input.ok_or_else(|| Box::new(EquiJoinError::MissingInput) as OperationError)?;
        self.validate_input(input)?;
        if self.prepared.is_none() {
            self.prepared = Some(self.prepare_claim(input)?);
        }
        let mut claim = self
            .prepared
            .take()
            .expect("the prepared Join Claim was initialized above");
        Ok(Turn::ready(move |access| {
            let action = self.apply_claim(&mut claim, access)?;
            let complete = matches!(&action, Action::Complete(_));
            self.prepared = Some(claim);
            let after_commit = if complete {
                AfterCommit::new(move || {
                    self.prepared = None;
                    Ok(())
                })
            } else {
                AfterCommit::none()
            };
            Ok((action, after_commit))
        }))
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

fn output_difference(difference: i128) -> Result<i64, EquiJoinError> {
    i64::try_from(difference).map_err(|_| EquiJoinError::OutputDifferenceOverflow)
}

fn persistent_row(row: usize) -> Result<u64, EquiJoinError> {
    u64::try_from(row).map_err(|_| EquiJoinError::InvalidContinuation("input row exceeds u64"))
}

fn map_weight_error(error: StoreError) -> EquiJoinError {
    match error {
        StoreError::MultiplicityUnderflow => EquiJoinError::NegativeWeight,
        StoreError::MultiplicityOverflow => EquiJoinError::WeightOverflow,
        source => EquiJoinError::Store(source),
    }
}

impl OutputRows {
    fn new(column_count: usize) -> Self {
        Self {
            columns: (0..column_count).map(|_| Vec::new()).collect(),
            differences: Vec::new(),
        }
    }

    fn push(&mut self, left: &[ScalarValue], right: &[ScalarValue], difference: i64) {
        debug_assert_eq!(self.columns.len(), left.len() + right.len());
        for (column, value) in self.columns.iter_mut().zip(left.iter().chain(right)) {
            column.push(value.clone());
        }
        self.differences.push(difference);
    }

    fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, EquiJoinError> {
        if self.differences.is_empty() {
            return Ok(None);
        }
        let row_count = self.differences.len();
        let columns = self
            .columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let records = RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)?;
        Ok(Some(Change::try_new(
            records,
            Int64Array::from(self.differences),
        )?))
    }
}
