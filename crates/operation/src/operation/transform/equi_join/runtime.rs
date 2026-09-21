// Input progress and durable writes stay here. Candidate evaluation and
// output construction are private children and share this runtime's state.
mod matches;
mod output;

use std::{cell::OnceCell, collections::HashMap, mem::size_of, ops::Deref, sync::Arc};

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_store::{MultisetEntry, OrderedMapAccess, StoreError, TransactionAccess};

use crate::{
    expression::BoundExpression,
    operation::{
        Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation,
        relation::{canonical_row, encode_canonical},
    },
};

use super::{
    EquiJoinError, EquiJoinKind,
    state::{
        Continuation, Counts, JoinContinuation, KeyCounts, MatchCounts, Rows, actual_match_key,
    },
};

use output::OutputRows;

const TURN_ITEMS: usize = 256;
const TURN_BYTES: usize = 4 * 1024 * 1024;
// A PartitionedMultiset<Vec<u8>, Vec<u8>> entry repeats an eight-byte
// partition-length frame and an eight-byte multiplicity around its two keys.
const PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES: usize = 2 * size_of::<u64>();

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
/// durable continuation advances through bounded output pages after exact-row
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
    pub(super) continuation: Continuation,
    pub(super) key_counts: Option<Counts>,
    pub(super) match_counts: Option<MatchCounts>,
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
    work: (usize, usize),
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
                .expect("a turn-local row is initialized by only one thread");
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

    fn prepare_claim(&self, input: OperationInput<'_>) -> Result<PreparedClaim, OperationError> {
        let records = input.change.records();
        let mut rows = (0..input.change.num_rows())
            .map(|index| PreparedRow {
                row: Vec::new(),
                key: Vec::new(),
                matchable: true,
                difference: input.change.diffs().value(index),
            })
            .collect::<Vec<_>>();
        // Retain at most one evaluated key array. Encode full rows only after
        // all key arrays have been released; the complete Claim still passes
        // admission before any output is published.
        for (key, pair) in self.keys.iter().enumerate() {
            let bound = pair.for_port(input.port);
            let column = bound.expression.evaluate(records).map_err(|source| {
                EquiJoinError::KeyExpression {
                    port: input.port,
                    key,
                    source,
                }
            })?;
            for (index, row) in rows.iter_mut().enumerate() {
                row.matchable &= !column.is_null(index);
                encode_canonical(&bound.field, column.as_ref(), index, "key", &mut row.key)
                    .map_err(|source| EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    })?;
            }
        }
        for (index, row) in rows.iter_mut().enumerate() {
            row.row = canonical_row(records, index)
                .map_err(|source| EquiJoinError::CanonicalRow { source })?;
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
        records: &RecordBatch,
        access: TransactionAccess<'_>,
    ) -> Result<Action, EquiJoinError> {
        if self.residual.is_some() {
            self.apply_residual_claim(claim, records, access)
        } else {
            self.apply_equi_claim(claim, records, access)
        }
    }

    fn apply_equi_claim(
        &self,
        claim: &mut PreparedClaim,
        records: &RecordBatch,
        access: TransactionAccess<'_>,
    ) -> Result<Action, EquiJoinError> {
        let mut continuation = self.continuation.access(access)?;
        let mut state = if let Some(state) = continuation.get()? {
            Self::validate_continuation(claim, &state)?;
            state
        } else {
            JoinContinuation {
                port: u8::try_from(claim.port).expect("the two validated Join ports fit in a byte"),
                row: 0,
                found_match: false,
                resume_after: None,
            }
        };
        if claim.effects.is_none() {
            // Only complete earlier rows have changed own rows/counts. After
            // reopen, simulate the still-unapplied suffix from the durable row.
            let start = usize::try_from(state.row)
                .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?;
            claim.effects = Some(self.preflight_admission(claim, start, access)?);
        }

        let mut budget = TurnBudget::new();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        loop {
            let row_index = usize::try_from(state.row)
                .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?;
            let prepared = &claim.rows[row_index];
            let effect = claim.effects.as_ref().expect("the Claim has been admitted")[row_index];
            if !budget.can_start(prepared) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            let row = ActiveRow::new(prepared, records, row_index);
            let Some(page) = self.scan_matches(
                claim.port,
                &row,
                effect,
                state.resume_after.as_ref(),
                &budget,
                access,
            )?
            else {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            };
            let work = self.output_work(claim.port, &row, effect, &page.entries);
            if !budget.can_accept(work) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            self.append_output_page(claim.port, &row, effect, &page.entries, &mut output)?;
            budget.charge(work);

            if let Some(resume_after) = page.continuation {
                state.resume_after = Some(resume_after);
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }

            self.adjust_own_row(claim.port, &row, access)?;
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

    fn apply_residual_claim(
        &self,
        claim: &mut PreparedClaim,
        records: &RecordBatch,
        access: TransactionAccess<'_>,
    ) -> Result<Action, EquiJoinError> {
        debug_assert!(self.residual.is_some());
        let mut continuation = self.continuation.access(access)?;
        let mut state = if let Some(state) = continuation.get()? {
            Self::validate_residual_continuation(claim, &state)?;
            state
        } else {
            JoinContinuation {
                port: u8::try_from(claim.port).expect("the two validated Join ports fit in a byte"),
                row: 0,
                found_match: false,
                resume_after: None,
            }
        };
        if claim.effects.is_none() {
            // Partial pages may update support counts, but own rows change only
            // on the last page. Admission reads own rows, not support counts.
            let start = usize::try_from(state.row)
                .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?;
            claim.effects = Some(self.preflight_residual_admission(claim, start, access)?);
        }

        let mut budget = TurnBudget::new();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        loop {
            let row_index = usize::try_from(state.row)
                .map_err(|_| EquiJoinError::InvalidContinuation("row exceeds usize"))?;
            let prepared = &claim.rows[row_index];
            let effect = claim.effects.as_ref().expect("the Claim has been admitted")[row_index];
            if !budget.can_start(prepared) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            let row = ActiveRow::new(prepared, records, row_index);
            if self.kind.left_only() && matches!(effect.transition, KeyTransition::None) {
                if state.resume_after.is_some() {
                    return Err(EquiJoinError::InvalidContinuation(
                        "stable left-only row has an opposite-row cursor",
                    ));
                }
                state.found_match = self.stable_left_only_found_match(claim.port, &row, access)?;
            }
            let Some(page) = self.scan_residual_matches(
                claim.port,
                &row,
                effect,
                state.resume_after.as_ref(),
                &budget,
                access,
            )?
            else {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            };
            if !budget.can_accept(page.work) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }

            self.emit_residual_page(
                claim.port,
                &row,
                effect,
                &page.matches,
                page.qualifying,
                &mut state,
                &mut output,
                access,
            )?;
            budget.charge(page.work);

            if let Some(resume_after) = page.continuation {
                state.resume_after = Some(resume_after);
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }

            self.append_residual_current(claim.port, &row, state.found_match, &mut output)?;
            self.validate_current_match_count(claim.port, &row, effect, state.found_match, access)?;
            self.adjust_own_row(claim.port, &row, access)?;
            let next = row_index + 1;
            if next == claim.rows.len() {
                continuation.clear()?;
                return Ok(Action::Complete(output.finish(&self.output_schema)?));
            }
            state.row = persistent_row(next)?;
            state.found_match = false;
            state.resume_after = None;
            if budget.exhausted() {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
        }
    }

    fn preflight_residual_admission(
        &self,
        claim: &PreparedClaim,
        start: usize,
        access: TransactionAccess<'_>,
    ) -> Result<Vec<RowEffect>, EquiJoinError> {
        let mut shadow = HashMap::<(&[u8], &[u8]), u64>::new();
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
            effect.transition = match (before == 0, *weight == 0) {
                (true, false) => KeyTransition::First,
                (false, true) => KeyTransition::Last,
                _ => KeyTransition::None,
            };
        }
        Ok(effects)
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
        state: &mut JoinContinuation,
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
        let before = match counts.get(&key)? {
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
            counts.remove(&key)?;
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
        match counts.get(&actual_match_key(port, row))? {
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

    fn validate_residual_continuation(
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
        if state.resume_after.is_some() && !claim.rows[row].matchable {
            return Err(EquiJoinError::InvalidContinuation(
                "NULL key has an opposite-row cursor",
            ));
        }
        if state.found_match && state.resume_after.is_none() {
            return Err(EquiJoinError::InvalidContinuation(
                "matched row has no committed opposite-row cursor",
            ));
        }
        Ok(())
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
        if state.found_match {
            return Err(EquiJoinError::InvalidContinuation(
                "pure equality join has residual continuation state",
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
            bytes = bytes.saturating_add(
                self.nulls[1 - port]
                    .len()
                    .saturating_mul(size_of::<ScalarValue>()),
            );
        } else if self.kind.preserves(1 - port) && !matches!(effect.transition, KeyTransition::None)
        {
            items = items.saturating_add(matches.len());
            for matched in matches {
                bytes = bytes.saturating_add(matched.key.len()).saturating_add(
                    self.nulls[port]
                        .len()
                        .saturating_mul(size_of::<ScalarValue>()),
                );
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
            && Self::stored_row_bytes(row).max(1) <= self.remaining_bytes()
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
        // Each processed page accounts for the driving row's durable access once.
        // Every candidate then repeats its partition key and Store framing; paths
        // that materialize relational pairs also repeat the driving row in output.
        let bytes = matches
            .iter()
            .fold(Self::stored_row_bytes(row), |bytes, matched| {
                bytes
                    .saturating_add(PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES)
                    .saturating_add(row.key.len())
                    .saturating_add(matched.key.len())
                    .saturating_add(if repeats_input { row.row.len() } else { 0 })
            });
        (items, bytes.max(1))
    }

    fn stored_row_bytes(row: &PreparedRow) -> usize {
        PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES
            .saturating_add(row.key.len())
            .saturating_add(row.row.len())
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
        let Some(input) = input else {
            return Ok(Turn::Idle);
        };
        self.validate_input(input)?;
        if self.prepared.is_none() {
            self.prepared = Some(self.prepare_claim(input)?);
        }
        let mut claim = self
            .prepared
            .take()
            .expect("the prepared Join Claim was initialized above");
        let records = input.change.records();
        Ok(Turn::ready(move |access| {
            let action = self.apply_claim(&mut claim, records, access)?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use dogpaddle_store::MultisetEntry;

    use super::{ActiveRow, PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES, PreparedRow, TurnBudget};

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
    fn turn_work_counts_partition_framing_and_key_for_every_candidate() {
        let row = PreparedRow {
            row: vec![0; 5],
            key: vec![0; 100],
            matchable: true,
            difference: 1,
        };
        let matches = [
            MultisetEntry {
                key: vec![0; 3],
                multiplicity: 1,
            },
            MultisetEntry {
                key: vec![0; 4],
                multiplicity: 1,
            },
        ];

        assert_eq!(
            TurnBudget::work(&row, &[], false),
            (
                1,
                PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES + row.key.len() + row.row.len()
            )
        );
        assert_eq!(
            TurnBudget::work(&row, &matches, true),
            (
                2,
                PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES
                    + row.key.len()
                    + row.row.len()
                    + 2 * (PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES
                        + row.key.len()
                        + row.row.len())
                    + 3
                    + 4
            )
        );
        assert_eq!(
            TurnBudget::work(&row, &matches, false),
            (
                2,
                PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES
                    + row.key.len()
                    + row.row.len()
                    + 2 * (PARTITIONED_MULTISET_ENTRY_FRAMING_BYTES + row.key.len())
                    + 3
                    + 4
            )
        );
    }
}
