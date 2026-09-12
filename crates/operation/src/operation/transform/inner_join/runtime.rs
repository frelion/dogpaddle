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
    InnerEquiJoinError,
    state::{Continuation, JoinContinuation, Phase, Rows},
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

/// Materialized, exact-Schema inner equality join.
///
/// The runtime keeps both input relations in private durable multisets. A
/// durable continuation first validates every match for the pinned input
/// Change, then emits bounded pages. This makes output-difference overflow and
/// corrupt stored rows fail before any page from that Change is published.
pub struct InnerEquiJoinOperation {
    input_schemas: [SchemaRef; 2],
    output_schema: SchemaRef,
    keys: Box<[BoundKeyPair]>,
    left_rows: Rows,
    right_rows: Rows,
    continuation: Continuation,
    prepared: Option<PreparedClaim>,
}

struct PreparedClaim {
    port: usize,
    rows: Vec<PreparedRow>,
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

impl InnerEquiJoinOperation {
    pub(super) fn new_bound(
        input_schemas: [SchemaRef; 2],
        output_schema: SchemaRef,
        keys: Box<[BoundKeyPair]>,
        left_rows: Rows,
        right_rows: Rows,
        continuation: Continuation,
    ) -> Self {
        Self {
            input_schemas,
            output_schema,
            keys,
            left_rows,
            right_rows,
            continuation,
            prepared: None,
        }
    }

    fn validate_input(&self, input: OperationInput<'_>) -> Result<(), InnerEquiJoinError> {
        if input.port >= self.input_schemas.len() {
            return Err(InnerEquiJoinError::InvalidInputPort { port: input.port });
        }
        if input.change.schema().as_ref() != self.input_schemas[input.port].as_ref() {
            return Err(InnerEquiJoinError::InputSchemaMismatch { port: input.port });
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
                    .map_err(|source| InnerEquiJoinError::KeyExpression {
                        port: input.port,
                        key,
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows = Vec::with_capacity(input.change.num_rows());
        for index in 0..input.change.num_rows() {
            let row = canonical_row(records, index)
                .map_err(|source| InnerEquiJoinError::CanonicalRow { source })?;
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
                    |source| InnerEquiJoinError::CanonicalRow {
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
        })
    }

    fn apply_claim(
        &self,
        claim: &PreparedClaim,
        access: TransactionAccess<'_>,
    ) -> Result<Action, InnerEquiJoinError> {
        let mut continuation = self.continuation.access(access)?;
        let mut state = if let Some(state) = continuation.get()? {
            Self::validate_continuation(claim, &state)?;
            state
        } else {
            self.preflight_admission(claim, access)?;
            JoinContinuation {
                port: u8::try_from(claim.port).expect("the two validated Join ports fit in a byte"),
                phase: Phase::Probe,
                row: 0,
                resume_after: None,
            }
        };

        let mut budget = TurnBudget::new();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        loop {
            let row_index = usize::try_from(state.row)
                .map_err(|_| InnerEquiJoinError::InvalidContinuation("row exceeds usize"))?;
            let row = &claim.rows[row_index];
            if !budget.can_start(row) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            let Some(page) = self.scan_matches(
                claim.port,
                row,
                state.resume_after.as_ref(),
                &budget,
                access,
            )?
            else {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            };
            if !budget.can_accept(row, &page.entries) {
                continuation.set(&state)?;
                return Ok(Action::Commit(output.finish(&self.output_schema)?));
            }
            match state.phase {
                Phase::Probe => self.validate_output_page(claim.port, row, &page.entries)?,
                Phase::Emit => {
                    self.append_output_page(claim.port, row, &page.entries, &mut output)?;
                }
            }
            budget.charge(row, &page.entries);

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
        access: TransactionAccess<'_>,
    ) -> Result<(), InnerEquiJoinError> {
        let mut shadow = HashMap::<(&[u8], &[u8]), u64>::new();
        let mut own_rows = self.rows(claim.port).access(access)?;
        for row in &claim.rows {
            let identity = (row.key.as_slice(), row.row.as_slice());
            let weight = match shadow.entry(identity) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let current = own_rows.partition(&row.key)?.multiplicity(&row.row)?;
                    entry.insert(current)
                }
            };
            *weight = adjusted_weight(*weight, row.difference)?;
        }
        Ok(())
    }

    fn validate_continuation(
        claim: &PreparedClaim,
        state: &JoinContinuation,
    ) -> Result<(), InnerEquiJoinError> {
        if usize::from(state.port) != claim.port {
            return Err(InnerEquiJoinError::InvalidContinuation(
                "port differs from the pinned input",
            ));
        }
        let row = usize::try_from(state.row)
            .ok()
            .filter(|row| *row < claim.rows.len())
            .ok_or(InnerEquiJoinError::InvalidContinuation(
                "row is outside the pinned input",
            ))?;
        if state.resume_after.is_none() {
            return Ok(());
        }
        if !claim.rows[row].matchable {
            return Err(InnerEquiJoinError::InvalidContinuation(
                "NULL key has an opposite-row cursor",
            ));
        }
        Ok(())
    }

    fn scan_matches(
        &self,
        port: usize,
        row: &PreparedRow,
        resume_after: Option<&Vec<u8>>,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<MultisetPage<Vec<u8>>>, InnerEquiJoinError> {
        if !row.matchable {
            return Ok(Some(MultisetPage {
                entries: Vec::new(),
                continuation: None,
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        let max_items = budget.max_scan_items(row);
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
        matches: &[MultisetEntry<Vec<u8>>],
    ) -> Result<(), InnerEquiJoinError> {
        let mut output = OutputRows::new(self.output_schema.fields().len());
        self.append_output_page(port, input, matches, &mut output)?;
        output.finish(&self.output_schema).map(|_| ())
    }

    fn append_output_page(
        &self,
        port: usize,
        input: &PreparedRow,
        matches: &[MultisetEntry<Vec<u8>>],
        output: &mut OutputRows,
    ) -> Result<(), InnerEquiJoinError> {
        let opposite_schema = &self.input_schemas[1 - port];
        for matched in matches {
            let opposite =
                decode_canonical_row(opposite_schema, &matched.key).map_err(|source| {
                    InnerEquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }
                })?;
            let difference = i128::from(input.difference) * i128::from(matched.multiplicity);
            let difference = i64::try_from(difference)
                .map_err(|_| InnerEquiJoinError::OutputDifferenceOverflow)?;
            if port == 0 {
                output.push(&input.values, &opposite, difference);
            } else {
                output.push(&opposite, &input.values, difference);
            }
        }
        Ok(())
    }

    fn adjust_own_row(
        &self,
        port: usize,
        row: &PreparedRow,
        access: TransactionAccess<'_>,
    ) -> Result<(), InnerEquiJoinError> {
        self.rows(port)
            .access(access)?
            .partition(&row.key)?
            .adjust(&row.row, row.difference)
            .map(|_| ())
            .map_err(map_weight_error)
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

    fn max_scan_items(&self, row: &PreparedRow) -> usize {
        let by_own_row = self.own_row_bytes() / row.row.len().max(1);
        self.remaining_items().min(by_own_row.max(1))
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

    fn charge(&mut self, row: &PreparedRow, matches: &[MultisetEntry<Vec<u8>>]) {
        let (items, bytes) = Self::work(row, matches);
        self.items = self.items.saturating_add(items);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn can_accept(&self, row: &PreparedRow, matches: &[MultisetEntry<Vec<u8>>]) -> bool {
        if self.is_empty() {
            return true;
        }
        let (items, bytes) = Self::work(row, matches);
        self.items.saturating_add(items) <= TURN_ITEMS
            && self.bytes.saturating_add(bytes) <= TURN_BYTES
    }

    fn work(row: &PreparedRow, matches: &[MultisetEntry<Vec<u8>>]) -> (usize, usize) {
        let items = matches.len().max(1);
        let bytes = if matches.is_empty() {
            row.row.len().saturating_add(row.key.len())
        } else {
            matches.iter().fold(row.key.len(), |bytes, matched| {
                bytes
                    .saturating_add(row.row.len())
                    .saturating_add(matched.key.len())
            })
        };
        (items, bytes.max(1))
    }

    fn exhausted(&self) -> bool {
        self.items >= TURN_ITEMS || self.bytes >= TURN_BYTES
    }
}

impl TurnOperation for InnerEquiJoinOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let input =
            input.ok_or_else(|| Box::new(InnerEquiJoinError::MissingInput) as OperationError)?;
        self.validate_input(input)?;
        if self.prepared.is_none() {
            self.prepared = Some(self.prepare_claim(input)?);
        }
        let claim = self
            .prepared
            .take()
            .expect("the prepared Join Claim was initialized above");
        Ok(Turn::ready(move |access| {
            let action = self.apply_claim(&claim, access)?;
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

fn adjusted_weight(weight: u64, difference: i64) -> Result<u64, InnerEquiJoinError> {
    if difference > 0 {
        weight
            .checked_add(difference.unsigned_abs())
            .ok_or(InnerEquiJoinError::WeightOverflow)
    } else {
        weight
            .checked_sub(difference.unsigned_abs())
            .ok_or(InnerEquiJoinError::NegativeWeight)
    }
}

fn persistent_row(row: usize) -> Result<u64, InnerEquiJoinError> {
    u64::try_from(row).map_err(|_| InnerEquiJoinError::InvalidContinuation("input row exceeds u64"))
}

fn map_weight_error(error: StoreError) -> InnerEquiJoinError {
    match error {
        StoreError::MultiplicityUnderflow => InnerEquiJoinError::NegativeWeight,
        StoreError::MultiplicityOverflow => InnerEquiJoinError::WeightOverflow,
        source => InnerEquiJoinError::Store(source),
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

    fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, InnerEquiJoinError> {
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
