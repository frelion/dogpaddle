use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    ops::Bound,
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, SchemaRef};
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::{
    CellAccess, OrderedMapAccess, ScanDirection, ScanLimit, StoreError, TransactionAccess,
};

use crate::{
    expression::BoundExpression,
    operation::{
        Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation,
        relation::{
            RowError, canonical_row_bounded, decode_canonical_row, encode_canonical_bounded,
            order_key, ordered_value,
        },
    },
};

use super::{
    AsOfDirection, AsOfEqualityMode, AsOfEquidistantPreference, AsOfJoinError, AsOfJoinKind,
    AsOfTieFallback,
    index::{
        ParsedIndexKey, matchable_partition_prefix, parse_row_key, prefix_successor,
        push_component, push_nullable_ordered_component, row_key, take_component,
    },
    state::{AsOfContinuation, Continuation, Phase, RowWeight, RowWeightError, Rows},
};

const TURN_ITEMS: usize = 256;
const TURN_BYTES: usize = 4 * 1024 * 1024;
const PREPARED_CLAIM_BYTES: usize = 64 * 1024 * 1024;
const CANDIDATE_ITEMS: usize = 64;
const CANDIDATE_BYTES: usize = 1024 * 1024;
const CANDIDATE_SCALAR_VALUES: usize = 16 * 1024;
const MAP_VALUE_BYTES: usize = size_of::<u64>();

pub(super) struct BoundScalar {
    pub(super) expression: BoundExpression,
    pub(super) field: Arc<Field>,
}

pub(super) struct BoundEqualityPair {
    pub(super) mode: AsOfEqualityMode,
    pub(super) left: BoundScalar,
    pub(super) right: BoundScalar,
}

pub(super) struct BoundOrderPair {
    pub(super) left: BoundScalar,
    pub(super) right: BoundScalar,
}

pub(super) struct BoundTieBreak {
    pub(super) value: BoundScalar,
    pub(super) descending: bool,
    pub(super) nulls_first: bool,
}

/// Materialized dynamic ASOF join.
pub struct AsOfJoinOperation {
    pub(super) kind: AsOfJoinKind,
    pub(super) direction: AsOfDirection,
    pub(super) tie_fallback: AsOfTieFallback,
    pub(super) tolerance: Option<u128>,
    pub(super) input_schemas: [SchemaRef; 2],
    pub(super) candidate_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) equalities: Box<[BoundEqualityPair]>,
    pub(super) orders: Box<[BoundOrderPair]>,
    pub(super) ties: Box<[BoundTieBreak]>,
    pub(super) right_nulls: Vec<ScalarValue>,
    pub(super) residual: Option<BoundExpression>,
    pub(super) left_rows: Rows,
    pub(super) right_rows: Rows,
    pub(super) continuation: Continuation,
    pub(super) prepared: Option<PreparedClaim>,
}

pub(super) struct PreparedClaim {
    port: usize,
    rows: Vec<PreparedRow>,
    effects: Option<Vec<RowEffect>>,
    overlay: CachedEventOverlay,
}

struct PreparedRow {
    key: Vec<u8>,
    partition: Vec<u8>,
    order: Vec<u8>,
    matchable: bool,
    difference: i64,
}

struct PreparingRow {
    row: Vec<u8>,
    partition: Vec<u8>,
    order: Vec<u8>,
    rank: Vec<u8>,
    matchable: bool,
    order_matchable: bool,
    difference: i64,
}

struct PreparationBudget {
    bytes: usize,
}

#[derive(Clone, Copy, Default)]
struct RowEffect {
    before: u64,
    after: u64,
}

impl RowEffect {
    const fn changes_presence(self) -> bool {
        (self.before == 0) != (self.after == 0)
    }
}

#[derive(Clone)]
struct Winner {
    key: Vec<u8>,
    order: Vec<u8>,
    rank: Vec<u8>,
    row: Vec<u8>,
}

struct Correction {
    left_row: Vec<u8>,
    left_weight: u64,
    before: Option<Winner>,
    after: Option<Winner>,
}

struct MergedCandidate {
    key: Vec<u8>,
    visible_before: bool,
    visible_after: bool,
}

struct MergedPage {
    entries: Vec<MergedCandidate>,
    continuation: Option<Vec<u8>>,
    work: (usize, usize),
}

type VisibilityOverlay = BTreeMap<Vec<u8>, RowEffect>;

#[derive(Default)]
struct CachedEventOverlay {
    phase: Option<Phase>,
    row: Option<usize>,
    weights: VisibilityOverlay,
}

struct SelectionPage {
    best_before: Option<Winner>,
    best_after: Option<Winner>,
    ambiguous_before: bool,
    ambiguous_after: bool,
    continuation: Option<Vec<u8>>,
    work: (usize, usize),
}

struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    differences: Vec<i64>,
}

#[derive(Clone)]
struct KeyRange {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

struct TurnBudget {
    items: usize,
    bytes: usize,
}

#[derive(Clone, Copy)]
enum Metric {
    Signed(i128),
    Unsigned(u128),
}

enum Step {
    Continue,
    Yield,
    Complete,
}

enum NextLeft {
    Row(Vec<u8>, u64, PreparedRow),
    Exhausted,
    Yield,
}

impl BoundEqualityPair {
    fn for_port(&self, port: usize) -> &BoundScalar {
        match port {
            0 => &self.left,
            1 => &self.right,
            _ => unreachable!("a prepared ASOF claim has a validated port"),
        }
    }
}

impl BoundOrderPair {
    fn for_port(&self, port: usize) -> &BoundScalar {
        match port {
            0 => &self.left,
            1 => &self.right,
            _ => unreachable!("a prepared ASOF claim has a validated port"),
        }
    }
}

impl PreparationBudget {
    fn new(rows: usize) -> Result<Self, AsOfJoinError> {
        let structural_bytes = rows
            .checked_mul(size_of::<PreparingRow>().saturating_add(size_of::<PreparedRow>()))
            .ok_or_else(prepared_claim_too_large)?;
        if structural_bytes > PREPARED_CLAIM_BYTES {
            return Err(prepared_claim_too_large());
        }
        Ok(Self {
            bytes: structural_bytes,
        })
    }

    fn remaining(&self) -> usize {
        PREPARED_CLAIM_BYTES.saturating_sub(self.bytes)
    }

    fn output_limit(&self, current: usize) -> Result<usize, AsOfJoinError> {
        current
            .checked_add(self.remaining())
            .ok_or_else(prepared_claim_too_large)
    }

    fn charge(&mut self, bytes: usize) -> Result<(), AsOfJoinError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= PREPARED_CLAIM_BYTES)
            .ok_or_else(prepared_claim_too_large)?;
        Ok(())
    }

    fn push_component(
        &mut self,
        output: &mut Vec<u8>,
        component: &[u8],
    ) -> Result<(), AsOfJoinError> {
        let bytes = framed_component_bytes(component).ok_or_else(prepared_claim_too_large)?;
        if bytes > self.remaining() {
            return Err(prepared_claim_too_large());
        }
        push_component(output, component);
        self.charge(bytes)
    }

    fn push_nullable_component(
        &mut self,
        output: &mut Vec<u8>,
        component: Option<&[u8]>,
        descending: bool,
        nulls_first: bool,
    ) -> Result<(), AsOfJoinError> {
        let marker = [u8::from(component.is_none() != nulls_first)];
        let bytes = framed_component_bytes(&marker)
            .and_then(|bytes| {
                component.map_or(Some(bytes), |component| {
                    bytes.checked_add(framed_component_bytes(component)?)
                })
            })
            .ok_or_else(prepared_claim_too_large)?;
        if bytes > self.remaining() {
            return Err(prepared_claim_too_large());
        }
        push_nullable_ordered_component(output, component, descending, nulls_first);
        self.charge(bytes)
    }

    fn row_key(
        &mut self,
        partition: &[u8],
        order: &[u8],
        rank: &[u8],
        row: &[u8],
    ) -> Result<Vec<u8>, AsOfJoinError> {
        let bytes = [partition, order, rank, row]
            .into_iter()
            .try_fold(0_usize, |bytes, component| {
                bytes.checked_add(framed_component_bytes(component)?)
            })
            .ok_or_else(prepared_claim_too_large)?;
        if bytes > self.remaining() {
            return Err(prepared_claim_too_large());
        }
        let key = row_key(partition, order, rank, row);
        debug_assert_eq!(key.len(), bytes);
        self.charge(bytes)?;
        Ok(key)
    }
}

fn framed_component_bytes(component: &[u8]) -> Option<usize> {
    component
        .iter()
        .try_fold(component.len().checked_add(2)?, |bytes, byte| {
            if *byte == 0 {
                bytes.checked_add(1)
            } else {
                Some(bytes)
            }
        })
}

const fn prepared_claim_too_large() -> AsOfJoinError {
    AsOfJoinError::PreparedClaimTooLarge {
        max_bytes: PREPARED_CLAIM_BYTES,
    }
}

fn map_preparation_row_error(source: OperationError) -> AsOfJoinError {
    if matches!(
        source.downcast_ref::<RowError>(),
        Some(RowError::SizeLimit { .. })
    ) {
        prepared_claim_too_large()
    } else {
        AsOfJoinError::CanonicalRow { source }
    }
}

fn map_preparation_codec_error(source: RowError) -> AsOfJoinError {
    if matches!(source, RowError::SizeLimit { .. }) {
        prepared_claim_too_large()
    } else {
        AsOfJoinError::CanonicalRow {
            source: Box::new(source),
        }
    }
}

impl AsOfJoinOperation {
    fn validate_input(&self, input: OperationInput<'_>) -> Result<(), AsOfJoinError> {
        if input.port >= self.input_schemas.len() {
            return Err(AsOfJoinError::InvalidInputPort { port: input.port });
        }
        if input.change.schema().as_ref() != self.input_schemas[input.port].as_ref() {
            return Err(AsOfJoinError::InputSchemaMismatch { port: input.port });
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "preparation evaluates one key expression at a time and bounds every retained encoding"
    )]
    fn prepare_claim(&self, input: OperationInput<'_>) -> Result<PreparedClaim, OperationError> {
        let records = input.change.records();
        let mut budget = PreparationBudget::new(input.change.num_rows())?;
        let mut preparing = Vec::with_capacity(input.change.num_rows());
        for index in 0..input.change.num_rows() {
            let row = canonical_row_bounded(records, index, budget.remaining())
                .map_err(map_preparation_row_error)?;
            budget.charge(row.len())?;
            budget.charge(1)?;
            preparing.push(PreparingRow {
                row,
                partition: Vec::new(),
                order: vec![0],
                rank: Vec::new(),
                matchable: true,
                order_matchable: true,
                difference: input.change.diffs().value(index),
            });
        }

        for (expression_index, pair) in self.equalities.iter().enumerate() {
            let scalar = pair.for_port(input.port);
            let column = scalar.expression.evaluate(records).map_err(|source| {
                AsOfJoinError::Expression {
                    role: "equality",
                    index: expression_index,
                    port: input.port,
                    source,
                }
            })?;
            for (row_index, row) in preparing.iter_mut().enumerate() {
                let scalar = pair.for_port(input.port);
                if pair.mode == AsOfEqualityMode::Equal && column.is_null(row_index) {
                    row.matchable = false;
                }
                let before = row.partition.len();
                encode_canonical_bounded(
                    &scalar.field,
                    column.as_ref(),
                    row_index,
                    "ASOF equality",
                    &mut row.partition,
                    budget.output_limit(before)?,
                )
                .map_err(map_preparation_codec_error)?;
                budget.charge(row.partition.len().saturating_sub(before))?;
            }
        }

        for (expression_index, pair) in self.orders.iter().enumerate() {
            let scalar = pair.for_port(input.port);
            let column = scalar.expression.evaluate(records).map_err(|source| {
                AsOfJoinError::Expression {
                    role: "order",
                    index: expression_index,
                    port: input.port,
                    source,
                }
            })?;
            for (row_index, row) in preparing.iter_mut().enumerate() {
                let scalar = pair.for_port(input.port);
                let value = ScalarValue::try_from_array(column.as_ref(), row_index)?;
                if let Some(component) = order_key(&scalar.field, &value)
                    .map_err(|_| AsOfJoinError::InvalidIndex("bound order value is invalid"))?
                {
                    budget.push_component(&mut row.order, &component)?;
                } else {
                    row.order_matchable = false;
                    budget.push_component(&mut row.order, &[])?;
                }
            }
        }
        for row in &mut preparing {
            row.matchable &= row.order_matchable;
            row.order[0] = u8::from(row.order_matchable);
        }

        if input.port == 1 {
            for (expression_index, tie) in self.ties.iter().enumerate() {
                let column = tie.value.expression.evaluate(records).map_err(|source| {
                    AsOfJoinError::Expression {
                        role: "tie break",
                        index: expression_index,
                        port: input.port,
                        source,
                    }
                })?;
                for (row_index, row) in preparing.iter_mut().enumerate() {
                    let value = ScalarValue::try_from_array(column.as_ref(), row_index)?;
                    let encoded = order_key(&tie.value.field, &value).map_err(|_| {
                        AsOfJoinError::InvalidIndex("bound tie-break value is invalid")
                    })?;
                    budget.push_nullable_component(
                        &mut row.rank,
                        encoded.as_deref(),
                        tie.descending,
                        tie.nulls_first,
                    )?;
                }
            }
        }

        let mut rows = Vec::with_capacity(preparing.len());
        for row in preparing {
            let key = budget.row_key(&row.partition, &row.order, &row.rank, &row.row)?;
            rows.push(PreparedRow {
                key,
                partition: row.partition,
                order: row.order,
                matchable: row.matchable,
                difference: row.difference,
            });
        }
        Ok(PreparedClaim {
            port: input.port,
            rows,
            effects: None,
            overlay: CachedEventOverlay::default(),
        })
    }

    fn initial_continuation(port: usize) -> AsOfContinuation {
        AsOfContinuation {
            port: u8::try_from(port).expect("the two ASOF ports fit in a byte"),
            phase: Phase::Probe,
            row: 0,
            left_resume_after: None,
            candidate_resume_after: None,
            best_before: None,
            best_after: None,
            ambiguous_before: false,
            ambiguous_after: false,
        }
    }

    fn apply_claim(
        &self,
        claim: &mut PreparedClaim,
        access: TransactionAccess<'_>,
    ) -> Result<Action, AsOfJoinError> {
        let mut continuation = self.continuation.access(access)?;
        let mut state = if let Some(state) = continuation.get()? {
            Self::validate_continuation(claim, &state)?;
            state
        } else {
            Self::initial_continuation(claim.port)
        };
        if claim.effects.is_none() {
            let start = if state.phase == Phase::Probe {
                0
            } else {
                usize::try_from(state.row)
                    .map_err(|_| AsOfJoinError::InvalidContinuation("row exceeds usize"))?
            };
            let current_applied =
                claim.port == 1 && state.phase == Phase::Emit && state.left_resume_after.is_some();
            claim.effects =
                Some(self.preflight_admission(claim, start, current_applied, access)?);
        }

        let mut budget = TurnBudget::new();
        let mut output = OutputRows::new(self.output_schema.fields().len());
        loop {
            let step = if claim.port == 0 {
                self.process_left_claim_row(
                    claim,
                    &mut state,
                    &mut continuation,
                    &mut budget,
                    &mut output,
                    access,
                )?
            } else {
                self.process_right_claim_row(
                    claim,
                    &mut state,
                    &mut continuation,
                    &mut budget,
                    &mut output,
                    access,
                )?
            };
            match step {
                Step::Complete => {
                    return Ok(Action::Complete(output.finish(&self.output_schema)?));
                }
                Step::Yield => {
                    continuation.set(&state)?;
                    return Ok(match state.phase {
                        Phase::Probe => Action::Commit(None),
                        Phase::Emit => Action::Commit(output.finish(&self.output_schema)?),
                    });
                }
                Step::Continue if budget.exhausted() => {
                    continuation.set(&state)?;
                    return Ok(match state.phase {
                        Phase::Probe => Action::Commit(None),
                        Phase::Emit => Action::Commit(output.finish(&self.output_schema)?),
                    });
                }
                Step::Continue => {}
            }
        }
    }

    fn preflight_admission(
        &self,
        claim: &PreparedClaim,
        start: usize,
        current_applied: bool,
        access: TransactionAccess<'_>,
    ) -> Result<Vec<RowEffect>, AsOfJoinError> {
        let rows = self.rows(claim.port).access(access)?;
        let mut overlay = BTreeMap::<&[u8], u64>::new();
        let mut effects = vec![RowEffect::default(); claim.rows.len()];
        for (index, row) in claim.rows.iter().enumerate().skip(start) {
            let effect = if index == start && current_applied {
                let after = rows.get(&row.key)?.map_or(0, RowWeight::get);
                let before = reverse_weight(after, row.difference)?;
                RowEffect { before, after }
            } else {
                let before = match overlay.get(row.key.as_slice()) {
                    Some(weight) => *weight,
                    None => rows.get(&row.key)?.map_or(0, RowWeight::get),
                };
                let after = adjusted_weight(before, row.difference)?;
                RowEffect { before, after }
            };
            overlay.insert(&row.key, effect.after);
            effects[index] = effect;
        }
        Ok(effects)
    }

    fn process_left_claim_row(
        &self,
        claim: &PreparedClaim,
        state: &mut AsOfContinuation,
        continuation: &mut CellAccess<'_, AsOfContinuation>,
        budget: &mut TurnBudget,
        output: &mut OutputRows,
        access: TransactionAccess<'_>,
    ) -> Result<Step, AsOfJoinError> {
        debug_assert_eq!(claim.port, 0);
        let row_index = continuation_row(claim, state)?;
        let row = &claim.rows[row_index];
        if !budget.can_start(row) {
            return Ok(Step::Yield);
        }
        let page = if row.matchable {
            let empty = VisibilityOverlay::new();
            let Some(page) = self.selection_page(row, &empty, false, state, budget, access)? else {
                return Ok(Step::Yield);
            };
            page
        } else {
            SelectionPage {
                best_before: None,
                best_after: None,
                ambiguous_before: false,
                ambiguous_after: false,
                continuation: None,
                work: (0, 0),
            }
        };
        let result_work = if page.continuation.is_none() {
            left_result_work(self.kind, row, page.best_after.as_ref())
        } else {
            (0, 0)
        };
        let work = add_work(page.work, result_work);
        if !budget.can_accept(work) {
            return Ok(Step::Yield);
        }
        budget.charge(work);
        Self::store_selection_page(state, &page, false);
        if page.continuation.is_some() {
            return Ok(Step::Yield);
        }
        let winner = finish_selection(state.best_after.as_deref(), state.ambiguous_after)?;
        state.ambiguous_after = false;
        let effect = claim.effects.as_ref().expect("the ASOF Claim was admitted")[row_index];
        match state.phase {
            Phase::Probe => {
                let mut validation = OutputRows::new(self.output_schema.fields().len());
                self.append_left_result(row, winner.as_ref(), &mut validation)?;
                validation.finish(&self.output_schema)?;
            }
            Phase::Emit => {
                self.append_left_result(row, winner.as_ref(), output)?;
                self.adjust_actual(0, row, effect, access)?;
            }
        }
        clear_candidate_state(state);
        Self::advance_claim_row(claim, state, continuation)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one row coordinates the durable outer cursor, inner selection cursor, and atomic correction"
    )]
    fn process_right_claim_row(
        &self,
        claim: &mut PreparedClaim,
        state: &mut AsOfContinuation,
        continuation: &mut CellAccess<'_, AsOfContinuation>,
        budget: &mut TurnBudget,
        output: &mut OutputRows,
        access: TransactionAccess<'_>,
    ) -> Result<Step, AsOfJoinError> {
        debug_assert_eq!(claim.port, 1);
        let row_index = continuation_row(claim, state)?;
        let right = &claim.rows[row_index];
        let effect = claim.effects.as_ref().expect("the ASOF Claim was admitted")[row_index];
        if !budget.can_start(right) {
            return Ok(Step::Yield);
        }
        if !right.matchable || !effect.changes_presence() {
            if !selection_state_is_empty(state) || state.left_resume_after.is_some() {
                return Err(AsOfJoinError::InvalidContinuation(
                    "right row without rematch work retains scan state",
                ));
            }
            if state.phase == Phase::Emit {
                self.adjust_actual(1, right, effect, access)?;
            }
            budget.charge(TurnBudget::stored_row_work(right));
            return Self::advance_claim_row(claim, state, continuation);
        }

        let applied = state.phase == Phase::Emit && state.left_resume_after.is_some();
        let first_left = state.left_resume_after.is_none();
        let (left_key, left_weight, left) =
            match self.current_or_next_left(right, state, budget, access)? {
                NextLeft::Row(key, weight, row) => (key, weight, row),
                NextLeft::Yield => return Ok(Step::Yield),
                NextLeft::Exhausted => {
                    if first_left {
                        budget.charge(TurnBudget::stored_row_work(right));
                    }
                    if state.phase == Phase::Emit && !applied {
                        self.adjust_actual(1, right, effect, access)?;
                    }
                    clear_outer_state(state);
                    return Self::advance_claim_row(claim, state, continuation);
                }
            };

        // Building the batch-prefix overlay is proportional to the number of prior
        // right events. Defer it until there is a matchable left row to select for:
        // empty partitions and NULL-order left rows must not pay that cost for every
        // event in a large right-hand Claim.
        let overlay = event_overlay(
            &claim.rows,
            claim
                .effects
                .as_ref()
                .expect("the ASOF Claim was admitted before building its overlay"),
            &mut claim.overlay,
            row_index,
            state.phase,
        );
        let page = self.selection_page(&left, overlay, true, state, budget, access)?;
        let Some(page) = page else {
            return Ok(Step::Yield);
        };
        let correction_work = if page.continuation.is_none() {
            correction_work(
                self.kind,
                &left,
                left_weight,
                page.best_before.as_ref(),
                page.best_after.as_ref(),
            )
        } else {
            (0, 0)
        };
        let work = add_work(
            add_work(page.work, correction_work),
            if first_left {
                TurnBudget::stored_row_work(right)
            } else {
                (0, 0)
            },
        );
        if !budget.can_accept(work) {
            return Ok(Step::Yield);
        }
        if state.phase == Phase::Emit && !applied {
            self.adjust_actual(1, right, effect, access)?;
        } else if state.phase == Phase::Emit {
            self.validate_applied(right, effect, access)?;
        }
        budget.charge(work);
        state.left_resume_after = Some(left_key);
        Self::store_selection_page(state, &page, true);
        if page.continuation.is_some() {
            return Ok(Step::Yield);
        }

        let before = finish_selection(state.best_before.as_deref(), state.ambiguous_before)?;
        let after = finish_selection(state.best_after.as_deref(), state.ambiguous_after)?;
        state.ambiguous_before = false;
        state.ambiguous_after = false;
        let correction = Correction {
            left_row: prepared_row_bytes(&left)?,
            left_weight,
            before,
            after,
        };
        match state.phase {
            Phase::Probe => {
                let mut validation = OutputRows::new(self.output_schema.fields().len());
                self.append_correction(&correction, &mut validation)?;
                validation.finish(&self.output_schema)?;
            }
            Phase::Emit => self.append_correction(&correction, output)?,
        }
        clear_candidate_state(state);
        Ok(Step::Continue)
    }

    fn current_or_next_left(
        &self,
        right: &PreparedRow,
        state: &AsOfContinuation,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<NextLeft, AsOfJoinError> {
        let rows = self.left_rows.access(access)?;
        if state.candidate_resume_after.is_some() {
            let key =
                state
                    .left_resume_after
                    .as_ref()
                    .ok_or(AsOfJoinError::InvalidContinuation(
                        "candidate cursor has no current left row",
                    ))?;
            let weight = rows.get(key)?.ok_or(AsOfJoinError::InvalidIndex(
                "current left row disappeared during right Claim",
            ))?;
            let parsed = parse_index_key(key)?;
            validate_partition(&parsed, &right.partition)?;
            let prepared = prepared_index_row(key.clone(), parsed, 0)?;
            if !prepared.matchable {
                return Err(AsOfJoinError::InvalidContinuation(
                    "candidate cursor identifies a NULL-order left row",
                ));
            }
            return Ok(NextLeft::Row(key.clone(), weight.get(), prepared));
        }

        // NULL-order left rows can never select any right candidate. Their order
        // tuple starts with marker `0`; constrain the durable outer scan to marker
        // `1` so a right presence transition does not rescan irrelevant history in
        // both Probe and Emit.
        let range = prefix_range(matchable_partition_prefix(&right.partition));
        let limit = ScanLimit::new(1, budget.remaining_bytes().max(1))
            .expect("one ASOF left row and positive bytes form a valid limit");
        let page = match rows.scan(
            range.bounds(),
            ScanDirection::Ascending,
            state.left_resume_after.as_ref(),
            limit,
        ) {
            Ok(page) => page,
            Err(StoreError::ItemTooLarge { .. }) if !budget.is_empty() => {
                return Ok(NextLeft::Yield);
            }
            Err(StoreError::ItemTooLarge { size, .. }) => {
                let limit = ScanLimit::new(1, size.max(1))
                    .expect("one item and positive observed bytes form a valid limit");
                rows.scan(
                    range.bounds(),
                    ScanDirection::Ascending,
                    state.left_resume_after.as_ref(),
                    limit,
                )?
            }
            Err(source) => return Err(source.into()),
        };
        let Some((key, weight)) = page.entries.into_iter().next() else {
            return Ok(NextLeft::Exhausted);
        };
        let parsed = parse_index_key(&key)?;
        validate_partition(&parsed, &right.partition)?;
        let prepared = prepared_index_row(key.clone(), parsed, 0)?;
        if !prepared.matchable {
            return Err(AsOfJoinError::InvalidIndex(
                "matchable left range contains a NULL-order row",
            ));
        }
        Ok(NextLeft::Row(key, weight.get(), prepared))
    }

    fn selection_page(
        &self,
        left: &PreparedRow,
        overlay: &VisibilityOverlay,
        track_before: bool,
        state: &AsOfContinuation,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<SelectionPage>, AsOfJoinError> {
        let rows = self.right_rows.access(access)?;
        // NULL-order right rows can never be candidates. Seek directly to the
        // matchable marker so irrelevant right history cannot turn one left
        // lookup (or each right-side rematch of a left row) into a full scan.
        let range = prefix_range(matchable_partition_prefix(&left.partition));
        let max_items = budget
            .remaining_items()
            .min(CANDIDATE_ITEMS)
            .min(candidate_item_limit(
                self.candidate_schema.fields().len(),
                left.key.len(),
            ));
        let max_bytes = budget.remaining_bytes().clamp(1, CANDIDATE_BYTES);
        let Some(page) = merged_page(
            &rows,
            overlay,
            &range,
            state.candidate_resume_after.as_ref(),
            max_items,
            max_bytes,
            budget.is_empty(),
        )?
        else {
            return Ok(None);
        };

        let mut eligible = Vec::with_capacity(page.entries.len());
        for (index, candidate) in page.entries.iter().enumerate() {
            if !candidate.visible_before && !candidate.visible_after {
                continue;
            }
            let winner = winner_from_key(&candidate.key)?;
            if self.candidate_is_eligible(left, &winner)? {
                eligible.push(index);
            }
        }
        let eligible_count = eligible.len();
        let qualifying = self.evaluate_residual(left, &page.entries, &eligible)?;
        let mut best_before = state
            .best_before
            .as_deref()
            .map(winner_from_key)
            .transpose()?;
        let mut best_after = state
            .best_after
            .as_deref()
            .map(winner_from_key)
            .transpose()?;
        let mut ambiguous_before = state.ambiguous_before;
        let mut ambiguous_after = state.ambiguous_after;
        for (index, qualifies) in eligible.into_iter().zip(qualifying) {
            if !qualifies {
                continue;
            }
            let candidate = &page.entries[index];
            let winner = winner_from_key(&candidate.key)?;
            if track_before && candidate.visible_before {
                self.consider(left, &winner, &mut best_before, &mut ambiguous_before)?;
            }
            if candidate.visible_after {
                self.consider(left, &winner, &mut best_after, &mut ambiguous_after)?;
            }
        }
        let scalar_slots = self
            .candidate_schema
            .fields()
            .len()
            .saturating_mul(eligible_count)
            .saturating_mul(size_of::<ScalarValue>());
        let repeated_left = if self.residual.is_some() {
            left.key.len().saturating_mul(eligible_count)
        } else {
            0
        };
        Ok(Some(SelectionPage {
            best_before,
            best_after,
            ambiguous_before,
            ambiguous_after,
            continuation: page.continuation,
            work: (
                page.work.0,
                page.work
                    .1
                    .saturating_add(scalar_slots)
                    .saturating_add(repeated_left),
            ),
        }))
    }

    fn evaluate_residual(
        &self,
        left: &PreparedRow,
        candidates: &[MergedCandidate],
        eligible: &[usize],
    ) -> Result<Vec<bool>, AsOfJoinError> {
        if self.residual.is_none() {
            return Ok(vec![true; eligible.len()]);
        }
        if eligible.is_empty() {
            return Ok(Vec::new());
        }
        let left_values = decode_row(&self.input_schemas[0], &prepared_row_bytes(left)?)?;
        let left_fields = left_values.len();
        let mut columns = (0..self.candidate_schema.fields().len())
            .map(|_| Vec::with_capacity(eligible.len()))
            .collect::<Vec<Vec<ScalarValue>>>();
        for index in eligible {
            let candidate = &candidates[*index];
            let winner = winner_from_key(&candidate.key)?;
            let right_values = decode_row(&self.input_schemas[1], &winner.row)?;
            for (column, value) in columns[..left_fields].iter_mut().zip(&left_values) {
                column.push(value.clone());
            }
            for (column, value) in columns[left_fields..].iter_mut().zip(right_values) {
                column.push(value);
            }
        }
        let arrays = columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(eligible.len()));
        let records = RecordBatch::try_new_with_options(
            Arc::clone(&self.candidate_schema),
            arrays,
            &options,
        )?;
        let predicate = self
            .residual
            .as_ref()
            .expect("the residual path has a bound expression")
            .evaluate(&records)
            .map_err(|source| AsOfJoinError::ResidualExpression { source })?;
        let predicate = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(AsOfJoinError::ResidualArray)?;
        if predicate.len() != eligible.len() {
            return Err(AsOfJoinError::ResidualArray);
        }
        Ok((0..predicate.len())
            .map(|index| predicate.is_valid(index) && predicate.value(index))
            .collect())
    }

    fn consider(
        &self,
        left: &PreparedRow,
        candidate: &Winner,
        best: &mut Option<Winner>,
        ambiguous: &mut bool,
    ) -> Result<(), AsOfJoinError> {
        debug_assert!(self.candidate_is_eligible(left, candidate)?);
        let Some(current) = best.as_ref() else {
            *best = Some(candidate.clone());
            *ambiguous = false;
            return Ok(());
        };
        if current.key == candidate.key {
            return Ok(());
        }
        match self.compare_quality(left, candidate, current)? {
            Ordering::Less => {
                *best = Some(candidate.clone());
                *ambiguous = false;
            }
            Ordering::Greater => {}
            Ordering::Equal => {
                debug_assert_eq!(self.tie_fallback, AsOfTieFallback::Reject);
                *ambiguous = true;
            }
        }
        Ok(())
    }

    fn candidate_is_eligible(
        &self,
        left: &PreparedRow,
        candidate: &Winner,
    ) -> Result<bool, AsOfJoinError> {
        if !left.matchable {
            return Ok(false);
        }
        if !matchable_order(&candidate.order)? {
            return Ok(false);
        }
        let comparison = candidate.order.cmp(&left.order);
        let eligible = match self.direction {
            AsOfDirection::Backward { allow_exact } => {
                comparison == Ordering::Less || allow_exact && comparison == Ordering::Equal
            }
            AsOfDirection::Forward { allow_exact } => {
                comparison == Ordering::Greater || allow_exact && comparison == Ordering::Equal
            }
            AsOfDirection::Nearest { allow_exact, .. } => {
                allow_exact || comparison != Ordering::Equal
            }
        };
        if !eligible {
            return Ok(false);
        }
        if let Some(tolerance) = self.tolerance
            && self.distance(&left.order, &candidate.order)? > tolerance
        {
            return Ok(false);
        }
        Ok(true)
    }

    /// Compares candidate quality; `Less` means `candidate` is preferred.
    fn compare_quality(
        &self,
        left: &PreparedRow,
        candidate: &Winner,
        current: &Winner,
    ) -> Result<Ordering, AsOfJoinError> {
        let order = if candidate.order == current.order {
            Ordering::Equal
        } else {
            match self.direction {
                AsOfDirection::Backward { .. } => current.order.cmp(&candidate.order),
                AsOfDirection::Forward { .. } => candidate.order.cmp(&current.order),
                AsOfDirection::Nearest { equidistant, .. } => {
                    let candidate_distance = self.distance(&left.order, &candidate.order)?;
                    let current_distance = self.distance(&left.order, &current.order)?;
                    match candidate_distance.cmp(&current_distance) {
                        Ordering::Equal => match equidistant {
                            AsOfEquidistantPreference::Backward => {
                                candidate.order.cmp(&current.order)
                            }
                            AsOfEquidistantPreference::Forward => {
                                current.order.cmp(&candidate.order)
                            }
                        },
                        ordering => ordering,
                    }
                }
            }
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
        let rank = candidate.rank.cmp(&current.rank);
        if rank != Ordering::Equal {
            return Ok(rank);
        }
        Ok(match self.tie_fallback {
            AsOfTieFallback::Reject => Ordering::Equal,
            AsOfTieFallback::CanonicalAscending => candidate.row.cmp(&current.row),
            AsOfTieFallback::CanonicalDescending => current.row.cmp(&candidate.row),
        })
    }

    fn distance(&self, left: &[u8], right: &[u8]) -> Result<u128, AsOfJoinError> {
        let field = &self.orders[0].left.field;
        match (decode_metric(field, left)?, decode_metric(field, right)?) {
            (Metric::Signed(left), Metric::Signed(right)) => Ok(left.abs_diff(right)),
            (Metric::Unsigned(left), Metric::Unsigned(right)) => Ok(left.abs_diff(right)),
            _ => Err(AsOfJoinError::InvalidIndex(
                "distance operands have different numeric domains",
            )),
        }
    }

    fn store_selection_page(
        state: &mut AsOfContinuation,
        page: &SelectionPage,
        track_before: bool,
    ) {
        state.candidate_resume_after.clone_from(&page.continuation);
        if track_before {
            clone_winner_key(&mut state.best_before, page.best_before.as_ref());
            state.ambiguous_before = page.ambiguous_before;
        }
        clone_winner_key(&mut state.best_after, page.best_after.as_ref());
        state.ambiguous_after = page.ambiguous_after;
    }

    fn append_left_result(
        &self,
        row: &PreparedRow,
        winner: Option<&Winner>,
        output: &mut OutputRows,
    ) -> Result<(), AsOfJoinError> {
        let left = decode_row(&self.input_schemas[0], &prepared_row_bytes(row)?)?;
        match self.kind {
            AsOfJoinKind::Inner => {
                if let Some(winner) = winner {
                    let right = decode_row(&self.input_schemas[1], &winner.row)?;
                    output.push(&left, &right, row.difference);
                }
            }
            AsOfJoinKind::LeftOuter => {
                if let Some(winner) = winner {
                    let right = decode_row(&self.input_schemas[1], &winner.row)?;
                    output.push(&left, &right, row.difference);
                } else {
                    output.push(&left, &self.right_nulls, row.difference);
                }
            }
            AsOfJoinKind::LeftSemi => {
                if winner.is_some() {
                    output.push(&left, &[], row.difference);
                }
            }
            AsOfJoinKind::LeftAnti => {
                if winner.is_none() {
                    output.push(&left, &[], row.difference);
                }
            }
        }
        Ok(())
    }

    fn append_correction(
        &self,
        correction: &Correction,
        output: &mut OutputRows,
    ) -> Result<(), AsOfJoinError> {
        if winner_key(correction.before.as_ref()) == winner_key(correction.after.as_ref()) {
            return Ok(());
        }
        let before_matches = correction.before.is_some();
        let after_matches = correction.after.is_some();
        if self.kind.left_only() {
            let (before_emits, after_emits) = match self.kind {
                AsOfJoinKind::LeftSemi => (before_matches, after_matches),
                AsOfJoinKind::LeftAnti => (!before_matches, !after_matches),
                _ => unreachable!("left-only ASOF kind was checked"),
            };
            if before_emits != after_emits {
                let left = decode_row(&self.input_schemas[0], &correction.left_row)?;
                let difference = if after_emits {
                    positive_difference(correction.left_weight)?
                } else {
                    negative_difference(correction.left_weight)?
                };
                output.push(&left, &[], difference);
            }
            return Ok(());
        }

        let left = decode_row(&self.input_schemas[0], &correction.left_row)?;
        if let Some(before) = &correction.before {
            let right = decode_row(&self.input_schemas[1], &before.row)?;
            output.push(&left, &right, negative_difference(correction.left_weight)?);
        } else if self.kind == AsOfJoinKind::LeftOuter {
            output.push(
                &left,
                &self.right_nulls,
                negative_difference(correction.left_weight)?,
            );
        }
        if let Some(after) = &correction.after {
            let right = decode_row(&self.input_schemas[1], &after.row)?;
            output.push(&left, &right, positive_difference(correction.left_weight)?);
        } else if self.kind == AsOfJoinKind::LeftOuter {
            output.push(
                &left,
                &self.right_nulls,
                positive_difference(correction.left_weight)?,
            );
        }
        Ok(())
    }

    fn adjust_actual(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        access: TransactionAccess<'_>,
    ) -> Result<(), AsOfJoinError> {
        let mut rows = self.rows(port).access(access)?;
        let current = rows.get(&row.key)?.map_or(0, RowWeight::get);
        if current != effect.before {
            return Err(AsOfJoinError::InvalidIndex(
                "actual row weight differs from admitted prefix",
            ));
        }
        match RowWeight::new(effect.after) {
            Some(weight) => rows.put(&row.key, &weight)?,
            None => {
                rows.remove(&row.key)?;
            }
        }
        Ok(())
    }

    fn validate_applied(
        &self,
        row: &PreparedRow,
        effect: RowEffect,
        access: TransactionAccess<'_>,
    ) -> Result<(), AsOfJoinError> {
        let rows = self.right_rows.access(access)?;
        if rows.get(&row.key)?.map_or(0, RowWeight::get) != effect.after {
            return Err(AsOfJoinError::InvalidIndex(
                "paged right event is not at its admitted weight",
            ));
        }
        Ok(())
    }

    fn advance_claim_row(
        claim: &PreparedClaim,
        state: &mut AsOfContinuation,
        continuation: &mut CellAccess<'_, AsOfContinuation>,
    ) -> Result<Step, AsOfJoinError> {
        let next = continuation_row(claim, state)? + 1;
        clear_outer_state(state);
        if next < claim.rows.len() {
            state.row = persistent_row(next)?;
            return Ok(Step::Continue);
        }
        if state.phase == Phase::Probe {
            state.phase = Phase::Emit;
            state.row = 0;
            return Ok(Step::Continue);
        }
        continuation.clear()?;
        Ok(Step::Complete)
    }

    fn validate_continuation(
        claim: &PreparedClaim,
        state: &AsOfContinuation,
    ) -> Result<(), AsOfJoinError> {
        if usize::from(state.port) != claim.port {
            return Err(AsOfJoinError::InvalidContinuation(
                "port differs from the pinned input",
            ));
        }
        let row_index = continuation_row(claim, state)?;
        let row = &claim.rows[row_index];
        if claim.port == 0 && state.left_resume_after.is_some() {
            return Err(AsOfJoinError::InvalidContinuation(
                "left Claim has an outer left-row cursor",
            ));
        }
        if state.candidate_resume_after.is_some()
            && claim.port == 1
            && state.left_resume_after.is_none()
        {
            return Err(AsOfJoinError::InvalidContinuation(
                "right candidate cursor has no current left row",
            ));
        }
        if state.candidate_resume_after.is_none()
            && (state.best_before.is_some()
                || state.best_after.is_some()
                || state.ambiguous_before
                || state.ambiguous_after)
        {
            return Err(AsOfJoinError::InvalidContinuation(
                "completed candidate scan retains selection state",
            ));
        }
        if state.ambiguous_before && state.best_before.is_none()
            || state.ambiguous_after && state.best_after.is_none()
        {
            return Err(AsOfJoinError::InvalidContinuation(
                "ambiguity marker has no selected candidate",
            ));
        }
        for key in [
            state.candidate_resume_after.as_ref(),
            state.best_before.as_ref(),
            state.best_after.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let parsed = parse_index_key(key).map_err(|_| {
                AsOfJoinError::InvalidContinuation("candidate key framing is invalid")
            })?;
            if parsed.partition != row.partition {
                return Err(AsOfJoinError::InvalidContinuation(
                    "candidate key is outside the current equality partition",
                ));
            }
        }
        if let Some(left) = &state.left_resume_after {
            let parsed = parse_index_key(left).map_err(|_| {
                AsOfJoinError::InvalidContinuation("left cursor framing is invalid")
            })?;
            if parsed.partition != row.partition {
                return Err(AsOfJoinError::InvalidContinuation(
                    "left cursor is outside the current equality partition",
                ));
            }
        }
        Ok(())
    }

    fn rows(&self, port: usize) -> &Rows {
        match port {
            0 => &self.left_rows,
            1 => &self.right_rows,
            _ => unreachable!("a prepared ASOF claim has a validated port"),
        }
    }
}

impl TurnOperation for AsOfJoinOperation {
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
            .expect("the prepared ASOF Claim was initialized above");
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

fn adjusted_weight(weight: u64, difference: i64) -> Result<u64, AsOfJoinError> {
    RowWeight::adjusted(RowWeight::new(weight), difference)
        .map(|weight| weight.map_or(0, RowWeight::get))
        .map_err(map_weight_error)
}

fn reverse_weight(weight: u64, difference: i64) -> Result<u64, AsOfJoinError> {
    if difference >= 0 {
        weight
            .checked_sub(difference.unsigned_abs())
            .ok_or(AsOfJoinError::NegativeWeight)
    } else {
        weight
            .checked_add(difference.unsigned_abs())
            .ok_or(AsOfJoinError::WeightOverflow)
    }
}

const fn map_weight_error(error: RowWeightError) -> AsOfJoinError {
    match error {
        RowWeightError::Negative => AsOfJoinError::NegativeWeight,
        RowWeightError::Overflow => AsOfJoinError::WeightOverflow,
    }
}

fn event_overlay<'cache>(
    rows: &[PreparedRow],
    effects: &[RowEffect],
    cache: &'cache mut CachedEventOverlay,
    row_index: usize,
    phase: Phase,
) -> &'cache VisibilityOverlay {
    let rebuild = cache.phase != Some(phase)
        || cache.row.is_none()
        || cache.row.is_some_and(|cached| cached > row_index);
    if rebuild {
        cache.weights.clear();
        if phase == Phase::Probe {
            for (row, effect) in rows[..row_index].iter().zip(&effects[..row_index]) {
                put_overlay(
                    &mut cache.weights,
                    &row.key,
                    RowEffect {
                        before: effect.after,
                        after: effect.after,
                    },
                );
            }
        }
    } else if phase == Phase::Probe {
        let cached = cache.row.expect("a reusable event overlay has a row");
        for index in cached..row_index {
            let effect = effects[index];
            put_overlay(
                &mut cache.weights,
                &rows[index].key,
                RowEffect {
                    before: effect.after,
                    after: effect.after,
                },
            );
        }
    } else if cache.row != Some(row_index) {
        cache.weights.clear();
    }

    put_overlay(&mut cache.weights, &rows[row_index].key, effects[row_index]);
    cache.phase = Some(phase);
    cache.row = Some(row_index);
    &cache.weights
}

fn put_overlay(overlay: &mut VisibilityOverlay, key: &[u8], effect: RowEffect) {
    if let Some(stored) = overlay.get_mut(key) {
        *stored = effect;
    } else {
        overlay.insert(key.to_vec(), effect);
    }
}

fn merged_page(
    rows: &OrderedMapAccess<'_, Vec<u8>, RowWeight>,
    overlay: &VisibilityOverlay,
    range: &KeyRange,
    resume_after: Option<&Vec<u8>>,
    max_items: usize,
    max_bytes: usize,
    allow_oversized: bool,
) -> Result<Option<MergedPage>, AsOfJoinError> {
    debug_assert!(max_items > 0);
    debug_assert!(max_bytes > 0);
    let limit = ScanLimit::new(max_items, max_bytes)
        .expect("positive ASOF candidate page limits are valid");
    let actual = match rows.scan(
        range.bounds(),
        ScanDirection::Ascending,
        resume_after,
        limit,
    ) {
        Ok(page) => page,
        Err(StoreError::ItemTooLarge { .. }) if !allow_oversized => return Ok(None),
        Err(StoreError::ItemTooLarge { size, .. }) => {
            let limit = ScanLimit::new(1, size.max(1))
                .expect("one item and a positive observed byte size are valid");
            rows.scan(
                range.bounds(),
                ScanDirection::Ascending,
                resume_after,
                limit,
            )?
        }
        Err(source) => return Err(source.into()),
    };

    // When the durable page has more entries, unseen actual keys can precede
    // an overlay key after the page frontier. Defer every such overlay key so
    // the conceptual merged scan remains globally ordered.
    let frontier = actual
        .continuation
        .as_ref()
        .and_then(|_| actual.entries.last().map(|(key, _)| key));
    let mut keys = BTreeSet::<&[u8]>::new();
    for (key, _) in &actual.entries {
        keys.insert(key);
    }
    let overlay_truncated =
        collect_overlay_keys(overlay, range, resume_after, frontier, max_items, &mut keys);

    let mut entries = Vec::new();
    let mut examined = 0_usize;
    let mut bytes = 0_usize;
    let mut last_examined = None;
    let mut stopped = false;
    for key in keys {
        let item_bytes = key.len().saturating_add(MAP_VALUE_BYTES).max(1);
        let exceeds = examined >= max_items || bytes.saturating_add(item_bytes) > max_bytes;
        if exceeds && (examined != 0 || !allow_oversized) {
            stopped = true;
            break;
        }
        examined = examined.saturating_add(1);
        bytes = bytes.saturating_add(item_bytes);
        last_examined = Some(key.to_vec());
        let actual_present = actual
            .entries
            .binary_search_by(|(actual_key, _)| actual_key.as_slice().cmp(key))
            .is_ok();
        let (visible_before, visible_after) = overlay
            .get(key)
            .map_or((actual_present, actual_present), |effect| {
                (effect.before != 0, effect.after != 0)
            });
        if visible_before || visible_after {
            entries.push(MergedCandidate {
                key: key.to_vec(),
                visible_before,
                visible_after,
            });
        }
    }
    if stopped && last_examined.is_none() {
        return Ok(None);
    }
    let has_more = stopped || actual.continuation.is_some() || overlay_truncated;
    let continuation = if has_more {
        last_examined
            .ok_or(AsOfJoinError::InvalidIndex(
                "merged candidate page made no progress",
            ))?
            .into()
    } else {
        None
    };
    Ok(Some(MergedPage {
        entries,
        continuation,
        work: (examined.max(1), bytes.max(1)),
    }))
}

fn collect_overlay_keys<'key>(
    overlay: &'key VisibilityOverlay,
    range: &KeyRange,
    resume_after: Option<&Vec<u8>>,
    frontier: Option<&Vec<u8>>,
    max_items: usize,
    keys: &mut BTreeSet<&'key [u8]>,
) -> bool {
    let lower = match resume_after {
        Some(key) => Bound::Excluded(key.as_slice()),
        None => range.start_bytes(),
    };
    let upper = match frontier {
        Some(key) => Bound::Included(key.as_slice()),
        None => range.end_bytes(),
    };
    let mut seen = 0_usize;
    for (key, _) in overlay.range::<[u8], _>((lower, upper)) {
        if seen == max_items {
            return true;
        }
        keys.insert(key);
        seen = seen.saturating_add(1);
    }
    false
}

fn candidate_item_limit(field_count: usize, left_row_bytes: usize) -> usize {
    let scalar_slots = field_count.max(1);
    let by_scalar_count = CANDIDATE_SCALAR_VALUES / scalar_slots;
    let per_candidate = left_row_bytes
        .saturating_add(scalar_slots.saturating_mul(size_of::<ScalarValue>()))
        .max(1);
    let by_materialized_bytes = CANDIDATE_BYTES / per_candidate;
    by_scalar_count.clamp(1, by_materialized_bytes.max(1))
}

fn winner_from_key(key: &[u8]) -> Result<Winner, AsOfJoinError> {
    let parsed = parse_index_key(key)?;
    Ok(Winner {
        key: key.to_vec(),
        order: parsed.order,
        rank: parsed.rank,
        row: parsed.row,
    })
}

fn finish_selection(
    winner: Option<&[u8]>,
    ambiguous: bool,
) -> Result<Option<Winner>, AsOfJoinError> {
    if ambiguous {
        Err(AsOfJoinError::AmbiguousTie)
    } else {
        winner.map(winner_from_key).transpose()
    }
}

fn parse_index_key(key: &[u8]) -> Result<ParsedIndexKey, AsOfJoinError> {
    parse_row_key(key).map_err(|_| AsOfJoinError::InvalidIndex("row key framing is invalid"))
}

fn prepared_index_row(
    key: Vec<u8>,
    parsed: ParsedIndexKey,
    difference: i64,
) -> Result<PreparedRow, AsOfJoinError> {
    let matchable = matchable_order(&parsed.order)?;
    Ok(PreparedRow {
        key,
        partition: parsed.partition,
        order: parsed.order,
        matchable,
        difference,
    })
}

fn validate_partition(parsed: &ParsedIndexKey, expected: &[u8]) -> Result<(), AsOfJoinError> {
    if parsed.partition == expected {
        Ok(())
    } else {
        Err(AsOfJoinError::InvalidIndex(
            "scanned row is outside its equality partition",
        ))
    }
}

fn matchable_order(order: &[u8]) -> Result<bool, AsOfJoinError> {
    let (&marker, mut remaining) = order
        .split_first()
        .ok_or(AsOfJoinError::InvalidIndex("order tuple is empty"))?;
    let mut components = 0_usize;
    while !remaining.is_empty() {
        take_component(&mut remaining)
            .map_err(|_| AsOfJoinError::InvalidIndex("order component framing is invalid"))?;
        components = components.saturating_add(1);
    }
    if components == 0 {
        return Err(AsOfJoinError::InvalidIndex(
            "order tuple has no scalar components",
        ));
    }
    match marker {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(AsOfJoinError::InvalidIndex(
            "order matchability marker is invalid",
        )),
    }
}

fn decode_metric(field: &Field, order: &[u8]) -> Result<Metric, AsOfJoinError> {
    if !matchable_order(order)? {
        return Err(AsOfJoinError::InvalidIndex(
            "NULL order value has no distance",
        ));
    }
    let mut remaining = &order[1..];
    let component = take_component(&mut remaining)
        .map_err(|_| AsOfJoinError::InvalidIndex("distance component framing is invalid"))?;
    if !remaining.is_empty() {
        return Err(AsOfJoinError::InvalidIndex(
            "distance order contains multiple components",
        ));
    }
    let value = ordered_value(field, &component)
        .map_err(|_| AsOfJoinError::InvalidIndex("distance scalar encoding is invalid"))?;
    match value {
        ScalarValue::Int8(Some(value)) => Ok(Metric::Signed(i128::from(value))),
        ScalarValue::Int16(Some(value)) => Ok(Metric::Signed(i128::from(value))),
        ScalarValue::Int32(Some(value)) | ScalarValue::Date32(Some(value)) => {
            Ok(Metric::Signed(i128::from(value)))
        }
        ScalarValue::Int64(Some(value))
        | ScalarValue::TimestampSecond(Some(value), _)
        | ScalarValue::TimestampMillisecond(Some(value), _)
        | ScalarValue::TimestampMicrosecond(Some(value), _)
        | ScalarValue::TimestampNanosecond(Some(value), _) => Ok(Metric::Signed(i128::from(value))),
        ScalarValue::Decimal128(Some(value), _, _) => Ok(Metric::Signed(value)),
        ScalarValue::UInt8(Some(value)) => Ok(Metric::Unsigned(u128::from(value))),
        ScalarValue::UInt16(Some(value)) => Ok(Metric::Unsigned(u128::from(value))),
        ScalarValue::UInt32(Some(value)) => Ok(Metric::Unsigned(u128::from(value))),
        ScalarValue::UInt64(Some(value)) => Ok(Metric::Unsigned(u128::from(value))),
        _ => Err(AsOfJoinError::InvalidIndex(
            "distance scalar has an unsupported type",
        )),
    }
}

fn prefix_range(prefix: Vec<u8>) -> KeyRange {
    let end = prefix_successor(&prefix).map_or(Bound::Unbounded, Bound::Excluded);
    KeyRange {
        start: Bound::Included(prefix),
        end,
    }
}

impl KeyRange {
    fn bounds(&self) -> (Bound<&Vec<u8>>, Bound<&Vec<u8>>) {
        (self.start.as_ref(), self.end.as_ref())
    }

    fn start_bytes(&self) -> Bound<&[u8]> {
        match &self.start {
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bytes(&self) -> Bound<&[u8]> {
        match &self.end {
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

fn clear_candidate_state(state: &mut AsOfContinuation) {
    state.candidate_resume_after = None;
    state.best_before = None;
    state.best_after = None;
    state.ambiguous_before = false;
    state.ambiguous_after = false;
}

fn clear_outer_state(state: &mut AsOfContinuation) {
    state.left_resume_after = None;
    clear_candidate_state(state);
}

fn selection_state_is_empty(state: &AsOfContinuation) -> bool {
    state.candidate_resume_after.is_none()
        && state.best_before.is_none()
        && state.best_after.is_none()
        && !state.ambiguous_before
        && !state.ambiguous_after
}

fn continuation_row(
    claim: &PreparedClaim,
    state: &AsOfContinuation,
) -> Result<usize, AsOfJoinError> {
    usize::try_from(state.row)
        .ok()
        .filter(|row| *row < claim.rows.len())
        .ok_or(AsOfJoinError::InvalidContinuation(
            "row is outside the pinned input",
        ))
}

fn persistent_row(row: usize) -> Result<u64, AsOfJoinError> {
    u64::try_from(row).map_err(|_| AsOfJoinError::InvalidContinuation("input row exceeds u64"))
}

fn decode_row(schema: &SchemaRef, row: &[u8]) -> Result<Vec<ScalarValue>, AsOfJoinError> {
    decode_canonical_row(schema.as_ref(), row).map_err(|source| AsOfJoinError::CanonicalRow {
        source: Box::new(source),
    })
}

fn prepared_row_bytes(row: &PreparedRow) -> Result<Vec<u8>, AsOfJoinError> {
    Ok(parse_index_key(&row.key)?.row)
}

fn winner_key(winner: Option<&Winner>) -> Option<&[u8]> {
    winner.map(|winner| winner.key.as_slice())
}

fn clone_winner_key(target: &mut Option<Vec<u8>>, source: Option<&Winner>) {
    match (target.as_mut(), source) {
        (Some(target), Some(source)) => target.clone_from(&source.key),
        (_, Some(source)) => *target = Some(source.key.clone()),
        (_, None) => *target = None,
    }
}

fn positive_difference(weight: u64) -> Result<i64, AsOfJoinError> {
    i64::try_from(weight).map_err(|_| AsOfJoinError::OutputDifferenceOverflow)
}

fn negative_difference(weight: u64) -> Result<i64, AsOfJoinError> {
    i64::try_from(-i128::from(weight)).map_err(|_| AsOfJoinError::OutputDifferenceOverflow)
}

fn add_work(left: (usize, usize), right: (usize, usize)) -> (usize, usize) {
    (
        left.0.saturating_add(right.0),
        left.1.saturating_add(right.1),
    )
}

fn left_result_work(
    kind: AsOfJoinKind,
    left: &PreparedRow,
    winner: Option<&Winner>,
) -> (usize, usize) {
    let emits = match kind {
        AsOfJoinKind::Inner | AsOfJoinKind::LeftSemi => winner.is_some(),
        AsOfJoinKind::LeftOuter => true,
        AsOfJoinKind::LeftAnti => winner.is_none(),
    };
    let mut bytes = TurnBudget::stored_row_work(left).1;
    if emits {
        bytes = bytes
            .saturating_add(decoded_row_work(&left.key))
            .saturating_add(winner.map_or(0, |winner| decoded_row_work(&winner.row)));
    }
    (1, bytes.max(1))
}

fn correction_work(
    kind: AsOfJoinKind,
    left: &PreparedRow,
    _left_weight: u64,
    before: Option<&Winner>,
    after: Option<&Winner>,
) -> (usize, usize) {
    if before.map(|winner| winner.key.as_slice()) == after.map(|winner| winner.key.as_slice()) {
        return TurnBudget::stored_row_work(left);
    }
    let output_rows = if kind.left_only() {
        usize::from(before.is_some() != after.is_some())
    } else {
        usize::from(before.is_some() || kind == AsOfJoinKind::LeftOuter).saturating_add(
            usize::from(after.is_some() || kind == AsOfJoinKind::LeftOuter),
        )
    };
    let mut bytes = TurnBudget::stored_row_work(left).1;
    bytes = bytes.saturating_add(decoded_row_work(&left.key).saturating_mul(output_rows));
    if !kind.left_only() {
        bytes = bytes
            .saturating_add(before.map_or(0, |winner| decoded_row_work(&winner.row)))
            .saturating_add(after.map_or(0, |winner| decoded_row_work(&winner.row)));
    }
    (output_rows.max(1), bytes.max(1))
}

fn decoded_row_work(row: &[u8]) -> usize {
    row.len().saturating_mul(2).max(1)
}

impl TurnBudget {
    const fn new() -> Self {
        Self { items: 0, bytes: 0 }
    }

    fn can_start(&self, row: &PreparedRow) -> bool {
        self.is_empty()
            || self.items < TURN_ITEMS
                && self.bytes < TURN_BYTES
                && Self::stored_row_work(row).1 <= self.remaining_bytes().max(1)
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

    fn can_accept(&self, work: (usize, usize)) -> bool {
        self.is_empty()
            || self.items.saturating_add(work.0) <= TURN_ITEMS
                && self.bytes.saturating_add(work.1) <= TURN_BYTES
    }

    fn charge(&mut self, work: (usize, usize)) {
        self.items = self.items.saturating_add(work.0);
        self.bytes = self.bytes.saturating_add(work.1);
    }

    fn exhausted(&self) -> bool {
        self.items >= TURN_ITEMS || self.bytes >= TURN_BYTES
    }

    fn stored_row_work(row: &PreparedRow) -> (usize, usize) {
        (1, row.key.len().saturating_add(MAP_VALUE_BYTES).max(1))
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

    fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, AsOfJoinError> {
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
