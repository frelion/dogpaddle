use std::collections::{BTreeSet, HashMap, HashSet};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_store::{Cell, TransactionAccess};

use super::{
    config::{PostgresSinkConfig, PostgresTargetSpec},
    error::{PostgresSinkError, invalid_batch},
    row::EncodedRow,
    state::PostgresSinkState,
    target::{Delivery, PostgresTarget},
};
use crate::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
    sink::relation::{
        Continuation, FIRST_TECHNICAL_ID, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID, Mutation,
        MutationKind, Position, RelationError, advance_position, ensure_id_capacity,
        first_position, position_index, validate_apply_for_change, validate_batch_boundary,
        validate_next_id, validate_position_for_change,
    },
};

const FIRST_DELIVERY: u64 = 1;

/// Materialized exact-Schema-bound `PostgreSQL` relation sink.
///
/// The Store cell is the source of truth. The runtime phase only caches which
/// durable state has already been restored or externally delivered.
pub struct PostgresSinkOperation {
    target: PostgresTarget,
    state: Cell<Vec<u8>>,
    phase: RuntimePhase,
    target_verified: bool,
}

enum RuntimePhase {
    Restore,
    InitializeDelivered,
    Ready(ReadyState),
    PreparedDelivered(PostgresSinkState),
}

#[derive(Clone, Copy)]
struct ReadyState {
    next_delivery: u64,
    next_id: u64,
    position: Option<Position>,
}

#[derive(Clone, Copy)]
struct Settlement {
    next_delivery: u64,
    next_id: u64,
    position: Option<Position>,
    completes_input: bool,
}

impl PostgresSinkOperation {
    pub(super) fn new_bound(
        spec: PostgresTargetSpec,
        input_schema: SchemaRef,
        state: Cell<Vec<u8>>,
        config: PostgresSinkConfig,
    ) -> Self {
        Self {
            target: PostgresTarget::new_bound(config, spec, input_schema),
            state,
            phase: RuntimePhase::Restore,
            target_verified: false,
        }
    }

    fn validate_input(&self, input: OperationInput<'_>) -> Result<(), PostgresSinkError> {
        if input.port != 0 {
            return Err(invalid_batch(format!(
                "input port {} is invalid; expected port 0",
                input.port
            )));
        }
        let actual = input.change.schema();
        if actual.as_ref() != self.target.schema().as_ref() {
            return Err(PostgresSinkError::InputSchemaMismatch {
                expected: self.target.schema().clone(),
                actual,
            });
        }
        Ok(())
    }

    fn restore<'turn>(&'turn mut self, input: OperationInput<'turn>) -> Turn<'turn> {
        let Self {
            target,
            state,
            phase,
            target_verified,
        } = self;
        Turn::ready(move |access| {
            let encoded = state.access(access)?.get()?;
            let durable = encoded
                .as_deref()
                .map(PostgresSinkState::decode)
                .transpose()
                .map_err(map_state_error)?
                .unwrap_or(PostgresSinkState::Initialize);
            validate_state_for_change(&durable, input.change)?;
            if encoded.is_none() {
                set_state(state, access, &durable)?;
            }

            match durable {
                PostgresSinkState::Initialize => Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        target.initialize().map_err(PostCommitError::new)?;
                        *target_verified = true;
                        *phase = RuntimePhase::InitializeDelivered;
                        Ok(())
                    }),
                )),
                PostgresSinkState::Ready {
                    next_delivery,
                    next_id,
                    position,
                } => Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        *phase = RuntimePhase::Ready(ReadyState {
                            next_delivery,
                            next_id,
                            position,
                        });
                        Ok(())
                    }),
                )),
                prepared @ PostgresSinkState::Prepared {
                    delivery,
                    digest,
                    next_id_before,
                    ..
                } => Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        let PostgresSinkState::Prepared { mutations, .. } = &prepared else {
                            unreachable!("the restored state was matched as Prepared")
                        };
                        target
                            .commit_batch(
                                Delivery {
                                    sequence: delivery,
                                    next_id_before,
                                    digest,
                                },
                                input.change,
                                mutations,
                            )
                            .map_err(PostCommitError::new)?;
                        *target_verified = true;
                        *phase = RuntimePhase::PreparedDelivered(prepared);
                        Ok(())
                    }),
                )),
            }
        })
    }

    fn settle_initialization(&mut self) -> Result<Turn<'_>, OperationError> {
        let durable = PostgresSinkState::Ready {
            next_delivery: FIRST_DELIVERY,
            next_id: FIRST_TECHNICAL_ID,
            position: None,
        };
        let encoded = durable.encode().map_err(map_state_error)?;
        let Self { state, phase, .. } = self;
        Ok(Turn::ready(move |access| {
            state.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    *phase = RuntimePhase::Ready(ReadyState {
                        next_delivery: FIRST_DELIVERY,
                        next_id: FIRST_TECHNICAL_ID,
                        position: None,
                    });
                    Ok(())
                }),
            ))
        }))
    }

    fn prepare<'turn>(
        &'turn mut self,
        input: OperationInput<'turn>,
        ready: ReadyState,
    ) -> Result<Turn<'turn>, OperationError> {
        if ready.next_delivery > MAX_TECHNICAL_ID {
            return Err(invalid_batch("delivery sequence is exhausted").into());
        }
        validate_next_id(ready.next_id).map_err(map_relation_error)?;
        if !self.target_verified {
            self.target
                .verify_ready(ready.next_id, ready.next_delivery)?;
            self.target_verified = true;
        }

        let start_position = ready
            .position
            .unwrap_or_else(|| first_position(input.change));
        let planned = plan_batch(
            &mut self.target,
            input.change,
            ready.next_id,
            start_position,
        );
        if planned.is_err() {
            self.target_verified = false;
        }
        let (continuation, mutations) = planned?;
        let digest = self.target.digest_batch(
            ready.next_delivery,
            ready.next_id,
            input.change,
            &mutations,
        )?;
        let prepared = PostgresSinkState::Prepared {
            delivery: ready.next_delivery,
            digest,
            next_id_before: ready.next_id,
            start_position,
            continuation,
            mutations,
        };
        let encoded = prepared.encode().map_err(map_state_error)?;

        let Self {
            target,
            state,
            phase,
            target_verified,
        } = self;
        Ok(Turn::ready(move |access| {
            state.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    let PostgresSinkState::Prepared {
                        delivery,
                        digest,
                        next_id_before,
                        mutations,
                        ..
                    } = &prepared
                    else {
                        unreachable!("the newly prepared state is Prepared")
                    };
                    target
                        .commit_batch(
                            Delivery {
                                sequence: *delivery,
                                next_id_before: *next_id_before,
                                digest: *digest,
                            },
                            input.change,
                            mutations,
                        )
                        .map_err(PostCommitError::new)?;
                    *target_verified = true;
                    *phase = RuntimePhase::PreparedDelivered(prepared);
                    Ok(())
                }),
            ))
        }))
    }

    fn settle_prepared(&mut self, settlement: Settlement) -> Result<Turn<'_>, OperationError> {
        let Settlement {
            next_delivery,
            next_id,
            position,
            completes_input,
        } = settlement;
        let ready = PostgresSinkState::Ready {
            next_delivery,
            next_id,
            position,
        };
        let encoded = ready.encode().map_err(map_state_error)?;
        let action = if completes_input {
            Action::Complete(None)
        } else {
            Action::Commit(None)
        };
        let Self { state, phase, .. } = self;
        Ok(Turn::ready(move |access| {
            state.access(access)?.set(&encoded)?;
            Ok((
                action,
                AfterCommit::new(move || {
                    *phase = RuntimePhase::Ready(ReadyState {
                        next_delivery,
                        next_id,
                        position,
                    });
                    Ok(())
                }),
            ))
        }))
    }
}

impl Operation for PostgresSinkOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let input = input.ok_or_else(|| invalid_batch("PostgreSQL sink requires one input"))?;
        self.validate_input(input)?;

        if matches!(self.phase, RuntimePhase::Restore) {
            return Ok(self.restore(input));
        }
        if matches!(self.phase, RuntimePhase::InitializeDelivered) {
            return self.settle_initialization();
        }
        if let RuntimePhase::Ready(ready) = &self.phase {
            let ready = *ready;
            return self.prepare(input, ready);
        }
        if let RuntimePhase::PreparedDelivered(prepared) = &self.phase {
            let settlement = prepared_settlement(prepared, input.change)?;
            return self.settle_prepared(settlement);
        }
        unreachable!("all PostgreSQL sink runtime phases are handled")
    }
}

fn plan_batch(
    target: &mut PostgresTarget,
    change: &Change,
    next_id: u64,
    start_position: Position,
) -> Result<(Continuation, Vec<Mutation>), PostgresSinkError> {
    validate_position_for_change(change, start_position).map_err(map_relation_error)?;
    validate_batch_boundary(change, start_position).map_err(map_relation_error)?;

    let mut position = start_position;
    let mut next_id_after = next_id;
    let mut mutations = Vec::with_capacity(MAX_MUTATIONS_PER_BATCH);
    let mut overlay = HashMap::<Vec<u8>, OverlayRow>::new();

    let continuation = loop {
        let row_index = position_index(change, position).map_err(map_relation_error)?;
        let kind = if change.diffs().value(row_index) > 0 {
            MutationKind::Insert
        } else {
            MutationKind::Delete
        };
        let mut encoded = target.encode_row(change, row_index)?;
        let key = std::mem::take(&mut encoded.canonical);
        let entry = overlay
            .entry(key)
            .or_insert_with(|| OverlayRow::new(encoded));
        let capacity = MAX_MUTATIONS_PER_BATCH - mutations.len();
        let take = position
            .remaining
            .min(u64::try_from(capacity).expect("the batch limit fits u64"));

        match kind {
            MutationKind::Insert => {
                ensure_id_capacity(next_id_after, position.remaining)
                    .map_err(map_relation_error)?;
                for _ in 0..take {
                    let technical_id = next_id_after;
                    next_id_after = next_id_after
                        .checked_add(1)
                        .expect("the admitted technical-ID range has a successor sentinel");
                    entry.pending_inserts.insert(technical_id);
                    mutations.push(Mutation {
                        kind,
                        row_index: position.row_index,
                        technical_id,
                    });
                }
            }
            MutationKind::Delete => {
                let selected =
                    select_delete_ids(target, entry, position.row_index, position.remaining, take)?;
                for technical_id in selected {
                    if !entry.pending_inserts.remove(&technical_id) {
                        entry.selected_deletes.insert(technical_id);
                    }
                    mutations.push(Mutation {
                        kind,
                        row_index: position.row_index,
                        technical_id,
                    });
                }
            }
        }

        match advance_position(change, position, take).map_err(map_relation_error)? {
            Continuation::Done => break Continuation::Done,
            Continuation::Position(next) => {
                position = next;
                if mutations.len() == MAX_MUTATIONS_PER_BATCH {
                    break Continuation::Position(position);
                }
            }
        }
    };

    Ok((continuation, mutations))
}

fn select_delete_ids(
    target: &mut PostgresTarget,
    overlay: &OverlayRow,
    row_index: u64,
    needed: u64,
    take: u64,
) -> Result<Vec<u64>, PostgresSinkError> {
    let pending_insert_count = u64::try_from(overlay.pending_inserts.len())
        .expect("one bounded pending batch cannot overflow u64");
    let required_database = needed.saturating_sub(pending_insert_count);
    let scan_target = required_database.max(take).min(MAX_TECHNICAL_ID);
    let selected_capacity =
        usize::try_from(take).expect("one selected deletion count is bounded by the batch limit");
    let matching = target.matching_ids(
        &overlay.encoded,
        &overlay.selected_deletes,
        scan_target,
        selected_capacity,
    )?;

    let available = matching.count.saturating_add(pending_insert_count);
    if available < needed {
        return Err(invalid_batch(format!(
            "row {row_index} retracts {needed} instances, but only {available} exist"
        )));
    }

    let mut selected = matching.selected;
    let remaining = selected_capacity - selected.len();
    selected.extend(overlay.pending_inserts.iter().copied().take(remaining));
    debug_assert_eq!(selected.len(), selected_capacity);
    Ok(selected)
}

struct OverlayRow {
    encoded: EncodedRow,
    pending_inserts: BTreeSet<u64>,
    selected_deletes: HashSet<u64>,
}

impl OverlayRow {
    fn new(encoded: EncodedRow) -> Self {
        Self {
            encoded,
            pending_inserts: BTreeSet::new(),
            selected_deletes: HashSet::new(),
        }
    }
}

fn validate_state_for_change(
    state: &PostgresSinkState,
    change: &Change,
) -> Result<(), PostgresSinkError> {
    match state {
        PostgresSinkState::Initialize | PostgresSinkState::Ready { position: None, .. } => Ok(()),
        PostgresSinkState::Ready {
            position: Some(position),
            ..
        } => {
            validate_position_for_change(change, *position).map_err(map_relation_error)?;
            validate_batch_boundary(change, *position).map_err(map_relation_error)
        }
        prepared @ PostgresSinkState::Prepared {
            start_position,
            continuation,
            mutations,
            ..
        } => {
            let (_, next_id) = prepared
                .settled_frontiers()
                .expect("the state was matched as Prepared");
            validate_apply_for_change(change, next_id, *start_position, *continuation, mutations)
                .map_err(map_relation_error)
        }
    }
}

fn prepared_settlement(
    prepared: &PostgresSinkState,
    change: &Change,
) -> Result<Settlement, PostgresSinkError> {
    validate_state_for_change(prepared, change)?;
    let (next_delivery, next_id) = prepared
        .settled_frontiers()
        .expect("the volatile delivered phase contains Prepared state");
    let continuation = match prepared {
        PostgresSinkState::Prepared { continuation, .. } => *continuation,
        _ => unreachable!("the volatile delivered phase contains Prepared state"),
    };
    Ok(match continuation {
        Continuation::Done => Settlement {
            next_delivery,
            next_id,
            position: None,
            completes_input: true,
        },
        Continuation::Position(position) => Settlement {
            next_delivery,
            next_id,
            position: Some(position),
            completes_input: false,
        },
    })
}

fn set_state(
    cell: &Cell<Vec<u8>>,
    access: TransactionAccess<'_>,
    state: &PostgresSinkState,
) -> Result<(), OperationError> {
    let encoded = state.encode().map_err(map_state_error)?;
    cell.access(access)?.set(&encoded)?;
    Ok(())
}

fn map_state_error(error: impl std::fmt::Display) -> PostgresSinkError {
    invalid_batch(format!("durable state is invalid: {error}"))
}

fn map_relation_error(error: RelationError) -> PostgresSinkError {
    match error {
        RelationError::InvalidNextId { next_id } => invalid_batch(format!(
            "next technical ID {next_id} is outside 1..=i64::MAX+1"
        )),
        RelationError::TechnicalIdExhausted { next_id, needed } => invalid_batch(format!(
            "technical ID range from {next_id} cannot reserve {needed} inserts"
        )),
        RelationError::InputMismatch { message } => invalid_batch(message),
    }
}
