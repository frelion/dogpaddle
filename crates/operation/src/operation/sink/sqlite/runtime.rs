use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::PathBuf,
};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_store::{Cell, TransactionAccess};

use super::{
    definition::SqliteSinkSchemaError,
    error::{SqliteSinkError, invalid_state, pending_mismatch},
    row::{EncodedRow, RowCodec},
    state::PendingState,
    target::SqliteTarget,
};
use crate::operation::{
    Action, OperationError, OperationInput, TransactionalOperation,
    sink::relation::{
        Continuation, FIRST_TECHNICAL_ID, MAX_MUTATIONS_PER_BATCH, Mutation, MutationKind,
        Position, RelationError, advance_position, ensure_id_capacity, first_position,
        position_index, validate_apply_for_change, validate_batch_boundary, validate_next_id,
        validate_position_for_change,
    },
};
/// Materialized exact-Schema-bound `SQLite` relation sink.
///
/// The operation owns a logical row codec, one lazily opened target, and the two
/// Store cells declared by its persistent definition.
pub struct SqliteSinkOperation {
    row_codec: RowCodec,
    target: SqliteTarget,
    next_id: Cell<u64>,
    pending: Cell<Vec<u8>>,
}

/// Pure Schema- and destination-bound plan captured before Store materialization.
pub(super) struct SqliteSinkCompiled {
    row_codec: RowCodec,
    target: SqliteTarget,
}

impl SqliteSinkCompiled {
    pub(super) fn try_new(
        database_path: PathBuf,
        table_name: String,
        input_schema: SchemaRef,
    ) -> Result<Self, SqliteSinkSchemaError> {
        let row_codec = RowCodec::new_validated(input_schema);
        let target = SqliteTarget::try_new(database_path, table_name, &row_codec)?;
        Ok(Self { row_codec, target })
    }
}

impl SqliteSinkOperation {
    pub(super) fn new_bound(
        compiled: SqliteSinkCompiled,
        next_id: Cell<u64>,
        pending: Cell<Vec<u8>>,
    ) -> Self {
        Self {
            row_codec: compiled.row_codec,
            target: compiled.target,
            next_id,
            pending,
        }
    }

    fn turn_inner(
        &mut self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        if input.port != 0 {
            return Err(SqliteSinkError::InvalidInputPort { port: input.port }.into());
        }
        let actual_schema = input.change.schema();
        if actual_schema.as_ref() != self.row_codec.schema().as_ref() {
            return Err(SqliteSinkError::InputSchemaMismatch {
                expected: self.row_codec.schema().clone(),
                actual: actual_schema,
            }
            .into());
        }

        let next_id = self.next_id.access(access)?.get()?;
        let pending = self
            .pending
            .access(access)?
            .get()?
            .map(|encoded| decode_pending(&encoded))
            .transpose()
            .map_err(OperationError::from)?;

        match (next_id, pending) {
            (None, None) => self.prepare_initialization(access),
            (None, Some(PendingState::Initialize)) => self.apply_initialization(access),
            (None, Some(_)) => Err(invalid_state(
                "pending row work exists before the target is initialized",
            )
            .into()),
            (Some(_), Some(PendingState::Initialize)) => Err(invalid_state(
                "initialization remains pending after next_id was initialized",
            )
            .into()),
            (Some(next_id), pending) => {
                validate_next_id(next_id).map_err(map_relation_error)?;
                self.target.verify_ready(next_id)?;
                match pending {
                    None => {
                        let start = first_position(input.change);
                        self.prepare_batch(input.change, access, next_id, start)
                    }
                    Some(PendingState::Prepare { position }) => {
                        self.prepare_batch(input.change, access, next_id, position)
                    }
                    Some(PendingState::Apply {
                        start_position,
                        continuation,
                        mutations,
                    }) => self.apply_batch(
                        input.change,
                        access,
                        next_id,
                        start_position,
                        continuation,
                        &mutations,
                    ),
                    Some(PendingState::Initialize) => {
                        unreachable!("handled by the enclosing state match")
                    }
                }
            }
        }
    }

    fn prepare_initialization(
        &mut self,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        self.target.require_absent()?;
        set_pending(&self.pending, access, &PendingState::Initialize)?;
        Ok(Action::Commit(None))
    }

    fn apply_initialization(
        &mut self,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let transaction = self.target.begin()?;
        transaction.initialize()?;

        self.next_id.access(access)?.set(&FIRST_TECHNICAL_ID)?;
        self.pending.access(access)?.clear()?;
        transaction.commit()?;
        Ok(Action::Commit(None))
    }

    fn prepare_batch(
        &mut self,
        change: &Change,
        access: TransactionAccess<'_>,
        next_id: u64,
        start_position: Position,
    ) -> Result<Action, OperationError> {
        validate_position_for_change(change, start_position).map_err(map_relation_error)?;
        validate_batch_boundary(change, start_position).map_err(map_relation_error)?;
        let mut position = start_position;
        let mut next_id_after = next_id;
        let mut mutations = Vec::with_capacity(MAX_MUTATIONS_PER_BATCH);
        let mut overlay = HashMap::<Vec<u8>, OverlayRow>::new();

        let continuation = loop {
            let row_index = position_index(change, position).map_err(map_relation_error)?;
            let diff = change.diffs().value(row_index);
            let kind = if diff > 0 {
                MutationKind::Insert
            } else {
                MutationKind::Delete
            };
            let mut encoded = self
                .row_codec
                .encode_row(change.records(), row_index)
                .map_err(SqliteSinkError::from)?;
            let key = encoded.take_canonical();
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
                    let selected = self.select_delete_ids(
                        entry,
                        position.row_index,
                        position.remaining,
                        take,
                    )?;
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

        let state = PendingState::Apply {
            start_position,
            continuation,
            mutations,
        };
        if next_id_after != next_id {
            self.next_id.access(access)?.set(&next_id_after)?;
        }
        set_pending(&self.pending, access, &state)?;
        Ok(Action::Commit(None))
    }

    fn select_delete_ids(
        &mut self,
        overlay: &OverlayRow,
        row_index: u64,
        needed: u64,
        take: u64,
    ) -> Result<Vec<u64>, SqliteSinkError> {
        let pending_insert_count = u64::try_from(overlay.pending_inserts.len())
            .expect("one bounded pending batch cannot overflow u64");
        let required_database = needed.saturating_sub(pending_insert_count);
        let scan_target = required_database.max(take);
        let selected_capacity = usize::try_from(take)
            .expect("one selected deletion count is bounded by the batch limit");
        let matching = self.target.matching_ids(
            &overlay.encoded,
            &overlay.selected_deletes,
            scan_target,
            selected_capacity,
        )?;

        let available = matching.count.saturating_add(pending_insert_count);
        if available < needed {
            return Err(SqliteSinkError::MissingRetraction {
                row_index,
                needed,
                available,
            });
        }

        let mut selected = matching.selected;
        let remaining = selected_capacity - selected.len();
        selected.extend(overlay.pending_inserts.iter().copied().take(remaining));
        debug_assert_eq!(selected.len(), selected_capacity);
        Ok(selected)
    }

    fn apply_batch(
        &mut self,
        change: &Change,
        access: TransactionAccess<'_>,
        next_id: u64,
        start_position: Position,
        continuation: Continuation,
        mutations: &[Mutation],
    ) -> Result<Action, OperationError> {
        validate_apply_for_change(change, next_id, start_position, continuation, mutations)
            .map_err(map_relation_error)?;
        let transaction = self.target.begin()?;
        transaction.apply(&self.row_codec, change, mutations)?;

        let action = match continuation {
            Continuation::Done => {
                self.pending.access(access)?.clear()?;
                Action::Complete(None)
            }
            Continuation::Position(position) => {
                set_pending(&self.pending, access, &PendingState::Prepare { position })?;
                Action::Commit(None)
            }
        };
        transaction.commit()?;
        Ok(action)
    }
}

impl TransactionalOperation for SqliteSinkOperation {
    fn apply(
        &mut self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let input = input.ok_or(SqliteSinkError::MissingInput)?;
        self.turn_inner(input, access)
    }
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

fn set_pending(
    pending: &Cell<Vec<u8>>,
    access: TransactionAccess<'_>,
    state: &PendingState,
) -> Result<(), OperationError> {
    let encoded = state.encode().map_err(SqliteSinkError::from)?;
    pending.access(access)?.set(&encoded)?;
    Ok(())
}

fn decode_pending(encoded: &[u8]) -> Result<PendingState, SqliteSinkError> {
    PendingState::decode(encoded).map_err(SqliteSinkError::from)
}

fn map_relation_error(error: RelationError) -> SqliteSinkError {
    match error {
        RelationError::InvalidNextId { next_id } => invalid_state(format!(
            "next technical ID {next_id} is outside 1..=i64::MAX+1"
        )),
        RelationError::TechnicalIdExhausted { next_id, needed } => {
            SqliteSinkError::TechnicalIdExhausted { next_id, needed }
        }
        RelationError::InputMismatch { message } => pending_mismatch(message),
    }
}
