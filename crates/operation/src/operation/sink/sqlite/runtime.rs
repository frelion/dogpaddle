use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_store::{Cell, TransactionAccess};
use rusqlite::{
    Connection, OpenFlags, ToSql, Transaction, TransactionBehavior, params, params_from_iter,
    types::ValueRef,
};
use thiserror::Error;

use super::{
    TECHNICAL_HASH, TECHNICAL_ID,
    row::{EncodedRow, RowCodec, RowError, column_definition, quote_identifier},
    state::{
        Continuation, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID, Mutation, MutationKind,
        PendingState, PendingStateCodecError, Position,
    },
};
use crate::operation::{Action, Operation, OperationError, OperationInput};

const FIRST_TECHNICAL_ID: u64 = 1;
const EXHAUSTED_TECHNICAL_ID: u64 = MAX_TECHNICAL_ID + 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Materialized exact-Schema-bound `SQLite` relation sink.
///
/// The operation owns only its compiled SQL/value plan, lazily opened `SQLite`
/// connection, and the two Store cells declared by its persistent definition.
pub struct SqliteSinkOperation {
    database_path: PathBuf,
    row_codec: RowCodec,
    sql: SqlPlan,
    next_id: Cell<u64>,
    pending: Cell<Vec<u8>>,
    connection: Mutex<Option<Connection>>,
}

/// Pure Schema- and destination-bound plan captured before Store materialization.
pub(super) struct SqliteSinkCompiled {
    database_path: PathBuf,
    row_codec: RowCodec,
    sql: SqlPlan,
}

/// SQLiteSink-specific failure during one operation turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteSinkError {
    /// The sink was called without an input Change.
    #[error("SQLite sink requires one input Change")]
    MissingInput,
    /// `SQLiteSink` only accepts its definition's first input port.
    #[error("SQLite sink does not accept input port {port}")]
    InvalidInputPort {
        /// Rejected zero-based port index.
        port: usize,
    },
    /// A direct caller supplied a Schema different from the bound Schema.
    #[error("SQLite sink input Schema differs from its bound Schema")]
    InputSchemaMismatch {
        /// Exact Schema fixed during binding.
        expected: SchemaRef,
        /// Schema supplied by this turn.
        actual: SchemaRef,
    },
    /// The lazily held `SQLite` connection mutex was poisoned by a panic.
    #[error("SQLite sink connection mutex is poisoned")]
    ConnectionPoisoned,
    /// The persistent next-ID and pending-state cells contradict each other.
    #[error("SQLite sink persistent state is invalid: {message}")]
    InvalidState {
        /// Stable diagnostic for the rejected state.
        message: String,
    },
    /// A retained Change does not agree with its persistent batch position.
    #[error("SQLite sink pending batch does not match its retained Change: {message}")]
    PendingInputMismatch {
        /// Stable diagnostic for the mismatch.
        message: String,
    },
    /// A logical row could not be encoded exactly.
    #[error("SQLite sink row processing failed: {message}")]
    Row {
        /// Stable diagnostic from the private row codec.
        message: String,
    },
    /// The target table or its reserved index already exists on first use.
    #[error("SQLite sink target object {name:?} already exists")]
    TargetExists {
        /// Existing `SQLite` object name.
        name: String,
    },
    /// A previously initialized target table or index is missing.
    #[error("SQLite sink target object {name:?} is missing")]
    TargetMissing {
        /// Missing `SQLite` object name.
        name: String,
    },
    /// A target object differs from the exact layout created by this sink.
    #[error("SQLite sink target object {name:?} has an incompatible layout")]
    TargetLayoutMismatch {
        /// Incompatible `SQLite` object name.
        name: String,
    },
    /// Initialization replay found rows in a table that should still be empty.
    #[error("SQLite sink target table {table:?} is not empty during initialization")]
    TargetNotEmpty {
        /// Target table name.
        table: String,
    },
    /// The target contains an ID outside the sink-owned positive ID range.
    #[error("SQLite sink target contains invalid technical ID {id}")]
    InvalidStoredTechnicalId {
        /// Invalid stored ID.
        id: i64,
    },
    /// The target contains an ID not below the durable next-ID frontier.
    #[error("SQLite sink target technical ID {id} is not below next ID {next_id}")]
    TechnicalIdFrontierMismatch {
        /// Observed target ID.
        id: u64,
        /// Durable next unallocated ID.
        next_id: u64,
    },
    /// A positive multiplicity cannot fit in the remaining technical-ID space.
    #[error("SQLite sink technical IDs are exhausted at {next_id}; {needed} IDs are required")]
    TechnicalIdExhausted {
        /// Current next unallocated ID.
        next_id: u64,
        /// Complete remaining multiplicity that must be admitted atomically.
        needed: u64,
    },
    /// A negative multiplicity has fewer exact physical rows than required.
    #[error(
        "SQLite sink cannot retract row {row_index}: {needed} instances are required but only {available} exist"
    )]
    MissingRetraction {
        /// Change row containing the invalid negative difference.
        row_index: u64,
        /// Complete remaining multiplicity requested by the event.
        needed: u64,
        /// Exact matching instances visible after earlier mutations in the batch.
        available: u64,
    },
    /// An idempotent insert found the same ID attached to another logical row.
    #[error("SQLite sink technical ID {id} belongs to a different logical row")]
    TechnicalIdConflict {
        /// Conflicting technical ID.
        id: u64,
    },
    /// A prepared delete found its ID attached to another logical row.
    #[error("SQLite sink delete ID {id} belongs to a different logical row")]
    DeleteRowMismatch {
        /// Mismatched technical ID.
        id: u64,
    },
    /// `SQLite` returned a mutation count impossible for a primary-key operation.
    #[error("SQLite sink {operation} for ID {id} changed {actual} rows, expected {expected}")]
    UnexpectedMutationCount {
        /// Operation being checked.
        operation: &'static str,
        /// Technical ID used by the mutation.
        id: u64,
        /// Required affected-row count.
        expected: usize,
        /// Reported affected-row count.
        actual: usize,
    },
    /// `SQLite` rejected connection setup, schema inspection, or row mutation.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

impl SqliteSinkCompiled {
    pub(super) fn new(database_path: PathBuf, table_name: String, input_schema: SchemaRef) -> Self {
        let row_codec = RowCodec::new_validated(input_schema);
        let sql = SqlPlan::new(table_name, &row_codec);
        Self {
            database_path,
            row_codec,
            sql,
        }
    }
}

impl SqliteSinkOperation {
    pub(super) fn new_bound(
        compiled: SqliteSinkCompiled,
        next_id: Cell<u64>,
        pending: Cell<Vec<u8>>,
    ) -> Self {
        Self {
            database_path: compiled.database_path,
            row_codec: compiled.row_codec,
            sql: compiled.sql,
            next_id,
            pending,
            connection: Mutex::new(None),
        }
    }

    fn turn_inner(
        &self,
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

        let mut connection_slot = self
            .connection
            .lock()
            .map_err(|_| SqliteSinkError::ConnectionPoisoned)?;
        if connection_slot.is_none() {
            *connection_slot = Some(open_connection(&self.database_path)?);
        }
        let connection = connection_slot
            .as_mut()
            .expect("the SQLite connection was initialized above");

        match (next_id, pending) {
            (None, None) => self.prepare_initialization(connection, access),
            (None, Some(PendingState::Initialize)) => self.apply_initialization(connection, access),
            (None, Some(_)) => Err(invalid_state(
                "pending row work exists before the target is initialized",
            )
            .into()),
            (Some(_), Some(PendingState::Initialize)) => Err(invalid_state(
                "initialization remains pending after next_id was initialized",
            )
            .into()),
            (Some(next_id), pending) => {
                validate_next_id(next_id)?;
                self.verify_ready_target(connection, next_id)?;
                match pending {
                    None => {
                        let start = first_position(input.change);
                        self.prepare_batch(connection, input.change, access, next_id, start)
                    }
                    Some(PendingState::Prepare { position }) => {
                        self.prepare_batch(connection, input.change, access, next_id, position)
                    }
                    Some(PendingState::Apply {
                        start_position,
                        continuation,
                        mutations,
                    }) => self.apply_batch(
                        connection,
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
        &self,
        connection: &Connection,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        for name in [&self.sql.table_name, &self.sql.index_name] {
            if object_exists(connection, name).map_err(SqliteSinkError::from)? {
                return Err(SqliteSinkError::TargetExists { name: name.clone() }.into());
            }
        }
        set_pending(&self.pending, access, &PendingState::Initialize)?;
        Ok(Action::Commit(None))
    }

    fn apply_initialization(
        &self,
        connection: &mut Connection,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteSinkError::from)?;
        if !object_exists(&transaction, &self.sql.table_name).map_err(SqliteSinkError::from)? {
            if object_exists(&transaction, &self.sql.index_name).map_err(SqliteSinkError::from)? {
                return Err(SqliteSinkError::TargetLayoutMismatch {
                    name: self.sql.index_name.clone(),
                }
                .into());
            }
            transaction
                .execute(&self.sql.create_table, [])
                .map_err(SqliteSinkError::from)?;
            transaction
                .execute(&self.sql.create_index, [])
                .map_err(SqliteSinkError::from)?;
        }
        require_exact_layout(&transaction, &self.sql)?;
        if technical_id_bounds(&transaction, &self.sql).map_err(SqliteSinkError::from)?
            != (None, None)
        {
            return Err(SqliteSinkError::TargetNotEmpty {
                table: self.sql.table_name.clone(),
            }
            .into());
        }

        self.next_id.access(access)?.set(&FIRST_TECHNICAL_ID)?;
        self.pending.access(access)?.clear()?;
        transaction.commit().map_err(SqliteSinkError::from)?;
        Ok(Action::Commit(None))
    }

    fn verify_ready_target(
        &self,
        connection: &Connection,
        next_id: u64,
    ) -> Result<(), SqliteSinkError> {
        require_exact_layout(connection, &self.sql)?;

        let (minimum, maximum) = technical_id_bounds(connection, &self.sql)?;
        if let Some(minimum) = minimum
            && minimum <= 0
        {
            return Err(SqliteSinkError::InvalidStoredTechnicalId { id: minimum });
        }
        if let Some(maximum) = maximum {
            let maximum = u64::try_from(maximum)
                .expect("the minimum-ID check proves every stored ID is positive");
            if maximum >= next_id {
                return Err(SqliteSinkError::TechnicalIdFrontierMismatch {
                    id: maximum,
                    next_id,
                });
            }
        }
        Ok(())
    }

    fn prepare_batch(
        &self,
        connection: &Connection,
        change: &Change,
        access: TransactionAccess<'_>,
        next_id: u64,
        start_position: Position,
    ) -> Result<Action, OperationError> {
        validate_position_for_change(change, start_position)?;
        validate_batch_boundary(change, start_position)?;
        let mut position = start_position;
        let mut next_id_after = next_id;
        let mut mutations = Vec::with_capacity(MAX_MUTATIONS_PER_BATCH);
        let mut overlay = HashMap::<Vec<u8>, OverlayRow>::new();

        let continuation = loop {
            let row_index = position_index(change, position)?;
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
                    ensure_id_capacity(next_id_after, position.remaining)?;
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
                        connection,
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

            match advance_position(change, position, take)? {
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
        &self,
        connection: &Connection,
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
        let mut selected_database = Vec::with_capacity(selected_capacity);
        let mut database_count = 0_u64;

        let mut statement = connection.prepare_cached(&self.sql.select_by_hash)?;
        let mut rows = statement.query(params![overlay.encoded.hash.as_slice()])?;
        while database_count < scan_target {
            let Some(row) = rows.next()? else {
                break;
            };
            let id: i64 = row.get(0)?;
            if id <= 0 {
                return Err(SqliteSinkError::InvalidStoredTechnicalId { id });
            }
            let id = u64::try_from(id).expect("a positive SQLite INTEGER fits u64");
            if overlay.selected_deletes.contains(&id) {
                continue;
            }
            if !overlay.encoded.matches(row, 1)? {
                continue;
            }
            database_count += 1;
            if selected_database.len() < selected_capacity {
                selected_database.push(id);
            }
        }

        let available = database_count.saturating_add(pending_insert_count);
        if available < needed {
            return Err(SqliteSinkError::MissingRetraction {
                row_index,
                needed,
                available,
            });
        }

        let mut selected = selected_database;
        let remaining = selected_capacity - selected.len();
        selected.extend(overlay.pending_inserts.iter().copied().take(remaining));
        debug_assert_eq!(selected.len(), selected_capacity);
        Ok(selected)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the durable Apply tuple is kept explicit at the transaction boundary"
    )]
    fn apply_batch(
        &self,
        connection: &mut Connection,
        change: &Change,
        access: TransactionAccess<'_>,
        next_id: u64,
        start_position: Position,
        continuation: Continuation,
        mutations: &[Mutation],
    ) -> Result<Action, OperationError> {
        validate_apply_for_change(change, next_id, start_position, continuation, mutations)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteSinkError::from)?;
        for row_mutations in mutations.chunk_by(|left, right| left.row_index == right.row_index) {
            let row_index = usize::try_from(row_mutations[0].row_index).map_err(|_| {
                pending_mismatch("mutation row index cannot be represented by usize")
            })?;
            let encoded = self
                .row_codec
                .encode_row(change.records(), row_index)
                .map_err(SqliteSinkError::from)?;
            for mutation in row_mutations {
                match mutation.kind {
                    MutationKind::Insert => {
                        self.apply_insert(&transaction, mutation.technical_id, &encoded)?;
                    }
                    MutationKind::Delete => {
                        self.apply_delete(&transaction, mutation.technical_id, &encoded)?;
                    }
                }
            }
        }

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
        transaction.commit().map_err(SqliteSinkError::from)?;
        Ok(action)
    }

    fn apply_insert(
        &self,
        transaction: &Transaction<'_>,
        technical_id: u64,
        encoded: &EncodedRow,
    ) -> Result<(), SqliteSinkError> {
        let id = technical_id_as_i64(technical_id)?;
        let technical_values = [&id as &dyn ToSql, &encoded.hash as &dyn ToSql];
        let values = technical_values
            .into_iter()
            .chain(encoded.values.iter().map(|value| value as &dyn ToSql));
        let actual = transaction
            .prepare_cached(&self.sql.insert)?
            .execute(params_from_iter(values))?;
        if actual == 1 {
            return Ok(());
        }
        if actual != 0 {
            return Err(SqliteSinkError::UnexpectedMutationCount {
                operation: "insert",
                id: technical_id,
                expected: 1,
                actual,
            });
        }
        match self.row_by_id_matches(transaction, id, encoded)? {
            Some(true) => Ok(()),
            Some(false) | None => Err(SqliteSinkError::TechnicalIdConflict { id: technical_id }),
        }
    }

    fn apply_delete(
        &self,
        transaction: &Transaction<'_>,
        technical_id: u64,
        encoded: &EncodedRow,
    ) -> Result<(), SqliteSinkError> {
        let id = technical_id_as_i64(technical_id)?;
        match self.row_by_id_matches(transaction, id, encoded)? {
            None => return Ok(()),
            Some(false) => {
                return Err(SqliteSinkError::DeleteRowMismatch { id: technical_id });
            }
            Some(true) => {}
        }
        let actual = transaction.execute(&self.sql.delete_by_id, params![id])?;
        if actual == 1 {
            Ok(())
        } else {
            Err(SqliteSinkError::UnexpectedMutationCount {
                operation: "delete",
                id: technical_id,
                expected: 1,
                actual,
            })
        }
    }

    fn row_by_id_matches(
        &self,
        transaction: &Transaction<'_>,
        id: i64,
        encoded: &EncodedRow,
    ) -> Result<Option<bool>, SqliteSinkError> {
        let mut statement = transaction.prepare_cached(&self.sql.select_by_id)?;
        let mut rows = statement.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let matches =
            stored_hash_matches(row.get_ref(0)?, &encoded.hash) && encoded.matches(row, 1)?;
        Ok(Some(matches))
    }
}

impl Operation for SqliteSinkOperation {
    fn turn(
        &self,
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

struct SqlPlan {
    table_name: String,
    index_name: String,
    create_table: String,
    create_index: String,
    insert: String,
    select_by_hash: String,
    select_by_id: String,
    delete_by_id: String,
    technical_id_bounds: String,
}

impl SqlPlan {
    fn new(table_name: String, row_codec: &RowCodec) -> Self {
        let quoted_table = quote_identifier(&table_name);
        let index_name = format!("$dogpaddle.hash_index.{table_name}");
        let quoted_index = quote_identifier(&index_name);
        let quoted_id = quote_identifier(TECHNICAL_ID);
        let quoted_hash = quote_identifier(TECHNICAL_HASH);

        let mut definitions = vec![
            format!("{quoted_id} INTEGER PRIMARY KEY"),
            format!(
                "{quoted_hash} BLOB NOT NULL CHECK(typeof({quoted_hash}) = 'blob' AND length({quoted_hash}) = 16)"
            ),
        ];
        definitions.extend(
            row_codec
                .schema()
                .fields()
                .iter()
                .map(|field| column_definition(field)),
        );
        let create_table = format!(
            "CREATE TABLE {quoted_table} ({}) STRICT",
            definitions.join(", ")
        );
        let create_index = format!("CREATE INDEX {quoted_index} ON {quoted_table}({quoted_hash})");

        let mut columns = vec![quoted_id.clone(), quoted_hash.clone()];
        let logical_columns = row_codec
            .schema()
            .fields()
            .iter()
            .map(|field| quote_identifier(field.name()))
            .collect::<Vec<_>>();
        columns.extend(logical_columns.iter().cloned());
        let placeholders = (1..=columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = format!(
            "INSERT INTO {quoted_table} ({}) VALUES ({placeholders}) ON CONFLICT({quoted_id}) DO NOTHING",
            columns.join(", ")
        );

        let logical = logical_columns.join(", ");
        let select_by_hash = if logical.is_empty() {
            format!(
                "SELECT {quoted_id} FROM {quoted_table} WHERE {quoted_hash} = ?1 ORDER BY {quoted_id}"
            )
        } else {
            format!(
                "SELECT {quoted_id}, {logical} FROM {quoted_table} WHERE {quoted_hash} = ?1 ORDER BY {quoted_id}"
            )
        };
        let select_by_id = if logical.is_empty() {
            format!("SELECT {quoted_hash} FROM {quoted_table} WHERE {quoted_id} = ?1")
        } else {
            format!("SELECT {quoted_hash}, {logical} FROM {quoted_table} WHERE {quoted_id} = ?1")
        };
        let delete_by_id = format!("DELETE FROM {quoted_table} WHERE {quoted_id} = ?1");
        let technical_id_bounds =
            format!("SELECT MIN({quoted_id}), MAX({quoted_id}) FROM {quoted_table}");

        Self {
            table_name,
            index_name,
            create_table,
            create_index,
            insert,
            select_by_hash,
            select_by_id,
            delete_by_id,
            technical_id_bounds,
        }
    }
}

fn open_connection(path: &Path) -> Result<Connection, SqliteSinkError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(path, flags)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(connection)
}

fn object_exists(connection: &Connection, name: &str) -> Result<bool, rusqlite::Error> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1 COLLATE NOCASE)",
        params![name],
        |row| row.get(0),
    )
}

fn require_exact_layout(connection: &Connection, sql: &SqlPlan) -> Result<(), SqliteSinkError> {
    let mut statement = connection.prepare_cached(
        "SELECT type, name, sql FROM sqlite_schema \
         WHERE tbl_name = ?1 COLLATE NOCASE \
         AND type IN ('table', 'index', 'trigger') \
         ORDER BY type COLLATE BINARY, name COLLATE BINARY",
    )?;
    let mut rows = statement.query(params![&sql.table_name])?;
    let mut found_table = false;
    let mut found_index = false;
    while let Some(row) = rows.next()? {
        let object_type: String = row.get(0)?;
        let name: String = row.get(1)?;
        let definition: Option<String> = row.get(2)?;
        match object_type.as_str() {
            "table" if name == sql.table_name => {
                found_table = true;
                if definition.as_deref() != Some(sql.create_table.as_str()) {
                    return Err(SqliteSinkError::TargetLayoutMismatch { name });
                }
            }
            "index" if name == sql.index_name => {
                found_index = true;
                if definition.as_deref() != Some(sql.create_index.as_str()) {
                    return Err(SqliteSinkError::TargetLayoutMismatch { name });
                }
            }
            _ => return Err(SqliteSinkError::TargetLayoutMismatch { name }),
        }
    }
    if !found_table {
        return Err(SqliteSinkError::TargetMissing {
            name: sql.table_name.clone(),
        });
    }
    if !found_index {
        return Err(SqliteSinkError::TargetMissing {
            name: sql.index_name.clone(),
        });
    }
    Ok(())
}

fn technical_id_bounds(
    connection: &Connection,
    sql: &SqlPlan,
) -> rusqlite::Result<(Option<i64>, Option<i64>)> {
    connection.query_row(&sql.technical_id_bounds, [], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })
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

fn validate_next_id(next_id: u64) -> Result<(), SqliteSinkError> {
    if (FIRST_TECHNICAL_ID..=EXHAUSTED_TECHNICAL_ID).contains(&next_id) {
        Ok(())
    } else {
        Err(invalid_state(format!(
            "next technical ID {next_id} is outside 1..=i64::MAX+1"
        )))
    }
}

fn ensure_id_capacity(next_id: u64, needed: u64) -> Result<(), SqliteSinkError> {
    let available = EXHAUSTED_TECHNICAL_ID.saturating_sub(next_id);
    if needed <= available {
        Ok(())
    } else {
        Err(SqliteSinkError::TechnicalIdExhausted { next_id, needed })
    }
}

fn first_position(change: &Change) -> Position {
    let diff = change.diffs().value(0);
    Position {
        row_index: 0,
        remaining: diff.unsigned_abs(),
    }
}

fn position_index(change: &Change, position: Position) -> Result<usize, SqliteSinkError> {
    let row_index = usize::try_from(position.row_index)
        .map_err(|_| pending_mismatch("row index cannot be represented by usize"))?;
    if row_index >= change.num_rows() {
        return Err(pending_mismatch(format!(
            "row index {} is outside a {}-row Change",
            position.row_index,
            change.num_rows()
        )));
    }
    Ok(row_index)
}

fn validate_position_for_change(
    change: &Change,
    position: Position,
) -> Result<(), SqliteSinkError> {
    if position.remaining == 0 {
        return Err(pending_mismatch("position has zero remaining multiplicity"));
    }
    let row_index = position_index(change, position)?;
    let magnitude = change.diffs().value(row_index).unsigned_abs();
    if position.remaining > magnitude {
        return Err(pending_mismatch(format!(
            "position remainder {} exceeds row {} magnitude {magnitude}",
            position.remaining, position.row_index
        )));
    }
    Ok(())
}

fn validate_batch_boundary(change: &Change, position: Position) -> Result<(), SqliteSinkError> {
    let row_index = position_index(change, position)?;
    let batch_size = u64::try_from(MAX_MUTATIONS_PER_BATCH)
        .expect("the fixed SQLite mutation batch size fits u64");
    let mut consumed_modulo = 0_u64;
    for index in 0..row_index {
        consumed_modulo = (consumed_modulo
            + change.diffs().value(index).unsigned_abs() % batch_size)
            % batch_size;
    }
    let row_magnitude = change.diffs().value(row_index).unsigned_abs();
    consumed_modulo =
        (consumed_modulo + (row_magnitude - position.remaining) % batch_size) % batch_size;
    if consumed_modulo == 0 {
        Ok(())
    } else {
        Err(pending_mismatch(
            "position is not aligned to a stable 1024-mutation batch boundary",
        ))
    }
}

fn advance_position(
    change: &Change,
    position: Position,
    consumed: u64,
) -> Result<Continuation, SqliteSinkError> {
    if consumed == 0 || consumed > position.remaining {
        return Err(pending_mismatch("batch consumed an invalid multiplicity"));
    }
    if consumed < position.remaining {
        return Ok(Continuation::Position(Position {
            row_index: position.row_index,
            remaining: position.remaining - consumed,
        }));
    }
    let next_row = position
        .row_index
        .checked_add(1)
        .ok_or_else(|| pending_mismatch("row index overflow"))?;
    let next_index = usize::try_from(next_row)
        .map_err(|_| pending_mismatch("row index cannot be represented by usize"))?;
    if next_index == change.num_rows() {
        return Ok(Continuation::Done);
    }
    if next_index > change.num_rows() {
        return Err(pending_mismatch("row position advanced beyond the Change"));
    }
    Ok(Continuation::Position(Position {
        row_index: next_row,
        remaining: change.diffs().value(next_index).unsigned_abs(),
    }))
}

fn validate_apply_for_change(
    change: &Change,
    next_id: u64,
    start_position: Position,
    continuation: Continuation,
    mutations: &[Mutation],
) -> Result<(), SqliteSinkError> {
    // `decode_pending` has already checked the batch's self-contained structural
    // invariants. This pass only relates it to the retained Change and ID frontier.
    validate_position_for_change(change, start_position)?;
    validate_batch_boundary(change, start_position)?;
    let insert_count = u64::try_from(
        mutations
            .iter()
            .filter(|mutation| mutation.kind == MutationKind::Insert)
            .count(),
    )
    .expect("the bounded mutation count fits u64");
    let reserved_start = if insert_count == 0 {
        None
    } else {
        Some(
            next_id
                .checked_sub(insert_count)
                .filter(|first| *first >= FIRST_TECHNICAL_ID)
                .ok_or_else(|| {
                    pending_mismatch("prepared insert count exceeds the durable ID frontier")
                })?,
        )
    };
    let mut actual = Continuation::Position(start_position);
    let mut last_insert_id = None;
    for (mutation_index, mutation) in mutations.iter().enumerate() {
        let Continuation::Position(position) = actual else {
            return Err(pending_mismatch("mutations continue after the Change ends"));
        };
        let row_index = position_index(change, position)?;
        if mutation.row_index != position.row_index {
            return Err(pending_mismatch(
                "mutation row does not match the flattened position",
            ));
        }
        let expected_kind = if change.diffs().value(row_index) > 0 {
            MutationKind::Insert
        } else {
            MutationKind::Delete
        };
        if mutation.kind != expected_kind {
            return Err(pending_mismatch(
                "mutation kind disagrees with the row difference",
            ));
        }
        if mutation.technical_id >= next_id {
            return Err(pending_mismatch(
                "prepared mutation ID is not below the durable next-ID frontier",
            ));
        }
        match mutation.kind {
            MutationKind::Insert => {
                last_insert_id = Some(mutation.technical_id);
            }
            MutationKind::Delete => {
                if reserved_start.is_some_and(|first| mutation.technical_id >= first)
                    && last_insert_id.is_none_or(|last| mutation.technical_id > last)
                {
                    return Err(pending_mismatch(
                        "a mutation deletes a newly allocated ID before inserting it",
                    ));
                }
            }
        }
        actual = advance_position(change, position, 1)?;
        if actual == Continuation::Done && mutation_index + 1 != mutations.len() {
            return Err(pending_mismatch("mutations continue after the Change ends"));
        }
    }
    if actual != continuation {
        return Err(pending_mismatch(
            "stored continuation differs from the flattened mutation sequence",
        ));
    }
    if let Some(last_insert_id) = last_insert_id
        && last_insert_id.checked_add(1) != Some(next_id)
    {
        return Err(pending_mismatch(
            "prepared insert range does not end at the durable next-ID frontier",
        ));
    }
    Ok(())
}

fn technical_id_as_i64(technical_id: u64) -> Result<i64, SqliteSinkError> {
    i64::try_from(technical_id).map_err(|_| {
        invalid_state(format!(
            "technical ID {technical_id} cannot be represented by SQLite INTEGER"
        ))
    })
}

fn stored_hash_matches(actual: ValueRef<'_>, expected: &[u8; 16]) -> bool {
    matches!(actual, ValueRef::Blob(bytes) if bytes == expected)
}

fn invalid_state(message: impl Into<String>) -> SqliteSinkError {
    SqliteSinkError::InvalidState {
        message: message.into(),
    }
}

fn pending_mismatch(message: impl Into<String>) -> SqliteSinkError {
    SqliteSinkError::PendingInputMismatch {
        message: message.into(),
    }
}

impl From<PendingStateCodecError> for SqliteSinkError {
    fn from(error: PendingStateCodecError) -> Self {
        invalid_state(error.to_string())
    }
}

impl From<RowError> for SqliteSinkError {
    fn from(error: RowError) -> Self {
        Self::Row {
            message: error.to_string(),
        }
    }
}
