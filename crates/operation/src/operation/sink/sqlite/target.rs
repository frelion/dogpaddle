use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use arrow_schema::{DataType, Field, SchemaRef};
use dogpaddle_change::Change;
use rusqlite::{Connection, OpenFlags, ToSql, TransactionBehavior, params, params_from_iter};

#[cfg(test)]
use super::row::EncodedRow;
use super::{
    TECHNICAL_HASH, TECHNICAL_ID, definition::SqliteSinkSchemaError, error::SqliteSinkError,
    row::RowCodec,
};
use crate::operation::{
    OperationError,
    sink::{
        buffered::DeliveryBatch,
        relation::{
            Lookup, Matches, RelationTarget, decode_signed_id, encode_signed_id, plan,
            validate_technical_id,
        },
    },
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Lazily opened `SQLite` destination and its Schema-bound SQL.
pub(crate) struct SqliteTarget {
    database_path: PathBuf,
    row_codec: RowCodec,
    sql: SqlPlan,
    connection: Option<Connection>,
    verified: bool,
}

impl SqliteTarget {
    pub(super) fn try_new(
        database_path: PathBuf,
        table_name: String,
        input_schema: SchemaRef,
    ) -> Result<Self, SqliteSinkSchemaError> {
        let row_codec = RowCodec::new_validated(input_schema);
        let sql = SqlPlan::try_new(table_name, &row_codec)?;
        Ok(Self {
            database_path,
            row_codec,
            sql,
            connection: None,
            verified: false,
        })
    }

    #[cfg(test)]
    pub(super) fn encode_row(
        &self,
        change: &Change,
        row_index: usize,
    ) -> Result<EncodedRow, SqliteSinkError> {
        self.row_codec
            .encode_row(change.records(), row_index)
            .map_err(SqliteSinkError::from)
    }

    pub(super) fn require_absent(&mut self) -> Result<(), SqliteSinkError> {
        let (connection, sql) = self.parts(Instant::now() + BUSY_TIMEOUT)?;
        for name in [&sql.table_name, &sql.index_name, &sql.frontier_name] {
            if object_exists(connection, name)? {
                return Err(SqliteSinkError::TargetExists { name: name.clone() });
            }
        }
        Ok(())
    }

    fn verify_ready(&mut self, deadline: Instant) -> Result<(), SqliteSinkError> {
        if self.verified {
            return Ok(());
        }
        let (connection, sql) = self.parts(deadline)?;
        require_exact_layout(connection, sql)?;

        self.verified = true;
        Ok(())
    }

    pub(super) fn initialize(&mut self) -> Result<(), SqliteSinkError> {
        let deadline = Instant::now() + BUSY_TIMEOUT;
        let (connection, sql) = self.parts(deadline)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !object_exists(&transaction, &sql.table_name)? {
            if object_exists(&transaction, &sql.index_name)?
                || object_exists(&transaction, &sql.frontier_name)?
            {
                return Err(SqliteSinkError::TargetLayoutMismatch {
                    name: sql.table_name.clone(),
                });
            }
            transaction.execute(&sql.create_table, [])?;
            transaction.execute(&sql.create_index, [])?;
            transaction.execute(&sql.create_frontier, [])?;
            transaction.execute(&sql.initialize_frontier, [encode_signed_id(1)])?;
        }
        require_exact_layout(&transaction, sql)?;
        if transaction.query_row(&sql.has_rows, [], |row| row.get::<_, bool>(0))? {
            return Err(SqliteSinkError::TargetNotEmpty {
                table: sql.table_name.clone(),
            });
        }
        if read_frontier(&transaction, sql)? != 1 {
            return Err(super::error::invalid_batch(
                "initialization frontier is not one",
            ));
        }
        refresh_deadline(&transaction, deadline)?;
        transaction.commit()?;
        refresh_deadline(connection, deadline)?;
        self.verified = true;
        Ok(())
    }

    fn deliver(
        &mut self,
        input: &DeliveryBatch,
        tail: u64,
        original_head: (u64, &Change),
    ) -> Result<(), OperationError> {
        let deadline = Instant::now() + BUSY_TIMEOUT;
        self.verify_ready(deadline)?;
        self.parts(deadline)?;
        let Self {
            connection,
            sql,
            row_codec,
            ..
        } = self;
        let connection = connection.as_mut().expect("parts opened the connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteSinkError::from)?;
        let frontier = read_frontier(&transaction, sql)?;
        let end = input.end_event_offset()?;
        if frontier < input.first_event_offset() || frontier > tail || end > tail {
            return Err(super::error::invalid_batch(
                "target frontier is outside the loaded durable prefix",
            )
            .into());
        }
        if end <= frontier {
            refresh_deadline(&transaction, deadline)?;
            transaction.commit().map_err(SqliteSinkError::from)?;
            refresh_deadline(connection, deadline)?;
            return Ok(());
        }
        let batch = plan(input, frontier, tail, original_head, |requests| {
            lookup(
                &transaction,
                sql,
                row_codec,
                input.change(),
                requests,
                frontier - 1,
                deadline,
            )
            .map_err(Into::into)
        })?;
        for inserts in batch
            .inserts
            .chunk_by(|left, right| left.row_index == right.row_index)
        {
            refresh_deadline(&transaction, deadline)?;
            let row_index = usize::try_from(inserts[0].row_index)?;
            let row = row_codec
                .encode_row(input.change().records(), row_index)
                .map_err(SqliteSinkError::from)?;
            let mut statement = transaction
                .prepare_cached(&sql.insert)
                .map_err(SqliteSinkError::from)?;
            for insert in inserts {
                let id = technical_id_as_i64(insert.technical_id)?;
                let values = [&id as &dyn ToSql, &row.hash as &dyn ToSql]
                    .into_iter()
                    .chain(row.values.iter().map(|value| value as &dyn ToSql));
                refresh_deadline(&transaction, deadline)?;
                statement
                    .execute(params_from_iter(values))
                    .map_err(SqliteSinkError::from)?;
            }
        }
        let deletes = batch
            .deletes
            .iter()
            .map(|delete| technical_id_as_i64(delete.technical_id))
            .collect::<Result<Vec<_>, _>>()?;
        if !deletes.is_empty() {
            refresh_deadline(&transaction, deadline)?;
            let placeholders = std::iter::repeat_n("?", deletes.len())
                .collect::<Vec<_>>()
                .join(", ");
            let delete = format!("{}({placeholders})", sql.delete_prefix);
            transaction
                .prepare_cached(&delete)
                .map_err(SqliteSinkError::from)?
                .execute(params_from_iter(deletes))
                .map_err(SqliteSinkError::from)?;
        }
        refresh_deadline(&transaction, deadline)?;
        if transaction
            .execute(
                &sql.advance_frontier,
                params![encode_signed_id(end), encode_signed_id(frontier)],
            )
            .map_err(SqliteSinkError::from)?
            != 1
        {
            return Err(
                super::error::invalid_batch("locked frontier changed or disappeared").into(),
            );
        }
        refresh_deadline(&transaction, deadline)?;
        transaction.commit().map_err(SqliteSinkError::from)?;
        refresh_deadline(connection, deadline)?;
        Ok(())
    }

    fn parts(&mut self, deadline: Instant) -> Result<(&mut Connection, &SqlPlan), SqliteSinkError> {
        let Self {
            database_path,
            sql,
            connection,
            ..
        } = self;
        if connection.is_none() {
            *connection = Some(open_connection(database_path)?);
        }
        let connection = connection
            .as_mut()
            .expect("the SQLite connection was initialized above");
        refresh_deadline(connection, deadline)?;
        connection.progress_handler(1000, Some(move || Instant::now() >= deadline))?;
        Ok((connection, sql))
    }
}

impl RelationTarget for SqliteTarget {
    fn require_absent(&mut self) -> Result<(), OperationError> {
        Self::require_absent(self).map_err(OperationError::from)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        Self::initialize(self).map_err(OperationError::from)
    }

    fn deliver_prefix(
        &mut self,
        input: &DeliveryBatch,
        tail: u64,
        original_head: (u64, &Change),
    ) -> Result<(), OperationError> {
        let result = self.deliver(input, tail, original_head);
        if result.is_err() {
            self.connection = None;
            self.verified = false;
        }
        result
    }
}

struct SqlPlan {
    table_name: String,
    index_name: String,
    frontier_name: String,
    create_table: String,
    create_index: String,
    create_frontier: String,
    initialize_frontier: String,
    read_frontier: String,
    advance_frontier: String,
    insert: String,
    select_matching_ids: String,
    delete_prefix: String,
    has_rows: String,
}

impl SqlPlan {
    fn try_new(table_name: String, row_codec: &RowCodec) -> Result<Self, SqliteSinkSchemaError> {
        let quoted_table = quote_identifier(&table_name);
        let index_name = format!("$dogpaddle.hash_index.{table_name}");
        let quoted_index = quote_identifier(&index_name);
        let frontier_name = format!("$dogpaddle.frontier.{table_name}");
        let quoted_frontier = quote_identifier(&frontier_name);
        let quoted_id = quote_identifier(TECHNICAL_ID);
        let quoted_hash = quote_identifier(TECHNICAL_HASH);

        let mut definitions = vec![
            format!(
                "{quoted_id} INTEGER PRIMARY KEY CONSTRAINT \"$dogpaddle.event-prefix.v1\" \
                 CHECK({quoted_id} > {} AND {quoted_id} < {})",
                i64::MIN,
                i64::MAX
            ),
            format!(
                "{quoted_hash} BLOB NOT NULL CHECK(typeof({quoted_hash}) = 'blob' AND length({quoted_hash}) = 16)"
            ),
        ];
        for field in row_codec.schema().fields() {
            definitions.push(column_definition(field)?);
        }
        let create_table = format!(
            "CREATE TABLE {quoted_table} ({}) STRICT",
            definitions.join(", ")
        );
        let create_index = format!("CREATE INDEX {quoted_index} ON {quoted_table}({quoted_hash})");
        let create_frontier = format!(
            "CREATE TABLE {quoted_frontier} (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), next_event INTEGER NOT NULL CONSTRAINT \"$dogpaddle.frontier.v1\" CHECK(next_event > {})) STRICT",
            i64::MIN
        );
        let initialize_frontier = format!("INSERT INTO {quoted_frontier} VALUES (1, ?1)");
        let read_frontier = format!("SELECT singleton, next_event FROM {quoted_frontier} LIMIT 2");
        let advance_frontier = format!(
            "UPDATE {quoted_frontier} SET next_event = ?1 WHERE singleton = 1 AND next_event = ?2"
        );

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
            "INSERT INTO {quoted_table} ({}) VALUES ({placeholders})",
            columns.join(", ")
        );

        let row_columns = std::iter::once(quoted_hash)
            .chain(logical_columns)
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = (1..columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = format!("({row_columns}) IS ({row_placeholders})");
        let limit = columns.len();
        let select_matching_ids = format!(
            "SELECT {quoted_id} FROM {quoted_table} WHERE {predicate} ORDER BY {quoted_id} LIMIT ?{limit}"
        );

        let delete_prefix = format!("DELETE FROM {quoted_table} WHERE {quoted_id} IN ");
        let has_rows = format!("SELECT EXISTS(SELECT 1 FROM {quoted_table} LIMIT 1)");

        Ok(Self {
            table_name,
            index_name,
            frontier_name,
            create_table,
            create_index,
            create_frontier,
            initialize_frontier,
            read_frontier,
            advance_frontier,
            insert,
            select_matching_ids,
            delete_prefix,
            has_rows,
        })
    }
}

fn refresh_deadline(connection: &Connection, deadline: Instant) -> Result<(), SqliteSinkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
            None,
        )
        .into());
    }
    connection.busy_timeout(remaining)?;
    Ok(())
}

fn read_frontier(connection: &Connection, sql: &SqlPlan) -> Result<u64, SqliteSinkError> {
    let mut statement = connection.prepare_cached(&sql.read_frontier)?;
    let mut rows = statement.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| super::error::invalid_batch("owned frontier has no row"))?;
    let singleton: i64 = row.get(0)?;
    let next: i64 = row.get(1)?;
    if singleton != 1 || next == i64::MIN || rows.next()?.is_some() {
        return Err(super::error::invalid_batch(
            "owned frontier is not one valid singleton",
        ));
    }
    Ok(u64::from_ne_bytes(next.to_ne_bytes()) ^ (1_u64 << 63))
}

fn lookup(
    connection: &Connection,
    sql: &SqlPlan,
    codec: &RowCodec,
    input: &Change,
    requests: &[Lookup],
    through: u64,
    deadline: Instant,
) -> Result<Vec<Matches>, SqliteSinkError> {
    requests
        .iter()
        .map(|request| {
            refresh_deadline(connection, deadline)?;
            let encoded = codec.encode_row(input.records(), request.row_index)?;
            let take = i64::try_from(request.take).expect("the bounded batch limit fits i64");
            let values = std::iter::once(&encoded.hash as &dyn ToSql)
                .chain(encoded.values.iter().map(|value| value as &dyn ToSql))
                .chain(std::iter::once(&take as &dyn ToSql))
                .collect::<Vec<_>>();
            let mut statement = connection.prepare_cached(&sql.select_matching_ids)?;
            let mut rows = statement.query(values.as_slice())?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next()? {
                let id: i64 = row.get(0)?;
                ids.push(
                    decode_signed_id(id)
                        .map_err(|_| SqliteSinkError::InvalidStoredTechnicalId { id })?,
                );
            }
            Ok(Matches { through, ids })
        })
        .collect()
}

pub(super) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn column_definition(field: &Field) -> Result<String, SqliteSinkSchemaError> {
    let name = quote_identifier(field.name());
    if matches!(field.data_type(), DataType::Null) {
        return Ok(format!("{name} BLOB CHECK({name} IS NULL)"));
    }

    let (storage, check) = match field.data_type() {
        DataType::Boolean => (
            "INTEGER",
            format!("typeof({name}) = 'integer' AND {name} IN (0, 1)"),
        ),
        DataType::Int8 => ("INTEGER", integer_range(&name, i8::MIN, i8::MAX)),
        DataType::Int16 => ("INTEGER", integer_range(&name, i16::MIN, i16::MAX)),
        DataType::Int32 | DataType::Date32 => ("INTEGER", integer_range(&name, i32::MIN, i32::MAX)),
        DataType::Int64 | DataType::Timestamp(_, _) => {
            ("INTEGER", format!("typeof({name}) = 'integer'"))
        }
        DataType::UInt8 => ("INTEGER", unsigned_range(&name, u8::MAX)),
        DataType::UInt16 => ("INTEGER", unsigned_range(&name, u16::MAX)),
        DataType::UInt32 => ("INTEGER", unsigned_range(&name, u32::MAX)),
        DataType::UInt64 | DataType::Float64 => ("BLOB", blob_check(&name, Some(8))),
        DataType::Float32 => ("BLOB", blob_check(&name, Some(4))),
        DataType::Decimal128(_, _) => ("BLOB", blob_check(&name, Some(16))),
        DataType::Utf8 => ("TEXT COLLATE BINARY", format!("typeof({name}) = 'text'")),
        DataType::Binary | DataType::List(_) | DataType::Struct(_) => {
            ("BLOB", blob_check(&name, None))
        }
        DataType::Null => unreachable!("handled above"),
        unsupported => {
            return Err(SqliteSinkSchemaError::UnsupportedType {
                field: field.name().clone(),
                data_type: unsupported.clone(),
            });
        }
    };
    let nullability = if field.is_nullable() {
        format!(" CHECK({name} IS NULL OR ({check}))")
    } else {
        format!(" NOT NULL CHECK({check})")
    };
    Ok(format!("{name} {storage}{nullability}"))
}

fn integer_range<T: std::fmt::Display>(name: &str, min: T, max: T) -> String {
    format!("typeof({name}) = 'integer' AND {name} BETWEEN {min} AND {max}")
}

fn unsigned_range<T: std::fmt::Display>(name: &str, max: T) -> String {
    format!("typeof({name}) = 'integer' AND {name} BETWEEN 0 AND {max}")
}

fn blob_check(name: &str, length: Option<usize>) -> String {
    if let Some(length) = length {
        format!("typeof({name}) = 'blob' AND length({name}) = {length}")
    } else {
        format!("typeof({name}) = 'blob'")
    }
}

fn open_connection(path: &std::path::Path) -> Result<Connection, SqliteSinkError> {
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
         WHERE (tbl_name = ?1 COLLATE NOCASE OR tbl_name = ?2 COLLATE NOCASE) \
         AND type IN ('table', 'index', 'trigger') \
         ORDER BY type COLLATE BINARY, name COLLATE BINARY",
    )?;
    let mut rows = statement.query(params![&sql.table_name, &sql.frontier_name])?;
    let mut found_table = false;
    let mut found_index = false;
    let mut found_frontier = false;
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
            "table" if name == sql.frontier_name => {
                found_frontier = true;
                if definition.as_deref() != Some(sql.create_frontier.as_str()) {
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
    if !found_frontier {
        return Err(SqliteSinkError::TargetMissing {
            name: sql.frontier_name.clone(),
        });
    }
    Ok(())
}

fn technical_id_as_i64(technical_id: u64) -> Result<i64, SqliteSinkError> {
    validate_technical_id(technical_id)
        .map_err(|error| super::error::invalid_batch(error.to_string()))?;
    Ok(encode_signed_id(technical_id))
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use arrow_schema::Schema;
    use std::sync::Arc;

    #[test]
    fn statement_deadline_interrupts_cpu_work_and_next_action_gets_a_fresh_deadline() {
        let root = tempfile::tempdir().unwrap();
        let mut target = SqliteTarget::try_new(
            root.path().join("target.sqlite"),
            "rows".into(),
            Arc::new(Schema::empty()),
        )
        .unwrap();
        let (connection, _) = target.parts(Instant::now() + BUSY_TIMEOUT).unwrap();
        let error = connection.query_row("WITH RECURSIVE forever(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM forever) SELECT count(*) FROM forever", [], |row| row.get::<_, i64>(0)).unwrap_err();
        assert!(
            matches!(error, rusqlite::Error::SqliteFailure(code, _) if code.code == rusqlite::ErrorCode::OperationInterrupted)
        );
        let (connection, _) = target.parts(Instant::now() + BUSY_TIMEOUT).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT 42", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            42
        );
    }
}
