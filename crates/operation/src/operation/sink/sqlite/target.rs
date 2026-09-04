use std::{collections::HashSet, path::PathBuf, time::Duration};

use arrow_schema::{DataType, Field};
use dogpaddle_change::Change;
use rusqlite::{
    Connection, OpenFlags, ToSql, Transaction, TransactionBehavior, params, params_from_iter,
    types::ValueRef,
};

use super::{
    TECHNICAL_HASH, TECHNICAL_ID,
    definition::SqliteSinkSchemaError,
    error::SqliteSinkError,
    row::{EncodedRow, RowCodec},
    state::{Mutation, MutationKind},
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Lazily opened `SQLite` destination and its Schema-bound SQL.
pub(super) struct SqliteTarget {
    database_path: PathBuf,
    sql: SqlPlan,
    connection: Option<Connection>,
}

impl SqliteTarget {
    pub(super) fn try_new(
        database_path: PathBuf,
        table_name: String,
        row_codec: &RowCodec,
    ) -> Result<Self, SqliteSinkSchemaError> {
        Ok(Self {
            database_path,
            sql: SqlPlan::try_new(table_name, row_codec)?,
            connection: None,
        })
    }

    pub(super) fn require_absent(&mut self) -> Result<(), SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        for name in [&sql.table_name, &sql.index_name] {
            if object_exists(connection, name)? {
                return Err(SqliteSinkError::TargetExists { name: name.clone() });
            }
        }
        Ok(())
    }

    pub(super) fn verify_ready(&mut self, next_id: u64) -> Result<(), SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        require_exact_layout(connection, sql)?;

        let (minimum, maximum) = technical_id_bounds(connection, sql)?;
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

    pub(super) fn matching_ids(
        &mut self,
        encoded: &EncodedRow,
        excluded: &HashSet<u64>,
        scan_limit: u64,
        select_limit: usize,
    ) -> Result<MatchingIds, SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        let mut selected = Vec::with_capacity(select_limit);
        let mut count = 0_u64;
        let mut statement = connection.prepare_cached(&sql.select_by_hash)?;
        let mut rows = statement.query(params![encoded.hash.as_slice()])?;
        while count < scan_limit {
            let Some(row) = rows.next()? else {
                break;
            };
            let id: i64 = row.get(0)?;
            if id <= 0 {
                return Err(SqliteSinkError::InvalidStoredTechnicalId { id });
            }
            let id = u64::try_from(id).expect("a positive SQLite INTEGER fits u64");
            if excluded.contains(&id) || !encoded.matches(row, 1)? {
                continue;
            }
            count += 1;
            if selected.len() < select_limit {
                selected.push(id);
            }
        }
        Ok(MatchingIds { count, selected })
    }

    pub(super) fn begin(&mut self) -> Result<SqliteTargetTransaction<'_>, SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(SqliteTargetTransaction { transaction, sql })
    }

    fn parts(&mut self) -> Result<(&mut Connection, &SqlPlan), SqliteSinkError> {
        let Self {
            database_path,
            sql,
            connection,
        } = self;
        if connection.is_none() {
            *connection = Some(open_connection(database_path)?);
        }
        Ok((
            connection
                .as_mut()
                .expect("the SQLite connection was initialized above"),
            sql,
        ))
    }
}

pub(super) struct MatchingIds {
    pub(super) count: u64,
    pub(super) selected: Vec<u64>,
}

/// One external transaction whose commit remains last in the operation turn.
pub(super) struct SqliteTargetTransaction<'connection> {
    transaction: Transaction<'connection>,
    sql: &'connection SqlPlan,
}

impl SqliteTargetTransaction<'_> {
    pub(super) fn initialize(&self) -> Result<(), SqliteSinkError> {
        if !object_exists(&self.transaction, &self.sql.table_name)? {
            if object_exists(&self.transaction, &self.sql.index_name)? {
                return Err(SqliteSinkError::TargetLayoutMismatch {
                    name: self.sql.index_name.clone(),
                });
            }
            self.transaction.execute(&self.sql.create_table, [])?;
            self.transaction.execute(&self.sql.create_index, [])?;
        }
        require_exact_layout(&self.transaction, self.sql)?;
        if technical_id_bounds(&self.transaction, self.sql)? != (None, None) {
            return Err(SqliteSinkError::TargetNotEmpty {
                table: self.sql.table_name.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn apply(
        &self,
        row_codec: &RowCodec,
        change: &Change,
        mutations: &[Mutation],
    ) -> Result<(), SqliteSinkError> {
        for row_mutations in mutations.chunk_by(|left, right| left.row_index == right.row_index) {
            let row_index = usize::try_from(row_mutations[0].row_index).map_err(|_| {
                super::error::pending_mismatch("mutation row index cannot be represented by usize")
            })?;
            let encoded = row_codec.encode_row(change.records(), row_index)?;
            for mutation in row_mutations {
                match mutation.kind {
                    MutationKind::Insert => {
                        self.apply_insert(mutation.technical_id, &encoded)?;
                    }
                    MutationKind::Delete => {
                        self.apply_delete(mutation.technical_id, &encoded)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn commit(self) -> Result<(), SqliteSinkError> {
        self.transaction.commit().map_err(SqliteSinkError::from)
    }

    fn apply_insert(&self, technical_id: u64, encoded: &EncodedRow) -> Result<(), SqliteSinkError> {
        let id = technical_id_as_i64(technical_id)?;
        let technical_values = [&id as &dyn ToSql, &encoded.hash as &dyn ToSql];
        let values = technical_values
            .into_iter()
            .chain(encoded.values.iter().map(|value| value as &dyn ToSql));
        let actual = self
            .transaction
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
        match self.row_by_id_matches(id, encoded)? {
            Some(true) => Ok(()),
            Some(false) | None => Err(SqliteSinkError::TechnicalIdConflict { id: technical_id }),
        }
    }

    fn apply_delete(&self, technical_id: u64, encoded: &EncodedRow) -> Result<(), SqliteSinkError> {
        let id = technical_id_as_i64(technical_id)?;
        match self.row_by_id_matches(id, encoded)? {
            None => return Ok(()),
            Some(false) => return Err(SqliteSinkError::DeleteRowMismatch { id: technical_id }),
            Some(true) => {}
        }
        let actual = self
            .transaction
            .execute(&self.sql.delete_by_id, params![id])?;
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
        id: i64,
        encoded: &EncodedRow,
    ) -> Result<Option<bool>, SqliteSinkError> {
        let mut statement = self.transaction.prepare_cached(&self.sql.select_by_id)?;
        let mut rows = statement.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(
            stored_hash_matches(row.get_ref(0)?, &encoded.hash) && encoded.matches(row, 1)?,
        ))
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
    fn try_new(table_name: String, row_codec: &RowCodec) -> Result<Self, SqliteSinkSchemaError> {
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
        for field in row_codec.schema().fields() {
            definitions.push(column_definition(field)?);
        }
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

        Ok(Self {
            table_name,
            index_name,
            create_table,
            create_index,
            insert,
            select_by_hash,
            select_by_id,
            delete_by_id,
            technical_id_bounds,
        })
    }
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

fn technical_id_as_i64(technical_id: u64) -> Result<i64, SqliteSinkError> {
    i64::try_from(technical_id).map_err(|_| {
        super::error::invalid_state(format!(
            "technical ID {technical_id} cannot be represented by SQLite INTEGER"
        ))
    })
}

fn stored_hash_matches(actual: ValueRef<'_>, expected: &[u8; 16]) -> bool {
    matches!(actual, ValueRef::Blob(bytes) if bytes == expected)
}
