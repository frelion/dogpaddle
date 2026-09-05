use std::{path::PathBuf, time::Duration};

use arrow_schema::{DataType, Field, SchemaRef};
use dogpaddle_change::Change;
use rusqlite::{Connection, OpenFlags, ToSql, TransactionBehavior, params, params_from_iter};

use super::{
    TECHNICAL_HASH, TECHNICAL_ID,
    definition::SqliteSinkSchemaError,
    error::SqliteSinkError,
    row::{EncodedRow, RowCodec},
};
use crate::operation::{
    OperationError,
    sink::relation::{Batch, Lookup, Matches, RelationTarget},
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
        let (connection, sql) = self.parts()?;
        for name in [&sql.table_name, &sql.index_name] {
            if object_exists(connection, name)? {
                return Err(SqliteSinkError::TargetExists { name: name.clone() });
            }
        }
        Ok(())
    }

    fn verify_ready(&mut self) -> Result<(), SqliteSinkError> {
        if self.verified {
            return Ok(());
        }
        let (connection, sql) = self.parts()?;
        require_exact_layout(connection, sql)?;

        self.verified = true;
        Ok(())
    }

    fn matching_ids(
        &mut self,
        encoded: &EncodedRow,
        scan_limit: u64,
        select_limit: usize,
    ) -> Result<Matches, SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        let select_limit = i64::try_from(select_limit).expect("the bounded batch limit fits i64");
        let mut values = std::iter::once(&encoded.hash as &dyn ToSql)
            .chain(encoded.values.iter().map(|value| value as &dyn ToSql))
            .collect::<Vec<_>>();
        let count_limit = i64::try_from(scan_limit).unwrap_or(i64::MAX);
        values.push(&select_limit);
        let mut statement = connection.prepare_cached(&sql.select_matching_ids)?;
        let mut rows = statement.query(values.as_slice())?;
        let mut selected = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            if id <= 0 {
                return Err(SqliteSinkError::InvalidStoredTechnicalId { id });
            }
            let id = u64::try_from(id).expect("a positive SQLite INTEGER fits u64");
            selected.push(id);
        }
        let selected_count = u64::try_from(selected.len()).expect("the bounded result fits u64");
        let count = if selected_count == select_limit.unsigned_abs() && scan_limit > selected_count
        {
            *values.last_mut().expect("the limit parameter was appended") = &count_limit;
            let count = connection
                .prepare_cached(&sql.count_matches)?
                .query_row(values.as_slice(), |row| row.get::<_, i64>(0))?;
            u64::try_from(count).expect("SQLite COUNT returns a nonnegative integer")
        } else {
            selected_count
        };
        Ok(Matches {
            count,
            ids: selected,
        })
    }

    pub(super) fn initialize(&mut self) -> Result<(), SqliteSinkError> {
        let (connection, sql) = self.parts()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !object_exists(&transaction, &sql.table_name)? {
            if object_exists(&transaction, &sql.index_name)? {
                return Err(SqliteSinkError::TargetLayoutMismatch {
                    name: sql.index_name.clone(),
                });
            }
            transaction.execute(&sql.create_table, [])?;
            transaction.execute(&sql.create_index, [])?;
        }
        require_exact_layout(&transaction, sql)?;
        if transaction.query_row(&sql.has_rows, [], |row| row.get::<_, bool>(0))? {
            return Err(SqliteSinkError::TargetNotEmpty {
                table: sql.table_name.clone(),
            });
        }
        transaction.commit()?;
        self.verified = true;
        Ok(())
    }

    fn write(&mut self, change: &Change, batch: &Batch) -> Result<(), SqliteSinkError> {
        self.verify_ready()?;
        let mut inserts = Vec::new();
        for group in batch
            .inserts
            .chunk_by(|left, right| left.row_index == right.row_index)
        {
            let row_index = usize::try_from(group[0].row_index).map_err(|_| {
                super::error::invalid_batch("mutation row index cannot be represented by usize")
            })?;
            let ids = group
                .iter()
                .map(|insert| technical_id_as_i64(insert.technical_id))
                .collect::<Result<Vec<_>, _>>()?;
            inserts.push((ids, self.encode_row(change, row_index)?));
        }
        let deletes = batch
            .deletes
            .iter()
            .map(|id| technical_id_as_i64(*id))
            .collect::<Result<Vec<_>, _>>()?;
        let (connection, sql) = self.parts()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (ids, encoded) in &inserts {
            let mut statement = transaction.prepare_cached(&sql.insert)?;
            for id in ids {
                let values = [id as &dyn ToSql, &encoded.hash as &dyn ToSql]
                    .into_iter()
                    .chain(encoded.values.iter().map(|value| value as &dyn ToSql));
                statement.execute(params_from_iter(values))?;
            }
        }
        if !deletes.is_empty() {
            let placeholders = std::iter::repeat_n("?", deletes.len())
                .collect::<Vec<_>>()
                .join(", ");
            let delete = format!("{}({placeholders})", sql.delete_prefix);
            transaction
                .prepare_cached(&delete)?
                .execute(params_from_iter(deletes))?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn parts(&mut self) -> Result<(&mut Connection, &SqlPlan), SqliteSinkError> {
        let Self {
            database_path,
            sql,
            connection,
            ..
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

impl RelationTarget for SqliteTarget {
    fn require_absent(&mut self) -> Result<(), OperationError> {
        Self::require_absent(self).map_err(OperationError::from)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        Self::initialize(self).map_err(OperationError::from)
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        self.verify_ready()?;
        requests
            .iter()
            .map(|request| {
                let encoded = self.encode_row(input, request.row_index)?;
                self.matching_ids(&encoded, request.needed, request.take)
                    .map_err(OperationError::from)
            })
            .collect()
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        self.write(input, batch).map_err(OperationError::from)
    }
}

struct SqlPlan {
    table_name: String,
    index_name: String,
    create_table: String,
    create_index: String,
    insert: String,
    select_matching_ids: String,
    count_matches: String,
    delete_prefix: String,
    has_rows: String,
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
        let count_matches = format!(
            "SELECT COUNT(*) FROM (SELECT 1 FROM {quoted_table} WHERE {predicate} LIMIT ?{limit})"
        );
        let delete_prefix = format!("DELETE FROM {quoted_table} WHERE {quoted_id} IN ");
        let has_rows = format!("SELECT EXISTS(SELECT 1 FROM {quoted_table} LIMIT 1)");

        Ok(Self {
            table_name,
            index_name,
            create_table,
            create_index,
            insert,
            select_matching_ids,
            count_matches,
            delete_prefix,
            has_rows,
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

fn technical_id_as_i64(technical_id: u64) -> Result<i64, SqliteSinkError> {
    i64::try_from(technical_id).map_err(|_| {
        super::error::invalid_batch(format!(
            "technical ID {technical_id} cannot be represented by SQLite INTEGER"
        ))
    })
}
