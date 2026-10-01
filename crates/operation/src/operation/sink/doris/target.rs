use arrow_schema::Schema;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    time::{Duration, Instant},
};

use dogpaddle_change::Change;
use mysql::{Conn, Params, Value, params, prelude::Queryable};

use super::{
    config::{DorisSinkConfig, require_absent},
    definition::DorisSinkDefinition,
    error::{DorisSinkError, database, invalid_batch},
    row::{DorisRowCodec, EncodedRow},
    schema::{
        self, PUBLIC_TECHNICAL_HASH, PUBLIC_TECHNICAL_ID, TECHNICAL_HASH, TECHNICAL_HASH_INDEX,
        TECHNICAL_ID, TECHNICAL_VERSION,
    },
};
use crate::operation::{
    OperationError,
    sink::buffered::DeliveryBatch,
    sink::relation::{
        Batch, Lookup, Matches, RelationTarget, decode_signed_id, encode_signed_id, plan,
        relation_event_bytes, terminal_mutations, validate_technical_id,
    },
};

const MAX_LOOKUP_CLAUSES: usize = 128;
const MAX_LOOKUP_PARAMETERS: usize = 65_535;
const MAX_LOOKUP_SQL_BYTES: usize = 4 * 1024 * 1024;
const MAX_WRITE_SQL_BYTES: usize = 4 * 1024 * 1024;
const MAX_WRITE_VALUES: usize = 10_000;
const WORK_UNIT_TIMEOUT: Duration = Duration::from_secs(5);

// The synchronous driver has socket inactivity timeouts. This shared budget
// rejects late success; it cannot interrupt an in-progress protocol read.
fn before(deadline: Instant) -> Result<(), DorisSinkError> {
    if Instant::now() >= deadline {
        Err(database("deliver input prefix"))
    } else {
        Ok(())
    }
}

fn execute_visible(connection: &mut Conn, sql: &str) -> Result<(), DorisSinkError> {
    let mut result = connection
        .query_iter(sql)
        .map_err(|_| database("publish relation batch"))?;
    if !result.columns().as_ref().is_empty() || result.info_ref().len() > 1024 {
        return Err(database("verify visible commit"));
    }
    let info = result.info_ref().to_vec();
    if result.next().is_some() {
        return Err(database("verify visible commit"));
    }
    drop(result);
    require_visible(&info, connection.info_ref())
}

fn ok_token(input: &mut &[u8], token: &[u8]) -> Result<(), DorisSinkError> {
    *input = input.trim_ascii_start();
    *input = input
        .strip_prefix(token)
        .ok_or_else(|| database("verify visible commit"))?;
    Ok(())
}

fn ok_quoted<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], DorisSinkError> {
    ok_token(input, b"'")?;
    let end = input
        .iter()
        .position(|byte| *byte == b'\'')
        .ok_or_else(|| database("verify visible commit"))?;
    let value = &input[..end];
    *input = &input[end + 1..];
    Ok(value)
}

// Doris' pinned OK envelope uses single quotes. Accept exactly its three fields;
// COMMITTED, PREPARE, an empty COMMIT or malformed data cannot authorize settle.
fn require_visible(mut input: &[u8], connection_info: &[u8]) -> Result<(), DorisSinkError> {
    if input != connection_info || input.len() > 1024 {
        return Err(database("verify visible commit"));
    }
    ok_token(&mut input, b"{")?;
    ok_token(&mut input, b"'label'")?;
    ok_token(&mut input, b":")?;
    let label = ok_quoted(&mut input)?;
    if label.is_empty()
        || !label
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(byte))
    {
        return Err(database("verify visible commit"));
    }
    ok_token(&mut input, b",")?;
    ok_token(&mut input, b"'status'")?;
    ok_token(&mut input, b":")?;
    if ok_quoted(&mut input)? != b"VISIBLE" {
        return Err(database("verify visible commit"));
    }
    ok_token(&mut input, b",")?;
    ok_token(&mut input, b"'txnId'")?;
    ok_token(&mut input, b":")?;
    let id = ok_quoted(&mut input)?;
    if !id.first().is_some_and(|byte| matches!(*byte, b'1'..=b'9'))
        || !id.iter().all(u8::is_ascii_digit)
        || std::str::from_utf8(id)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .is_none()
    {
        return Err(database("verify visible commit"));
    }
    ok_token(&mut input, b"}")?;
    if !input.trim_ascii().is_empty() {
        return Err(database("verify visible commit"));
    }
    Ok(())
}

pub(super) struct DorisTarget {
    config: DorisSinkConfig,
    spec: DorisSinkDefinition,
    codec: DorisRowCodec,
    connection: Option<Conn>,
    verified: bool,
}

impl DorisTarget {
    pub(super) fn new_bound(
        config: DorisSinkConfig,
        spec: DorisSinkDefinition,
        codec: DorisRowCodec,
    ) -> Self {
        Self {
            config,
            spec,
            codec,
            connection: None,
            verified: false,
        }
    }

    fn connect(&mut self, deadline: Instant) -> Result<&mut Conn, DorisSinkError> {
        if self.config.database() != self.spec.database() {
            return Err(DorisSinkError::DatabaseMismatch);
        }
        if self.connection.is_none() {
            let mut connection = self.config.connect()?;
            for (name, value) in [
                ("enable_strong_consistency_read", "true"),
                ("enable_sql_cache", "false"),
                ("enable_query_cache", "false"),
                ("skip_missing_version", "false"),
                ("skip_bad_tablet", "false"),
                ("group_commit", "off_mode"),
                ("enable_insert_strict", "true"),
                ("query_timeout", "5"),
                ("insert_timeout", "5"),
                ("exec_mem_limit", "67108864"),
            ] {
                before(deadline)?;
                connection
                    .query_drop(format!("SET {name} = '{value}'"))
                    .map_err(|_| database("set session guarantees"))?;
                let rows: Vec<mysql::Row> = connection
                    .query(format!("SHOW VARIABLES LIKE '{name}'"))
                    .map_err(|_| database("verify session guarantees"))?;
                if rows.len() != 1
                    || rows[0].get::<String, _>(0).as_deref() != Some(name)
                    || rows[0].get::<String, _>(1).as_deref() != Some(value)
                {
                    return Err(database("verify session guarantees"));
                }
            }
            let ids: Vec<u64> = connection
                .query("SELECT DISTINCT ClusterId FROM frontends()")
                .map_err(|_| database("read cluster identity"))?;
            if ids.as_slice() != [self.spec.cluster_id()] {
                return Err(DorisSinkError::TargetIdentityChanged);
            }
            before(deadline)?;
            self.connection = Some(connection);
            self.verified = false;
        }
        Ok(self.connection.as_mut().expect("connection was installed"))
    }

    fn ensure_ready(&mut self, deadline: Instant) -> Result<(), DorisSinkError> {
        self.connect(deadline)?;
        if !self.verified {
            let mut connection = self.connection.take().expect("connection was installed");
            let result = verify_state(&mut connection, &self.spec, self.codec.schema())
                .and_then(|()| verify_view(&mut connection, &self.spec, self.codec.schema()));
            result?;
            self.connection = Some(connection);
            self.verified = true;
        }
        Ok(())
    }
}

impl RelationTarget for DorisTarget {
    fn event_bytes(&self, input: &Change, row_index: usize) -> Result<u64, OperationError> {
        let baseline = relation_event_bytes(input, row_index)?;
        let row = self.codec.encode_row(input.records(), row_index)?;
        // ID 1 maps to i64::MIN + 1: the longest valid signed BIGINT literal.
        let encoded = mutation_values(1, 1, &row)
            .len()
            .checked_add(insert_prefix(&self.spec, self.codec.schema()).len())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| invalid_batch("Doris event byte charge exceeds u64"))?;
        Ok(baseline.max(encoded))
    }

    fn require_absent(&mut self) -> Result<(), OperationError> {
        let deadline = Instant::now() + WORK_UNIT_TIMEOUT;
        let spec = self.spec.clone();
        let result = (|| {
            require_absent(self.connect(deadline)?, &spec)?;
            before(deadline)
        })();
        if result.is_err() {
            self.connection = None;
            self.verified = false;
        }
        result.map_err(Into::into)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        let deadline = Instant::now() + WORK_UNIT_TIMEOUT;
        self.connect(deadline)?;
        let mut connection = self.connection.take().expect("connection was installed");
        let result = self
            .initialize_with(&mut connection)
            .and_then(|()| before(deadline));
        if result.is_ok() {
            self.connection = Some(connection);
            self.verified = true;
        } else {
            self.verified = false;
        }
        result.map_err(Into::into)
    }

    fn deliver_prefix(
        &mut self,
        input: &DeliveryBatch,
        tail: u64,
        original_head: (u64, &Change),
    ) -> Result<(), OperationError> {
        let deadline = Instant::now() + WORK_UNIT_TIMEOUT;
        let result = (|| {
            self.ensure_ready(deadline)?;
            before(deadline)?;
            let batch = plan(
                input,
                input.first_event_offset(),
                tail,
                original_head,
                |requests| self.lookup(input.change(), requests, deadline),
            )?;
            before(deadline)?;
            self.write_relation_batch(input.change(), &batch, tail, deadline)?;
            before(deadline)?;
            Ok(())
        })();
        if result.is_err() {
            self.connection = None;
            self.verified = false;
        }
        result
    }
}

impl DorisTarget {
    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
        deadline: Instant,
    ) -> Result<Vec<Matches>, OperationError> {
        let mut output = Vec::with_capacity(requests.len());
        let mut clauses = Vec::new();
        let mut parameters = Vec::new();
        let mut sql_bytes = 0_usize;
        for (request_index, request) in requests.iter().enumerate() {
            let row = self.codec.encode_row(input.records(), request.row_index)?;
            let (predicate, row_parameters) = row_predicate(self.codec.schema(), &row);
            let clause = lookup_clause(&self.spec, request_index, request, &predicate);
            if !clauses.is_empty()
                && (clauses.len() == MAX_LOOKUP_CLAUSES
                    || parameters
                        .len()
                        .saturating_add(row_parameters.len().saturating_mul(2))
                        > MAX_LOOKUP_PARAMETERS
                    || sql_bytes.saturating_add(clause.len()) > MAX_LOOKUP_SQL_BYTES)
            {
                if Instant::now() >= deadline {
                    return Err(database("match target rows").into());
                }
                self.read_lookup_clauses(
                    &clauses,
                    std::mem::take(&mut parameters),
                    &mut output,
                    deadline,
                )?;
                clauses.clear();
                sql_bytes = 0;
            }
            parameters.extend(row_parameters.iter().cloned());
            parameters.extend(row_parameters);
            sql_bytes = sql_bytes.saturating_add(clause.len());
            clauses.push(clause);
        }
        if !clauses.is_empty() {
            if Instant::now() >= deadline {
                return Err(database("match target rows").into());
            }
            self.read_lookup_clauses(&clauses, parameters, &mut output, deadline)?;
        }
        if output.len() != requests.len() {
            return Err(database("decode matching rows").into());
        }
        Ok(output)
    }

    fn read_lookup_clauses(
        &mut self,
        clauses: &[String],
        parameters: Vec<Value>,
        output: &mut Vec<Matches>,
        deadline: Instant,
    ) -> Result<(), DorisSinkError> {
        before(deadline)?;
        let expected_rows = output.len().saturating_add(clauses.len());
        let sql = format!("{} ORDER BY n, kind, value", clauses.join(" UNION ALL "));
        let rows: Vec<(u64, u8, Option<i64>)> = self
            .connect(deadline)?
            .exec(sql, Params::Positional(parameters))
            .map_err(|_| database("match target rows"))?;
        before(deadline)?;
        for (request_index, kind, value) in rows {
            let request_index = usize::try_from(request_index)
                .map_err(|_| database("decode matching request index"))?;
            if kind == 0 && request_index == output.len() {
                let through = value
                    .map(decode_signed_id)
                    .transpose()
                    .map_err(|error| invalid_batch(error.to_string()))?
                    .unwrap_or(0);
                output.push(Matches {
                    through,
                    ids: Vec::new(),
                });
            } else if kind == 1 && request_index.checked_add(1) == Some(output.len()) {
                let id = value.ok_or_else(|| database("decode matching ID"))?;
                output
                    .last_mut()
                    .expect("a matching request is installed")
                    .ids
                    .push(decode_signed_id(id).map_err(|error| invalid_batch(error.to_string()))?);
            } else {
                return Err(database("decode matching request order"));
            }
        }
        if output.len() != expected_rows {
            return Err(database("decode matching rows"));
        }
        Ok(())
    }

    fn initialize_with(&self, connection: &mut Conn) -> Result<(), DorisSinkError> {
        let state = self.spec.state_table();
        let target = self.spec.table().to_owned();
        let objects = object_kinds(connection, &self.spec)?;
        match (objects.get(&state), objects.get(&target)) {
            (None, None) => {
                let create = create_state_sql(&self.spec, self.codec.schema());
                connection
                    .query_drop(create)
                    .map_err(|_| database("create state table"))?;
            }
            (Some(kind), None) if kind == "BASE TABLE" => {}
            (Some(_), None) => {
                return Err(DorisSinkError::TargetLayoutMismatch { name: state });
            }
            (None, Some(_)) => {
                return Err(DorisSinkError::TargetMissing { name: state });
            }
            (Some(_), Some(_)) => {}
        }
        verify_state(connection, &self.spec, self.codec.schema())?;
        let objects = object_kinds(connection, &self.spec)?;
        if !objects.contains_key(&target) {
            connection
                .query_drop(create_view_sql(&self.spec, self.codec.schema()))
                .map_err(|_| database("create target view"))?;
        }
        verify_view(connection, &self.spec, self.codec.schema())?;
        let count: Option<u64> = connection
            .query_first(format!(
                "SELECT count(*) FROM {}",
                qualified(self.spec.database(), &state)
            ))
            .map_err(|_| database("verify empty state table"))?;
        if count != Some(0) {
            return Err(DorisSinkError::TargetNotEmpty);
        }
        Ok(())
    }

    fn write_relation_batch(
        &mut self,
        input: &Change,
        batch: &Batch,
        tail: u64,
        deadline: Instant,
    ) -> Result<(), DorisSinkError> {
        let terminal = terminal_mutations(batch);
        let existing = self.read_ids(
            terminal.iter().map(|mutation| mutation.technical_id),
            tail,
            deadline,
        )?;
        let mut actions = Vec::new();
        for mutation in terminal {
            let id = mutation.technical_id;
            let version = mutation.version;
            let row_index = usize::try_from(mutation.row_index)
                .map_err(|_| invalid_batch("row index exceeds usize"))?;
            let row = self.codec.encode_row(input.records(), row_index)?;
            if let Some(actual) = existing.get(&id) {
                if !same_row(actual, &row) {
                    return Err(invalid_batch(format!(
                        "technical ID {id} is bound to another logical row"
                    )));
                }
                if actual.version >= version {
                    continue;
                }
            }
            actions.push((id, version, row));
        }
        if actions.is_empty() {
            return Ok(());
        }
        let statements = insert_statements(&self.spec, self.codec.schema(), &actions);
        let transactional = statements.len() > 1;
        before(deadline)?;
        let connection = self.connect(deadline)?;
        if transactional {
            connection
                .query_drop("BEGIN")
                .map_err(|_| database("begin relation batch"))?;
            for statement in statements {
                before(deadline)?;
                connection
                    .query_drop(statement)
                    .map_err(|_| database("apply relation batch"))?;
            }
            before(deadline)?;
            execute_visible(connection, "COMMIT")?;
        } else if let Some(statement) = statements.into_iter().next() {
            execute_visible(connection, &statement)?;
        }
        before(deadline)?;
        Ok(())
    }
}

#[derive(Debug)]
struct StoredRow {
    hash: Vec<u8>,
    version: u64,
    values: Vec<Value>,
}

impl DorisTarget {
    fn read_ids(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
        tail: u64,
        deadline: Instant,
    ) -> Result<BTreeMap<u64, StoredRow>, DorisSinkError> {
        let ids = ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let columns = selected_columns(self.codec.schema());
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT {columns} FROM {} WHERE {} IN ({placeholders}) ORDER BY {}",
            qualified(self.spec.database(), &self.spec.state_table()),
            quote(TECHNICAL_ID),
            quote(TECHNICAL_ID)
        );
        let parameters = ids
            .into_iter()
            .map(|id| {
                validate_technical_id(id).map_err(|error| invalid_batch(error.to_string()))?;
                Ok(Value::Int(encode_signed_id(id)))
            })
            .collect::<Result<Vec<_>, DorisSinkError>>()?;
        let rows: Vec<mysql::Row> = self
            .connect(deadline)?
            .exec(sql, Params::Positional(parameters))
            .map_err(|_| database("read mutation IDs"))?;
        let mut output = BTreeMap::new();
        for row in rows {
            let mut values = row.unwrap().into_iter();
            let id = value_id(
                values
                    .next()
                    .ok_or_else(|| database("decode mutation ID"))?,
            )?;
            let hash = value_bytes(values.next().ok_or_else(|| database("decode row hash"))?)?;
            let version = value_id(
                values
                    .next()
                    .ok_or_else(|| database("decode occurrence version"))?,
            )?;
            if version < id || version >= tail {
                return Err(invalid_batch(
                    "stored version is outside the delivery domain",
                ));
            }
            let stored = StoredRow {
                hash,
                version,
                values: values.collect(),
            };
            if output.insert(id, stored).is_some() {
                return Err(DorisSinkError::TargetLayoutMismatch {
                    name: self.spec.state_table(),
                });
            }
        }
        Ok(output)
    }
}

fn same_row(actual: &StoredRow, expected: &EncodedRow) -> bool {
    actual.hash == expected.hash
        && actual.values.len() == expected.values.len()
        && actual
            .values
            .iter()
            .zip(&expected.values)
            .all(|(left, right)| values_equal(left, right))
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::UInt(right)) | (Value::UInt(right), Value::Int(left)) => {
            u64::try_from(*left).ok() == Some(*right)
        }
        _ => left == right,
    }
}

fn value_id(value: Value) -> Result<u64, DorisSinkError> {
    let signed = match value {
        Value::UInt(value) => i64::try_from(value).map_err(|_| database("decode mutation ID")),
        Value::Int(value) => Ok(value),
        Value::Bytes(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| database("decode mutation ID")),
        _ => Err(database("decode mutation ID")),
    }?;
    decode_signed_id(signed).map_err(|error| invalid_batch(error.to_string()))
}

fn value_bytes(value: Value) -> Result<Vec<u8>, DorisSinkError> {
    match value {
        Value::Bytes(value) => Ok(value),
        _ => Err(database("decode row hash")),
    }
}

fn row_predicate(schema: &Schema, row: &EncodedRow) -> (String, Vec<Value>) {
    let mut predicates = vec![format!("{} = ?", quote(TECHNICAL_HASH))];
    let mut parameters = vec![Value::Bytes(row.hash.clone())];
    for (column, value) in schema.fields().iter().zip(&row.values) {
        predicates.push(format!("{} <=> ?", quote(column.name())));
        parameters.push(value.clone());
    }
    (predicates.join(" AND "), parameters)
}

fn lookup_clause(
    spec: &DorisSinkDefinition,
    request_index: usize,
    request: &Lookup,
    predicate: &str,
) -> String {
    debug_assert!(request.take > 0);
    let table = qualified(spec.database(), &spec.state_table());
    let id = quote(TECHNICAL_ID);
    let version = quote(TECHNICAL_VERSION);
    format!(
        "(SELECT {request_index} AS n, 0 AS kind, MAX({version}) AS value FROM {table} WHERE {predicate}) \
         UNION ALL (SELECT {request_index} AS n, 1 AS kind, {id} AS value FROM {table} \
         WHERE {version} = {id} AND {predicate} ORDER BY {id} LIMIT {})",
        request.take
    )
}

fn insert_prefix(spec: &DorisSinkDefinition, schema: &Schema) -> String {
    let columns = std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
        .chain(std::iter::once(TECHNICAL_VERSION))
        .chain(schema.fields().iter().map(|field| field.name().as_str()))
        .map(quote)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "INSERT INTO {} ({columns}) VALUES ",
        qualified(spec.database(), &spec.state_table())
    )
}

fn insert_statements(
    spec: &DorisSinkDefinition,
    schema: &Schema,
    actions: &[(u64, u64, EncodedRow)],
) -> Vec<String> {
    let prefix = insert_prefix(spec, schema);
    let mut statements = Vec::new();
    let mut statement = prefix.clone();
    let mut rows = 0_usize;
    let values_per_row = schema.fields().len().saturating_add(3);
    for (id, version, row) in actions {
        let values = mutation_values(*id, *version, row);
        if rows != 0
            && (statement
                .len()
                .saturating_add(values.len())
                .saturating_add(1)
                > MAX_WRITE_SQL_BYTES
                || rows.saturating_add(1).saturating_mul(values_per_row) > MAX_WRITE_VALUES)
        {
            statements.push(std::mem::replace(&mut statement, prefix.clone()));
            rows = 0;
        }
        if rows != 0 {
            statement.push(',');
        }
        statement.push_str(&values);
        rows += 1;
    }
    if rows != 0 {
        statements.push(statement);
    }
    statements
}

fn mutation_values(id: u64, version: u64, row: &EncodedRow) -> String {
    let mut values = Vec::with_capacity(row.values.len() + 3);
    values.push(encode_signed_id(id).to_string());
    values.push(bytes_literal(&row.hash));
    values.push(encode_signed_id(version).to_string());
    values.extend(row.values.iter().map(value_literal));
    format!("({})", values.join(","))
}

fn value_literal(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_owned(),
        Value::Bytes(value) => bytes_literal(value),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(..) | Value::Time(..) => {
            unreachable!("the Doris row codec does not emit temporal MySQL values")
        }
    }
}

fn bytes_literal(value: &[u8]) -> String {
    let mut literal = String::with_capacity(value.len().saturating_mul(2).saturating_add(3));
    literal.push_str("X'");
    for byte in value {
        write!(literal, "{byte:02x}").expect("writing to String cannot fail");
    }
    literal.push('\'');
    literal
}

fn create_state_sql(spec: &DorisSinkDefinition, schema: &Schema) -> String {
    let mut columns = vec![
        format!("{} BIGINT NOT NULL", quote(TECHNICAL_ID)),
        format!("{} CHAR(32) NOT NULL", quote(TECHNICAL_HASH)),
        format!("{} BIGINT NOT NULL", quote(TECHNICAL_VERSION)),
    ];
    columns.extend(schema.fields().iter().map(|column| {
        format!(
            "{} {} {}",
            quote(column.name()),
            schema::sql_type(column.data_type()),
            if schema::nullable(column) {
                "NULL"
            } else {
                "NOT NULL"
            }
        )
    }));
    columns.push(format!(
        "INDEX {} ({}) USING INVERTED",
        quote(TECHNICAL_HASH_INDEX),
        quote(TECHNICAL_HASH)
    ));
    format!(
        "CREATE TABLE {} ({}) UNIQUE KEY({}) COMMENT {} \
         DISTRIBUTED BY HASH({}) BUCKETS 1 \
         PROPERTIES (\"enable_unique_key_merge_on_write\" = \"true\", \
         \"function_column.sequence_col\" = {})",
        qualified(spec.database(), &spec.state_table()),
        columns.join(","),
        quote(TECHNICAL_ID),
        literal(&spec.marker()),
        quote(TECHNICAL_ID),
        literal(TECHNICAL_VERSION)
    )
}

fn create_view_sql(spec: &DorisSinkDefinition, schema: &Schema) -> String {
    format!(
        "CREATE VIEW {} AS SELECT {} FROM {} WHERE {} = {}",
        qualified(spec.database(), spec.table()),
        selected_public_columns(schema),
        qualified(spec.database(), &spec.state_table()),
        quote(TECHNICAL_VERSION),
        quote(TECHNICAL_ID)
    )
}

fn verify_state(
    connection: &mut Conn,
    spec: &DorisSinkDefinition,
    schema: &Schema,
) -> Result<(), DorisSinkError> {
    type Column = (String, String, String);
    let columns: Vec<Column> = connection
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table ORDER BY ORDINAL_POSITION",
            params! { "database" => spec.database(), "table" => spec.state_table() },
        )
        .map_err(|_| database("inspect state-table columns"))?;
    let mut expected = vec![
        (TECHNICAL_ID.to_owned(), "bigint(20)", "NO"),
        (TECHNICAL_HASH.to_owned(), "char(32)", "NO"),
        (TECHNICAL_VERSION.to_owned(), "bigint(20)", "NO"),
    ];
    expected.extend(schema.fields().iter().map(|column| {
        (
            column.name().to_owned(),
            schema::catalog_type(column.data_type()),
            if schema::nullable(column) {
                "YES"
            } else {
                "NO"
            },
        )
    }));
    let matches = columns.len() == expected.len()
        && columns.iter().zip(expected).all(|(actual, expected)| {
            actual.0 == expected.0
                && actual.1.eq_ignore_ascii_case(expected.1)
                && actual.2 == expected.2
        });
    let properties: Option<(String, String)> = connection
        .exec_first(
            "SELECT TABLE_TYPE, TABLE_COMMENT FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table",
            params! { "database" => spec.database(), "table" => spec.state_table() },
        )
        .map_err(|_| database("inspect state-table ownership"))?;
    if !matches || properties != Some(("BASE TABLE".to_owned(), spec.marker())) {
        return Err(DorisSinkError::TargetLayoutMismatch {
            name: spec.state_table(),
        });
    }
    let create: Option<(String, String)> = connection
        .query_first(format!(
            "SHOW CREATE TABLE {}",
            qualified(spec.database(), &spec.state_table())
        ))
        .map_err(|_| database("inspect state-table definition"))?;
    if create.as_ref().is_none_or(|(_, sql)| {
        let compact = compact_sql(sql);
        !compact.contains(&format!("UNIQUEKEY({TECHNICAL_ID})"))
            || !compact.contains(&format!(
                "DISTRIBUTEDBYHASH({TECHNICAL_ID})BUCKETS1PROPERTIES("
            ))
            || !compact.contains("\"enable_unique_key_merge_on_write\"=\"true\"")
            || !compact.contains(&format!(
                "\"function_column.sequence_col\"=\"{TECHNICAL_VERSION}\""
            ))
            || !compact.contains(&format!(
                "INDEX{TECHNICAL_HASH_INDEX}({TECHNICAL_HASH})USINGINVERTED"
            ))
    }) {
        return Err(DorisSinkError::TargetLayoutMismatch {
            name: spec.state_table(),
        });
    }
    Ok(())
}

fn verify_view(
    connection: &mut Conn,
    spec: &DorisSinkDefinition,
    schema: &Schema,
) -> Result<(), DorisSinkError> {
    let definition: Option<String> = connection
        .exec_first(
            "SELECT VIEW_DEFINITION FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table",
            params! { "database" => spec.database(), "table" => spec.table() },
        )
        .map_err(|_| database("inspect target view"))?;
    let expected = expected_view_definition(spec, schema);
    if definition
        .as_ref()
        .is_none_or(|definition| !compact_sql(definition).eq_ignore_ascii_case(&expected))
    {
        return Err(DorisSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    let actual: Option<u64> = connection
        .exec_first(
            "SELECT count(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table",
            params! { "database" => spec.database(), "table" => spec.table() },
        )
        .map_err(|_| database("inspect target-view columns"))?;
    if actual != u64::try_from(schema.fields().len() + 2).ok() {
        return Err(DorisSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    Ok(())
}

fn expected_view_definition(spec: &DorisSinkDefinition, schema: &Schema) -> String {
    let source = format!("internal.{}.{}", spec.database(), spec.state_table());
    let columns = std::iter::once(format!("{source}.{TECHNICAL_ID}AS{PUBLIC_TECHNICAL_ID}"))
        .chain(std::iter::once(format!(
            "{source}.{TECHNICAL_HASH}AS{PUBLIC_TECHNICAL_HASH}"
        )))
        .chain(
            schema
                .fields()
                .iter()
                .map(|column| format!("{source}.{}", column.name())),
        )
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT{columns}FROM{source}WHERE{source}.{TECHNICAL_VERSION}={source}.{TECHNICAL_ID}")
}

fn compact_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != '`')
        .collect()
}

fn object_kinds(
    connection: &mut Conn,
    spec: &DorisSinkDefinition,
) -> Result<BTreeMap<String, String>, DorisSinkError> {
    let names = spec.object_names();
    let rows: Vec<(String, String)> = connection
        .exec(
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME IN (:target, :state)",
            params! {
                "database" => spec.database(),
                "target" => names[0].as_str(),
                "state" => names[1].as_str(),
            },
        )
        .map_err(|_| database("inspect target objects"))?;
    Ok(rows.into_iter().collect())
}

fn selected_columns(schema: &Schema) -> String {
    std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
        .chain(std::iter::once(TECHNICAL_VERSION))
        .chain(schema.fields().iter().map(|field| field.name().as_str()))
        .map(quote)
        .collect::<Vec<_>>()
        .join(",")
}

fn selected_public_columns(schema: &Schema) -> String {
    let mut columns = vec![
        format!("{} AS {}", quote(TECHNICAL_ID), quote(PUBLIC_TECHNICAL_ID)),
        format!(
            "{} AS {}",
            quote(TECHNICAL_HASH),
            quote(PUBLIC_TECHNICAL_HASH)
        ),
    ];
    columns.extend(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .map(quote),
    );
    columns.join(",")
}

fn qualified(database: &str, object: &str) -> String {
    format!("{}.{}", quote(database), quote(object))
}

fn quote(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn only_complete_visible_ok_envelopes_authorize_settlement() {
        let visible = b"{'label':'label_a-1.2:3','status':'VISIBLE','txnId':'1'}";
        assert!(require_visible(visible, visible).is_ok());
        let maximum =
            b" \t{ 'label' : 'a' , 'status' : 'VISIBLE' , 'txnId' : '18446744073709551615' }\r\n";
        assert!(require_visible(maximum, maximum).is_ok());
        for bad in [
            b"".as_slice(),
            b"{}",
            b"\xff",
            b"{'label':'a','status':'COMMITTED','txnId':'1'}",
            b"{'label':'a','status':'PREPARE','txnId':'1'}",
            b"{\"label\":\"a\",\"status\":\"VISIBLE\",\"txnId\":\"1\"}",
            b"{'status':'VISIBLE','label':'a','txnId':'1'}",
            b"{'label':'a','label':'b','status':'VISIBLE','txnId':'1'}",
            b"{'label':'a','status':'COMMITTED','status':'VISIBLE','txnId':'1'}",
            b"{'label':'a','status':'VISIBLE','txnId':'1','err':'publish timeout'}",
            b"{'label':'a','status':'VISIBLE','txnId':'0'}",
            b"{'label':'a','status':'VISIBLE','txnId':'01'}",
            b"{'label':'a','status':'VISIBLE','txnId':'-1'}",
            b"{'label':'a','status':'VISIBLE','txnId':'18446744073709551616'}",
            b"{'label':'a','status':'VISIBLE','txnId':1}",
            b"{'label':'','status':'VISIBLE','txnId':'1'}",
            b"{'label':'a b','status':'VISIBLE','txnId':'1'}",
            b"{'label':'a\\x','status':'VISIBLE','txnId':'1'}",
            b"{'label':'a','status':'visible','txnId':'1'}",
            b"{'label':'a','status':'VISIBLE','txnId':'1'} trailing",
        ] {
            assert!(require_visible(bad, bad).is_err(), "accepted {bad:?}");
        }
        assert!(require_visible(visible, b"").is_err());
        assert!(require_visible(b"", visible).is_err());
    }

    #[test]
    fn target_id_values_decode_signed_driver_representations() {
        for (signed, id) in [
            (i64::MIN + 1, 1),
            (-1, i64::MAX.unsigned_abs()),
            (0, 1_u64 << 63),
            (i64::MAX - 1, u64::MAX - 1),
        ] {
            assert_eq!(value_id(Value::Int(signed)).unwrap(), id);
            assert_eq!(
                value_id(Value::Bytes(signed.to_string().into_bytes())).unwrap(),
                id
            );
            if let Ok(unsigned) = u64::try_from(signed) {
                assert_eq!(value_id(Value::UInt(unsigned)).unwrap(), id);
            }
        }
        for value in [
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::UInt(u64::MAX),
            Value::Bytes(b"18446744073709551615".to_vec()),
        ] {
            assert!(value_id(value).is_err());
        }
    }

    #[test]
    fn signed_mutation_literals_fit_the_event_byte_bound() {
        let row = EncodedRow {
            hash: b"0123456789abcdef0123456789abcdef".to_vec(),
            values: vec![],
        };
        let bound = mutation_values(1, 1, &row).len();
        assert!(mutation_values(1, 1, &row).starts_with("(-9223372036854775807,"));
        assert!(mutation_values(1_u64 << 63, 1_u64 << 63, &row).starts_with("(0,"));
        for id in [1, i64::MAX.unsigned_abs(), 1_u64 << 63, u64::MAX - 1] {
            assert!(mutation_values(id, id, &row).len() <= bound);
        }
    }
}

#[cfg(test)]
mod live_tests {
    fn config() -> DorisSinkConfig {
        let port = std::env::var("DOGPADDLE_DORIS_TEST_PORT")
            .map_or(19030, |value| value.parse().expect("native fixture port"));
        DorisSinkConfig::new_unencrypted("127.0.0.1", port, "dogpaddle", "root", "").unwrap()
    }

    fn write(
        target: &mut DorisTarget,
        input: &Change,
        batch: &Batch,
    ) -> Result<(), OperationError> {
        let deadline = Instant::now() + WORK_UNIT_TIMEOUT;
        target.ensure_ready(deadline)?;
        target
            .write_relation_batch(input, batch, u64::MAX, deadline)
            .map_err(Into::into)
    }

    fn lookup(
        target: &mut DorisTarget,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        let deadline = Instant::now() + WORK_UNIT_TIMEOUT;
        target.ensure_ready(deadline)?;
        target.lookup(input, requests, deadline)
    }

    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, NullArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::operation::sink::relation::{Delete, Insert};

    #[test]
    #[ignore = "requires the Doris system-test fixture on 127.0.0.1:19030"]
    #[allow(clippy::too_many_lines)]
    fn adapter_replay_is_convergent_and_rejects_id_rebinding() {
        let config = config();
        cleanup(&config);
        let spec = config.discover_target("rust_live", "rust_live").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input = Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![7, 8]))],
            )
            .unwrap(),
            Int64Array::from(vec![1, 1]),
        )
        .unwrap();
        let mut target =
            DorisTarget::new_bound(config, spec, DorisRowCodec::try_new(schema).unwrap());
        target.initialize().unwrap();
        let insert = Batch {
            inserts: vec![Insert {
                row_index: 0,
                technical_id: 1,
            }],
            deletes: vec![],
        };
        write(&mut target, &input, &insert).unwrap();
        write(&mut target, &input, &insert).unwrap();
        let found = lookup(
            &mut target,
            &input,
            &[
                Lookup {
                    row_index: 0,
                    take: 1,
                },
                Lookup {
                    row_index: 1,
                    take: 1,
                },
            ],
        )
        .unwrap();
        assert_eq!(found[0].ids.len() as u64, 1);
        assert_eq!(found[0].ids, [1]);
        assert_eq!(found[1].ids.len() as u64, 0);
        assert!(found[1].ids.is_empty());

        let rebound = Batch {
            inserts: vec![Insert {
                row_index: 1,
                technical_id: 1,
            }],
            deletes: vec![],
        };
        assert!(write(&mut target, &input, &rebound).is_err());
        let upper_ids = [1_u64 << 63, u64::MAX - 1];
        let upper = Batch {
            inserts: upper_ids
                .into_iter()
                .map(|technical_id| Insert {
                    row_index: 1,
                    technical_id,
                })
                .collect(),
            deletes: vec![],
        };
        write(&mut target, &input, &upper).unwrap();
        write(&mut target, &input, &upper).unwrap();
        let upper_found = lookup(
            &mut target,
            &input,
            &[Lookup {
                row_index: 1,
                take: 2,
            }],
        )
        .unwrap();
        assert_eq!(upper_found[0].ids, upper_ids);
        let delete = Batch {
            inserts: vec![],
            deletes: vec![Delete {
                row_index: 0,
                technical_id: 1,
                event_offset: 2,
            }],
        };
        write(&mut target, &input, &delete).unwrap();
        write(&mut target, &input, &delete).unwrap();
        let stale_row = target.codec.encode_row(input.records(), 0).unwrap();
        let stale = format!(
            "{}{}",
            insert_prefix(&target.spec, target.codec.schema()),
            mutation_values(1, 1, &stale_row)
        );
        target.config.connect().unwrap().query_drop(stale).unwrap();
        assert_eq!(
            lookup(
                &mut target,
                &input,
                &[Lookup {
                    row_index: 0,
                    take: 1,
                }],
            )
            .unwrap()[0]
                .ids
                .len(),
            0
        );
        let mut connection = target.config.connect().unwrap();
        connection
            .query_drop(format!(
                "DROP VIEW {}",
                qualified(target.spec.database(), target.spec.table())
            ))
            .unwrap();
        connection
            .query_drop(
                create_view_sql(&target.spec, target.codec.schema())
                    .replace("WHERE", "WHERE 1 = 0 AND"),
            )
            .unwrap();
        target.connection = Some(connection);
        target.verified = false;
        assert!(
            lookup(
                &mut target,
                &input,
                &[Lookup {
                    row_index: 0,
                    take: 1,
                }],
            )
            .is_err()
        );
        cleanup(&target.config);
    }

    #[test]
    #[ignore = "requires the doris system-test fixture"]
    fn committed_weighted_prefix_can_be_recut_without_repeating_fifo_deaths() {
        let config = config();
        cleanup(&config);
        let spec = config.discover_target("rust_live", "rust_live").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input = Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![7, 7, 8, 7, 7, 8, 7]))],
            )
            .unwrap(),
            Int64Array::from(vec![3, -2, 2, -1, 1, -2, 2]),
        )
        .unwrap();
        let mut target =
            DorisTarget::new_bound(config, spec, DorisRowCodec::try_new(schema).unwrap());
        target.initialize().unwrap();
        let full = DeliveryBatch::for_test(input.clone(), 1).unwrap();
        target.deliver_prefix(&full, 14, (1, &input)).unwrap();
        let short = DeliveryBatch::for_test(
            Change::try_new(input.records().slice(0, 2), Int64Array::from(vec![3, -2])).unwrap(),
            1,
        )
        .unwrap();
        let suffix = DeliveryBatch::for_test(
            Change::try_new(
                input.records().slice(2, 5),
                Int64Array::from(vec![2, -1, 1, -2, 2]),
            )
            .unwrap(),
            6,
        )
        .unwrap();
        for delivery in [&full, &short, &suffix] {
            target.deliver_prefix(delivery, 14, (1, &input)).unwrap();
            let rows: Vec<(i64, i64)> = target
                .connect(Instant::now() + WORK_UNIT_TIMEOUT)
                .unwrap()
                .query("SELECT `$dogpaddle.id`, value FROM rust_live ORDER BY `$dogpaddle.id`")
                .unwrap();
            let rows = rows
                .into_iter()
                .map(|(id, value)| (decode_signed_id(id).unwrap(), value))
                .collect::<Vec<_>>();
            assert_eq!(rows, [(9, 7), (12, 7), (13, 7)]);
        }
        let matches = lookup(
            &mut target,
            &input,
            &[
                Lookup {
                    row_index: 0,
                    take: 3,
                },
                Lookup {
                    row_index: 2,
                    take: 1,
                },
            ],
        )
        .unwrap();
        assert_eq!(matches[0].through, 13);
        assert_eq!(matches[0].ids, [9, 12, 13]);
        assert_eq!(matches[1].through, 11);
        assert!(matches[1].ids.is_empty());
        cleanup(&target.config);
    }

    fn cleanup(config: &DorisSinkConfig) {
        let mut connection = config.connect().unwrap();
        connection
            .query_drop("DROP VIEW IF EXISTS `rust_live`")
            .unwrap();
        connection
            .query_drop("DROP TABLE IF EXISTS `$dogpaddle.state.rust_live`")
            .unwrap();
    }

    #[test]
    #[ignore = "requires the Doris system-test fixture on 127.0.0.1:19030"]
    fn wide_batch_is_split_inside_one_explicit_transaction() {
        let config = config();
        cleanup(&config);
        let spec = config.discover_target("rust_live", "rust_live").unwrap();
        let fields = (0..100)
            .map(|index| Field::new(format!("f{index}"), DataType::Null, true))
            .collect::<Vec<_>>();
        let columns = (0..100)
            .map(|_| Arc::new(NullArray::new(1)) as ArrayRef)
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let input = Change::try_new(
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap(),
            Int64Array::from(vec![1024]),
        )
        .unwrap();
        let mut target =
            DorisTarget::new_bound(config, spec, DorisRowCodec::try_new(schema).unwrap());
        target.initialize().unwrap();
        let inserts = (1..=1024)
            .map(|technical_id| Insert {
                row_index: 0,
                technical_id,
            })
            .collect();
        let batch = Batch {
            inserts,
            deletes: vec![],
        };
        write(&mut target, &input, &batch).unwrap();
        write(&mut target, &input, &batch).unwrap();
        let found = lookup(
            &mut target,
            &input,
            &[Lookup {
                row_index: 0,
                take: 1024,
            }],
        )
        .unwrap();
        assert_eq!(found[0].ids.len() as u64, 1024);
        assert_eq!(found[0].ids.len(), 1024);
        cleanup(&target.config);
    }
}
