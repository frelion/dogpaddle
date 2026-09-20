use std::{
    collections::BTreeMap,
    fmt::Write as _,
    time::{Duration, Instant},
};

use dogpaddle_change::Change;
use mysql::{Conn, Params, Value, params, prelude::Queryable};

use super::{
    config::{DorisSinkConfig, DorisTargetSpec, require_absent},
    error::{DorisSinkError, database, invalid_batch},
    row::{DorisRowCodec, EncodedRow},
    schema::{
        DorisLayout, PUBLIC_TECHNICAL_HASH, PUBLIC_TECHNICAL_ID, TECHNICAL_DELETED, TECHNICAL_HASH,
        TECHNICAL_HASH_INDEX, TECHNICAL_ID,
    },
};
use crate::operation::{
    OperationError,
    sink::relation::{
        Batch, Lookup, Matches, RelationTarget, relation_event_bytes, terminal_mutations,
    },
};

const MAX_LOOKUP_CLAUSES: usize = 128;
const MAX_LOOKUP_PARAMETERS: usize = 65_535;
const MAX_LOOKUP_SQL_BYTES: usize = 4 * 1024 * 1024;
const MAX_WRITE_SQL_BYTES: usize = 4 * 1024 * 1024;
const MAX_WRITE_VALUES: usize = 10_000;
const WORK_UNIT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct DorisTarget {
    config: DorisSinkConfig,
    spec: DorisTargetSpec,
    codec: DorisRowCodec,
    connection: Option<Conn>,
    verified: bool,
}

impl DorisTarget {
    pub(super) fn new_bound(
        config: DorisSinkConfig,
        spec: DorisTargetSpec,
        layout: DorisLayout,
    ) -> Self {
        Self {
            config,
            spec,
            codec: DorisRowCodec::new(layout),
            connection: None,
            verified: false,
        }
    }

    fn connect(&mut self) -> Result<&mut Conn, DorisSinkError> {
        if self.config.database() != self.spec.database() {
            return Err(DorisSinkError::DatabaseMismatch);
        }
        if self.connection.is_none() {
            let mut connection = self.config.connect()?;
            let ids: Vec<u64> = connection
                .query("SELECT DISTINCT ClusterId FROM frontends()")
                .map_err(|_| database("read cluster identity"))?;
            if ids.as_slice() != [self.spec.cluster_id()] {
                return Err(DorisSinkError::TargetIdentityChanged);
            }
            self.connection = Some(connection);
        }
        Ok(self.connection.as_mut().expect("connection was installed"))
    }

    fn ensure_ready(&mut self) -> Result<(), DorisSinkError> {
        self.connect()?;
        if !self.verified {
            let mut connection = self.connection.take().expect("connection was installed");
            let result = verify_state(&mut connection, &self.spec, self.codec.layout())
                .and_then(|()| verify_view(&mut connection, &self.spec, self.codec.layout()));
            self.connection = Some(connection);
            result?;
            self.verified = true;
        }
        Ok(())
    }
}

impl RelationTarget for DorisTarget {
    fn event_bytes(&self, input: &Change, row_index: usize) -> Result<u64, OperationError> {
        let baseline = relation_event_bytes(input, row_index)?;
        let row = self.codec.encode_row(input.records(), row_index)?;
        let encoded = mutation_values(u64::MAX, true, &row)
            .len()
            .checked_add(insert_prefix(&self.spec, self.codec.layout()).len())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| invalid_batch("Doris event byte charge exceeds u64"))?;
        Ok(baseline.max(encoded))
    }

    fn require_absent(&mut self) -> Result<(), OperationError> {
        let spec = self.spec.clone();
        require_absent(self.connect()?, &spec).map_err(Into::into)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        self.connect()?;
        let mut connection = self.connection.take().expect("connection was installed");
        let result = self.initialize_with(&mut connection);
        self.connection = Some(connection);
        if result.is_ok() {
            self.verified = true;
        }
        result.map_err(Into::into)
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        self.ensure_ready()?;
        let mut output = Vec::with_capacity(requests.len());
        let started = Instant::now();
        let mut clauses = Vec::new();
        let mut parameters = Vec::new();
        let mut sql_bytes = 0_usize;
        for (request_index, request) in requests.iter().enumerate() {
            let row = self.codec.encode_row(input.records(), request.row_index)?;
            let (predicate, row_parameters) = row_predicate(self.codec.layout(), &row);
            let clause = lookup_clause(&self.spec, request_index, request, &predicate);
            if !clauses.is_empty()
                && (clauses.len() == MAX_LOOKUP_CLAUSES
                    || parameters.len().saturating_add(row_parameters.len())
                        > MAX_LOOKUP_PARAMETERS
                    || sql_bytes.saturating_add(clause.len()) > MAX_LOOKUP_SQL_BYTES)
            {
                if started.elapsed() >= WORK_UNIT_TIMEOUT {
                    return Err(database("match target rows").into());
                }
                self.read_lookup_clauses(&clauses, std::mem::take(&mut parameters), &mut output)?;
                clauses.clear();
                sql_bytes = 0;
            }
            parameters.extend(row_parameters);
            sql_bytes = sql_bytes.saturating_add(clause.len());
            clauses.push(clause);
        }
        if !clauses.is_empty() {
            if started.elapsed() >= WORK_UNIT_TIMEOUT {
                return Err(database("match target rows").into());
            }
            self.read_lookup_clauses(&clauses, parameters, &mut output)?;
        }
        if output.len() != requests.len() {
            return Err(database("decode matching rows").into());
        }
        Ok(output)
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        self.ensure_ready()?;
        self.write_relation_batch(input, batch).map_err(Into::into)
    }
}

impl DorisTarget {
    fn read_lookup_clauses(
        &mut self,
        clauses: &[String],
        parameters: Vec<Value>,
        output: &mut Vec<Matches>,
    ) -> Result<(), DorisSinkError> {
        let expected_rows = output.len().saturating_add(clauses.len());
        let sql = format!("{} ORDER BY n, id IS NULL, id", clauses.join(" UNION ALL "));
        let rows: Vec<(u64, Option<u64>, u64)> = self
            .connect()?
            .exec(sql, Params::Positional(parameters))
            .map_err(|_| database("match target rows"))?;
        let mut current = None;
        for (request_index, id, count) in rows {
            let request_index = usize::try_from(request_index)
                .map_err(|_| database("decode matching request index"))?;
            if current != Some(request_index) {
                if request_index != output.len() {
                    return Err(database("decode matching request order"));
                }
                output.push(Matches {
                    count,
                    ids: Vec::new(),
                });
                current = Some(request_index);
            }
            let matched = output
                .last_mut()
                .expect("a matching request was installed above");
            if matched.count != count || (id.is_none() && count != 0) {
                return Err(database("decode matching-row count"));
            }
            if let Some(id) = id {
                matched.ids.push(id);
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
                let create = create_state_sql(&self.spec, self.codec.layout());
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
        verify_state(connection, &self.spec, self.codec.layout())?;
        let objects = object_kinds(connection, &self.spec)?;
        if !objects.contains_key(&target) {
            connection
                .query_drop(create_view_sql(&self.spec, self.codec.layout()))
                .map_err(|_| database("create target view"))?;
        }
        verify_view(connection, &self.spec, self.codec.layout())?;
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
    ) -> Result<(), DorisSinkError> {
        let terminal = terminal_mutations(batch);
        let existing = self.read_ids(terminal.iter().map(|mutation| mutation.technical_id))?;
        let mut actions = Vec::new();
        for mutation in terminal {
            let id = mutation.technical_id;
            let want_deleted = mutation.deleted;
            let row_index = usize::try_from(mutation.row_index)
                .map_err(|_| invalid_batch("row index exceeds usize"))?;
            let row = self.codec.encode_row(input.records(), row_index)?;
            match existing.get(&id) {
                Some(actual) => {
                    if !same_row(actual, &row) {
                        return Err(invalid_batch(format!(
                            "technical ID {id} is bound to another logical row"
                        )));
                    }
                    if actual.deleted && !want_deleted {
                        return Err(invalid_batch(format!(
                            "technical ID {id} cannot be resurrected"
                        )));
                    }
                    if actual.deleted != want_deleted {
                        actions.push((id, want_deleted, row));
                    }
                }
                None if !want_deleted => actions.push((id, false, row)),
                None => {}
            }
        }
        if actions.is_empty() {
            return Ok(());
        }
        let statements = insert_statements(&self.spec, self.codec.layout(), &actions);
        let transactional = statements.len() > 1;
        let started = Instant::now();
        let connection = self.connect()?;
        if transactional {
            connection
                .query_drop("BEGIN")
                .map_err(|_| database("begin relation batch"))?;
            for statement in statements {
                if started.elapsed() >= WORK_UNIT_TIMEOUT {
                    let _ = connection.query_drop("ROLLBACK");
                    return Err(database("apply relation batch"));
                }
                if connection.query_drop(statement).is_err() {
                    let _ = connection.query_drop("ROLLBACK");
                    return Err(database("apply relation batch"));
                }
            }
            connection
                .query_drop("COMMIT")
                .map_err(|_| database("commit relation batch"))?;
        } else if let Some(statement) = statements.into_iter().next()
            && connection.query_drop(statement).is_err()
        {
            return Err(database("apply relation batch"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StoredRow {
    hash: Vec<u8>,
    deleted: bool,
    values: Vec<Value>,
}

impl DorisTarget {
    fn read_ids(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
    ) -> Result<BTreeMap<u64, StoredRow>, DorisSinkError> {
        let ids = ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let columns = selected_columns(self.codec.layout());
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT {columns} FROM {} WHERE {} IN ({placeholders}) ORDER BY {}",
            qualified(self.spec.database(), &self.spec.state_table()),
            quote(TECHNICAL_ID),
            quote(TECHNICAL_ID)
        );
        let parameters = ids.into_iter().map(Value::UInt).collect::<Vec<_>>();
        let rows: Vec<mysql::Row> = self
            .connect()?
            .exec(sql, Params::Positional(parameters))
            .map_err(|_| database("read mutation IDs"))?;
        let mut output = BTreeMap::new();
        for row in rows {
            let mut values = row.unwrap().into_iter();
            let id = value_u64(
                values
                    .next()
                    .ok_or_else(|| database("decode mutation ID"))?,
            )?;
            let hash = value_bytes(values.next().ok_or_else(|| database("decode row hash"))?)?;
            let deleted = value_bool(
                values
                    .next()
                    .ok_or_else(|| database("decode delete marker"))?,
            )?;
            let stored = StoredRow {
                hash,
                deleted,
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

fn value_u64(value: Value) -> Result<u64, DorisSinkError> {
    match value {
        Value::UInt(value) => Ok(value),
        Value::Int(value) => u64::try_from(value).map_err(|_| database("decode mutation ID")),
        Value::Bytes(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| database("decode mutation ID")),
        _ => Err(database("decode mutation ID")),
    }
}

fn value_bytes(value: Value) -> Result<Vec<u8>, DorisSinkError> {
    match value {
        Value::Bytes(value) => Ok(value),
        _ => Err(database("decode row hash")),
    }
}

fn value_bool(value: Value) -> Result<bool, DorisSinkError> {
    match value {
        Value::Int(0) | Value::UInt(0) => Ok(false),
        Value::Int(1) | Value::UInt(1) => Ok(true),
        Value::Bytes(value) if value == b"0" => Ok(false),
        Value::Bytes(value) if value == b"1" => Ok(true),
        _ => Err(database("decode delete marker")),
    }
}

fn row_predicate(layout: &DorisLayout, row: &EncodedRow) -> (String, Vec<Value>) {
    let mut predicates = vec![format!("{} = ?", quote(TECHNICAL_HASH))];
    let mut parameters = vec![Value::Bytes(row.hash.clone())];
    for (column, value) in layout.columns().iter().zip(&row.values) {
        predicates.push(format!("{} <=> ?", quote(column.name())));
        parameters.push(value.clone());
    }
    (predicates.join(" AND "), parameters)
}

fn lookup_clause(
    spec: &DorisTargetSpec,
    request_index: usize,
    request: &Lookup,
    predicate: &str,
) -> String {
    debug_assert!(request.take > 0);
    format!(
        "(SELECT {request_index} AS n, id, count FROM \
         (SELECT id, count(id) OVER () AS count FROM \
         ((SELECT {} AS id FROM {} WHERE {} = 0 AND {predicate} ORDER BY {} LIMIT {}) \
         UNION ALL SELECT CAST(NULL AS BIGINT) AS id) AS matched) AS counted \
         ORDER BY id IS NULL, id LIMIT {})",
        quote(TECHNICAL_ID),
        qualified(spec.database(), &spec.state_table()),
        quote(TECHNICAL_DELETED),
        quote(TECHNICAL_ID),
        request.needed,
        request.take
    )
}

fn insert_prefix(spec: &DorisTargetSpec, layout: &DorisLayout) -> String {
    let columns = std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
        .chain(std::iter::once(TECHNICAL_DELETED))
        .chain(
            layout
                .columns()
                .iter()
                .map(super::schema::ColumnLayout::name),
        )
        .map(quote)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "INSERT INTO {} ({columns}) VALUES ",
        qualified(spec.database(), &spec.state_table())
    )
}

fn insert_statements(
    spec: &DorisTargetSpec,
    layout: &DorisLayout,
    actions: &[(u64, bool, EncodedRow)],
) -> Vec<String> {
    let prefix = insert_prefix(spec, layout);
    let mut statements = Vec::new();
    let mut statement = prefix.clone();
    let mut rows = 0_usize;
    let values_per_row = layout.columns().len().saturating_add(3);
    for (id, deleted, row) in actions {
        let values = mutation_values(*id, *deleted, row);
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

fn mutation_values(id: u64, deleted: bool, row: &EncodedRow) -> String {
    let mut values = Vec::with_capacity(row.values.len() + 3);
    values.push(id.to_string());
    values.push(bytes_literal(&row.hash));
    values.push(u8::from(deleted).to_string());
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

fn create_state_sql(spec: &DorisTargetSpec, layout: &DorisLayout) -> String {
    let mut columns = vec![
        format!("{} BIGINT NOT NULL", quote(TECHNICAL_ID)),
        format!("{} CHAR(32) NOT NULL", quote(TECHNICAL_HASH)),
        format!("{} TINYINT NOT NULL", quote(TECHNICAL_DELETED)),
    ];
    columns.extend(layout.columns().iter().map(|column| {
        format!(
            "{} {} {}",
            quote(column.name()),
            column.storage().sql(),
            if column.nullable() {
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
        literal(TECHNICAL_DELETED)
    )
}

fn create_view_sql(spec: &DorisTargetSpec, layout: &DorisLayout) -> String {
    format!(
        "CREATE VIEW {} AS SELECT {} FROM {} WHERE {} = 0",
        qualified(spec.database(), spec.table()),
        selected_public_columns(layout),
        qualified(spec.database(), &spec.state_table()),
        quote(TECHNICAL_DELETED)
    )
}

fn verify_state(
    connection: &mut Conn,
    spec: &DorisTargetSpec,
    layout: &DorisLayout,
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
        (TECHNICAL_DELETED.to_owned(), "tinyint(4)", "NO"),
    ];
    expected.extend(layout.columns().iter().map(|column| {
        (
            column.name().to_owned(),
            column.storage().catalog_type(),
            if column.nullable() { "YES" } else { "NO" },
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
                "\"function_column.sequence_col\"=\"{TECHNICAL_DELETED}\""
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
    spec: &DorisTargetSpec,
    layout: &DorisLayout,
) -> Result<(), DorisSinkError> {
    let definition: Option<String> = connection
        .exec_first(
            "SELECT VIEW_DEFINITION FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table",
            params! { "database" => spec.database(), "table" => spec.table() },
        )
        .map_err(|_| database("inspect target view"))?;
    let expected = expected_view_definition(spec, layout);
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
    if actual != u64::try_from(layout.columns().len() + 2).ok() {
        return Err(DorisSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    Ok(())
}

fn expected_view_definition(spec: &DorisTargetSpec, layout: &DorisLayout) -> String {
    let source = format!("internal.{}.{}", spec.database(), spec.state_table());
    let columns = std::iter::once(format!("{source}.{TECHNICAL_ID}AS{PUBLIC_TECHNICAL_ID}"))
        .chain(std::iter::once(format!(
            "{source}.{TECHNICAL_HASH}AS{PUBLIC_TECHNICAL_HASH}"
        )))
        .chain(
            layout
                .columns()
                .iter()
                .map(|column| format!("{source}.{}", column.name())),
        )
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT{columns}FROM{source}WHERE{source}.{TECHNICAL_DELETED}=0")
}

fn compact_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != '`')
        .collect()
}

fn object_kinds(
    connection: &mut Conn,
    spec: &DorisTargetSpec,
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

fn selected_columns(layout: &DorisLayout) -> String {
    std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
        .chain(std::iter::once(TECHNICAL_DELETED))
        .chain(
            layout
                .columns()
                .iter()
                .map(super::schema::ColumnLayout::name),
        )
        .map(quote)
        .collect::<Vec<_>>()
        .join(",")
}

fn selected_public_columns(layout: &DorisLayout) -> String {
    let mut columns = vec![
        format!("{} AS {}", quote(TECHNICAL_ID), quote(PUBLIC_TECHNICAL_ID)),
        format!(
            "{} AS {}",
            quote(TECHNICAL_HASH),
            quote(PUBLIC_TECHNICAL_HASH)
        ),
    ];
    columns.extend(
        layout
            .columns()
            .iter()
            .map(super::schema::ColumnLayout::name)
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
mod live_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, NullArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::operation::sink::relation::{Delete, Insert};

    #[test]
    #[ignore = "requires the Doris system-test fixture on 127.0.0.1:19030"]
    #[allow(clippy::too_many_lines)]
    fn adapter_replay_is_convergent_and_rejects_id_rebinding() {
        let config =
            DorisSinkConfig::new_unencrypted("127.0.0.1", 19030, "dogpaddle", "root", "").unwrap();
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
            DorisTarget::new_bound(config, spec, DorisLayout::try_new(schema).unwrap());
        target.initialize().unwrap();
        let insert = Batch {
            inserts: vec![Insert {
                row_index: 0,
                technical_id: 1,
            }],
            deletes: vec![],
        };
        target.write_batch(&input, &insert).unwrap();
        target.write_batch(&input, &insert).unwrap();
        let found = target
            .lookup(
                &input,
                &[
                    Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    },
                    Lookup {
                        row_index: 1,
                        needed: 1,
                        take: 1,
                    },
                ],
            )
            .unwrap();
        assert_eq!(found[0].count, 1);
        assert_eq!(found[0].ids, [1]);
        assert_eq!(found[1].count, 0);
        assert!(found[1].ids.is_empty());

        let rebound = Batch {
            inserts: vec![Insert {
                row_index: 1,
                technical_id: 1,
            }],
            deletes: vec![],
        };
        assert!(target.write_batch(&input, &rebound).is_err());
        let delete = Batch {
            inserts: vec![],
            deletes: vec![Delete {
                row_index: 0,
                technical_id: 1,
            }],
        };
        target.write_batch(&input, &delete).unwrap();
        target.write_batch(&input, &delete).unwrap();
        let stale_row = target.codec.encode_row(input.records(), 0).unwrap();
        let stale = format!(
            "{}{}",
            insert_prefix(&target.spec, target.codec.layout()),
            mutation_values(1, false, &stale_row)
        );
        target.config.connect().unwrap().query_drop(stale).unwrap();
        assert_eq!(
            target
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    }],
                )
                .unwrap()[0]
                .count,
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
                create_view_sql(&target.spec, target.codec.layout()).replace(" = 0", " = 1"),
            )
            .unwrap();
        target.connection = Some(connection);
        target.verified = false;
        assert!(
            target
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    }],
                )
                .is_err()
        );
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
        let config =
            DorisSinkConfig::new_unencrypted("127.0.0.1", 19030, "dogpaddle", "root", "").unwrap();
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
            DorisTarget::new_bound(config, spec, DorisLayout::try_new(schema).unwrap());
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
        target.write_batch(&input, &batch).unwrap();
        target.write_batch(&input, &batch).unwrap();
        let found = target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    needed: 1024,
                    take: 1024,
                }],
            )
            .unwrap();
        assert_eq!(found[0].count, 1024);
        assert_eq!(found[0].ids.len(), 1024);
        cleanup(&target.config);
    }
}
