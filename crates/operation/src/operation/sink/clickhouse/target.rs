use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use serde_json::Value;

use super::{
    config::{ClickHouseSinkConfig, ClickHouseTargetSpec, require_absent, string_literal},
    error::{ClickHouseSinkError, database, invalid_batch, invalid_response},
    row::{ClickHouseRowCodec, EncodedRow},
    schema::{
        ClickHouseLayout, TECHNICAL_DELETED, TECHNICAL_HASH, TECHNICAL_HASH_INDEX, TECHNICAL_ID,
        TECHNICAL_VERSION,
    },
};
use crate::operation::{
    OperationError,
    sink::relation::{
        Batch, Lookup, Matches, RelationTarget, relation_event_bytes, terminal_mutations,
    },
};

const MAX_LOOKUP_SQL_BYTES: usize = 4 * 1024 * 1024;
const WORK_UNIT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct ClickHouseTarget {
    config: ClickHouseSinkConfig,
    spec: ClickHouseTargetSpec,
    codec: ClickHouseRowCodec,
    verified: bool,
}

impl ClickHouseTarget {
    pub(super) fn new_bound(
        config: ClickHouseSinkConfig,
        spec: ClickHouseTargetSpec,
        schema: SchemaRef,
    ) -> Self {
        let layout =
            ClickHouseLayout::try_new(schema).expect("binding validated the ClickHouse layout");
        Self {
            config,
            spec,
            codec: ClickHouseRowCodec::new(layout),
            verified: false,
        }
    }

    fn verify_identity(&self) -> Result<(), ClickHouseSinkError> {
        if self.config.database() != self.spec.database() {
            return Err(ClickHouseSinkError::DatabaseMismatch);
        }
        let uuid = self.config.command(
            &format!(
                "SELECT toString(uuid) FROM system.databases WHERE name = {} FORMAT TabSeparatedRaw",
                string_literal(self.spec.database())
            ),
            "read database identity",
        )?;
        if uuid.trim() != self.spec.database_uuid() {
            return Err(ClickHouseSinkError::TargetIdentityChanged);
        }
        Ok(())
    }

    fn ensure_ready(&mut self) -> Result<(), ClickHouseSinkError> {
        self.verify_identity()?;
        if !self.verified {
            verify_state(&self.config, &self.spec, self.codec.layout())?;
            verify_view(&self.config, &self.spec, self.codec.layout())?;
            self.verified = true;
        }
        Ok(())
    }
}

impl RelationTarget for ClickHouseTarget {
    fn event_bytes(&self, input: &Change, row_index: usize) -> Result<u64, OperationError> {
        let baseline = relation_event_bytes(input, row_index)?;
        let row = self.codec.encode_row(input.records(), row_index)?;
        let mut values = vec![
            Value::from(u64::MAX),
            Value::String(row.hash),
            Value::from(1),
            Value::from(1),
        ];
        values.extend(row.values);
        let row_bytes = serde_json::to_vec(&Value::Array(values))
            .expect("ClickHouse row JSON serialization cannot fail")
            .len();
        let framing = selected_columns(self.codec.layout())
            .len()
            .checked_add(self.spec.database().len())
            .and_then(|bytes| bytes.checked_add(self.spec.state_table().len()))
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid_batch("ClickHouse delivery framing exceeds usize"))?;
        let encoded = u64::try_from(
            row_bytes
                .checked_add(framing)
                .ok_or_else(|| invalid_batch("ClickHouse event byte charge exceeds usize"))?,
        )
        .map_err(|_| invalid_batch("ClickHouse event byte charge exceeds u64"))?;
        Ok(baseline.max(encoded))
    }

    fn require_absent(&mut self) -> Result<(), OperationError> {
        self.verify_identity()?;
        require_absent(&self.config, &self.spec).map_err(Into::into)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        self.verify_identity()?;
        let state = self.spec.state_table();
        let objects = object_kinds(&self.config, &self.spec)?;
        match (objects.get(&state), objects.get(self.spec.table())) {
            (None, None) => {
                self.config.command(
                    &create_state_sql(&self.spec, self.codec.layout()),
                    "create state table",
                )?;
            }
            (Some(engine), None) if engine == "ReplacingMergeTree" => {}
            (Some(_), None) => {
                return Err(ClickHouseSinkError::TargetLayoutMismatch { name: state }.into());
            }
            (None, Some(_)) => {
                return Err(ClickHouseSinkError::TargetMissing { name: state }.into());
            }
            (Some(_), Some(_)) => {}
        }
        verify_state(&self.config, &self.spec, self.codec.layout())?;
        let objects = object_kinds(&self.config, &self.spec)?;
        if !objects.contains_key(self.spec.table()) {
            self.config.command(
                &create_view_sql(&self.spec, self.codec.layout()),
                "create target view",
            )?;
        }
        verify_view(&self.config, &self.spec, self.codec.layout())?;
        let count = scalar_u64(
            &self.config.command(
                &format!(
                    "SELECT count() FROM {} FORMAT TabSeparatedRaw",
                    qualified(self.spec.database(), &state)
                ),
                "verify empty state table",
            )?,
            "verify empty state table",
        )?;
        if count != 0 {
            return Err(ClickHouseSinkError::TargetNotEmpty.into());
        }
        self.verified = true;
        Ok(())
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        self.ensure_ready()?;
        let mut output = Vec::with_capacity(requests.len());
        let started = Instant::now();
        let mut rows = Vec::new();
        let mut sql_bytes = 0_usize;
        for (request_index, request) in requests.iter().enumerate() {
            let row = self.codec.encode_row(input.records(), request.row_index)?;
            let encoded = lookup_row(request_index, request, &row);
            if !rows.is_empty() && sql_bytes.saturating_add(encoded.len()) > MAX_LOOKUP_SQL_BYTES {
                if started.elapsed() >= WORK_UNIT_TIMEOUT {
                    return Err(database("match target rows").into());
                }
                self.read_lookup_rows(&rows, &mut output)?;
                rows.clear();
                sql_bytes = 0;
            }
            sql_bytes = sql_bytes.saturating_add(encoded.len());
            rows.push(encoded);
        }
        if !rows.is_empty() {
            if started.elapsed() >= WORK_UNIT_TIMEOUT {
                return Err(database("match target rows").into());
            }
            self.read_lookup_rows(&rows, &mut output)?;
        }
        if output.len() != requests.len() {
            return Err(invalid_response("match target rows").into());
        }
        Ok(output)
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        self.ensure_ready()?;
        self.write_relation_batch(input, batch).map_err(Into::into)
    }
}

fn lookup_row(request_index: usize, request: &Lookup, row: &EncodedRow) -> String {
    debug_assert!(request.take > 0);
    let mut values = vec![
        request_index.to_string(),
        request.needed.to_string(),
        request.take.to_string(),
        string_literal(&row.hash),
    ];
    values.extend(row.values.iter().map(value_literal));
    format!("({})", values.join(","))
}

impl ClickHouseTarget {
    fn read_lookup_rows(
        &self,
        rows: &[String],
        output: &mut Vec<Matches>,
    ) -> Result<(), ClickHouseSinkError> {
        let request_columns = std::iter::once("n UInt64".to_owned())
            .chain(std::iter::once("needed UInt64".to_owned()))
            .chain(std::iter::once("take UInt64".to_owned()))
            .chain(std::iter::once("hash FixedString(32)".to_owned()))
            .chain(
                self.codec
                    .layout()
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| format!("v{index} {}", column.sql_type())),
            )
            .collect::<Vec<_>>()
            .join(",");
        let join = lookup_join_predicate(self.codec.layout());
        let sql = format!(
            "WITH requests AS (SELECT * FROM values({}, {})) \
             SELECT r.n, least(countIf(isNotNull(s.id)), any(r.needed)) AS count, \
             arraySlice(arraySort(groupArrayIf(assumeNotNull(s.id), isNotNull(s.id))), 1, any(r.take)) AS ids \
             FROM requests AS r LEFT JOIN \
             (SELECT {} AS id, {} AS hash{} FROM {} FINAL WHERE {} = 0 AND {} IN (SELECT hash FROM requests)) AS s ON {} \
             GROUP BY r.n ORDER BY r.n SETTINGS join_use_nulls = 1 FORMAT JSONCompactEachRow",
            string_literal(&request_columns),
            rows.join(","),
            quote(TECHNICAL_ID),
            quote(TECHNICAL_HASH),
            lookup_state_columns(self.codec.layout()),
            qualified(self.spec.database(), &self.spec.state_table()),
            quote(TECHNICAL_DELETED),
            quote(TECHNICAL_HASH),
            join,
        );
        let body = self.config.command(&sql, "match target rows")?;
        let expected_rows = output.len().saturating_add(rows.len());
        for line in body.lines().filter(|line| !line.is_empty()) {
            let values: Vec<Value> =
                serde_json::from_str(line).map_err(|_| invalid_response("match target rows"))?;
            let [n, count, ids] = values.as_slice() else {
                return Err(invalid_response("match target rows"));
            };
            if value_ref_u64(n, "match target rows")?
                != u64::try_from(output.len()).expect("request index fits u64")
            {
                return Err(invalid_response("match target rows"));
            }
            let count = value_ref_u64(count, "match target rows")?;
            let ids = ids
                .as_array()
                .ok_or_else(|| invalid_response("match target rows"))?
                .iter()
                .map(|id| value_ref_u64(id, "match target rows"))
                .collect::<Result<Vec<_>, _>>()?;
            output.push(Matches { count, ids });
        }
        if output.len() != expected_rows {
            return Err(invalid_response("match target rows"));
        }
        Ok(())
    }

    fn write_relation_batch(
        &self,
        input: &Change,
        batch: &Batch,
    ) -> Result<(), ClickHouseSinkError> {
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
        let mut payload = format!(
            "INSERT INTO {} ({}) SETTINGS async_insert = 0, wait_for_async_insert = 1, max_insert_block_size = 1048576 FORMAT JSONCompactEachRow\n",
            qualified(self.spec.database(), &self.spec.state_table()),
            selected_columns(self.codec.layout())
        );
        for (id, deleted, row) in actions {
            let mut values = vec![
                Value::from(id),
                Value::String(row.hash),
                Value::from(u8::from(deleted)),
                Value::from(u8::from(deleted)),
            ];
            values.extend(row.values);
            serde_json::to_writer(string_writer(&mut payload), &Value::Array(values))
                .expect("JSON serialization into String is infallible");
            payload.push('\n');
        }
        self.config.command(&payload, "apply relation batch")?;
        Ok(())
    }

    fn read_ids(
        &self,
        ids: impl IntoIterator<Item = u64>,
    ) -> Result<BTreeMap<u64, StoredRow>, ClickHouseSinkError> {
        let ids = ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let body = self.config.command(
            &format!(
                "SELECT {} FROM {} FINAL WHERE {} IN ({}) ORDER BY {} FORMAT JSONCompactEachRow",
                selected_columns(self.codec.layout()),
                qualified(self.spec.database(), &self.spec.state_table()),
                quote(TECHNICAL_ID),
                ids.iter().map(u64::to_string).collect::<Vec<_>>().join(","),
                quote(TECHNICAL_ID)
            ),
            "read mutation IDs",
        )?;
        let mut output = BTreeMap::new();
        for line in body.lines().filter(|line| !line.is_empty()) {
            let values: Vec<Value> =
                serde_json::from_str(line).map_err(|_| invalid_response("read mutation IDs"))?;
            if values.len() != self.codec.layout().columns().len() + 4 {
                return Err(invalid_response("read mutation IDs"));
            }
            let mut values = values.into_iter();
            let id = value_u64(values.next(), "read mutation IDs")?;
            let hash = value_string(values.next(), "read mutation IDs")?;
            let version = value_u64(values.next(), "read mutation IDs")?;
            let deleted = value_u64(values.next(), "read mutation IDs")?;
            if version != deleted || version > 1 {
                return Err(ClickHouseSinkError::TargetLayoutMismatch {
                    name: self.spec.state_table(),
                });
            }
            let values = values.collect::<Vec<_>>();
            if output
                .insert(
                    id,
                    StoredRow {
                        hash,
                        deleted: deleted == 1,
                        values,
                    },
                )
                .is_some()
            {
                return Err(ClickHouseSinkError::TargetLayoutMismatch {
                    name: self.spec.state_table(),
                });
            }
        }
        Ok(output)
    }
}

#[derive(Debug)]
struct StoredRow {
    hash: String,
    deleted: bool,
    values: Vec<Value>,
}

fn same_row(actual: &StoredRow, expected: &EncodedRow) -> bool {
    actual.hash == expected.hash
        && actual.values.len() == expected.values.len()
        && actual
            .values
            .iter()
            .zip(&expected.values)
            .all(|(actual, expected)| json_equal(actual, expected))
}

fn json_equal(actual: &Value, expected: &Value) -> bool {
    if actual == expected {
        return true;
    }
    match (actual, expected) {
        (Value::String(actual), Value::Number(expected)) => actual == &expected.to_string(),
        _ => false,
    }
}

fn lookup_join_predicate(layout: &ClickHouseLayout) -> String {
    std::iter::once("s.hash = r.hash".to_owned())
        .chain(layout.columns().iter().enumerate().map(|(index, column)| {
            format!("isNotDistinctFrom(s.{}, r.v{index})", quote(column.name()))
        }))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn lookup_state_columns(layout: &ClickHouseLayout) -> String {
    layout
        .columns()
        .iter()
        .fold(String::new(), |mut sql, column| {
            sql.push(',');
            sql.push_str(&quote(column.name()));
            sql
        })
}

fn value_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(value) => u8::from(*value).to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => string_literal(value),
        Value::Array(_) | Value::Object(_) => unreachable!("row codec emits scalar JSON values"),
    }
}

fn create_state_sql(spec: &ClickHouseTargetSpec, layout: &ClickHouseLayout) -> String {
    let mut columns = vec![
        format!("{} UInt64", quote(TECHNICAL_ID)),
        format!("{} FixedString(32)", quote(TECHNICAL_HASH)),
        format!("{} UInt8", quote(TECHNICAL_VERSION)),
        format!("{} UInt8", quote(TECHNICAL_DELETED)),
    ];
    columns.extend(
        layout
            .columns()
            .iter()
            .map(|column| format!("{} {}", quote(column.name()), column.sql_type())),
    );
    columns.push(format!(
        "INDEX {} {} TYPE set(0) GRANULARITY 1",
        quote(TECHNICAL_HASH_INDEX),
        quote(TECHNICAL_HASH)
    ));
    format!(
        "CREATE TABLE {} ({}) ENGINE = ReplacingMergeTree({}) ORDER BY {} \
         SETTINGS fsync_after_insert = 1, fsync_part_directory = 1 COMMENT {}",
        qualified(spec.database(), &spec.state_table()),
        columns.join(","),
        quote(TECHNICAL_VERSION),
        quote(TECHNICAL_ID),
        string_literal(&spec.marker())
    )
}

fn create_view_sql(spec: &ClickHouseTargetSpec, layout: &ClickHouseLayout) -> String {
    format!(
        "CREATE VIEW {} AS SELECT {} FROM {} FINAL WHERE {} = 0",
        qualified(spec.database(), spec.table()),
        selected_public_columns(layout),
        qualified(spec.database(), &spec.state_table()),
        quote(TECHNICAL_DELETED)
    )
}

fn verify_state(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
    layout: &ClickHouseLayout,
) -> Result<(), ClickHouseSinkError> {
    let expected = std::iter::once((TECHNICAL_ID, "UInt64".to_owned()))
        .chain(std::iter::once((
            TECHNICAL_HASH,
            "FixedString(32)".to_owned(),
        )))
        .chain(std::iter::once((TECHNICAL_VERSION, "UInt8".to_owned())))
        .chain(std::iter::once((TECHNICAL_DELETED, "UInt8".to_owned())))
        .chain(
            layout
                .columns()
                .iter()
                .map(|column| (column.name(), column.sql_type())),
        )
        .collect::<Vec<_>>();
    let body = config.command(
        &format!(
            "SELECT name, type FROM system.columns WHERE database = {} AND table = {} ORDER BY position FORMAT JSONEachRow",
            string_literal(spec.database()),
            string_literal(&spec.state_table())
        ),
        "inspect state-table columns",
    )?;
    let actual = body
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let value: Value = serde_json::from_str(line)
                .map_err(|_| invalid_response("inspect state-table columns"))?;
            Ok((
                value
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>, ClickHouseSinkError>>()?;
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual.0 != expected.0 || actual.1 != expected.1)
    {
        return Err(ClickHouseSinkError::TargetLayoutMismatch {
            name: spec.state_table(),
        });
    }
    let metadata = object_metadata(config, spec, &spec.state_table())?;
    let expected_engine_full = format!(
        "ReplacingMergeTree({}) ORDER BY {} SETTINGS fsync_after_insert = 1, \
         fsync_part_directory = 1, index_granularity = 8192",
        quote(TECHNICAL_VERSION),
        quote(TECHNICAL_ID)
    );
    if metadata.engine != "ReplacingMergeTree"
        || metadata.comment != spec.marker()
        || metadata.sorting_key != quote(TECHNICAL_ID)
        || metadata.primary_key != quote(TECHNICAL_ID)
        || metadata.engine_full != expected_engine_full
        || !has_exact_hash_index(config, spec)?
    {
        return Err(ClickHouseSinkError::TargetLayoutMismatch {
            name: spec.state_table(),
        });
    }
    Ok(())
}

fn has_exact_hash_index(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
) -> Result<bool, ClickHouseSinkError> {
    let body = config.command(
        &format!(
            "SELECT name, type_full, expr, granularity FROM system.data_skipping_indices \
             WHERE database = {} AND table = {} FORMAT JSONEachRow",
            string_literal(spec.database()),
            string_literal(&spec.state_table())
        ),
        "inspect state-table indexes",
    )?;
    let rows = body
        .lines()
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let [row] = rows.as_slice() else {
        return Ok(false);
    };
    let value: Value =
        serde_json::from_str(row).map_err(|_| invalid_response("inspect state-table indexes"))?;
    Ok(
        value.get("name").and_then(Value::as_str) == Some(TECHNICAL_HASH_INDEX)
            && value.get("type_full").and_then(Value::as_str) == Some("set(0)")
            && value.get("expr").and_then(Value::as_str) == Some(quote(TECHNICAL_HASH).as_str())
            && value.get("granularity").and_then(Value::as_u64) == Some(1),
    )
}

fn verify_view(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
    layout: &ClickHouseLayout,
) -> Result<(), ClickHouseSinkError> {
    let metadata = object_metadata(config, spec, spec.table())?;
    if metadata.engine != "View"
        || select_tail(&metadata.create_query) != select_tail(&create_view_sql(spec, layout))
    {
        return Err(ClickHouseSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    let count = scalar_u64(
        &config.command(
            &format!(
                "SELECT count() FROM system.columns WHERE database = {} AND table = {} FORMAT TabSeparatedRaw",
                string_literal(spec.database()),
                string_literal(spec.table())
            ),
            "inspect target-view columns",
        )?,
        "inspect target-view columns",
    )?;
    if count != u64::try_from(layout.columns().len() + 2).expect("column count fits u64") {
        return Err(ClickHouseSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    Ok(())
}

struct ObjectMetadata {
    engine: String,
    comment: String,
    create_query: String,
    sorting_key: String,
    primary_key: String,
    engine_full: String,
}

fn object_metadata(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
    name: &str,
) -> Result<ObjectMetadata, ClickHouseSinkError> {
    let body = config.command(
        &format!(
            "SELECT engine, comment, create_table_query, sorting_key, primary_key, engine_full FROM system.tables WHERE database = {} AND name = {} FORMAT JSONEachRow",
            string_literal(spec.database()),
            string_literal(name)
        ),
        "inspect target object",
    )?;
    let value: Value =
        serde_json::from_str(body.trim()).map_err(|_| invalid_response("inspect target object"))?;
    Ok(ObjectMetadata {
        engine: value
            .get("engine")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        comment: value
            .get("comment")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        create_query: value
            .get("create_table_query")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        sorting_key: value
            .get("sorting_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        primary_key: value
            .get("primary_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        engine_full: value
            .get("engine_full")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

fn select_tail(sql: &str) -> Option<String> {
    let compact = sql
        .chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != '`')
        .collect::<String>();
    compact
        .find("ASSELECT")
        .map(|start| compact[start..].to_owned())
}

fn object_kinds(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
) -> Result<BTreeMap<String, String>, ClickHouseSinkError> {
    let body = config.command(
        &format!(
            "SELECT name, engine FROM system.tables WHERE database = {} AND name IN ({}, {}) FORMAT JSONEachRow",
            string_literal(spec.database()),
            string_literal(spec.table()),
            string_literal(&spec.state_table())
        ),
        "inspect target objects",
    )?;
    body.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let value: Value = serde_json::from_str(line)
                .map_err(|_| invalid_response("inspect target objects"))?;
            Ok((
                value
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                value
                    .get("engine")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ))
        })
        .collect()
}

fn selected_columns(layout: &ClickHouseLayout) -> String {
    std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
        .chain(std::iter::once(TECHNICAL_VERSION))
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

fn selected_public_columns(layout: &ClickHouseLayout) -> String {
    std::iter::once(TECHNICAL_ID)
        .chain(std::iter::once(TECHNICAL_HASH))
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

fn value_u64(value: Option<Value>, stage: &'static str) -> Result<u64, ClickHouseSinkError> {
    match value {
        Some(Value::Number(value)) => value.as_u64().ok_or_else(|| invalid_response(stage)),
        Some(Value::String(value)) => value.parse().map_err(|_| invalid_response(stage)),
        _ => Err(invalid_response(stage)),
    }
}

fn value_ref_u64(value: &Value, stage: &'static str) -> Result<u64, ClickHouseSinkError> {
    match value {
        Value::Number(value) => value.as_u64().ok_or_else(|| invalid_response(stage)),
        Value::String(value) => value.parse().map_err(|_| invalid_response(stage)),
        _ => Err(invalid_response(stage)),
    }
}

fn value_string(value: Option<Value>, stage: &'static str) -> Result<String, ClickHouseSinkError> {
    value
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| invalid_response(stage))
}

fn scalar_u64(body: &str, stage: &'static str) -> Result<u64, ClickHouseSinkError> {
    body.trim().parse().map_err(|_| invalid_response(stage))
}

fn qualified(database: &str, object: &str) -> String {
    format!("{}.{}", quote(database), quote(object))
}

fn quote(identifier: &str) -> String {
    format!("`{}`", identifier.replace('\\', "\\\\").replace('`', "\\`"))
}

fn string_writer(output: &mut String) -> impl std::io::Write + '_ {
    struct Writer<'a>(&'a mut String);
    impl std::io::Write for Writer<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let value = std::str::from_utf8(bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            self.0.push_str(value);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    Writer(output)
}

#[cfg(test)]
mod live_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::operation::sink::relation::{Delete, Insert};

    #[test]
    #[ignore = "requires the ClickHouse system-test fixture on 127.0.0.1:18123"]
    #[allow(clippy::too_many_lines)]
    fn adapter_replay_is_convergent_and_rejects_id_rebinding() {
        let config = ClickHouseSinkConfig::new_unencrypted(
            "127.0.0.1",
            18123,
            "dogpaddle",
            "dogpaddle",
            "dogpaddle",
        )
        .unwrap();
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
        let mut target = ClickHouseTarget::new_bound(config, spec, schema);
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
            "INSERT INTO {} ({}) FORMAT JSONCompactEachRow\n{}\n",
            qualified(target.spec.database(), &target.spec.state_table()),
            selected_columns(target.codec.layout()),
            serde_json::to_string(&serde_json::json!([
                1,
                stale_row.hash,
                0,
                0,
                stale_row.values[0]
            ]))
            .unwrap()
        );
        target
            .config
            .command(&stale, "inject stale replay")
            .unwrap();
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
        target
            .config
            .command(
                &format!(
                    "DROP VIEW {}",
                    qualified(target.spec.database(), target.spec.table())
                ),
                "replace target view",
            )
            .unwrap();
        target
            .config
            .command(
                &create_view_sql(&target.spec, target.codec.layout()).replace(" = 0", " = 1"),
                "replace target view",
            )
            .unwrap();
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

    #[test]
    #[ignore = "requires the ClickHouse system-test fixture on 127.0.0.1:18123"]
    fn adapter_batches_maximum_distinct_lookup_over_historical_state() {
        const ROWS: usize = 16 * 1024;
        const LOOKUPS: usize = 1024;

        let config = ClickHouseSinkConfig::new_unencrypted(
            "127.0.0.1",
            18123,
            "dogpaddle",
            "dogpaddle",
            "dogpaddle",
        )
        .unwrap();
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
                vec![Arc::new(Int64Array::from_iter_values(
                    0..i64::try_from(ROWS).unwrap(),
                ))],
            )
            .unwrap(),
            Int64Array::from(vec![1; ROWS]),
        )
        .unwrap();
        let mut target = ClickHouseTarget::new_bound(config, spec, schema);
        target.initialize().unwrap();
        for start in (0..ROWS).step_by(LOOKUPS) {
            let batch = Batch {
                inserts: (start..start + LOOKUPS)
                    .map(|row_index| Insert {
                        row_index: u64::try_from(row_index).unwrap(),
                        technical_id: u64::try_from(row_index + 1).unwrap(),
                    })
                    .collect(),
                deletes: vec![],
            };
            target.write_batch(&input, &batch).unwrap();
        }
        let first = ROWS - LOOKUPS;
        let requests = (first..ROWS)
            .map(|row_index| Lookup {
                row_index,
                needed: 1,
                take: 1,
            })
            .collect::<Vec<_>>();
        let found = target.lookup(&input, &requests).unwrap();
        assert_eq!(found.len(), LOOKUPS);
        for (offset, matches) in found.into_iter().enumerate() {
            assert_eq!(matches.count, 1);
            assert_eq!(matches.ids, [u64::try_from(first + offset + 1).unwrap()]);
        }
        cleanup(&target.config);
    }

    fn cleanup(config: &ClickHouseSinkConfig) {
        config
            .command("DROP VIEW IF EXISTS `dogpaddle`.`rust_live`", "cleanup")
            .unwrap();
        config
            .command(
                "DROP TABLE IF EXISTS `dogpaddle`.`$dogpaddle.state.rust_live`",
                "cleanup",
            )
            .unwrap();
    }
}
