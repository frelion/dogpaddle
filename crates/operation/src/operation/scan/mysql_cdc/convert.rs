use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::SchemaRef;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_change::Change;
use dogpaddle_debezium::Record;
use serde_json::{Map, Value};

use super::{MySqlCdcScanError, MySqlColumn, MySqlType};

type Row = Map<String, Value>;

/// Progress of the temporary `no_data` schema bootstrap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BootstrapControl {
    /// More snapshot schema controls must be acknowledged before the seed is usable.
    Continuing,
    /// The final snapshot schema control made the offset a completed seed.
    Complete,
}

pub(super) fn convert_records(
    columns: &[MySqlColumn],
    output_schema: SchemaRef,
    topic_prefix: &str,
    database: &str,
    table: &str,
    records: &[Record],
) -> Result<Option<Change>, MySqlCdcScanError> {
    convert_values(
        columns,
        output_schema,
        topic_prefix,
        database,
        table,
        records
            .iter()
            .map(|record| (record.topic(), record.value())),
    )
}

/// Verifies one delivery from the temporary schema bootstrap.
///
/// The temporary connector uses one-record batches and disabled heartbeats.
/// It must ACK only snapshot schema controls until the final control marks the
/// snapshot complete; accepting a data record would let a later binlog offset
/// skip an unpersisted table event.
pub(super) fn validate_bootstrap_control(
    records: &[Record],
    topic_prefix: &str,
) -> Result<BootstrapControl, MySqlCdcScanError> {
    let [record] = records else {
        return Err(invalid(
            "bootstrap must contain exactly one snapshot schema control record",
        ));
    };
    validate_bootstrap_value(record.topic(), record.value(), topic_prefix)
}

pub(super) fn validate_bootstrap_value(
    topic: Option<&str>,
    bytes: Option<&[u8]>,
    topic_prefix: &str,
) -> Result<BootstrapControl, MySqlCdcScanError> {
    if topic != Some(topic_prefix) {
        return Err(invalid(
            "bootstrap record has an unexpected schema-control topic",
        ));
    }
    let bytes = bytes.ok_or_else(|| invalid("bootstrap schema control cannot be a tombstone"))?;
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| invalid("bootstrap record is not valid schemas-enabled Connect JSON"))?;
    bootstrap_schema_control(
        object_field(&value, "schema")?,
        object_field(&value, "payload")?,
        topic_prefix,
    )
}

// The byte-level boundary also lets tests use actual Connect JSON without
// exposing constructors for the runtime's owned Record capability.
pub(super) fn convert_values<'a>(
    columns: &[MySqlColumn],
    output_schema: SchemaRef,
    topic_prefix: &str,
    database: &str,
    table: &str,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, MySqlCdcScanError> {
    let table_topic = format!("{topic_prefix}.{database}.{table}");
    let heartbeat_topic = format!("__debezium-heartbeat.{topic_prefix}");
    let schema_topic = topic_prefix;
    let mut rows = Vec::new();
    let mut diffs = Vec::new();
    for (topic, bytes) in values {
        if topic != Some(table_topic.as_str())
            && topic != Some(heartbeat_topic.as_str())
            && topic != Some(schema_topic)
        {
            return Err(invalid("record has an unexpected topic"));
        }
        let Some(bytes) = bytes else {
            if topic == Some(table_topic.as_str()) {
                continue;
            }
            return Err(invalid("heartbeat cannot be a tombstone"));
        };
        let mut value: Value = serde_json::from_slice(bytes)
            .map_err(|_| invalid("record is not valid schemas-enabled Connect JSON"))?;
        let schema = object_field(&value, "schema")?;
        if topic == Some(heartbeat_topic.as_str()) {
            validate_heartbeat(schema, object_field(&value, "payload")?)?;
            continue;
        }
        if topic == Some(schema_topic) {
            validate_schema_snapshot(schema, object_field(&value, "payload")?, topic_prefix)?;
            continue;
        }
        validate_envelope(columns, schema)?;
        let payload = value
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("missing object field payload"))?;
        validate_metadata(payload, database, table)?;
        let before = payload
            .remove("before")
            .ok_or_else(|| invalid("missing before"))?;
        let after = payload
            .remove("after")
            .ok_or_else(|| invalid("missing after"))?;
        match payload.get("op").and_then(Value::as_str) {
            Some("c") if before.is_null() => {
                rows.push(complete_row(columns, after)?);
                diffs.push(1);
            }
            Some("u") => {
                rows.push(complete_row(columns, before)?);
                rows.push(complete_row(columns, after)?);
                diffs.extend([-1, 1]);
            }
            Some("d") if after.is_null() => {
                rows.push(complete_row(columns, before)?);
                diffs.push(-1);
            }
            _ => return Err(invalid("expected a streaming insert, update, or delete")),
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    let arrays = columns
        .iter()
        .map(|column| column_array(column, &rows))
        .collect::<Result<Vec<_>, _>>()?;
    let records = RecordBatch::try_new(output_schema, arrays)?;
    Ok(Some(Change::try_new(records, Int64Array::from(diffs))?))
}

fn validate_schema_snapshot(
    schema: &Row,
    payload: &Row,
    topic_prefix: &str,
) -> Result<(), MySqlCdcScanError> {
    let _ = bootstrap_schema_control(schema, payload, topic_prefix)?;
    Ok(())
}

fn bootstrap_schema_control(
    schema: &Row,
    payload: &Row,
    topic_prefix: &str,
) -> Result<BootstrapControl, MySqlCdcScanError> {
    if schema.get("type").and_then(Value::as_str) != Some("struct")
        || schema.get("name").and_then(Value::as_str)
            != Some("io.debezium.connector.mysql.SchemaChangeValue")
        || payload.get("ts_ms").and_then(Value::as_i64).is_none()
        || !matches!(
            payload.get("databaseName"),
            Some(Value::Null | Value::String(_))
        )
        || !matches!(
            payload.get("schemaName"),
            Some(Value::Null | Value::String(_))
        )
        || !matches!(payload.get("ddl"), Some(Value::Null | Value::String(_)))
        || !payload.get("tableChanges").is_some_and(Value::is_array)
    {
        return Err(invalid("unexpected MySQL schema control record"));
    }
    let source = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("schema control record is missing source metadata"))?;
    let snapshot = match source.get("snapshot").and_then(Value::as_str) {
        Some("true") => BootstrapControl::Continuing,
        Some("last") => BootstrapControl::Complete,
        _ => {
            return Err(invalid(
                "schema control record is not an initial or recovery snapshot record",
            ));
        }
    };
    if source.get("connector").and_then(Value::as_str) != Some("mysql")
        || source.get("name").and_then(Value::as_str) != Some(topic_prefix)
    {
        return Err(invalid(
            "schema control record is not an initial or recovery snapshot record",
        ));
    }
    Ok(snapshot)
}

fn object_field<'a>(value: &'a Value, field: &str) -> Result<&'a Row, MySqlCdcScanError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("missing object field {field}")))
}

fn validate_metadata(payload: &Row, database: &str, table: &str) -> Result<(), MySqlCdcScanError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium CDC metadata"))?;
    for (field, expected) in [("db", database), ("table", table), ("connector", "mysql")] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium CDC metadata does not match configured {field}"
            )));
        }
    }
    // SnapshotRecord.FALSE deliberately leaves this Struct field unset in
    // some Debezium paths. Snapshot operations are independently rejected by
    // the operation guard below.
    if !matches!(
        metadata.get("snapshot"),
        Some(Value::Null | Value::Bool(false))
    ) && metadata.get("snapshot").and_then(Value::as_str) != Some("false")
    {
        return Err(invalid(
            "Debezium CDC metadata does not identify a non-snapshot record",
        ));
    }
    Ok(())
}

fn validate_envelope(columns: &[MySqlColumn], schema: &Row) -> Result<(), MySqlCdcScanError> {
    if schema.get("type").and_then(Value::as_str) != Some("struct") {
        return Err(invalid("envelope schema must be a struct"));
    }
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("envelope schema has no fields"))?;
    for row_name in ["before", "after"] {
        let mut matches = fields
            .iter()
            .filter(|field| field.get("field").and_then(Value::as_str) == Some(row_name));
        let row = matches
            .next()
            .ok_or_else(|| invalid("missing row schema"))?;
        if matches.next().is_some()
            || row.get("type").and_then(Value::as_str) != Some("struct")
            || row.get("optional").and_then(Value::as_bool) != Some(true)
        {
            return Err(invalid("row schema must be one optional struct"));
        }
        let fields = row
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("row schema has no fields"))?;
        if fields.len() != columns.len() {
            return Err(invalid("table schema changed its column count"));
        }
        for (column, field) in columns.iter().zip(fields) {
            let (literal, logical) = column.data_type().connect_type();
            let logical_matches = match (logical, field.get("name")) {
                (None, None) => true,
                (Some(expected), Some(Value::String(actual))) => actual == expected,
                _ => false,
            };
            if field.get("field").and_then(Value::as_str) != Some(column.name())
                || field.get("type").and_then(Value::as_str) != Some(literal)
                || !logical_matches
                || field.get("optional").and_then(Value::as_bool) != Some(column.is_nullable())
            {
                return Err(invalid(format!(
                    "schema changed at column {}",
                    column.name()
                )));
            }
            if let MySqlType::Decimal { precision, scale } = column.data_type() {
                let parameters = object_field(field, "parameters")?;
                if parameters.get("scale").and_then(Value::as_str)
                    != Some(scale.to_string().as_str())
                    || parameters
                        .get("connect.decimal.precision")
                        .and_then(Value::as_str)
                        != Some(precision.to_string().as_str())
                {
                    return Err(invalid(format!(
                        "decimal schema changed at column {}",
                        column.name()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_heartbeat(schema: &Row, payload: &Row) -> Result<(), MySqlCdcScanError> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .filter(|fields| fields.len() == 1)
        .ok_or_else(|| invalid("unexpected heartbeat schema"))?;
    let timestamp = &fields[0];
    if schema.get("type").and_then(Value::as_str) != Some("struct")
        || schema.get("name").and_then(Value::as_str)
            != Some("io.debezium.connector.common.Heartbeat")
        || timestamp.get("field").and_then(Value::as_str) != Some("ts_ms")
        || timestamp.get("type").and_then(Value::as_str) != Some("int64")
        || timestamp.get("optional").and_then(Value::as_bool) != Some(false)
        || timestamp.get("name").is_some()
        || payload.len() != 1
        || payload.get("ts_ms").and_then(Value::as_i64).is_none()
    {
        return Err(invalid("unexpected heartbeat record"));
    }
    Ok(())
}

fn complete_row(columns: &[MySqlColumn], value: Value) -> Result<Row, MySqlCdcScanError> {
    let Value::Object(row) = value else {
        return Err(invalid(
            "missing complete row image; the captured table requires binlog_row_image=FULL",
        ));
    };
    if row.len() != columns.len() {
        return Err(invalid(
            "row image does not contain exactly the declared columns",
        ));
    }
    for column in columns {
        let value = row
            .get(column.name())
            .ok_or_else(|| invalid(format!("row image is missing column {}", column.name())))?;
        if value.is_null() && !column.is_nullable() {
            return Err(invalid(format!(
                "non-null column {} contains null",
                column.name()
            )));
        }
    }
    Ok(row)
}

fn column_values<'a, T>(
    column: &MySqlColumn,
    rows: &'a [Row],
    parse: impl Fn(&'a Value) -> Option<T>,
) -> Result<Vec<Option<T>>, MySqlCdcScanError> {
    rows.iter()
        .map(|row| {
            let value = &row[column.name()];
            if value.is_null() {
                Ok(None)
            } else {
                parse(value).map(Some).ok_or_else(|| {
                    invalid(format!(
                        "invalid or unsupported value in column {}",
                        column.name()
                    ))
                })
            }
        })
        .collect()
}

fn column_array(column: &MySqlColumn, rows: &[Row]) -> Result<ArrayRef, MySqlCdcScanError> {
    Ok(match column.data_type() {
        MySqlType::Int16 => Arc::new(Int16Array::from(column_values(column, rows, |value| {
            i16::try_from(value.as_i64()?).ok()
        })?)),
        MySqlType::Int32 => Arc::new(Int32Array::from(column_values(column, rows, |value| {
            i32::try_from(value.as_i64()?).ok()
        })?)),
        MySqlType::Int64 => Arc::new(Int64Array::from(column_values(
            column,
            rows,
            Value::as_i64,
        )?)),
        MySqlType::Float64 => Arc::new(Float64Array::from(column_values(
            column,
            rows,
            parse_float64,
        )?)),
        MySqlType::Text => Arc::new(StringArray::from(column_values(
            column,
            rows,
            Value::as_str,
        )?)),
        MySqlType::Binary => {
            let values = column_values(column, rows, |value| {
                BASE64_STANDARD.decode(value.as_str()?).ok()
            })?;
            Arc::new(values.iter().map(Option::as_deref).collect::<BinaryArray>())
        }
        MySqlType::Decimal { precision, scale } => Arc::new(
            Decimal128Array::from(column_values(column, rows, |value| {
                parse_decimal(value, precision)
            })?)
            .with_precision_and_scale(precision, scale)?,
        ),
    })
}

fn parse_float64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| match value.as_str()? {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    })
}

fn parse_decimal(value: &Value, precision: u8) -> Option<i128> {
    let bytes = BASE64_STANDARD.decode(value.as_str()?).ok()?;
    let first = *bytes.first()?;
    if bytes.len() > size_of::<i128>() {
        return None;
    }
    let mut encoded = [if first & 0x80 == 0 { 0 } else { 0xff }; size_of::<i128>()];
    encoded[size_of::<i128>() - bytes.len()..].copy_from_slice(&bytes);
    let unscaled = i128::from_be_bytes(encoded);
    (unscaled.unsigned_abs() < 10_u128.pow(u32::from(precision))).then_some(unscaled)
}

fn invalid(message: impl Into<String>) -> MySqlCdcScanError {
    MySqlCdcScanError::InvalidRecord(message.into())
}
