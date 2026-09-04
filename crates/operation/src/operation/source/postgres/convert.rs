use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::SchemaRef;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use chrono::DateTime;
use dogpaddle_change::Change;
use dogpaddle_debezium::Record;
use serde_json::{Map, Value};

use super::{PostgresColumn, PostgresSourceError, PostgresType};

type Row = Map<String, Value>;

pub(super) fn convert_records(
    columns: &[PostgresColumn],
    output_schema: SchemaRef,
    topic_prefix: &str,
    table_schema: &str,
    table: &str,
    records: &[Record],
) -> Result<Option<Change>, PostgresSourceError> {
    convert_values(
        columns,
        output_schema,
        topic_prefix,
        table_schema,
        table,
        records
            .iter()
            .map(|record| (record.topic(), record.value())),
    )
}

// The byte-level boundary also lets tests use actual Connect JSON without
// exposing constructors for the runtime's owned Record capability.
pub(super) fn convert_values<'a>(
    columns: &[PostgresColumn],
    output_schema: SchemaRef,
    topic_prefix: &str,
    table_schema: &str,
    table: &str,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, PostgresSourceError> {
    let table_topic = format!("{topic_prefix}.{table_schema}.{table}");
    let heartbeat_topic = format!("__debezium-heartbeat.{topic_prefix}");
    let mut rows = Vec::new();
    let mut diffs = Vec::new();
    for (topic, bytes) in values {
        if topic != Some(table_topic.as_str()) && topic != Some(heartbeat_topic.as_str()) {
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
        validate_envelope(columns, schema)?;
        let payload = value
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("missing object field payload"))?;
        validate_source(payload, table_schema, table)?;
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

fn object_field<'a>(value: &'a Value, field: &str) -> Result<&'a Row, PostgresSourceError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("missing object field {field}")))
}

fn validate_source(
    payload: &Row,
    table_schema: &str,
    table: &str,
) -> Result<(), PostgresSourceError> {
    let source = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing source metadata"))?;
    for (field, expected) in [
        ("schema", table_schema),
        ("table", table),
        ("connector", "postgresql"),
    ] {
        if source.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "source metadata does not match configured {field}"
            )));
        }
    }
    // Debezium 3.6 SnapshotRecord.FALSE deliberately leaves the Struct field
    // unset. Our bridge disables default substitution, so Connect JSON uses
    // null here. Snapshot operations are independently rejected by the op guard.
    if !matches!(
        source.get("snapshot"),
        Some(Value::Null | Value::Bool(false))
    ) && source.get("snapshot").and_then(Value::as_str) != Some("false")
    {
        return Err(invalid(
            "source metadata does not identify a non-snapshot record",
        ));
    }
    Ok(())
}

fn validate_envelope(columns: &[PostgresColumn], schema: &Row) -> Result<(), PostgresSourceError> {
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
            if let PostgresType::Numeric { precision, scale } = column.data_type() {
                let parameters = object_field(field, "parameters")?;
                if parameters.get("scale").and_then(Value::as_str)
                    != Some(scale.to_string().as_str())
                    || parameters
                        .get("connect.decimal.precision")
                        .and_then(Value::as_str)
                        != Some(precision.to_string().as_str())
                {
                    return Err(invalid(format!(
                        "numeric schema changed at column {}",
                        column.name()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_heartbeat(schema: &Row, payload: &Row) -> Result<(), PostgresSourceError> {
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

fn complete_row(columns: &[PostgresColumn], value: Value) -> Result<Row, PostgresSourceError> {
    let Value::Object(row) = value else {
        return Err(invalid(
            "missing complete row image; the source table requires REPLICA IDENTITY FULL",
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
    column: &PostgresColumn,
    rows: &'a [Row],
    parse: impl Fn(&'a Value) -> Option<T>,
) -> Result<Vec<Option<T>>, PostgresSourceError> {
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

fn column_array(column: &PostgresColumn, rows: &[Row]) -> Result<ArrayRef, PostgresSourceError> {
    Ok(match column.data_type() {
        PostgresType::Boolean => Arc::new(BooleanArray::from(column_values(
            column,
            rows,
            Value::as_bool,
        )?)),
        PostgresType::Int16 => Arc::new(Int16Array::from(column_values(column, rows, |value| {
            i16::try_from(value.as_i64()?).ok()
        })?)),
        PostgresType::Int32 => Arc::new(Int32Array::from(column_values(column, rows, |value| {
            i32::try_from(value.as_i64()?).ok()
        })?)),
        PostgresType::Int64 => Arc::new(Int64Array::from(column_values(
            column,
            rows,
            Value::as_i64,
        )?)),
        PostgresType::Float32 => Arc::new(Float32Array::from(column_values(
            column,
            rows,
            parse_float32,
        )?)),
        PostgresType::Float64 => Arc::new(Float64Array::from(column_values(
            column,
            rows,
            parse_float64,
        )?)),
        PostgresType::Text => {
            let values = column_values(column, rows, |value| {
                let text = value.as_str()?;
                (text != "__debezium_unavailable_value").then_some(text)
            })?;
            Arc::new(StringArray::from(values))
        }
        PostgresType::Bytea => {
            let values = column_values(column, rows, |value| {
                let bytes = BASE64_STANDARD.decode(value.as_str()?).ok()?;
                (bytes != b"__debezium_unavailable_value").then_some(bytes)
            })?;
            Arc::new(values.iter().map(Option::as_deref).collect::<BinaryArray>())
        }
        PostgresType::Date => Arc::new(Date32Array::from(column_values(column, rows, |value| {
            let days = i32::try_from(value.as_i64()?).ok()?;
            // Earlier values include Debezium's wrapped PostgreSQL infinity sentinels.
            (days >= -2_440_588).then_some(days)
        })?)),
        PostgresType::Timestamp => Arc::new(TimestampMicrosecondArray::from(column_values(
            column,
            rows,
            |value| {
                let micros = value.as_i64()?;
                (!matches!(
                    micros,
                    9_223_372_036_825_200_000 | -9_223_372_036_832_400_000
                ))
                .then_some(micros)
            },
        )?)),
        PostgresType::TimestampTz => Arc::new(
            TimestampMicrosecondArray::from(column_values(column, rows, |value| {
                let timestamp = DateTime::parse_from_rfc3339(value.as_str()?).ok()?;
                (timestamp.timestamp_subsec_nanos() < 1_000_000_000
                    && timestamp.timestamp_subsec_nanos() % 1_000 == 0)
                    .then(|| timestamp.timestamp_micros())
            })?)
            .with_timezone("UTC"),
        ),
        PostgresType::Numeric { precision, scale } => Arc::new(
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

#[allow(clippy::cast_possible_truncation)]
fn parse_float32(value: &Value) -> Option<f32> {
    let parsed = parse_float64(value)?;
    let narrowed = parsed as f32;
    // Connect emits the shortest decimal spelling of the Java float. Parsing
    // that spelling as f32 restores the same value, but finite overflow is invalid.
    (!parsed.is_finite() || narrowed.is_finite()).then_some(narrowed)
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

fn invalid(message: impl Into<String>) -> PostgresSourceError {
    PostgresSourceError::InvalidRecord(message.into())
}
