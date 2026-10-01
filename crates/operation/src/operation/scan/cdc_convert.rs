//! Connect row validation and Arrow construction shared by the two CDC sources.

use std::{collections::HashSet, io, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use base64::{Engine as _, prelude::BASE64_STANDARD, read::DecoderReader};
use chrono::DateTime;
use dogpaddle_change::{Change, ChangeError};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

pub(super) type Row = Map<String, Value>;

#[derive(Debug, thiserror::Error)]
pub(super) enum ConvertError {
    #[error("{0}")]
    Invalid(String),
    #[error("missing complete row image")]
    IncompleteImage,
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Change(#[from] ChangeError),
}

fn invalid(message: impl Into<String>) -> ConvertError {
    ConvertError::Invalid(message.into())
}

// Arrow DataType is recursive, while source columns are flat. Inspect only the
// outer type shape before asking Arrow to deserialize the field recursively.
pub(super) fn deserialize_flat_fields<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Fields, D::Error> {
    let columns = Vec::<Value>::deserialize(decoder)?;
    if columns.is_empty() || columns.len() > 1600 {
        return Err(serde::de::Error::custom(
            "table must have between 1 and 1600 columns",
        ));
    }
    for column in &columns {
        let flat = match column.get("data_type") {
            Some(Value::String(_)) => true,
            Some(Value::Object(kind)) if kind.len() == 1 => {
                kind.contains_key("Decimal128") || kind.contains_key("Timestamp")
            }
            _ => false,
        };
        if !flat {
            return Err(serde::de::Error::custom("source column type must be flat"));
        }
    }
    serde_json::from_value(Value::Array(columns)).map_err(serde::de::Error::custom)
}

pub(super) fn source_schema(
    columns: &Fields,
    supported: impl Fn(&DataType) -> bool,
) -> Result<SchemaRef, String> {
    if columns.is_empty() || columns.len() > 1600 {
        return Err("table must have between 1 and 1600 columns".into());
    }
    let mut names = HashSet::with_capacity(columns.len());
    for field in columns {
        if !supported(field.data_type()) {
            return Err("unsupported source column type".into());
        }
        if field.name().is_empty() || field.name().contains('\0') || !names.insert(field.name()) {
            return Err("column names must be nonempty, NUL-free, and unique".into());
        }
        if !field.metadata().is_empty() {
            return Err("source column metadata must be empty".into());
        }
    }
    let schema = Arc::new(Schema::new(columns.clone()));
    dogpaddle_change::validate_schema(&schema).map_err(|error| error.to_string())?;
    Ok(schema)
}

pub(super) fn connect_type(
    data_type: &DataType,
) -> Result<(&'static str, Option<&'static str>), ConvertError> {
    Ok(match data_type {
        DataType::Boolean => ("boolean", None),
        DataType::Int16 => ("int16", None),
        DataType::Int32 => ("int32", None),
        DataType::Int64 => ("int64", None),
        DataType::Float32 => ("float", None),
        DataType::Float64 => ("double", None),
        DataType::Utf8 => ("string", None),
        DataType::Binary => ("bytes", None),
        DataType::Date32 => ("int32", Some("io.debezium.time.Date")),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            ("int64", Some("io.debezium.time.MicroTimestamp"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
            ("string", Some("io.debezium.time.ZonedTimestamp"))
        }
        DataType::Decimal128(precision, scale) if valid_decimal(*precision, *scale) => {
            ("bytes", Some("org.apache.kafka.connect.data.Decimal"))
        }
        _ => return Err(invalid("unsupported source column type")),
    })
}

pub(super) fn valid_decimal(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision)
        && scale >= 0
        && u8::try_from(scale).is_ok_and(|scale| scale <= precision)
}

fn object_field<'a>(value: &'a Value, field: &str) -> Result<&'a Row, ConvertError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("missing object field {field}")))
}

pub(super) fn validate_snapshot_notification(payload: &Row) -> Result<bool, ConvertError> {
    if payload.get("aggregate_type").and_then(Value::as_str) != Some("Initial Snapshot") {
        return Err(invalid("unexpected Debezium notification aggregate"));
    }
    match payload.get("type").and_then(Value::as_str) {
        Some("STARTED" | "IN_PROGRESS" | "TABLE_SCAN_COMPLETED") => Ok(false),
        Some("COMPLETED") => Ok(true),
        Some("ABORTED" | "SKIPPED") => Err(invalid(
            "Debezium initial snapshot did not complete successfully",
        )),
        _ => Err(invalid("unexpected Debezium initial snapshot notification")),
    }
}

pub(super) fn validate_envelope(columns: &Fields, schema: &Row) -> Result<(), ConvertError> {
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
            let kind = column.data_type();
            let (literal, logical) = connect_type(kind)?;
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
            if let DataType::Decimal128(precision, scale) = kind {
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

pub(super) fn validate_heartbeat(schema: &Row, payload: &Row) -> Result<(), ConvertError> {
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

#[allow(clippy::cast_possible_truncation)] // A validated source has at most 1600 columns.
pub(super) fn complete_row(
    columns: &Fields,
    output_projection: &[u32],
    value: Value,
) -> Result<Row, ConvertError> {
    let Value::Object(mut row) = value else {
        return Err(ConvertError::IncompleteImage);
    };
    if row.len() != columns.len() {
        return Err(invalid(
            "row image does not contain exactly the declared columns",
        ));
    }
    let mut projection = output_projection.iter().copied();
    let mut next = projection.next();
    for (index, column) in columns.iter().enumerate() {
        {
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
        if next == Some(index as u32) {
            next = projection.next();
        } else {
            validate_column_value(column, &row[column.name()])?;
            let _ = row.remove(column.name());
        }
    }
    Ok(row)
}

pub(super) fn build_change(
    columns: &Fields,
    output_projection: &[u32],
    output_schema: SchemaRef,
    rows: &[Row],
    diffs: Vec<i64>,
) -> Result<Option<Change>, ConvertError> {
    if rows.is_empty() {
        return Ok(None);
    }
    let arrays = output_projection
        .iter()
        .map(|index| {
            let column = usize::try_from(*index)
                .ok()
                .and_then(|index| columns.get(index))
                .ok_or_else(|| invalid("output projection is outside the source schema"))?;
            column_array(column, rows)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    let records = RecordBatch::try_new_with_options(output_schema, arrays, &options)?;
    Ok(Some(Change::try_new(records, Int64Array::from(diffs))?))
}

fn column_values<'a, T>(
    column: &Field,
    rows: &'a [Row],
    parse: impl Fn(&'a Value) -> Option<T>,
) -> Result<Vec<Option<T>>, ConvertError> {
    rows.iter()
        .map(|row| {
            let value = &row[column.name()];
            if value.is_null() {
                Ok(None)
            } else {
                parse(value)
                    .map(Some)
                    .ok_or_else(|| invalid_column_value(column))
            }
        })
        .collect()
}

fn validate_column_value(column: &Field, value: &Value) -> Result<(), ConvertError> {
    let valid = value.is_null()
        || match column.data_type() {
            DataType::Boolean => value.as_bool().is_some(),
            DataType::Int16 => parse_int16(value).is_some(),
            DataType::Int32 => parse_int32(value).is_some(),
            DataType::Int64 => value.as_i64().is_some(),
            DataType::Float32 => parse_float32(value).is_some(),
            DataType::Float64 => parse_float64(value).is_some(),
            DataType::Utf8 => value.as_str().is_some(),
            DataType::Binary => valid_binary(value),
            DataType::Date32 => parse_date(value).is_some(),
            DataType::Timestamp(TimeUnit::Microsecond, None) => parse_timestamp(value).is_some(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
                parse_timestamp_tz(value).is_some()
            }
            DataType::Decimal128(precision, _) => parse_decimal(value, *precision).is_some(),
            _ => false,
        };
    valid
        .then_some(())
        .ok_or_else(|| invalid_column_value(column))
}

fn invalid_column_value(column: &Field) -> ConvertError {
    invalid(format!(
        "invalid or unsupported value in column {}",
        column.name()
    ))
}

fn column_array(column: &Field, rows: &[Row]) -> Result<ArrayRef, ConvertError> {
    Ok(match column.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(column_values(
            column,
            rows,
            Value::as_bool,
        )?)),
        DataType::Int16 => Arc::new(Int16Array::from(column_values(column, rows, parse_int16)?)),
        DataType::Int32 => Arc::new(Int32Array::from(column_values(column, rows, parse_int32)?)),
        DataType::Int64 => Arc::new(Int64Array::from(column_values(
            column,
            rows,
            Value::as_i64,
        )?)),
        DataType::Float32 => Arc::new(Float32Array::from(column_values(
            column,
            rows,
            parse_float32,
        )?)),
        DataType::Float64 => Arc::new(Float64Array::from(column_values(
            column,
            rows,
            parse_float64,
        )?)),
        DataType::Utf8 => Arc::new(StringArray::from(column_values(
            column,
            rows,
            Value::as_str,
        )?)),
        DataType::Binary => {
            let values = column_values(column, rows, parse_binary)?;
            Arc::new(values.iter().map(Option::as_deref).collect::<BinaryArray>())
        }
        DataType::Date32 => Arc::new(Date32Array::from(column_values(column, rows, parse_date)?)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(
            TimestampMicrosecondArray::from(column_values(column, rows, parse_timestamp)?),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
            Arc::new(
                TimestampMicrosecondArray::from(column_values(column, rows, parse_timestamp_tz)?)
                    .with_timezone("UTC"),
            )
        }
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from(column_values(column, rows, |value| {
                parse_decimal(value, *precision)
            })?)
            .with_precision_and_scale(*precision, *scale)?,
        ),
        _ => return Err(invalid("unsupported source column type")),
    })
}

fn parse_int16(value: &Value) -> Option<i16> {
    i16::try_from(value.as_i64()?).ok()
}

fn parse_int32(value: &Value) -> Option<i32> {
    i32::try_from(value.as_i64()?).ok()
}

fn parse_binary(value: &Value) -> Option<Vec<u8>> {
    BASE64_STANDARD.decode(value.as_str()?).ok()
}

fn valid_binary(value: &Value) -> bool {
    value.as_str().is_some_and(|encoded| {
        let mut decoder = DecoderReader::new(encoded.as_bytes(), &BASE64_STANDARD);
        io::copy(&mut decoder, &mut io::sink()).is_ok()
    })
}

fn parse_date(value: &Value) -> Option<i32> {
    let days = i32::try_from(value.as_i64()?).ok()?;
    // Earlier values include Debezium's wrapped PostgreSQL infinity sentinels.
    (days >= -2_440_588).then_some(days)
}

fn parse_timestamp(value: &Value) -> Option<i64> {
    let micros = value.as_i64()?;
    (!matches!(
        micros,
        9_223_372_036_825_200_000 | -9_223_372_036_832_400_000
    ))
    .then_some(micros)
}

fn parse_timestamp_tz(value: &Value) -> Option<i64> {
    let timestamp = DateTime::parse_from_rfc3339(value.as_str()?).ok()?;
    (timestamp.timestamp_subsec_nanos() < 1_000_000_000
        && timestamp.timestamp_subsec_nanos() % 1_000 == 0)
        .then(|| timestamp.timestamp_micros())
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
    if !(1..=38).contains(&precision) {
        return None;
    }
    let encoded_value = value.as_str()?;
    // At most 16 decoded bytes fit in i128; a 24-byte Base64 input needs
    // an 18-byte output slice for the decoder's conservative size estimate.
    if encoded_value.len() > 24 {
        return None;
    }
    let mut decoded = [0; 18];
    let decoded_len = BASE64_STANDARD
        .decode_slice(encoded_value, &mut decoded)
        .ok()?;
    let bytes = decoded.get(..decoded_len)?;
    let first = *bytes.first()?;
    if bytes.len() > size_of::<i128>() {
        return None;
    }
    let mut encoded = [if first & 0x80 == 0 { 0 } else { 0xff }; size_of::<i128>()];
    encoded[size_of::<i128>() - bytes.len()..].copy_from_slice(bytes);
    let unscaled = i128::from_be_bytes(encoded);
    (unscaled.unsigned_abs() < 10_u128.pow(u32::from(precision))).then_some(unscaled)
}
