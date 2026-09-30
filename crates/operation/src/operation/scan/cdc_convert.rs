//! Connect row validation and Arrow construction shared by the two CDC sources.

use std::{io, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};
use base64::{Engine as _, prelude::BASE64_STANDARD, read::DecoderReader};
use chrono::DateTime;
use dogpaddle_change::{Change, ChangeError};
use serde_json::{Map, Value};

pub(super) type Row = Map<String, Value>;

/// The Connect representation of one supported Arrow column.
#[derive(Clone, Copy)]
pub(super) enum WireKind {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Decimal { precision: u8, scale: i8 },
}

impl WireKind {
    pub(super) fn arrow_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Text => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
        }
    }

    pub(super) const fn connect_type(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::Boolean => ("boolean", None),
            Self::Int16 => ("int16", None),
            Self::Int32 => ("int32", None),
            Self::Int64 => ("int64", None),
            Self::Float32 => ("float", None),
            Self::Float64 => ("double", None),
            Self::Text => ("string", None),
            Self::Binary => ("bytes", None),
            Self::Date => ("int32", Some("io.debezium.time.Date")),
            Self::Timestamp => ("int64", Some("io.debezium.time.MicroTimestamp")),
            Self::TimestampTz => ("string", Some("io.debezium.time.ZonedTimestamp")),
            Self::Decimal { .. } => ("bytes", Some("org.apache.kafka.connect.data.Decimal")),
        }
    }
}

/// Binds the common wire algorithm to one source's declared column and error type.
pub(super) trait WireColumn {
    type Error: From<ArrowError> + From<ChangeError>;

    const INCOMPLETE_IMAGE: &'static str;
    const DECIMAL_LABEL: &'static str;

    fn name(&self) -> &str;
    fn nullable(&self) -> bool;
    fn wire_kind(&self) -> WireKind;
    fn invalid_record(message: String) -> Self::Error;
}

fn invalid<C: WireColumn>(message: impl Into<String>) -> C::Error {
    C::invalid_record(message.into())
}

fn object_field<'a, C: WireColumn>(value: &'a Value, field: &str) -> Result<&'a Row, C::Error> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid::<C>(format!("missing object field {field}")))
}

pub(super) fn validate_snapshot_notification<C: WireColumn>(
    payload: &Row,
) -> Result<bool, C::Error> {
    if payload.get("aggregate_type").and_then(Value::as_str) != Some("Initial Snapshot") {
        return Err(invalid::<C>("unexpected Debezium notification aggregate"));
    }
    match payload.get("type").and_then(Value::as_str) {
        Some("STARTED" | "IN_PROGRESS" | "TABLE_SCAN_COMPLETED") => Ok(false),
        Some("COMPLETED") => Ok(true),
        Some("ABORTED" | "SKIPPED") => Err(invalid::<C>(
            "Debezium initial snapshot did not complete successfully",
        )),
        _ => Err(invalid::<C>(
            "unexpected Debezium initial snapshot notification",
        )),
    }
}

pub(super) fn validate_envelope<C: WireColumn>(
    columns: &[C],
    schema: &Row,
) -> Result<(), C::Error> {
    if schema.get("type").and_then(Value::as_str) != Some("struct") {
        return Err(invalid::<C>("envelope schema must be a struct"));
    }
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid::<C>("envelope schema has no fields"))?;
    for row_name in ["before", "after"] {
        let mut matches = fields
            .iter()
            .filter(|field| field.get("field").and_then(Value::as_str) == Some(row_name));
        let row = matches
            .next()
            .ok_or_else(|| invalid::<C>("missing row schema"))?;
        if matches.next().is_some()
            || row.get("type").and_then(Value::as_str) != Some("struct")
            || row.get("optional").and_then(Value::as_bool) != Some(true)
        {
            return Err(invalid::<C>("row schema must be one optional struct"));
        }
        let fields = row
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid::<C>("row schema has no fields"))?;
        if fields.len() != columns.len() {
            return Err(invalid::<C>("table schema changed its column count"));
        }
        for (column, field) in columns.iter().zip(fields) {
            let kind = column.wire_kind();
            let (literal, logical) = kind.connect_type();
            let logical_matches = match (logical, field.get("name")) {
                (None, None) => true,
                (Some(expected), Some(Value::String(actual))) => actual == expected,
                _ => false,
            };
            if field.get("field").and_then(Value::as_str) != Some(column.name())
                || field.get("type").and_then(Value::as_str) != Some(literal)
                || !logical_matches
                || field.get("optional").and_then(Value::as_bool) != Some(column.nullable())
            {
                return Err(invalid::<C>(format!(
                    "schema changed at column {}",
                    column.name()
                )));
            }
            if let WireKind::Decimal { precision, scale } = kind {
                let parameters = object_field::<C>(field, "parameters")?;
                if parameters.get("scale").and_then(Value::as_str)
                    != Some(scale.to_string().as_str())
                    || parameters
                        .get("connect.decimal.precision")
                        .and_then(Value::as_str)
                        != Some(precision.to_string().as_str())
                {
                    return Err(invalid::<C>(format!(
                        "{} schema changed at column {}",
                        C::DECIMAL_LABEL,
                        column.name()
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_heartbeat<C: WireColumn>(
    schema: &Row,
    payload: &Row,
) -> Result<(), C::Error> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .filter(|fields| fields.len() == 1)
        .ok_or_else(|| invalid::<C>("unexpected heartbeat schema"))?;
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
        return Err(invalid::<C>("unexpected heartbeat record"));
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)] // A validated source has at most 1600 columns.
pub(super) fn complete_row<C: WireColumn>(
    columns: &[C],
    output_projection: &[u32],
    value: Value,
) -> Result<Row, C::Error> {
    let Value::Object(mut row) = value else {
        return Err(invalid::<C>(C::INCOMPLETE_IMAGE));
    };
    if row.len() != columns.len() {
        return Err(invalid::<C>(
            "row image does not contain exactly the declared columns",
        ));
    }
    let mut projection = output_projection.iter().copied();
    let mut next = projection.next();
    for (index, column) in columns.iter().enumerate() {
        {
            let value = row.get(column.name()).ok_or_else(|| {
                invalid::<C>(format!("row image is missing column {}", column.name()))
            })?;
            if value.is_null() && !column.nullable() {
                return Err(invalid::<C>(format!(
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

pub(super) fn build_change<C: WireColumn>(
    columns: &[C],
    output_projection: &[u32],
    output_schema: SchemaRef,
    rows: &[Row],
    diffs: Vec<i64>,
) -> Result<Option<Change>, C::Error> {
    if rows.is_empty() {
        return Ok(None);
    }
    let arrays = output_projection
        .iter()
        .map(|index| {
            let column = usize::try_from(*index)
                .ok()
                .and_then(|index| columns.get(index))
                .ok_or_else(|| invalid::<C>("output projection is outside the source schema"))?;
            column_array(column, rows)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    let records = RecordBatch::try_new_with_options(output_schema, arrays, &options)?;
    Ok(Some(Change::try_new(records, Int64Array::from(diffs))?))
}

fn column_values<'a, C: WireColumn, T>(
    column: &C,
    rows: &'a [Row],
    parse: impl Fn(&'a Value) -> Option<T>,
) -> Result<Vec<Option<T>>, C::Error> {
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

fn validate_column_value<C: WireColumn>(column: &C, value: &Value) -> Result<(), C::Error> {
    let valid = value.is_null()
        || match column.wire_kind() {
            WireKind::Boolean => value.as_bool().is_some(),
            WireKind::Int16 => parse_int16(value).is_some(),
            WireKind::Int32 => parse_int32(value).is_some(),
            WireKind::Int64 => value.as_i64().is_some(),
            WireKind::Float32 => parse_float32(value).is_some(),
            WireKind::Float64 => parse_float64(value).is_some(),
            WireKind::Text => value.as_str().is_some(),
            WireKind::Binary => valid_binary(value),
            WireKind::Date => parse_date(value).is_some(),
            WireKind::Timestamp => parse_timestamp(value).is_some(),
            WireKind::TimestampTz => parse_timestamp_tz(value).is_some(),
            WireKind::Decimal { precision, .. } => parse_decimal(value, precision).is_some(),
        };
    valid
        .then_some(())
        .ok_or_else(|| invalid_column_value(column))
}

fn invalid_column_value<C: WireColumn>(column: &C) -> C::Error {
    invalid::<C>(format!(
        "invalid or unsupported value in column {}",
        column.name()
    ))
}

fn column_array<C: WireColumn>(column: &C, rows: &[Row]) -> Result<ArrayRef, C::Error> {
    Ok(match column.wire_kind() {
        WireKind::Boolean => Arc::new(BooleanArray::from(column_values(
            column,
            rows,
            Value::as_bool,
        )?)),
        WireKind::Int16 => Arc::new(Int16Array::from(column_values(column, rows, parse_int16)?)),
        WireKind::Int32 => Arc::new(Int32Array::from(column_values(column, rows, parse_int32)?)),
        WireKind::Int64 => Arc::new(Int64Array::from(column_values(
            column,
            rows,
            Value::as_i64,
        )?)),
        WireKind::Float32 => Arc::new(Float32Array::from(column_values(
            column,
            rows,
            parse_float32,
        )?)),
        WireKind::Float64 => Arc::new(Float64Array::from(column_values(
            column,
            rows,
            parse_float64,
        )?)),
        WireKind::Text => Arc::new(StringArray::from(column_values(
            column,
            rows,
            Value::as_str,
        )?)),
        WireKind::Binary => {
            let values = column_values(column, rows, parse_binary)?;
            Arc::new(values.iter().map(Option::as_deref).collect::<BinaryArray>())
        }
        WireKind::Date => Arc::new(Date32Array::from(column_values(column, rows, parse_date)?)),
        WireKind::Timestamp => Arc::new(TimestampMicrosecondArray::from(column_values(
            column,
            rows,
            parse_timestamp,
        )?)),
        WireKind::TimestampTz => Arc::new(
            TimestampMicrosecondArray::from(column_values(column, rows, parse_timestamp_tz)?)
                .with_timezone("UTC"),
        ),
        WireKind::Decimal { precision, scale } => Arc::new(
            Decimal128Array::from(column_values(column, rows, |value| {
                parse_decimal(value, precision)
            })?)
            .with_precision_and_scale(precision, scale)?,
        ),
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
