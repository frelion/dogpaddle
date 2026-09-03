use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::{
    DateUnit as IpcDateUnit, Endianness, Field as IpcField, Message, MessageHeader,
    MetadataVersion, Precision, RecordBatch as IpcRecordBatch, Schema as IpcSchema,
    TimeUnit as IpcTimeUnit, Type as IpcType, root_as_message,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::CodecError;
use crate::{
    change::Change,
    schema::{
        MAX_NESTING_DEPTH, RESERVED_METADATA_PREFIX, valid_decimal128_parameters, validate_schema,
    },
};

const DIFF_FIELD_NAME: &str = "$dogpaddle.diff";
const CHANGE_KIND_KEY: &str = "dogpaddle.kind";
const CHANGE_KIND: &str = "change";
const CHANGE_VERSION_KEY: &str = "dogpaddle.change.version";
const CHANGE_VERSION: &str = "1";
const CANONICAL_CONTINUATION: &[u8; 4] = &[0xff; 4];
const CANONICAL_EOS: &[u8; 8] = &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

pub(super) struct ParsedChange<'encoded> {
    pub(super) physical_schema: SchemaRef,
    pub(super) logical_schema: SchemaRef,
    pub(super) batch: IpcRecordBatch<'encoded>,
    pub(super) body: &'encoded [u8],
    pub(super) row_count: usize,
}

pub(super) fn encode(change: &Change) -> Result<Vec<u8>, CodecError> {
    let physical = physical_batch(change)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), physical.schema_ref(), options)?;
    writer.write(&physical)?;
    Ok(writer.into_inner()?)
}

pub(super) fn parse(encoded: &[u8]) -> Result<ParsedChange<'_>, CodecError> {
    let schema_message = parse_message(encoded, 0, MessageHeader::Schema)?;
    if !schema_message.body.is_empty() {
        return Err(CodecError::invalid(
            "Schema message declares a non-empty body",
        ));
    }
    let embedded_schema = schema_message
        .message
        .header_as_schema()
        .ok_or_else(|| CodecError::invalid("Schema message has no Schema header"))?;
    if embedded_schema.endianness() != Endianness::Little {
        return Err(CodecError::invalid(
            "only little-endian Arrow Schemas are supported",
        ));
    }
    if embedded_schema
        .features()
        .is_some_and(|features| !features.is_empty())
    {
        return Err(CodecError::invalid(
            "Arrow Schema features are not supported",
        ));
    }
    let physical_schema = Arc::new(parse_schema(embedded_schema)?);
    let logical_schema = logical_schema(&physical_schema)?;

    let batch_message = parse_message(encoded, schema_message.end, MessageHeader::RecordBatch)?;
    if encoded.get(batch_message.end..) != Some(CANONICAL_EOS.as_slice()) {
        return Err(CodecError::invalid(
            "the first record batch must be followed by one canonical EOS marker and no other bytes",
        ));
    }
    let batch = batch_message
        .message
        .header_as_record_batch()
        .ok_or_else(|| CodecError::invalid("RecordBatch message has no RecordBatch header"))?;
    let row_count = usize::try_from(batch.length()).map_err(|_| {
        CodecError::invalid("RecordBatch row count is negative or does not fit this platform")
    })?;
    if row_count == 0 {
        return Err(CodecError::invalid(
            "RecordBatch must contain at least one row",
        ));
    }
    if batch.compression().is_some() {
        return Err(CodecError::invalid(
            "compressed RecordBatch messages are not supported",
        ));
    }
    if batch
        .variadicBufferCounts()
        .is_some_and(|counts| !counts.is_empty())
    {
        return Err(CodecError::invalid(
            "variadic RecordBatch buffers are not supported",
        ));
    }

    Ok(ParsedChange {
        physical_schema,
        logical_schema,
        batch,
        body: batch_message.body,
        row_count,
    })
}

// Arrow's general FlatBuffer converter is infallible and panics on malformed
// type parameters. Decode the deliberately narrow v1 type set directly.
fn parse_schema(embedded: IpcSchema<'_>) -> Result<Schema, CodecError> {
    let fields = embedded
        .fields()
        .ok_or_else(|| CodecError::invalid("Arrow Schema has no fields vector"))?
        .iter()
        .map(|field| parse_field(field, 0))
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = parse_metadata(embedded.custom_metadata());
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn parse_field(field: IpcField<'_>, depth: usize) -> Result<Field, CodecError> {
    let name = field.name().unwrap_or_default();
    if field.dictionary().is_some() {
        return Err(CodecError::invalid(format!(
            "dictionary encoding is not supported at Arrow field {name:?}"
        )));
    }

    let data_type = match field.type_type() {
        IpcType::Null => DataType::Null,
        IpcType::Bool => DataType::Boolean,
        IpcType::Int => parse_integer_type(field)?,
        IpcType::FloatingPoint => parse_floating_type(field)?,
        IpcType::Date => parse_date_type(field)?,
        IpcType::Timestamp => parse_timestamp_type(field)?,
        IpcType::Decimal => parse_decimal_type(field)?,
        IpcType::Binary => DataType::Binary,
        IpcType::Utf8 => DataType::Utf8,
        IpcType::List => {
            let children = field.children().ok_or_else(|| {
                CodecError::invalid(format!("List field {name:?} has no children vector"))
            })?;
            if children.len() != 1 {
                return Err(CodecError::invalid(format!(
                    "List field {name:?} must have exactly one child, found {}",
                    children.len()
                )));
            }
            DataType::List(Arc::new(parse_field(
                children.get(0),
                nested_depth(depth)?,
            )?))
        }
        IpcType::Struct_ => {
            let nested = nested_depth(depth)?;
            let children = field
                .children()
                .map(|children| {
                    children
                        .iter()
                        .map(|child| parse_field(child, nested))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            DataType::Struct(children.into())
        }
        data_type => {
            return Err(CodecError::invalid(format!(
                "unsupported Arrow IPC type {data_type:?} at field {name:?}"
            )));
        }
    };

    Ok(Field::new(name, data_type, field.nullable())
        .with_metadata(parse_metadata(field.custom_metadata())))
}

fn parse_date_type(field: IpcField<'_>) -> Result<DataType, CodecError> {
    let name = field.name().unwrap_or_default();
    let date = field.type_as_date().ok_or_else(|| {
        CodecError::invalid(format!("Date field {name:?} has no Date type table"))
    })?;
    match date.unit() {
        IpcDateUnit::DAY => Ok(DataType::Date32),
        unit => Err(CodecError::invalid(format!(
            "Date field {name:?} has unsupported unit {unit:?}; only DAY is supported"
        ))),
    }
}

fn parse_timestamp_type(field: IpcField<'_>) -> Result<DataType, CodecError> {
    let name = field.name().unwrap_or_default();
    let timestamp = field.type_as_timestamp().ok_or_else(|| {
        CodecError::invalid(format!(
            "Timestamp field {name:?} has no Timestamp type table"
        ))
    })?;
    let unit = match timestamp.unit() {
        IpcTimeUnit::SECOND => TimeUnit::Second,
        IpcTimeUnit::MILLISECOND => TimeUnit::Millisecond,
        IpcTimeUnit::MICROSECOND => TimeUnit::Microsecond,
        IpcTimeUnit::NANOSECOND => TimeUnit::Nanosecond,
        unit => {
            return Err(CodecError::invalid(format!(
                "Timestamp field {name:?} has unsupported unit {unit:?}"
            )));
        }
    };
    let timezone = timestamp.timezone();
    if timezone.is_some_and(str::is_empty) {
        return Err(CodecError::invalid(format!(
            "Timestamp field {name:?} has an empty timezone; use no timezone for a naive timestamp"
        )));
    }
    Ok(DataType::Timestamp(unit, timezone.map(Into::into)))
}

fn parse_decimal_type(field: IpcField<'_>) -> Result<DataType, CodecError> {
    let name = field.name().unwrap_or_default();
    let decimal = field.type_as_decimal().ok_or_else(|| {
        CodecError::invalid(format!("Decimal field {name:?} has no Decimal type table"))
    })?;
    if decimal.bitWidth() != 128 {
        return Err(CodecError::invalid(format!(
            "Decimal field {name:?} has unsupported bit width {}; only 128 is supported",
            decimal.bitWidth()
        )));
    }
    let precision = u8::try_from(decimal.precision()).map_err(|_| {
        CodecError::invalid(format!(
            "Decimal128 field {name:?} precision {} does not fit u8",
            decimal.precision()
        ))
    })?;
    let scale = i8::try_from(decimal.scale()).map_err(|_| {
        CodecError::invalid(format!(
            "Decimal128 field {name:?} scale {} does not fit i8",
            decimal.scale()
        ))
    })?;
    if !valid_decimal128_parameters(precision, scale) {
        return Err(CodecError::invalid(format!(
            "Decimal128 field {name:?} has invalid precision {precision} and scale {scale}; precision must be 1..=38 and a positive scale cannot exceed precision"
        )));
    }
    Ok(DataType::Decimal128(precision, scale))
}

fn parse_integer_type(field: IpcField<'_>) -> Result<DataType, CodecError> {
    let name = field.name().unwrap_or_default();
    let integer = field
        .type_as_int()
        .ok_or_else(|| CodecError::invalid(format!("Int field {name:?} has no Int type table")))?;
    match (integer.bitWidth(), integer.is_signed()) {
        (8, true) => Ok(DataType::Int8),
        (8, false) => Ok(DataType::UInt8),
        (16, true) => Ok(DataType::Int16),
        (16, false) => Ok(DataType::UInt16),
        (32, true) => Ok(DataType::Int32),
        (32, false) => Ok(DataType::UInt32),
        (64, true) => Ok(DataType::Int64),
        (64, false) => Ok(DataType::UInt64),
        (bit_width, is_signed) => Err(CodecError::invalid(format!(
            "Int field {name:?} has unsupported bit width {bit_width} and signedness {is_signed}"
        ))),
    }
}

fn parse_floating_type(field: IpcField<'_>) -> Result<DataType, CodecError> {
    let name = field.name().unwrap_or_default();
    let floating = field.type_as_floating_point().ok_or_else(|| {
        CodecError::invalid(format!(
            "FloatingPoint field {name:?} has no FloatingPoint type table"
        ))
    })?;
    match floating.precision() {
        Precision::SINGLE => Ok(DataType::Float32),
        Precision::DOUBLE => Ok(DataType::Float64),
        precision => Err(CodecError::invalid(format!(
            "FloatingPoint field {name:?} has unsupported precision {precision:?}"
        ))),
    }
}

fn nested_depth(depth: usize) -> Result<usize, CodecError> {
    let nested = depth
        .checked_add(1)
        .ok_or_else(|| CodecError::invalid("Arrow Schema nesting depth overflowed"))?;
    if nested > MAX_NESTING_DEPTH {
        Err(CodecError::invalid(format!(
            "Arrow Schema nesting exceeds the maximum depth of {MAX_NESTING_DEPTH}"
        )))
    } else {
        Ok(nested)
    }
}

fn parse_metadata<'a>(
    metadata: Option<
        flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<arrow_ipc::KeyValue<'a>>>,
    >,
) -> HashMap<String, String> {
    let mut parsed = HashMap::new();
    if let Some(metadata) = metadata {
        for pair in metadata {
            if let (Some(key), Some(value)) = (pair.key(), pair.value()) {
                parsed.insert(key.to_owned(), value.to_owned());
            }
        }
    }
    parsed
}

struct ParsedMessage<'encoded> {
    message: Message<'encoded>,
    body: &'encoded [u8],
    end: usize,
}

fn parse_message(
    encoded: &[u8],
    offset: usize,
    expected: MessageHeader,
) -> Result<ParsedMessage<'_>, CodecError> {
    if !offset.is_multiple_of(8) {
        return Err(CodecError::invalid(format!(
            "{expected:?} message is not 8-byte aligned"
        )));
    }
    let prefix = encoded
        .get(offset..)
        .and_then(|remaining| remaining.get(..8))
        .ok_or_else(|| CodecError::invalid(format!("{expected:?} message prefix is truncated")))?;
    if prefix[..4] != *CANONICAL_CONTINUATION {
        return Err(CodecError::invalid(format!(
            "{expected:?} message must use canonical non-legacy framing"
        )));
    }
    let metadata_len =
        usize::try_from(i32::from_le_bytes(prefix[4..].try_into().map_err(
            |_| CodecError::invalid("invalid IPC metadata length prefix"),
        )?))
        .map_err(|_| CodecError::invalid(format!("{expected:?} metadata length is negative")))?;
    if metadata_len == 0 || !metadata_len.is_multiple_of(8) {
        return Err(CodecError::invalid(format!(
            "{expected:?} metadata length must be positive and 8-byte aligned"
        )));
    }

    let metadata_start = offset
        .checked_add(prefix.len())
        .ok_or_else(|| CodecError::invalid("IPC metadata offset overflowed"))?;
    let metadata_end = metadata_start
        .checked_add(metadata_len)
        .ok_or_else(|| CodecError::invalid("IPC metadata length overflowed"))?;
    let metadata = encoded.get(metadata_start..metadata_end).ok_or_else(|| {
        CodecError::invalid(format!(
            "{expected:?} metadata length exceeds the encoded entry"
        ))
    })?;
    let message = root_as_message(metadata).map_err(|error| {
        CodecError::invalid(format!("invalid {expected:?} IPC metadata: {error}"))
    })?;
    if message.version() != MetadataVersion::V5 {
        return Err(CodecError::invalid(format!(
            "{expected:?} metadata version {:?} is not V5",
            message.version()
        )));
    }
    if message.header_type() != expected {
        return Err(CodecError::invalid(format!(
            "expected {expected:?} message, found {:?}",
            message.header_type()
        )));
    }
    if message
        .custom_metadata()
        .is_some_and(|metadata| !metadata.is_empty())
    {
        return Err(CodecError::invalid(format!(
            "{expected:?} message custom metadata is not supported"
        )));
    }

    let body_len = usize::try_from(message.bodyLength())
        .map_err(|_| CodecError::invalid(format!("{expected:?} body length is negative")))?;
    if !body_len.is_multiple_of(8) {
        return Err(CodecError::invalid(format!(
            "{expected:?} body length is not 8-byte aligned"
        )));
    }
    let body_end = metadata_end
        .checked_add(body_len)
        .ok_or_else(|| CodecError::invalid("IPC body length overflowed"))?;
    let body = encoded.get(metadata_end..body_end).ok_or_else(|| {
        CodecError::invalid(format!(
            "{expected:?} body length exceeds the encoded entry"
        ))
    })?;
    Ok(ParsedMessage {
        message,
        body,
        end: body_end,
    })
}

fn physical_batch(change: &Change) -> Result<RecordBatch, CodecError> {
    let mut columns = Vec::with_capacity(change.records().num_columns() + 1);
    columns.push(Arc::new(change.diffs().clone()) as ArrayRef);
    columns.extend(change.records().columns().iter().cloned());
    Ok(RecordBatch::try_new(
        physical_schema(change.records().schema_ref()),
        columns,
    )?)
}

pub(super) fn physical_schema(logical: &Schema) -> SchemaRef {
    let mut fields = Vec::with_capacity(logical.fields().len() + 1);
    fields.push(Arc::new(Field::new(
        DIFF_FIELD_NAME,
        DataType::Int64,
        false,
    )));
    fields.extend(logical.fields().iter().cloned());

    let mut metadata = logical.metadata().clone();
    metadata.insert(CHANGE_KIND_KEY.to_owned(), CHANGE_KIND.to_owned());
    metadata.insert(CHANGE_VERSION_KEY.to_owned(), CHANGE_VERSION.to_owned());
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

fn logical_schema(physical: &Schema) -> Result<SchemaRef, CodecError> {
    let (diff, logical_fields) = physical
        .fields()
        .split_first()
        .ok_or_else(|| CodecError::invalid("physical Schema has no diff field"))?;
    let expected_diff = Field::new(DIFF_FIELD_NAME, DataType::Int64, false);
    if diff.as_ref() != &expected_diff {
        return Err(CodecError::invalid(format!(
            "first physical field must be the non-null Int64 field {DIFF_FIELD_NAME:?}"
        )));
    }

    let mut metadata = physical.metadata().clone();
    match metadata.remove(CHANGE_KIND_KEY) {
        Some(kind) if kind == CHANGE_KIND => {}
        Some(kind) => {
            return Err(CodecError::invalid(format!(
                "Arrow Schema kind is {kind:?}, expected {CHANGE_KIND:?}"
            )));
        }
        None => {
            return Err(CodecError::invalid(format!(
                "Arrow Schema is missing {CHANGE_KIND_KEY:?}"
            )));
        }
    }
    match metadata.remove(CHANGE_VERSION_KEY) {
        Some(version) if version == CHANGE_VERSION => {}
        Some(version) => return Err(CodecError::UnsupportedVersion { version }),
        None => {
            return Err(CodecError::invalid(format!(
                "Arrow Schema is missing {CHANGE_VERSION_KEY:?}"
            )));
        }
    }
    if let Some(key) = metadata
        .keys()
        .filter(|key| key.starts_with(RESERVED_METADATA_PREFIX))
        .min()
    {
        return Err(CodecError::invalid(format!(
            "Arrow Schema contains unknown reserved metadata key {key:?}"
        )));
    }

    let logical = Arc::new(Schema::new_with_metadata(logical_fields.to_vec(), metadata));
    validate_schema(&logical)?;
    Ok(logical)
}
