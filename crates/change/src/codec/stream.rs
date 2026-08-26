use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::{
    Endianness, Message, MessageHeader, MetadataVersion, RecordBatch as IpcRecordBatch,
    convert::fb_to_schema,
    root_as_message,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::CodecError;
use crate::{
    change::Change,
    schema::{RESERVED_METADATA_PREFIX, validate_schema},
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
    let physical_schema = Arc::new(fb_to_schema(embedded_schema));
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
