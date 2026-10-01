use std::{io::Write, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::{
    Message, MessageHeader, MetadataVersion, RecordBatch as IpcRecordBatch,
    root_as_message_with_opts, writer::IpcWriteOptions,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::CodecError;
use crate::Change;

const DIFF_FIELD_NAME: &str = "$dogpaddle.diff";
const CHANGE_KIND_KEY: &str = "dogpaddle.kind";
const CHANGE_KIND: &str = "change";
const CHANGE_VERSION_KEY: &str = "dogpaddle.change.version";
const CHANGE_VERSION: &str = "1";
const CANONICAL_CONTINUATION: &[u8; 4] = &[0xff; 4];
const CANONICAL_EOS: &[u8; 8] = &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
// A valid entry has only Message and RecordBatch tables; nodes/buffers are struct vectors.
const MAX_FLATBUFFER_TABLES: usize = 1024;
const MAX_FLATBUFFER_APPARENT_SIZE: usize = 64 * 1024 * 1024;

pub(super) struct ParsedChange<'encoded> {
    pub(super) physical_schema: SchemaRef,
    pub(super) logical_schema: SchemaRef,
    pub(super) batch: IpcRecordBatch<'encoded>,
    pub(super) body: &'encoded [u8],
    pub(super) row_count: usize,
}

pub(super) struct BoundedWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    limit_hit: bool,
}

impl BoundedWriter {
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(64 * 1024)),
            max_bytes,
            limit_hit: false,
        }
    }

    pub(super) fn remaining(&self) -> usize {
        self.max_bytes - self.bytes.len()
    }

    pub(super) const fn limit_hit(&self) -> bool {
        self.limit_hit
    }

    pub(super) fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(input.len())
            .is_none_or(|length| length > self.max_bytes)
        {
            self.limit_hit = true;
            return Err(std::io::Error::other(
                "DogPaddle Change size limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn parse_record_batch(
    encoded: &[u8],
    offset: usize,
    physical_schema: SchemaRef,
    logical_schema: SchemaRef,
) -> Result<ParsedChange<'_>, CodecError> {
    let batch_message = parse_message(encoded, offset, MessageHeader::RecordBatch)?;
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
    let verifier = flatbuffers::VerifierOptions {
        max_tables: MAX_FLATBUFFER_TABLES,
        max_apparent_size: MAX_FLATBUFFER_APPARENT_SIZE,
        ..flatbuffers::VerifierOptions::default()
    };
    let message = root_as_message_with_opts(&verifier, metadata).map_err(|error| {
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

pub(super) fn physical_batch(
    change: &Change,
    physical_schema: SchemaRef,
) -> Result<RecordBatch, CodecError> {
    let mut columns = Vec::with_capacity(change.records().num_columns() + 1);
    columns.push(Arc::new(change.diffs().clone()) as ArrayRef);
    columns.extend(change.records().columns().iter().cloned());
    Ok(RecordBatch::try_new(physical_schema, columns)?)
}

pub(super) fn write_options() -> Result<IpcWriteOptions, CodecError> {
    Ok(IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?)
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
