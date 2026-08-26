use std::{
    io::Cursor,
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_ipc::{
    Endianness, Message, MessageHeader, MetadataVersion,
    reader::StreamReader,
    root_as_message,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use thiserror::Error;

use crate::{
    Change, ChangeError, ChangeProjection, ProjectionError, SchemaError,
    schema::RESERVED_METADATA_PREFIX, validate_schema,
};

mod projected;

const DIFF_FIELD_NAME: &str = "$dogpaddle.diff";
const CHANGE_KIND_KEY: &str = "dogpaddle.kind";
const CHANGE_KIND: &str = "change";
const CHANGE_VERSION_KEY: &str = "dogpaddle.change.version";
const CHANGE_VERSION: &str = "1";
const CANONICAL_CONTINUATION: &[u8; 4] = &[0xff, 0xff, 0xff, 0xff];
const CANONICAL_EOS: &[u8; 8] = &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// Encodes one self-contained change as a standard Arrow IPC stream.
///
/// The stream contains one physical Schema, exactly one `RecordBatch`, and the
/// canonical end-of-stream marker. The physical Schema prepends the non-null
/// `$dogpaddle.diff` `Int64` field and carries `DogPaddle`'s kind and format
/// version markers in Arrow Schema metadata. Record rows and their paired diffs
/// are written in their original event order without sorting or consolidation.
///
/// # Errors
///
/// Returns `CodecError` when the logical Schema is unsupported, the current
/// target is not little-endian, or Arrow cannot encode the physical batch.
pub fn encode_change(change: &Change) -> Result<Vec<u8>, CodecError> {
    ensure_little_endian_target()?;
    let physical = physical_batch(change)?;
    let options = write_options()?;
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), physical.schema_ref(), options)?;
    writer.write(&physical)?;
    Ok(writer.into_inner()?)
}

/// Decodes one self-contained `DogPaddle` change from an Arrow IPC stream.
///
/// The decoder requires the canonical v1 shape produced by [`encode_change`]:
/// one marked physical Schema, exactly one non-empty `RecordBatch`, and one
/// canonical end-of-stream marker with no trailing bytes. Decoding preserves
/// the encoded event order exactly.
///
/// # Errors
///
/// Returns `CodecError` for malformed Arrow IPC, a missing or unsupported
/// `DogPaddle` format marker, an invalid physical Schema, more or fewer than one
/// batch, non-canonical framing, or invalid differences.
pub fn decode_change(encoded: &[u8]) -> Result<Change, CodecError> {
    ensure_little_endian_target()?;
    catch_unwind(AssertUnwindSafe(|| decode_change_inner(encoded)))
        .map_err(|_| CodecError::invalid("Arrow IPC decoding panicked"))?
}

/// Decodes selected top-level logical fields from one self-contained Change.
///
/// The embedded Schema, stream framing, complete `RecordBatch` metadata, and
/// every buffer descriptor are validated before body access. Only the physical
/// diff buffers and the complete buffer subtrees of fields selected by
/// `projection` are then copied into owned Arrow memory and decoded. The
/// returned value is an ordinary `Change`, not a lazy view over `encoded`.
///
/// Value-level invariants of unselected field bodies, such as UTF-8 contents or
/// nested offsets, cannot be validated without reading those bodies. Use
/// [`decode_change`] when every logical field must be fully decoded and
/// validated.
///
/// # Errors
///
/// Returns `CodecError` for malformed or non-canonical Arrow IPC, an invalid
/// `DogPaddle` physical Schema, a Schema different from the one bound to
/// `projection`, malformed batch metadata, invalid selected Arrow values, or
/// invalid differences.
pub fn decode_change_projected(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, CodecError> {
    ensure_little_endian_target()?;
    catch_unwind(AssertUnwindSafe(|| projected::decode(encoded, projection)))
        .map_err(|_| CodecError::invalid("Arrow IPC projected decoding panicked"))?
}

/// A self-contained Change encoding failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The logical Schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// The decoded change violates its logical invariants.
    #[error(transparent)]
    Change(#[from] ChangeError),
    /// The requested logical projection is invalid for this Change.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    /// The embedded `DogPaddle` Change format version is unsupported.
    #[error("unsupported DogPaddle Change format version {version:?}")]
    UnsupportedVersion {
        /// Unsupported metadata value.
        version: String,
    },
    /// The current target cannot safely interpret v1 Arrow IPC buffers.
    #[error("DogPaddle Change v1 only supports little-endian targets")]
    UnsupportedTargetEndianness,
    /// The encoding is not a canonical `DogPaddle` Change stream.
    #[error("invalid DogPaddle Change encoding: {message}")]
    InvalidEncoding {
        /// Diagnostic reason.
        message: String,
    },
    /// Arrow IPC rejected the Schema, stream, or record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

impl CodecError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidEncoding {
            message: message.into(),
        }
    }
}

fn decode_change_inner(encoded: &[u8]) -> Result<Change, CodecError> {
    let layout = preflight_stream(encoded)?;
    let cursor = Cursor::new(encoded);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let logical_schema = logical_schema(reader.schema().as_ref())?;
    require_position(&reader, layout.schema.end(), "Schema")?;

    let physical = reader
        .next()
        .transpose()?
        .ok_or_else(|| CodecError::invalid("stream contains no record batch"))?;
    require_position(&reader, layout.batch.end(), "RecordBatch")?;
    if reader.next().transpose()?.is_some() {
        return Err(CodecError::invalid(
            "stream contains more than one record batch",
        ));
    }

    change_from_physical(physical, logical_schema)
}

#[cfg(test)]
pub(crate) fn projected_body_lengths_for_test(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<(usize, usize), CodecError> {
    projected::body_lengths(encoded, projection)
}

fn change_from_physical(
    physical: RecordBatch,
    logical_schema: SchemaRef,
) -> Result<Change, CodecError> {
    let (_, mut columns, row_count) = physical.into_parts();
    if columns.is_empty() {
        return Err(CodecError::invalid(
            "physical Schema is missing its diff column",
        ));
    }
    let diffs = columns
        .remove(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CodecError::invalid("physical diff column is not Int64"))?
        .clone();
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let records = RecordBatch::try_new_with_options(logical_schema, columns, &options)?;
    Change::try_new(records, diffs).map_err(CodecError::from)
}

struct StreamLayout {
    schema: MessageLayout,
    batch: MessageLayout,
}

struct MessageLayout {
    metadata: Range<usize>,
    body: Range<usize>,
}

impl MessageLayout {
    const fn end(&self) -> usize {
        self.body.end
    }
}

fn preflight_stream(encoded: &[u8]) -> Result<StreamLayout, CodecError> {
    let schema = preflight_message(encoded, 0, MessageHeader::Schema)?;
    let batch = preflight_message(encoded, schema.end(), MessageHeader::RecordBatch)?;
    if encoded.get(batch.end()..) != Some(CANONICAL_EOS.as_slice()) {
        return Err(CodecError::invalid(
            "the first record batch must be followed by one canonical EOS marker and no other bytes",
        ));
    }
    Ok(StreamLayout { schema, batch })
}

fn preflight_message(
    encoded: &[u8],
    offset: usize,
    expected: MessageHeader,
) -> Result<MessageLayout, CodecError> {
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

    match expected {
        MessageHeader::Schema => preflight_schema_message(message)?,
        MessageHeader::RecordBatch => preflight_batch_message(message)?,
        _ => unreachable!("only Schema and RecordBatch messages are preflighted"),
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
    if body_end > encoded.len() {
        return Err(CodecError::invalid(format!(
            "{expected:?} body length exceeds the encoded entry"
        )));
    }
    Ok(MessageLayout {
        metadata: metadata_start..metadata_end,
        body: metadata_end..body_end,
    })
}

fn message_at<'encoded>(
    encoded: &'encoded [u8],
    layout: &MessageLayout,
    expected: MessageHeader,
) -> Result<Message<'encoded>, CodecError> {
    let metadata = encoded.get(layout.metadata.clone()).ok_or_else(|| {
        CodecError::invalid(format!("{expected:?} metadata is outside the entry"))
    })?;
    let message = root_as_message(metadata).map_err(|error| {
        CodecError::invalid(format!("invalid {expected:?} IPC metadata: {error}"))
    })?;
    if message.header_type() != expected {
        return Err(CodecError::invalid(format!(
            "expected {expected:?} message, found {:?}",
            message.header_type()
        )));
    }
    Ok(message)
}

fn preflight_schema_message(message: arrow_ipc::Message<'_>) -> Result<(), CodecError> {
    if message.bodyLength() != 0 {
        return Err(CodecError::invalid(
            "Schema message declares a non-empty body",
        ));
    }
    let schema = message
        .header_as_schema()
        .ok_or_else(|| CodecError::invalid("Schema message has no Schema header"))?;
    if schema.endianness() != Endianness::Little {
        return Err(CodecError::invalid(
            "only little-endian Arrow Schemas are supported",
        ));
    }
    if schema
        .features()
        .is_some_and(|features| !features.is_empty())
    {
        return Err(CodecError::invalid(
            "Arrow Schema features are not supported",
        ));
    }
    Ok(())
}

fn preflight_batch_message(message: arrow_ipc::Message<'_>) -> Result<(), CodecError> {
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| CodecError::invalid("RecordBatch message has no RecordBatch header"))?;
    if batch.length() <= 0 {
        return Err(CodecError::invalid(
            "RecordBatch must contain at least one row",
        ));
    }
    usize::try_from(batch.length())
        .map_err(|_| CodecError::invalid("RecordBatch row count does not fit this platform"))?;
    if let Some(nodes) = batch.nodes() {
        for (index, node) in nodes.iter().enumerate() {
            let length = usize::try_from(node.length()).map_err(|_| {
                CodecError::invalid(format!(
                    "RecordBatch field node {index} length does not fit this platform"
                ))
            })?;
            let null_count = usize::try_from(node.null_count()).map_err(|_| {
                CodecError::invalid(format!(
                    "RecordBatch field node {index} null count does not fit this platform"
                ))
            })?;
            if null_count > length {
                return Err(CodecError::invalid(format!(
                    "RecordBatch field node {index} has more nulls than rows"
                )));
            }
        }
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
    Ok(())
}

fn require_position(
    reader: &StreamReader<Cursor<&[u8]>>,
    expected: usize,
    message: &'static str,
) -> Result<(), CodecError> {
    let actual = usize::try_from(reader.get_ref().position())
        .map_err(|_| CodecError::invalid("stream position does not fit this platform"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(CodecError::invalid(format!(
            "Arrow reader consumed {actual} bytes after {message}; expected {expected}"
        )))
    }
}

fn physical_batch(change: &Change) -> Result<RecordBatch, CodecError> {
    let logical_schema = change.records().schema_ref();
    validate_schema(logical_schema)?;
    let physical_schema = physical_schema(logical_schema);
    let mut columns = Vec::with_capacity(change.records().num_columns() + 1);
    columns.push(Arc::new(change.diffs().clone()) as ArrayRef);
    columns.extend(change.records().columns().iter().cloned());
    Ok(RecordBatch::try_new(physical_schema, columns)?)
}

fn physical_schema(logical: &Schema) -> SchemaRef {
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
        .find(|key| key.starts_with(RESERVED_METADATA_PREFIX))
    {
        return Err(CodecError::invalid(format!(
            "Arrow Schema contains unknown reserved metadata key {key:?}"
        )));
    }

    let logical = Arc::new(Schema::new_with_metadata(logical_fields.to_vec(), metadata));
    validate_schema(&logical)?;
    Ok(logical)
}

fn write_options() -> Result<IpcWriteOptions, CodecError> {
    Ok(IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?)
}

fn ensure_little_endian_target() -> Result<(), CodecError> {
    if cfg!(target_endian = "little") {
        Ok(())
    } else {
        Err(CodecError::UnsupportedTargetEndianness)
    }
}
