use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_ipc::{
    Buffer as IpcBuffer, FieldNode, MetadataVersion, RecordBatch as IpcRecordBatch,
    reader::RecordBatchDecoder,
};
use arrow_schema::{Field, SchemaRef};

use super::{CodecError, stream::ParsedChange};
use crate::{change::Change, schema::DataTypeLayout};

pub(super) fn decode(parsed: &ParsedChange<'_>) -> Result<Change, CodecError> {
    validate_layout(parsed)?;
    let body = ArrowBuffer::from(parsed.body);
    let physical = decode_record_batch(&body, parsed.batch, Arc::clone(&parsed.physical_schema))?;
    change_from_physical(physical, Arc::clone(&parsed.logical_schema))
}

pub(super) fn decode_owned(
    encoded: &ArrowBuffer,
    parsed: &ParsedChange<'_>,
) -> Result<Change, CodecError> {
    validate_layout(parsed)?;
    let body_offset = (parsed.body.as_ptr() as usize)
        .checked_sub(encoded.as_ptr() as usize)
        .ok_or_else(|| CodecError::invalid("IPC body precedes its owned allocation"))?;
    let body = encoded.slice_with_length(body_offset, parsed.body.len());
    let physical = decode_record_batch(&body, parsed.batch, Arc::clone(&parsed.physical_schema))?;
    change_from_physical(physical, Arc::clone(&parsed.logical_schema))
}

fn decode_record_batch(
    body: &ArrowBuffer,
    batch: IpcRecordBatch<'_>,
    schema: SchemaRef,
) -> Result<RecordBatch, CodecError> {
    let dictionaries = HashMap::<i64, ArrayRef>::new();
    // IPC guarantees eight-byte alignment, while Decimal128 has a sixteen-byte
    // native alignment on supported Rust targets. Let Arrow copy only a
    // selected buffer whose IPC offset is insufficiently aligned; all already
    // aligned buffers remain shared with `body` and validation stays enabled.
    Ok(
        RecordBatchDecoder::try_new(body, batch, schema, &dictionaries, &MetadataVersion::V5)?
            .with_require_alignment(false)
            .read_record_batch()?,
    )
}

fn change_from_physical(
    physical: RecordBatch,
    logical_schema: SchemaRef,
) -> Result<Change, CodecError> {
    let (_, columns, row_count) = physical.into_parts();
    let mut columns = columns.into_iter();
    let diffs = columns
        .next()
        .ok_or_else(|| CodecError::invalid("physical Schema is missing its diff column"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CodecError::invalid("physical diff column is not Int64"))?
        .clone();
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let records = RecordBatch::try_new_with_options(logical_schema, columns.collect(), &options)?;
    Ok(Change::try_new_with_validated_schema(records, diffs)?)
}

struct LayoutCursor<'encoded> {
    nodes: flatbuffers::VectorIter<'encoded, FieldNode>,
    buffers: flatbuffers::VectorIter<'encoded, IpcBuffer>,
    next_offset: usize,
    body_len: usize,
}

impl LayoutCursor<'_> {
    fn buffer(&mut self, expected_length: Option<usize>) -> Result<(), CodecError> {
        let buffer = self
            .buffers
            .next()
            .ok_or_else(|| CodecError::invalid("RecordBatch has too few buffers"))?;
        let offset = usize::try_from(buffer.offset()).map_err(|_| {
            CodecError::invalid("RecordBatch buffer offset is negative or oversized")
        })?;
        let length = usize::try_from(buffer.length()).map_err(|_| {
            CodecError::invalid("RecordBatch buffer length is negative or oversized")
        })?;
        if offset != self.next_offset {
            return Err(CodecError::invalid(
                "RecordBatch buffer offset is not canonical",
            ));
        }
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.body_len)
            .ok_or_else(|| CodecError::invalid("RecordBatch buffer exceeds the declared body"))?;
        if expected_length.is_some_and(|expected| length != expected) {
            return Err(CodecError::invalid(
                "RecordBatch buffer length differs from its field layout",
            ));
        }
        self.next_offset = align_to_eight(end)?;
        Ok(())
    }

    fn finish(&self) -> Result<(), CodecError> {
        if self.nodes.len() != 0 || self.buffers.len() != 0 {
            return Err(CodecError::invalid(
                "RecordBatch has unconsumed field nodes or buffers",
            ));
        }
        if self.next_offset != self.body_len {
            return Err(CodecError::invalid(
                "RecordBatch buffers do not cover the declared body",
            ));
        }
        Ok(())
    }
}

fn validate_layout(parsed: &ParsedChange<'_>) -> Result<(), CodecError> {
    let nodes = parsed
        .batch
        .nodes()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no field nodes"))?;
    let buffers = parsed
        .batch
        .buffers()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no buffers"))?;
    let mut cursor = LayoutCursor {
        nodes: nodes.iter(),
        buffers: buffers.iter(),
        next_offset: 0,
        body_len: parsed.body.len(),
    };
    for field in parsed.physical_schema.fields() {
        consume_field_layout(field, Some(parsed.row_count), 0, &mut cursor)?;
    }
    cursor.finish()
}

fn consume_field_layout(
    field: &Field,
    expected_length: Option<usize>,
    masked_nulls: usize,
    cursor: &mut LayoutCursor<'_>,
) -> Result<(), CodecError> {
    let node = cursor
        .nodes
        .next()
        .ok_or_else(|| CodecError::invalid("RecordBatch has too few field nodes"))?;
    let length = usize::try_from(node.length())
        .map_err(|_| CodecError::invalid("RecordBatch field length is negative or oversized"))?;
    let null_count = usize::try_from(node.null_count()).map_err(|_| {
        CodecError::invalid("RecordBatch field null count is negative or oversized")
    })?;
    if null_count > length {
        return Err(CodecError::invalid(
            "RecordBatch field has more nulls than rows",
        ));
    }
    let layout = DataTypeLayout::classify(field.data_type())
        .ok_or_else(|| CodecError::invalid("unsupported Arrow type in RecordBatch layout"))?;
    if !matches!(layout, DataTypeLayout::Null) && !field.is_nullable() && null_count > masked_nulls
    {
        return Err(CodecError::invalid(
            "RecordBatch non-nullable field has unmasked nulls",
        ));
    }
    if expected_length.is_some_and(|expected| length != expected) {
        return Err(CodecError::invalid(
            "RecordBatch field length differs from its parent",
        ));
    }
    if matches!(layout, DataTypeLayout::Null) {
        if null_count != length {
            return Err(CodecError::invalid(
                "RecordBatch Null field must mark every row as null",
            ));
        }
        return Ok(());
    }

    cursor.buffer(Some(bitmap_byte_len(length)?))?;
    match layout {
        DataTypeLayout::Bitmap => cursor.buffer(Some(bitmap_byte_len(length)?))?,
        DataTypeLayout::FixedWidth(width) => {
            cursor.buffer(Some(fixed_width_byte_len(length, width)?))?;
        }
        DataTypeLayout::VariableWidth | DataTypeLayout::List(_) => {
            let offsets = length
                .checked_add(1)
                .ok_or_else(|| CodecError::invalid("Arrow offset count overflowed"))?;
            cursor.buffer(Some(fixed_width_byte_len(offsets, 4)?))?;
            if matches!(layout, DataTypeLayout::VariableWidth) {
                cursor.buffer(None)?;
            }
        }
        DataTypeLayout::Struct(_) | DataTypeLayout::Null => {}
    }
    match layout {
        DataTypeLayout::List(child) => {
            consume_field_layout(child, None, 0, cursor)?;
        }
        DataTypeLayout::Struct(children) => {
            for child in children {
                consume_field_layout(child, Some(length), null_count, cursor)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn fixed_width_byte_len(elements: usize, byte_width: usize) -> Result<usize, CodecError> {
    elements
        .checked_mul(byte_width)
        .ok_or_else(|| CodecError::invalid("Arrow fixed-width buffer length overflowed"))
}

fn bitmap_byte_len(length: usize) -> Result<usize, CodecError> {
    length
        .checked_add(7)
        .map(|length| length / 8)
        .ok_or_else(|| CodecError::invalid("Arrow bitmap length overflowed"))
}

fn align_to_eight(value: usize) -> Result<usize, CodecError> {
    value
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| CodecError::invalid("Arrow IPC alignment overflowed"))
}
