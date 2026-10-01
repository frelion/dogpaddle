use std::{collections::HashMap, ops::Range, sync::Arc};

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

#[derive(Default)]
struct LayoutCursor {
    nodes: usize,
    buffers: usize,
}

fn validate_layout(parsed: &ParsedChange<'_>) -> Result<(), CodecError> {
    let expected = expected_layout_counts(&parsed.physical_schema)?;
    let node_descriptors = parsed
        .batch
        .nodes()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no field nodes"))?;
    if node_descriptors.len() != expected.nodes {
        return Err(CodecError::invalid(format!(
            "RecordBatch has {} field nodes; Schema requires {}",
            node_descriptors.len(),
            expected.nodes
        )));
    }
    let nodes = node_descriptors.iter().copied().collect::<Vec<_>>();
    let descriptors = parsed
        .batch
        .buffers()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no buffers"))?;
    if descriptors.len() != expected.buffers {
        return Err(CodecError::invalid(format!(
            "RecordBatch has {} buffers; Schema requires {}",
            descriptors.len(),
            expected.buffers
        )));
    }
    let buffers = validate_buffer_layout(descriptors.iter().copied(), parsed.body.len())?;

    let mut cursor = LayoutCursor::default();
    for field in parsed.physical_schema.fields() {
        consume_field_layout(
            field,
            Some(parsed.row_count),
            0,
            &nodes,
            &buffers,
            &mut cursor,
        )?;
    }
    if cursor.nodes != nodes.len() {
        return Err(CodecError::invalid(format!(
            "RecordBatch has {} field nodes; Schema requires {}",
            nodes.len(),
            cursor.nodes
        )));
    }
    if cursor.buffers != buffers.len() {
        return Err(CodecError::invalid(format!(
            "RecordBatch has {} buffers; Schema requires {}",
            buffers.len(),
            cursor.buffers
        )));
    }
    Ok(())
}

fn expected_layout_counts(schema: &SchemaRef) -> Result<LayoutCursor, CodecError> {
    fn add_field(field: &Field, counts: &mut LayoutCursor) -> Result<(), CodecError> {
        let layout = DataTypeLayout::classify(field.data_type())
            .ok_or_else(|| CodecError::invalid("Schema contains an unsupported field type"))?;
        counts.nodes = counts
            .nodes
            .checked_add(1)
            .ok_or_else(|| CodecError::invalid("Schema field-node count overflowed"))?;
        counts.buffers = counts
            .buffers
            .checked_add(layout.own_buffer_count())
            .ok_or_else(|| CodecError::invalid("Schema buffer count overflowed"))?;
        match layout {
            DataTypeLayout::List(child) => add_field(child, counts),
            DataTypeLayout::Struct(fields) => {
                for child in fields {
                    add_field(child, counts)?;
                }
                Ok(())
            }
            DataTypeLayout::Null
            | DataTypeLayout::Bitmap
            | DataTypeLayout::FixedWidth(_)
            | DataTypeLayout::VariableWidth => Ok(()),
        }
    }

    let mut counts = LayoutCursor::default();
    for field in schema.fields() {
        add_field(field, &mut counts)?;
    }
    Ok(counts)
}

fn consume_field_layout(
    field: &Field,
    expected_length: Option<usize>,
    masked_nulls: usize,
    nodes: &[FieldNode],
    buffers: &[Range<usize>],
    cursor: &mut LayoutCursor,
) -> Result<(), CodecError> {
    let node_start = cursor.nodes;
    let node = nodes.get(node_start).ok_or_else(|| {
        CodecError::invalid(format!(
            "RecordBatch has no field node for {:?}",
            field.name()
        ))
    })?;
    cursor.nodes = cursor
        .nodes
        .checked_add(1)
        .ok_or_else(|| CodecError::invalid("RecordBatch field node count overflowed"))?;
    let length = usize::try_from(node.length()).map_err(|_| {
        CodecError::invalid(format!(
            "RecordBatch field {:?} has a negative or oversized length",
            field.name()
        ))
    })?;
    let null_count = usize::try_from(node.null_count()).map_err(|_| {
        CodecError::invalid(format!(
            "RecordBatch field {:?} has a negative or oversized null count",
            field.name()
        ))
    })?;
    if null_count > length {
        return Err(CodecError::invalid(format!(
            "RecordBatch field {:?} has more nulls than rows",
            field.name()
        )));
    }
    let data_type_layout = DataTypeLayout::classify(field.data_type()).ok_or_else(|| {
        CodecError::invalid(format!(
            "unsupported Arrow type {} in RecordBatch layout",
            field.data_type()
        ))
    })?;
    if !matches!(data_type_layout, DataTypeLayout::Null)
        && !field.is_nullable()
        && null_count > masked_nulls
    {
        return Err(CodecError::invalid(format!(
            "RecordBatch non-nullable field {:?} has {null_count} nulls, but its parent can mask at most {masked_nulls}",
            field.name()
        )));
    }
    if expected_length.is_some_and(|expected| length != expected) {
        return Err(CodecError::invalid(format!(
            "RecordBatch field {:?} length differs from its parent",
            field.name()
        )));
    }
    if matches!(data_type_layout, DataTypeLayout::Null) && null_count != length {
        return Err(CodecError::invalid(format!(
            "RecordBatch Null field {:?} must mark every row as null",
            field.name()
        )));
    }

    let buffer_start = cursor.buffers;
    let own_buffer_end = cursor
        .buffers
        .checked_add(data_type_layout.own_buffer_count())
        .ok_or_else(|| CodecError::invalid("RecordBatch buffer count overflowed"))?;
    let own_buffers = buffers.get(buffer_start..own_buffer_end).ok_or_else(|| {
        CodecError::invalid(format!(
            "RecordBatch has too few buffers for field {:?}",
            field.name()
        ))
    })?;
    validate_own_buffer_lengths(data_type_layout, field, length, own_buffers)?;
    cursor.buffers = own_buffer_end;

    match data_type_layout {
        DataTypeLayout::List(child) => {
            consume_field_layout(child, None, 0, nodes, buffers, cursor)?;
        }
        DataTypeLayout::Struct(children) => {
            for child in children {
                consume_field_layout(child, Some(length), null_count, nodes, buffers, cursor)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_own_buffer_lengths(
    data_type: DataTypeLayout<'_>,
    field: &Field,
    length: usize,
    buffers: &[Range<usize>],
) -> Result<(), CodecError> {
    if matches!(data_type, DataTypeLayout::Null) {
        return Ok(());
    }

    require_buffer_length(field, &buffers[0], bitmap_byte_len(length)?, "validity")?;
    let value_length = match data_type {
        DataTypeLayout::Struct(_) => return Ok(()),
        DataTypeLayout::Bitmap => bitmap_byte_len(length),
        DataTypeLayout::FixedWidth(byte_width) => fixed_width_byte_len(length, byte_width),
        DataTypeLayout::VariableWidth | DataTypeLayout::List(_) => {
            let offset_count = length
                .checked_add(1)
                .ok_or_else(|| CodecError::invalid("Arrow offset count overflowed"))?;
            fixed_width_byte_len(offset_count, 4)
        }
        DataTypeLayout::Null => unreachable!("Null fields returned before buffer validation"),
    }?;
    require_buffer_length(field, &buffers[1], value_length, "values")
}

fn fixed_width_byte_len(elements: usize, byte_width: usize) -> Result<usize, CodecError> {
    elements
        .checked_mul(byte_width)
        .ok_or_else(|| CodecError::invalid("Arrow fixed-width buffer length overflowed"))
}

fn require_buffer_length(
    field: &Field,
    buffer: &Range<usize>,
    expected: usize,
    role: &'static str,
) -> Result<(), CodecError> {
    let actual = buffer.len();
    if actual == expected {
        Ok(())
    } else {
        Err(CodecError::invalid(format!(
            "RecordBatch field {:?} {role} buffer has length {actual}; expected {expected}",
            field.name()
        )))
    }
}

fn bitmap_byte_len(length: usize) -> Result<usize, CodecError> {
    length
        .checked_add(7)
        .map(|length| length / 8)
        .ok_or_else(|| CodecError::invalid("Arrow bitmap length overflowed"))
}

fn validate_buffer_layout(
    buffers: impl Iterator<Item = IpcBuffer>,
    body_len: usize,
) -> Result<Vec<Range<usize>>, CodecError> {
    let mut ranges = Vec::with_capacity(buffers.size_hint().0);
    let mut expected_offset = 0;
    for (index, buffer) in buffers.enumerate() {
        let offset = usize::try_from(buffer.offset()).map_err(|_| {
            CodecError::invalid(format!("RecordBatch buffer {index} offset is negative"))
        })?;
        let length = usize::try_from(buffer.length()).map_err(|_| {
            CodecError::invalid(format!("RecordBatch buffer {index} length is negative"))
        })?;
        if offset != expected_offset {
            return Err(CodecError::invalid(format!(
                "RecordBatch buffer {index} offset {offset} is not the canonical offset {expected_offset}"
            )));
        }
        let end = offset.checked_add(length).ok_or_else(|| {
            CodecError::invalid(format!("RecordBatch buffer {index} range overflowed"))
        })?;
        if end > body_len {
            return Err(CodecError::invalid(format!(
                "RecordBatch buffer {index} exceeds the declared body"
            )));
        }
        ranges.push(offset..end);
        expected_offset = align_to_eight(end)?;
    }
    if expected_offset != body_len {
        return Err(CodecError::invalid(format!(
            "RecordBatch buffers end at {expected_offset} bytes; declared body has {body_len} bytes"
        )));
    }
    Ok(ranges)
}

fn align_to_eight(value: usize) -> Result<usize, CodecError> {
    value
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| CodecError::invalid("Arrow IPC alignment overflowed"))
}
