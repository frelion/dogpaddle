use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::{Buffer as ArrowBuffer, MutableBuffer};
use arrow_ipc::{
    Buffer as IpcBuffer, FieldNode, MessageHeader, MetadataVersion, RecordBatch as IpcRecordBatch,
    RecordBatchArgs, convert::fb_to_schema, reader::RecordBatchDecoder,
};
use arrow_schema::{DataType, Field, Schema};
use flatbuffers::FlatBufferBuilder;

use super::{
    CodecError, change_from_physical, logical_schema, message_at, physical_schema, preflight_stream,
};
use crate::{Change, ChangeProjection};

pub(super) fn decode(encoded: &[u8], projection: &ChangeProjection) -> Result<Change, CodecError> {
    let ProjectedInput {
        body,
        batch,
        physical_schema,
    } = ProjectedInput::parse(encoded, projection)?;
    let physical = decode_physical_batch(body, batch, &physical_schema, projection)?;
    change_from_physical(physical, Arc::clone(projection.output_schema()))
}

#[cfg(test)]
pub(super) fn body_lengths(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<(usize, usize), CodecError> {
    let ProjectedInput {
        body,
        batch,
        physical_schema,
    } = ProjectedInput::parse(encoded, projection)?;
    let compact = compact_projected_batch(body, &batch, &physical_schema, projection)?;
    Ok((body.len(), compact.body.len()))
}

struct ProjectedInput<'encoded> {
    body: &'encoded [u8],
    batch: IpcRecordBatch<'encoded>,
    physical_schema: Schema,
}

impl<'encoded> ProjectedInput<'encoded> {
    fn parse(encoded: &'encoded [u8], projection: &ChangeProjection) -> Result<Self, CodecError> {
        let layout = preflight_stream(encoded)?;
        let schema_message = message_at(encoded, &layout.schema, MessageHeader::Schema)?;
        let embedded = schema_message
            .header_as_schema()
            .ok_or_else(|| CodecError::invalid("Schema message has no Schema header"))?;
        let physical_schema = fb_to_schema(embedded);
        let logical_schema = logical_schema(&physical_schema)?;
        projection.require_schema(logical_schema.as_ref())?;

        let batch_message = message_at(encoded, &layout.batch, MessageHeader::RecordBatch)?;
        let batch = batch_message
            .header_as_record_batch()
            .ok_or_else(|| CodecError::invalid("RecordBatch message has no RecordBatch header"))?;
        let body = encoded
            .get(layout.batch.body)
            .ok_or_else(|| CodecError::invalid("RecordBatch body is outside the encoded entry"))?;
        Ok(Self {
            body,
            batch,
            physical_schema,
        })
    }
}

fn decode_physical_batch(
    body: &[u8],
    batch: IpcRecordBatch<'_>,
    embedded_physical_schema: &Schema,
    projection: &ChangeProjection,
) -> Result<RecordBatch, CodecError> {
    let compact = compact_projected_batch(body, &batch, embedded_physical_schema, projection)?;
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&compact.nodes);
    let buffers = builder.create_vector(&compact.buffers);
    let projected_batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: batch.length(),
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: None,
        },
    );
    builder.finish_minimal(projected_batch);
    let projected_batch = flatbuffers::root::<IpcRecordBatch<'_>>(builder.finished_data())
        .map_err(|error| {
            CodecError::invalid(format!("invalid compacted RecordBatch metadata: {error}"))
        })?;

    let dictionaries = HashMap::<i64, ArrayRef>::new();
    let projected_schema = physical_schema(projection.output_schema().as_ref());
    Ok(RecordBatchDecoder::try_new(
        &compact.body,
        projected_batch,
        projected_schema,
        &dictionaries,
        &MetadataVersion::V5,
    )?
    .with_require_alignment(true)
    .read_record_batch()?)
}

struct CompactedBatch {
    nodes: Vec<FieldNode>,
    buffers: Vec<IpcBuffer>,
    body: ArrowBuffer,
}

fn compact_projected_batch(
    body: &[u8],
    batch: &IpcRecordBatch<'_>,
    physical_schema: &Schema,
    projection: &ChangeProjection,
) -> Result<CompactedBatch, CodecError> {
    let nodes = batch
        .nodes()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no field nodes"))?
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let buffers = batch
        .buffers()
        .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no buffers"))?
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let fields = validate_batch_layout(
        physical_schema,
        batch.length(),
        &nodes,
        &buffers,
        body.len(),
    )?;
    let selected_fields = selected_physical_fields(projection)?;
    compact_selected_buffers(body, &nodes, &buffers, &fields, &selected_fields)
}

struct FieldLayout {
    nodes: Range<usize>,
    buffers: Range<usize>,
}

#[derive(Default)]
struct LayoutCursor {
    nodes: usize,
    buffers: usize,
}

fn validate_batch_layout(
    schema: &Schema,
    row_count: i64,
    nodes: &[FieldNode],
    buffers: &[IpcBuffer],
    body_len: usize,
) -> Result<Vec<FieldLayout>, CodecError> {
    let row_count = usize::try_from(row_count)
        .map_err(|_| CodecError::invalid("RecordBatch row count does not fit this platform"))?;
    let mut cursor = LayoutCursor::default();
    let fields = schema
        .fields()
        .iter()
        .map(|field| consume_field_layout(field, Some(row_count), nodes, buffers, &mut cursor))
        .collect::<Result<Vec<_>, _>>()?;
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
    validate_buffer_layout(buffers, body_len)?;
    Ok(fields)
}

fn consume_field_layout(
    field: &Field,
    expected_length: Option<usize>,
    nodes: &[FieldNode],
    buffers: &[IpcBuffer],
    cursor: &mut LayoutCursor,
) -> Result<FieldLayout, CodecError> {
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
    if expected_length.is_some_and(|expected| length != expected) {
        return Err(CodecError::invalid(format!(
            "RecordBatch field {:?} length differs from its parent",
            field.name()
        )));
    }
    if matches!(field.data_type(), DataType::Null) && null_count != length {
        return Err(CodecError::invalid(format!(
            "RecordBatch Null field {:?} must mark every row as null",
            field.name()
        )));
    }

    let buffer_start = cursor.buffers;
    let own_buffer_end = cursor
        .buffers
        .checked_add(own_buffer_count(field.data_type())?)
        .ok_or_else(|| CodecError::invalid("RecordBatch buffer count overflowed"))?;
    let own_buffers = buffers.get(buffer_start..own_buffer_end).ok_or_else(|| {
        CodecError::invalid(format!(
            "RecordBatch has too few buffers for field {:?}",
            field.name()
        ))
    })?;
    validate_own_buffer_lengths(field, length, own_buffers)?;
    cursor.buffers = own_buffer_end;

    match field.data_type() {
        DataType::List(child) => {
            consume_field_layout(child, None, nodes, buffers, cursor)?;
        }
        DataType::Struct(children) => {
            for child in children {
                consume_field_layout(child, Some(length), nodes, buffers, cursor)?;
            }
        }
        _ => {}
    }
    Ok(FieldLayout {
        nodes: node_start..cursor.nodes,
        buffers: buffer_start..cursor.buffers,
    })
}

fn validate_own_buffer_lengths(
    field: &Field,
    length: usize,
    buffers: &[IpcBuffer],
) -> Result<(), CodecError> {
    if matches!(field.data_type(), DataType::Null) {
        return Ok(());
    }

    require_buffer_length(field, &buffers[0], bitmap_byte_len(length)?, "validity")?;
    match field.data_type() {
        DataType::Boolean => {
            require_buffer_length(field, &buffers[1], bitmap_byte_len(length)?, "values")
        }
        DataType::Int8 | DataType::UInt8 => {
            require_buffer_length(field, &buffers[1], length, "values")
        }
        DataType::Int16 | DataType::UInt16 => {
            require_fixed_width_buffer_length(field, &buffers[1], length, 2)
        }
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => {
            require_fixed_width_buffer_length(field, &buffers[1], length, 4)
        }
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
            require_fixed_width_buffer_length(field, &buffers[1], length, 8)
        }
        DataType::Utf8 | DataType::Binary | DataType::List(_) => {
            let offset_count = length
                .checked_add(1)
                .ok_or_else(|| CodecError::invalid("Arrow offset count overflowed"))?;
            require_fixed_width_buffer_length(field, &buffers[1], offset_count, 4)
        }
        DataType::Struct(_) => Ok(()),
        DataType::Null => unreachable!("Null fields returned before buffer validation"),
        data_type => Err(CodecError::invalid(format!(
            "unsupported Arrow type {data_type} in RecordBatch layout"
        ))),
    }
}

fn require_fixed_width_buffer_length(
    field: &Field,
    buffer: &IpcBuffer,
    elements: usize,
    byte_width: usize,
) -> Result<(), CodecError> {
    let expected = elements
        .checked_mul(byte_width)
        .ok_or_else(|| CodecError::invalid("Arrow fixed-width buffer length overflowed"))?;
    require_buffer_length(field, buffer, expected, "values")
}

fn require_buffer_length(
    field: &Field,
    buffer: &IpcBuffer,
    expected: usize,
    role: &'static str,
) -> Result<(), CodecError> {
    let actual = usize::try_from(buffer.length()).map_err(|_| {
        CodecError::invalid(format!(
            "RecordBatch field {:?} has a negative or oversized {role} buffer length",
            field.name()
        ))
    })?;
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

fn own_buffer_count(data_type: &DataType) -> Result<usize, CodecError> {
    match data_type {
        DataType::Null => Ok(0),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::List(_) => Ok(2),
        DataType::Utf8 | DataType::Binary => Ok(3),
        DataType::Struct(_) => Ok(1),
        data_type => Err(CodecError::invalid(format!(
            "unsupported Arrow type {data_type} in RecordBatch layout"
        ))),
    }
}

fn validate_buffer_layout(buffers: &[IpcBuffer], body_len: usize) -> Result<(), CodecError> {
    let mut expected_offset = 0;
    for (index, buffer) in buffers.iter().enumerate() {
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
        expected_offset = align_to_eight(end)?;
    }
    if expected_offset != body_len {
        return Err(CodecError::invalid(format!(
            "RecordBatch buffers end at {expected_offset} bytes; declared body has {body_len} bytes"
        )));
    }
    Ok(())
}

fn selected_physical_fields(projection: &ChangeProjection) -> Result<Vec<usize>, CodecError> {
    let mut selected = Vec::with_capacity(projection.field_indices().len() + 1);
    selected.push(0);
    for &logical in projection.field_indices() {
        selected.push(
            logical
                .checked_add(1)
                .ok_or_else(|| CodecError::invalid("logical projection index overflowed"))?,
        );
    }
    Ok(selected)
}

fn compact_selected_buffers(
    body: &[u8],
    nodes: &[FieldNode],
    buffers: &[IpcBuffer],
    fields: &[FieldLayout],
    selected_fields: &[usize],
) -> Result<CompactedBatch, CodecError> {
    let capacity = compact_body_len(buffers, fields, selected_fields)?;
    let mut compact = MutableBuffer::new(capacity);
    let mut selected_nodes = Vec::new();
    let mut selected_buffers = Vec::new();

    for &field_index in selected_fields {
        let field = fields.get(field_index).ok_or_else(|| {
            CodecError::invalid(format!(
                "projected physical field {field_index} is outside the Schema"
            ))
        })?;
        selected_nodes.extend_from_slice(&nodes[field.nodes.clone()]);
        for descriptor in &buffers[field.buffers.clone()] {
            let source_start = usize::try_from(descriptor.offset())
                .map_err(|_| CodecError::invalid("selected buffer offset is negative"))?;
            let length = usize::try_from(descriptor.length())
                .map_err(|_| CodecError::invalid("selected buffer length is negative"))?;
            let source_end = source_start
                .checked_add(length)
                .ok_or_else(|| CodecError::invalid("selected buffer range overflowed"))?;
            let source = body
                .get(source_start..source_end)
                .ok_or_else(|| CodecError::invalid("selected buffer exceeds the body"))?;
            let target_offset = i64::try_from(compact.len())
                .map_err(|_| CodecError::invalid("compacted buffer offset exceeds i64"))?;
            selected_buffers.push(IpcBuffer::new(target_offset, descriptor.length()));
            compact.extend_from_slice(source);
            compact.resize(align_to_eight(compact.len())?, 0);
        }
    }
    debug_assert_eq!(compact.len(), capacity);
    Ok(CompactedBatch {
        nodes: selected_nodes,
        buffers: selected_buffers,
        body: compact.into(),
    })
}

fn compact_body_len(
    buffers: &[IpcBuffer],
    fields: &[FieldLayout],
    selected_fields: &[usize],
) -> Result<usize, CodecError> {
    let mut length = 0_usize;
    for &field_index in selected_fields {
        let field = fields.get(field_index).ok_or_else(|| {
            CodecError::invalid(format!(
                "projected physical field {field_index} is outside the Schema"
            ))
        })?;
        for descriptor in &buffers[field.buffers.clone()] {
            let buffer_len = usize::try_from(descriptor.length())
                .map_err(|_| CodecError::invalid("selected buffer length is negative"))?;
            length = length
                .checked_add(buffer_len)
                .ok_or_else(|| CodecError::invalid("compacted body length overflowed"))?;
            length = align_to_eight(length)?;
        }
    }
    Ok(length)
}

fn align_to_eight(value: usize) -> Result<usize, CodecError> {
    value
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| CodecError::invalid("Arrow IPC alignment overflowed"))
}
