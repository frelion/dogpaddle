use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_buffer::{Buffer as ArrowBuffer, MutableBuffer};
use arrow_ipc::{
    Buffer as IpcBuffer, FieldNode, MetadataVersion, RecordBatch as IpcRecordBatch,
    RecordBatchArgs, reader::RecordBatchDecoder,
};
use arrow_schema::{Field, SchemaRef};
use flatbuffers::FlatBufferBuilder;

use super::{
    CodecError,
    stream::{ParsedChange, physical_schema},
};
use crate::{change::Change, projection::ChangeProjection, schema::DataTypeLayout};

pub(super) fn decode(
    parsed: &ParsedChange<'_>,
    projection: Option<&ChangeProjection>,
) -> Result<Change, CodecError> {
    let layout = BatchLayout::parse(parsed)?;
    let logical_schema = projection.map_or_else(
        || Arc::clone(&parsed.logical_schema),
        |projection| Arc::clone(projection.output_schema_ref()),
    );

    let physical =
        if let Some(projection) = projection.filter(|projection| !projection.is_identity()) {
            let compact = layout.compact(parsed.body, projection)?;
            decode_compact(&compact, parsed.batch.length(), projection)?
        } else {
            decode_complete(
                parsed.body,
                parsed.batch,
                Arc::clone(&parsed.physical_schema),
            )?
        };
    change_from_physical(physical, logical_schema)
}

fn decode_complete(
    body: &[u8],
    batch: IpcRecordBatch<'_>,
    schema: SchemaRef,
) -> Result<RecordBatch, CodecError> {
    let body = ArrowBuffer::from(body);
    decode_record_batch(&body, batch, schema)
}

fn decode_compact(
    compact: &CompactedBatch,
    row_count: i64,
    projection: &ChangeProjection,
) -> Result<RecordBatch, CodecError> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&compact.nodes);
    let buffers = builder.create_vector(&compact.buffers);
    let batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: row_count,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: None,
        },
    );
    builder.finish_minimal(batch);
    let batch =
        flatbuffers::root::<IpcRecordBatch<'_>>(builder.finished_data()).map_err(|error| {
            CodecError::invalid(format!("invalid compacted RecordBatch metadata: {error}"))
        })?;
    decode_record_batch(
        &compact.body,
        batch,
        physical_schema(projection.output_schema_ref()),
    )
}

fn decode_record_batch(
    body: &ArrowBuffer,
    batch: IpcRecordBatch<'_>,
    schema: SchemaRef,
) -> Result<RecordBatch, CodecError> {
    let dictionaries = HashMap::<i64, ArrayRef>::new();
    Ok(
        RecordBatchDecoder::try_new(body, batch, schema, &dictionaries, &MetadataVersion::V5)?
            .with_require_alignment(true)
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

pub(super) struct BatchLayout {
    pub(super) nodes: Vec<FieldNode>,
    pub(super) buffers: Vec<Range<usize>>,
    pub(super) fields: Vec<FieldLayout>,
}

pub(super) struct FieldLayout {
    pub(super) nodes: Range<usize>,
    pub(super) buffers: Range<usize>,
}

#[derive(Default)]
struct LayoutCursor {
    nodes: usize,
    buffers: usize,
}

impl BatchLayout {
    pub(super) fn parse(parsed: &ParsedChange<'_>) -> Result<Self, CodecError> {
        let nodes = parsed
            .batch
            .nodes()
            .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no field nodes"))?
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let descriptors = parsed
            .batch
            .buffers()
            .ok_or_else(|| CodecError::invalid("RecordBatch metadata has no buffers"))?;
        let buffers = validate_buffer_layout(descriptors.iter().copied(), parsed.body.len())?;

        let mut cursor = LayoutCursor::default();
        let fields = parsed
            .physical_schema
            .fields()
            .iter()
            .map(|field| {
                consume_field_layout(
                    field,
                    Some(parsed.row_count),
                    0,
                    &nodes,
                    &buffers,
                    &mut cursor,
                )
            })
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
        Ok(Self {
            nodes,
            buffers,
            fields,
        })
    }

    pub(super) fn compact(
        &self,
        body: &[u8],
        projection: &ChangeProjection,
    ) -> Result<CompactedBatch, CodecError> {
        let selected = std::iter::once(0)
            .chain(projection.field_indices().iter().map(|index| index + 1))
            .map(|index| {
                self.fields.get(index).ok_or_else(|| {
                    CodecError::invalid(format!(
                        "projected physical field {index} is outside the Schema"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let capacity = self.compact_body_len(&selected)?;
        let mut compact = MutableBuffer::new(capacity);
        let mut nodes = Vec::new();
        let mut buffers = Vec::new();
        for field in selected {
            nodes.extend_from_slice(&self.nodes[field.nodes.clone()]);
            for range in &self.buffers[field.buffers.clone()] {
                let source = body.get(range.clone()).ok_or_else(|| {
                    CodecError::invalid("validated selected buffer is outside the body")
                })?;
                let target_offset = i64::try_from(compact.len())
                    .map_err(|_| CodecError::invalid("compacted buffer offset exceeds i64"))?;
                let length = i64::try_from(range.len())
                    .map_err(|_| CodecError::invalid("compacted buffer length exceeds i64"))?;
                buffers.push(IpcBuffer::new(target_offset, length));
                compact.extend_from_slice(source);
                compact.resize(align_to_eight(compact.len())?, 0);
            }
        }
        debug_assert_eq!(compact.len(), capacity);
        Ok(CompactedBatch {
            nodes,
            buffers,
            body: compact.into(),
        })
    }

    fn compact_body_len(&self, selected: &[&FieldLayout]) -> Result<usize, CodecError> {
        let mut length = 0_usize;
        for field in selected {
            for range in &self.buffers[field.buffers.clone()] {
                length = length
                    .checked_add(range.len())
                    .ok_or_else(|| CodecError::invalid("compacted body length overflowed"))?;
                length = align_to_eight(length)?;
            }
        }
        Ok(length)
    }
}

pub(super) struct CompactedBatch {
    pub(super) nodes: Vec<FieldNode>,
    pub(super) buffers: Vec<IpcBuffer>,
    pub(super) body: ArrowBuffer,
}

fn consume_field_layout(
    field: &Field,
    expected_length: Option<usize>,
    masked_nulls: usize,
    nodes: &[FieldNode],
    buffers: &[Range<usize>],
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
    if !field.is_nullable() && null_count > masked_nulls {
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
    let data_type_layout = DataTypeLayout::classify(field.data_type()).ok_or_else(|| {
        CodecError::invalid(format!(
            "unsupported Arrow type {} in RecordBatch layout",
            field.data_type()
        ))
    })?;
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
    Ok(FieldLayout {
        nodes: node_start..cursor.nodes,
        buffers: buffer_start..cursor.buffers,
    })
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
