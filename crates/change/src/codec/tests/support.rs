use std::{ops::Range, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampNanosecondArray, UInt64Array, types::Int64Type,
};
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, Buffer as IpcBuffer, FieldNode, Message as IpcMessage,
    MessageArgs, MessageHeader, MetadataVersion, RecordBatch as IpcRecordBatch, RecordBatchArgs,
};
use arrow_schema::{DataType, Field, Schema};
use flatbuffers::FlatBufferBuilder;

use super::super::{CodecError, SchemaBoundChangeCodec, stream};
use crate::Change;

pub(super) fn simple_change(diffs: &[i64]) -> Change {
    let values = (0..u64::try_from(diffs.len()).unwrap()).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

pub(super) fn layout_change() -> Change {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(Vec::<Option<i64>>::new()),
    ]);
    let object_score = Arc::new(Field::new("score", DataType::Int64, false));
    let object = StructArray::from(vec![(
        Arc::clone(&object_score),
        Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
    )]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![7, 7, 8])),
        Arc::new(StringArray::from(vec![Some("add"), None, Some("next")])),
        Arc::new(BinaryArray::from(vec![
            Some(b"one".as_slice()),
            Some(b"two".as_slice()),
            Some(b"three".as_slice()),
        ])),
        Arc::new(items),
        Arc::new(object),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, false),
        Field::new("items", columns[3].data_type().clone(), true),
        Field::new("object", DataType::Struct(vec![object_score].into()), true),
    ]));
    let records = RecordBatch::try_new(schema, columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap()
}

pub(super) fn extended_fixed_width_change() -> Change {
    let date = Arc::new(Date32Array::from(vec![Some(-1), None])) as ArrayRef;
    let timestamp =
        Arc::new(TimestampNanosecondArray::from(vec![Some(-1), None]).with_timezone("UTC"))
            as ArrayRef;
    let decimal = Arc::new(
        Decimal128Array::from(vec![Some(-12_345), None])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ) as ArrayRef;
    let columns = vec![date, timestamp, decimal];
    let fields = ["date", "timestamp", "decimal"]
        .into_iter()
        .zip(&columns)
        .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
        .collect::<Vec<_>>();
    let records = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap()
}

pub(super) fn frame_ipc_message(metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let metadata_len = metadata.len().next_multiple_of(8);
    let mut framed = Vec::with_capacity(8 + metadata_len + body.len());
    framed.extend_from_slice(&[0xff; 4]);
    framed.extend_from_slice(&i32::try_from(metadata_len).unwrap().to_le_bytes());
    framed.extend_from_slice(metadata);
    framed.resize(8 + metadata_len, 0);
    framed.extend_from_slice(body);
    framed
}

pub(super) fn replace_batch_message(encoded: &[u8], metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let batch_offset = 40;
    let mut replaced = encoded[..batch_offset].to_vec();
    replaced.extend_from_slice(&frame_ipc_message(metadata, body));
    replaced.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
    replaced
}

pub(super) fn replace_batch_layout(
    encoded: &[u8],
    row_count: i64,
    nodes: &[FieldNode],
    buffers: &[IpcBuffer],
    body: &[u8],
) -> Vec<u8> {
    let metadata = ipc_batch_metadata(
        row_count,
        Some(nodes),
        Some(buffers),
        i64::try_from(body.len()).unwrap(),
        false,
    );
    replace_batch_message(encoded, &metadata, body)
}

pub(super) fn ipc_batch_metadata(
    row_count: i64,
    nodes: Option<&[FieldNode]>,
    buffers: Option<&[IpcBuffer]>,
    body_length: i64,
    compressed: bool,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = nodes.map(|nodes| builder.create_vector(nodes));
    let buffers = buffers.map(|buffers| builder.create_vector(buffers));
    let compression =
        compressed.then(|| BodyCompression::create(&mut builder, &BodyCompressionArgs::default()));
    let batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: row_count,
            nodes,
            buffers,
            compression,
            ..RecordBatchArgs::default()
        },
    );
    let message = IpcMessage::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: body_length,
            ..MessageArgs::default()
        },
    );
    builder.finish(message, None);
    builder.finished_data().to_vec()
}

pub(super) struct FieldLayout {
    pub(super) nodes: Range<usize>,
    pub(super) buffers: Range<usize>,
}

pub(super) struct TestLayout {
    pub(super) nodes: Vec<FieldNode>,
    pub(super) buffers: Vec<Range<usize>>,
    fields: Vec<FieldLayout>,
}

// Only locates fixture fields for corruption; it deliberately does not validate them.
fn layout_counts(data_type: &DataType) -> (usize, usize) {
    match data_type {
        DataType::Null => (1, 0),
        DataType::Utf8 | DataType::Binary => (1, 3),
        DataType::List(child) => {
            let (nodes, buffers) = layout_counts(child.data_type());
            (nodes + 1, buffers + 2)
        }
        DataType::Struct(children) => children.iter().fold((1, 1), |(nodes, buffers), field| {
            let child = layout_counts(field.data_type());
            (nodes + child.0, buffers + child.1)
        }),
        _ => (1, 2),
    }
}

pub(super) fn parsed_layout<'a>(
    encoded: &'a [u8],
    codec: &SchemaBoundChangeCodec,
) -> (stream::ParsedChange<'a>, TestLayout) {
    let schema = codec.schema();
    let parsed =
        stream::parse_record_batch(encoded, 40, stream::physical_schema(&schema), schema).unwrap();
    let mut cursor = (0, 0);
    let fields = parsed
        .physical_schema
        .fields()
        .iter()
        .map(|field| {
            let counts = layout_counts(field.data_type());
            let field = FieldLayout {
                nodes: cursor.0..cursor.0 + counts.0,
                buffers: cursor.1..cursor.1 + counts.1,
            };
            cursor.0 += counts.0;
            cursor.1 += counts.1;
            field
        })
        .collect();
    let nodes = parsed.batch.nodes().unwrap().iter().copied().collect();
    let buffers = parsed
        .batch
        .buffers()
        .unwrap()
        .iter()
        .map(|buffer| {
            let start = usize::try_from(buffer.offset()).unwrap();
            start..start + usize::try_from(buffer.length()).unwrap()
        })
        .collect();
    (
        parsed,
        TestLayout {
            nodes,
            buffers,
            fields,
        },
    )
}

pub(super) fn field_layout<'layout>(
    parsed: &stream::ParsedChange<'_>,
    layout: &'layout TestLayout,
    name: &str,
) -> &'layout FieldLayout {
    let index = parsed.physical_schema.index_of(name).unwrap();
    &layout.fields[index]
}

pub(super) fn field_buffer_range(
    encoded: &[u8],
    codec: &SchemaBoundChangeCodec,
    name: &str,
    own_buffer: usize,
) -> Range<usize> {
    let (parsed, layout) = parsed_layout(encoded, codec);
    let field = field_layout(&parsed, &layout, name);
    let relative = layout.buffers[field.buffers.start + own_buffer].clone();
    let body_start = parsed.body.as_ptr() as usize - encoded.as_ptr() as usize;
    body_start + relative.start..body_start + relative.end
}

pub(super) fn corrupt_layout(
    encoded: &[u8],
    codec: &SchemaBoundChangeCodec,
    edit: impl FnOnce(
        &stream::ParsedChange<'_>,
        &TestLayout,
        &mut [FieldNode],
        &mut [IpcBuffer],
    ) -> usize,
) -> Vec<u8> {
    let (parsed, layout) = parsed_layout(encoded, codec);
    let mut nodes = layout.nodes.clone();
    let mut buffers = parsed
        .batch
        .buffers()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let truncate = edit(&parsed, &layout, &mut nodes, &mut buffers);
    replace_batch_layout(
        encoded,
        parsed.batch.length(),
        &nodes,
        &buffers,
        &parsed.body[..parsed.body.len() - truncate],
    )
}

pub(super) fn assert_both_invalid_encoding(encoded: &[u8], codec: &SchemaBoundChangeCodec) {
    assert_invalid_encoding_without_decoder_panic(codec.decode(encoded));
    assert_invalid_encoding_without_decoder_panic(codec.decode_owned(encoded.to_vec()));
}

pub(super) fn assert_arrow_error(result: &Result<Change, CodecError>) {
    assert!(
        matches!(result, Err(CodecError::Arrow(_))),
        "expected Arrow, found {result:?}"
    );
}

pub(super) fn assert_invalid_encoding_without_decoder_panic(result: Result<Change, CodecError>) {
    match result {
        Err(CodecError::InvalidEncoding { message }) => {
            assert_ne!(
                message, "Arrow IPC decoding panicked",
                "decoder panic was caught"
            );
        }
        other => panic!("expected InvalidEncoding, found {other:?}"),
    }
}
