use std::{collections::HashMap, fmt::Write as _, io::Cursor, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, RecordBatchOptions, StructArray,
    UInt64Array, new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, Buffer as IpcBuffer, Endianness, FieldNode,
    Message as IpcMessage, MessageArgs, MessageHeader, MetadataVersion,
    RecordBatch as IpcRecordBatch, RecordBatchArgs, Schema as IpcSchema, SchemaArgs,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use flatbuffers::FlatBufferBuilder;

use super::{
    CodecError, batch::BatchLayout, decode_change, decode_change_projected, encode_change, stream,
};
use crate::{
    Change, ChangeError, ChangeProjection, MAX_NESTING_DEPTH, ProjectionError,
    tests::{assert_change_eq, representative_change},
};

const KIND_KEY: &str = "dogpaddle.kind";
const VERSION_KEY: &str = "dogpaddle.change.version";
const OFFSETS_BUFFER: usize = 1;
const VARIABLE_VALUES_BUFFER: usize = 2;

fn simple_change(diffs: &[i64]) -> Change {
    let values = (0..u64::try_from(diffs.len()).unwrap()).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn marked_metadata() -> HashMap<String, String> {
    HashMap::from([
        (KIND_KEY.to_owned(), "change".to_owned()),
        (VERSION_KEY.to_owned(), "1".to_owned()),
    ])
}

fn unit_physical_schema(metadata: HashMap<String, String>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("$dogpaddle.diff", DataType::Int64, false)],
        metadata,
    ))
}

fn unit_physical_batch(schema: SchemaRef, diffs: Int64Array) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(diffs)]).unwrap()
}

fn encode_stream(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    encode_stream_with_options(
        schema,
        batches,
        IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
    )
}

fn encode_stream_with_options(
    schema: &Schema,
    batches: &[RecordBatch],
    options: IpcWriteOptions,
) -> Vec<u8> {
    let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, options).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.into_inner().unwrap()
}

fn frame_ipc_message(metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let metadata_len = metadata.len().next_multiple_of(8);
    let mut framed = Vec::with_capacity(8 + metadata_len + body.len());
    framed.extend_from_slice(&[0xff; 4]);
    framed.extend_from_slice(&i32::try_from(metadata_len).unwrap().to_le_bytes());
    framed.extend_from_slice(metadata);
    framed.resize(8 + metadata_len, 0);
    framed.extend_from_slice(body);
    framed
}

fn replace_batch_message(encoded: &[u8], metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
    let batch_offset = usize::try_from(reader.get_ref().position()).unwrap();
    let mut replaced = encoded[..batch_offset].to_vec();
    replaced.extend_from_slice(&frame_ipc_message(metadata, body));
    replaced.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
    replaced
}

fn replace_batch_layout(
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

fn ipc_batch_metadata(
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

fn big_endian_schema_stream() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let fields = builder.create_vector::<flatbuffers::WIPOffset<arrow_ipc::Field<'_>>>(&[]);
    let schema = IpcSchema::create(
        &mut builder,
        &SchemaArgs {
            endianness: Endianness::Big,
            fields: Some(fields),
            ..SchemaArgs::default()
        },
    );
    let message = IpcMessage::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            ..MessageArgs::default()
        },
    );
    builder.finish(message, None);
    let mut encoded = frame_ipc_message(builder.finished_data(), &[]);
    encoded.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
    encoded
}

fn parsed_layout(encoded: &[u8]) -> (stream::ParsedChange<'_>, BatchLayout) {
    let parsed = stream::parse(encoded).unwrap();
    let layout = BatchLayout::parse(&parsed).unwrap();
    (parsed, layout)
}

fn field_layout<'layout>(
    parsed: &stream::ParsedChange<'_>,
    layout: &'layout BatchLayout,
    name: &str,
) -> &'layout super::batch::FieldLayout {
    let index = parsed.physical_schema.index_of(name).unwrap();
    &layout.fields[index]
}

fn field_buffer_range(encoded: &[u8], name: &str, own_buffer: usize) -> Range<usize> {
    let (parsed, layout) = parsed_layout(encoded);
    let field = field_layout(&parsed, &layout, name);
    let relative = layout.buffers[field.buffers.start + own_buffer].clone();
    let body_start = parsed.body.as_ptr() as usize - encoded.as_ptr() as usize;
    body_start + relative.start..body_start + relative.end
}

fn corrupt_layout(
    encoded: &[u8],
    edit: impl FnOnce(
        &stream::ParsedChange<'_>,
        &BatchLayout,
        &mut [FieldNode],
        &mut [IpcBuffer],
    ) -> usize,
) -> Vec<u8> {
    let (parsed, layout) = parsed_layout(encoded);
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

fn assert_both_reject(encoded: &[u8], projection: &ChangeProjection) {
    assert!(decode_change(encoded).is_err());
    assert!(decode_change_projected(encoded, projection).is_err());
}

fn assert_both_reject_metadata(encoded: &[u8], projection: &ChangeProjection) {
    assert!(matches!(
        decode_change(encoded),
        Err(CodecError::InvalidEncoding { .. })
    ));
    assert!(matches!(
        decode_change_projected(encoded, projection),
        Err(CodecError::InvalidEncoding { .. })
    ));
}

fn assert_type_round_trips(data_type: &DataType) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        data_type.clone(),
        true,
    )]));
    let records =
        RecordBatch::try_new(Arc::clone(&schema), vec![new_null_array(data_type, 1)]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let encoded = encode_change(&change).unwrap();
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
}

fn hex(encoded: &[u8]) -> String {
    let mut output = String::with_capacity(encoded.len() * 2);
    for byte in encoded {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

#[test]
fn complete_round_trip_preserves_order_and_is_a_standard_marked_arrow_stream() {
    let change = representative_change();
    let encoded = encode_change(&change).unwrap();
    let decoded = decode_change(&encoded).unwrap();

    assert_change_eq(&decoded, &change);
    assert_eq!(decoded.diffs().values(), &[1, -1, 2]);

    let slice = change.try_slice(1, 2).unwrap();
    let slice_encoded = encode_change(&slice).unwrap();
    assert_change_eq(&decode_change(&slice_encoded).unwrap(), &slice);
    let identity =
        ChangeProjection::try_new(slice.schema(), 0..slice.schema().fields().len()).unwrap();
    assert_change_eq(
        &decode_change_projected(&slice_encoded, &identity).unwrap(),
        &slice,
    );
    let empty = ChangeProjection::try_new(slice.schema(), []).unwrap();
    let empty = decode_change_projected(&slice_encoded, &empty).unwrap();
    assert_eq!(empty.num_rows(), 2);
    assert_eq!(empty.diffs().values(), &[-1, 2]);

    let mut reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let schema = reader.schema();

    assert_eq!(schema.field(0).name(), "$dogpaddle.diff");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.metadata().get(KIND_KEY).unwrap(), "change");
    assert_eq!(schema.metadata().get(VERSION_KEY).unwrap(), "1");
    let physical = reader.next().unwrap().unwrap();
    assert_eq!(physical.num_columns(), change.records().num_columns() + 1);
    assert_eq!(
        physical
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        change.diffs()
    );
    assert!(reader.next().is_none());
}

#[test]
fn zero_column_change_stream_has_stable_golden_bytes() {
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let records =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1])).unwrap();
    let expected = concat!(
        "ffffffff000100001000000000000a000e000c000b0004000a00000014000000",
        "0000000104000a000c000000080004000a000000080000007800000002000000",
        "3c00000004000000d4ffffff0800000010000000060000006368616e67650000",
        "0e000000646f67706164646c652e6b696e64000008000c000800040008000000",
        "080000000c000000010000003100000018000000646f67706164646c652e6368",
        "616e67652e76657273696f6e0000000001000000140000001000140010000000",
        "0f00040000000800100000001800000020000000000000021c00000008000c00",
        "04000b00080000004000000000000001000000000f00000024646f6770616464",
        "6c652e6469666600ffffffff88000000100000000c001a001800170004000800",
        "0c000000200000001000000000000000000000000000000304000a0018000c00",
        "080004000a0000002c0000001000000001000000000000000000000001000000",
        "0100000000000000000000000000000000000000020000000000000000000000",
        "010000000000000008000000000000000800000000000000ff00000000000000",
        "ffffffffffffffffffffffff00000000",
    );

    let encoded = encode_change(&change).unwrap();
    assert_eq!(hex(&encoded), expected);
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
}

#[test]
fn metadata_insertion_order_does_not_change_stream_bytes() {
    let encode = |metadata| {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::UInt64, false)],
            metadata,
        ));
        let records =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap();
        encode_change(&Change::try_new(records, Int64Array::from(vec![1])).unwrap()).unwrap()
    };
    assert_eq!(
        encode(HashMap::from([
            ("z".to_owned(), "last".to_owned()),
            ("a".to_owned(), "first".to_owned()),
        ])),
        encode(HashMap::from([
            ("a".to_owned(), "first".to_owned()),
            ("z".to_owned(), "last".to_owned()),
        ]))
    );
}

#[test]
fn zero_logical_columns_keep_their_non_zero_row_count() {
    let options = RecordBatchOptions::new().with_row_count(Some(2));
    let records =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1, 1])).unwrap();
    let encoded = encode_change(&change).unwrap();
    assert_change_eq(&decode_change(&encoded).unwrap(), &change);

    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &projection).unwrap(),
        &change,
    );
}

#[test]
fn every_scalar_and_maximum_nested_layout_round_trips() {
    for data_type in [
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
    ] {
        assert_type_round_trips(&data_type);
    }

    let mut list = DataType::Int64;
    let mut structure = DataType::Int64;
    let mut mixed = DataType::Int64;
    for depth in 0..MAX_NESTING_DEPTH {
        list = DataType::List(Arc::new(Field::new("item", list, true)));
        structure = DataType::Struct(vec![Field::new("member", structure, true)].into());
        mixed = if depth.is_multiple_of(2) {
            DataType::List(Arc::new(Field::new("item", mixed, true)))
        } else {
            DataType::Struct(vec![Field::new("member", mixed, true)].into())
        };
    }
    for data_type in [list, structure, mixed] {
        assert_type_round_trips(&data_type);
    }
}

#[test]
fn decoder_rejects_invalid_physical_schema_and_version_markers() {
    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let schemas = [
        unit_physical_schema(HashMap::new()),
        unit_physical_schema(HashMap::from([
            (KIND_KEY.to_owned(), "record".to_owned()),
            (VERSION_KEY.to_owned(), "1".to_owned()),
        ])),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("diff", DataType::Int64, false)],
            marked_metadata(),
        )),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("$dogpaddle.diff", DataType::Int64, true)],
            marked_metadata(),
        )),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("$dogpaddle.diff", DataType::UInt64, false)],
            marked_metadata(),
        )),
    ];
    for schema in schemas {
        let column: ArrayRef = match schema.field(0).data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(vec![1])),
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![1])),
            data_type => unreachable!("unexpected physical diff type {data_type}"),
        };
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        assert_both_reject(&encode_stream(&schema, &[batch]), &projection);
    }

    let mut version = marked_metadata();
    version.insert(VERSION_KEY.to_owned(), "2".to_owned());
    let schema = unit_physical_schema(version);
    let encoded = encode_stream(
        &schema,
        &[unit_physical_batch(
            Arc::clone(&schema),
            Int64Array::from(vec![1]),
        )],
    );
    assert!(matches!(
        decode_change(&encoded),
        Err(CodecError::UnsupportedVersion { version }) if version == "2"
    ));
    assert!(decode_change_projected(&encoded, &projection).is_err());

    let mut unknown = marked_metadata();
    unknown.insert("dogpaddle.unknown".to_owned(), "value".to_owned());
    let schema = unit_physical_schema(unknown);
    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    assert_both_reject(&encode_stream(&schema, &[batch]), &projection);
}

#[test]
fn decoder_rejects_incomplete_noncanonical_or_unsupported_streams() {
    let change = simple_change(&[-1, 1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    for end in 0..encoded.len() {
        assert_both_reject(&encoded[..end], &projection);
    }

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_both_reject(&trailing, &projection);
    assert_both_reject(&encoded[4..], &projection);

    let reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let batch_offset = usize::try_from(reader.get_ref().position()).unwrap();
    let mut legacy_batch = encoded.clone();
    legacy_batch.drain(batch_offset..batch_offset + 4);
    assert_both_reject(&legacy_batch, &projection);

    let schema = unit_physical_schema(marked_metadata());
    let unit_projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert_both_reject(&encode_stream(&schema, &[]), &unit_projection);
    let batches = [
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1])),
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![-1])),
    ];
    assert_both_reject(&encode_stream(&schema, &batches), &unit_projection);

    let mut oversized_metadata = encoded.clone();
    oversized_metadata[4..8].copy_from_slice(&(i32::MAX - 7).to_le_bytes());
    assert_both_reject_metadata(&oversized_metadata, &projection);
    let metadata = ipc_batch_metadata(1, None, None, i64::MAX - 7, false);
    let oversized_body = replace_batch_message(&encoded, &metadata, &[]);
    assert_both_reject_metadata(&oversized_body, &projection);

    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap();
    assert_both_reject(
        &encode_stream_with_options(&schema, &[batch], options),
        &unit_projection,
    );
    assert_both_reject(&big_endian_schema_stream(), &unit_projection);
    let compressed =
        replace_batch_message(&encoded, &ipc_batch_metadata(1, None, None, 0, true), &[]);
    assert_both_reject(&compressed, &projection);
}

#[test]
fn decoder_rechecks_zero_diff_after_arrow_decoding() {
    let schema = unit_physical_schema(marked_metadata());
    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let encoded = encode_stream(
        &schema,
        &[unit_physical_batch(
            Arc::clone(&schema),
            Int64Array::from(vec![0]),
        )],
    );
    assert!(matches!(
        decode_change(&encoded),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));
    assert!(matches!(
        decode_change_projected(&encoded, &projection),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));

    let empty = unit_physical_batch(Arc::clone(&schema), Int64Array::from(Vec::<i64>::new()));
    assert_both_reject(&encode_stream(&schema, &[empty]), &projection);
}

#[test]
fn projected_decode_matches_memory_for_every_top_level_layout() {
    let change = representative_change();
    let encoded = encode_change(&change).unwrap();
    let field_count = change.schema().fields().len();
    let mut selections = vec![vec![], vec![0, 2, 4, 6]];
    selections.extend((0..field_count).map(|index| vec![index]));
    selections.push((0..field_count).collect());

    for selection in selections {
        let full = selection.len() == field_count;
        let projection = ChangeProjection::try_new(change.schema(), selection).unwrap();
        let expected = change.try_project(&projection).unwrap();
        let actual = decode_change_projected(&encoded, &projection).unwrap();
        assert_change_eq(&actual, &expected);
        assert_eq!(actual.schema(), projection.output_schema());
        if full {
            assert_eq!(encode_change(&actual).unwrap(), encoded);
        }
    }
}

#[test]
fn projected_body_omits_a_large_unselected_binary_field() {
    let huge = vec![7_u8; 64 * 1_024];
    let schema = Arc::new(Schema::new(vec![
        Field::new("head", DataType::UInt64, false),
        Field::new("huge", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10])),
            Arc::new(BinaryArray::from(vec![Some(huge.as_slice())])),
            Arc::new(UInt64Array::from(vec![20])),
        ],
    )
    .unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let projection = ChangeProjection::try_new(schema, [0, 2]).unwrap();
    let encoded = encode_change(&change).unwrap();
    let (parsed, layout) = parsed_layout(&encoded);
    let compact = layout.compact(parsed.body, &projection).unwrap();

    assert!(compact.body.len() * 100 < parsed.body.len());
    assert_change_eq(
        &decode_change_projected(&encoded, &projection).unwrap(),
        &change.try_project(&projection).unwrap(),
    );
}

#[test]
fn projected_decode_skips_only_unselected_utf8_value_validation() {
    let change = representative_change();
    let mut encoded = encode_change(&change).unwrap();
    let values = field_buffer_range(&encoded, "label", VARIABLE_VALUES_BUFFER);
    encoded[values.start] = 0xff;

    let schema = change.schema();
    let mut metadata = schema.metadata().clone();
    metadata.insert("schema-drift".to_owned(), "true".to_owned());
    let drifted = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    let label = drifted.index_of("label").unwrap();
    let drifted = ChangeProjection::try_new(drifted, [label]).unwrap();
    assert!(matches!(
        decode_change_projected(&encoded, &drifted),
        Err(CodecError::Projection(ProjectionError::SchemaMismatch))
    ));

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert!(decode_change_projected(&encoded, &keep_id).is_ok());
    let label = change.schema().index_of("label").unwrap();
    let select_label = ChangeProjection::try_new(change.schema(), [label]).unwrap();
    assert!(decode_change_projected(&encoded, &select_label).is_err());
    assert!(decode_change(&encoded).is_err());
}

#[test]
fn projected_decode_skips_only_unselected_list_offset_validation() {
    let change = representative_change();
    let mut encoded = encode_change(&change).unwrap();
    let offsets = field_buffer_range(&encoded, "items", OFFSETS_BUFFER);
    let last = offsets.start + change.num_rows() * size_of::<i32>();
    encoded[last..last + size_of::<i32>()].copy_from_slice(&i32::MAX.to_le_bytes());

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert!(decode_change_projected(&encoded, &keep_id).is_ok());
    let items = change.schema().index_of("items").unwrap();
    let select_items = ChangeProjection::try_new(change.schema(), [items]).unwrap();
    assert!(decode_change_projected(&encoded, &select_items).is_err());
    assert!(decode_change(&encoded).is_err());
}

#[test]
fn both_decoders_validate_all_unselected_batch_metadata() {
    let change = representative_change();
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let short_fixed_width = corrupt_layout(&encoded, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() - 8);
        8
    });
    let invalid_struct = corrupt_layout(&encoded, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "object").nodes.start;
        nodes[index] = FieldNode::new(parsed.batch.length() - 1, nodes[index].null_count());
        0
    });
    let invalid_non_nullable = corrupt_layout(&encoded, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "payload").nodes.start;
        nodes[index] = FieldNode::new(nodes[index].length(), 1);
        0
    });
    let out_of_body = corrupt_layout(&encoded, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() + 8);
        0
    });

    for malformed in [
        short_fixed_width,
        invalid_struct,
        invalid_non_nullable,
        out_of_body,
    ] {
        assert_both_reject_metadata(&malformed, &projection);
    }
}

#[test]
fn nullable_struct_masks_nulls_in_a_non_nullable_child() {
    let child = Arc::new(Field::new("value", DataType::Int64, false));
    let object = StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(vec![None, Some(2)]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "object",
        object.data_type().clone(),
        true,
    )]));
    let records = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(object)]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, 1])).unwrap();
    let encoded = encode_change(&change).unwrap();

    assert_change_eq(&decode_change(&encoded).unwrap(), &change);
    let identity = ChangeProjection::try_new(Arc::clone(&schema), [0]).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &identity).unwrap(),
        &change,
    );
    let empty = ChangeProjection::try_new(schema, []).unwrap();
    let empty = decode_change_projected(&encoded, &empty).unwrap();
    assert_eq!(empty.num_rows(), 2);
    assert_eq!(empty.diffs().values(), &[1, 1]);
}
