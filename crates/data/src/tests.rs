use std::{collections::HashMap, fmt::Write as _, io::Cursor, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, UInt64Array, new_null_array, types::Int64Type,
};
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, Buffer as IpcBuffer, Endianness, FieldNode,
    Message as IpcMessage, MessageArgs, MessageHeader, MetadataVersion,
    RecordBatch as IpcRecordBatch, RecordBatchArgs, Schema as IpcSchema, SchemaArgs,
    reader::StreamReader,
    root_as_message,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use flatbuffers::FlatBufferBuilder;

use super::{
    Change, ChangeError, ChangeProjection, CodecError, MAX_NESTING_DEPTH, ProjectionError,
    SchemaError, codec::projected_body_lengths_for_test, decode_change, decode_change_projected,
    encode_change, validate_schema,
};

const KIND_KEY: &str = "dogpaddle.kind";
const VERSION_KEY: &str = "dogpaddle.change.version";

fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn simple_change(diffs: impl IntoIterator<Item = i64>) -> Change {
    let diffs: Vec<_> = diffs.into_iter().collect();
    let values: Vec<_> = (0..u64::try_from(diffs.len()).unwrap()).collect();
    let records =
        RecordBatch::try_new(simple_schema(), vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

fn projection_change() -> Change {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(Vec::<Option<i64>>::new()),
    ]);
    let name_field = Arc::new(Field::new("name", DataType::Utf8, true));
    let score_field = Arc::new(Field::new("score", DataType::Int64, false));
    let object = StructArray::from(vec![
        (
            Arc::clone(&name_field),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        ),
        (
            Arc::clone(&score_field),
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![7, 7, 8])),
        Arc::new(StringArray::from(vec![Some("add"), None, Some("next")])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(items),
        Arc::new(object),
        new_null_array(&DataType::Null, 3),
        Arc::new(UInt64Array::from(vec![70, 71, 72])),
    ];
    let mut fields = [
        "id", "label", "enabled", "items", "object", "nothing", "tail",
    ]
    .into_iter()
    .zip(&columns)
    .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
    .collect::<Vec<_>>();
    fields[0] = Field::new("id", DataType::UInt64, false);
    fields[1] = fields[1]
        .clone()
        .with_metadata(HashMap::from([("semantic".to_owned(), "label".to_owned())]));
    fields[6] = Field::new("tail", DataType::UInt64, false);
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("source".to_owned(), "projection-test".to_owned())]),
    ));
    let records = RecordBatch::try_new(schema, columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap()
}

fn event_change(events: &[(u64, i64)]) -> Change {
    let values = events.iter().map(|(value, _)| *value).collect::<Vec<_>>();
    let diffs = events.iter().map(|(_, diff)| *diff).collect::<Vec<_>>();
    let records =
        RecordBatch::try_new(simple_schema(), vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

fn events(change: &Change) -> Vec<(u64, i64)> {
    let values = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    values
        .values()
        .iter()
        .copied()
        .zip(change.diffs().values().iter().copied())
        .collect()
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
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    encode_stream_with_options(schema, batches, options)
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
    let padded_metadata_len = metadata.len().next_multiple_of(8);
    let mut framed = Vec::with_capacity(8 + padded_metadata_len + body.len());
    framed.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
    framed.extend_from_slice(&i32::try_from(padded_metadata_len).unwrap().to_le_bytes());
    framed.extend_from_slice(metadata);
    framed.resize(8 + padded_metadata_len, 0);
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

fn message_layout(encoded: &[u8], offset: usize) -> (Range<usize>, Range<usize>) {
    let metadata_len = usize::try_from(i32::from_le_bytes(
        encoded[offset + 4..offset + 8].try_into().unwrap(),
    ))
    .unwrap();
    let metadata = offset + 8..offset + 8 + metadata_len;
    let message = root_as_message(&encoded[metadata.clone()]).unwrap();
    let body_len = usize::try_from(message.bodyLength()).unwrap();
    let body = metadata.end..metadata.end + body_len;
    (metadata, body)
}

fn batch_body_and_buffers(encoded: &[u8]) -> (Range<usize>, Vec<IpcBuffer>) {
    let (body, _, _, buffers) = batch_body_nodes_and_buffers(encoded);
    (body, buffers)
}

fn batch_body_nodes_and_buffers(
    encoded: &[u8],
) -> (Range<usize>, i64, Vec<FieldNode>, Vec<IpcBuffer>) {
    let (_, schema_body) = message_layout(encoded, 0);
    let (batch_metadata, batch_body) = message_layout(encoded, schema_body.end);
    let message = root_as_message(&encoded[batch_metadata]).unwrap();
    let batch = message.header_as_record_batch().unwrap();
    let nodes = batch.nodes().unwrap().iter().copied().collect::<Vec<_>>();
    let buffers = batch.buffers().unwrap().iter().copied().collect();
    (batch_body, batch.length(), nodes, buffers)
}

fn replace_batch_layout(
    encoded: &[u8],
    row_count: i64,
    nodes: &[FieldNode],
    buffers: &[IpcBuffer],
    body: &[u8],
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(nodes);
    let buffers = builder.create_vector(buffers);
    let batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: row_count,
            nodes: Some(nodes),
            buffers: Some(buffers),
            ..RecordBatchArgs::default()
        },
    );
    let message = IpcMessage::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: i64::try_from(body.len()).unwrap(),
            ..MessageArgs::default()
        },
    );
    builder.finish(message, None);
    replace_batch_message(encoded, builder.finished_data(), body)
}

fn ipc_batch_metadata(body_length: i64, compressed: bool) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let compression =
        compressed.then(|| BodyCompression::create(&mut builder, &BodyCompressionArgs::default()));
    let batch = IpcRecordBatch::create(
        &mut builder,
        &RecordBatchArgs {
            length: 1,
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
    let no_fields: [flatbuffers::WIPOffset<arrow_ipc::Field<'_>>; 0] = [];
    let fields = builder.create_vector(&no_fields);
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

fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
}

fn assert_schema_type_round_trips(data_type: &DataType) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        data_type.clone(),
        true,
    )]));
    assert!(validate_schema(&schema).is_ok());
    let records =
        RecordBatch::try_new(Arc::clone(&schema), vec![new_null_array(data_type, 1)]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let encoded = encode_change(&change).unwrap();
    let decoded = decode_change(&encoded).unwrap();
    assert_eq!(decoded.schema(), change.schema());

    let selected = ChangeProjection::try_new(Arc::clone(&schema), [0]).unwrap();
    let selected = decode_change_projected(&encoded, &selected).unwrap();
    assert_change_eq(&selected, &change);
    let skipped = ChangeProjection::try_new(schema, []).unwrap();
    let skipped = decode_change_projected(&encoded, &skipped).unwrap();
    assert_eq!(skipped.records().num_columns(), 0);
    assert_eq!(skipped.num_rows(), 1);
    assert_eq!(skipped.diffs().values(), &[1]);
}

fn hex(encoded: &[u8]) -> String {
    let mut output = String::with_capacity(encoded.len() * 2);
    for byte in encoded {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

#[test]
fn change_enforces_rows_and_non_zero_non_null_diffs() {
    let empty = RecordBatch::new_empty(simple_schema());
    assert!(matches!(
        Change::try_new(empty, Int64Array::from(Vec::<i64>::new())),
        Err(ChangeError::Empty)
    ));

    let records = RecordBatch::try_new(
        simple_schema(),
        vec![Arc::new(UInt64Array::from(vec![1, 2]))],
    )
    .unwrap();
    assert!(matches!(
        Change::try_new(records.clone(), Int64Array::from(vec![1])),
        Err(ChangeError::LengthMismatch {
            records: 2,
            diffs: 1
        })
    ));
    assert!(matches!(
        Change::try_new(records.clone(), Int64Array::from(vec![Some(1), None])),
        Err(ChangeError::NullDiff { index: 1 })
    ));
    assert!(matches!(
        Change::try_new(records, Int64Array::from(vec![i64::MIN, 0])),
        Err(ChangeError::ZeroDiff { index: 1 })
    ));

    let accepted = simple_change([i64::MIN, -1, 1, i64::MAX]);
    assert_eq!(accepted.diffs().values(), &[i64::MIN, -1, 1, i64::MAX]);
}

#[test]
fn change_preserves_event_order_and_record_diff_alignment() {
    let expected = vec![(7, 1), (8, 1), (7, -1), (9, 1), (9, -1)];
    let change = event_change(&expected);

    assert_eq!(events(&change), expected);
}

#[test]
fn change_represents_a_negative_prefix_without_assuming_prior_state() {
    let change = event_change(&[(7, -1), (7, 1)]);

    assert_eq!(events(&change), [(7, -1), (7, 1)]);
}

#[test]
fn slice_preserves_a_contiguous_event_subsequence_and_rejects_invalid_ranges() {
    let change = event_change(&[(7, 1), (8, 1), (7, -1), (9, 1)]);
    let slice = change.try_slice(1, 2).unwrap();
    assert_eq!(slice.num_rows(), 2);
    assert_eq!(events(&slice), [(8, 1), (7, -1)]);
    assert!(matches!(change.try_slice(0, 0), Err(ChangeError::Empty)));
    assert!(matches!(
        change.try_slice(3, 2),
        Err(ChangeError::SliceOutOfBounds { .. })
    ));
}

#[test]
fn projection_only_deletes_columns_and_shares_in_memory_arrow_buffers() {
    let change = projection_change();
    let projection = ChangeProjection::try_new(change.schema(), [0, 3, 6]).unwrap();
    let projected = change.try_project(&projection).unwrap();

    assert_eq!(
        projected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "items", "tail"]
    );
    assert_eq!(projected.schema().metadata(), change.schema().metadata());
    assert_eq!(projected.num_rows(), 3);
    assert_eq!(projected.diffs().values(), &[1, -1, 2]);
    for (source, target) in [(0, 0), (3, 1), (6, 2)] {
        assert!(Arc::ptr_eq(
            change.records().column(source),
            projected.records().column(target)
        ));
    }
    assert_eq!(
        change.diffs().values().as_ptr(),
        projected.diffs().values().as_ptr()
    );
}

#[test]
fn empty_and_full_projections_preserve_rows_diffs_and_schema_identity() {
    let change = projection_change();
    let empty = ChangeProjection::try_new(change.schema(), []).unwrap();
    let empty = change.try_project(&empty).unwrap();
    assert_eq!(empty.records().num_columns(), 0);
    assert_eq!(empty.num_rows(), change.num_rows());
    assert_eq!(empty.diffs(), change.diffs());
    assert_eq!(empty.schema().metadata(), change.schema().metadata());

    let field_count = change.schema().fields().len();
    let full = ChangeProjection::try_new(change.schema(), 0..field_count).unwrap();
    let full = change.try_project(&full).unwrap();
    assert_change_eq(&full, &change);
    for index in 0..field_count {
        assert!(Arc::ptr_eq(
            change.records().column(index),
            full.records().column(index)
        ));
    }
}

#[test]
fn zero_column_change_accepts_the_empty_projection() {
    let schema = Arc::new(Schema::empty());
    let records = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(2)),
    )
    .unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1, 1])).unwrap();
    let projection = ChangeProjection::try_new(schema, []).unwrap();
    let projected = change.try_project(&projection).unwrap();
    assert_eq!(projected.records().num_columns(), 0);
    assert_eq!(projected.num_rows(), 2);
    assert_eq!(projected.diffs().values(), &[-1, 1]);
}

#[test]
fn projection_rejects_reordering_duplicates_bounds_and_schema_drift() {
    let change = projection_change();
    let schema = change.schema();
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [2, 0]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 2,
            current: 0
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [1, 1]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 1,
            current: 1
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [schema.fields().len()]),
        Err(ProjectionError::FieldOutOfBounds { .. })
    ));
    assert!(matches!(
        ChangeProjection::try_new(schema, [usize::MAX]),
        Err(ProjectionError::FieldOutOfBounds { .. })
    ));

    let other = ChangeProjection::try_new(simple_schema(), [0]).unwrap();
    assert!(matches!(
        change.try_project(&other),
        Err(ProjectionError::SchemaMismatch)
    ));
}

#[test]
fn projected_ipc_decode_matches_in_memory_projection_for_every_layout_shape() {
    let change = projection_change();
    let encoded = encode_change(&change).unwrap();
    let selections = [
        vec![],
        vec![0],
        vec![1],
        vec![2],
        vec![3],
        vec![4],
        vec![5],
        vec![6],
        vec![0, 2, 6],
        (0..change.schema().fields().len()).collect(),
    ];
    for selection in selections {
        let projection = ChangeProjection::try_new(change.schema(), selection).unwrap();
        let expected = change.try_project(&projection).unwrap();
        let actual = decode_change_projected(&encoded, &projection).unwrap();
        assert_change_eq(&actual, &expected);
        assert_eq!(actual.schema(), projection.output_schema().clone());
    }
}

#[test]
fn projected_decode_rejects_schema_drift_before_selected_body_values() {
    let change = projection_change();
    let mut corrupted = encode_change(&change).unwrap();
    let (body, buffers) = batch_body_and_buffers(&corrupted);
    let label_values = buffers[6];
    corrupted[body.start + usize::try_from(label_values.offset()).unwrap()] = 0xff;

    let schema = change.schema();
    let mut metadata = schema.metadata().clone();
    metadata.insert("schema-drift".to_owned(), "true".to_owned());
    let drifted = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    let projection = ChangeProjection::try_new(drifted, [1]).unwrap();
    assert!(matches!(
        decode_change_projected(&corrupted, &projection),
        Err(CodecError::Projection(ProjectionError::SchemaMismatch))
    ));
}

#[test]
fn projected_decode_validates_metadata_of_unselected_fields() {
    let change = projection_change();
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let (body_range, row_count, nodes, buffers) = batch_body_nodes_and_buffers(&encoded);
    let body = &encoded[body_range];
    assert_eq!(nodes.len(), 11);
    assert_eq!(buffers.len(), 21);

    let mut short_tail = buffers.clone();
    let tail_values = short_tail.last_mut().unwrap();
    assert_eq!(tail_values.length(), 24);
    *tail_values = IpcBuffer::new(tail_values.offset(), 16);
    let short_tail = replace_batch_layout(
        &encoded,
        row_count,
        &nodes,
        &short_tail,
        &body[..body.len() - 8],
    );
    assert!(matches!(
        decode_change_projected(&short_tail, &projection),
        Err(CodecError::InvalidEncoding { .. })
    ));

    let mut invalid_struct_nodes = nodes.clone();
    invalid_struct_nodes[8] = FieldNode::new(2, invalid_struct_nodes[8].null_count());
    let invalid_struct =
        replace_batch_layout(&encoded, row_count, &invalid_struct_nodes, &buffers, body);
    assert!(matches!(
        decode_change_projected(&invalid_struct, &projection),
        Err(CodecError::InvalidEncoding { .. })
    ));

    let mut out_of_bounds = buffers.clone();
    out_of_bounds[6] = IpcBuffer::new(i64::try_from(body.len() + 8).unwrap(), 1);
    let out_of_bounds = replace_batch_layout(&encoded, row_count, &nodes, &out_of_bounds, body);
    assert!(matches!(
        decode_change_projected(&out_of_bounds, &projection),
        Err(CodecError::InvalidEncoding { .. })
    ));
}

#[test]
fn projected_decode_validates_nested_offsets_only_when_selected() {
    let change = projection_change();
    let mut corrupted = encode_change(&change).unwrap();
    let (body, buffers) = batch_body_and_buffers(&corrupted);
    let list_offsets = buffers[10];
    assert_eq!(list_offsets.length(), 16);
    let last_offset = body.start
        + usize::try_from(list_offsets.offset()).unwrap()
        + change.num_rows() * size_of::<i32>();
    corrupted[last_offset..last_offset + size_of::<i32>()].copy_from_slice(&i32::MAX.to_le_bytes());

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert!(decode_change_projected(&corrupted, &keep_id).is_ok());
    let select_list = ChangeProjection::try_new(change.schema(), [3]).unwrap();
    assert!(decode_change_projected(&corrupted, &select_list).is_err());
    assert!(decode_change(&corrupted).is_err());
}

#[test]
fn full_projected_decode_reencodes_to_the_identical_canonical_stream() {
    let change = projection_change();
    let encoded = encode_change(&change).unwrap();
    let projection =
        ChangeProjection::try_new(change.schema(), 0..change.schema().fields().len()).unwrap();
    let projected = decode_change_projected(&encoded, &projection).unwrap();

    assert_change_eq(&projected, &change);
    assert_eq!(encode_change(&projected).unwrap(), encoded);
}

#[test]
fn projected_ipc_body_omits_a_large_unselected_middle_column() {
    let huge = vec![7_u8; 8 * 1_024 * 1_024];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![10])),
        Arc::new(BinaryArray::from(vec![Some(huge.as_slice())])),
        Arc::new(UInt64Array::from(vec![20])),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("head", DataType::UInt64, false),
        Field::new("huge", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let change = Change::try_new(
        RecordBatch::try_new(Arc::clone(&schema), columns).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(schema, [0, 2]).unwrap();
    let (source_body, compact_body) =
        projected_body_lengths_for_test(&encoded, &projection).unwrap();
    assert!(source_body > 8 * 1_024 * 1_024);
    assert!(compact_body < 1_024);

    let projected = decode_change_projected(&encoded, &projection).unwrap();
    assert_eq!(projected.records().num_columns(), 2);
    assert_eq!(
        projected
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[10]
    );
    assert_eq!(
        projected
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[20]
    );
}

#[test]
fn projected_decode_skips_unselected_value_validation_but_validates_selected_values() {
    let change = projection_change();
    let mut corrupted = encode_change(&change).unwrap();
    let (body, buffers) = batch_body_and_buffers(&corrupted);
    assert_eq!(buffers.len(), 21);
    let label_values = buffers[6];
    let value_offset = usize::try_from(label_values.offset()).unwrap();
    assert!(label_values.length() > 0);
    corrupted[body.start + value_offset] = 0xff;

    assert!(decode_change(&corrupted).is_err());
    let keep = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert!(decode_change_projected(&corrupted, &keep).is_ok());
    let empty = ChangeProjection::try_new(change.schema(), []).unwrap();
    assert!(decode_change_projected(&corrupted, &empty).is_ok());
    let poison = ChangeProjection::try_new(change.schema(), [1]).unwrap();
    assert!(decode_change_projected(&corrupted, &poison).is_err());
}

#[test]
fn empty_projection_still_decodes_and_validates_differences() {
    let schema = unit_physical_schema(marked_metadata());
    let zero = unit_physical_batch(schema.clone(), Int64Array::from(vec![0]));
    let encoded = encode_stream(&schema, &[zero]);
    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert!(matches!(
        decode_change_projected(&encoded, &projection),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));
}

#[test]
fn ipc_round_trip_preserves_and_distinguishes_event_order() {
    let expected = vec![(7, 1), (8, 1), (7, -1), (9, 1), (9, -1)];
    let change = event_change(&expected);
    let decoded = decode_change(&encode_change(&change).unwrap()).unwrap();
    assert_eq!(events(&decoded), expected);

    let reordered = event_change(&[(9, -1), (9, 1), (7, -1), (8, 1), (7, 1)]);
    assert_ne!(
        encode_change(&change).unwrap(),
        encode_change(&reordered).unwrap()
    );
}

#[test]
fn schema_validation_rejects_ambiguous_unsupported_and_reserved_shapes() {
    let duplicate = Schema::new(vec![
        Field::new("same", DataType::Int64, false),
        Field::new("same", DataType::Utf8, true),
    ]);
    assert!(matches!(
        validate_schema(&duplicate),
        Err(SchemaError::DuplicateField { .. })
    ));

    let nested_duplicate = Schema::new(vec![Field::new(
        "object",
        DataType::Struct(
            vec![
                Field::new("same", DataType::Int64, false),
                Field::new("same", DataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    )]);
    assert!(matches!(
        validate_schema(&nested_duplicate),
        Err(SchemaError::DuplicateField { .. })
    ));

    let dictionary = Schema::new(vec![Field::new(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]);
    assert!(matches!(
        validate_schema(&dictionary),
        Err(SchemaError::UnsupportedType { .. })
    ));

    let reserved_field = Schema::new(vec![Field::new("$dogpaddle.diff", DataType::Int64, false)]);
    assert!(matches!(
        validate_schema(&reserved_field),
        Err(SchemaError::ReservedFieldName { .. })
    ));
    let reserved_schema_metadata = Schema::new_with_metadata(
        Vec::<Field>::new(),
        HashMap::from([(KIND_KEY.to_owned(), "user".to_owned())]),
    );
    assert!(matches!(
        validate_schema(&reserved_schema_metadata),
        Err(SchemaError::ReservedMetadataKey { .. })
    ));
    let reserved_field_metadata = Schema::new(vec![
        Field::new("value", DataType::Int64, false).with_metadata(HashMap::from([(
            "dogpaddle.user".to_owned(),
            "value".to_owned(),
        )])),
    ]);
    assert!(matches!(
        validate_schema(&reserved_field_metadata),
        Err(SchemaError::ReservedMetadataKey { .. })
    ));
    let reserved_list_child = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new(
            "$dogpaddle.item",
            DataType::Int64,
            true,
        ))),
        true,
    )]);
    assert!(matches!(
        validate_schema(&reserved_list_child),
        Err(SchemaError::ReservedFieldName { .. })
    ));
}

#[test]
fn every_supported_nesting_shape_round_trips_to_the_documented_limit() {
    let mut accepted_type = DataType::Int64;
    for depth in 0..MAX_NESTING_DEPTH {
        accepted_type = DataType::List(Arc::new(Field::new(
            format!("item_{depth}"),
            accepted_type,
            true,
        )));
        assert_schema_type_round_trips(&accepted_type);
    }

    let mut struct_type = DataType::Int64;
    let mut mixed_type = DataType::Int64;
    for depth in 0..MAX_NESTING_DEPTH {
        struct_type =
            DataType::Struct(vec![Field::new(format!("member_{depth}"), struct_type, true)].into());
        mixed_type = if depth % 2 == 0 {
            DataType::List(Arc::new(Field::new(
                format!("mixed_item_{depth}"),
                mixed_type,
                true,
            )))
        } else {
            DataType::Struct(
                vec![Field::new(
                    format!("mixed_member_{depth}"),
                    mixed_type,
                    true,
                )]
                .into(),
            )
        };
    }
    assert_schema_type_round_trips(&struct_type);
    assert_schema_type_round_trips(&mixed_type);

    let rejected_type = DataType::List(Arc::new(Field::new("too_deep", accepted_type, true)));
    assert!(matches!(
        validate_schema(&Schema::new(vec![Field::new(
            "nested",
            rejected_type,
            true
        )])),
        Err(SchemaError::NestingTooDeep { .. })
    ));
}

#[test]
fn representative_change_is_one_self_describing_standard_arrow_stream() {
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        Some(vec![Some(2), Some(3)]),
    ]);
    let inner_field = Arc::new(Field::new("label", DataType::Utf8, true));
    let object = StructArray::from(vec![(
        inner_field,
        Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
    )]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Float64Array::from(vec![1.5, -2.0])),
        Arc::new(StringArray::from(vec!["old", "new"])),
        Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), Some(&b"b"[..])])),
        Arc::new(list),
        Arc::new(object),
    ];
    let mut fields = ["flag", "number", "text", "bytes", "items", "object"]
        .into_iter()
        .zip(&columns)
        .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
        .collect::<Vec<_>>();
    fields[2] = fields[2]
        .clone()
        .with_metadata(HashMap::from([("semantic".to_owned(), "name".to_owned())]));
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("stream".to_owned(), "representative".to_owned())]),
    ));
    let records = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1, 1])).unwrap();

    let encoded = encode_change(&change).unwrap();
    assert_eq!(encoded, encode_change(&change).unwrap());

    let mut arrow_reader = StreamReader::try_new(Cursor::new(encoded.as_slice()), None).unwrap();
    let physical_schema = arrow_reader.schema();
    assert_eq!(physical_schema.field(0).name(), "$dogpaddle.diff");
    assert_eq!(physical_schema.field(0).data_type(), &DataType::Int64);
    assert!(!physical_schema.field(0).is_nullable());
    assert_eq!(physical_schema.metadata().get(KIND_KEY).unwrap(), "change");
    assert_eq!(physical_schema.metadata().get(VERSION_KEY).unwrap(), "1");
    let physical = arrow_reader.next().unwrap().unwrap();
    assert_eq!(physical.num_columns(), change.records().num_columns() + 1);
    assert_eq!(
        physical
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        change.diffs()
    );
    assert!(arrow_reader.next().is_none());

    let decoded = decode_change(&encoded).unwrap();
    assert_change_eq(&decoded, &change);
    assert_eq!(decoded.schema().as_ref(), schema.as_ref());
}

#[test]
fn metadata_order_does_not_change_the_stable_stream() {
    let mut first_metadata = HashMap::new();
    first_metadata.insert("z".to_owned(), "last".to_owned());
    first_metadata.insert("a".to_owned(), "first".to_owned());
    let first_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::UInt64, false)],
        first_metadata,
    ));
    let mut second_metadata = HashMap::new();
    second_metadata.insert("a".to_owned(), "first".to_owned());
    second_metadata.insert("z".to_owned(), "last".to_owned());
    let second_schema = Arc::new(Schema::new_with_metadata(
        first_schema.fields().clone(),
        second_metadata,
    ));
    let first = Change::try_new(
        RecordBatch::try_new(first_schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let second = Change::try_new(
        RecordBatch::try_new(second_schema, vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();

    assert_eq!(
        encode_change(&first).unwrap(),
        encode_change(&second).unwrap()
    );
}

#[test]
fn zero_column_records_keep_their_non_zero_row_count_through_ipc() {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(2));
    let records = RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![-1, 1])).unwrap();
    let decoded = decode_change(&encode_change(&change).unwrap()).unwrap();

    assert_eq!(decoded.records().num_columns(), 0);
    assert_eq!(decoded.records().num_rows(), 2);
    assert_eq!(decoded.diffs().values(), &[-1, 1]);
}

#[test]
fn decoder_requires_the_physical_schema_contract_and_version_markers() {
    let cases = [
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
    ];
    for schema in cases {
        let batch = unit_physical_batch(schema.clone(), Int64Array::from(vec![1]));
        assert!(decode_change(&encode_stream(&schema, &[batch])).is_err());
    }

    let wrong_type_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("$dogpaddle.diff", DataType::UInt64, false)],
        marked_metadata(),
    ));
    let wrong_type_batch = RecordBatch::try_new(
        wrong_type_schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(decode_change(&encode_stream(&wrong_type_schema, &[wrong_type_batch])).is_err());

    let mut unsupported_metadata = marked_metadata();
    unsupported_metadata.insert(VERSION_KEY.to_owned(), "2".to_owned());
    let unsupported_schema = unit_physical_schema(unsupported_metadata);
    let unsupported_batch =
        unit_physical_batch(unsupported_schema.clone(), Int64Array::from(vec![1]));
    assert!(matches!(
        decode_change(&encode_stream(&unsupported_schema, &[unsupported_batch])),
        Err(CodecError::UnsupportedVersion { version }) if version == "2"
    ));

    let mut unknown_metadata = marked_metadata();
    unknown_metadata.insert("dogpaddle.unknown".to_owned(), "value".to_owned());
    let unknown_schema = unit_physical_schema(unknown_metadata);
    let unknown_batch = unit_physical_batch(unknown_schema.clone(), Int64Array::from(vec![1]));
    assert!(decode_change(&encode_stream(&unknown_schema, &[unknown_batch])).is_err());
}

#[test]
fn decoder_requires_exactly_one_batch_and_canonical_eos() {
    let change = simple_change([-1, 1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    for end in 0..encoded.len() {
        assert!(
            decode_change(&encoded[..end]).is_err(),
            "accepted truncation at byte {end}"
        );
        assert!(
            decode_change_projected(&encoded[..end], &projection).is_err(),
            "projected decoder accepted truncation at byte {end}"
        );
    }

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_change(&trailing).is_err());
    assert!(decode_change_projected(&trailing, &projection).is_err());
    let mut repeated_eos = encoded;
    repeated_eos.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
    assert!(decode_change(&repeated_eos).is_err());
    assert!(decode_change_projected(&repeated_eos, &projection).is_err());

    let encoded = encode_change(&change).unwrap();
    let legacy_schema_framing = encoded[4..].to_vec();
    assert!(decode_change(&legacy_schema_framing).is_err());
    assert!(decode_change_projected(&legacy_schema_framing, &projection).is_err());
    let arrow_reader = StreamReader::try_new(Cursor::new(encoded.as_slice()), None).unwrap();
    let batch_offset = usize::try_from(arrow_reader.get_ref().position()).unwrap();
    let mut legacy_batch_framing = encoded;
    legacy_batch_framing.drain(batch_offset..batch_offset + 4);
    assert!(decode_change(&legacy_batch_framing).is_err());
    assert!(decode_change_projected(&legacy_batch_framing, &projection).is_err());

    let schema = unit_physical_schema(marked_metadata());
    let unit_projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let empty_stream = encode_stream(&schema, &[]);
    assert!(decode_change(&empty_stream).is_err());
    assert!(decode_change_projected(&empty_stream, &unit_projection).is_err());
    let first = unit_physical_batch(schema.clone(), Int64Array::from(vec![1]));
    let second = unit_physical_batch(schema.clone(), Int64Array::from(vec![-1]));
    let multiple = encode_stream(&schema, &[first, second]);
    assert!(decode_change(&multiple).is_err());
    assert!(decode_change_projected(&multiple, &unit_projection).is_err());
}

#[test]
fn decoder_preflights_declared_lengths_before_arrow_allocates() {
    let change = simple_change([1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    let mut oversized_metadata = encoded.clone();
    let oversized_aligned_length = i32::MAX - 7;
    oversized_metadata[4..8].copy_from_slice(&oversized_aligned_length.to_le_bytes());
    assert!(matches!(
        decode_change(&oversized_metadata),
        Err(CodecError::InvalidEncoding { .. })
    ));
    assert!(matches!(
        decode_change_projected(&oversized_metadata, &projection),
        Err(CodecError::InvalidEncoding { .. })
    ));

    let oversized_aligned_body = i64::MAX - 7;
    let metadata = ipc_batch_metadata(oversized_aligned_body, false);
    let oversized_body = replace_batch_message(&encoded, &metadata, &[]);
    assert!(matches!(
        decode_change(&oversized_body),
        Err(CodecError::InvalidEncoding { .. })
    ));
    assert!(matches!(
        decode_change_projected(&oversized_body, &projection),
        Err(CodecError::InvalidEncoding { .. })
    ));
}

#[test]
fn decoder_rejects_non_v5_big_endian_and_compressed_ipc() {
    let schema = unit_physical_schema(marked_metadata());
    let unit_projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let batch = unit_physical_batch(schema.clone(), Int64Array::from(vec![1]));
    let v4_options = IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap();
    let v4 = encode_stream_with_options(&schema, &[batch], v4_options);
    assert!(decode_change(&v4).is_err());
    assert!(decode_change_projected(&v4, &unit_projection).is_err());

    let big_endian = big_endian_schema_stream();
    assert!(decode_change(&big_endian).is_err());
    assert!(decode_change_projected(&big_endian, &unit_projection).is_err());

    let change = simple_change([1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    let compressed_metadata = ipc_batch_metadata(0, true);
    let compressed = replace_batch_message(&encoded, &compressed_metadata, &[]);
    assert!(decode_change(&compressed).is_err());
    assert!(decode_change_projected(&compressed, &projection).is_err());
}

#[test]
fn decoder_rechecks_representable_change_invariants_after_arrow_decoding() {
    let schema = unit_physical_schema(marked_metadata());
    let zero = unit_physical_batch(schema.clone(), Int64Array::from(vec![0]));
    assert!(matches!(
        decode_change(&encode_stream(&schema, &[zero])),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));

    let empty = unit_physical_batch(schema.clone(), Int64Array::from(Vec::<i64>::new()));
    assert!(decode_change(&encode_stream(&schema, &[empty])).is_err());
}

#[test]
fn unit_relation_change_has_stable_arrow_stream_bytes() {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let records = RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
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
    let decoded = decode_change(&encoded).unwrap();
    assert_eq!(decoded.records().num_columns(), 0);
    assert_eq!(decoded.records().num_rows(), 1);
    assert_eq!(decoded.diffs().values(), &[-1]);
}
