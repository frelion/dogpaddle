use std::{
    collections::HashMap,
    io::Cursor,
    ops::Range,
    process::{Command, exit},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt64Array, types::Int64Type,
};
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, Buffer as IpcBuffer, DictionaryEncoding,
    DictionaryEncodingArgs, Endianness, Field as IpcField, FieldArgs, FieldNode,
    FloatingPoint as IpcFloatingPoint, FloatingPointArgs, Int as IpcInt, IntArgs,
    LargeUtf8 as IpcLargeUtf8, LargeUtf8Args, List as IpcList, ListArgs, Message as IpcMessage,
    MessageArgs, MessageHeader, MetadataVersion, Null as IpcNull, NullArgs, Precision,
    RecordBatch as IpcRecordBatch, RecordBatchArgs, Schema as IpcSchema, SchemaArgs,
    Type as IpcType,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use flatbuffers::FlatBufferBuilder;

use super::{
    CodecError, batch::BatchLayout, decode_change, decode_change_projected, encode_change, stream,
};
use crate::{Change, ChangeError, ChangeProjection, ProjectionError};

const KIND_KEY: &str = "dogpaddle.kind";
const VERSION_KEY: &str = "dogpaddle.change.version";
const OFFSETS_BUFFER: usize = 1;
const VARIABLE_VALUES_BUFFER: usize = 2;
const MALFORMED_SCHEMA_PANIC_PROBE: &str = "DOGPADDLE_CHANGE_MALFORMED_SCHEMA_PANIC_PROBE";
const PANIC_HOOK_EXIT_CODE: i32 = 86;
const DECODE_PANIC_MESSAGE: &str = "Arrow IPC decoding panicked";
const PANIC_PROBE_COMPLETED: &str = "dogpaddle-change malformed decoder probe completed";

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

fn layout_change() -> Change {
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

fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
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

#[derive(Clone, Copy, Debug)]
enum MalformedSchemaCase {
    Dictionary,
    FloatMissingTable,
    HalfFloat,
    IntMissingTable,
    IntWidth,
    ListMissingChildren,
    ListTwoChildren,
    NestingTooDeep,
    UnsupportedType,
}

const MALFORMED_SCHEMA_CASES: &[(MalformedSchemaCase, &str)] = &[
    (MalformedSchemaCase::Dictionary, "dictionary encoding"),
    (
        MalformedSchemaCase::FloatMissingTable,
        "Exactly one of union discriminant",
    ),
    (MalformedSchemaCase::HalfFloat, "unsupported precision"),
    (
        MalformedSchemaCase::IntMissingTable,
        "Exactly one of union discriminant",
    ),
    (MalformedSchemaCase::IntWidth, "unsupported bit width"),
    (
        MalformedSchemaCase::ListMissingChildren,
        "has no children vector",
    ),
    (
        MalformedSchemaCase::ListTwoChildren,
        "must have exactly one child",
    ),
    (
        MalformedSchemaCase::NestingTooDeep,
        "Nested table depth limit reached",
    ),
    (
        MalformedSchemaCase::UnsupportedType,
        "unsupported Arrow IPC type",
    ),
];

#[expect(
    clippy::too_many_lines,
    reason = "one table-driven FlatBuffer fixture keeps every malformed schema shape comparable"
)]
fn malformed_schema_stream(case: MalformedSchemaCase) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let name = builder.create_string("malformed");
    let field = if matches!(case, MalformedSchemaCase::NestingTooDeep) {
        let leaf_name = builder.create_string("leaf");
        let null_type = IpcNull::create(&mut builder, &NullArgs::default());
        let mut current = IpcField::create(
            &mut builder,
            &FieldArgs {
                name: Some(leaf_name),
                type_type: IpcType::Null,
                type_: Some(null_type.as_union_value()),
                ..FieldArgs::default()
            },
        );
        for _ in 0..=crate::MAX_NESTING_DEPTH {
            let children = builder.create_vector(&[current]);
            let data_type = IpcList::create(&mut builder, &ListArgs::default());
            current = IpcField::create(
                &mut builder,
                &FieldArgs {
                    name: Some(name),
                    type_type: IpcType::List,
                    type_: Some(data_type.as_union_value()),
                    children: Some(children),
                    ..FieldArgs::default()
                },
            );
        }
        current
    } else {
        let (type_type, type_, children, dictionary) = match case {
            MalformedSchemaCase::Dictionary => {
                let data_type = IpcNull::create(&mut builder, &NullArgs::default());
                let dictionary =
                    DictionaryEncoding::create(&mut builder, &DictionaryEncodingArgs::default());
                (
                    IpcType::Null,
                    Some(data_type.as_union_value()),
                    None,
                    Some(dictionary),
                )
            }
            MalformedSchemaCase::FloatMissingTable => (IpcType::FloatingPoint, None, None, None),
            MalformedSchemaCase::HalfFloat => {
                let data_type = IpcFloatingPoint::create(
                    &mut builder,
                    &FloatingPointArgs {
                        precision: Precision::HALF,
                    },
                );
                (
                    IpcType::FloatingPoint,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::IntMissingTable => (IpcType::Int, None, None, None),
            MalformedSchemaCase::IntWidth => {
                let data_type = IpcInt::create(
                    &mut builder,
                    &IntArgs {
                        bitWidth: 24,
                        is_signed: true,
                    },
                );
                (IpcType::Int, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::ListMissingChildren => {
                let data_type = IpcList::create(&mut builder, &ListArgs::default());
                (IpcType::List, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::ListTwoChildren => {
                let child_name = builder.create_string("child");
                let children = [0, 1].map(|_| {
                    let data_type = IpcNull::create(&mut builder, &NullArgs::default());
                    IpcField::create(
                        &mut builder,
                        &FieldArgs {
                            name: Some(child_name),
                            type_type: IpcType::Null,
                            type_: Some(data_type.as_union_value()),
                            ..FieldArgs::default()
                        },
                    )
                });
                let children = builder.create_vector(&children);
                let data_type = IpcList::create(&mut builder, &ListArgs::default());
                (
                    IpcType::List,
                    Some(data_type.as_union_value()),
                    Some(children),
                    None,
                )
            }
            MalformedSchemaCase::UnsupportedType => {
                let data_type = IpcLargeUtf8::create(&mut builder, &LargeUtf8Args::default());
                (
                    IpcType::LargeUtf8,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::NestingTooDeep => unreachable!("handled above"),
        };
        IpcField::create(
            &mut builder,
            &FieldArgs {
                name: Some(name),
                type_type,
                type_,
                children,
                dictionary,
                ..FieldArgs::default()
            },
        )
    };
    let fields = builder.create_vector(&[field]);
    let schema = IpcSchema::create(
        &mut builder,
        &SchemaArgs {
            endianness: Endianness::Little,
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

fn assert_both_invalid_encoding(encoded: &[u8], projection: &ChangeProjection) {
    assert_invalid_encoding_without_decoder_panic(decode_change(encoded));
    assert_invalid_encoding_without_decoder_panic(decode_change_projected(encoded, projection));
}

fn assert_arrow_error(result: &Result<Change, CodecError>) {
    assert!(
        matches!(result, Err(CodecError::Arrow(_))),
        "expected Arrow, found {result:?}"
    );
}

fn assert_invalid_encoding_without_decoder_panic(result: Result<Change, CodecError>) {
    match result {
        Err(CodecError::InvalidEncoding { message }) => {
            assert_ne!(message, DECODE_PANIC_MESSAGE, "decoder panic was caught");
        }
        other => panic!("expected InvalidEncoding, found {other:?}"),
    }
}

#[test]
fn decoder_rejects_malformed_schema_without_invoking_the_panic_hook() {
    // Isolate the process-wide hook from other tests that may run concurrently.
    if std::env::var_os(MALFORMED_SCHEMA_PANIC_PROBE).is_some() {
        let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| exit(PANIC_HOOK_EXIT_CODE)));

        for &(case, expected) in MALFORMED_SCHEMA_CASES {
            let encoded = malformed_schema_stream(case);
            let Err(CodecError::InvalidEncoding { message }) = stream::parse(&encoded) else {
                panic!("{case:?} did not return InvalidEncoding");
            };
            assert!(
                message.contains(expected),
                "{case:?} returned {message:?}, expected a diagnostic containing {expected:?}"
            );
            assert_ne!(message, DECODE_PANIC_MESSAGE, "decoder panic was caught");
            assert_invalid_encoding_without_decoder_panic(decode_change(&encoded));
            assert_invalid_encoding_without_decoder_panic(decode_change_projected(
                &encoded,
                &projection,
            ));
        }

        std::panic::set_hook(previous_hook);
        println!("{PANIC_PROBE_COMPLETED}");
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "codec::tests::decoder_rejects_malformed_schema_without_invoking_the_panic_hook",
            "--nocapture",
        ])
        .env(MALFORMED_SCHEMA_PANIC_PROBE, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "malformed Schema decoder probe exited with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(PANIC_PROBE_COMPLETED),
        "malformed Schema decoder probe did not execute the child test\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
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
        assert_both_invalid_encoding(&encode_stream(&schema, &[batch]), &projection);
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
    assert!(matches!(
        decode_change_projected(&encoded, &projection),
        Err(CodecError::UnsupportedVersion { version }) if version == "2"
    ));

    let mut unknown = marked_metadata();
    unknown.insert("dogpaddle.unknown".to_owned(), "value".to_owned());
    let schema = unit_physical_schema(unknown);
    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    assert_both_invalid_encoding(&encode_stream(&schema, &[batch]), &projection);
}

#[test]
fn decoder_rejects_incomplete_noncanonical_or_unsupported_streams() {
    let change = simple_change(&[-1, 1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    for end in 0..encoded.len() {
        assert_both_invalid_encoding(&encoded[..end], &projection);
    }

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_both_invalid_encoding(&trailing, &projection);
    assert_both_invalid_encoding(&encoded[4..], &projection);

    let reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let batch_offset = usize::try_from(reader.get_ref().position()).unwrap();
    let mut legacy_batch = encoded.clone();
    legacy_batch.drain(batch_offset..batch_offset + 4);
    assert_both_invalid_encoding(&legacy_batch, &projection);

    let schema = unit_physical_schema(marked_metadata());
    let unit_projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert_both_invalid_encoding(&encode_stream(&schema, &[]), &unit_projection);
    let batches = [
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1])),
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![-1])),
    ];
    assert_both_invalid_encoding(&encode_stream(&schema, &batches), &unit_projection);

    let mut oversized_metadata = encoded.clone();
    oversized_metadata[4..8].copy_from_slice(&(i32::MAX - 7).to_le_bytes());
    assert_both_invalid_encoding(&oversized_metadata, &projection);
    let metadata = ipc_batch_metadata(1, None, None, i64::MAX - 7, false);
    let oversized_body = replace_batch_message(&encoded, &metadata, &[]);
    assert_both_invalid_encoding(&oversized_body, &projection);

    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap();
    assert_both_invalid_encoding(
        &encode_stream_with_options(&schema, &[batch], options),
        &unit_projection,
    );
    assert_both_invalid_encoding(&big_endian_schema_stream(), &unit_projection);
    let compressed =
        replace_batch_message(&encoded, &ipc_batch_metadata(1, None, None, 0, true), &[]);
    assert_both_invalid_encoding(&compressed, &projection);
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
    assert_both_invalid_encoding(&encode_stream(&schema, &[empty]), &projection);
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
    let change = layout_change();
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
    assert_change_eq(
        &decode_change_projected(&encoded, &keep_id).unwrap(),
        &change.try_project(&keep_id).unwrap(),
    );
    let label = change.schema().index_of("label").unwrap();
    let select_label = ChangeProjection::try_new(change.schema(), [label]).unwrap();
    assert_arrow_error(&decode_change_projected(&encoded, &select_label));
    assert_arrow_error(&decode_change(&encoded));
}

#[test]
fn projected_decode_skips_only_unselected_list_offset_validation() {
    let change = layout_change();
    let mut encoded = encode_change(&change).unwrap();
    let offsets = field_buffer_range(&encoded, "items", OFFSETS_BUFFER);
    let last = offsets.start + change.num_rows() * size_of::<i32>();
    encoded[last..last + size_of::<i32>()].copy_from_slice(&i32::MAX.to_le_bytes());

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &keep_id).unwrap(),
        &change.try_project(&keep_id).unwrap(),
    );
    let items = change.schema().index_of("items").unwrap();
    let select_items = ChangeProjection::try_new(change.schema(), [items]).unwrap();
    assert_arrow_error(&decode_change_projected(&encoded, &select_items));
    assert_arrow_error(&decode_change(&encoded));
}

#[test]
fn both_decoders_validate_all_unselected_batch_metadata() {
    let change = layout_change();
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
        assert_both_invalid_encoding(&malformed, &projection);
    }
}

#[test]
fn batch_layout_rejects_missing_extra_negative_and_noncanonical_descriptors() {
    let change = simple_change(&[1, -1]);
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), []).unwrap();
    let (parsed, layout) = parsed_layout(&encoded);
    let row_count = parsed.batch.length();
    let body = parsed.body.to_vec();
    let nodes = layout.nodes;
    let buffers = parsed
        .batch
        .buffers()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();

    let replace = |nodes: Option<&[FieldNode]>, buffers: Option<&[IpcBuffer]>| {
        let metadata = ipc_batch_metadata(
            row_count,
            nodes,
            buffers,
            i64::try_from(body.len()).unwrap(),
            false,
        );
        replace_batch_message(&encoded, &metadata, &body)
    };

    let mut extra_nodes = nodes.clone();
    extra_nodes.push(FieldNode::new(0, 0));
    let mut extra_buffers = buffers.clone();
    extra_buffers.push(IpcBuffer::new(i64::try_from(body.len()).unwrap(), 0));

    let mut negative_length_node = nodes.clone();
    negative_length_node[0] = FieldNode::new(-1, 0);
    let mut negative_null_count = nodes.clone();
    negative_null_count[0] = FieldNode::new(row_count, -1);
    let mut excessive_null_count = nodes.clone();
    excessive_null_count[0] = FieldNode::new(row_count, row_count + 1);

    let mut negative_offset = buffers.clone();
    negative_offset[0] = IpcBuffer::new(-1, negative_offset[0].length());
    let mut negative_buffer_length = buffers.clone();
    negative_buffer_length[0] = IpcBuffer::new(negative_buffer_length[0].offset(), -1);
    let mut gap = buffers.clone();
    gap[1] = IpcBuffer::new(gap[1].offset() + 8, gap[1].length());
    let mut overlap = buffers.clone();
    overlap[1] = IpcBuffer::new(0, overlap[1].length());

    let malformed = [
        replace(None, Some(&buffers)),
        replace(Some(&nodes), None),
        replace(Some(&extra_nodes), Some(&buffers)),
        replace(Some(&nodes), Some(&extra_buffers)),
        replace(Some(&negative_length_node), Some(&buffers)),
        replace(Some(&negative_null_count), Some(&buffers)),
        replace(Some(&excessive_null_count), Some(&buffers)),
        replace(Some(&nodes), Some(&negative_offset)),
        replace(Some(&nodes), Some(&negative_buffer_length)),
        replace(Some(&nodes), Some(&gap)),
        replace(Some(&nodes), Some(&overlap)),
    ];
    for encoded in malformed {
        assert_both_invalid_encoding(&encoded, &projection);
    }
}
