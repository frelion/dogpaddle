use std::{
    collections::HashMap,
    num::NonZeroU32,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_proto::bytes::Serializeable;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, DefinitionCodecError, Expr, OperationDefinition, Operator, ScalarValue, cast,
    col, decode_definition, encode_definition, lit,
    operation::{
        Action, OperationInput,
        sink::{DiscardDefinition, SqliteSinkDefinition},
        source::SequenceSourceDefinition,
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, RunningEventCountDefinition,
            SchemaAlignDefinition, SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
    try_cast,
};
use dogpaddle_store::Store;

use super::support::{TestStore, decode_hex};

const RUNNING_EVENT_COUNT_V1: &str =
    include_str!("../fixtures/v1/running_event_count_definition.hex");
const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");
const EXTEND_V1: &str = include_str!("../fixtures/v1/extend_is_seven.hex");
const FILTER_V1: &str = include_str!("../fixtures/v1/filter_complex_expression.hex");
const PROJECT_V1: &str = include_str!("../fixtures/v1/project_fields_0_2.hex");
const SCHEMA_ALIGN_V1: &str = include_str!("../fixtures/v1/schema_align_explicit.hex");
const SELECT_V1: &str = include_str!("../fixtures/v1/select_named_expressions.hex");
const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_source_start_42.hex");
const SQLITE_SINK_V1: &str = include_str!("../fixtures/v1/sqlite_sink_output_events.hex");
const UNION_ALL_V1: &str = include_str!("../fixtures/v1/union_all_two_inputs.hex");
const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

fn length_prefixed_bytes(encoded: &[u8], length_offset: usize) -> &[u8] {
    let length = usize::try_from(u32::from_be_bytes(
        encoded[length_offset..length_offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let value_offset = length_offset + size_of::<u32>();
    assert_eq!(value_offset + length, encoded.len());
    &encoded[value_offset..]
}

fn skip_length_prefixed(encoded: &[u8], offset: &mut usize) {
    let length = usize::try_from(u32::from_be_bytes(
        encoded[*offset..*offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    *offset += size_of::<u32>() + length;
}

fn skip_metadata(encoded: &[u8], offset: &mut usize) {
    let count = u32::from_be_bytes(
        encoded[*offset..*offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    *offset += size_of::<u32>();
    for _ in 0..count {
        skip_length_prefixed(encoded, offset);
        skip_length_prefixed(encoded, offset);
    }
}

fn binary(operator: Operator, left: Expr, right: Expr) -> Expr {
    match operator {
        Operator::Eq => left.eq(right),
        Operator::NotEq => left.not_eq(right),
        Operator::And => left.and(right),
        Operator::Or => left.or(right),
        _ => panic!("test helper does not support {operator}"),
    }
}

fn filter(predicate: Expr) -> FilterDefinition {
    FilterDefinition::try_new(predicate).unwrap()
}

fn extend(field_name: &str, expression: Expr) -> ExtendDefinition {
    ExtendDefinition::try_new(field_name, expression).unwrap()
}

fn select() -> SelectDefinition {
    SelectDefinition::try_new([
        ("renamed", col("value")),
        ("next", col("value") + lit(1_u64)),
    ])
    .unwrap()
}

fn schema_align() -> SchemaAlignDefinition {
    SchemaAlignDefinition::try_new_with_metadata(
        [
            SchemaAlignField::try_new_with_metadata(
                "renamed",
                col("value"),
                true,
                HashMap::from([
                    ("z".to_owned(), "last".to_owned()),
                    ("a".to_owned(), "first".to_owned()),
                ]),
            )
            .unwrap(),
            SchemaAlignField::try_new("signed", cast(col("value"), DataType::Int64), false)
                .unwrap(),
        ],
        HashMap::from([
            ("version".to_owned(), "1".to_owned()),
            ("owner".to_owned(), "test".to_owned()),
        ]),
    )
    .unwrap()
}

fn complex_predicate() -> Expr {
    let uint_match = binary(Operator::Eq, col("value"), lit(7_u64));
    let signed_null = binary(Operator::Eq, lit(-2_i64), lit(ScalarValue::Int64(None))).is_null();
    let utf8_null = binary(Operator::NotEq, lit("x"), lit(ScalarValue::Utf8(None))).is_null();
    let boolean_null = binary(Operator::Eq, lit(true), lit(ScalarValue::Boolean(None))).is_null();
    let nullable_or = binary(Operator::Or, lit(ScalarValue::Boolean(None)), lit(false)).is_null();
    let known_true = !lit(false);
    let uint_null = lit(ScalarValue::UInt64(None)).is_null();
    [
        signed_null,
        utf8_null,
        boolean_null,
        nullable_or,
        known_true,
        uint_null,
    ]
    .into_iter()
    .fold(uint_match, |left, right| binary(Operator::And, left, right))
}

fn golden_cases() -> Vec<(Vec<u8>, Box<dyn OperationDefinition>)> {
    vec![
        (
            decode_hex(RUNNING_EVENT_COUNT_V1),
            Box::new(RunningEventCountDefinition::new()),
        ),
        (decode_hex(DISCARD_V1), Box::new(DiscardDefinition::new())),
        (
            decode_hex(EXTEND_V1),
            Box::new(extend(
                "is_seven",
                binary(Operator::Eq, col("value"), lit(7_u64)),
            )),
        ),
        (decode_hex(FILTER_V1), Box::new(filter(complex_predicate()))),
        (
            decode_hex(PROJECT_V1),
            Box::new(ProjectDefinition::new([0, 2])),
        ),
        (
            decode_hex(SEQUENCE_V1),
            Box::new(SequenceSourceDefinition::new(42)),
        ),
        (decode_hex(SELECT_V1), Box::new(select())),
        (decode_hex(SCHEMA_ALIGN_V1), Box::new(schema_align())),
        (
            decode_hex(UNION_ALL_V1),
            Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        ),
        (
            decode_hex(SQLITE_SINK_V1),
            Box::new(
                SqliteSinkDefinition::try_new("/var/lib/dogpaddle/output.sqlite", "events")
                    .unwrap(),
            ),
        ),
    ]
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn arbitrary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ]))
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[test]
fn every_builtin_definition_has_stable_v1_golden_bytes() {
    for (golden, definition) in golden_cases() {
        assert_eq!(encode_definition(definition.as_ref()), golden);
        let decoded = decode_definition(&golden).unwrap();
        assert_eq!(decoded.kind(), definition.kind());
        assert_eq!(encode_definition(decoded.as_ref()), golden);
    }
}

#[test]
fn sqlite_sink_payload_is_two_big_endian_length_prefixed_utf8_strings() {
    let encoded = encode_definition(
        &SqliteSinkDefinition::try_new("/var/lib/dogpaddle/output.sqlite", "events").unwrap(),
    );
    let path = b"/var/lib/dogpaddle/output.sqlite";
    let table = b"events";
    let path_length_offset = DEFINITION_HEADER_LEN;
    let path_offset = path_length_offset + size_of::<u32>();
    let table_length_offset = path_offset + path.len();
    let table_offset = table_length_offset + size_of::<u32>();

    assert_eq!(
        &encoded[path_length_offset..path_offset],
        &u32::try_from(path.len()).unwrap().to_be_bytes()
    );
    assert_eq!(&encoded[path_offset..table_length_offset], path);
    assert_eq!(
        &encoded[table_length_offset..table_offset],
        &u32::try_from(table.len()).unwrap().to_be_bytes()
    );
    assert_eq!(&encoded[table_offset..], table);
}

#[test]
fn every_decoded_builtin_golden_reconstructs_its_schema_binding() {
    let sequence = decode_definition(&decode_hex(SEQUENCE_V1)).unwrap();
    assert_eq!(
        sequence.bind(&[]).unwrap().output_schema(),
        Some(&value_schema())
    );

    let arbitrary = arbitrary_schema();
    let count = decode_definition(&decode_hex(RUNNING_EVENT_COUNT_V1)).unwrap();
    assert_eq!(
        count
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema(),
        Some(&count_schema())
    );

    let project = decode_definition(&decode_hex(PROJECT_V1)).unwrap();
    let expected = Arc::new(arbitrary.project(&[0, 2]).unwrap());
    assert_eq!(
        project
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    let filter = decode_definition(&decode_hex(FILTER_V1)).unwrap();
    assert_eq!(
        filter
            .bind(std::slice::from_ref(&value_schema()))
            .unwrap()
            .output_schema(),
        Some(&value_schema())
    );

    let extend = decode_definition(&decode_hex(EXTEND_V1)).unwrap();
    let expected = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("is_seven", DataType::Boolean, false),
    ]));
    assert_eq!(
        extend
            .bind(std::slice::from_ref(&value_schema()))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    let select = decode_definition(&decode_hex(SELECT_V1)).unwrap();
    let expected = Arc::new(Schema::new(vec![
        Field::new("renamed", DataType::UInt64, false),
        Field::new("next", DataType::UInt64, false),
    ]));
    assert_eq!(
        select
            .bind(std::slice::from_ref(&value_schema()))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    let schema_align = decode_definition(&decode_hex(SCHEMA_ALIGN_V1)).unwrap();
    let expected = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("renamed", DataType::UInt64, true).with_metadata(HashMap::from([
                ("a".to_owned(), "first".to_owned()),
                ("z".to_owned(), "last".to_owned()),
            ])),
            Field::new("signed", DataType::Int64, false),
        ],
        HashMap::from([
            ("owner".to_owned(), "test".to_owned()),
            ("version".to_owned(), "1".to_owned()),
        ]),
    ));
    assert_eq!(
        schema_align
            .bind(std::slice::from_ref(&value_schema()))
            .unwrap()
            .output_schema(),
        Some(&expected)
    );

    let union_all = decode_definition(&decode_hex(UNION_ALL_V1)).unwrap();
    let union_inputs = [value_schema(), value_schema()];
    assert_eq!(
        union_all.bind(&union_inputs).unwrap().output_schema(),
        Some(&value_schema())
    );

    let discard = decode_definition(&decode_hex(DISCARD_V1)).unwrap();
    assert!(
        discard
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema()
            .is_none()
    );

    let sqlite = decode_definition(&decode_hex(SQLITE_SINK_V1)).unwrap();
    assert!(
        sqlite
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn decoded_filter_and_extend_goldens_reconstruct_their_runtime_semantics() {
    let schema = value_schema();
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let filter = decode_definition(&decode_hex(FILTER_V1)).unwrap();
    let mut data = DataInstances::new();
    let filter = filter
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    let Action::Complete(Some(filtered)) = filter
        .turn(
            Some(OperationInput {
                port: 0,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("decoded complex Filter did not emit its expected rows");
    };
    let values = filtered
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.values(), &[7, 7]);
    assert_eq!(filtered.diffs().values(), &[1, 2]);

    let extend = decode_definition(&decode_hex(EXTEND_V1)).unwrap();
    let mut data = DataInstances::new();
    let extend = extend
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    let Action::Complete(Some(extended)) = extend
        .turn(
            Some(OperationInput {
                port: 0,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("decoded Extend did not append its expected field");
    };
    assert_eq!(extended.schema().field(1).name(), "is_seven");
    assert!(!extended.schema().field(1).is_nullable());
    let values = extended
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true)]
    );
}

#[test]
fn decoded_select_and_union_goldens_reconstruct_their_runtime_semantics() {
    let schema = value_schema();
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let select = decode_definition(&decode_hex(SELECT_V1)).unwrap();
    let mut data = DataInstances::new();
    let select = select
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    let Action::Complete(Some(selected)) = select
        .turn(
            Some(OperationInput {
                port: 0,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("decoded Select did not emit its expected fields");
    };
    assert_eq!(selected.schema().field(0).name(), "renamed");
    assert_eq!(selected.schema().field(1).name(), "next");
    let renamed = selected
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let next = selected
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(renamed.values(), &[7, 8, 7]);
    assert_eq!(next.values(), &[8, 9, 8]);
    assert_eq!(selected.diffs().values(), &[1, -1, 2]);

    let union_all = decode_definition(&decode_hex(UNION_ALL_V1)).unwrap();
    let mut data = DataInstances::new();
    let union_inputs = [Arc::clone(&schema), Arc::clone(&schema)];
    let union_all = union_all
        .bind(&union_inputs)
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    let Action::Complete(Some(forwarded)) = union_all
        .turn(
            Some(OperationInput {
                port: 1,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("decoded UnionAll did not forward its input Change");
    };
    assert!(Arc::ptr_eq(
        input.records().column(0),
        forwarded.records().column(0)
    ));
    assert_eq!(
        input.diffs().values().as_ptr(),
        forwarded.diffs().values().as_ptr()
    );
}

#[test]
fn decoded_schema_align_golden_reconstructs_its_runtime_semantics() {
    let schema = value_schema();
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();

    let definition = decode_definition(&decode_hex(SCHEMA_ALIGN_V1)).unwrap();
    let mut data = DataInstances::new();
    let operation = definition
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();
    let Action::Complete(Some(aligned)) = operation
        .turn(
            Some(OperationInput {
                port: 0,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("decoded SchemaAlign did not emit its expected fields");
    };
    assert_eq!(aligned.schema().metadata().get("owner").unwrap(), "test");
    assert_eq!(aligned.schema().field(0).name(), "renamed");
    assert!(aligned.schema().field(0).is_nullable());
    assert_eq!(
        aligned.schema().field(0).metadata().get("a").unwrap(),
        "first"
    );
    assert!(Arc::ptr_eq(
        aligned.records().column(0),
        input.records().column(0)
    ));
    let signed = aligned
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(signed.values(), &[7, 8, 7]);
    assert_eq!(
        aligned.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
}

#[test]
fn every_truncated_golden_prefix_is_rejected() {
    for (golden, _) in golden_cases() {
        for length in 0..golden.len() {
            assert_eq!(
                decode_definition(&golden[..length]).unwrap_err(),
                DefinitionCodecError::Truncated,
                "wrong error for prefix length {length} of {} bytes",
                golden.len(),
            );
        }
    }
}

#[test]
fn definition_decoder_rejects_non_canonical_or_unknown_input() {
    let count = decode_hex(RUNNING_EVENT_COUNT_V1);
    assert_eq!(
        decode_definition(b"short").unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_magic = count.clone();
    invalid_magic[0] ^= 0xff;
    assert_eq!(
        decode_definition(&invalid_magic).unwrap_err(),
        DefinitionCodecError::InvalidMagic
    );

    let version_offset = b"dogpaddle.operation\0".len();
    let mut unsupported = count.clone();
    unsupported[version_offset..version_offset + 2].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unsupported).unwrap_err(),
        DefinitionCodecError::UnsupportedVersion(2)
    );

    let mut unknown = count.clone();
    unknown[version_offset + 2..version_offset + 4].copy_from_slice(&99_u16.to_be_bytes());
    assert_eq!(
        decode_definition(&unknown).unwrap_err(),
        DefinitionCodecError::UnknownTag(99)
    );

    for (mut golden, _) in golden_cases() {
        golden.push(0);
        assert_eq!(
            decode_definition(&golden).unwrap_err(),
            DefinitionCodecError::TrailingBytes
        );
    }
}

#[test]
fn expression_payloads_are_length_prefixed_canonical_datafusion_protobuf() {
    let expressions = [
        col("value").eq(lit(7_u64)),
        !lit(false),
        col("value").is_null(),
        col("value").is_not_null(),
        lit(1_i64).lt(lit(2_i64)),
        lit(1_i64) + lit(2_i64),
        cast(lit(1_i64), DataType::Utf8),
        try_cast(lit("1"), DataType::Int64),
    ];

    for expression in expressions {
        let protobuf = expression.to_bytes().unwrap();
        let encoded = encode_definition(&filter(expression));
        assert_eq!(
            &encoded[DEFINITION_HEADER_LEN..DEFINITION_HEADER_LEN + size_of::<u32>()],
            &u32::try_from(protobuf.len()).unwrap().to_be_bytes(),
        );
        assert_eq!(
            length_prefixed_bytes(&encoded, DEFINITION_HEADER_LEN),
            protobuf.as_ref()
        );

        let decoded = decode_definition(&encoded).unwrap();
        assert_eq!(encode_definition(decoded.as_ref()), encoded);
    }

    let expression = col("value").eq(lit(7_u64));
    let protobuf = expression.to_bytes().unwrap();
    let encoded = encode_definition(&extend("is_seven", expression));
    let name_length = usize::try_from(u32::from_be_bytes(
        encoded[DEFINITION_HEADER_LEN..DEFINITION_HEADER_LEN + size_of::<u32>()]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let expression_length_offset = DEFINITION_HEADER_LEN + size_of::<u32>() + name_length;
    assert_eq!(
        &encoded[DEFINITION_HEADER_LEN + size_of::<u32>()..expression_length_offset],
        b"is_seven"
    );
    assert_eq!(
        length_prefixed_bytes(&encoded, expression_length_offset),
        protobuf.as_ref()
    );
}

#[test]
fn expression_decoder_rejects_bad_lengths_malformed_and_noncanonical_protobuf() {
    let canonical = encode_definition(&filter(lit(true)));
    let protobuf = length_prefixed_bytes(&canonical, DEFINITION_HEADER_LEN).to_vec();
    let wrap = |protobuf: &[u8]| {
        let mut encoded = canonical[..DEFINITION_HEADER_LEN].to_vec();
        encoded.extend_from_slice(&u32::try_from(protobuf.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(protobuf);
        encoded
    };

    let empty = wrap(&[]);
    assert!(matches!(
        decode_definition(&empty),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let malformed = wrap(&[u8::MAX]);
    assert!(matches!(
        decode_definition(&malformed),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut forged_length = canonical.clone();
    forged_length[DEFINITION_HEADER_LEN..DEFINITION_HEADER_LEN + size_of::<u32>()]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut trailing = canonical.clone();
    trailing.push(0);
    assert_eq!(
        decode_definition(&trailing).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );

    let mut protobuf_with_unknown_field = protobuf;
    // Unknown protobuf field 127 with a canonical zero varint value.
    protobuf_with_unknown_field.extend_from_slice(&[0xf8, 0x07, 0x00]);
    let noncanonical = wrap(&protobuf_with_unknown_field);
    assert!(matches!(
        decode_definition(&noncanonical),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut invalid_name = encode_definition(&extend("x", lit(1_u64)));
    invalid_name[DEFINITION_HEADER_LEN + size_of::<u32>()] = u8::MAX;
    assert!(matches!(
        decode_definition(&invalid_name),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn select_decoder_rejects_a_forged_count_and_invalid_field_name_without_panicking() {
    let canonical = encode_definition(&select());

    let mut forged_count = canonical[..DEFINITION_HEADER_LEN + size_of::<u32>()].to_vec();
    forged_count[DEFINITION_HEADER_LEN..].copy_from_slice(&u32::MAX.to_be_bytes());
    let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&forged_count)));
    assert!(
        result.is_ok(),
        "Select decoder panicked for a forged field count"
    );
    assert_eq!(
        result.unwrap().unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_utf8 = canonical.clone();
    let first_name_offset = DEFINITION_HEADER_LEN + size_of::<u32>() * 2;
    invalid_utf8[first_name_offset] = u8::MAX;
    assert!(matches!(
        decode_definition(&invalid_utf8),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn schema_align_encoding_canonicalizes_metadata_and_rejects_noncanonical_payloads() {
    let canonical = encode_definition(&schema_align());
    let reversed_input_order = SchemaAlignDefinition::try_new_with_metadata(
        [
            SchemaAlignField::try_new_with_metadata(
                "renamed",
                col("value"),
                true,
                [
                    ("z".to_owned(), "last".to_owned()),
                    ("a".to_owned(), "first".to_owned()),
                ],
            )
            .unwrap(),
            SchemaAlignField::try_new("signed", cast(col("value"), DataType::Int64), false)
                .unwrap(),
        ],
        [
            ("version".to_owned(), "1".to_owned()),
            ("owner".to_owned(), "test".to_owned()),
        ],
    )
    .unwrap();
    assert_eq!(encode_definition(&reversed_input_order), canonical);

    let mut offset = DEFINITION_HEADER_LEN;
    let field_count = u32::from_be_bytes(
        canonical[offset..offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    offset += size_of::<u32>();
    let mut first_nullable = None;
    for field in 0..field_count {
        skip_length_prefixed(&canonical, &mut offset);
        skip_length_prefixed(&canonical, &mut offset);
        if field == 0 {
            first_nullable = Some(offset);
        }
        offset += 1;
        skip_metadata(&canonical, &mut offset);
    }

    let mut invalid_nullability = canonical.clone();
    invalid_nullability[first_nullable.unwrap()] = 2;
    assert!(matches!(
        decode_definition(&invalid_nullability),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let schema_metadata_count_offset = offset;
    let metadata_count = u32::from_be_bytes(
        canonical[offset..offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    assert_eq!(metadata_count, 2);
    offset += size_of::<u32>();
    let first_start = offset;
    skip_length_prefixed(&canonical, &mut offset);
    skip_length_prefixed(&canonical, &mut offset);
    let first_end = offset;
    let second_start = offset;
    skip_length_prefixed(&canonical, &mut offset);
    skip_length_prefixed(&canonical, &mut offset);
    let second_end = offset;

    let mut unsorted = canonical[..schema_metadata_count_offset + size_of::<u32>()].to_vec();
    unsorted.extend_from_slice(&canonical[second_start..second_end]);
    unsorted.extend_from_slice(&canonical[first_start..first_end]);
    unsorted.extend_from_slice(&canonical[second_end..]);
    assert!(matches!(
        decode_definition(&unsorted),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn union_all_decoder_rejects_zero_input_count() {
    let canonical = encode_definition(&UnionAllDefinition::new(NonZeroU32::new(2).unwrap()));

    let mut zero = canonical.clone();
    zero[DEFINITION_HEADER_LEN..].copy_from_slice(&0_u32.to_be_bytes());
    assert!(matches!(
        decode_definition(&zero),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn sqlite_sink_decoder_rejects_invalid_lengths_strings_and_paths() {
    let canonical = decode_hex(SQLITE_SINK_V1);
    let path_length_offset = DEFINITION_HEADER_LEN;
    let path_length = usize::try_from(u32::from_be_bytes(
        canonical[path_length_offset..path_length_offset + size_of::<u32>()]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let path_offset = path_length_offset + size_of::<u32>();
    let table_length_offset = path_offset + path_length;
    let table_offset = table_length_offset + size_of::<u32>();

    let mut forged_path_length = canonical.clone();
    forged_path_length[path_length_offset..path_offset].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_path_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut forged_table_length = canonical.clone();
    forged_table_length[table_length_offset..table_offset].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_table_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    for invalid_utf8_offset in [path_offset, table_offset] {
        let mut invalid_utf8 = canonical.clone();
        invalid_utf8[invalid_utf8_offset] = u8::MAX;
        assert!(matches!(
            decode_definition(&invalid_utf8),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
    }

    let wrap = |path: &[u8], table: &[u8]| {
        let mut encoded = canonical[..DEFINITION_HEADER_LEN].to_vec();
        encoded.extend_from_slice(&u32::try_from(path.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(path);
        encoded.extend_from_slice(&u32::try_from(table.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(table);
        encoded
    };
    for invalid in [
        wrap(b"relative.sqlite", b"events"),
        wrap(b":memory:", b"events"),
        wrap(b"/tmp/invalid\0.sqlite", b"events"),
        wrap(b"/tmp/output.sqlite", b""),
        wrap(b"/tmp/output.sqlite", b"bad\0table"),
        wrap(b"/tmp/output.sqlite", b"SQLITE_reserved"),
    ] {
        assert!(matches!(
            decode_definition(&invalid),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
    }
}

#[test]
fn sqlite_sink_decoder_never_panics_for_valid_header_arbitrary_payloads() {
    let mut header = decode_hex(SQLITE_SINK_V1);
    header.truncate(DEFINITION_HEADER_LEN);
    let mut state = 0x3c6e_f372_fe94_f82b_u64;
    for length in 0..=256 {
        let mut input = header.clone();
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            input.push(state.to_le_bytes()[0]);
        }
        let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&input)));
        assert!(
            result.is_ok(),
            "SQLiteSink decoder panicked for payload length {length}"
        );
    }
}

#[test]
fn definition_decoder_never_panics_for_deterministic_arbitrary_bytes() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for length in 0..=256 {
        let mut input = vec![0_u8; length];
        for byte in &mut input {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&input)));
        assert!(result.is_ok(), "decoder panicked for input length {length}");
    }
}

#[test]
fn expression_decoder_never_panics_for_valid_header_arbitrary_payloads() {
    let payload_offset = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;
    let mut header = encode_definition(&filter(lit(true)));
    header.truncate(payload_offset);
    let mut state = 0xbb67_ae85_84ca_a73b_u64;
    for length in 0..=256 {
        let mut input = header.clone();
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            input.push(state.to_le_bytes()[0]);
        }
        let result = catch_unwind(AssertUnwindSafe(|| decode_definition(&input)));
        assert!(
            result.is_ok(),
            "expression decoder panicked for payload length {length}"
        );
    }
}
