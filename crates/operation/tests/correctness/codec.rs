use std::{
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
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ExtendDefinition, FilterDefinition, ProjectDefinition},
    },
    try_cast,
};
use dogpaddle_store::Store;

use super::support::{TestStore, decode_hex};

const COUNT_V1: &str = include_str!("../fixtures/v1/count_definition.hex");
const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");
const EXTEND_V1: &str = include_str!("../fixtures/v1/extend_is_seven.hex");
const FILTER_V1: &str = include_str!("../fixtures/v1/filter_complex_expression.hex");
const PROJECT_V1: &str = include_str!("../fixtures/v1/project_fields_0_2.hex");
const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_source_start_42.hex");
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
        (decode_hex(COUNT_V1), Box::new(CountDefinition::new())),
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
fn every_decoded_builtin_golden_reconstructs_its_schema_binding() {
    let sequence = decode_definition(&decode_hex(SEQUENCE_V1)).unwrap();
    assert_eq!(
        sequence.bind(&[]).unwrap().output_schema(),
        Some(&value_schema())
    );

    let arbitrary = arbitrary_schema();
    let count = decode_definition(&decode_hex(COUNT_V1)).unwrap();
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

    let discard = decode_definition(&decode_hex(DISCARD_V1)).unwrap();
    assert!(
        discard
            .bind(std::slice::from_ref(&arbitrary))
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn expression_goldens_reconstruct_their_persisted_runtime_semantics() {
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
    let count = decode_hex(COUNT_V1);
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
