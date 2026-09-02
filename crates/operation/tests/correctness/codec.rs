use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    BinaryOperator, DataInstances, DefinitionCodecError, Expression, Literal, OperationDefinition,
    UnaryOperator, decode_definition, encode_definition,
    operation::{
        Action, OperationInput,
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ExtendDefinition, FilterDefinition, ProjectDefinition},
    },
};
use dogpaddle_store::Store;

use super::support::{TestStore, decode_hex};

const COUNT_V1: &str = include_str!("../fixtures/v1/count_definition.hex");
const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");
const EXTEND_V1: &str = include_str!("../fixtures/v1/extend_is_seven.hex");
const FILTER_V1: &str = include_str!("../fixtures/v1/filter_complex_expression.hex");
const PROJECT_V1: &str = include_str!("../fixtures/v1/project_fields_0_2.hex");
const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_source_start_42.hex");

fn binary(operator: BinaryOperator, left: Expression, right: Expression) -> Expression {
    Expression::binary(operator, left, right)
}

fn complex_predicate() -> Expression {
    let uint_match = binary(
        BinaryOperator::Equal,
        Expression::column(0),
        Expression::literal(Literal::UInt64(Some(7))),
    );
    let signed_null = Expression::unary(
        UnaryOperator::IsNull,
        binary(
            BinaryOperator::Equal,
            Expression::literal(Literal::Int64(Some(-2))),
            Expression::literal(Literal::Int64(None)),
        ),
    );
    let utf8_null = Expression::unary(
        UnaryOperator::IsNull,
        binary(
            BinaryOperator::NotEqual,
            Expression::literal(Literal::Utf8(Some("x".to_owned()))),
            Expression::literal(Literal::Utf8(None)),
        ),
    );
    let boolean_null = Expression::unary(
        UnaryOperator::IsNull,
        binary(
            BinaryOperator::Equal,
            Expression::literal(Literal::Boolean(Some(true))),
            Expression::literal(Literal::Boolean(None)),
        ),
    );
    let nullable_or = Expression::unary(
        UnaryOperator::IsNull,
        binary(
            BinaryOperator::Or,
            Expression::literal(Literal::Boolean(None)),
            Expression::literal(Literal::Boolean(Some(false))),
        ),
    );
    let known_true = Expression::unary(
        UnaryOperator::Not,
        Expression::literal(Literal::Boolean(Some(false))),
    );
    let uint_null = Expression::unary(
        UnaryOperator::IsNull,
        Expression::literal(Literal::UInt64(None)),
    );
    [
        signed_null,
        utf8_null,
        boolean_null,
        nullable_or,
        known_true,
        uint_null,
    ]
    .into_iter()
    .fold(uint_match, |left, right| {
        binary(BinaryOperator::And, left, right)
    })
}

fn golden_cases() -> Vec<(Vec<u8>, Box<dyn OperationDefinition>)> {
    vec![
        (decode_hex(COUNT_V1), Box::new(CountDefinition::new())),
        (decode_hex(DISCARD_V1), Box::new(DiscardDefinition::new())),
        (
            decode_hex(EXTEND_V1),
            Box::new(ExtendDefinition::new(
                "is_seven",
                binary(
                    BinaryOperator::Equal,
                    Expression::column(0),
                    Expression::literal(Literal::UInt64(Some(7))),
                ),
            )),
        ),
        (
            decode_hex(FILTER_V1),
            Box::new(FilterDefinition::new(complex_predicate())),
        ),
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
fn deep_linear_expression_roundtrips_binds_and_executes_without_a_recursive_limit() {
    let mut expression = Expression::literal(Literal::Boolean(Some(true)));
    for _ in 0..2_048 {
        expression = Expression::unary(UnaryOperator::Not, expression);
    }
    let encoded = encode_definition(&FilterDefinition::new(expression));
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), encoded);

    let schema = value_schema();
    let mut data = DataInstances::new();
    let operation = decoded
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .materialize(&mut data)
        .unwrap();
    data.finish().unwrap();

    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let Action::Complete(Some(output)) = operation
        .turn(
            Some(OperationInput {
                port: 0,
                change: &input,
            }),
            transaction.access(),
        )
        .unwrap()
    else {
        panic!("deep linear expression did not retain its true row");
    };
    assert_eq!(output.schema(), input.schema());
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(0)
    ));
    assert_eq!(output.diffs().values(), input.diffs().values());
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
fn expression_decoder_rejects_bad_tags_stacks_scalars_and_utf8() {
    let payload_offset = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;
    let simple = encode_definition(&FilterDefinition::new(Expression::literal(
        Literal::Boolean(Some(true)),
    )));

    let mut unknown_node = simple.clone();
    unknown_node[payload_offset + 4] = u8::MAX;
    assert!(matches!(
        decode_definition(&unknown_node),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut missing_unary_operand = simple.clone();
    missing_unary_operand[payload_offset + 4] = 5;
    assert!(matches!(
        decode_definition(&missing_unary_operand),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut unknown_unary = missing_unary_operand.clone();
    unknown_unary[payload_offset + 5] = u8::MAX;
    assert!(matches!(
        decode_definition(&unknown_unary),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut non_canonical_presence = simple.clone();
    non_canonical_presence[payload_offset + 5] = 2;
    assert!(matches!(
        decode_definition(&non_canonical_presence),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut non_canonical_boolean = simple;
    non_canonical_boolean[payload_offset + 6] = 2;
    assert!(matches!(
        decode_definition(&non_canonical_boolean),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let utf8 = FilterDefinition::new(Expression::literal(Literal::Utf8(Some("x".to_owned()))));
    let mut invalid_utf8 = encode_definition(&utf8);
    invalid_utf8[payload_offset + 10] = u8::MAX;
    assert!(matches!(
        decode_definition(&invalid_utf8),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut oversized_utf8 = encode_definition(&utf8);
    oversized_utf8[payload_offset + 6..payload_offset + 10]
        .copy_from_slice(&((i32::MAX as u32) + 1).to_be_bytes());
    assert!(matches!(
        decode_definition(&oversized_utf8),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let equality = FilterDefinition::new(binary(
        BinaryOperator::Equal,
        Expression::literal(Literal::Boolean(Some(true))),
        Expression::literal(Literal::Boolean(Some(false))),
    ));
    let mut unknown_binary = encode_definition(&equality);
    *unknown_binary.last_mut().unwrap() = u8::MAX;
    assert!(matches!(
        decode_definition(&unknown_binary),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut extra_stack_value = encode_definition(&equality);
    let last_node_tag = extra_stack_value.len() - 2;
    extra_stack_value[last_node_tag] = 1;
    extra_stack_value[last_node_tag + 1] = 0;
    assert!(matches!(
        decode_definition(&extra_stack_value),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut zero_count = encode_definition(&FilterDefinition::new(Expression::literal(
        Literal::Boolean(Some(true)),
    )));
    zero_count[payload_offset..payload_offset + 4].copy_from_slice(&0_u32.to_be_bytes());
    assert!(matches!(
        decode_definition(&zero_count),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));

    let mut forged_count = encode_definition(&FilterDefinition::new(Expression::literal(
        Literal::Boolean(Some(true)),
    )));
    forged_count[payload_offset..payload_offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_count).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut invalid_name = encode_definition(&ExtendDefinition::new(
        "x",
        Expression::literal(Literal::UInt64(Some(1))),
    ));
    invalid_name[payload_offset + 4] = u8::MAX;
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
    let mut header = encode_definition(&FilterDefinition::new(Expression::literal(
        Literal::Boolean(Some(true)),
    )));
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
