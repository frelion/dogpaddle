use std::{
    collections::{BTreeMap, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    process::Command,
    sync::Arc,
};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion_common::metadata::FieldMetadata;
use datafusion_expr::{Volatility, create_udf, expr::Cast, placeholder};
use datafusion_proto::bytes::Serializeable;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DefinitionCodecError, Expr, ExpressionBindError, ExpressionDefinitionError, OperationBindError,
    Operator, ScalarValue, cast, col, decode_definition, encode_definition, lit,
    operation::{
        Action,
        transform::{
            ExtendDefinition, ExtendSchemaError, FilterDefinition, FilterSchemaError,
            ProjectDefinition, SelectDefinition,
        },
    },
    try_cast,
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, bind, commit_ready, project_input_schema, roundtripped_output, stateless_operation,
    temporal_and_decimal_change, turn_input,
};

fn filter(predicate: Expr) -> FilterDefinition {
    FilterDefinition::try_new(predicate).unwrap()
}

fn extend(field_name: &str, expression: Expr) -> ExtendDefinition {
    ExtendDefinition::try_new(field_name, expression).unwrap()
}

const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;
const MAP_EXPRESSION_PROBE: &str = "DOGPADDLE_MAP_EXPRESSION_PROBE";

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

    for malformed in [wrap(&[]), wrap(&[u8::MAX])] {
        assert!(matches!(
            decode_definition(&malformed),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
    }

    let mut forged_length = canonical.clone();
    forged_length[DEFINITION_HEADER_LEN..DEFINITION_HEADER_LEN + size_of::<u32>()]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_definition(&forged_length).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    let mut protobuf_with_unknown_field = protobuf;
    protobuf_with_unknown_field.extend_from_slice(&[0xf8, 0x07, 0x00]);
    assert!(matches!(
        decode_definition(&wrap(&protobuf_with_unknown_field)),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn expression_decoder_never_panics_for_valid_header_arbitrary_payloads() {
    let mut header = encode_definition(&filter(lit(true)));
    header.truncate(DEFINITION_HEADER_LEN);
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

#[test]
fn expression_binding_delegates_planning_errors_and_enforces_filter_results() {
    let input = project_input_schema();
    let Err(OperationBindError::Rejected { source }) = bind(
        &extend("copy", col("missing")),
        std::slice::from_ref(&input),
    ) else {
        panic!("out-of-bounds expression column unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<ExtendSchemaError>(),
        Some(ExtendSchemaError::Expression(
            ExpressionBindError::DataFusion(_)
        ))
    ));

    let Err(OperationBindError::Rejected { source }) =
        bind(&filter(col("id")), std::slice::from_ref(&input))
    else {
        panic!("non-Boolean filter predicate unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<FilterSchemaError>(),
        Some(FilterSchemaError::PredicateType {
            actual: DataType::UInt64
        })
    ));
}

#[test]
fn expression_constructors_accept_exactly_round_tripping_datafusion_exprs() {
    for expression in [
        lit(1_i32),
        col("scope.value"),
        col("value").alias("renamed"),
    ] {
        let definition = FilterDefinition::try_new(expression.clone()).unwrap();
        assert_eq!(definition.predicate(), &expression);
    }

    let expression = col("value").between(lit(1_u64), lit(10_u64));
    let definition = ExtendDefinition::try_new("in_range", expression.clone()).unwrap();
    assert_eq!(definition.expression(), &expression);
}

#[test]
fn expression_constructor_rejects_literal_metadata_that_uses_a_protobuf_map() {
    let expression = Expr::Literal(
        ScalarValue::Int64(Some(7)),
        Some(FieldMetadata::new(BTreeMap::from([(
            "source".to_owned(),
            "test".to_owned(),
        )]))),
    );

    assert!(matches!(
        FilterDefinition::try_new(expression),
        Err(ExpressionDefinitionError::NonCanonical)
    ));
}

fn map_bearing_cast() -> Expr {
    let child = Arc::new(
        Field::new("child", DataType::UInt64, true).with_metadata(HashMap::from([
            ("alpha".to_owned(), "1".to_owned()),
            ("beta".to_owned(), "2".to_owned()),
            ("delta".to_owned(), "4".to_owned()),
            ("gamma".to_owned(), "3".to_owned()),
        ])),
    );
    Expr::Cast(Cast::new_from_field(
        Box::new(col("value")),
        Arc::new(Field::new(
            "cast",
            DataType::Struct(vec![child].into()),
            true,
        )),
    ))
}

#[test]
fn map_bearing_expression_is_rejected_consistently_across_processes() {
    if let Some(path) = std::env::var_os(MAP_EXPRESSION_PROBE) {
        assert!(matches!(
            FilterDefinition::try_new(map_bearing_cast()),
            Err(ExpressionDefinitionError::NonCanonical)
        ));
        assert!(matches!(
            decode_definition(&std::fs::read(path).unwrap()),
            Err(DefinitionCodecError::InvalidPayload(_))
        ));
        return;
    }

    let protobuf = map_bearing_cast().to_bytes().unwrap();
    let canonical = encode_definition(&filter(lit(true)));
    let mut encoded = canonical[..DEFINITION_HEADER_LEN].to_vec();
    encoded.extend_from_slice(&u32::try_from(protobuf.len()).unwrap().to_be_bytes());
    encoded.extend_from_slice(&protobuf);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("map-bearing.definition");
    std::fs::write(&path, encoded).unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "expression::map_bearing_expression_is_rejected_consistently_across_processes",
        ])
        .env(MAP_EXPRESSION_PROBE, &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "map-bearing expression child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn expression_boundaries_reject_external_registry_variables_and_unbound_parameters() {
    let external = create_udf(
        "external_identity",
        vec![DataType::UInt64],
        DataType::UInt64,
        Volatility::Volatile,
        Arc::new(|arguments| Ok(arguments[0].clone())),
    );
    assert!(matches!(
        FilterDefinition::try_new(external.call(vec![col("id")])),
        Err(ExpressionDefinitionError::DataFusion(_))
    ));

    let variable = Expr::ScalarVariable(
        Arc::new(Field::new("session.value", DataType::Utf8, true)),
        vec!["session".to_owned(), "value".to_owned()],
    );
    assert!(matches!(
        FilterDefinition::try_new(variable),
        Err(ExpressionDefinitionError::DataFusion(_))
    ));

    let parameter = extend("parameter", placeholder("$1"));
    let Err(OperationBindError::Rejected { source }) =
        bind(&parameter, std::slice::from_ref(&project_input_schema()))
    else {
        panic!("unbound expression parameter unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<ExtendSchemaError>(),
        Some(ExtendSchemaError::Expression(
            ExpressionBindError::DataFusion(_)
        ))
    ));
}

#[test]
fn datafusion_binding_derives_arithmetic_and_cast_output_schema() {
    let input = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, true),
    ]));

    let arithmetic = extend("next", cast(col("value"), DataType::Int64) + lit(1_i64));
    let binding = bind(&arithmetic, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.field(2).data_type(), &DataType::Int64);
    assert!(!output.field(2).is_nullable());

    let parsed = extend("parsed", try_cast(col("text"), DataType::Int64));
    let binding = bind(&parsed, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.field(2).data_type(), &DataType::Int64);
    assert!(output.field(2).is_nullable());
}

fn comparison(operator: Operator, left: Expr, right: Expr) -> Expr {
    match operator {
        Operator::Eq => left.eq(right),
        Operator::NotEq => left.not_eq(right),
        _ => panic!("comparison helper received unsupported operator {operator}"),
    }
}

fn repeat_each(values: [Option<bool>; 3]) -> Vec<Option<bool>> {
    values
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value, 3))
        .collect()
}

fn kleene_cases() -> Vec<(&'static str, Expr, Vec<Option<bool>>)> {
    vec![
        (
            "and",
            col("left").and(col("right")),
            vec![
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
                None,
            ],
        ),
        (
            "or",
            col("left").or(col("right")),
            vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
                None,
            ],
        ),
        (
            "and_scalar_right",
            col("left").and(lit(ScalarValue::Boolean(None))),
            repeat_each([None, Some(false), None]),
        ),
        (
            "or_scalar_left",
            lit(false).or(col("right")),
            [Some(true), Some(false), None].repeat(3),
        ),
        (
            "not",
            !col("left"),
            repeat_each([Some(false), Some(true), None]),
        ),
        (
            "null_type_is_null",
            col("nothing").is_null(),
            vec![Some(true); 9],
        ),
        (
            "non_null_is_null",
            col("number").is_null(),
            vec![Some(false); 9],
        ),
    ]
}

#[test]
fn temporal_and_decimal_direct_columns_cross_project_select_and_extend_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let schema = input.schema();

    let projected = roundtripped_output(&ProjectDefinition::new([0, 1, 2]), &input);
    assert_eq!(projected.schema(), schema);
    for index in 0..3 {
        assert!(Arc::ptr_eq(
            projected.records().column(index),
            input.records().column(index)
        ));
    }
    assert_eq!(
        projected.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    let select_definition = SelectDefinition::try_new([
        ("selected_amount", col("amount")),
        ("selected_date", col("date")),
        ("selected_time", col("occurred_at")),
    ])
    .unwrap();
    let selected = roundtripped_output(&select_definition, &input);
    assert_eq!(
        selected
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("selected_amount", &DataType::Decimal128(10, 2), true),
            ("selected_date", &DataType::Date32, false),
            (
                "selected_time",
                &DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]
    );
    for (output, input_index) in [(0, 2), (1, 0), (2, 1)] {
        assert!(Arc::ptr_eq(
            selected.records().column(output),
            input.records().column(input_index)
        ));
    }
    assert_eq!(
        selected.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    for (source, copy, input_index) in [
        ("date", "date_copy", 0),
        ("occurred_at", "occurred_at_copy", 1),
        ("amount", "amount_copy", 2),
    ] {
        let definition = ExtendDefinition::try_new(copy, col(source)).unwrap();
        let extended = roundtripped_output(&definition, &input);
        assert_eq!(extended.schema().field(3).name(), copy);
        assert_eq!(
            extended.schema().field(3).data_type(),
            schema.field(input_index).data_type()
        );
        assert_eq!(
            extended.schema().field(3).is_nullable(),
            schema.field(input_index).is_nullable()
        );
        assert!(Arc::ptr_eq(
            extended.records().column(3),
            input.records().column(input_index)
        ));
        assert_eq!(
            extended.diffs().values().as_ptr(),
            input.diffs().values().as_ptr()
        );
    }
}

#[test]
fn boolean_expression_operators_follow_complete_kleene_truth_tables() {
    let values = [Some(true), Some(false), None];
    let left = values
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(value, 3))
        .collect::<Vec<_>>();
    let right = values.repeat(3);
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Boolean, true),
        Field::new("right", DataType::Boolean, true),
        Field::new("nothing", DataType::Null, false),
        Field::new("number", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(left)),
            Arc::new(BooleanArray::from(right)),
            arrow_array::new_null_array(&DataType::Null, 9),
            Arc::new(UInt64Array::from(vec![1; 9])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1; 9])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    for (name, expression, expected) in kleene_cases() {
        let mut operation = stateless_operation(
            &ExtendDefinition::try_new(name, expression).unwrap(),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) =
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
        else {
            panic!("Boolean expression Extend returned the wrong action");
        };
        let actual = output
            .records()
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "wrong result for {name}");
    }
}

#[test]
fn equality_operators_cover_representative_scalar_types_and_propagate_null() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("boolean", DataType::Boolean, true),
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int64Array::from(vec![Some(-2), Some(3), None])),
            Arc::new(UInt64Array::from(vec![Some(7), Some(8), None])),
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 1, 1])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let operands = [
        ("boolean", ScalarValue::Boolean(Some(true))),
        ("signed", ScalarValue::Int64(Some(-2))),
        ("unsigned", ScalarValue::UInt64(Some(7))),
        ("text", ScalarValue::Utf8(Some("x".to_owned()))),
    ];

    for (column, literal) in operands {
        for (operator, expected) in [
            (Operator::Eq, [Some(true), Some(false), None]),
            (Operator::NotEq, [Some(false), Some(true), None]),
        ] {
            for expression in [
                comparison(operator, col(column), lit(literal.clone())),
                comparison(operator, lit(literal.clone()), col(column)),
            ] {
                let mut operation = stateless_operation(
                    &ExtendDefinition::try_new("result", expression).unwrap(),
                    Arc::clone(&schema),
                );
                let Action::Complete(Some(output)) =
                    commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions)
                        .unwrap()
                else {
                    panic!("comparison Extend returned the wrong action");
                };
                let actual = output
                    .records()
                    .column(4)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
    }

    for (operator, expected) in [
        (Operator::Eq, [Some(true), Some(true), None]),
        (Operator::NotEq, [Some(false), Some(false), None]),
    ] {
        let mut operation = stateless_operation(
            &ExtendDefinition::try_new(
                "array_result",
                comparison(operator, col("boolean"), col("boolean")),
            )
            .unwrap(),
            Arc::clone(&schema),
        );
        let Action::Complete(Some(output)) =
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
        else {
            panic!("array comparison Extend returned the wrong action");
        };
        let actual = output
            .records()
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[test]
fn datafusion_arithmetic_comparison_and_casts_execute_vectorized() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![Some("10"), Some("bad")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let predicate = (cast(col("value"), DataType::Int64) + lit(1_i64)).gt(lit(8_i64));
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("greater", predicate).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("arithmetic expression did not produce an output");
    };
    let greater = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        greater.iter().collect::<Vec<_>>(),
        [Some(false), Some(true)]
    );

    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("parsed", try_cast(col("text"), DataType::Int64)).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("try-cast expression did not produce an output");
    };
    let parsed = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(parsed.iter().collect::<Vec<_>>(), [Some(10), None]);
}
