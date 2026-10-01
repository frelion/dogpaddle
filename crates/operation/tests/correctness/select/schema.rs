use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{
    Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Metadata, Schema};
use dogpaddle_change::{Change, MAX_SCHEMA_TEXT_BYTES, SchemaError};
use dogpaddle_operation::{
    ExpressionBindError, OperationBindError, OperationKind, ProjectionError, cast, col,
    operation::{
        OperationInput,
        transform::{SelectDefinition, SelectField, SelectSchemaError},
    },
};
use dogpaddle_store::Store;

use super::super::support::{
    TestStore, assert_literal_definition, change, change_with_field_name, construct_checked,
    decode_hex, project_input_schema, rollback_input, roundtripped_output, run_input,
    stateless_operation, step_input, temporal_and_decimal_change, value_schema,
};

const SELECT_EXPLICIT_V1: &str = include_str!("../../fixtures/v1/select_explicit_schema.hex");

fn align(fields: impl IntoIterator<Item = SelectField>) -> SelectDefinition {
    SelectDefinition::try_new(fields)
        .unwrap()
        .with_metadata(arrow_schema::Metadata::new())
}

fn persisted_definition() -> SelectDefinition {
    SelectDefinition::try_new([
        SelectField {
            name: "renamed".into(),
            expression: col("value"),
            nullable: Some(true),
            metadata: Some(
                (HashMap::from([
                    ("z".to_owned(), "last".to_owned()),
                    ("a".to_owned(), "first".to_owned()),
                ]))
                .into(),
            ),
        },
        SelectField {
            name: "signed".into(),
            expression: cast(col("value"), DataType::Int64),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ])
    .unwrap()
    .with_metadata(HashMap::from([
        ("version".to_owned(), "1".to_owned()),
        ("owner".to_owned(), "test".to_owned()),
    ]))
}

#[test]
fn json_plan_rejects_retired_schema_align_variant() {
    let canonical = serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(
        &persisted_definition().into(),
    )
    .unwrap();
    let payload = std::str::from_utf8(&canonical[..]).unwrap();
    let retired = payload
        .replacen("\"select\"", "\"schema_align\"", 1)
        .into_bytes();
    assert!(
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&retired)
            .unwrap_err()
            .is_data()
    );
}

#[test]
fn omitted_metadata_inherits_and_explicit_empty_metadata_clears_without_copying_values() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, true)
                .with_metadata(Metadata::from([("role", "source")])),
        ],
        Metadata::from([("owner", "source")]),
    ));
    let input = Change::try_new(
        RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![Some(7), None]))],
        )
        .unwrap(),
        Int64Array::from(vec![1, -1]),
    )
    .unwrap();
    let inherited = SelectDefinition::try_new([("id", col("id"))]).unwrap();
    let cleared = SelectDefinition::try_new([SelectField {
        name: "id".into(),
        expression: col("id"),
        nullable: None,
        metadata: Some(Metadata::new()),
    }])
    .unwrap()
    .with_metadata(Metadata::new());
    for (definition, preserves) in [(inherited, true), (cleared, false)] {
        let encoded = serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(
            &definition.clone().into(),
        )
        .unwrap();
        let payload = std::str::from_utf8(&encoded[..]).unwrap();
        assert!(!payload.contains("nullable"));
        assert_eq!(
            payload.matches("\"metadata\":{}").count(),
            if preserves { 0 } else { 2 }
        );
        let output = roundtripped_output(&definition, &input);
        assert!(output.schema().field(0).is_nullable());
        assert_eq!(
            output.schema().metadata().get("owner").map(String::as_str),
            preserves.then_some("source")
        );
        assert_eq!(
            output
                .schema()
                .field(0)
                .metadata()
                .get("role")
                .map(String::as_str),
            preserves.then_some("source")
        );
        assert!(Arc::ptr_eq(
            output.records().column(0),
            input.records().column(0)
        ));
        assert_eq!(
            output.diffs().values().as_ptr(),
            input.diffs().values().as_ptr()
        );
    }
}

#[test]
fn field_and_schema_metadata_share_the_complete_output_schema_text_limit() {
    let input = value_schema();
    let metadata = Metadata::from([("owner", "x".repeat(MAX_SCHEMA_TEXT_BYTES))]);
    for definition in [
        SelectDefinition::try_new([("value", col("value"))])
            .unwrap()
            .with_metadata(metadata.clone()),
        SelectDefinition::try_new([SelectField {
            name: "value".into(),
            expression: col("value"),
            nullable: None,
            metadata: Some(metadata),
        }])
        .unwrap(),
    ] {
        assert!(matches!(
            construct_checked(&definition, std::slice::from_ref(&input)),
            Err(OperationBindError::InvalidOutputSchema {
                source: SchemaError::TooManyTextBytes {
                    max_bytes: MAX_SCHEMA_TEXT_BYTES,
                    ..
                },
            })
        ));
    }
}

#[test]
fn literal_definition_reconstructs_metadata_binding_and_runtime() {
    let definition = persisted_definition();
    let decoded = assert_literal_definition(
        &definition,
        SELECT_EXPLICIT_V1,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert!(definition.fields().eq([
        ("renamed", &col("value")),
        ("signed", &cast(col("value"), DataType::Int64)),
    ]));

    let schema = value_schema();
    let constructed = construct_checked(&decoded, std::slice::from_ref(&schema)).unwrap();
    let output_schema = constructed.as_ref().unwrap();
    assert_eq!(output_schema.metadata().get("owner").unwrap(), "test");
    assert_eq!(output_schema.field(0).name(), "renamed");
    assert!(output_schema.field(0).is_nullable());
    assert_eq!(output_schema.field(0).metadata().get("a").unwrap(), "first");
    assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);

    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let operation = stateless_operation(&decoded, schema);
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Some(aligned) = run_input(&operation, step_input(&input), &mut transactions).unwrap()
    else {
        panic!("decoded Select did not emit its expected fields");
    };
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

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let decoded = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&decode_hex(
        SELECT_EXPLICIT_V1,
    ))
    .unwrap();
    let operation = stateless_operation(&decoded, input.schema());
    let mut transactions = store.into_transactions();
    let Some(reopened_aligned) =
        run_input(&operation, step_input(&input), &mut transactions).unwrap()
    else {
        panic!("reopened Select did not emit its expected fields");
    };
    assert_eq!(
        reopened_aligned.schema().metadata().get("owner").unwrap(),
        "test"
    );
    assert!(Arc::ptr_eq(
        reopened_aligned.records().column(0),
        input.records().column(0)
    ));
    let signed = reopened_aligned
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(signed.values(), &[7, 8, 7]);
    assert_eq!(
        reopened_aligned.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
}

#[test]
fn encoding_orders_metadata_and_deserialization_checks_nullability() {
    let canonical = serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(
        &persisted_definition().into(),
    )
    .unwrap();
    let reversed_input_order = SelectDefinition::try_new([
        SelectField {
            name: "renamed".into(),
            expression: col("value"),
            nullable: Some(true),
            metadata: Some(
                ([
                    ("z".to_owned(), "last".to_owned()),
                    ("a".to_owned(), "first".to_owned()),
                ])
                .into(),
            ),
        },
        SelectField {
            name: "signed".into(),
            expression: cast(col("value"), DataType::Int64),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ])
    .unwrap()
    .with_metadata([
        ("version".to_owned(), "1".to_owned()),
        ("owner".to_owned(), "test".to_owned()),
    ]);
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(
            &reversed_input_order.clone().into()
        )
        .unwrap(),
        canonical
    );

    let payload = std::str::from_utf8(&canonical[..]).unwrap();
    let invalid_nullability = payload
        .replacen("\"nullable\":true", "\"nullable\":2", 1)
        .into_bytes();
    assert_ne!(invalid_nullability, canonical);
    assert!(
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&invalid_nullability)
            .unwrap_err()
            .is_data()
    );
}

#[test]
fn explicit_select_rejects_expression_failures_and_nullability_narrowing() {
    let input = project_input_schema();
    let missing = align([SelectField {
        name: "missing".into(),
        expression: col("missing"),
        nullable: Some(true),
        metadata: Some(arrow_schema::Metadata::new()),
    }]);
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&missing, std::slice::from_ref(&input))
    else {
        panic!("Select missing-column expression unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression {
            field: 0,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let narrowed = align([SelectField {
        name: "message".into(),
        expression: col("message"),
        nullable: Some(false),
        metadata: Some(arrow_schema::Metadata::new()),
    }]);
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&narrowed, std::slice::from_ref(&input))
    else {
        panic!("Select nullable-to-non-null field unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::NullabilityNarrowing { field: 0 })
    ));

    let widened = align([SelectField {
        name: "id".into(),
        expression: col("id"),
        nullable: Some(true),
        metadata: Some(arrow_schema::Metadata::new()),
    }]);
    assert!(
        construct_checked(&widened, std::slice::from_ref(&input))
            .unwrap()
            .unwrap()
            .field(0)
            .is_nullable()
    );

    let duplicate = align([
        SelectField {
            name: "same".into(),
            expression: col("id"),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "same".into(),
            expression: col("score"),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ]);
    assert!(matches!(
        construct_checked(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "same"
    ));

    let reserved_metadata = SelectDefinition::try_new([SelectField {
        name: "id".into(),
        expression: col("id"),
        nullable: Some(false),
        metadata: Some(arrow_schema::Metadata::new()),
    }])
    .unwrap()
    .with_metadata(HashMap::from([(
        "dogpaddle.private".to_owned(),
        "x".to_owned(),
    )]));
    assert!(matches!(
        construct_checked(&reserved_metadata, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedMetadataKey { ref key, .. }
        }) if key == "dogpaddle.private"
    ));
}

#[test]
fn temporal_and_decimal_explicit_select_executes_explicit_casts_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let align_definition = SelectDefinition::try_new([
        SelectField {
            name: "aligned_date".into(),
            expression: col("date"),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "aligned_time".into(),
            expression: col("occurred_at"),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "aligned_amount".into(),
            expression: col("amount"),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "date_days".into(),
            expression: cast(col("date"), DataType::Int32),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "time_millis".into(),
            expression: cast(col("occurred_at"), DataType::Int64),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "amount_rescaled".into(),
            expression: cast(col("amount"), DataType::Decimal128(12, 3)),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ])
    .unwrap()
    .with_metadata(arrow_schema::Metadata::new());
    let aligned = roundtripped_output(&align_definition, &input);
    for index in 0..3 {
        assert!(Arc::ptr_eq(
            aligned.records().column(index),
            input.records().column(index)
        ));
        assert!(aligned.schema().field(index).is_nullable());
    }
    assert_eq!(aligned.schema().field(3).data_type(), &DataType::Int32);
    assert!(!aligned.schema().field(3).is_nullable());
    assert_eq!(aligned.schema().field(4).data_type(), &DataType::Int64);
    assert!(aligned.schema().field(4).is_nullable());
    assert_eq!(
        aligned.schema().field(5).data_type(),
        &DataType::Decimal128(12, 3)
    );
    assert!(aligned.schema().field(5).is_nullable());
    let date_days = aligned
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(date_days.values(), &[0, 1, 2, 3, 4]);
    let time_millis = aligned
        .records()
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        time_millis.iter().collect::<Vec<_>>(),
        [Some(1_000), Some(2_000), None, Some(2_500), Some(4_000)]
    );
    let amount_rescaled = aligned
        .records()
        .column(5)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        amount_rescaled.iter().collect::<Vec<_>>(),
        [Some(1_000), None, Some(3_000), Some(4_000), Some(5_000)]
    );
    assert_eq!(
        aligned.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
}

#[test]
fn explicit_select_applies_explicit_schema_and_shares_direct_columns_and_diffs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let definition = SelectDefinition::try_new([
        SelectField {
            name: "renamed_label".into(),
            expression: col("label"),
            nullable: Some(true),
            metadata: Some((HashMap::from([("role".to_owned(), "label".to_owned())])).into()),
        },
        SelectField {
            name: "signed_id".into(),
            expression: cast(col("id"), DataType::Int64),
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "original_id".into(),
            expression: col("id"),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ])
    .unwrap()
    .with_metadata(HashMap::from([("normalized".to_owned(), "v1".to_owned())]));
    let operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("Select did not complete with one output Change");
    };
    assert_eq!(output.schema().metadata().get("normalized").unwrap(), "v1");
    assert_eq!(output.schema().field(0).name(), "renamed_label");
    assert_eq!(
        output.schema().field(0).metadata().get("role").unwrap(),
        "label"
    );
    assert_eq!(output.schema().field(1).data_type(), &DataType::Int64);
    assert!(output.schema().field(1).is_nullable());
    assert!(!output.schema().field(2).is_nullable());
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(1)
    ));
    assert!(Arc::ptr_eq(
        output.records().column(2),
        input.records().column(0)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let signed = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(signed.values(), &[10, 20, 30]);
}

#[test]
fn empty_explicit_select_preserves_row_count_and_diffs_and_rejects_schema_drift() {
    let input = change(&[1, -1, 2]);
    let definition = SelectDefinition::try_new(std::iter::empty::<SelectField>())
        .unwrap()
        .with_metadata(arrow_schema::Metadata::new());
    let operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("empty Select did not complete with one output Change");
    };
    assert_eq!(output.num_rows(), input.num_rows());
    assert!(output.schema().fields().is_empty());
    assert!(output.records().columns().is_empty());
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    let drifted = change_with_field_name("other", &[1, -1, 2]);
    let error = rollback_input(&operation, step_input(&drifted), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InputSchemaMismatch)
    ));
}

#[test]
fn explicit_select_rejects_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let definition = SelectDefinition::try_new([SelectField {
        name: "renamed".into(),
        expression: col("input"),
        nullable: Some(false),
        metadata: Some(arrow_schema::Metadata::new()),
    }])
    .unwrap()
    .with_metadata(arrow_schema::Metadata::new());
    let operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_input(
        &operation,
        OperationInput {
            port: 1,
            change: &input,
        },
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("other", &[1]);
    let error = rollback_input(&operation, step_input(&drifted), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InputSchemaMismatch)
    ));
}

#[test]
fn projection_reports_the_failing_field_after_checking_the_complete_schema() {
    let input = change(&[1, -1]);
    let failing = col("input") / dogpaddle_operation::lit(0_u64);
    let definition = SelectDefinition::try_new([
        SelectField {
            name: "first".into(),
            expression: col("input"),
            nullable: Some(false),
            metadata: Some(arrow_schema::Metadata::new()),
        },
        SelectField {
            name: "second".into(),
            expression: failing,
            nullable: Some(true),
            metadata: Some(arrow_schema::Metadata::new()),
        },
    ])
    .unwrap()
    .with_metadata(arrow_schema::Metadata::new());
    let operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let mut transactions = Store::create(fixture.path()).unwrap().into_transactions();
    let drifted = change_with_field_name("other", &[1, -1]);
    let error = rollback_input(&operation, step_input(&drifted), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InputSchemaMismatch)
    ));
    let error = rollback_input(&operation, step_input(&input), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::Expression { field: 1, .. })
    ));
    assert!(matches!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<dogpaddle_operation::ExpressionError>()),
        Some(dogpaddle_operation::ExpressionError::DataFusion(_))
    ));
}
