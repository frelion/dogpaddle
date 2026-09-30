use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{
    Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, SchemaError};
use dogpaddle_operation::{
    DefinitionCodecError, ExpressionBindError, OperationBindError, OperationKind, ProjectionError,
    cast, col, decode_definition, encode_definition,
    operation::{
        OperationInput,
        transform::{
            SchemaAlignDefinition, SchemaAlignDefinitionError, SchemaAlignField,
            SchemaAlignFieldError, SchemaAlignSchemaError,
        },
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, change, change_with_field_name, construct_checked,
    decode_hex, project_input_schema, rollback_input, roundtripped_output, run_input,
    stateless_operation, step_input, temporal_and_decimal_change, value_schema,
};

const SCHEMA_ALIGN_V1: &str = include_str!("../fixtures/v1/schema_align_explicit.hex");
const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

fn align(fields: impl IntoIterator<Item = SchemaAlignField>) -> SchemaAlignDefinition {
    SchemaAlignDefinition::try_new(fields).unwrap()
}

fn persisted_definition() -> SchemaAlignDefinition {
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

#[test]
fn public_json_deserialization_rejects_duplicate_schema_metadata() {
    let forged = r#"{"fields":[],"metadata":{"owner":"first","owner":"second"}}"#;
    assert!(serde_json::from_str::<SchemaAlignDefinition>(forged).is_err());
}

#[test]
fn literal_definition_reconstructs_metadata_binding_and_runtime() {
    let definition = persisted_definition();
    let decoded = assert_literal_definition(
        &definition,
        SCHEMA_ALIGN_V1,
        9,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    let fields = definition.fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "renamed");
    assert_eq!(fields[0].expression(), &col("value"));
    assert!(fields[0].is_nullable());
    assert_eq!(
        fields[0].metadata().collect::<Vec<_>>(),
        [("a", "first"), ("z", "last")]
    );
    assert_eq!(fields[1].name(), "signed");
    assert_eq!(fields[1].expression(), &cast(col("value"), DataType::Int64));
    assert!(!fields[1].is_nullable());
    assert_eq!(fields[1].metadata().collect::<Vec<_>>(), []);
    assert_eq!(
        definition.metadata().collect::<Vec<_>>(),
        [("owner", "test"), ("version", "1")]
    );

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
        panic!("decoded SchemaAlign did not emit its expected fields");
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
    let decoded = decode_definition(&decode_hex(SCHEMA_ALIGN_V1)).unwrap();
    let operation = stateless_operation(&decoded, input.schema());
    let mut transactions = store.into_transactions();
    let Some(reopened_aligned) =
        run_input(&operation, step_input(&input), &mut transactions).unwrap()
    else {
        panic!("reopened SchemaAlign did not emit its expected fields");
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
fn encoding_canonicalizes_metadata_and_decoder_rejects_noncanonical_payloads() {
    let canonical = encode_definition(&persisted_definition().into());
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
    assert_eq!(
        encode_definition(&reversed_input_order.clone().into()),
        canonical
    );

    let payload = std::str::from_utf8(&canonical[DEFINITION_HEADER_LEN..]).unwrap();
    let wrap = |payload: &str| {
        let mut encoded = canonical[..DEFINITION_HEADER_LEN].to_vec();
        encoded.extend_from_slice(payload.as_bytes());
        encoded
    };

    let invalid_nullability = wrap(&payload.replacen("\"nullable\":true", "\"nullable\":2", 1));
    assert_ne!(invalid_nullability, canonical);
    assert!(matches!(
        decode_definition(&invalid_nullability).unwrap_err(),
        DefinitionCodecError::InvalidJsonPayload {
            reason: "invalid value",
            ..
        }
    ));

    let unsorted = wrap(&payload.replace(
        "\"metadata\":{\"owner\":\"test\",\"version\":\"1\"}",
        "\"metadata\":{\"version\":\"1\",\"owner\":\"test\"}",
    ));
    assert_ne!(unsorted, canonical);
    assert_eq!(
        decode_definition(&unsorted).unwrap_err(),
        DefinitionCodecError::InvalidPayload("invalid SchemaAlign payload")
    );
}

#[test]
fn schema_align_rejects_expression_failures_and_nullability_narrowing() {
    let input = project_input_schema();
    let missing = align([SchemaAlignField::try_new("missing", col("missing"), true).unwrap()]);
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&missing, std::slice::from_ref(&input))
    else {
        panic!("SchemaAlign missing-column expression unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SchemaAlignSchemaError>(),
        Some(SchemaAlignSchemaError::Expression {
            field: 0,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let narrowed = align([SchemaAlignField::try_new("message", col("message"), false).unwrap()]);
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&narrowed, std::slice::from_ref(&input))
    else {
        panic!("SchemaAlign nullable-to-non-null field unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SchemaAlignSchemaError>(),
        Some(SchemaAlignSchemaError::NullabilityNarrowing { field: 0 })
    ));

    let widened = align([SchemaAlignField::try_new("id", col("id"), true).unwrap()]);
    assert!(
        construct_checked(&widened, std::slice::from_ref(&input))
            .unwrap()
            .unwrap()
            .field(0)
            .is_nullable()
    );

    let duplicate = align([
        SchemaAlignField::try_new("same", col("id"), false).unwrap(),
        SchemaAlignField::try_new("same", col("score"), false).unwrap(),
    ]);
    assert!(matches!(
        construct_checked(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "same"
    ));

    let reserved_metadata = SchemaAlignDefinition::try_new_with_metadata(
        [SchemaAlignField::try_new("id", col("id"), false).unwrap()],
        HashMap::from([("dogpaddle.private".to_owned(), "x".to_owned())]),
    )
    .unwrap();
    assert!(matches!(
        construct_checked(&reserved_metadata, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedMetadataKey { ref key, .. }
        }) if key == "dogpaddle.private"
    ));
}

#[test]
fn schema_align_rejects_duplicate_metadata_keys_at_construction() {
    let field_error = SchemaAlignField::try_new_with_metadata(
        "id",
        col("id"),
        false,
        [
            ("role".to_owned(), "first".to_owned()),
            ("role".to_owned(), "second".to_owned()),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        field_error,
        SchemaAlignFieldError::DuplicateMetadataKey { ref key } if key == "role"
    ));

    let field = SchemaAlignField::try_new("id", col("id"), false).unwrap();
    let definition_error = SchemaAlignDefinition::try_new_with_metadata(
        [field],
        [
            ("owner".to_owned(), "first".to_owned()),
            ("owner".to_owned(), "second".to_owned()),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        definition_error,
        SchemaAlignDefinitionError::DuplicateMetadataKey { ref key } if key == "owner"
    ));
}

#[test]
fn temporal_and_decimal_schema_align_executes_explicit_casts_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let align_definition = SchemaAlignDefinition::try_new([
        SchemaAlignField::try_new("aligned_date", col("date"), true).unwrap(),
        SchemaAlignField::try_new("aligned_time", col("occurred_at"), true).unwrap(),
        SchemaAlignField::try_new("aligned_amount", col("amount"), true).unwrap(),
        SchemaAlignField::try_new("date_days", cast(col("date"), DataType::Int32), false).unwrap(),
        SchemaAlignField::try_new(
            "time_millis",
            cast(col("occurred_at"), DataType::Int64),
            true,
        )
        .unwrap(),
        SchemaAlignField::try_new(
            "amount_rescaled",
            cast(col("amount"), DataType::Decimal128(12, 3)),
            true,
        )
        .unwrap(),
    ])
    .unwrap();
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
fn schema_align_applies_explicit_schema_and_shares_direct_columns_and_diffs() {
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
    let definition = SchemaAlignDefinition::try_new_with_metadata(
        [
            SchemaAlignField::try_new_with_metadata(
                "renamed_label",
                col("label"),
                true,
                HashMap::from([("role".to_owned(), "label".to_owned())]),
            )
            .unwrap(),
            SchemaAlignField::try_new("signed_id", cast(col("id"), DataType::Int64), true).unwrap(),
            SchemaAlignField::try_new("original_id", col("id"), false).unwrap(),
        ],
        HashMap::from([("normalized".to_owned(), "v1".to_owned())]),
    )
    .unwrap();
    let operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("SchemaAlign did not complete with one output Change");
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
fn empty_schema_align_preserves_row_count_and_diffs_and_rejects_schema_drift() {
    let input = change(&[1, -1, 2]);
    let definition = SchemaAlignDefinition::try_new([]).unwrap();
    let operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("empty SchemaAlign did not complete with one output Change");
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
fn schema_align_rejects_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let definition =
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("renamed", col("input"), false).unwrap()
        ])
        .unwrap();
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
    let definition = SchemaAlignDefinition::try_new([
        SchemaAlignField::try_new("first", col("input"), false).unwrap(),
        SchemaAlignField::try_new("second", failing, true).unwrap(),
    ])
    .unwrap();
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
