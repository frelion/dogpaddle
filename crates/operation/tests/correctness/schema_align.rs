use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{
    Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, SchemaError};
use dogpaddle_operation::{
    DefinitionCodecError, ExpressionBindError, OperationBindError, OperationKind, cast, col,
    decode_definition, encode_definition,
    operation::{
        Action, OperationInput,
        transform::{
            SchemaAlignDefinition, SchemaAlignDefinitionError, SchemaAlignError, SchemaAlignField,
            SchemaAlignFieldError, SchemaAlignSchemaError,
        },
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, change_with_field_name, commit_ready,
    data_names, decode_hex, project_input_schema, rollback_ready, roundtripped_output,
    stateless_operation, temporal_and_decimal_change, turn_input, value_schema,
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

#[test]
fn literal_definition_reconstructs_metadata_binding_and_runtime() {
    let definition = persisted_definition();
    let decoded = assert_literal_definition(
        &definition,
        SCHEMA_ALIGN_V1,
        9,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert!(data_names(&definition).is_empty());
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
    let binding = decoded.bind(std::slice::from_ref(&schema)).unwrap();
    let output_schema = binding.output_schema().unwrap();
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
    let mut operation = stateless_operation(decoded.as_ref(), schema);
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(aligned)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
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
    let mut operation = stateless_operation(decoded.as_ref(), input.schema());
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(reopened_aligned)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
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
    let canonical = encode_definition(&persisted_definition());
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
fn schema_align_rejects_expression_failures_and_nullability_narrowing() {
    let input = project_input_schema();
    let missing = align([SchemaAlignField::try_new("missing", col("missing"), true).unwrap()]);
    let Err(OperationBindError::Rejected { source }) = bind(&missing, std::slice::from_ref(&input))
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
        bind(&narrowed, std::slice::from_ref(&input))
    else {
        panic!("SchemaAlign nullable-to-non-null field unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<SchemaAlignSchemaError>(),
        Some(SchemaAlignSchemaError::NullabilityNarrowing { field: 0 })
    ));

    let widened = align([SchemaAlignField::try_new("id", col("id"), true).unwrap()]);
    assert!(
        bind(&widened, std::slice::from_ref(&input))
            .unwrap()
            .output_schema()
            .unwrap()
            .field(0)
            .is_nullable()
    );

    let duplicate = align([
        SchemaAlignField::try_new("same", col("id"), false).unwrap(),
        SchemaAlignField::try_new("same", col("score"), false).unwrap(),
    ]);
    assert!(matches!(
        bind(&duplicate, std::slice::from_ref(&input)),
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
        bind(&reserved_metadata, std::slice::from_ref(&input)),
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
    let mut operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InputSchemaMismatch)
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
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("other", &[1]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SchemaAlignError>(),
        Some(SchemaAlignError::InputSchemaMismatch)
    ));
}
