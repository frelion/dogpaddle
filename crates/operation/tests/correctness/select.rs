use std::{
    collections::HashMap,
    num::NonZeroU32,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, SchemaError};
use dogpaddle_operation::{
    DefinitionCodecError, Expr, ExpressionBindError, OperationBindError, OperationKind, col,
    decode_definition, encode_definition, lit,
    operation::{
        Action, OperationInput,
        transform::{SelectDefinition, SelectError, SelectSchemaError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, change_with_field_name, commit_ready,
    data_names, decode_hex, project_input_schema, rollback_ready, stateless_operation, turn_input,
    value_schema,
};

const SELECT_V1: &str = include_str!("../fixtures/v1/select_named_expressions.hex");
const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

fn persisted_definition() -> SelectDefinition {
    SelectDefinition::try_new([
        ("renamed", col("value")),
        ("next", col("value") + lit(1_u64)),
    ])
    .unwrap()
}

#[test]
fn literal_definition_reconstructs_ordered_fields_binding_and_runtime() {
    let definition = persisted_definition();
    let decoded = assert_literal_definition(
        &definition,
        SELECT_V1,
        7,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    let expected_fields = [
        ("renamed", col("value")),
        ("next", col("value") + lit(1_u64)),
    ];
    assert!(
        definition.fields().eq(expected_fields
            .iter()
            .map(|(name, expression)| (*name, expression)))
    );
    assert!(data_names(&definition).is_empty());

    let schema = value_schema();
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let mut operation = stateless_operation(decoded.as_ref(), Arc::clone(&schema));
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(selected)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
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

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&decode_hex(SELECT_V1)).unwrap();
    let mut operation = stateless_operation(decoded.as_ref(), input.schema());
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(reopened_selected)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("reopened Select did not emit its expected fields");
    };
    let renamed = reopened_selected
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let next = reopened_selected
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(renamed.values(), &[7, 8, 7]);
    assert_eq!(next.values(), &[8, 9, 8]);
    assert_eq!(reopened_selected.diffs().values(), &[1, -1, 2]);
}

#[test]
fn decoder_rejects_a_forged_count_and_invalid_field_name_without_panicking() {
    let canonical = encode_definition(&persisted_definition());

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

    let mut invalid_utf8 = canonical;
    let first_name_offset = DEFINITION_HEADER_LEN + size_of::<u32>() * 2;
    invalid_utf8[first_name_offset] = u8::MAX;
    assert!(matches!(
        decode_definition(&invalid_utf8),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn select_binds_ordered_independent_expressions_and_preserves_schema_metadata() {
    let metadata = HashMap::from([("owner".to_owned(), "test".to_owned())]);
    let input = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("message", DataType::Utf8, true),
        ],
        metadata.clone(),
    ));
    let definition = SelectDefinition::try_new([
        ("missing", col("message").is_null()),
        ("copied", col("message")),
        ("id", col("id")),
    ])
    .unwrap();

    let binding = bind(&definition, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.metadata(), &metadata);
    assert_eq!(output.fields().len(), 3);
    assert_eq!(
        output.field(0),
        &Field::new("missing", DataType::Boolean, false)
    );
    assert_eq!(output.field(1), &Field::new("copied", DataType::Utf8, true));
    assert_eq!(output.field(2), &Field::new("id", DataType::UInt64, false));
    assert!(
        output
            .fields()
            .iter()
            .all(|field| field.metadata().is_empty())
    );
}

#[test]
fn select_reports_expression_context_and_rejects_invalid_output_names_centrally() {
    let input = project_input_schema();
    let alias_reference = SelectDefinition::try_new([
        ("derived_alias", col("id")),
        ("uses_alias", col("derived_alias")),
    ])
    .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        bind(&alias_reference, std::slice::from_ref(&input))
    else {
        panic!("Select expression unexpectedly referenced an earlier output alias");
    };
    assert!(matches!(
        source.downcast_ref::<SelectSchemaError>(),
        Some(SelectSchemaError::Expression {
            field: 1,
            source: ExpressionBindError::DataFusion(_),
        })
    ));

    let duplicate =
        SelectDefinition::try_new([("same", col("id")), ("same", col("score"))]).unwrap();
    assert!(matches!(
        bind(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "same"
    ));

    let reserved = SelectDefinition::try_new([("$dogpaddle.internal", col("id"))]).unwrap();
    assert!(matches!(
        bind(&reserved, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedFieldName { ref name, .. }
        }) if name == "$dogpaddle.internal"
    ));
}

#[test]
fn runtime_rejects_invalid_ports() {
    let input = change(&[1]);
    let mut operation = stateless_operation(
        &SelectDefinition::try_new([("input", col("input"))]).unwrap(),
        input.schema(),
    );
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
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
        error.downcast_ref::<SelectError>(),
        Some(SelectError::InvalidInputPort { port: 1 })
    ));
}

#[test]
fn select_evaluates_ordered_expressions_and_shares_direct_columns_and_diffs() {
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
    let definition =
        SelectDefinition::try_new([("copied", col("label")), ("next", col("id") + lit(1_u64))])
            .unwrap();
    let mut operation = stateless_operation(&definition, Arc::clone(&schema));
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("Select did not complete with one output Change");
    };
    assert_eq!(output.schema().field(0).name(), "copied");
    assert_eq!(output.schema().field(1).name(), "next");
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(1)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let next = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(next.values(), &[11, 21, 31]);
}

#[test]
fn empty_select_preserves_input_row_count_and_diffs_and_rejects_schema_drift() {
    let input = change(&[1, -1, 2]);
    let definition = SelectDefinition::try_new(std::iter::empty::<(&str, Expr)>()).unwrap();
    let mut operation = stateless_operation(&definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SelectError>(),
        Some(SelectError::InputSchemaMismatch)
    ));
}
