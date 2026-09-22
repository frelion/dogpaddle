use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, SchemaError};
use dogpaddle_operation::{
    OperationBindError, ProjectionError, ScalarValue, col, lit,
    operation::{Action, OperationInput, transform::SelectDefinition},
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, change, change_with_field_name, commit_ready, construct_checked,
    project_input_schema, rollback_ready, stateless_operation, turn_input,
};

#[test]
fn extend_derives_one_valid_field_and_preserves_input_schema_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_owned(), "test".to_owned());
    let input = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("flag", DataType::Boolean, true)
                .with_metadata(HashMap::from([("meaning".to_owned(), "input".to_owned())])),
            Field::new("nothing", DataType::Null, true),
        ],
        metadata.clone(),
    ));

    let copied_flag = SelectDefinition::try_extend(&input, [("copied_flag", col("flag"))]).unwrap();
    let binding = construct_checked(&copied_flag, std::slice::from_ref(&input)).unwrap();
    let output = binding.as_ref().unwrap();
    assert_eq!(output.metadata(), &metadata);
    assert_eq!(output.field(0), input.field(0));
    assert_eq!(output.field(2).data_type(), &DataType::Boolean);
    assert!(output.field(2).is_nullable());
    assert_eq!(output.field(2).metadata(), input.field(0).metadata());

    let copied_null =
        SelectDefinition::try_extend(&input, [("copied_null", col("nothing"))]).unwrap();
    let binding = construct_checked(&copied_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.as_ref().unwrap().field(2).is_nullable());

    let non_null = SelectDefinition::try_extend(&input, [("constant", lit("ready"))]).unwrap();
    let binding = construct_checked(&non_null, std::slice::from_ref(&input)).unwrap();
    assert!(!binding.as_ref().unwrap().field(2).is_nullable());

    let typed_null =
        SelectDefinition::try_extend(&input, [("missing", lit(ScalarValue::Int64(None)))]).unwrap();
    let binding = construct_checked(&typed_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.as_ref().unwrap().field(2).is_nullable());
}

#[test]
fn extend_output_schema_rejects_duplicate_and_reserved_names_centrally() {
    let input = project_input_schema();
    let duplicate = SelectDefinition::try_extend(&input, [("id", col("id"))]).unwrap();
    assert!(matches!(
        construct_checked(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "id"
    ));

    let reserved =
        SelectDefinition::try_extend(&input, [("$dogpaddle.internal", col("id"))]).unwrap();
    assert!(matches!(
        construct_checked(&reserved, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedFieldName { ref name, .. }
        }) if name == "$dogpaddle.internal"
    ));
}

#[test]
fn runtime_rejects_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let mut operation = stateless_operation(
        &SelectDefinition::try_extend(&input.schema(), [("copy", col("input"))]).unwrap(),
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
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("renamed", &[1]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionError>(),
        Some(ProjectionError::InputSchemaMismatch)
    ));
}

#[test]
fn extend_appends_one_derived_column_and_shares_every_input_buffer() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let expression = col("flag")
        .and(lit(ScalarValue::Boolean(None)))
        .or(col("label").is_null());
    let mut operation = stateless_operation(
        &SelectDefinition::try_extend(&input.schema(), [("selected", expression)]).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("Extend did not complete with one output Change");
    };
    assert_eq!(output.schema().fields().len(), 3);
    assert_eq!(output.schema().field(2).name(), "selected");
    assert_eq!(output.schema().field(2).data_type(), &DataType::Boolean);
    assert!(output.schema().field(2).is_nullable());
    for index in 0..2 {
        assert!(Arc::ptr_eq(
            output.records().column(index),
            input.records().column(index)
        ));
    }
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let selected = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        selected.iter().collect::<Vec<_>>(),
        [None, Some(true), None]
    );

    let mut copy = stateless_operation(
        &SelectDefinition::try_extend(&input.schema(), [("label_copy", col("label"))]).unwrap(),
        Arc::clone(&schema),
    );
    let Action::Complete(Some(copied)) =
        commit_ready(&mut copy, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("column-copy Extend did not complete");
    };
    assert!(Arc::ptr_eq(
        copied.records().column(2),
        input.records().column(1)
    ));
}

#[test]
fn append_expands_to_an_ordinary_select_and_preserves_literal_names_and_nested_buffers() {
    use super::support::roundtripped_output;
    use arrow_array::{ArrayRef, StructArray};
    use dogpaddle_operation::{encode_definition, ident};
    let nested_field = Arc::new(Field::new("child", DataType::UInt64, false));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::clone(&nested_field),
        Arc::new(UInt64Array::from(vec![7, 8])) as ArrayRef,
    )]));
    let metadata = HashMap::from([("meaning".to_owned(), "original".to_owned())]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("a.b", DataType::UInt64, false).with_metadata(metadata.clone()),
            Field::new("nested", DataType::Struct(vec![nested_field].into()), false)
                .with_metadata(metadata.clone()),
        ],
        metadata.clone(),
    ));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![1, 2])), nested],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let definition = SelectDefinition::try_extend(
        &schema,
        [
            ("next", ident("a.b") + lit(1_u64)),
            ("copy", ident("nested")),
        ],
    )
    .unwrap();
    let explicit = SelectDefinition::try_new([
        ("a.b", ident("a.b")),
        ("nested", ident("nested")),
        ("next", ident("a.b") + lit(1_u64)),
        ("copy", ident("nested")),
    ])
    .unwrap();
    assert_eq!(encode_definition(&definition), encode_definition(&explicit));
    let output = roundtripped_output(&definition, &input);
    assert_eq!(output.schema().metadata(), &metadata);
    for index in 0..2 {
        assert_eq!(output.schema().field(index), schema.field(index));
        assert!(Arc::ptr_eq(
            output.records().column(index),
            input.records().column(index)
        ));
    }
    assert!(Arc::ptr_eq(
        output.records().column(3),
        input.records().column(1)
    ));
    assert_eq!(output.schema().field(3).metadata(), &metadata);
    assert!(output.schema().field(2).metadata().is_empty());
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
    let invalid =
        SelectDefinition::try_extend(&schema, [("next", ident("a.b")), ("copy", col("next"))])
            .unwrap();
    assert!(construct_checked(&invalid, &[schema]).is_err());
}
