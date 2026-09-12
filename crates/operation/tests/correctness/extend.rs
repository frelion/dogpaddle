use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_proto::bytes::Serializeable;
use dogpaddle_change::{Change, SchemaError};
use dogpaddle_operation::{
    DefinitionCodecError, Expr, ExpressionError, OperationBindError, OperationKind, ScalarValue,
    col, decode_definition, encode_definition, lit,
    operation::{
        Action, OperationInput,
        transform::{ExtendDefinition, ExtendError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, change_with_field_name, commit_ready,
    data_names, decode_hex, project_input_schema, rollback_ready, stateless_operation, turn_input,
    value_schema,
};

const EXTEND_V1: &str = include_str!("../fixtures/v1/extend_is_seven.hex");
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

fn extend(field_name: &str, expression: Expr) -> ExtendDefinition {
    ExtendDefinition::try_new(field_name, expression).unwrap()
}

#[test]
fn literal_definition_reconstructs_expression_binding_and_runtime() {
    let expression = col("value").eq(lit(7_u64));
    let definition = extend("is_seven", expression.clone());
    let decoded = assert_literal_definition(
        &definition,
        EXTEND_V1,
        6,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert_eq!(definition.field_name(), "is_seven");
    assert_eq!(definition.expression(), &expression);
    assert!(data_names(&definition).is_empty());

    let schema = value_schema();
    let output_schema = decoded
        .bind(std::slice::from_ref(&schema))
        .unwrap()
        .output_schema()
        .unwrap()
        .clone();
    assert_eq!(output_schema.field(1).name(), "is_seven");
    assert_eq!(output_schema.field(1).data_type(), &DataType::Boolean);
    assert!(!output_schema.field(1).is_nullable());

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
    let Action::Complete(Some(extended)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("decoded Extend did not append its expected field");
    };
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

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&decode_hex(EXTEND_V1)).unwrap();
    let mut operation = stateless_operation(decoded.as_ref(), input.schema());
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(reopened_extended)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("reopened Extend did not append its expected field");
    };
    let values = reopened_extended
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true)]
    );

    let protobuf = expression.to_bytes().unwrap();
    let encoded = encode_definition(&definition);
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
fn decoder_rejects_an_invalid_utf8_field_name() {
    let mut invalid = encode_definition(&extend("x", lit(1_u64)));
    invalid[DEFINITION_HEADER_LEN + size_of::<u32>()] = u8::MAX;
    assert!(matches!(
        decode_definition(&invalid),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

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

    let copied_flag = extend("copied_flag", col("flag"));
    let binding = bind(&copied_flag, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.metadata(), &metadata);
    assert_eq!(output.field(0), input.field(0));
    assert_eq!(output.field(2).data_type(), &DataType::Boolean);
    assert!(output.field(2).is_nullable());
    assert!(output.field(2).metadata().is_empty());

    let copied_null = extend("copied_null", col("nothing"));
    let binding = bind(&copied_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.output_schema().unwrap().field(2).is_nullable());

    let non_null = extend("constant", lit("ready"));
    let binding = bind(&non_null, std::slice::from_ref(&input)).unwrap();
    assert!(!binding.output_schema().unwrap().field(2).is_nullable());

    let typed_null = extend("missing", lit(ScalarValue::Int64(None)));
    let binding = bind(&typed_null, std::slice::from_ref(&input)).unwrap();
    assert!(binding.output_schema().unwrap().field(2).is_nullable());
}

#[test]
fn extend_output_schema_rejects_duplicate_and_reserved_names_centrally() {
    let input = project_input_schema();
    let duplicate = extend("id", col("id"));
    assert!(matches!(
        bind(&duplicate, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::DuplicateField { ref name, .. }
        }) if name == "id"
    ));

    let reserved = extend("$dogpaddle.internal", col("id"));
    assert!(matches!(
        bind(&reserved, std::slice::from_ref(&input)),
        Err(OperationBindError::InvalidOutputSchema {
            source: SchemaError::ReservedFieldName { ref name, .. }
        }) if name == "$dogpaddle.internal"
    ));
}

#[test]
fn runtime_rejects_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("copy", col("input")).unwrap(),
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
        error.downcast_ref::<ExtendError>(),
        Some(ExtendError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("renamed", &[1]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ExtendError>(),
        Some(ExtendError::Expression(ExpressionError::SchemaMismatch))
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
        &ExtendDefinition::try_new("selected", expression).unwrap(),
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
        &ExtendDefinition::try_new("label_copy", col("label")).unwrap(),
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
