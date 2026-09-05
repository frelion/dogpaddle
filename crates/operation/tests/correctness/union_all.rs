use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, DefinitionCodecError, OperationBindError, OperationDefinition, OperationKind,
    decode_definition, encode_definition,
    operation::{
        Action, OperationInput,
        transform::{UnionAllDefinition, UnionAllError, UnionAllSchemaError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, change_with_field_name, commit_ready,
    data_names, decode_hex, rollback_ready, value_schema,
};

const UNION_ALL_V1: &str = include_str!("../fixtures/v1/union_all_two_inputs.hex");
const DEFINITION_HEADER_LEN: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

fn decoded_definition() -> Box<dyn OperationDefinition> {
    decode_definition(&decode_hex(UNION_ALL_V1)).unwrap()
}

fn assert_forwarded_without_copying(input: &Change, output: &Change) {
    assert_eq!(output.schema(), input.schema());
    assert_eq!(output.num_rows(), input.num_rows());
    assert!(
        output
            .records()
            .columns()
            .iter()
            .zip(input.records().columns())
            .all(|(output, input)| Arc::ptr_eq(output, input))
    );
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );
}

#[test]
fn union_all_requires_its_non_zero_arity_and_exact_input_schema() {
    let definition = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    let expected = value_schema();
    let mismatched = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        true,
    )]));

    let Err(OperationBindError::Rejected { source }) = bind(
        &definition,
        &[Arc::clone(&expected), Arc::clone(&mismatched)],
    ) else {
        panic!("mismatched UnionAll input unexpectedly bound");
    };
    assert!(matches!(
        source.downcast_ref::<UnionAllSchemaError>(),
        Some(UnionAllSchemaError::InputSchemaMismatch {
            input: 1,
            expected: actual_expected,
            actual,
        }) if actual_expected == &expected && actual == &mismatched
    ));

    let binding = bind(&definition, &[Arc::clone(&expected), Arc::clone(&expected)]).unwrap();
    assert_eq!(binding.output_schema(), Some(&expected));
}

#[test]
fn literal_definition_preserves_arity_binding_and_data_contract() {
    let definition = UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
    let decoded = assert_literal_definition(
        &definition,
        UNION_ALL_V1,
        8,
        OperationKind::Transform(NonZeroU32::new(2).unwrap()),
    );
    assert_eq!(definition.input_count().get(), 2);
    assert!(data_names(&definition).is_empty());
    let inputs = [value_schema(), value_schema()];
    assert_eq!(
        decoded.bind(&inputs).unwrap().output_schema(),
        Some(&value_schema())
    );
}

#[test]
fn decoder_rejects_zero_input_count() {
    let mut zero = encode_definition(&UnionAllDefinition::new(NonZeroU32::new(2).unwrap()));
    zero[DEFINITION_HEADER_LEN..].copy_from_slice(&0_u32.to_be_bytes());
    assert!(matches!(
        decode_definition(&zero),
        Err(DefinitionCodecError::InvalidPayload(_))
    ));
}

#[test]
fn union_all_forwards_every_legal_port_without_copying() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let input = Change::try_new(
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, -1, 2]),
    )
    .unwrap();
    let data = DataInstances::new();
    let mut operation = decoded_definition()
        .bind(&[input.schema(), input.schema()])
        .unwrap()
        .materialize(data, dogpaddle_operation::RuntimeResource::none())
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    for port in 0..2 {
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(OperationInput {
                port,
                change: &input,
            }),
            &mut transactions,
        )
        .unwrap() else {
            panic!("UnionAll did not forward input port {port}");
        };
        assert_forwarded_without_copying(&input, &output);
    }

    let drifted = change_with_field_name("other", &[1, -1, 2]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &drifted,
        }),
        &mut transactions,
    )
    .unwrap_err();
    let Some(UnionAllError::InputSchemaMismatch {
        port,
        expected,
        actual,
    }) = error.downcast_ref::<UnionAllError>()
    else {
        panic!("UnionAll accepted a runtime Schema that differs from its binding");
    };
    assert_eq!(*port, 1);
    assert_eq!(expected.as_ref(), input.schema().as_ref());
    assert_eq!(actual.as_ref(), drifted.schema().as_ref());

    drop((operation, transactions));
    let store = Store::open(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut operation = decoded_definition()
        .bind(&[input.schema(), input.schema()])
        .unwrap()
        .materialize(
            DataInstances::new(),
            dogpaddle_operation::RuntimeResource::none(),
        )
        .unwrap();
    let Action::Complete(Some(reopened_output)) = commit_ready(
        operation.as_mut(),
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap() else {
        panic!("reopened UnionAll did not forward input port 1");
    };
    assert_forwarded_without_copying(&input, &reopened_output);
}

#[test]
fn runtime_rejects_missing_and_invalid_ports() {
    let input = change(&[1]);
    let definition = UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
    let mut operation = (&definition as &dyn OperationDefinition)
        .bind(&[input.schema(), input.schema()])
        .unwrap()
        .materialize(
            DataInstances::new(),
            dogpaddle_operation::RuntimeResource::none(),
        )
        .unwrap();
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(operation.as_mut(), None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<UnionAllError>(),
        Some(UnionAllError::MissingInput)
    ));
    let error = rollback_ready(
        operation.as_mut(),
        Some(OperationInput {
            port: 2,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<UnionAllError>(),
        Some(UnionAllError::InvalidInputPort {
            port: 2,
            input_count: 2,
        })
    ));
}
