use std::num::NonZeroU32;

use dogpaddle_operation::{
    DataInstances, OperationDefinition, OperationKind, RuntimeResource, decode_definition,
    operation::{
        Action, OperationInput,
        sink::{DiscardDefinition, DiscardError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, commit_ready, data_names, decode_hex,
    rollback_ready, turn_input, value_schema,
};

const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");

fn decoded_definition() -> Box<dyn OperationDefinition> {
    decode_definition(&decode_hex(DISCARD_V1)).unwrap()
}

#[test]
fn definition_has_stable_v1_literal_and_is_a_data_free_exact_sink() {
    let definition = DiscardDefinition::new();
    let decoded = assert_literal_definition(
        &definition,
        DISCARD_V1,
        3,
        OperationKind::Sink(NonZeroU32::MIN),
    );
    assert!(data_names(&definition).is_empty());
    assert!(
        bind(decoded.as_ref(), &[value_schema()])
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn runtime_completes_input_and_rejects_missing_or_invalid_ports() {
    let input = change(&[1, -1]);
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut operation = decoded_definition()
        .bind(&[input.schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none())
        .unwrap();
    assert!(matches!(
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
        Action::Complete(None)
    ));
    let error = rollback_ready(&mut operation, None, &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<DiscardError>(),
        Some(DiscardError::MissingInput)
    ));
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
        error.downcast_ref::<DiscardError>(),
        Some(DiscardError::InvalidInputPort { port: 1 })
    ));

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut operation = decoded_definition()
        .bind(&[input.schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none())
        .unwrap();
    assert!(matches!(
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
        Action::Complete(None)
    ));
}
