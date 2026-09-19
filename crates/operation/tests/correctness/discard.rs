use std::num::NonZeroU32;

use dogpaddle_operation::{
    OperationDefinition, OperationKind, RuntimeResource, create_operation, decode_definition,
    open_operation,
    operation::{
        Action, OperationInput, Turn,
        sink::{DiscardDefinition, DiscardError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, commit_ready, decode_hex, rollback_ready,
    turn_input, value_schema,
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
    assert!(
        bind(decoded.as_ref(), &[value_schema()])
            .unwrap()
            .output_schema()
            .is_none()
    );
}

#[test]
fn runtime_completes_input_idles_without_input_and_rejects_invalid_ports() {
    let input = change(&[1, -1]);
    let root = TestStore::new();
    let mut setup = Store::setup(root.path()).unwrap();
    let binding = bind(decoded_definition().as_ref(), &[input.schema()]).unwrap();
    let mut operation =
        create_operation(binding, &mut setup, "operation", RuntimeResource::none()).unwrap();
    let mut transactions = setup.commit(|_| Ok(())).unwrap();
    assert!(matches!(
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
        Action::Complete(None)
    ));
    assert!(matches!(operation.turn(None).unwrap(), Turn::Idle));
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
    let binding = bind(decoded_definition().as_ref(), &[input.schema()]).unwrap();
    let mut operation =
        open_operation(binding, &store, "operation", RuntimeResource::none()).unwrap();
    let mut transactions = store.into_transactions();
    assert!(matches!(
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
        Action::Complete(None)
    ));
}
