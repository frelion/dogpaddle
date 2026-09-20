use std::num::NonZeroU32;

use dogpaddle_operation::{
    OperationDefinition, OperationKind, RuntimeResource, decode_definition,
    operation::{
        Action, OperationInput, Turn,
        sink::{DiscardDefinition, DiscardError},
    },
};
use dogpaddle_store::{Store, StoreSetup};

use super::support::{
    TestStore, assert_literal_definition, change, commit_ready, construct_checked, decode_hex,
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
    assert!(
        construct_checked(decoded.as_ref(), &[value_schema()])
            .unwrap()
            .is_none()
    );
}

#[test]
fn runtime_completes_input_idles_without_input_and_rejects_invalid_ports() {
    let input = change(&[1, -1]);
    let root = TestStore::new();
    let mut setup = StoreSetup::new();
    let (mut operation, output) = decoded_definition()
        .construct(
            &[input.schema()],
            &mut setup.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let mut transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
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
    let (mut operation, output) = decoded_definition()
        .construct(
            &[input.schema()],
            &mut store.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    assert!(output.is_none());
    let mut transactions = store.into_transactions();
    assert!(matches!(
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
        Action::Complete(None)
    ));
}
