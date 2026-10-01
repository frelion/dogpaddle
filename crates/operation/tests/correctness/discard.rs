use std::num::NonZeroU32;

use dogpaddle_operation::{
    OperationDefinition, OperationKind, RuntimeResource, operation::sink::DiscardDefinition,
};
use dogpaddle_store::StoreSetup;

use super::support::{
    TestStore, assert_literal_definition, change, construct_checked, decode_hex, value_schema,
};

const DISCARD_V1: &str = include_str!("../fixtures/v1/discard_definition.hex");

fn decoded_definition() -> OperationDefinition {
    serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&decode_hex(DISCARD_V1))
        .unwrap()
}

#[test]
fn definition_has_stable_v1_literal_and_is_a_data_free_exact_sink() {
    let definition = DiscardDefinition::new();
    let decoded = assert_literal_definition(
        &definition,
        DISCARD_V1,
        OperationKind::Sink(NonZeroU32::MIN),
    );
    assert!(
        construct_checked(&decoded, &[value_schema()])
            .unwrap()
            .is_none()
    );
}

#[test]
fn enqueue_completes_without_outbox_or_target_work() {
    let root = TestStore::new();
    let input = change(&[1, -1]);
    let mut setup = StoreSetup::new();
    let (operation, _) = decoded_definition()
        .construct(
            &[input.schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let (mut writes, reads) = setup.commit(root.path(), |_| Ok(())).unwrap().split();
    let dogpaddle_operation::operation::Operation::Sink(mut sink) = operation else {
        panic!("expected sink");
    };
    let txn = writes.begin();
    assert!(sink.try_enqueue(txn.access(), &input).unwrap());
    txn.commit().unwrap();
    assert!(sink.load(reads.begin().access()).unwrap().is_none());
}
