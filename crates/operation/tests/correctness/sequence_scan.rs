use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, decode_definition,
    operation::{
        Action, Operation, OperationInput,
        scan::{SequenceScanDefinition, SequenceScanError, SequenceScanOperation},
    },
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::{
    ExpectedAction, TestStore, assert_literal_definition, bind, change, commit_ready, data_names,
    decode_hex, materialize, output_values, rollback_ready, value_schema,
};

const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_scan_start_42.hex");

#[test]
fn definition_has_stable_v1_literal_exact_schema_and_position_declaration() {
    let definition = SequenceScanDefinition::new(42);
    let decoded = assert_literal_definition(&definition, SEQUENCE_V1, 1, OperationKind::Scan);
    assert_eq!(definition.start(), 42);
    assert_eq!(data_names(&definition), ["sequence_scan.position"]);
    assert_eq!(
        bind(decoded.as_ref(), &[]).unwrap().output_schema(),
        Some(&value_schema())
    );
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    decoded.data()[0].create(&mut store, "position").unwrap();
    let mut operation = materialize(decoded.as_ref(), &[], &store, &["position"]);
    let mut transactions = store.into_transactions();
    assert_eq!(
        output_values(
            commit_ready(&mut operation, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [42]
    );
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&decode_hex(SEQUENCE_V1)).unwrap();
    let mut operation = materialize(decoded.as_ref(), &[], &store, &["position"]);
    let mut transactions = store.into_transactions();
    assert_eq!(
        output_values(
            commit_ready(&mut operation, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [43]
    );
    assert!(matches!(
        bind(&definition, &[value_schema()]),
        Err(OperationBindError::InputCount {
            expected: 0,
            actual: 1
        })
    ));
}

#[test]
fn rollback_commit_reopen_and_terminal_position_are_exact() {
    let root = TestStore::new();
    let definition = SequenceScanDefinition::new(u64::MAX - 1);
    let mut store = Store::create(root.path()).unwrap();
    definition.data()[0].create(&mut store, "position").unwrap();
    drop(store);

    let store = Store::open(root.path()).unwrap();
    let mut operation = materialize(&definition, &[], &store, &["position"]);
    let mut transactions = store.into_transactions();
    assert_eq!(
        output_values(
            rollback_ready(&mut operation, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [u64::MAX - 1]
    );
    assert_eq!(
        output_values(
            commit_ready(&mut operation, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [u64::MAX - 1]
    );
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let position = store.open_data::<Cell<u64>>("position").unwrap();
    let mut operation = materialize(&definition, &[], &store, &["position"]);
    let mut transactions = store.into_transactions();
    assert_eq!(
        output_values(
            commit_ready(&mut operation, None, &mut transactions).unwrap(),
            ExpectedAction::Commit,
            "value",
        ),
        [u64::MAX]
    );
    assert!(matches!(
        rollback_ready(&mut operation, None, &mut transactions).unwrap(),
        Action::Idle
    ));
    let transaction = transactions.begin();
    assert_eq!(
        position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(u64::MAX)
    );
    transaction.commit().unwrap();
}

#[test]
fn runtime_rejects_input_and_a_foreign_store() {
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    let position = store.create_data::<Cell<u64>>("position").unwrap();
    let mut operation = Operation::Turn(Box::new(SequenceScanOperation::new(0, position)));
    let input = change(&[1]);
    let mut transactions = store.into_transactions();
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<SequenceScanError>(),
        Some(SequenceScanError::UnexpectedInput)
    ));
    drop(transactions);

    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = Store::create(foreign_root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign.into_transactions();
    let error = rollback_ready(&mut operation, None, &mut foreign_transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
}
