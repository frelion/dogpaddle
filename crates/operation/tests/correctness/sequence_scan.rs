use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, RuntimeResource, decode_definition,
    operation::{
        Action, Operation, OperationInput,
        scan::{SequenceScanDefinition, SequenceScanError},
    },
};
use dogpaddle_store::{Cell, Store, StoreError, StoreSetup, Transactions};

use super::support::{
    ExpectedAction, TestStore, assert_literal_definition, change, commit_ready, construct_checked,
    decode_hex, output_values, rollback_ready, value_schema,
};

const SEQUENCE_V1: &str = include_str!("../fixtures/v1/sequence_scan_start_42.hex");

fn construct_operation(
    root: &TestStore,
    definition: &dyn OperationDefinition,
) -> (Operation, Transactions) {
    let mut setup = StoreSetup::new();
    let constructed = definition
        .construct(
            &[],
            &mut setup.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .unwrap();
    let (operation, _) = constructed.into_parts();
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    (operation, transactions)
}

#[test]
fn definition_has_stable_v1_literal_exact_schema_and_position_declaration() {
    let definition = SequenceScanDefinition::new(42);
    let decoded = assert_literal_definition(&definition, SEQUENCE_V1, 1, OperationKind::Scan);
    assert_eq!(definition.start(), 42);
    assert_eq!(
        construct_checked(decoded.as_ref(), &[]).unwrap().as_ref(),
        Some(&value_schema())
    );
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_operation(&root, decoded.as_ref());
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
    let constructed = decoded
        .construct(
            &[],
            &mut store.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .unwrap();
    let (mut operation, _) = constructed.into_parts();
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
        construct_checked(&definition, &[value_schema()]),
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
    let (mut operation, mut transactions) = construct_operation(&root, &definition);
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
    let position = store
        .open_data::<Cell<u64>>("operation/sequence_scan.position")
        .unwrap();
    let constructed = (&definition as &dyn OperationDefinition)
        .construct(
            &[],
            &mut store.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .unwrap();
    let (mut operation, _) = constructed.into_parts();
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
    let (mut operation, mut transactions) =
        construct_operation(&root, &SequenceScanDefinition::new(0));
    let input = change(&[1]);
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
