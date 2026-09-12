use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, OperationKind, decode_definition,
    operation::{
        Operation, OperationInput,
        transform::{
            RunningEventCountDefinition, RunningEventCountError, RunningEventCountOperation,
        },
    },
};
use dogpaddle_store::{Cell, Store, StoreError};

use super::support::{
    ExpectedAction, TestStore, assert_literal_definition, bind, change, change_with_field_name,
    commit_ready, count_schema, data_names, decode_hex, materialize, output_values, rollback_ready,
    turn_input, value_schema,
};

const RUNNING_EVENT_COUNT_V1: &str =
    include_str!("../fixtures/v1/running_event_count_definition.hex");

fn decoded_definition() -> Box<dyn OperationDefinition> {
    decode_definition(&decode_hex(RUNNING_EVENT_COUNT_V1)).unwrap()
}

#[test]
fn definition_has_stable_v1_literal_exact_schema_and_count_declaration() {
    let definition = RunningEventCountDefinition::new();
    let decoded = assert_literal_definition(
        &definition,
        RUNNING_EVENT_COUNT_V1,
        2,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert_eq!(data_names(&definition), ["running_event_count.count"]);
    assert_eq!(
        bind(decoded.as_ref(), &[value_schema()])
            .unwrap()
            .output_schema(),
        Some(&count_schema())
    );

    let invalid = Arc::new(Schema::new(vec![Field::new(
        "$dogpaddle.reserved",
        DataType::UInt64,
        false,
    )]));
    assert!(matches!(
        bind(&definition, &[invalid]),
        Err(OperationBindError::InvalidInputSchema { input: 0, .. })
    ));
}

#[test]
fn runtime_rejects_missing_invalid_port_and_foreign_store() {
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    let mut operation = Operation::Atomic(Box::new(RunningEventCountOperation::new(
        value_schema(),
        store.create_data::<Cell<u64>>("count").unwrap(),
    )));
    let input = value_change(&[1]);
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
        error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::InvalidInputPort { port: 1 })
    ));
    drop(transactions);

    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = Store::create(foreign_root.path().join("foreign")).unwrap();
    let mut foreign_transactions = foreign.into_transactions();
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&input)),
        &mut foreign_transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::WrongStore)
    ));
}

fn running_event_count_trace(diffs: &[i64], batches: &[usize]) -> Vec<u64> {
    assert_eq!(batches.iter().sum::<usize>(), diffs.len());
    let fixture = TestStore::new();
    let definition = RunningEventCountDefinition::new();
    let mut store = Store::create(fixture.path()).unwrap();
    definition.data()[0].create(&mut store, "count").unwrap();
    let mut operation = materialize(&definition, &[value_schema()], &store, &["count"]);
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let input = value_change(&diffs[start..start + rows]);
        output.extend(output_values(
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap(),
            ExpectedAction::Complete,
            "count",
        ));
        start += rows;
    }
    output
}

#[test]
fn running_event_count_trace_is_rebatch_invariant_and_overflow_is_atomic() {
    let diffs = [1, 1, 1, -1, 1];
    let expected = [1, 2, 3, 4, 5];
    for batches in [&[5][..], &[2, 3], &[1, 1, 1, 1, 1]] {
        assert_eq!(running_event_count_trace(&diffs, batches), expected);
    }

    let reopened_root = TestStore::new();
    let decoded = decoded_definition();
    let mut store = Store::create(reopened_root.path()).unwrap();
    decoded.data()[0].create(&mut store, "count").unwrap();
    let mut operation = materialize(decoded.as_ref(), &[value_schema()], &store, &["count"]);
    let mut transactions = store.into_transactions();
    let first = value_change(&[1, -1]);
    assert_eq!(
        output_values(
            commit_ready(&mut operation, Some(turn_input(&first)), &mut transactions,).unwrap(),
            ExpectedAction::Complete,
            "count",
        ),
        [1, 2]
    );
    drop((operation, transactions));

    let store = Store::open(reopened_root.path()).unwrap();
    let decoded = decoded_definition();
    let mut operation = materialize(decoded.as_ref(), &[value_schema()], &store, &["count"]);
    let mut transactions = store.into_transactions();
    let second = value_change(&[1]);
    assert_eq!(
        output_values(
            commit_ready(&mut operation, Some(turn_input(&second)), &mut transactions,).unwrap(),
            ExpectedAction::Complete,
            "count",
        ),
        [3]
    );

    let fixture = TestStore::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let state = store.create_data::<Cell<u64>>("count").unwrap();
    let mut operation = Operation::Atomic(Box::new(RunningEventCountOperation::new(
        value_schema(),
        state.clone(),
    )));
    let input = value_change(&[1, 1]);
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        state
            .access(transaction.access())
            .unwrap()
            .set(&(u64::MAX - 1))
            .unwrap();
        transaction.commit().unwrap();
    }
    let error =
        rollback_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::Overflow)
    ));
    let transaction = transactions.begin();
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        Some(u64::MAX - 1)
    );
    transaction.commit().unwrap();
}

#[test]
fn running_event_count_preserves_persisted_bytes_when_state_codec_is_wrong() {
    let fixture = TestStore::new();
    let persisted = "not-a-u64".to_owned();
    let mut store = Store::create(fixture.path()).unwrap();
    let raw = store.create_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        raw.access(transaction.access())
            .unwrap()
            .set(&persisted)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let mut operation = Operation::Atomic(Box::new(RunningEventCountOperation::new(
        value_schema(),
        store.open_data::<Cell<u64>>("count").unwrap(),
    )));
    let mut transactions = store.into_transactions();
    let change = value_change(&[1]);
    let error =
        rollback_ready(&mut operation, Some(turn_input(&change)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Codec(_))
    ));
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let raw = store.open_data::<Cell<String>>("count").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        raw.access(transaction.access()).unwrap().get().unwrap(),
        Some(persisted)
    );
}

#[test]
fn runtime_rejects_schema_drift_before_touching_state() {
    let fixture = TestStore::new();
    let definition = RunningEventCountDefinition::new();
    let mut store = Store::create(fixture.path()).unwrap();
    definition.data()[0].create(&mut store, "count").unwrap();
    let state = store.open_data::<Cell<u64>>("count").unwrap();
    let mut operation = materialize(&definition, &[value_schema()], &store, &["count"]);
    let mismatched = change(&[1]);
    let mut transactions = store.into_transactions();

    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&mismatched)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RunningEventCountError>(),
        Some(RunningEventCountError::InputSchemaMismatch)
    ));
    let transaction = transactions.begin();
    assert_eq!(
        state.access(transaction.access()).unwrap().get().unwrap(),
        None
    );
    transaction.commit().unwrap();
}

fn value_change(diffs: &[i64]) -> dogpaddle_change::Change {
    change_with_field_name("value", diffs)
}
