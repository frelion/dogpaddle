use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{
    Float64Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition,
    operation::{
        Action, Operation,
        transform::{DistinctDefinition, DistinctError},
    },
};
use dogpaddle_store::{
    Large, OrderedMap, ScanDirection, ScanLimit, Store, StoreError, Transactions,
};

use super::support::{
    TestStore, assert_literal_definition, bind, commit_ready, data_names, decode_hex, materialize,
    rollback_ready, turn_input,
};

const DISTINCT_V1: &str = include_str!("../fixtures/v1/distinct_definition.hex");

fn decoded_definition() -> Box<dyn OperationDefinition> {
    decode_definition(&decode_hex(DISTINCT_V1)).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn change(values: &[u64], diffs: &[i64]) -> Change {
    assert_eq!(values.len(), diffs.len());
    let records =
        RecordBatch::try_new(schema(), vec![Arc::new(UInt64Array::from(values.to_vec()))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn output_rows(output: &Change) -> Vec<(u64, i64)> {
    let values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..output.num_rows())
        .map(|row| (values.value(row), output.diffs().value(row)))
        .collect()
}

fn append_action(action: Action, rows: &mut Vec<(u64, i64)>) {
    match action {
        Action::Complete(Some(output)) => rows.extend(output_rows(&output)),
        Action::Complete(None) => {}
        Action::Idle | Action::Commit(_) => panic!("Distinct returned the wrong action"),
    }
}

fn create_operation(
    root: &TestStore,
    input_schema: &SchemaRef,
) -> (Box<dyn Operation>, Transactions) {
    let definition = DistinctDefinition::new();
    let mut store = Store::create(root.path()).unwrap();
    definition.data()[0].create(&mut store, "weights").unwrap();
    let operation = materialize(
        &definition,
        std::slice::from_ref(input_schema),
        &store,
        &["weights"],
    );
    (operation, store.into_transactions())
}

#[test]
fn literal_definition_has_tag_13_exact_schema_and_one_weight_map() {
    let definition = DistinctDefinition::new();
    let decoded = assert_literal_definition(
        &definition,
        DISTINCT_V1,
        13,
        OperationKind::Transform(NonZeroU32::MIN),
    );
    assert_eq!(data_names(&definition), ["distinct.weights"]);
    let input = schema();
    assert_eq!(
        bind(decoded.as_ref(), std::slice::from_ref(&input))
            .unwrap()
            .output_schema(),
        Some(&input)
    );

    let result = bind(&definition, &[schema()])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none());
    assert!(matches!(
        result,
        Err(MaterializeError::MissingData {
            name: "distinct.weights"
        })
    ));
}

fn distinct_trace(events: &[(u64, i64)], batches: &[usize]) -> Vec<(u64, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), events.len());
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &schema());
    let mut trace = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let batch = &events[start..start + rows];
        let input = change(
            &batch.iter().map(|event| event.0).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.1).collect::<Vec<_>>(),
        );
        append_action(
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&input)),
                &mut transactions,
            )
            .unwrap(),
            &mut trace,
        );
        start += rows;
    }
    trace
}

#[test]
fn event_order_presence_boundaries_and_rebatching_are_stable() {
    let events = [
        (1, 2),
        (1, -1),
        (2, 3),
        (1, -1),
        (1, 1),
        (2, -2),
        (2, -1),
        (1, -1),
    ];
    let expected = [(1, 1), (2, 1), (1, -1), (1, 1), (2, -1), (1, -1)];
    for batches in [
        &[8][..],
        &[1, 2, 5],
        &[3, 1, 1, 3],
        &[1, 1, 1, 1, 1, 1, 1, 1],
    ] {
        assert_eq!(distinct_trace(&events, batches), expected);
    }
}

#[test]
fn invalid_weight_changes_roll_back_the_whole_turn() {
    {
        let root = TestStore::new();
        let (mut operation, mut transactions) = create_operation(&root, &schema());
        let invalid = change(&[20, 99], &[1, -1]);
        let error = rollback_ready(
            operation.as_mut(),
            Some(turn_input(&invalid)),
            &mut transactions,
        )
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DistinctError>(),
            Some(DistinctError::NegativeWeight)
        ));

        let retry = change(&[20], &[1]);
        let mut output = Vec::new();
        append_action(
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&retry)),
                &mut transactions,
            )
            .unwrap(),
            &mut output,
        );
        assert_eq!(output, [(20, 1)]);
    }

    {
        let root = TestStore::new();
        let (mut operation, mut transactions) = create_operation(&root, &schema());
        for diff in [i64::MAX, i64::MAX] {
            let input = change(&[7], &[diff]);
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&input)),
                &mut transactions,
            )
            .unwrap();
        }

        let invalid = change(&[20, 7], &[1, 2]);
        let error = rollback_ready(
            operation.as_mut(),
            Some(turn_input(&invalid)),
            &mut transactions,
        )
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DistinctError>(),
            Some(DistinctError::WeightOverflow)
        ));

        let retry = change(&[20], &[1]);
        let mut output = Vec::new();
        append_action(
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&retry)),
                &mut transactions,
            )
            .unwrap(),
            &mut output,
        );
        assert_eq!(output, [(20, 1)]);
    }
}

#[test]
fn distinct_reopens_from_durable_weights_and_a_decoded_definition() {
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    let definition = DistinctDefinition::new();
    definition.data()[0].create(&mut store, "weights").unwrap();
    let mut operation = materialize(&definition, &[schema()], &store, &["weights"]);
    let mut transactions = store.into_transactions();
    let first = change(&[7, 8], &[2, 1]);
    let mut output = Vec::new();
    append_action(
        commit_ready(
            operation.as_mut(),
            Some(turn_input(&first)),
            &mut transactions,
        )
        .unwrap(),
        &mut output,
    );
    assert_eq!(output, [(7, 1), (8, 1)]);
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decoded_definition();
    let mut operation = materialize(decoded.as_ref(), &[schema()], &store, &["weights"]);
    let mut transactions = store.into_transactions();
    let second = change(&[7, 8, 7], &[-1, -1, -1]);
    let mut output = Vec::new();
    append_action(
        commit_ready(
            operation.as_mut(),
            Some(turn_input(&second)),
            &mut transactions,
        )
        .unwrap(),
        &mut output,
    );
    assert_eq!(output, [(8, -1), (7, -1)]);
}

#[test]
fn long_rows_are_not_used_as_store_keys() {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8,
        false,
    )]));
    let long = "x".repeat(16 * 1024);
    let records = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(StringArray::from(vec![long.as_str()]))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &input_schema);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Distinct did not emit the new row");
    };
    let labels = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(labels.value(0), long);
}

#[test]
fn signed_float_zeroes_are_distinct_exact_rows() {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let values = [-0.0_f64, 0.0, -0.0, 0.0];
    let records = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(Float64Array::from(values.to_vec()))],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 1, -1, -1])).unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &input_schema);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Distinct did not emit both signed-zero lifecycles");
    };
    let output_values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        output_values
            .values()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        values.map(f64::to_bits)
    );
    assert_eq!(output.diffs().values(), &[1, 1, -1, -1]);
}

#[test]
fn empty_logical_rows_keep_their_selected_row_count() {
    let input_schema = Arc::new(Schema::empty());
    let records = RecordBatch::try_new_with_options(
        Arc::clone(&input_schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(4)),
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 1, -1, -1])).unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &input_schema);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Distinct did not emit both empty-row boundaries");
    };
    assert_eq!(output.num_rows(), 2);
    assert_eq!(output.diffs().values(), &[1, -1]);
}

#[test]
fn zero_weight_removes_the_empty_bucket() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &schema());
    let input = change(&[7, 7], &[3, -3]);
    let mut output = Vec::new();
    append_action(
        commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap(),
        &mut output,
    );
    assert_eq!(output, [(7, 1), (7, -1)]);
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let weights: OrderedMap<Vec<u8>, Vec<u8>, Large> = store.open_data("weights").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = weights.access(transaction.access()).unwrap();
    let mut count = 0;
    access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(1, 1024).unwrap(),
            |_| {
                count += 1;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(count, 0);
}
