use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    Expr, MaterializeError, OperationBindError, OperationDefinition, OperationKind, col,
    operation::{
        Action, Operation, OperationError, OperationInput,
        transform::{
            InnerEquiJoinDefinition, InnerEquiJoinDefinitionError, InnerEquiJoinError,
            InnerEquiJoinSchemaError,
        },
    },
};
use dogpaddle_store::{PartitionedMultiset, Store, Transactions};

use super::support::{
    TestStore, assert_literal_definition, bind, commit_ready, data_names, materialize,
    rollback_ready,
};

const PHYSICAL_DATA: [&str; 3] = ["left-rows", "right-rows", "continuation"];
const INNER_JOIN_V1: &str = include_str!("../fixtures/v1/inner_equi_join_id.hex");
type OutputRow = (Option<u64>, String, Option<u64>, i64, i64);

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, true)
                .with_metadata(HashMap::from([("source".into(), "left".into())])),
            Field::new("label", DataType::Utf8, false),
        ],
        HashMap::from([("relation".into(), "left".into())]),
    ))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("fk", DataType::UInt64, true)
                .with_metadata(HashMap::from([("source".into(), "right".into())])),
            Field::new("amount", DataType::Int64, false),
        ],
        HashMap::from([("relation".into(), "right".into())]),
    ))
}

fn definition() -> InnerEquiJoinDefinition {
    InnerEquiJoinDefinition::try_new(
        [(col("id"), col("fk"))],
        ["left_id", "left_label", "right_fk", "right_amount"],
    )
    .unwrap()
}

fn left_change(ids: Vec<Option<u64>>, labels: Vec<&str>, differences: Vec<i64>) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            left_schema(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap(),
        Int64Array::from(differences),
    )
    .unwrap()
}

fn right_change(keys: Vec<Option<u64>>, amounts: Vec<i64>, differences: Vec<i64>) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            right_schema(),
            vec![
                Arc::new(UInt64Array::from(keys)),
                Arc::new(Int64Array::from(amounts)),
            ],
        )
        .unwrap(),
        Int64Array::from(differences),
    )
    .unwrap()
}

fn create_operation(root: &TestStore) -> (Operation, Transactions) {
    let definition = definition();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
        declaration.create(&mut store, physical).unwrap();
    }
    let operation = materialize(
        &definition,
        &[left_schema(), right_schema()],
        &store,
        &PHYSICAL_DATA,
    );
    (operation, store.into_transactions())
}

fn run_claim(
    operation: &mut Operation,
    transactions: &mut Transactions,
    port: usize,
    input: &Change,
) -> Result<Vec<Change>, OperationError> {
    run_claim_with_turns(operation, transactions, port, input).map(|(outputs, _turns)| outputs)
}

fn run_claim_with_turns(
    operation: &mut Operation,
    transactions: &mut Transactions,
    port: usize,
    input: &Change,
) -> Result<(Vec<Change>, usize), OperationError> {
    let mut outputs = Vec::new();
    for turn in 1..=10_000 {
        let action = commit_ready(
            operation,
            Some(OperationInput {
                port,
                change: input,
            }),
            transactions,
        )?;
        match action {
            Action::Commit(output) => outputs.extend(output),
            Action::Complete(output) => {
                outputs.extend(output);
                return Ok((outputs, turn));
            }
            Action::Idle => panic!("inner join returned Idle for a pinned input"),
        }
    }
    panic!("inner join did not complete a bounded test claim")
}

fn output_rows(outputs: &[Change]) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for output in outputs {
        let left_ids = output
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let labels = output
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right_ids = output
            .records()
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let amounts = output
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            rows.push((
                (!left_ids.is_null(row)).then(|| left_ids.value(row)),
                labels.value(row).to_owned(),
                (!right_ids.is_null(row)).then(|| right_ids.value(row)),
                amounts.value(row),
                output.diffs().value(row),
            ));
        }
    }
    rows
}

#[test]
fn definition_binds_fixed_left_then_right_output_and_exact_data() {
    let definition = definition();
    let decoded = assert_literal_definition(
        &definition,
        INNER_JOIN_V1,
        16,
        OperationKind::TurnTransform(NonZeroU32::new(2).unwrap()),
    );
    assert_eq!(
        data_names(&definition),
        [
            "inner_join.left_rows",
            "inner_join.right_rows",
            "inner_join.continuation"
        ]
    );
    assert_eq!(definition.keys().len(), 1);
    assert_eq!(
        definition.output_names().collect::<Vec<_>>(),
        ["left_id", "left_label", "right_fk", "right_amount"]
    );

    let binding = bind(decoded.as_ref(), &[left_schema(), right_schema()]).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(
        output
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["left_id", "left_label", "right_fk", "right_amount"]
    );
    assert!(output.metadata().is_empty());
    assert_eq!(output.field(0).metadata().get("source").unwrap(), "left");
    assert_eq!(output.field(2).metadata().get("source").unwrap(), "right");

    let result = binding.materialize(
        dogpaddle_operation::DataInstances::new(),
        dogpaddle_operation::RuntimeResource::none(),
    );
    assert!(matches!(
        result,
        Err(MaterializeError::MissingData {
            name: "inner_join.left_rows"
        })
    ));
}

#[test]
fn definition_rejects_empty_mismatched_and_unsupported_keys_and_bad_names() {
    assert!(matches!(
        InnerEquiJoinDefinition::try_new(std::iter::empty::<(Expr, Expr)>(), ["value"]),
        Err(InnerEquiJoinDefinitionError::EmptyKeys)
    ));

    let mismatched = InnerEquiJoinDefinition::try_new(
        [(col("id"), col("amount"))],
        ["left_id", "left_label", "right_fk", "right_amount"],
    )
    .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        bind(&mismatched, &[left_schema(), right_schema()])
    else {
        panic!("mismatched Join key types unexpectedly bound")
    };
    assert!(matches!(
        source.downcast_ref::<InnerEquiJoinSchemaError>(),
        Some(InnerEquiJoinSchemaError::KeyTypeMismatch { key: 0, .. })
    ));

    let floats = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Float64,
        false,
    )]));
    let unsupported =
        InnerEquiJoinDefinition::try_new([(col("key"), col("key"))], ["left_key", "right_key"])
            .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        bind(&unsupported, &[Arc::clone(&floats), floats])
    else {
        panic!("floating Join key unexpectedly bound")
    };
    assert!(matches!(
        source.downcast_ref::<InnerEquiJoinSchemaError>(),
        Some(InnerEquiJoinSchemaError::UnsupportedKeyType { key: 0, .. })
    ));

    let wrong_count =
        InnerEquiJoinDefinition::try_new([(col("id"), col("fk"))], ["only_one"]).unwrap();
    assert!(matches!(
        bind(&wrong_count, &[left_schema(), right_schema()]),
        Err(OperationBindError::Rejected { .. })
    ));
    let duplicate_names = InnerEquiJoinDefinition::try_new(
        [(col("id"), col("fk"))],
        ["same", "same", "third", "fourth"],
    )
    .unwrap();
    assert!(matches!(
        bind(&duplicate_names, &[left_schema(), right_schema()]),
        Err(OperationBindError::InvalidOutputSchema { .. })
    ));
}

#[test]
fn both_input_ports_update_relations_and_emit_weighted_matches_in_left_right_order() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let right = right_change(
        vec![Some(1), Some(1), None],
        vec![10, 20, 99],
        vec![3, 1, 1],
    );
    assert!(
        run_claim(&mut operation, &mut transactions, 1, &right)
            .unwrap()
            .is_empty()
    );

    let left = left_change(vec![Some(1), None], vec!["matched", "null"], vec![2, 1]);
    let output = run_claim(&mut operation, &mut transactions, 0, &left).unwrap();
    assert_eq!(
        output_rows(&output),
        [
            (Some(1), "matched".into(), Some(1), 10, 6),
            (Some(1), "matched".into(), Some(1), 20, 2),
        ]
    );

    let retract = right_change(vec![Some(1)], vec![10], vec![-2]);
    let output = run_claim(&mut operation, &mut transactions, 1, &retract).unwrap();
    assert_eq!(
        output_rows(&output),
        [(Some(1), "matched".into(), Some(1), 10, -4)]
    );

    let null_left = left_change(vec![None], vec!["null"], vec![-1]);
    let null_right = right_change(vec![None], vec![99], vec![-1]);
    assert!(
        run_claim(&mut operation, &mut transactions, 0, &null_left)
            .unwrap()
            .is_empty()
    );
    assert!(
        run_claim(&mut operation, &mut transactions, 1, &null_right)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn runtime_rejects_missing_invalid_port_and_exact_schema_drift() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let Err(error) = operation.turn(None) else {
        panic!("inner join accepted a missing input");
    };
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::MissingInput)
    ));

    let input = left_change(vec![Some(1)], vec!["left"], vec![1]);
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 2,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::InvalidInputPort { port: 2 })
    ));

    let drifted = right_change(vec![Some(1)], vec![10], vec![1]);
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &drifted,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::InputSchemaMismatch { port: 0 })
    ));
}

#[test]
fn whole_claim_admission_rejects_negative_prefix_without_partial_state() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let invalid = left_change(vec![Some(7), Some(7)], vec!["same", "same"], vec![1, -2]);
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &invalid,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::NegativeWeight)
    ));

    let retract = left_change(vec![Some(7)], vec!["same"], vec![-1]);
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &retract,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::NegativeWeight)
    ));
}

#[test]
fn rolled_back_emit_page_replays_and_adjusts_the_input_once() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let right = right_change(vec![Some(3)], vec![30], vec![1]);
    run_claim(&mut operation, &mut transactions, 1, &right).unwrap();
    let left = left_change(vec![Some(3)], vec!["left"], vec![1]);

    assert!(matches!(
        rollback_ready(
            &mut operation,
            Some(OperationInput {
                port: 0,
                change: &left
            }),
            &mut transactions
        )
        .unwrap(),
        Action::Complete(Some(_))
    ));

    let output = run_claim(&mut operation, &mut transactions, 0, &left).unwrap();
    assert_eq!(
        output_rows(&output).as_slice(),
        &[(Some(3), "left".into(), Some(3), 30, 1)]
    );
    let retract = left_change(vec![Some(3)], vec!["left"], vec![-1]);
    assert_eq!(
        output_rows(&run_claim(&mut operation, &mut transactions, 0, &retract).unwrap()),
        [(Some(3), "left".into(), Some(3), 30, -1)]
    );
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &retract,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::NegativeWeight)
    ));
}

#[test]
fn state_and_output_weight_overflow_fail_before_emission() {
    {
        let root = TestStore::new();
        let definition = definition();
        let mut store = Store::create(root.path()).unwrap();
        for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
            declaration.create(&mut store, physical).unwrap();
        }
        let left_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> =
            store.open_data(PHYSICAL_DATA[0]).unwrap();
        let mut operation = materialize(
            &definition,
            &[left_schema(), right_schema()],
            &store,
            &PHYSICAL_DATA,
        );
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        let mut rows = left_rows.access(transaction.access()).unwrap();
        let mut partition = rows.partition(&canonical_u64(9)).unwrap();
        let row = canonical_left_row(9, "full");
        partition.adjust(&row, i64::MAX).unwrap();
        partition.adjust(&row, i64::MAX).unwrap();
        partition.adjust(&row, 1).unwrap();
        transaction.commit().unwrap();

        let overflow = left_change(vec![Some(9)], vec!["full"], vec![1]);
        let error = rollback_ready(
            &mut operation,
            Some(OperationInput {
                port: 0,
                change: &overflow,
            }),
            &mut transactions,
        )
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<InnerEquiJoinError>(),
            Some(InnerEquiJoinError::WeightOverflow)
        ));
    }

    {
        let root = TestStore::new();
        let definition = definition();
        let mut store = Store::create(root.path()).unwrap();
        for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
            declaration.create(&mut store, physical).unwrap();
        }
        let right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> =
            store.open_data(PHYSICAL_DATA[1]).unwrap();
        let mut operation = materialize(
            &definition,
            &[left_schema(), right_schema()],
            &store,
            &PHYSICAL_DATA,
        );
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        right_rows
            .access(transaction.access())
            .unwrap()
            .partition(&canonical_u64(4))
            .unwrap()
            .adjust(&canonical_right_row(4, 40), i64::MAX)
            .unwrap();
        transaction.commit().unwrap();

        let overflow = left_change(vec![Some(4)], vec!["left"], vec![2]);
        let error = rollback_ready(
            &mut operation,
            Some(OperationInput {
                port: 0,
                change: &overflow,
            }),
            &mut transactions,
        )
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<InnerEquiJoinError>(),
            Some(InnerEquiJoinError::OutputDifferenceOverflow)
        ));
    }
}

#[test]
fn composite_variable_width_keys_keep_component_boundaries() {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("left_value", DataType::Utf8, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Utf8, false),
        Field::new("y", DataType::Utf8, false),
        Field::new("right_value", DataType::Utf8, false),
    ]));
    let definition = InnerEquiJoinDefinition::try_new(
        [(col("a"), col("x")), (col("b"), col("y"))],
        ["a", "b", "left_value", "x", "y", "right_value"],
    )
    .unwrap();
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
        declaration.create(&mut store, physical).unwrap();
    }
    let mut operation = materialize(
        &definition,
        &[Arc::clone(&left_schema), Arc::clone(&right_schema)],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let right = Change::try_new(
        RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(StringArray::from(vec!["ab", "a"])),
                Arc::new(StringArray::from(vec!["c", "bc"])),
                Arc::new(StringArray::from(vec!["collision", "match"])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, 1]),
    )
    .unwrap();
    run_claim(&mut operation, &mut transactions, 1, &right).unwrap();
    let left = Change::try_new(
        RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["bc"])),
                Arc::new(StringArray::from(vec!["left"])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let outputs = run_claim(&mut operation, &mut transactions, 0, &left).unwrap();
    assert_eq!(outputs.iter().map(Change::num_rows).sum::<usize>(), 1);
    let value = outputs[0]
        .records()
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(value.value(0), "match");
}

#[test]
fn per_port_event_trace_is_stable_across_input_rebatching() {
    let events = [
        (Some(1), "same", 2),
        (Some(1), "same", -1),
        (Some(2), "other", 1),
        (Some(1), "same", -1),
        (Some(1), "same", 1),
    ];
    let trace = |batches: &[usize]| {
        let root = TestStore::new();
        let (mut operation, mut transactions) = create_operation(&root);
        let right = right_change(vec![Some(1), Some(2)], vec![10, 20], vec![3, 2]);
        run_claim(&mut operation, &mut transactions, 1, &right).unwrap();
        let mut output = Vec::new();
        let mut start = 0;
        for &length in batches {
            let batch = &events[start..start + length];
            let input = left_change(
                batch.iter().map(|event| event.0).collect(),
                batch.iter().map(|event| event.1).collect(),
                batch.iter().map(|event| event.2).collect(),
            );
            output.extend(output_rows(
                &run_claim(&mut operation, &mut transactions, 0, &input).unwrap(),
            ));
            start += length;
        }
        output
    };

    let expected = trace(&[events.len()]);
    assert_eq!(trace(&[1, 2, 2]), expected);
    assert_eq!(trace(&[1, 1, 1, 1, 1]), expected);
}

#[test]
fn sparse_claims_advance_many_rows_per_transaction() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let rows = 300_u64;
    let input = right_change(
        (0..rows).map(Some).collect(),
        (0..rows)
            .map(|value| i64::try_from(value).unwrap())
            .collect(),
        vec![1; usize::try_from(rows).unwrap()],
    );
    let (outputs, turns) =
        run_claim_with_turns(&mut operation, &mut transactions, 1, &input).unwrap();
    assert!(outputs.is_empty());
    assert_eq!(turns, 3);
}

#[test]
fn one_to_one_claims_aggregate_many_output_rows_per_transaction() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let rows = 300_u64;
    let right = right_change(
        (0..rows).map(Some).collect(),
        (0..rows)
            .map(|value| i64::try_from(value).unwrap())
            .collect(),
        vec![1; usize::try_from(rows).unwrap()],
    );
    run_claim(&mut operation, &mut transactions, 1, &right).unwrap();

    let left = left_change(
        (0..rows).map(Some).collect(),
        vec!["left"; usize::try_from(rows).unwrap()],
        vec![1; usize::try_from(rows).unwrap()],
    );
    let (outputs, turns) =
        run_claim_with_turns(&mut operation, &mut transactions, 0, &left).unwrap();

    assert_eq!(turns, 3);
    assert_eq!(
        outputs.iter().map(Change::num_rows).collect::<Vec<_>>(),
        [212, 88]
    );
    assert_eq!(
        output_rows(&outputs)
            .into_iter()
            .map(|(left_id, _, right_id, _, _)| (left_id, right_id))
            .collect::<Vec<_>>(),
        (0..rows)
            .map(|value| (Some(value), Some(value)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn sparse_large_rows_respect_the_turn_byte_budget() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root);
    let large = "x".repeat(2_200_000);
    let input = left_change(
        vec![Some(1), Some(2), Some(3)],
        vec![large.as_str(); 3],
        vec![1; 3],
    );

    let (outputs, turns) =
        run_claim_with_turns(&mut operation, &mut transactions, 0, &input).unwrap();

    assert!(outputs.is_empty());
    assert_eq!(turns, 6);
}

#[test]
fn continuation_reopens_after_probe_and_emit_pages_without_duplicates() {
    let root = TestStore::new();
    let definition = definition();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
        declaration.create(&mut store, physical).unwrap();
    }
    let right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> =
        store.open_data(PHYSICAL_DATA[1]).unwrap();
    let mut operation = materialize(
        &definition,
        &[left_schema(), right_schema()],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut rows = right_rows.access(transaction.access()).unwrap();
    let mut partition = rows.partition(&canonical_u64(7)).unwrap();
    for amount in 0..257_i64 {
        partition
            .adjust(&canonical_right_row(7, amount), 1)
            .unwrap();
    }
    transaction.commit().unwrap();

    let input = left_change(vec![Some(7)], vec!["left"], vec![1]);
    assert!(matches!(
        commit_ready(
            &mut operation,
            Some(OperationInput {
                port: 0,
                change: &input
            }),
            &mut transactions
        )
        .unwrap(),
        Action::Commit(None)
    ));
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let mut operation = materialize(
        &definition,
        &[left_schema(), right_schema()],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let Action::Commit(Some(first_output)) = commit_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap() else {
        panic!("reopened Join did not commit its first Emit page");
    };
    assert_eq!(first_output.num_rows(), 255);
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let mut operation = materialize(
        &definition,
        &[left_schema(), right_schema()],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let mut outputs = vec![first_output];
    outputs.extend(run_claim(&mut operation, &mut transactions, 0, &input).unwrap());
    assert_eq!(
        outputs.iter().map(Change::num_rows).collect::<Vec<_>>(),
        [255, 2]
    );
    let rows = output_rows(&outputs);
    assert_eq!(rows.len(), 257);
    assert_eq!(rows.first().unwrap().3, 0);
    assert_eq!(rows.last().unwrap().3, 256);
    assert!(rows.iter().all(|row| row.4 == 1));

    let retract = left_change(vec![Some(7)], vec!["left"], vec![-1]);
    let retractions = run_claim(&mut operation, &mut transactions, 0, &retract).unwrap();
    assert_eq!(retractions.iter().map(Change::num_rows).sum::<usize>(), 257);
    assert!(output_rows(&retractions).iter().all(|row| row.4 == -1));
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &retract,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<InnerEquiJoinError>(),
        Some(InnerEquiJoinError::NegativeWeight)
    ));
}

#[test]
fn large_driving_rows_reduce_match_pages_to_bound_output_amplification() {
    let left = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let right = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt64, false),
    ]));
    let definition = InnerEquiJoinDefinition::try_new(
        [(col("id"), col("fk"))],
        ["left_id", "payload", "right_fk", "ordinal"],
    )
    .unwrap();
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
        declaration.create(&mut store, physical).unwrap();
    }
    let mut operation = materialize(
        &definition,
        &[Arc::clone(&left), Arc::clone(&right)],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let right_input = Change::try_new(
        RecordBatch::try_new(
            right,
            vec![
                Arc::new(UInt64Array::from(vec![1; 5])),
                Arc::new(UInt64Array::from((0..5_u64).collect::<Vec<_>>())),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1; 5]),
    )
    .unwrap();
    assert!(
        run_claim(&mut operation, &mut transactions, 1, &right_input)
            .unwrap()
            .is_empty()
    );

    let payload = "x".repeat(2_200_000);
    let left_input = Change::try_new(
        RecordBatch::try_new(
            left,
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![payload.as_str()])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let outputs = run_claim(&mut operation, &mut transactions, 0, &left_input).unwrap();
    assert_eq!(
        outputs.iter().map(Change::num_rows).collect::<Vec<_>>(),
        [1; 5]
    );
}

#[test]
fn an_oversized_scan_item_waits_for_an_empty_turn_budget() {
    let left = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt64, false),
    ]));
    let right = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let definition = InnerEquiJoinDefinition::try_new(
        [(col("id"), col("fk"))],
        ["left_id", "ordinal", "right_fk", "payload"],
    )
    .unwrap();
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition.data().iter().zip(PHYSICAL_DATA) {
        declaration.create(&mut store, physical).unwrap();
    }
    let mut operation = materialize(
        &definition,
        &[Arc::clone(&left), Arc::clone(&right)],
        &store,
        &PHYSICAL_DATA,
    );
    let mut transactions = store.into_transactions();
    let large = "x".repeat(2_200_000);
    let right_input = Change::try_new(
        RecordBatch::try_new(
            right,
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["small", large.as_str()])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, 1]),
    )
    .unwrap();
    run_claim(&mut operation, &mut transactions, 1, &right_input).unwrap();

    let left_input = Change::try_new(
        RecordBatch::try_new(
            left,
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])),
                Arc::new(UInt64Array::from(vec![10, 20])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1, 1]),
    )
    .unwrap();
    let (outputs, turns) =
        run_claim_with_turns(&mut operation, &mut transactions, 0, &left_input).unwrap();

    assert_eq!(turns, 3);
    assert_eq!(
        outputs.iter().map(Change::num_rows).collect::<Vec<_>>(),
        [1, 1]
    );
    assert_eq!(
        outputs[0]
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "small"
    );
    assert_eq!(
        outputs[1]
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        large
    );
}

fn canonical_u64(value: u64) -> Vec<u8> {
    let mut encoded = vec![1];
    encoded.extend_from_slice(&value.to_be_bytes());
    encoded
}

fn canonical_right_row(key: u64, amount: i64) -> Vec<u8> {
    let mut encoded = canonical_u64(key);
    encoded.push(1);
    encoded.extend_from_slice(&amount.to_be_bytes());
    encoded
}

fn canonical_left_row(key: u64, label: &str) -> Vec<u8> {
    let mut encoded = canonical_u64(key);
    encoded.push(1);
    encoded.extend_from_slice(&u64::try_from(label.len()).unwrap().to_be_bytes());
    encoded.extend_from_slice(label.as_bytes());
    encoded
}
