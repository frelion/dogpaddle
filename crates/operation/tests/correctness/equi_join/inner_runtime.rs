use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    Expr, OperationBindError, OperationDefinition, RuntimeResource, col,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn,
        transform::{
            EquiJoinDefinition, EquiJoinDefinitionError, EquiJoinError, EquiJoinKind,
            EquiJoinSchemaError,
        },
    },
};
use dogpaddle_store::{PartitionedMultiset, Store, StoreSetup, Transactions};

use crate::support::{TestStore, commit_ready, construct_checked, rollback_ready};

const OPERATION_PREFIX: &str = "operation";
const BASE_RESOURCES: [&str; 3] = [
    "equi_join.left_rows",
    "equi_join.right_rows",
    "equi_join.continuation",
];
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

fn definition() -> EquiJoinDefinition {
    EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("fk"))],
        ["left_id", "left_label", "right_fk", "right_amount"],
        None,
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

fn construct_operation(root: &TestStore) -> (Operation, Transactions) {
    construct_operation_for_schemas(root, &definition(), &[left_schema(), right_schema()])
}

fn construct_operation_for_schemas(
    root: &TestStore,
    definition: &dyn OperationDefinition,
    schemas: &[SchemaRef],
) -> (Operation, Transactions) {
    let mut setup = StoreSetup::new();
    let (operation, _) = definition
        .construct(
            schemas,
            &mut setup.data_scope(),
            OPERATION_PREFIX,
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    (operation, transactions)
}

fn reopen_join(
    store: &Store,
    definition: &dyn OperationDefinition,
    schemas: &[SchemaRef],
) -> Operation {
    definition
        .construct(
            schemas,
            &mut store.data_scope(),
            OPERATION_PREFIX,
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0
}

fn resource_name(logical: &str) -> String {
    format!("{OPERATION_PREFIX}/{logical}")
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
fn inner_binding_preserves_field_metadata_and_uses_exact_typed_layout() {
    let definition = definition();
    let binding = construct_checked(&definition, &[left_schema(), right_schema()]).unwrap();
    let output = binding.as_ref().unwrap();
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

    assert_eq!(
        BASE_RESOURCES,
        [
            "equi_join.left_rows",
            "equi_join.right_rows",
            "equi_join.continuation",
        ]
    );
}

#[test]
fn definition_rejects_empty_mismatched_and_unsupported_keys_and_bad_names() {
    assert!(EquiJoinDefinition::supports_key_type(&DataType::UInt64));
    assert!(!EquiJoinDefinition::supports_key_type(&DataType::Float64));

    assert!(matches!(
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            std::iter::empty::<(Expr, Expr)>(),
            ["value"],
            None,
        ),
        Err(EquiJoinDefinitionError::EmptyKeys)
    ));

    let mismatched = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("amount"))],
        ["left_id", "left_label", "right_fk", "right_amount"],
        None,
    )
    .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&mismatched, &[left_schema(), right_schema()])
    else {
        panic!("mismatched Join key types unexpectedly bound")
    };
    assert!(matches!(
        source.downcast_ref::<EquiJoinSchemaError>(),
        Some(EquiJoinSchemaError::KeyTypeMismatch { key: 0, .. })
    ));

    let floats = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Float64,
        false,
    )]));
    let unsupported = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("key"), col("key"))],
        ["left_key", "right_key"],
        None,
    )
    .unwrap();
    let Err(OperationBindError::Rejected { source }) =
        construct_checked(&unsupported, &[Arc::clone(&floats), floats])
    else {
        panic!("floating Join key unexpectedly bound")
    };
    assert!(matches!(
        source.downcast_ref::<EquiJoinSchemaError>(),
        Some(EquiJoinSchemaError::UnsupportedKeyType { key: 0, .. })
    ));

    let wrong_count = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("fk"))],
        ["only_one"],
        None,
    )
    .unwrap();
    assert!(matches!(
        construct_checked(&wrong_count, &[left_schema(), right_schema()]),
        Err(OperationBindError::Rejected { .. })
    ));
    let duplicate_names = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("fk"))],
        ["same", "same", "third", "fourth"],
        None,
    )
    .unwrap();
    assert!(matches!(
        construct_checked(&duplicate_names, &[left_schema(), right_schema()]),
        Err(OperationBindError::InvalidOutputSchema { .. })
    ));
}

#[test]
fn both_input_ports_update_relations_and_emit_weighted_matches_in_left_right_order() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_operation(&root);
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
fn runtime_idles_without_input_and_rejects_invalid_port_and_exact_schema_drift() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_operation(&root);
    assert!(matches!(operation.turn(None).unwrap(), Turn::Idle));

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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::InvalidInputPort { port: 2 })
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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::InputSchemaMismatch { port: 0 })
    ));
}

#[test]
fn whole_claim_admission_rejects_negative_prefix_without_partial_state() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_operation(&root);
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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::NegativeWeight)
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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::NegativeWeight)
    ));
}

#[test]
fn rolled_back_emit_page_replays_and_adjusts_the_input_once() {
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_operation(&root);
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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::NegativeWeight)
    ));
}

#[test]
fn state_and_output_weight_overflow_fail_before_emission() {
    {
        let root = TestStore::new();
        let definition = definition();
        let (operation, transactions) = construct_operation(&root);
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let left_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> = store
            .open_data(&resource_name("equi_join.left_rows"))
            .unwrap();
        let mut operation = reopen_join(&store, &definition, &[left_schema(), right_schema()]);
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
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::WeightOverflow)
        ));
    }

    {
        let root = TestStore::new();
        let definition = definition();
        let (operation, transactions) = construct_operation(&root);
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> = store
            .open_data(&resource_name("equi_join.right_rows"))
            .unwrap();
        let mut operation = reopen_join(&store, &definition, &[left_schema(), right_schema()]);
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
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::OutputDifferenceOverflow)
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
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("a"), col("x")), (col("b"), col("y"))],
        ["a", "b", "left_value", "x", "y", "right_value"],
        None,
    )
    .unwrap();
    let root = TestStore::new();
    let schemas = [Arc::clone(&left_schema), Arc::clone(&right_schema)];
    let (mut operation, mut transactions) =
        construct_operation_for_schemas(&root, &definition, &schemas);
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
        let (mut operation, mut transactions) = construct_operation(&root);
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
    let (mut operation, mut transactions) = construct_operation(&root);
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
    let (mut operation, mut transactions) = construct_operation(&root);
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
    let (mut operation, mut transactions) = construct_operation(&root);
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
fn residual_wide_candidates_are_split_by_scalar_working_set() {
    const VALUE_FIELDS: usize = 128;
    const RIGHT_ROWS: usize = 129;

    let schema = Arc::new(Schema::new(
        std::iter::once(Field::new("key", DataType::UInt64, false))
            .chain(
                (0..VALUE_FIELDS)
                    .map(|field| Field::new(format!("value_{field}"), DataType::Int64, false)),
            )
            .collect::<Vec<_>>(),
    ));
    let output_names = (0..schema.fields().len())
        .map(|field| format!("left_{field}"))
        .chain((0..schema.fields().len()).map(|field| format!("right_{field}")))
        .collect::<Vec<_>>();
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("key"), col("key"))],
        output_names,
        Some(col("left.value_0").lt(col("right.value_0"))),
    )
    .unwrap();
    let root = TestStore::new();
    let schemas = [Arc::clone(&schema), Arc::clone(&schema)];
    let (mut operation, mut transactions) =
        construct_operation_for_schemas(&root, &definition, &schemas);

    let right_values = (1..=RIGHT_ROWS)
        .map(|value| i64::try_from(value).unwrap())
        .collect::<Vec<_>>();
    let mut right_columns = Vec::<ArrayRef>::with_capacity(VALUE_FIELDS + 1);
    right_columns.push(Arc::new(UInt64Array::from(vec![7; RIGHT_ROWS])));
    right_columns.push(Arc::new(Int64Array::from(right_values)));
    right_columns.extend(
        (1..VALUE_FIELDS).map(|_| Arc::new(Int64Array::from(vec![0; RIGHT_ROWS])) as ArrayRef),
    );
    let right = Change::try_new(
        RecordBatch::try_new(Arc::clone(&schema), right_columns).unwrap(),
        Int64Array::from(vec![1; RIGHT_ROWS]),
    )
    .unwrap();
    run_claim(&mut operation, &mut transactions, 1, &right).unwrap();

    let mut left_columns = Vec::<ArrayRef>::with_capacity(VALUE_FIELDS + 1);
    left_columns.push(Arc::new(UInt64Array::from(vec![7])));
    left_columns.extend((0..VALUE_FIELDS).map(|_| Arc::new(Int64Array::from(vec![0])) as ArrayRef));
    let left = Change::try_new(
        RecordBatch::try_new(schema, left_columns).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let (outputs, turns) =
        run_claim_with_turns(&mut operation, &mut transactions, 0, &left).unwrap();

    assert_eq!(
        outputs.iter().map(Change::num_rows).sum::<usize>(),
        RIGHT_ROWS
    );
    assert!(turns >= 5);
    assert!(outputs.iter().all(|output| output.num_rows() <= 63));
}

#[test]
fn continuation_reopens_after_probe_and_emit_pages_without_duplicates() {
    let root = TestStore::new();
    let definition = definition();
    let (operation, transactions) = construct_operation(&root);
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>> = store
        .open_data(&resource_name("equi_join.right_rows"))
        .unwrap();
    let mut operation = reopen_join(&store, &definition, &[left_schema(), right_schema()]);
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
    let mut operation = reopen_join(&store, &definition, &[left_schema(), right_schema()]);
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
    let mut operation = reopen_join(&store, &definition, &[left_schema(), right_schema()]);
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
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::NegativeWeight)
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
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("fk"))],
        ["left_id", "payload", "right_fk", "ordinal"],
        None,
    )
    .unwrap();
    let root = TestStore::new();
    let schemas = [Arc::clone(&left), Arc::clone(&right)];
    let (mut operation, mut transactions) =
        construct_operation_for_schemas(&root, &definition, &schemas);
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
fn semi_and_anti_presence_budget_charges_the_unemitted_driving_row_once_per_phase() {
    const MATCHES: usize = 32;
    let left = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value", DataType::UInt64, false),
    ]));
    let right = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let left_input = Change::try_new(
        RecordBatch::try_new(
            Arc::clone(&left),
            vec![
                Arc::new(UInt64Array::from(vec![7; MATCHES])),
                Arc::new(UInt64Array::from(
                    (0..u64::try_from(MATCHES).unwrap()).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1; MATCHES]),
    )
    .unwrap();
    // This row exceeds the half-turn scan budget. Semi/Anti output only the
    // matched left rows, so its payload is charged once in Probe and once in
    // Emit, rather than once for each match.
    let payload = "x".repeat(2_200_000);
    let right_change = |difference| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&right),
                vec![
                    Arc::new(UInt64Array::from(vec![7])),
                    Arc::new(StringArray::from(vec![payload.as_str()])),
                ],
            )
            .unwrap(),
            Int64Array::from(vec![difference]),
        )
        .unwrap()
    };
    let insert = right_change(1);
    let retract = right_change(-1);

    for (kind, insert_difference, retract_difference) in [
        (EquiJoinKind::LeftSemi, 1, -1),
        (EquiJoinKind::LeftAnti, -1, 1),
    ] {
        let definition = EquiJoinDefinition::try_new(
            kind,
            [(col("key"), col("key"))],
            ["left_key", "left_value"],
            None,
        )
        .unwrap();
        let root = TestStore::new();
        let schemas = [Arc::clone(&left), Arc::clone(&right)];
        let (mut operation, mut transactions) =
            construct_operation_for_schemas(&root, &definition, &schemas);
        run_claim(&mut operation, &mut transactions, 0, &left_input).unwrap();

        for (input, expected_difference) in
            [(&insert, insert_difference), (&retract, retract_difference)]
        {
            let (outputs, turns) =
                run_claim_with_turns(&mut operation, &mut transactions, 1, input).unwrap();
            assert_eq!(
                turns, 2,
                "{kind:?} did not charge the right driving row once per phase"
            );
            assert_eq!(outputs.len(), 1);
            assert_eq!(outputs[0].num_rows(), MATCHES);
            assert!(
                outputs[0]
                    .diffs()
                    .values()
                    .iter()
                    .all(|difference| *difference == expected_difference)
            );
        }
    }
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
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("id"), col("fk"))],
        ["left_id", "ordinal", "right_fk", "payload"],
        None,
    )
    .unwrap();
    let root = TestStore::new();
    let schemas = [Arc::clone(&left), Arc::clone(&right)];
    let (mut operation, mut transactions) =
        construct_operation_for_schemas(&root, &definition, &schemas);
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
