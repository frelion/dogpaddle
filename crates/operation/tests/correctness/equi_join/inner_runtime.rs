use arrow_schema::{DataType, Field, Schema};
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col,
    operation::transform::{EquiJoinDefinition, EquiJoinKind, EquiJoinSchemaError},
};
use dogpaddle_store::StoreSetup;
use std::sync::Arc;

#[test]
fn binding_rejects_key_type_drift_and_non_boolean_residual() {
    let left = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::UInt64,
        false,
    )]));
    let right = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("key"), col("key"))],
        ["left", "right"],
        None,
    )
    .unwrap();
    let error = OperationDefinition::from(definition).construct(
        &[Arc::clone(&left), right],
        &mut StoreSetup::new().data_scope(),
        RuntimeResource::none(),
    );
    assert!(error.is_err());
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("key"), col("key"))],
        ["left", "right"],
        Some(col("left.key")),
    )
    .unwrap();
    let dogpaddle_operation::OperationSetupError::Schema { source } =
        OperationDefinition::from(definition)
            .construct(
                &[Arc::clone(&left), left],
                &mut StoreSetup::new().data_scope(),
                RuntimeResource::none(),
            )
            .err()
            .unwrap()
    else {
        panic!("expected schema rejection")
    };
    assert!(matches!(
        source.downcast_ref::<EquiJoinSchemaError>(),
        Some(EquiJoinSchemaError::ResidualType { .. })
    ));
}

#[test]
fn continued_candidate_validation_must_fit_the_shared_byte_budget() {
    use dogpaddle_operation::operation::{BudgetExceeded, OperationInput, Progress, StepBudget};
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let definition = EquiJoinDefinition::try_new(
        EquiJoinKind::Inner,
        [(col("key"), col("key"))],
        [
            "left_key",
            "left_id",
            "left_payload",
            "right_key",
            "right_id",
            "right_payload",
        ],
        None,
    )
    .unwrap();
    let (_root, operation, _left_rows, mut transactions) = runtime_fixture(&schema, definition);
    let event = |id, payload: &str| payload_event(&schema, id, payload);
    for right in [event(0, &"x".repeat(8 * 1024)), event(1, "a")] {
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port: 1,
                    change: &right,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 4 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(step.progress, Progress::Done);
        transaction.commit().unwrap();
    }
    let left = event(2, "left");
    let resume = {
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port: 0,
                    change: &left,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 4 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(step.output.unwrap().num_rows(), 1);
        let Progress::More(resume) = step.progress else {
            panic!("the second candidate remains");
        };
        transaction.commit().unwrap();
        resume
    };
    for _ in 0..2 {
        let transaction = transactions.begin();
        let error = operation
            .step(
                OperationInput {
                    port: 0,
                    change: &left,
                },
                &resume,
                transaction.access(),
                &mut StepBudget::new(1, 2 * 1024),
            )
            .unwrap_err();
        assert!(error.is::<BudgetExceeded>());
    }
    let transaction = transactions.begin();
    let step = operation
        .step(
            OperationInput {
                port: 0,
                change: &left,
            },
            &resume,
            transaction.access(),
            &mut StepBudget::new(1, 4 * 1024 * 1024),
        )
        .unwrap();
    assert_eq!(step.progress, Progress::Done);
    assert_eq!(step.output.unwrap().num_rows(), 1);
    transaction.commit().unwrap();
}

#[test]
fn nested_candidate_decode_is_charged_before_pure_or_residual_output_allocation() {
    use arrow_array::ListArray;
    use dogpaddle_operation::{
        lit,
        operation::{OperationInput, Progress, StepBudget, transform::EquiJoinError},
    };
    for residual in [None, Some(lit(true))] {
        let item = Arc::new(Field::new("item", DataType::Null, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("items", DataType::List(Arc::clone(&item)), false),
        ]));
        let definition = EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("key"), col("key"))],
            ["left_key", "left_items", "right_key", "right_items"],
            residual,
        )
        .unwrap();
        let (_root, operation, left_rows, mut transactions) = runtime_fixture(&schema, definition);
        let event = |length| list_event(&schema, &item, length);
        let right = event(128 * 1024);
        {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 1,
                        change: &right,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 4 * 1024 * 1024),
                )
                .unwrap();
            assert_eq!(step.progress, Progress::Done);
            assert!(step.output.is_none());
            transaction.commit().unwrap();
        }
        let left = event(0);
        {
            let transaction = transactions.begin();
            let error = operation
                .step(
                    OperationInput {
                        port: 0,
                        change: &left,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 4 * 1024 * 1024),
                )
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EquiJoinError>(),
                Some(EquiJoinError::Budget(_))
            ));
            assert!(
                left_rows
                    .access(transaction.access())
                    .unwrap()
                    .scan(
                        ..,
                        dogpaddle_store::ScanDirection::Ascending,
                        None,
                        dogpaddle_store::ScanLimit::new(1, 1024).unwrap()
                    )
                    .unwrap()
                    .entries
                    .is_empty()
            );
        }
        {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 0,
                        change: &left,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 64 * 1024 * 1024),
                )
                .unwrap();
            assert_eq!(step.progress, Progress::Done);
            let output = step.output.unwrap();
            assert_eq!(output.num_rows(), 1);
            let list = output
                .records()
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.value_length(0), 128 * 1024);
            transaction.commit().unwrap();
        }
    }
}

fn runtime_fixture(
    schema: &arrow_schema::SchemaRef,
    definition: EquiJoinDefinition,
) -> (
    tempfile::TempDir,
    dogpaddle_operation::operation::Operation,
    dogpaddle_store::OrderedMap<Vec<u8>, Vec<u8>>,
    dogpaddle_store::Transactions,
) {
    let root = tempfile::tempdir().unwrap();
    let definition = OperationDefinition::from(definition);
    let mut setup = StoreSetup::new();
    let operation = definition
        .construct(
            &[Arc::clone(schema), Arc::clone(schema)],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0;
    let transactions = setup.commit(root.path().join("store"), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = dogpaddle_store::Store::open(root.path().join("store")).unwrap();
    let operation = definition
        .construct(
            &[Arc::clone(schema), Arc::clone(schema)],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0;
    let left_rows = store.open_data("operation/equi_join.left_rows").unwrap();
    (root, operation, left_rows, store.into_transactions())
}
fn list_event(
    schema: &arrow_schema::SchemaRef,
    item: &Arc<Field>,
    length: i32,
) -> dogpaddle_change::Change {
    use arrow_array::{Int64Array, ListArray, NullArray, RecordBatch, UInt64Array};
    use arrow_buffer::OffsetBuffer;
    dogpaddle_change::Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(ListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::new(vec![0, length].into()),
                    Arc::new(NullArray::new(usize::try_from(length).unwrap())),
                    None,
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap()
}

fn payload_event(
    schema: &arrow_schema::SchemaRef,
    id: i64,
    payload: &str,
) -> dogpaddle_change::Change {
    use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
    dogpaddle_change::Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![payload])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "One fixture compares refused reads and complete replay for pure and residual joins."
)]
fn rejected_wide_output_still_charges_pure_and_residual_candidate_reads() {
    use arrow_array::{ArrayRef, Int64Array, NullArray, RecordBatch, StringArray, UInt64Array};
    use dogpaddle_operation::{
        lit,
        operation::{BudgetExceeded, OperationInput, Progress, StepBudget},
    };
    let mut fields = vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ];
    fields.extend((0..100).map(|index| Field::new(format!("n{index}"), DataType::Null, true)));
    let schema = Arc::new(Schema::new(fields));
    let event = |id, payload: &str| {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload])),
        ];
        columns.extend((0..100).map(|_| Arc::new(NullArray::new(1)) as ArrayRef));
        dogpaddle_change::Change::try_new(
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap(),
            Int64Array::from(vec![1]),
        )
        .unwrap()
    };
    for residual in [None, Some(lit(true))] {
        let definition = EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("key"), col("key"))],
            (0..206).map(|index| format!("out{index}")),
            residual,
        )
        .unwrap();
        let (_root, operation, _left_rows, mut transactions) = runtime_fixture(&schema, definition);
        for id in 0..64 {
            let right = event(id, &"x".repeat(1800));
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 1,
                        change: &right,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 4 * 1024 * 1024),
                )
                .unwrap();
            assert_eq!(step.progress, Progress::Done);
            assert!(step.output.is_none());
            transaction.commit().unwrap();
        }
        let left = event(1000, "");
        for _ in 0..2 {
            let transaction = transactions.begin();
            let mut budget = StepBudget::new(256, 80 * 1024);
            let error = operation
                .step(
                    OperationInput {
                        port: 0,
                        change: &left,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut budget,
                )
                .unwrap_err();
            assert!(error.is::<BudgetExceeded>());
            // At least sixteen 1800-byte payloads fit the encoded scan page,
            // even though their 206-column output cannot fit the remaining budget.
            assert!(80 * 1024 - budget.remaining_bytes() >= 16 * 1800);
        }
        let mut resume = operation.initial_resume();
        let mut ids = Vec::new();
        loop {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 0,
                        change: &left,
                    },
                    &resume,
                    transaction.access(),
                    &mut StepBudget::new(1, 4 * 1024 * 1024),
                )
                .unwrap();
            let output = step.output.unwrap();
            assert_eq!(output.num_rows(), 1);
            assert_eq!(output.diffs().value(0), 1);
            ids.push(
                output
                    .records()
                    .column(104)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
            );
            transaction.commit().unwrap();
            match step.progress {
                Progress::Done => break,
                Progress::More(next) => resume = next,
            }
        }
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }
}
