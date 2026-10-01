use std::{borrow::Cow, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col,
    operation::{
        Operation, OperationInput, Progress, Resume, StepBudget,
        transform::{EquiJoinDefinition, EquiJoinKind},
    },
};
use dogpaddle_store::{Store, StoreSetup, StoreValue};

const KEY_BYTES: usize = 100;
const KEY_REPETITIONS: usize = 4096;
const PAGE_BYTES: usize = 4 * 1024 * 1024;

fn event(schema: &SchemaRef, value: i64) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["k".repeat(KEY_BYTES)])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap()
}

fn open_operation(
    store: &Store,
    definition: &OperationDefinition,
    schema: &SchemaRef,
) -> Operation {
    definition
        .construct(
            &[Arc::clone(schema), Arc::clone(schema)],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "One workload proves narrow rows with wide equality keys fit minimum pages across rollback and reopen."
)]
fn narrow_rows_with_wide_equality_complete_minimum_pages_after_reopen() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let definition = OperationDefinition::from(
        EquiJoinDefinition::try_new(
            EquiJoinKind::FullOuter,
            (0..KEY_REPETITIONS).map(|_| (col("key"), col("key"))),
            ["left_key", "left_value", "right_key", "right_value"],
            Some(col("left.value").lt(col("right.value"))),
        )
        .unwrap(),
    );
    // Each exact row has just one short string and one integer. Repeated
    // equality expressions make its index partition exceed 400 KiB instead.
    let definition = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
        &serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition).unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut setup = StoreSetup::new();
    let mut operation = definition
        .construct(
            &[Arc::clone(&schema), Arc::clone(&schema)],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0;
    let mut transactions = setup.commit(&path, |_| Ok(())).unwrap();
    for value in [0, 1] {
        let left = event(&schema, value);
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port: 0,
                    change: &left,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, PAGE_BYTES),
            )
            .unwrap();
        assert_eq!(step.progress, Progress::Done);
        let output = step.output.unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.diffs().values(), &[1]);
        assert!(output.records().column(3).is_null(0));
        transaction.commit().unwrap();
    }
    let right = event(&schema, 2);
    let mut resume = operation.initial_resume();
    let mut actual = Vec::new();
    for page in 0..2 {
        let rolled_back = {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 1,
                        change: &right,
                    },
                    &resume,
                    transaction.access(),
                    &mut StepBudget::new(1, PAGE_BYTES),
                )
                .unwrap();
            (step.progress, step.output.unwrap())
        };
        drop((operation, transactions));
        let store = Store::open(&path).unwrap();
        operation = open_operation(&store, &definition, &schema);
        transactions = store.into_transactions();
        let transaction = transactions.begin();
        let mut budget = StepBudget::new(1, PAGE_BYTES);
        let step = operation
            .step(
                OperationInput {
                    port: 1,
                    change: &right,
                },
                &resume,
                transaction.access(),
                &mut budget,
            )
            .unwrap();
        assert!(budget.remaining_bytes() > 0);
        assert_eq!(step.progress, rolled_back.0);
        let output = step.output.unwrap();
        assert_eq!(output.records(), rolled_back.1.records());
        assert_eq!(output.diffs(), rolled_back.1.diffs());
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.diffs().values(), &[-1, 1]);
        let left = output
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = output
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(left.values(), &[i64::from(page); 2]);
        assert!(right.is_null(0));
        assert_eq!(right.value(1), 2);
        actual.push(left.value(1));
        match step.progress {
            Progress::More(next) => {
                assert_eq!(page, 0);
                let encoded = next.encode_value().unwrap();
                assert!(encoded.as_ref().len() < 1024);
                resume = Resume::decode_value(Cow::Borrowed(encoded.as_ref())).unwrap();
            }
            Progress::Done => assert_eq!(page, 1),
        }
        transaction.commit().unwrap();
        // Rebuild even after a committed page: no running instance retains
        // the input admission or partial support-count progress.
        drop((operation, transactions));
        let store = Store::open(&path).unwrap();
        operation = open_operation(&store, &definition, &schema);
        transactions = store.into_transactions();
    }
    assert_eq!(actual, [0, 1]);
}
