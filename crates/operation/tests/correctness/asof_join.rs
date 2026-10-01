use crate::support::{TestStore, decode_hex};
use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, OperationKind, RuntimeResource, col,
    operation::{
        Operation, OperationError, OperationInput, Progress, Resume, Step, StepBudget,
        transform::{
            AsOfDirection, AsOfEqualityKey, AsOfJoinDefinition, AsOfJoinError, AsOfOrderKey,
        },
    },
};
use dogpaddle_store::{
    Cell, OrderedMap, ScanDirection, ScanLimit, Store, StoreSetup, Transactions,
};
use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};

type Row = (Option<u64>, Option<i64>, i64);
type ResultRow = (Row, Option<Row>);
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::UInt64, true),
        Field::new("at", DataType::Int64, true),
        Field::new("id", DataType::Int64, false),
    ]))
}
fn definition(direction: AsOfDirection) -> AsOfJoinDefinition {
    AsOfJoinDefinition::try_new(
        direction,
        [AsOfEqualityKey::new(col("group"), col("group"))],
        AsOfOrderKey::new(col("at"), col("at")),
        [
            "left_group",
            "left_at",
            "left_id",
            "right_group",
            "right_at",
            "right_id",
        ],
    )
    .unwrap()
}
fn change(events: &[(Row, i64)]) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from(
                    events.iter().map(|(row, _)| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    events.iter().map(|(row, _)| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    events.iter().map(|(row, _)| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(events.iter().map(|(_, diff)| *diff).collect::<Vec<_>>()),
    )
    .unwrap()
}
fn output(change: &Change) -> Vec<(ResultRow, i64)> {
    let records = change.records();
    let uint = |column, index| {
        let array = records
            .column(column)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (!array.is_null(index)).then(|| array.value(index))
    };
    let int = |column, index| {
        let array = records
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (!array.is_null(index)).then(|| array.value(index))
    };
    (0..change.num_rows())
        .map(|index| {
            let left = (uint(0, index), int(1, index), int(2, index).unwrap());
            let right = int(5, index).map(|id| (uint(3, index), int(4, index), id));
            ((left, right), change.diffs().value(index))
        })
        .collect()
}
fn adjust<K: Ord>(bag: &mut BTreeMap<K, i64>, key: K, diff: i64) {
    let next = bag.get(&key).copied().unwrap_or(0) + diff;
    assert!(next >= 0);
    if next == 0 {
        bag.remove(&key);
    } else {
        bag.insert(key, next);
    }
}
fn oracle(
    left: &BTreeMap<Row, i64>,
    right: &BTreeMap<Row, i64>,
    direction: AsOfDirection,
) -> BTreeMap<ResultRow, i64> {
    left.iter()
        .map(|(&left, &weight)| {
            let selected = if left.0.is_none() || left.1.is_none() {
                None
            } else {
                let eligible = right.keys().filter(|right| {
                    right.0 == left.0
                        && right.1.is_some()
                        && match direction {
                            AsOfDirection::Backward { allow_exact } => {
                                right.1 < left.1 || (allow_exact && right.1 == left.1)
                            }
                            AsOfDirection::Forward { allow_exact } => {
                                right.1 > left.1 || (allow_exact && right.1 == left.1)
                            }
                        }
                });
                match direction {
                    AsOfDirection::Backward { .. } => eligible.max_by_key(|right| right.1),
                    AsOfDirection::Forward { .. } => eligible.min_by_key(|right| right.1),
                }
                .copied()
            };
            ((left, selected), weight)
        })
        .collect()
}
struct Fixture {
    root: TestStore,
    definition: AsOfJoinDefinition,
    operation: Operation,
    frame: Cell<Resume>,
    raw_right: OrderedMap<Vec<u8>, Vec<u8>>,
    transactions: Transactions,
}
impl Fixture {
    fn new(direction: AsOfDirection) -> Self {
        let root = TestStore::new();
        let definition = definition(direction);
        let mut setup = StoreSetup::new();
        let operation = OperationDefinition::from(definition.clone())
            .construct(
                &[schema(), schema()],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts()
            .0;
        let frame = setup
            .data_scope()
            .data::<Cell<Resume>>("test.frame")
            .unwrap();
        let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
        drop((operation, frame, transactions));
        let store = Store::open(root.path()).unwrap();
        let operation = OperationDefinition::from(definition.clone())
            .construct(
                &[schema(), schema()],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts()
            .0;
        let frame = store.open_data("test.frame").unwrap();
        let raw_right = store.open_data("operation/asof_join.right_rows").unwrap();
        Self {
            root,
            definition,
            operation,
            frame,
            raw_right,
            transactions: store.into_transactions(),
        }
    }
    fn reopen(self) -> Self {
        let Self {
            root,
            definition,
            operation,
            frame: _,
            raw_right: _,
            transactions,
        } = self;
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let operation = OperationDefinition::from(definition.clone())
            .construct(
                &[schema(), schema()],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts()
            .0;
        let frame = store.open_data("test.frame").unwrap();
        let raw_right = store.open_data("operation/asof_join.right_rows").unwrap();
        Self {
            root,
            definition,
            operation,
            frame,
            raw_right,
            transactions: store.into_transactions(),
        }
    }
    fn page(
        &mut self,
        port: usize,
        input: &Change,
        items: usize,
        commit: bool,
    ) -> Result<Step, OperationError> {
        let transaction = self.transactions.begin();
        let mut frame = self.frame.access(transaction.access())?;
        let resume = frame
            .get()?
            .unwrap_or_else(|| self.operation.initial_resume());
        let step = self.operation.step(
            OperationInput {
                port,
                change: input,
            },
            &resume,
            transaction.access(),
            &mut StepBudget::new(items, 4 * 1024 * 1024),
        )?;
        match &step.progress {
            Progress::More(next) => {
                assert_ne!(next, &resume);
                frame.set(next)?;
            }
            Progress::Done => {
                frame.clear()?;
            }
        }
        if commit {
            transaction.commit()?;
        }
        Ok(step)
    }
    fn run(&mut self, port: usize, input: &Change, items: usize) -> Vec<Change> {
        let mut outputs = Vec::new();
        for _ in 0..10000 {
            let step = self.page(port, input, items, true).unwrap();
            outputs.extend(step.output);
            if step.progress == Progress::Done {
                return outputs;
            }
        }
        panic!("ASOF did not finish")
    }
    fn right_rows(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let transaction = self.transactions.begin();
        self.raw_right
            .access(transaction.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(4096, 4 * 1024 * 1024).unwrap(),
            )
            .unwrap()
            .entries
    }
}
#[test]
fn strict_and_inclusive_neighbors_match_independent_bags_after_historical_insert_delete() {
    for direction in [
        AsOfDirection::Backward { allow_exact: false },
        AsOfDirection::Backward { allow_exact: true },
        AsOfDirection::Forward { allow_exact: false },
        AsOfDirection::Forward { allow_exact: true },
    ] {
        for items in [1, 2, 256] {
            let mut fixture = Fixture::new(direction);
            let mut left = BTreeMap::new();
            let mut right = BTreeMap::new();
            let mut actual = BTreeMap::new();
            let events = [
                (
                    1,
                    vec![
                        ((Some(1), Some(10), 10), 2),
                        ((Some(1), Some(30), 30), 1),
                        ((Some(2), Some(20), 20), 1),
                    ],
                ),
                (
                    0,
                    (0..=40)
                        .step_by(5)
                        .map(|at| ((Some(1), Some(at), at), 2))
                        .chain([
                            ((None, Some(20), 99), 1),
                            ((Some(1), None, 100), 1),
                            ((Some(2), Some(20), 21), 1),
                        ])
                        .collect(),
                ),
                (1, vec![((Some(1), Some(20), 20), 1)]),
                (
                    1,
                    vec![((Some(1), Some(20), 20), 3), ((Some(1), Some(20), 20), -3)],
                ),
                (1, vec![((Some(1), Some(20), 20), -1)]),
                (1, vec![((Some(1), Some(10), 10), -2)]),
                (
                    1,
                    vec![((Some(1), Some(15), 15), 1), ((Some(1), Some(35), 35), 1)],
                ),
                (
                    0,
                    vec![((Some(1), Some(20), 20), -1), ((Some(1), Some(20), 20), -1)],
                ),
                (
                    1,
                    vec![((Some(1), None, 999), 1), ((None, Some(20), 998), 1)],
                ),
            ];
            for (port, events) in events {
                let input = change(&events);
                loop {
                    let rolled = fixture.page(port, &input, items, false).unwrap();
                    fixture = fixture.reopen();
                    let step = fixture.page(port, &input, items, true).unwrap();
                    assert_eq!(rolled.progress, step.progress);
                    assert_eq!(
                        rolled.output.as_ref().map(output),
                        step.output.as_ref().map(output)
                    );
                    if let Some(output_change) = step.output {
                        for (row, diff) in output(&output_change) {
                            adjust(&mut actual, row, diff);
                        }
                    }
                    let done = step.progress == Progress::Done;
                    fixture = fixture.reopen();
                    if done {
                        break;
                    }
                }
                for (row, diff) in events {
                    adjust(if port == 0 { &mut left } else { &mut right }, row, diff);
                }
                assert_eq!(
                    actual,
                    oracle(&left, &right, direction),
                    "direction={direction:?},items={items}"
                );
            }
        }
    }
}
#[test]
fn a_right_event_stays_unapplied_until_the_last_correction_page() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(1, &change(&[((Some(1), Some(10), 10), 1)]), 256);
    fixture.run(
        0,
        &change(
            &(0..600)
                .map(|id| ((Some(1), Some(100 + id), id), 1))
                .collect::<Vec<_>>(),
        ),
        256,
    );
    let input = change(&[((Some(1), Some(20), 20), 1)]);
    let first = fixture.page(1, &input, 256, true).unwrap();
    assert!(matches!(first.progress, Progress::More(_)));
    assert_eq!(first.output.unwrap().num_rows(), 512);
    fixture = fixture.reopen();
    assert_eq!(fixture.right_rows().len(), 1);
    let rest = fixture.run(1, &input, 256);
    assert_eq!(rest.iter().map(Change::num_rows).sum::<usize>(), 688);
    assert_eq!(fixture.right_rows().len(), 2);
}
#[test]
fn same_exact_right_row_multiplicity_does_not_create_ties_or_corrections() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(1, &change(&[((Some(1), Some(10), 10), 3)]), 256);
    fixture.run(0, &change(&[((Some(1), Some(20), 20), 1)]), 256);
    assert!(
        fixture
            .run(
                1,
                &change(&[((Some(1), Some(10), 10), 2), ((Some(1), Some(10), 10), -4)]),
                256
            )
            .is_empty()
    );
    let changes = fixture.run(1, &change(&[((Some(1), Some(10), 10), -1)]), 256);
    let events = changes.iter().flat_map(output).collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert!(events[1].0.1.is_none());
}
#[test]
fn ambiguous_selected_buckets_fail_when_exposed_and_roll_back_the_current_page() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(
        1,
        &change(&[((Some(1), Some(10), 1), 1), ((Some(1), Some(10), 2), 1)]),
        256,
    );
    let input = change(&[((Some(1), Some(20), 3), 1)]);
    for _ in 0..2 {
        let error = fixture.page(0, &input, 256, true).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<AsOfJoinError>(),
            Some(AsOfJoinError::AmbiguousTie)
        ));
        fixture = fixture.reopen();
    }
    fixture.run(1, &change(&[((Some(1), Some(10), 2), -1)]), 256);
    assert_eq!(fixture.run(0, &input, 256)[0].num_rows(), 1);
}
#[test]
fn a_right_tie_outside_every_left_influence_interval_is_not_exposed() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(
        1,
        &change(&[((Some(1), Some(10), 1), 1), ((Some(1), Some(30), 3), 1)]),
        256,
    );
    fixture.run(0, &change(&[((Some(1), Some(40), 4), 1)]), 256);
    assert!(
        fixture
            .run(1, &change(&[((Some(1), Some(10), 2), 1)]), 256)
            .is_empty()
    );
    let error = fixture
        .page(0, &change(&[((Some(1), Some(20), 2), 1)]), 256, true)
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AsOfJoinError>(),
        Some(AsOfJoinError::AmbiguousTie)
    ));
}
#[test]
fn unrelated_history_and_null_order_rows_are_skipped_by_index_bounds() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(
        0,
        &change(
            &(0..4096)
                .map(|id| ((Some(1), Some(1000 + id), id), 1))
                .collect::<Vec<_>>(),
        ),
        256,
    );
    fixture.run(1, &change(&[((Some(1), Some(500), 1), 1)]), 256);
    let step = fixture
        .page(1, &change(&[((Some(1), Some(10), 2), 1)]), 1, true)
        .unwrap();
    assert_eq!(step.progress, Progress::Done);
    assert!(step.output.is_none());
    let step = fixture
        .page(1, &change(&[((Some(1), None, 3), 1)]), 1, true)
        .unwrap();
    assert_eq!(step.progress, Progress::Done);
    assert!(step.output.is_none());
}
#[test]
fn the_current_v1_payload_and_layout_reject_retired_asof_capabilities() {
    let literal = decode_hex(include_str!(
        "../fixtures/v1/asof_join_backward_left_outer.hex"
    ));
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&literal).unwrap();
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
        literal
    );
    assert_eq!(
        decoded.kind(),
        OperationKind::PagedTransform(NonZeroU32::new(2).unwrap())
    );
    let mut payload =
        serde_json::to_value(definition(AsOfDirection::Backward { allow_exact: true })).unwrap();
    payload["tolerance"] = serde_json::json!(1);
    assert!(serde_json::from_value::<AsOfJoinDefinition>(payload).is_err());
    let fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    let Fixture {
        root,
        operation,
        transactions,
        ..
    } = fixture;
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    assert!(
        store
            .open_data::<Cell<Resume>>("operation/asof_join.continuation")
            .is_err()
    );
}

#[test]
fn budget_failure_keeps_the_current_frame_unchanged() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    let input = change(&[((Some(1), Some(10), 10), 1)]);
    {
        let transaction = fixture.transactions.begin();
        let error = fixture
            .operation
            .step(
                OperationInput {
                    port: 1,
                    change: &input,
                },
                &fixture.operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 1),
            )
            .unwrap_err();
        assert!(error.is::<dogpaddle_operation::operation::BudgetExceeded>());
    }
    assert!(fixture.right_rows().is_empty());
    assert_eq!(
        fixture.page(1, &input, 1, true).unwrap().progress,
        Progress::Done
    );
    assert_eq!(fixture.right_rows().len(), 1);
}

#[test]
fn late_negative_event_preserves_committed_prefix_and_failed_resume_after_reopen() {
    let input = change(&[
        ((Some(1), Some(10), 10), 1),
        ((Some(1), Some(20), 20), 1),
        ((Some(1), Some(30), 30), -1),
    ]);
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    let page = fixture.page(1, &input, 2, true).unwrap();
    assert!(matches!(page.progress, Progress::More(_)));
    fixture = fixture.reopen();
    assert_eq!(fixture.right_rows().len(), 2);
    for _ in 0..2 {
        let error = fixture.page(1, &input, 2, true).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<AsOfJoinError>(),
            Some(AsOfJoinError::NegativeWeight)
        ));
        fixture = fixture.reopen();
        assert_eq!(fixture.right_rows().len(), 2);
    }
}

#[test]
fn byte_truncated_winner_probe_rolls_back_instead_of_accepting_a_tie() {
    for direction in [
        AsOfDirection::Forward { allow_exact: true },
        AsOfDirection::Backward { allow_exact: true },
    ] {
        let (root, schema, operation, mut transactions) = payload_fixture(direction);
        let event = |id, payload: &str| payload_event(&schema, 10, id, payload);
        let (small_id, large_id) = match direction {
            AsOfDirection::Forward { .. } => (0, 1),
            AsOfDirection::Backward { .. } => (1, 0),
        };
        for right in [
            event(small_id, "a"),
            event(large_id, &"x".repeat(64 * 1024)),
        ] {
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
        let left = event(2, "left");
        for bytes in [16 * 1024, 4 * 1024 * 1024] {
            let transaction = transactions.begin();
            let error = operation
                .step(
                    OperationInput {
                        port: 0,
                        change: &left,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, bytes),
                )
                .unwrap_err();
            if bytes == 16 * 1024 {
                assert!(
                    error.is::<dogpaddle_operation::operation::BudgetExceeded>(),
                    "{direction:?}: {error}"
                );
            } else {
                assert!(matches!(
                    error.downcast_ref::<AsOfJoinError>(),
                    Some(AsOfJoinError::AmbiguousTie)
                ));
            }
        }
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let left: OrderedMap<Vec<u8>, Vec<u8>> =
            store.open_data("operation/asof_join.left_rows").unwrap();
        let read = store.read_transaction();
        assert!(
            left.read(read.access())
                .unwrap()
                .scan(
                    ..,
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(1, 1024).unwrap()
                )
                .unwrap()
                .entries
                .is_empty()
        );
    }
}

fn payload_fixture(direction: AsOfDirection) -> (TestStore, SchemaRef, Operation, Transactions) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("at", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let (root, operation, _left_rows, transactions) = asof_runtime_fixture(&schema, direction);
    (root, schema, operation, transactions)
}
fn payload_event(schema: &SchemaRef, at: i64, id: i64, payload: &str) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![at])),
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(arrow_array::StringArray::from(vec![payload])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap()
}

#[test]
fn shared_winners_still_admit_every_output_reconstruction() {
    let (_root, schema, operation, mut transactions) =
        payload_fixture(AsOfDirection::Backward { allow_exact: true });
    let old_payload = "o".repeat(64 * 1024);
    let new_payload = "n".repeat(64 * 1024);
    let old = payload_event(&schema, 10, 0, &old_payload);
    for (port, event) in std::iter::once((1, old))
        .chain((0..16).map(|id| (0, payload_event(&schema, 20, id, "left"))))
    {
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port,
                    change: &event,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 4 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(step.progress, Progress::Done);
        transaction.commit().unwrap();
    }
    let replacement = payload_event(&schema, 15, 1, &new_payload);
    {
        let transaction = transactions.begin();
        let error = operation
            .step(
                OperationInput {
                    port: 1,
                    change: &replacement,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(16, 4 * 1024 * 1024),
            )
            .unwrap_err();
        assert!(error.is::<dogpaddle_operation::operation::BudgetExceeded>());
    }
    let transaction = transactions.begin();
    let step = operation
        .step(
            OperationInput {
                port: 1,
                change: &replacement,
            },
            &operation.initial_resume(),
            transaction.access(),
            &mut StepBudget::new(1, 4 * 1024 * 1024),
        )
        .unwrap();
    assert!(matches!(step.progress, Progress::More(_)));
    let output = step.output.unwrap();
    assert_eq!(output.diffs().values().as_ref(), &[-1, 1]);
    let payloads = output
        .records()
        .column(5)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(payloads.value(0), old_payload);
    assert_eq!(payloads.value(1), new_payload);
    transaction.commit().unwrap();
}

#[test]
fn continued_right_correction_validates_its_cursor_with_the_shared_byte_budget() {
    let (root, schema, operation, mut transactions) =
        payload_fixture(AsOfDirection::Backward { allow_exact: true });
    for left in [
        payload_event(&schema, 20, 0, &"x".repeat(8 * 1024)),
        payload_event(&schema, 21, 1, "a"),
    ] {
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
        assert_eq!(step.progress, Progress::Done);
        transaction.commit().unwrap();
    }
    let right = payload_event(&schema, 10, 2, "right");
    let resume = {
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
        let Progress::More(resume) = step.progress else {
            panic!("one left correction per page");
        };
        transaction.commit().unwrap();
        resume
    };
    for _ in 0..2 {
        let transaction = transactions.begin();
        let error = operation
            .step(
                OperationInput {
                    port: 1,
                    change: &right,
                },
                &resume,
                transaction.access(),
                &mut StepBudget::new(1, 16 * 1024),
            )
            .unwrap_err();
        assert!(error.is::<dogpaddle_operation::operation::BudgetExceeded>());
    }
    {
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port: 1,
                    change: &right,
                },
                &resume,
                transaction.access(),
                &mut StepBudget::new(1, 4 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(step.progress, Progress::Done);
        assert_eq!(step.output.unwrap().num_rows(), 2);
        transaction.commit().unwrap();
    }
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let right: OrderedMap<Vec<u8>, Vec<u8>> =
        store.open_data("operation/asof_join.right_rows").unwrap();
    let read = store.read_transaction();
    assert_eq!(
        right
            .read(read.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(2, 1024).unwrap()
            )
            .unwrap()
            .entries
            .len(),
        1
    );
}

fn asof_runtime_fixture(
    schema: &SchemaRef,
    direction: AsOfDirection,
) -> (
    TestStore,
    Operation,
    OrderedMap<Vec<u8>, Vec<u8>>,
    Transactions,
) {
    let root = TestStore::new();
    let definition = OperationDefinition::from(
        AsOfJoinDefinition::try_new(
            direction,
            [],
            AsOfOrderKey::new(col("at"), col("at")),
            ["left", "right"].into_iter().flat_map(|side| {
                schema
                    .fields()
                    .iter()
                    .map(move |field| format!("{side}_{}", field.name()))
            }),
        )
        .unwrap(),
    );
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
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let operation = definition
        .construct(
            &[Arc::clone(schema), Arc::clone(schema)],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0;
    let left_rows = store.open_data("operation/asof_join.left_rows").unwrap();
    (root, operation, left_rows, store.into_transactions())
}
#[test]
fn nested_winner_decode_fails_before_allocating_more_than_the_shared_budget() {
    use arrow_array::{ListArray, NullArray};
    use arrow_buffer::OffsetBuffer;
    let item = Arc::new(Field::new("item", DataType::Null, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("at", DataType::Int64, false),
        Field::new("items", DataType::List(Arc::clone(&item)), false),
    ]));
    let (_root, operation, left_rows, mut transactions) =
        asof_runtime_fixture(&schema, AsOfDirection::Backward { allow_exact: true });
    let event = |at, length: i32| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![at])),
                    Arc::new(ListArray::new(
                        Arc::clone(&item),
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
    };
    let right = event(10, 128 * 1024);
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
    let left = event(20, 0);
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
        assert!(error.is::<dogpaddle_operation::operation::BudgetExceeded>());
        assert!(
            left_rows
                .access(transaction.access())
                .unwrap()
                .scan(
                    ..,
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(1, 1024).unwrap()
                )
                .unwrap()
                .entries
                .is_empty()
        );
    }
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

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload =
        serde_json::to_value(definition(AsOfDirection::Backward { allow_exact: true })).unwrap();
    payload["output_names"] = serde_json::json!(["x".repeat(65_537)]);
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"asof_join": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[schema(), schema()],
        RuntimeResource::none(),
    );
}

#[test]
fn raw_duplicate_output_names_are_rejected_before_store_handle_access() {
    let mut payload =
        serde_json::to_value(definition(AsOfDirection::Backward { allow_exact: true })).unwrap();
    payload["output_names"][1] = payload["output_names"][0].clone();
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"asof_join": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[schema(), schema()],
        RuntimeResource::none(),
    );
}
