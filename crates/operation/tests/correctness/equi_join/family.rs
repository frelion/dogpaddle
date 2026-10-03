use std::{collections::BTreeMap, sync::Arc};

use crate::support::TestStore;
use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col, lit,
    operation::{
        Operation, OperationError, OperationInput, Progress, Resume, Step, StepBudget,
        transform::{EquiJoinDefinition, EquiJoinError, EquiJoinKind},
    },
};
use dogpaddle_store::{Cell, Store, StoreSetup, Transactions};

type Row = (Option<u64>, i64);
type ResultRow = (Option<Row>, Option<Row>);
const KINDS: [EquiJoinKind; 5] = [
    EquiJoinKind::Inner,
    EquiJoinKind::LeftSemi,
    EquiJoinKind::LeftAnti,
    EquiJoinKind::LeftOuter,
    EquiJoinKind::FullOuter,
];

#[derive(Clone, Copy)]
enum Residual {
    None,
    Greater,
    False,
    Null,
}
impl Residual {
    fn expression(self) -> Option<dogpaddle_operation::Expr> {
        match self {
            Self::None => None,
            Self::Greater => Some(col("left.value").gt(col("right.value"))),
            Self::False => Some(lit(false)),
            Self::Null => Some(lit(dogpaddle_operation::ScalarValue::Boolean(None))),
        }
    }
    fn qualifies(self, left: Row, right: Row) -> bool {
        left.0.is_some()
            && left.0 == right.0
            && match self {
                Self::None => true,
                Self::Greater => left.1 > right.1,
                Self::False | Self::Null => false,
            }
    }
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("value", DataType::Int64, false),
    ]))
}
fn definition(kind: EquiJoinKind, residual: Residual) -> EquiJoinDefinition {
    let names: &[&str] = if matches!(kind, EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti) {
        &["left_key", "left_value"]
    } else {
        &["left_key", "left_value", "right_key", "right_value"]
    };
    EquiJoinDefinition::try_new(
        kind,
        [(col("key"), col("key"))],
        names.iter().copied(),
        residual.expression(),
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
            ],
        )
        .unwrap(),
        Int64Array::from(events.iter().map(|(_, diff)| *diff).collect::<Vec<_>>()),
    )
    .unwrap()
}
fn output(change: &Change, kind: EquiJoinKind) -> Vec<(ResultRow, i64)> {
    let records = change.records();
    let key = |column, index| {
        let values = records
            .column(column)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (!values.is_null(index)).then(|| values.value(index))
    };
    let value = |column, index| {
        let values = records
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (!values.is_null(index)).then(|| values.value(index))
    };
    (0..change.num_rows())
        .map(|index| {
            let left = value(1, index).map(|value| (key(0, index), value));
            let right = if matches!(kind, EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti) {
                None
            } else {
                value(3, index).map(|value| (key(2, index), value))
            };
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
    kind: EquiJoinKind,
    residual: Residual,
) -> BTreeMap<ResultRow, i64> {
    let mut output = BTreeMap::new();
    for (&left, &weight) in left {
        let matches = right
            .iter()
            .filter(|(right, _)| residual.qualifies(left, **right))
            .collect::<Vec<_>>();
        match kind {
            EquiJoinKind::LeftSemi if !matches.is_empty() => {
                output.insert((Some(left), None), weight);
            }
            EquiJoinKind::LeftAnti if matches.is_empty() => {
                output.insert((Some(left), None), weight);
            }
            EquiJoinKind::Inner | EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => {
                for &(&right, &right_weight) in &matches {
                    output.insert((Some(left), Some(right)), weight * right_weight);
                }
                if matches.is_empty() && kind != EquiJoinKind::Inner {
                    output.insert((Some(left), None), weight);
                }
            }
            _ => {}
        }
    }
    if kind == EquiJoinKind::FullOuter {
        for (&right, &weight) in right {
            if !left.keys().any(|left| residual.qualifies(*left, right)) {
                output.insert((None, Some(right)), weight);
            }
        }
    }
    output
}
struct Fixture {
    root: TestStore,
    definition: EquiJoinDefinition,
    operation: Operation,
    frame: Cell<Resume>,
    transactions: Transactions,
}
impl Fixture {
    fn new(kind: EquiJoinKind, residual: Residual) -> Self {
        Self::with_definition(definition(kind, residual))
    }
    fn with_definition(definition: EquiJoinDefinition) -> Self {
        let root = TestStore::new();
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
        Self {
            root,
            definition,
            operation,
            frame,
            transactions,
        }
    }
    fn reopen(self) -> Self {
        let Self {
            root,
            definition,
            operation,
            frame: _,
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
        Self {
            root,
            definition,
            operation,
            frame,
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
        let mut output = Vec::new();
        for _ in 0..10000 {
            let step = self.page(port, input, items, true).unwrap();
            output.extend(step.output);
            if step.progress == Progress::Done {
                return output;
            }
        }
        panic!("join did not finish")
    }
}

#[test]
fn null_type_equality_never_matches_across_join_kinds_and_reopen() {
    for kind in KINDS {
        for residual in [Residual::None, Residual::Greater] {
            let names: &[&str] = if matches!(kind, EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti)
            {
                &["left_key", "left_value"]
            } else {
                &["left_key", "left_value", "right_key", "right_value"]
            };
            let null = lit(dogpaddle_operation::ScalarValue::Null);
            let definition = EquiJoinDefinition::try_new(
                kind,
                [(null.clone(), null)],
                names.iter().copied(),
                residual.expression(),
            )
            .unwrap();
            let mut fixture = Fixture::with_definition(definition);
            let mut left = BTreeMap::new();
            let mut right = BTreeMap::new();
            let mut actual = BTreeMap::new();
            for (port, row, diff) in [
                (0, (None, 20), 2),
                (1, (None, 10), 3),
                (0, (None, 20), -1),
                (1, (None, 10), -3),
                (0, (None, 20), -1),
            ] {
                let input = change(&[(row, diff)]);
                let rolled = fixture.page(port, &input, 1, false).unwrap();
                fixture = fixture.reopen();
                let step = fixture.page(port, &input, 1, true).unwrap();
                assert_eq!(rolled.progress, step.progress);
                assert_eq!(step.progress, Progress::Done);
                assert_eq!(
                    rolled.output.as_ref().map(|c| output(c, kind)),
                    step.output.as_ref().map(|c| output(c, kind))
                );
                if let Some(change) = step.output {
                    for (row, diff) in output(&change, kind) {
                        adjust(&mut actual, row, diff);
                    }
                }
                adjust(if port == 0 { &mut left } else { &mut right }, row, diff);
                assert_eq!(actual, oracle(&left, &right, kind, residual));
                fixture = fixture.reopen();
            }
            assert!(actual.is_empty());
        }
    }
}

#[test]
fn a_budget_rejected_stable_semi_or_anti_row_never_publishes_an_invalid_resume() {
    use dogpaddle_operation::operation::BudgetExceeded;
    for kind in [EquiJoinKind::LeftSemi, EquiJoinKind::LeftAnti] {
        let mut fixture = Fixture::new(kind, Residual::Greater);
        fixture.run(1, &change(&[((Some(1), 0), 1)]), 1);
        let input = change(&[((Some(1), 10), 1)]);
        fixture.run(0, &input, 1);
        let mut refused = 0;
        let mut accepted = 0;
        for bytes in 250..450 {
            let transaction = fixture.transactions.begin();
            let offered = OperationInput {
                port: 0,
                change: &input,
            };
            match fixture.operation.step(
                offered,
                &fixture.operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, bytes),
            ) {
                Err(error) => {
                    assert!(
                        std::iter::successors(
                            Some(error.as_ref() as &(dyn std::error::Error + 'static)),
                            |cause| cause.source(),
                        )
                        .any(<dyn std::error::Error>::is::<BudgetExceeded>),
                        "{error}"
                    );
                    refused += 1;
                }
                Ok(step) => {
                    if let Progress::More(next) = step.progress {
                        fixture.operation.validate_resume(offered, &next).unwrap();
                    }
                    accepted += 1;
                }
            }
            // Every probe rolls back, including one that could finish.
        }
        assert!(refused > 0 && accepted > 0);
        fixture = fixture.reopen();
        let outputs = fixture.run(0, &input, 1);
        let events = outputs
            .iter()
            .flat_map(|change| output(change, kind))
            .collect::<Vec<_>>();
        if kind == EquiJoinKind::LeftSemi {
            assert_eq!(events, [((Some((Some(1), 10)), None), 1)]);
        } else {
            assert!(events.is_empty());
        }
    }
}

#[test]
fn every_join_kind_and_residual_matches_an_independent_bag_after_weighted_events() {
    for kind in KINDS {
        for residual in [
            Residual::None,
            Residual::Greater,
            Residual::False,
            Residual::Null,
        ] {
            for items in [1, 2, 256] {
                let mut fixture = Fixture::new(kind, residual);
                let mut left = BTreeMap::new();
                let mut right = BTreeMap::new();
                let mut actual = BTreeMap::new();
                let events = [
                    (0, vec![((Some(7), 10), 2), ((None, 3), 1)]),
                    (1, vec![((Some(7), 5), 3), ((Some(8), 20), 1)]),
                    (0, vec![((Some(7), 1), 1), ((Some(8), 40), 2)]),
                    (1, vec![((Some(7), 12), 1), ((None, 4), 1)]),
                    (
                        1,
                        vec![((Some(7), 5), -2), ((Some(7), 5), -1), ((Some(7), 5), 1)],
                    ),
                    (
                        0,
                        vec![((Some(7), 10), -1), ((Some(7), 10), -1), ((Some(7), 10), 1)],
                    ),
                    (1, vec![((Some(7), 12), -1)]),
                    (0, vec![((Some(8), 40), -2), ((None, 3), -1)]),
                ];
                for (port, events) in events {
                    let input = change(&events);
                    let mut outputs = Vec::new();
                    loop {
                        let rolled_back = fixture.page(port, &input, items, false).unwrap();
                        fixture = fixture.reopen();
                        let committed = fixture.page(port, &input, items, true).unwrap();
                        assert_eq!(rolled_back.progress, committed.progress);
                        assert_eq!(
                            rolled_back
                                .output
                                .as_ref()
                                .map(|output| output_events(output, kind)),
                            committed
                                .output
                                .as_ref()
                                .map(|output| output_events(output, kind))
                        );
                        outputs.extend(committed.output);
                        let done = committed.progress == Progress::Done;
                        fixture = fixture.reopen();
                        if done {
                            break;
                        }
                    }
                    for output in &outputs {
                        for (row, diff) in output_events(output, kind) {
                            adjust(&mut actual, row, diff);
                        }
                    }
                    for (row, diff) in events {
                        adjust(if port == 0 { &mut left } else { &mut right }, row, diff);
                    }
                    assert_eq!(
                        actual,
                        oracle(&left, &right, kind, residual),
                        "kind={kind:?},items={items}"
                    );
                }
            }
        }
    }
}
fn output_events(change: &Change, kind: EquiJoinKind) -> Vec<(ResultRow, i64)> {
    output(change, kind)
}

#[test]
fn fanout_pages_rebuild_from_only_the_frame_and_real_relation_state() {
    for kind in KINDS {
        for residual in [Residual::None, Residual::Greater] {
            let mut fixture = Fixture::new(kind, residual);
            let right = change(
                &(0..600)
                    .map(|value| ((Some(7), value), 1))
                    .collect::<Vec<_>>(),
            );
            fixture.run(1, &right, 256);
            let input = change(&[((Some(7), 1000), 1)]);
            let mut rows = 0;
            let mut pages = 0;
            loop {
                let step = fixture.page(0, &input, 256, true).unwrap();
                rows += step.output.as_ref().map_or(0, Change::num_rows);
                pages += 1;
                let done = step.progress == Progress::Done;
                fixture = fixture.reopen();
                if done {
                    break;
                }
            }
            if !matches!(
                (kind, residual),
                (
                    EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti,
                    Residual::None
                )
            ) {
                assert!(pages >= 3, "scanned candidates are charged");
            }
            assert_eq!(
                rows,
                match kind {
                    EquiJoinKind::LeftSemi => 1,
                    EquiJoinKind::LeftAnti => 0,
                    EquiJoinKind::FullOuter => 1200,
                    _ => 600,
                }
            );
            let retractions = fixture.run(0, &change(&[((Some(7), 1000), -1)]), 256);
            assert_eq!(
                retractions.iter().map(Change::num_rows).sum::<usize>(),
                rows
            );
        }
    }
}
#[test]
fn late_negative_event_keeps_committed_pages_and_repeats_after_reconstruction() {
    let mut fixture = Fixture::new(EquiJoinKind::Inner, Residual::None);
    let mut events = (0..300)
        .map(|value| ((Some(7), value), 1))
        .collect::<Vec<_>>();
    events.push(((Some(8), 999), -1));
    let input = change(&events);
    let step = fixture.page(0, &input, 256, true).unwrap();
    assert!(matches!(step.progress, Progress::More(_)));
    fixture = fixture.reopen();
    for _ in 0..2 {
        let error = fixture.page(0, &input, 256, true).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::NegativeWeight)
        ));
        fixture = fixture.reopen();
    }
}
#[test]
fn empty_buckets_advance_batchwise_without_per_event_transactions() {
    let mut fixture = Fixture::new(EquiJoinKind::Inner, Residual::None);
    let input = change(
        &(0..4096)
            .map(|value| ((Some(u64::try_from(value).unwrap()), value), 1))
            .collect::<Vec<_>>(),
    );
    let mut pages = 0;
    loop {
        let step = fixture.page(0, &input, 256, true).unwrap();
        assert!(step.output.is_none());
        pages += 1;
        if step.progress == Progress::Done {
            break;
        }
    }
    assert_eq!(pages, 16);
}
#[test]
fn definitions_keep_the_five_literal_golden_payloads_and_no_operator_cursor_resource() {
    let literals = [
        include_str!("../../fixtures/v1/equi_join_inner.hex"),
        include_str!("../../fixtures/v1/equi_join_left_semi.hex"),
        include_str!("../../fixtures/v1/equi_join_left_anti.hex"),
        include_str!("../../fixtures/v1/equi_join_left_outer.hex"),
        include_str!("../../fixtures/v1/equi_join_full_outer.hex"),
    ];
    // Golden schemas use id/fk and corresponding physical names.
    for (kind, literal) in KINDS.into_iter().zip(literals) {
        let decoded =
            serde_json::from_slice::<OperationDefinition>(&crate::support::decode_hex(literal))
                .unwrap();
        assert_eq!(decoded.input_count(), 2);
        assert_eq!(
            serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
            crate::support::decode_hex(literal)
        );
        let _ = kind;
    }
    let fixture = Fixture::new(EquiJoinKind::Inner, Residual::None);
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
            .open_data::<Cell<Resume>>("operation/equi_join.continuation")
            .is_err()
    );
}
#[test]
fn wrong_resume_variant_and_input_port_are_rejected_before_state_changes() {
    let mut fixture = Fixture::new(EquiJoinKind::Inner, Residual::None);
    let input = change(&[((Some(7), 1), 1)]);
    let transaction = fixture.transactions.begin();
    assert!(
        fixture
            .operation
            .step(
                OperationInput {
                    port: 0,
                    change: &input
                },
                &Resume::batch(),
                transaction.access(),
                &mut StepBudget::new(1, 4096)
            )
            .is_err()
    );
    assert!(
        fixture
            .operation
            .step(
                OperationInput {
                    port: 2,
                    change: &input
                },
                &fixture.operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 4096)
            )
            .is_err()
    );
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload =
        serde_json::to_value(definition(EquiJoinKind::Inner, Residual::None)).unwrap();
    payload["keys"] = serde_json::json!([]);
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"equi_join": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[schema(), schema()],
        RuntimeResource::none(),
    );
}

#[test]
fn raw_duplicate_output_names_are_rejected_before_store_handle_access() {
    let mut payload =
        serde_json::to_value(definition(EquiJoinKind::Inner, Residual::None)).unwrap();
    payload["output_names"][1] = payload["output_names"][0].clone();
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"equi_join": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[schema(), schema()],
        RuntimeResource::none(),
    );
}

#[test]
fn pure_join_uses_current_row_membership_after_each_event_in_one_window() {
    let kind = EquiJoinKind::FullOuter;
    let left = (Some(1), 10);
    let right = (Some(1), 20);
    let other = (Some(1), 30);
    let mut fixture = Fixture::new(kind, Residual::None);
    fixture.run(0, &change(&[(left, 1)]), 64);
    // The 2 -> 1 adjustment does not change distinct membership; the last
    // deletion alone restores the left NULL row, in the same input window.
    let step = fixture
        .page(
            1,
            &change(&[(right, 2), (right, -1), (right, -1)]),
            64,
            true,
        )
        .unwrap();
    assert_eq!(step.progress, Progress::Done);
    assert_eq!(
        output(&step.output.unwrap(), kind),
        [
            ((Some(left), None), -1),
            ((Some(left), Some(right)), 2),
            ((Some(left), Some(right)), -1),
            ((Some(left), Some(right)), -1),
            ((Some(left), None), 1),
        ]
    );
    fixture = fixture.reopen();
    // Removing the first exact row leaves the other row under the same key;
    // only removing that other row makes the equality partition empty.
    let input = change(&[(right, 1), (other, 1), (right, -1), (other, -1)]);
    let rolled_back = fixture.page(1, &input, 64, false).unwrap();
    fixture = fixture.reopen();
    let step = fixture.page(1, &input, 64, true).unwrap();
    assert_eq!(step.progress, Progress::Done);
    let expected = [
        ((Some(left), None), -1),
        ((Some(left), Some(right)), 1),
        ((Some(left), Some(other)), 1),
        ((Some(left), Some(right)), -1),
        ((Some(left), Some(other)), -1),
        ((Some(left), None), 1),
    ];
    assert_eq!(output(&rolled_back.output.unwrap(), kind), expected);
    assert_eq!(output(&step.output.unwrap(), kind), expected);
    fixture = fixture.reopen();
    assert_eq!(
        output(&fixture.run(0, &change(&[(left, 1)]), 64).remove(0), kind),
        [((Some(left), None), 1)]
    );
}

#[test]
fn pure_join_reopens_with_rows_without_declaring_derived_key_counts() {
    use dogpaddle_store::{OrderedMap, PartitionKey};
    use std::num::NonZeroU64;
    for kind in KINDS {
        let fixture = Fixture::new(kind, Residual::None);
        let Fixture {
            root,
            operation,
            transactions,
            ..
        } = fixture;
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        for side in ["left_rows", "right_rows"] {
            assert!(
                store
                    .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>>(&format!(
                        "operation/equi_join.{side}"
                    ))
                    .is_ok()
            );
        }
        assert!(
            store
                .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/equi_join.key_counts")
                .is_err()
        );
        assert!(
            store
                .open_data::<OrderedMap<Vec<u8>, u64>>("operation/equi_join.match_counts")
                .is_err()
        );
        assert!(
            OperationDefinition::from(definition(kind, Residual::None))
                .construct(
                    &[schema(), schema()],
                    &mut store.data_scope().scoped("operation"),
                    RuntimeResource::none(),
                )
                .is_ok()
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "One fixture verifies malformed and overflowing durable weights, transaction poison, and unchanged reopened state."
)]
fn pure_join_checks_current_weight_before_writing_checked_after() {
    use dogpaddle_store::{OrderedMap, PartitionKey, ScanDirection, ScanLimit, StoreError};
    let mut fixture = Fixture::new(EquiJoinKind::FullOuter, Residual::None);
    let left = (Some(1), 10);
    fixture.run(0, &change(&[(left, 1)]), 64);
    let Fixture {
        root,
        operation,
        transactions,
        ..
    } = fixture;
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let raw = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, Vec<u8>>>(
            "operation/equi_join.left_rows",
        )
        .unwrap();
    let operation = OperationDefinition::from(definition(EquiJoinKind::FullOuter, Residual::None))
        .construct(
            &[schema(), schema()],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0;
    let mut transactions = store.into_transactions();
    let key = {
        let transaction = transactions.begin();
        let access = raw.access(transaction.access()).unwrap();
        let key = access
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(1, 1024).unwrap(),
            )
            .unwrap()
            .entries
            .pop()
            .unwrap()
            .0;
        transaction.commit().unwrap();
        key
    };
    for bytes in [
        vec![0; 8],
        vec![1; 7],
        vec![1; 9],
        u64::MAX.to_be_bytes().to_vec(),
    ] {
        {
            let transaction = transactions.begin();
            raw.access(transaction.access())
                .unwrap()
                .put(&key, &bytes)
                .unwrap();
            transaction.commit().unwrap();
        }
        let transaction = transactions.begin();
        let error = operation
            .step(
                OperationInput {
                    port: 0,
                    change: &change(&[(left, 1)]),
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(64, 4 * 1024 * 1024),
            )
            .unwrap_err();
        if bytes == u64::MAX.to_be_bytes() {
            assert!(matches!(
                error.downcast_ref::<EquiJoinError>(),
                Some(EquiJoinError::WeightOverflow)
            ));
            // The pure arithmetic admission error keeps the existing policy:
            // no write occurred, and the unchanged raw weight is still readable.
            assert_eq!(
                raw.access(transaction.access()).unwrap().get(&key).unwrap(),
                Some(bytes)
            );
        } else {
            assert!(matches!(
                error.downcast_ref::<EquiJoinError>(),
                Some(EquiJoinError::Store(StoreError::Codec(_)))
            ));
            assert!(matches!(
                transaction.commit(),
                Err(StoreError::TransactionPoisoned)
            ));
        }
    }
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let raw = store
        .open_data::<OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, Vec<u8>>>(
            "operation/equi_join.left_rows",
        )
        .unwrap();
    let snapshot = store.read_transaction();
    assert_eq!(
        raw.read(snapshot.access()).unwrap().get(&key).unwrap(),
        Some(u64::MAX.to_be_bytes().to_vec())
    );
}
