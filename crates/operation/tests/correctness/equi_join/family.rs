use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, placeholder};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DefinitionCodecError, OperationBindError, OperationKind, RuntimeResource, col,
    decode_definition, lit,
    operation::{
        Action, Operation, OperationError, OperationInput,
        transform::{
            EquiJoinDefinition, EquiJoinDefinitionError, EquiJoinError, EquiJoinKind,
            EquiJoinSchemaError,
        },
    },
};
use dogpaddle_store::{Cell, PartitionedMultiset, Store, StoreSetup, Transactions};

use crate::support::{
    TestStore, assert_literal_definition, commit_ready, construct_checked, decode_hex,
    rollback_ready,
};

const OPERATION_PREFIX: &str = "operation";
const BASE_RESOURCES: [&str; 3] = [
    "equi_join.left_rows",
    "equi_join.right_rows",
    "equi_join.continuation",
];

const KINDS: [EquiJoinKind; 5] = [
    EquiJoinKind::Inner,
    EquiJoinKind::LeftSemi,
    EquiJoinKind::LeftAnti,
    EquiJoinKind::LeftOuter,
    EquiJoinKind::FullOuter,
];
const PRESENCE_KINDS: [EquiJoinKind; 4] = [
    EquiJoinKind::LeftSemi,
    EquiJoinKind::LeftAnti,
    EquiJoinKind::LeftOuter,
    EquiJoinKind::FullOuter,
];
const INNER_V1: &str = include_str!("../../fixtures/v1/equi_join_inner.hex");
const LEFT_SEMI_V1: &str = include_str!("../../fixtures/v1/equi_join_left_semi.hex");
const LEFT_ANTI_V1: &str = include_str!("../../fixtures/v1/equi_join_left_anti.hex");
const LEFT_OUTER_V1: &str = include_str!("../../fixtures/v1/equi_join_left_outer.hex");
const FULL_OUTER_V1: &str = include_str!("../../fixtures/v1/equi_join_full_outer.hex");
const LEFT_SEMI_RESIDUAL_V1: &str =
    include_str!("../../fixtures/v1/equi_join_left_semi_residual.hex");
const OLD_INNER_ONLY_TAG_16_V1: &str = "
646f67706164646c652e6f7065726174696f6e0000010010
00000001000000060a040a026964000000060a040a02666b
00000004000000076c6566745f69640000000a6c6566745f
6c6162656c0000000872696768745f666b0000000c726967
68745f616d6f756e74
";
const DEFINITION_HEADER_BYTES: usize = b"dogpaddle.operation\0".len() + size_of::<u16>() * 2;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct InputRow {
    key: Option<u64>,
    value: i64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OutputRow {
    left_key: Option<u64>,
    left_value: Option<i64>,
    right_key: Option<u64>,
    right_value: Option<i64>,
}

#[derive(Clone, Debug)]
struct InputEvent {
    key: Option<u64>,
    value: i64,
    difference: i64,
}

#[derive(Clone, Default)]
struct NaiveRelation {
    left: BTreeMap<InputRow, u64>,
    right: BTreeMap<InputRow, u64>,
    residual: ResidualCase,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ResidualCase {
    #[default]
    None,
    GreaterThan,
    LessThan,
    False,
    Null,
}

struct Fixture {
    kind: EquiJoinKind,
    definition: EquiJoinDefinition,
    root: TestStore,
    operation: Operation,
    transactions: Transactions,
}

type ResidualErrorPredicate = fn(&EquiJoinSchemaError) -> bool;

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("left_value", DataType::Int64, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("right_value", DataType::Int64, false),
    ]))
}

fn output_names(kind: EquiJoinKind) -> &'static [&'static str] {
    match kind {
        EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti => &["left_key", "left_value"],
        EquiJoinKind::Inner | EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => {
            &["left_key", "left_value", "right_key", "right_value"]
        }
    }
}

fn definition(kind: EquiJoinKind) -> EquiJoinDefinition {
    definition_with_residual(kind, ResidualCase::None)
}

fn definition_with_residual(kind: EquiJoinKind, residual: ResidualCase) -> EquiJoinDefinition {
    EquiJoinDefinition::try_new(
        kind,
        [(col("key"), col("key"))],
        output_names(kind).iter().copied(),
        residual.expression(),
    )
    .unwrap()
}

fn residual_definition(kind: EquiJoinKind) -> EquiJoinDefinition {
    definition_with_residual(kind, ResidualCase::GreaterThan)
}

fn input_change(port: usize, events: &[InputEvent]) -> Change {
    let schema = match port {
        0 => left_schema(),
        1 => right_schema(),
        _ => panic!("test input port is outside the equi-join"),
    };
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                events.iter().map(|event| event.key).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                events.iter().map(|event| event.value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    Change::try_new(
        records,
        Int64Array::from(
            events
                .iter()
                .map(|event| event.difference)
                .collect::<Vec<_>>(),
        ),
    )
    .unwrap()
}

impl Fixture {
    fn new(kind: EquiJoinKind) -> Self {
        Self::with_residual(kind, ResidualCase::None)
    }

    fn with_residual(kind: EquiJoinKind, residual: ResidualCase) -> Self {
        let definition = definition_with_residual(kind, residual);
        let root = TestStore::new();
        let (operation, transactions) = construct_join(&root, &definition);
        Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        }
    }

    fn reopen(self) -> Self {
        let Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        } = self;
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let operation = reopen_join(&store, &definition);
        Self {
            kind,
            definition,
            root,
            operation,
            transactions: store.into_transactions(),
        }
    }

    fn run(&mut self, port: usize, events: &[InputEvent]) -> Result<Vec<Change>, OperationError> {
        run_claim(
            &mut self.operation,
            &mut self.transactions,
            port,
            &input_change(port, events),
        )
    }

    fn commit_once(&mut self, port: usize, input: &Change) -> Result<Action, OperationError> {
        commit_ready(
            &mut self.operation,
            Some(OperationInput {
                port,
                change: input,
            }),
            &mut self.transactions,
        )
    }

    fn rollback_once(&mut self, port: usize, input: &Change) -> Result<Action, OperationError> {
        rollback_ready(
            &mut self.operation,
            Some(OperationInput {
                port,
                change: input,
            }),
            &mut self.transactions,
        )
    }

    fn corrupt_raw_row(self, port: usize, key: &[u8], row: &[u8], difference: i64) -> Self {
        let Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        } = self;
        drop((operation, transactions));

        let store = Store::open(root.path()).unwrap();
        let data_name = match port {
            0 => "equi_join.left_rows",
            1 => "equi_join.right_rows",
            _ => panic!("test input port is outside the equi-join"),
        };
        let rows: PartitionedMultiset<Vec<u8>, Vec<u8>> = store
            .open_data(&format!("{OPERATION_PREFIX}/{data_name}"))
            .unwrap();
        let operation = reopen_join(&store, &definition);
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        rows.access(transaction.access())
            .unwrap()
            .partition(&key.to_vec())
            .unwrap()
            .adjust(&row.to_vec(), difference)
            .unwrap();
        transaction.commit().unwrap();

        Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        }
    }

    fn rewrite_raw_continuation(self, rewrite: impl FnOnce(&mut Vec<u8>)) -> Self {
        let Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        } = self;
        drop((operation, transactions));

        let store = Store::open(root.path()).unwrap();
        let raw: Cell<Vec<u8>> = store
            .open_data(&format!("{OPERATION_PREFIX}/equi_join.continuation"))
            .unwrap();
        let operation = reopen_join(&store, &definition);
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        let mut raw = raw.access(transaction.access()).unwrap();
        let mut value = raw
            .get()
            .unwrap()
            .expect("the test requires a continuation");
        rewrite(&mut value);
        raw.set(&value).unwrap();
        transaction.commit().unwrap();

        Self {
            kind,
            definition,
            root,
            operation,
            transactions,
        }
    }
}

impl ResidualCase {
    fn expression(self) -> Option<Expr> {
        match self {
            Self::None => None,
            Self::GreaterThan => Some(col("left.left_value").gt(col("right.right_value"))),
            Self::LessThan => Some(col("left.left_value").lt(col("right.right_value"))),
            Self::False => Some(lit(false)),
            Self::Null => Some(lit(ScalarValue::Boolean(None))),
        }
    }

    fn matches(self, left: &InputRow, right: &InputRow) -> bool {
        match self {
            Self::None => true,
            Self::GreaterThan => left.value > right.value,
            Self::LessThan => left.value < right.value,
            Self::False | Self::Null => false,
        }
    }
}

fn construct_join(root: &TestStore, definition: &EquiJoinDefinition) -> (Operation, Transactions) {
    let mut setup = StoreSetup::new();
    let (operation, _) = (definition as &dyn dogpaddle_operation::OperationDefinition)
        .construct(
            &[left_schema(), right_schema()],
            &mut setup.data_scope().scoped(OPERATION_PREFIX),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    (operation, transactions)
}

fn reopen_join(store: &Store, definition: &EquiJoinDefinition) -> Operation {
    (definition as &dyn dogpaddle_operation::OperationDefinition)
        .construct(
            &[left_schema(), right_schema()],
            &mut store.data_scope().scoped(OPERATION_PREFIX),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts()
        .0
}

fn resource_names(kind: EquiJoinKind, has_residual: bool) -> Vec<&'static str> {
    let mut names = BASE_RESOURCES.to_vec();
    if kind != EquiJoinKind::Inner {
        names.push(if has_residual {
            "equi_join.match_counts"
        } else {
            "equi_join.key_counts"
        });
    }
    names
}

fn run_claim(
    operation: &mut Operation,
    transactions: &mut Transactions,
    port: usize,
    input: &Change,
) -> Result<Vec<Change>, OperationError> {
    let mut outputs = Vec::new();
    for _ in 0..10_000 {
        match commit_ready(
            operation,
            Some(OperationInput {
                port,
                change: input,
            }),
            transactions,
        )? {
            Action::Commit(output) => outputs.extend(output),
            Action::Complete(output) => {
                outputs.extend(output);
                return Ok(outputs);
            }
            Action::Idle => panic!("equi-join returned Idle for a pinned input"),
        }
    }
    panic!("equi-join did not complete a bounded test Claim")
}

fn rollback_and_commit_first_output_page(
    fixture: &mut Fixture,
    port: usize,
    input: &Change,
) -> Vec<Change> {
    loop {
        let Action::Commit(rolled_back) = fixture.rollback_once(port, input).unwrap() else {
            panic!("{:?} completed before its expected Emit page", fixture.kind)
        };
        let Action::Commit(committed) = fixture.commit_once(port, input).unwrap() else {
            panic!("{:?} did not replay its rolled-back turn", fixture.kind)
        };
        assert_events(
            fixture.kind,
            &rolled_back.into_iter().collect::<Vec<_>>(),
            output_events(
                fixture.kind,
                &committed.clone().into_iter().collect::<Vec<_>>(),
            ),
        );
        if let Some(output) = committed {
            return vec![output];
        }
    }
}

fn commit_prefix_and_rollback_complete(
    fixture: &mut Fixture,
    port: usize,
    input: &Change,
) -> (Vec<Change>, Vec<Change>, usize) {
    let mut committed_outputs = Vec::new();
    for committed_turns in 0..10_000 {
        match fixture.rollback_once(port, input).unwrap() {
            Action::Commit(rolled_back) => {
                let Action::Commit(committed) = fixture.commit_once(port, input).unwrap() else {
                    panic!("rolled-back intermediate equi-join turn did not replay as Commit")
                };
                assert_events(
                    fixture.kind,
                    &rolled_back.into_iter().collect::<Vec<_>>(),
                    output_events(
                        fixture.kind,
                        &committed.clone().into_iter().collect::<Vec<_>>(),
                    ),
                );
                committed_outputs.extend(committed);
            }
            Action::Complete(rolled_back) => {
                return (
                    committed_outputs,
                    rolled_back.into_iter().collect(),
                    committed_turns,
                );
            }
            Action::Idle => panic!("equi-join returned Idle for a pinned input"),
        }
    }
    panic!("equi-join did not reach a bounded final Complete turn")
}

fn assert_outer_corrections_are_paired(outputs: &[Change]) {
    for output in outputs {
        assert_eq!(output.num_rows() % 2, 0);
        for row in (0..output.num_rows()).step_by(2) {
            assert_eq!(
                optional_u64(output, 0, row),
                optional_u64(output, 0, row + 1)
            );
            assert_eq!(
                optional_i64(output, 1, row),
                optional_i64(output, 1, row + 1)
            );
            assert_ne!(
                optional_u64(output, 2, row).is_none(),
                optional_u64(output, 2, row + 1).is_none()
            );
            assert_eq!(output.diffs().value(row), -1);
            assert_eq!(output.diffs().value(row + 1), 1);
        }
    }
}

impl NaiveRelation {
    fn with_residual(residual: ResidualCase) -> Self {
        Self {
            residual,
            ..Self::default()
        }
    }

    fn apply_claim(
        &mut self,
        kind: EquiJoinKind,
        port: usize,
        events: &[InputEvent],
    ) -> Vec<(OutputRow, i64)> {
        let mut output = Vec::new();
        for event in events {
            let before = self.render(kind);
            self.adjust(port, event);
            let after = self.render(kind);
            output.extend(relation_delta(before, after));
        }
        output
    }

    fn adjust(&mut self, port: usize, event: &InputEvent) {
        let relation = match port {
            0 => &mut self.left,
            1 => &mut self.right,
            _ => panic!("oracle input port is outside the equi-join"),
        };
        let row = InputRow {
            key: event.key,
            value: event.value,
        };
        let before = relation.get(&row).copied().unwrap_or(0);
        let after = i128::from(before) + i128::from(event.difference);
        assert!((0..=i128::from(u64::MAX)).contains(&after));
        if after == 0 {
            relation.remove(&row);
        } else {
            relation.insert(row, u64::try_from(after).unwrap());
        }
    }

    fn render(&self, kind: EquiJoinKind) -> BTreeMap<OutputRow, i128> {
        let mut output = BTreeMap::new();
        for (left, left_weight) in &self.left {
            let matches = self
                .right
                .iter()
                .filter(|(right, _)| self.matchable(left, right))
                .collect::<Vec<_>>();
            match kind {
                EquiJoinKind::Inner => {
                    add_matches(&mut output, left, *left_weight, &matches);
                }
                EquiJoinKind::LeftSemi => {
                    if !matches.is_empty() {
                        add_weight(&mut output, left_only(left), i128::from(*left_weight));
                    }
                }
                EquiJoinKind::LeftAnti => {
                    if matches.is_empty() {
                        add_weight(&mut output, left_only(left), i128::from(*left_weight));
                    }
                }
                EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => {
                    if matches.is_empty() {
                        add_weight(&mut output, left_unmatched(left), i128::from(*left_weight));
                    } else {
                        add_matches(&mut output, left, *left_weight, &matches);
                    }
                }
            }
        }
        if kind == EquiJoinKind::FullOuter {
            for (right, right_weight) in &self.right {
                let matched = self.left.keys().any(|left| self.matchable(left, right));
                if !matched {
                    add_weight(
                        &mut output,
                        right_unmatched(right),
                        i128::from(*right_weight),
                    );
                }
            }
        }
        output
    }

    fn matchable(&self, left: &InputRow, right: &InputRow) -> bool {
        left.key.is_some() && left.key == right.key && self.residual.matches(left, right)
    }
}

fn add_matches(
    output: &mut BTreeMap<OutputRow, i128>,
    left: &InputRow,
    left_weight: u64,
    matches: &[(&InputRow, &u64)],
) {
    for (right, right_weight) in matches {
        add_weight(
            output,
            OutputRow {
                left_key: left.key,
                left_value: Some(left.value),
                right_key: right.key,
                right_value: Some(right.value),
            },
            i128::from(left_weight) * i128::from(**right_weight),
        );
    }
}

fn left_only(left: &InputRow) -> OutputRow {
    OutputRow {
        left_key: left.key,
        left_value: Some(left.value),
        right_key: None,
        right_value: None,
    }
}

fn left_unmatched(left: &InputRow) -> OutputRow {
    left_only(left)
}

fn right_unmatched(right: &InputRow) -> OutputRow {
    OutputRow {
        left_key: None,
        left_value: None,
        right_key: right.key,
        right_value: Some(right.value),
    }
}

fn add_weight(output: &mut BTreeMap<OutputRow, i128>, row: OutputRow, weight: i128) {
    *output.entry(row).or_default() += weight;
}

fn relation_delta(
    before: BTreeMap<OutputRow, i128>,
    mut after: BTreeMap<OutputRow, i128>,
) -> Vec<(OutputRow, i64)> {
    for (row, weight) in before {
        *after.entry(row).or_default() -= weight;
    }
    after
        .into_iter()
        .filter_map(|(row, difference)| {
            (difference != 0).then(|| (row, i64::try_from(difference).unwrap()))
        })
        .collect()
}

fn output_events(kind: EquiJoinKind, outputs: &[Change]) -> Vec<(OutputRow, i64)> {
    let mut events = Vec::new();
    for output in outputs {
        for row in 0..output.num_rows() {
            let left_key = optional_u64(output, 0, row);
            let left_value = optional_i64(output, 1, row);
            let (right_key, right_value) = match kind {
                EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti => (None, None),
                EquiJoinKind::Inner | EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter => {
                    (optional_u64(output, 2, row), optional_i64(output, 3, row))
                }
            };
            events.push((
                OutputRow {
                    left_key,
                    left_value,
                    right_key,
                    right_value,
                },
                output.diffs().value(row),
            ));
        }
    }
    events
}

fn optional_u64(change: &Change, column: usize, row: usize) -> Option<u64> {
    let values = change
        .records()
        .column(column)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (!values.is_null(row)).then(|| values.value(row))
}

fn optional_i64(change: &Change, column: usize, row: usize) -> Option<i64> {
    let values = change
        .records()
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (!values.is_null(row)).then(|| values.value(row))
}

fn assert_events(kind: EquiJoinKind, outputs: &[Change], mut expected: Vec<(OutputRow, i64)>) {
    let mut actual = output_events(kind, outputs);
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "wrong {kind:?} output events");
}

fn kind_fixture(kind: EquiJoinKind) -> &'static str {
    match kind {
        EquiJoinKind::Inner => INNER_V1,
        EquiJoinKind::LeftSemi => LEFT_SEMI_V1,
        EquiJoinKind::LeftAnti => LEFT_ANTI_V1,
        EquiJoinKind::LeftOuter => LEFT_OUTER_V1,
        EquiJoinKind::FullOuter => FULL_OUTER_V1,
    }
}

const fn event(key: Option<u64>, value: i64, difference: i64) -> InputEvent {
    InputEvent {
        key,
        value,
        difference,
    }
}

fn canonical_u64(value: u64) -> Vec<u8> {
    let mut encoded = vec![1];
    encoded.extend_from_slice(&value.to_be_bytes());
    encoded
}

fn oracle_claims() -> Vec<(usize, Vec<InputEvent>)> {
    vec![
        (
            0,
            vec![
                event(Some(1), 10, 2),
                event(None, 11, 1),
                event(Some(2), 12, 1),
                event(Some(3), 13, 1),
            ],
        ),
        (1, vec![event(Some(1), 20, 3), event(None, 21, 2)]),
        (
            1,
            vec![
                event(Some(2), 22, 1),
                event(Some(2), 23, 1),
                event(Some(2), 22, -1),
            ],
        ),
        (
            1,
            vec![
                event(Some(3), 30, 1),
                event(Some(3), 30, -1),
                event(Some(3), 31, 2),
            ],
        ),
        (
            0,
            vec![
                event(Some(1), 10, -1),
                event(Some(2), 12, -1),
                event(None, 11, -1),
            ],
        ),
        (
            1,
            vec![
                event(Some(1), 20, -2),
                event(Some(1), 20, -1),
                event(Some(3), 31, -2),
                event(None, 21, -2),
            ],
        ),
        (0, vec![event(Some(1), 10, -1), event(Some(3), 13, -1)]),
    ]
}

fn residual_oracle_claims(residual: ResidualCase) -> Vec<(usize, Vec<InputEvent>)> {
    let (dominant_left, marginal_left, first_right, second_right, roundtrip_right, never) =
        match residual {
            ResidualCase::GreaterThan => (10, 5, 7, 4, 3, (1, 20)),
            ResidualCase::LessThan => (0, 5, 3, 6, 7, (20, 1)),
            _ => panic!("the relational residual oracle needs an ordered predicate"),
        };
    vec![
        (
            0,
            vec![
                event(Some(1), dominant_left, 2),
                event(Some(1), marginal_left, 1),
                event(Some(2), never.0, 1),
                event(None, 99, 1),
            ],
        ),
        (
            1,
            vec![
                event(Some(1), first_right, 3),
                event(Some(2), never.1, 1),
                event(None, -99, 2),
            ],
        ),
        (
            1,
            vec![
                event(Some(1), first_right, 2),
                event(Some(1), first_right, -1),
            ],
        ),
        (1, vec![event(Some(1), second_right, 1)]),
        (
            0,
            vec![
                event(Some(1), dominant_left, -1),
                event(Some(1), marginal_left, 2),
                event(Some(1), marginal_left, -1),
            ],
        ),
        (1, vec![event(Some(1), first_right, -4)]),
        (1, vec![event(Some(1), second_right, -1)]),
        (
            1,
            vec![
                event(Some(1), roundtrip_right, 1),
                event(Some(1), roundtrip_right, -1),
            ],
        ),
        (
            0,
            vec![
                event(Some(1), dominant_left, -1),
                event(Some(1), marginal_left, -2),
                event(Some(2), never.0, -1),
                event(None, 99, -1),
            ],
        ),
        (1, vec![event(Some(2), never.1, -1), event(None, -99, -2)]),
    ]
}

#[test]
fn every_kind_has_a_literal_tag_layout_and_exact_nullable_schema() {
    for kind in KINDS {
        let definition = definition(kind);
        assert_eq!(definition.join_kind(), kind);
        let decoded = assert_literal_definition(
            &definition,
            kind_fixture(kind),
            16,
            OperationKind::TurnTransform(NonZeroU32::new(2).unwrap()),
        );
        let expected_data = if kind == EquiJoinKind::Inner {
            vec![
                "equi_join.left_rows",
                "equi_join.right_rows",
                "equi_join.continuation",
            ]
        } else {
            vec![
                "equi_join.left_rows",
                "equi_join.right_rows",
                "equi_join.continuation",
                "equi_join.key_counts",
            ]
        };
        assert_eq!(resource_names(kind, false), expected_data);

        let binding =
            construct_checked(decoded.as_ref(), &[left_schema(), right_schema()]).unwrap();
        let output = binding.as_ref().unwrap();
        assert_eq!(
            output
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            output_names(kind)
        );
        let expected_nullable: &[bool] = match kind {
            EquiJoinKind::Inner => &[true, false, true, false],
            EquiJoinKind::LeftSemi | EquiJoinKind::LeftAnti => &[true, false],
            EquiJoinKind::LeftOuter => &[true, false, true, true],
            EquiJoinKind::FullOuter => &[true, true, true, true],
        };
        assert_eq!(
            output
                .fields()
                .iter()
                .map(|field| field.is_nullable())
                .collect::<Vec<_>>(),
            expected_nullable
        );
    }
}

#[test]
fn residual_round_trips_with_qualified_pair_binding_and_selects_match_count_layout() {
    let expected_residual = col("left.left_value").gt(col("right.right_value"));
    for kind in KINDS {
        let definition = residual_definition(kind);
        assert_eq!(definition.residual(), Some(&expected_residual));
        let expected_data = if kind == EquiJoinKind::Inner {
            vec![
                "equi_join.left_rows",
                "equi_join.right_rows",
                "equi_join.continuation",
            ]
        } else {
            vec![
                "equi_join.left_rows",
                "equi_join.right_rows",
                "equi_join.continuation",
                "equi_join.match_counts",
            ]
        };
        assert_eq!(resource_names(kind, true), expected_data);
        construct_checked(&definition, &[left_schema(), right_schema()]).unwrap();
    }

    let definition = residual_definition(EquiJoinKind::LeftSemi);
    let decoded = assert_literal_definition(
        &definition,
        LEFT_SEMI_RESIDUAL_V1,
        16,
        OperationKind::TurnTransform(NonZeroU32::new(2).unwrap()),
    );
    let output = construct_checked(decoded.as_ref(), &[left_schema(), right_schema()])
        .unwrap()
        .unwrap();
    assert_eq!(output.fields().len(), left_schema().fields().len());
}

#[test]
fn residual_binding_rejects_non_boolean_missing_and_ambiguous_columns() {
    let cases: [(Expr, ResidualErrorPredicate); 3] = [
        (col("left.left_value"), |error: &EquiJoinSchemaError| {
            matches!(
                error,
                EquiJoinSchemaError::ResidualType {
                    actual: DataType::Int64
                }
            )
        }),
        (
            col("left.missing").eq(lit(0_i64)),
            |error: &EquiJoinSchemaError| {
                matches!(error, EquiJoinSchemaError::ResidualExpression { .. })
            },
        ),
        (col("key").eq(lit(0_u64)), |error: &EquiJoinSchemaError| {
            matches!(error, EquiJoinSchemaError::ResidualExpression { .. })
        }),
    ];
    for (residual, expected) in cases {
        let definition = EquiJoinDefinition::try_new(
            EquiJoinKind::LeftSemi,
            [(col("key"), col("key"))],
            output_names(EquiJoinKind::LeftSemi).iter().copied(),
            Some(residual),
        )
        .unwrap();
        let Err(OperationBindError::Rejected { source }) =
            construct_checked(&definition, &[left_schema(), right_schema()])
        else {
            panic!("invalid residual unexpectedly bound")
        };
        let error = source.downcast_ref::<EquiJoinSchemaError>().unwrap();
        assert!(expected(error), "unexpected residual rejection: {error}");
    }
}

#[test]
fn definition_rejects_a_non_immutable_residual() {
    assert!(matches!(
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("key"), col("key"))],
            output_names(EquiJoinKind::Inner).iter().copied(),
            Some(placeholder("$1")),
        ),
        Err(EquiJoinDefinitionError::NonImmutableResidual)
    ));
}

#[test]
fn tag_16_rejects_invalid_kind_residual_marker_and_removed_payloads() {
    let mut invalid_kind = decode_hex(INNER_V1);
    invalid_kind[DEFINITION_HEADER_BYTES] = u8::MAX;
    assert_eq!(
        decode_definition(&invalid_kind).unwrap_err(),
        DefinitionCodecError::InvalidPayload("equi-join kind is invalid")
    );

    let mut invalid_marker = decode_hex(INNER_V1);
    *invalid_marker.last_mut().unwrap() = u8::MAX;
    assert_eq!(
        decode_definition(&invalid_marker).unwrap_err(),
        DefinitionCodecError::InvalidPayload("equi-join residual marker is invalid")
    );

    let mut missing_marker = decode_hex(INNER_V1);
    missing_marker.pop();
    assert_eq!(
        decode_definition(&missing_marker).unwrap_err(),
        DefinitionCodecError::Truncated
    );

    assert!(
        decode_definition(&decode_hex(OLD_INNER_ONLY_TAG_16_V1)).is_err(),
        "the rewritten v1 decoder accepted the removed Inner-only payload"
    );
}

#[test]
fn five_kinds_match_a_naive_relation_oracle_across_multiplicity_nulls_and_flips() {
    let claims = oracle_claims();

    for kind in KINDS {
        let mut fixture = Fixture::new(kind);
        let mut oracle = NaiveRelation::default();
        for (port, events) in &claims {
            let expected = oracle.apply_claim(kind, *port, events);
            let actual = fixture.run(*port, events).unwrap();
            assert_events(kind, &actual, expected);
        }
    }
}

#[test]
fn residual_kinds_match_independent_row_support_oracles_in_both_input_directions() {
    for residual in [ResidualCase::GreaterThan, ResidualCase::LessThan] {
        let claims = residual_oracle_claims(residual);
        for kind in KINDS {
            let mut fixture = Fixture::with_residual(kind, residual);
            let mut oracle = NaiveRelation::with_residual(residual);
            for (port, events) in &claims {
                let expected = oracle.apply_claim(kind, *port, events);
                let actual = fixture.run(*port, events).unwrap();
                assert_events(kind, &actual, expected);
            }
        }
    }
}

#[test]
fn false_and_null_residuals_never_qualify_for_any_join_kind() {
    let claims = [
        (0, vec![event(Some(1), 10, 2), event(None, 11, 1)]),
        (1, vec![event(Some(1), -5, 3), event(None, -6, 2)]),
        (0, vec![event(Some(1), 10, -1)]),
        (1, vec![event(Some(1), -5, -2)]),
    ];

    for residual in [ResidualCase::False, ResidualCase::Null] {
        for kind in KINDS {
            let mut fixture = Fixture::with_residual(kind, residual);
            let mut oracle = NaiveRelation::with_residual(residual);
            for (port, events) in &claims {
                let expected = oracle.apply_claim(kind, *port, events);
                let actual = fixture.run(*port, events).unwrap();
                assert_events(kind, &actual, expected);
            }
        }
    }
}

#[test]
fn residual_left_only_skips_stable_multiplicity_without_scanning_the_opposite_rows() {
    let key = canonical_u64(23);
    let corrupt_row = vec![u8::MAX; 32];
    let left = [event(Some(23), 10, 1)];
    let right = [event(Some(23), 5, 1)];

    for kind in [EquiJoinKind::LeftSemi, EquiJoinKind::LeftAnti] {
        let mut fixture = Fixture::with_residual(kind, ResidualCase::GreaterThan);
        fixture.run(0, &left).unwrap();
        fixture.run(1, &right).unwrap();
        fixture = fixture.corrupt_raw_row(0, &key, &corrupt_row, 1);

        assert!(
            fixture.run(1, &right).unwrap().is_empty(),
            "{kind:?} emitted output for a stable right multiplicity change"
        );

        fixture = fixture.corrupt_raw_row(1, &key, &corrupt_row, 1);
        let output = output_events(kind, &fixture.run(0, &left).unwrap());
        if kind == EquiJoinKind::LeftSemi {
            assert_eq!(
                output,
                vec![(
                    OutputRow {
                        left_key: Some(23),
                        left_value: Some(10),
                        right_key: None,
                        right_value: None,
                    },
                    1,
                )]
            );
        } else {
            assert!(output.is_empty());
        }
    }
}

#[test]
fn residual_left_only_uses_same_claim_shadow_support_for_a_stable_left_row() {
    let right = [event(Some(31), 5, 1)];
    let left = [
        event(Some(31), 10, 1),
        event(Some(31), 10, 1),
        event(Some(31), 10, -1),
    ];

    for kind in [EquiJoinKind::LeftSemi, EquiJoinKind::LeftAnti] {
        let residual = ResidualCase::GreaterThan;
        let mut fixture = Fixture::with_residual(kind, residual);
        let mut oracle = NaiveRelation::with_residual(residual);
        let expected = oracle.apply_claim(kind, 1, &right);
        assert_events(kind, &fixture.run(1, &right).unwrap(), expected);
        let expected = oracle.apply_claim(kind, 0, &left);
        assert_events(kind, &fixture.run(0, &left).unwrap(), expected);
    }
}

#[test]
fn residual_left_only_qualifying_pages_replay_without_intermediate_output() {
    let right = (0_i64..257)
        .map(|value| event(Some(41), value, 1))
        .collect::<Vec<_>>();
    let inserted = [event(Some(41), 900, 1)];
    let retracted = [event(Some(41), 900, -1)];

    for kind in [EquiJoinKind::LeftSemi, EquiJoinKind::LeftAnti] {
        let residual = ResidualCase::GreaterThan;
        let mut fixture = Fixture::with_residual(kind, residual);
        let mut oracle = NaiveRelation::with_residual(residual);

        let expected = oracle.apply_claim(kind, 1, &right);
        assert_events(kind, &fixture.run(1, &right).unwrap(), expected);
        let expected = oracle.apply_claim(kind, 0, &inserted);
        let input = input_change(0, &inserted);

        assert!(matches!(
            fixture.commit_once(0, &input).unwrap(),
            Action::Commit(None)
        ));
        fixture = fixture.reopen();

        let (committed, rolled_back_complete, committed_turns) =
            commit_prefix_and_rollback_complete(&mut fixture, 0, &input);
        assert!(
            committed_turns > 0,
            "{kind:?} did not page its qualifying scan"
        );
        assert!(
            committed.is_empty(),
            "{kind:?} emitted per-candidate rows from its left-only path"
        );

        fixture = fixture.reopen();
        let retried_complete = fixture.run(0, &inserted).unwrap();
        assert_events(
            kind,
            &retried_complete,
            output_events(kind, &rolled_back_complete),
        );
        assert_events(kind, &retried_complete, expected);

        let expected = oracle.apply_claim(kind, 0, &retracted);
        assert_events(kind, &fixture.run(0, &retracted).unwrap(), expected);
    }
}

#[test]
fn presence_kinds_reopen_after_probe_and_rolled_back_first_and_last_emit_pages() {
    let left = (0..257)
        .map(|value| InputEvent {
            key: Some(7),
            value,
            difference: 1,
        })
        .collect::<Vec<_>>();
    let trigger = [InputEvent {
        key: Some(7),
        value: 900,
        difference: 1,
    }];

    for kind in PRESENCE_KINDS {
        let mut fixture = Fixture::new(kind);
        fixture.run(0, &left).unwrap();
        let mut oracle = NaiveRelation::default();
        oracle.apply_claim(kind, 0, &left);
        let expected = oracle.apply_claim(kind, 1, &trigger);
        let input = input_change(1, &trigger);

        assert!(matches!(
            fixture.commit_once(1, &input).unwrap(),
            Action::Commit(None)
        ));
        fixture = fixture.reopen();

        let mut outputs = rollback_and_commit_first_output_page(&mut fixture, 1, &input);
        fixture = fixture.reopen();
        outputs.extend(fixture.run(1, &trigger).unwrap());
        if matches!(kind, EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter) {
            assert_outer_corrections_are_paired(&outputs);
        }
        assert_events(kind, &outputs, expected);

        let retract = [InputEvent {
            difference: -1,
            ..trigger[0].clone()
        }];
        let expected = oracle.apply_claim(kind, 1, &retract);
        let input = input_change(1, &retract);
        assert!(matches!(
            fixture.commit_once(1, &input).unwrap(),
            Action::Commit(None)
        ));
        fixture = fixture.reopen();
        let mut outputs = rollback_and_commit_first_output_page(&mut fixture, 1, &input);
        fixture = fixture.reopen();
        outputs.extend(fixture.run(1, &retract).unwrap());
        if matches!(kind, EquiJoinKind::LeftOuter | EquiJoinKind::FullOuter) {
            assert_outer_corrections_are_paired(&outputs);
        }
        assert_events(kind, &outputs, expected);
    }
}

#[test]
fn residual_presence_paging_replays_probe_shadow_cleanup_and_late_emit() {
    let mut candidates = (0_i64..767)
        .map(|value| event(Some(19), value, 1))
        .collect::<Vec<_>>();
    // Canonical positive i64 rows sort before -1, so the only qualifying
    // candidate is the 768th and forces every Probe and Emit page to run.
    candidates.push(event(Some(19), -1, 1));
    assert_eq!(candidates.len(), 768);

    let residual = ResidualCase::GreaterThan;
    let kind = EquiJoinKind::LeftOuter;
    let mut fixture = Fixture::with_residual(kind, residual);
    let mut oracle = NaiveRelation::with_residual(residual);
    let expected = oracle.apply_claim(kind, 1, &candidates);
    assert_events(kind, &fixture.run(1, &candidates).unwrap(), expected);

    let inserted = [event(Some(19), 0, 1)];
    let expected = oracle.apply_claim(kind, 0, &inserted);
    let input = input_change(0, &inserted);

    // Commit the first Probe page, reopen, then prove the next Probe page is
    // transactionally replayable from the same cursor.
    assert!(matches!(
        fixture.commit_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    fixture = fixture.reopen();
    assert!(matches!(
        fixture.rollback_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(
        fixture.commit_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    fixture = fixture.reopen();

    // The third 256-candidate page ends Probe exactly at the turn boundary,
    // leaving ClearShadow durable for reopen.
    assert!(matches!(
        fixture.commit_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    fixture = fixture.reopen();

    // Clearing the preflight shadow and starting Emit share one transaction;
    // rolling it back must restore both the shadow and the continuation.
    assert!(matches!(
        fixture.rollback_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(
        fixture.commit_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    fixture = fixture.reopen();

    // Roll back the final Complete carrying the late match, reopen, and prove
    // it is produced exactly once by the retried durable suffix.
    let (mut outputs, rolled_back_complete, committed_turns) =
        commit_prefix_and_rollback_complete(&mut fixture, 0, &input);
    assert!(committed_turns > 0);
    fixture = fixture.reopen();
    let retried_complete = fixture.run(0, &inserted).unwrap();
    assert_events(
        kind,
        &retried_complete,
        output_events(kind, &rolled_back_complete),
    );
    outputs.extend(retried_complete);
    assert_events(kind, &outputs, expected);

    let retracted = [event(Some(19), 0, -1)];
    let expected = oracle.apply_claim(kind, 0, &retracted);
    assert_events(kind, &fixture.run(0, &retracted).unwrap(), expected);
}

#[test]
fn residual_full_outer_replays_coalesced_counts_and_multi_page_shadow_cleanup() {
    let left = (0_i64..257)
        .map(|value| event(Some(29), value, 1))
        .collect::<Vec<_>>();
    let right = [event(Some(29), -1, 1)];
    let kind = EquiJoinKind::FullOuter;
    let residual = ResidualCase::GreaterThan;
    let mut fixture = Fixture::with_residual(kind, residual);
    let mut oracle = NaiveRelation::with_residual(residual);

    let expected = oracle.apply_claim(kind, 0, &left);
    assert_events(kind, &fixture.run(0, &left).unwrap(), expected);
    let expected = oracle.apply_claim(kind, 1, &right);
    let input = input_change(1, &right);

    // Every intermediate page is first rolled back and then replayed. The 257
    // qualifying left rows create 258 shadow keys, so cleanup itself is paged.
    let (mut output, rolled_back_complete, committed_turns) =
        commit_prefix_and_rollback_complete(&mut fixture, 1, &input);
    assert!(committed_turns >= 4);
    fixture = fixture.reopen();
    let retried_complete = fixture.run(1, &right).unwrap();
    assert_events(
        kind,
        &retried_complete,
        output_events(kind, &rolled_back_complete),
    );
    output.extend(retried_complete);
    assert_events(kind, &output, expected);

    let retracted = [event(Some(29), -1, -1)];
    let expected = oracle.apply_claim(kind, 1, &retracted);
    assert_events(kind, &fixture.run(1, &retracted).unwrap(), expected);
}

#[test]
fn residual_clear_shadow_rejects_a_cursor_that_skips_remaining_counts() {
    let left = (0_i64..257)
        .map(|value| event(Some(37), value, 1))
        .collect::<Vec<_>>();
    let right = [event(Some(37), -1, 1)];
    let kind = EquiJoinKind::FullOuter;
    let residual = ResidualCase::GreaterThan;
    let mut fixture = Fixture::with_residual(kind, residual);
    let mut oracle = NaiveRelation::with_residual(residual);
    let expected = oracle.apply_claim(kind, 0, &left);
    assert_events(kind, &fixture.run(0, &left).unwrap(), expected);
    let expected = oracle.apply_claim(kind, 1, &right);
    let input = input_change(1, &right);

    for _ in 0..3 {
        assert!(matches!(
            fixture.commit_once(1, &input).unwrap(),
            Action::Commit(None)
        ));
    }
    let mut original = None;
    fixture = fixture.rewrite_raw_continuation(|value| {
        assert_eq!(value[0], 2, "unexpected continuation version");
        assert_eq!(value[2], 1, "the test did not reach ClearShadow");
        assert_eq!(value[12], 1, "the cleanup page did not retain a cursor");
        original = Some(value.clone());
        value.truncate(13);
        value.extend_from_slice(&34_u64.to_be_bytes());
        value.extend_from_slice(&[1, 1]);
        value.extend_from_slice(&[u8::MAX; 32]);
    });

    let error = fixture.commit_once(1, &input).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::InvalidContinuation(
            "shadow cleanup cursor skipped a remaining count"
        ))
    ));

    let original = original.expect("the original cleanup continuation was captured");
    fixture = fixture.rewrite_raw_continuation(move |value| *value = original);
    let Action::Commit(Some(first_output)) = fixture.commit_once(1, &input).unwrap() else {
        panic!("the restored cleanup did not enter a paged Emit");
    };

    let mut original = None;
    fixture = fixture.rewrite_raw_continuation(|value| {
        assert_eq!(value[2], 2, "the test did not reach Emit");
        assert_eq!(value[11], 1, "the Emit page did not find a match");
        assert_eq!(value[12], 1, "the Emit page did not retain a cursor");
        original = Some(value.clone());
        value[12] = 0;
        value.truncate(13);
    });
    let error = fixture.commit_once(1, &input).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::InvalidContinuation(
            "matched row has no committed opposite-row cursor"
        ))
    ));

    let original = original.expect("the original Emit continuation was captured");
    fixture = fixture.rewrite_raw_continuation(move |value| *value = original);
    let mut output = vec![first_output];
    output.extend(fixture.run(1, &right).unwrap());
    assert_events(kind, &output, expected);
}

#[test]
fn counted_kinds_reopen_after_a_rolled_back_final_complete_without_partial_state() {
    let left = (0..257)
        .map(|value| event(Some(11), value, 1))
        .collect::<Vec<_>>();
    let inserted = [event(Some(11), 900, 1)];
    let retracted = [event(Some(11), 900, -1)];
    let replacement = [event(Some(11), 901, 1)];

    for kind in PRESENCE_KINDS {
        let mut fixture = Fixture::new(kind);
        fixture.run(0, &left).unwrap();
        let mut oracle = NaiveRelation::default();
        oracle.apply_claim(kind, 0, &left);
        let mut speculative = oracle.clone();
        let expected_insert = speculative.apply_claim(kind, 1, &inserted);
        let input = input_change(1, &inserted);

        let (mut committed, rolled_back_complete, committed_turns) =
            commit_prefix_and_rollback_complete(&mut fixture, 1, &input);
        assert!(
            committed_turns > 0,
            "{kind:?} reached Complete without a durable continuation"
        );
        fixture = fixture.reopen();
        let retried_complete = fixture.run(1, &inserted).unwrap();
        assert_events(
            kind,
            &retried_complete,
            output_events(kind, &rolled_back_complete),
        );
        committed.extend(retried_complete);
        assert_events(kind, &committed, expected_insert);
        oracle.apply_claim(kind, 1, &inserted);
        let expected_retract = oracle.apply_claim(kind, 1, &retracted);
        assert_events(kind, &fixture.run(1, &retracted).unwrap(), expected_retract);

        let error = fixture.run(1, &retracted).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::NegativeWeight)
        ));
        assert_events(
            kind,
            &fixture.run(1, &replacement).unwrap(),
            oracle.apply_claim(kind, 1, &replacement),
        );
    }
}

#[test]
fn last_presence_transition_accepts_the_exact_i64_minimum_difference() {
    let mut fixture = Fixture::new(EquiJoinKind::LeftSemi);
    fixture.run(1, &[event(Some(8), 80, 1)]).unwrap();
    let positive = fixture
        .run(0, &[event(Some(8), 8, i64::MAX), event(Some(8), 8, 1)])
        .unwrap();
    assert_eq!(
        positive
            .iter()
            .flat_map(|change| change.diffs().values().iter().copied())
            .collect::<Vec<_>>(),
        [i64::MAX, 1]
    );

    let output = fixture.run(1, &[event(Some(8), 80, -1)]).unwrap();
    assert_eq!(output_events(EquiJoinKind::LeftSemi, &output).len(), 1);
    assert_eq!(output[0].diffs().value(0), i64::MIN);
}

#[test]
fn presence_outputs_overflow_before_any_state_is_committed() {
    let huge = [InputEvent {
        key: Some(5),
        value: 50,
        difference: i64::MAX,
    }];
    let one_more = [InputEvent {
        key: Some(5),
        value: 50,
        difference: 2,
    }];
    let one_less = [InputEvent {
        key: Some(5),
        value: 50,
        difference: -2,
    }];
    let first_match = [InputEvent {
        key: Some(5),
        value: 500,
        difference: 1,
    }];

    for kind in PRESENCE_KINDS {
        let mut fixture = Fixture::new(kind);
        fixture.run(0, &huge).unwrap();
        fixture.run(0, &one_more).unwrap();
        let input = input_change(1, &first_match);
        let error = fixture.rollback_once(1, &input).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::OutputDifferenceOverflow)
        ));

        fixture.run(0, &one_less).unwrap();
        assert!(
            !fixture.run(1, &first_match).unwrap().is_empty(),
            "{kind:?} overflow left partial state"
        );
    }
}

#[test]
fn a_late_outer_overflow_rejects_the_whole_claim_before_earlier_output() {
    let mut fixture = Fixture::new(EquiJoinKind::FullOuter);
    fixture.run(0, &[event(Some(1), 10, 1)]).unwrap();
    fixture.run(0, &[event(Some(2), 20, i64::MAX)]).unwrap();
    fixture.run(0, &[event(Some(2), 20, 2)]).unwrap();

    let invalid = input_change(1, &[event(Some(1), 100, 1), event(Some(2), 200, 1)]);
    let error = fixture.rollback_once(1, &invalid).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::OutputDifferenceOverflow)
    ));

    let output = fixture.run(1, &[event(Some(1), 100, 1)]).unwrap();
    assert_events(
        EquiJoinKind::FullOuter,
        &output,
        vec![
            (
                OutputRow {
                    left_key: Some(1),
                    left_value: Some(10),
                    right_key: None,
                    right_value: None,
                },
                -1,
            ),
            (
                OutputRow {
                    left_key: Some(1),
                    left_value: Some(10),
                    right_key: Some(1),
                    right_value: Some(100),
                },
                1,
            ),
        ],
    );
}

#[test]
fn probe_rejects_a_late_corrupt_row_before_output_with_or_without_a_residual() {
    let valid_right = (0..257)
        .map(|value| event(Some(42), value, 1))
        .collect::<Vec<_>>();
    let left = [event(Some(42), 900, 1)];
    let input = input_change(0, &left);
    let key = canonical_u64(42);
    let corrupt_row = vec![u8::MAX; 32];

    for residual in [ResidualCase::None, ResidualCase::GreaterThan] {
        let mut fixture = Fixture::with_residual(EquiJoinKind::Inner, residual);
        assert!(fixture.run(1, &valid_right).unwrap().is_empty());
        fixture = fixture.corrupt_raw_row(1, &key, &corrupt_row, 1);

        assert!(matches!(
            fixture.commit_once(0, &input).unwrap(),
            Action::Commit(None)
        ));
        fixture = fixture.reopen();
        let error = fixture.commit_once(0, &input).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::CanonicalRow { .. })
        ));

        fixture = fixture.corrupt_raw_row(1, &key, &corrupt_row, -1);
        let output = fixture.run(0, &left).unwrap();
        let mut values = output_events(EquiJoinKind::Inner, &output)
            .into_iter()
            .map(|(row, difference)| {
                assert_eq!(difference, 1);
                row.right_value.unwrap()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (0..257).collect::<Vec<_>>());

        let retract = [event(Some(42), 900, -1)];
        assert_eq!(
            output_events(EquiJoinKind::Inner, &fixture.run(0, &retract).unwrap()).len(),
            257
        );
        let error = fixture.run(0, &retract).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EquiJoinError>(),
            Some(EquiJoinError::NegativeWeight)
        ));
    }
}
