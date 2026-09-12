use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DefinitionCodecError, OperationDefinition, OperationKind, col, decode_definition,
    operation::{
        Action, Operation, OperationError, OperationInput,
        transform::{EquiJoinDefinition, EquiJoinError, EquiJoinKind},
    },
};
use dogpaddle_store::{PartitionedMultiset, Store, Transactions};

use crate::support::{
    TestStore, assert_literal_definition, bind, commit_ready, data_names, decode_hex, materialize,
    rollback_ready,
};

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
}

struct Fixture {
    kind: EquiJoinKind,
    definition: EquiJoinDefinition,
    root: TestStore,
    operation: Operation,
    transactions: Transactions,
}

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
    EquiJoinDefinition::try_new(
        kind,
        [(col("key"), col("key"))],
        output_names(kind).iter().copied(),
    )
    .unwrap()
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
        let definition = definition(kind);
        let root = TestStore::new();
        let store = create_store(&root, &definition);
        let operation = open_operation(&store, &definition);
        Self {
            kind,
            definition,
            root,
            operation,
            transactions: store.into_transactions(),
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
        let operation = open_operation(&store, &definition);
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

    fn adjust_raw_row(self, port: usize, key: &[u8], row: &[u8], difference: i64) -> Self {
        assert_eq!(
            self.kind,
            EquiJoinKind::Inner,
            "raw row injection intentionally bypasses counted-kind key_counts"
        );
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
        let rows: PartitionedMultiset<Vec<u8>, Vec<u8>> = store.open_data(data_name).unwrap();
        let operation = open_operation(&store, &definition);
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
}

fn create_store(root: &TestStore, definition: &EquiJoinDefinition) -> Store {
    let mut store = Store::create(root.path()).unwrap();
    for declaration in definition.data() {
        declaration.create(&mut store, declaration.name()).unwrap();
    }
    store
}

fn open_operation(store: &Store, definition: &EquiJoinDefinition) -> Operation {
    let names = definition
        .data()
        .iter()
        .map(dogpaddle_operation::DataDeclaration::name)
        .collect::<Vec<_>>();
    materialize(definition, &[left_schema(), right_schema()], store, &names)
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
                .filter(|(right, _)| matchable(left, right))
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
                let matched = self.left.keys().any(|left| matchable(left, right));
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
}

fn matchable(left: &InputRow, right: &InputRow) -> bool {
    left.key.is_some() && left.key == right.key
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
        assert_eq!(data_names(&definition), expected_data);

        let binding = bind(decoded.as_ref(), &[left_schema(), right_schema()]).unwrap();
        let output = binding.output_schema().unwrap();
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
fn tag_16_rejects_an_invalid_kind_and_the_old_inner_only_payload() {
    let mut invalid_kind = decode_hex(INNER_V1);
    invalid_kind[DEFINITION_HEADER_BYTES] = u8::MAX;
    assert_eq!(
        decode_definition(&invalid_kind).unwrap_err(),
        DefinitionCodecError::InvalidPayload("equi-join kind is invalid")
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
fn probe_rejects_a_late_corrupt_persistent_row_without_partial_output_or_input_state() {
    let valid_right = (0..257)
        .map(|value| event(Some(42), value, 1))
        .collect::<Vec<_>>();
    let left = [event(Some(42), 900, 1)];
    let input = input_change(0, &left);
    let key = canonical_u64(42);
    let corrupt_row = vec![u8::MAX; 32];

    let mut fixture = Fixture::new(EquiJoinKind::Inner);
    assert!(fixture.run(1, &valid_right).unwrap().is_empty());
    fixture = fixture.adjust_raw_row(1, &key, &corrupt_row, 1);

    assert!(matches!(
        fixture.commit_once(0, &input).unwrap(),
        Action::Commit(None)
    ));
    let error = fixture.commit_once(0, &input).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EquiJoinError>(),
        Some(EquiJoinError::CanonicalRow { .. })
    ));

    fixture = fixture.adjust_raw_row(1, &key, &corrupt_row, -1);
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
