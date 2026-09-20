use crate::support::construct_checked;
use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    Expr, OperationBindError, OperationDefinition, OperationKind, RuntimeResource, ScalarValue,
    col, lit,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn,
        transform::{
            AsOfDirection, AsOfEqualityKey, AsOfEqualityMode, AsOfEquidistantPreference,
            AsOfJoinDefinition, AsOfJoinError, AsOfJoinKind, AsOfJoinSchemaError, AsOfOrderKey,
            AsOfTieBreak, AsOfTieFallback,
        },
    },
};
use dogpaddle_store::{Store, StoreSetup, Transactions};

use crate::support::{TestStore, assert_literal_definition, commit_ready, rollback_ready};

const OPERATION_PREFIX: &str = "operation";
const ASOF_RESOURCE_NAMES: [&str; 3] = [
    "asof_join.left_rows",
    "asof_join.right_rows",
    "asof_join.continuation",
];

const ASOF_JOIN_LITERAL: &str = "646f67706164646c652e6f7065726174696f6e000001001101000101000000000100000000090a070a0567726f7570000000090a070a0567726f757000000001000000060a040a026174000000060a040a02617400000000000000070000000a6c6566745f67726f7570000000076c6566745f6174000000076c6566745f69640000000b72696768745f67726f75700000000872696768745f61740000000e72696768745f7072696f726974790000000872696768745f696400";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LeftRow {
    group: Option<String>,
    at: Option<i64>,
    id: i64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RightRow {
    group: Option<String>,
    at: Option<i64>,
    priority: Option<i64>,
    id: i64,
}

#[derive(Clone, Debug)]
struct LeftEvent {
    row: LeftRow,
    difference: i64,
}

#[derive(Clone, Debug)]
struct RightEvent {
    row: RightRow,
    difference: i64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OutputRow {
    Left(LeftRow),
    Pair(LeftRow, Option<RightRow>),
}

#[derive(Clone, Copy)]
struct Config {
    kind: AsOfJoinKind,
    direction: AsOfDirection,
    equality: Option<AsOfEqualityMode>,
    second_order: bool,
    tie: bool,
    tie_fallback: AsOfTieFallback,
    tolerance: Option<u128>,
}

impl Config {
    const fn backward(kind: AsOfJoinKind) -> Self {
        Self {
            kind,
            direction: AsOfDirection::Backward { allow_exact: true },
            equality: Some(AsOfEqualityMode::Equal),
            second_order: false,
            tie: false,
            tie_fallback: AsOfTieFallback::CanonicalAscending,
            tolerance: None,
        }
    }
}

#[derive(Clone, Default)]
struct Oracle {
    left: BTreeMap<LeftRow, u64>,
    right: BTreeMap<RightRow, u64>,
}

struct Fixture {
    config: Config,
    definition: AsOfJoinDefinition,
    root: TestStore,
    operation: Operation,
    transactions: Transactions,
}

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Utf8, true),
        Field::new("at", DataType::Int64, true),
        Field::new("left_id", DataType::Int64, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Utf8, true),
        Field::new("at", DataType::Int64, true),
        Field::new("priority", DataType::Int64, true),
        Field::new("right_id", DataType::Int64, false),
    ]))
}

fn output_names(kind: AsOfJoinKind) -> &'static [&'static str] {
    match kind {
        AsOfJoinKind::LeftSemi | AsOfJoinKind::LeftAnti => &["left_group", "left_at", "left_id"],
        AsOfJoinKind::Inner | AsOfJoinKind::LeftOuter => &[
            "left_group",
            "left_at",
            "left_id",
            "right_group",
            "right_at",
            "right_priority",
            "right_id",
        ],
    }
}

fn definition(config: Config) -> AsOfJoinDefinition {
    definition_with_residual(config, None)
}

fn definition_with_residual(config: Config, residual: Option<Expr>) -> AsOfJoinDefinition {
    let equalities = config
        .equality
        .into_iter()
        .map(|mode| AsOfEqualityKey::new(mode, col("group"), col("group")));
    let mut orders = vec![AsOfOrderKey::new(col("at"), col("at"))];
    if config.second_order {
        orders.push(AsOfOrderKey::new(col("left_id"), col("right_id")));
    }
    let ties = config
        .tie
        .then(|| AsOfTieBreak::new(col("priority"), true, false));
    AsOfJoinDefinition::try_new(
        config.kind,
        config.direction,
        equalities,
        orders,
        ties,
        config.tie_fallback,
        config.tolerance,
        output_names(config.kind).iter().copied(),
        residual,
    )
    .unwrap()
}

fn left_change(events: &[LeftEvent]) -> Change {
    let groups = events
        .iter()
        .map(|event| event.row.group.as_deref())
        .collect::<Vec<_>>();
    Change::try_new(
        RecordBatch::try_new(
            left_schema(),
            vec![
                Arc::new(StringArray::from(groups)),
                Arc::new(Int64Array::from(
                    events.iter().map(|event| event.row.at).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    events.iter().map(|event| event.row.id).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(
            events
                .iter()
                .map(|event| event.difference)
                .collect::<Vec<_>>(),
        ),
    )
    .unwrap()
}

fn right_change(events: &[RightEvent]) -> Change {
    let groups = events
        .iter()
        .map(|event| event.row.group.as_deref())
        .collect::<Vec<_>>();
    Change::try_new(
        RecordBatch::try_new(
            right_schema(),
            vec![
                Arc::new(StringArray::from(groups)),
                Arc::new(Int64Array::from(
                    events.iter().map(|event| event.row.at).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    events
                        .iter()
                        .map(|event| event.row.priority)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    events.iter().map(|event| event.row.id).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(
            events
                .iter()
                .map(|event| event.difference)
                .collect::<Vec<_>>(),
        ),
    )
    .unwrap()
}

#[test]
fn repeated_wide_index_expressions_are_bounded_before_turn_work() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Utf8, false),
        Field::new("at", DataType::Int64, false),
    ]));
    let definition = AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        (0..128)
            .map(|_| AsOfEqualityKey::new(AsOfEqualityMode::Equal, col("payload"), col("payload"))),
        [AsOfOrderKey::new(col("at"), col("at"))],
        std::iter::empty::<AsOfTieBreak>(),
        AsOfTieFallback::CanonicalAscending,
        None,
        ["left_payload", "left_at", "right_payload", "right_at"],
        None,
    )
    .unwrap();
    let root = TestStore::new();
    let schemas = [Arc::clone(&schema), Arc::clone(&schema)];
    let (mut operation, mut transactions) = construct_join_operation(&root, &definition, &schemas);
    let payload = "x".repeat(512 * 1024);
    let change = Change::try_new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![payload])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();

    let error = commit_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &change,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AsOfJoinError>(),
        Some(AsOfJoinError::PreparedClaimTooLarge { .. })
    ));
}

impl Fixture {
    fn new(config: Config) -> Self {
        let definition = definition(config);
        Self::with_definition(config, definition)
    }

    fn with_definition(config: Config, definition: AsOfJoinDefinition) -> Self {
        let root = TestStore::new();
        let schemas = [left_schema(), right_schema()];
        let (operation, transactions) = construct_join_operation(&root, &definition, &schemas);
        Self {
            config,
            definition,
            root,
            operation,
            transactions,
        }
    }

    fn reopen(self) -> Self {
        let Self {
            config,
            definition,
            root,
            operation,
            transactions,
        } = self;
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let operation =
            reconstruct_join_operation(&store, &definition, &[left_schema(), right_schema()]);
        Self {
            config,
            definition,
            root,
            operation,
            transactions: store.into_transactions(),
        }
    }

    fn run(&mut self, port: usize, change: &Change) -> Result<Vec<Change>, OperationError> {
        let mut outputs = Vec::new();
        for _ in 0..20_000 {
            match commit_ready(
                &mut self.operation,
                Some(OperationInput { port, change }),
                &mut self.transactions,
            )? {
                Action::Commit(output) => outputs.extend(output),
                Action::Complete(output) => {
                    outputs.extend(output);
                    return Ok(outputs);
                }
                Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
            }
        }
        panic!("ASOF join did not complete a bounded test Claim")
    }

    fn commit_once(&mut self, port: usize, change: &Change) -> Result<Action, OperationError> {
        commit_ready(
            &mut self.operation,
            Some(OperationInput { port, change }),
            &mut self.transactions,
        )
    }

    fn rollback_once(&mut self, port: usize, change: &Change) -> Result<Action, OperationError> {
        rollback_ready(
            &mut self.operation,
            Some(OperationInput { port, change }),
            &mut self.transactions,
        )
    }
}

fn construct_join_operation(
    root: &TestStore,
    definition: &dyn OperationDefinition,
    schemas: &[SchemaRef],
) -> (Operation, Transactions) {
    let mut setup = StoreSetup::new();
    let constructed = definition
        .construct(
            schemas,
            &mut setup.data_scope(),
            OPERATION_PREFIX,
            RuntimeResource::none(),
        )
        .unwrap();
    let (operation, _) = constructed.into_parts();
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    (operation, transactions)
}

fn reconstruct_join_operation(
    store: &Store,
    definition: &dyn OperationDefinition,
    schemas: &[SchemaRef],
) -> Operation {
    let constructed = definition
        .construct(
            schemas,
            &mut store.data_scope(),
            OPERATION_PREFIX,
            RuntimeResource::none(),
        )
        .unwrap();
    constructed.into_parts().0
}

impl Oracle {
    fn output(&self, config: Config) -> BTreeMap<OutputRow, u64> {
        let mut output = BTreeMap::new();
        for (left, weight) in &self.left {
            let winner = self.winner(left, config);
            let row = match config.kind {
                AsOfJoinKind::Inner => {
                    winner.map(|right| OutputRow::Pair(left.clone(), Some(right)))
                }
                AsOfJoinKind::LeftOuter => Some(OutputRow::Pair(left.clone(), winner)),
                AsOfJoinKind::LeftSemi => winner.map(|_| OutputRow::Left(left.clone())),
                AsOfJoinKind::LeftAnti => winner.is_none().then(|| OutputRow::Left(left.clone())),
            };
            if let Some(row) = row {
                output.insert(row, *weight);
            }
        }
        output
    }

    fn winner(&self, left: &LeftRow, config: Config) -> Option<RightRow> {
        let left_at = left.at?;
        let mut eligible = self
            .right
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(row, _)| row)
            .filter(|right| equality_matches(left, right, config.equality))
            .filter(|right| right.at.is_some())
            .filter(|right| order_eligible(left, right, config))
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return None;
        }

        let selected_order = match config.direction {
            AsOfDirection::Backward { .. } => eligible
                .iter()
                .map(|row| order_tuple_right(row, config))
                .max()
                .unwrap(),
            AsOfDirection::Forward { .. } => eligible
                .iter()
                .map(|row| order_tuple_right(row, config))
                .min()
                .unwrap(),
            AsOfDirection::Nearest { equidistant, .. } => {
                let mut orders = eligible
                    .iter()
                    .map(|row| order_tuple_right(row, config))
                    .collect::<Vec<_>>();
                orders.sort();
                orders.dedup();
                orders
                    .into_iter()
                    .min_by_key(|order| {
                        let value = order[0];
                        let distance = left_at.abs_diff(value);
                        let preference = match equidistant {
                            AsOfEquidistantPreference::Backward => value > left_at,
                            AsOfEquidistantPreference::Forward => value < left_at,
                        };
                        (distance, preference)
                    })
                    .unwrap()
            }
        };
        eligible.retain(|row| order_tuple_right(row, config) == selected_order);

        if config.tie {
            let best = eligible
                .iter()
                .map(|row| (row.priority.is_some(), row.priority.unwrap_or_default()))
                .max()
                .unwrap();
            eligible
                .retain(|row| (row.priority.is_some(), row.priority.unwrap_or_default()) == best);
        }
        match config.tie_fallback {
            AsOfTieFallback::Reject if eligible.len() > 1 => {
                panic!("ambiguous oracle winner is tested through the runtime error path")
            }
            AsOfTieFallback::Reject | AsOfTieFallback::CanonicalAscending => {
                eligible.into_iter().min().cloned()
            }
            AsOfTieFallback::CanonicalDescending => eligible.into_iter().max().cloned(),
        }
    }

    fn apply_left(&mut self, events: &[LeftEvent], config: Config) -> BTreeMap<OutputRow, i128> {
        let before = self.output(config);
        for event in events {
            adjust(&mut self.left, event.row.clone(), event.difference);
        }
        relation_delta(&before, &self.output(config))
    }

    fn apply_right(&mut self, events: &[RightEvent], config: Config) -> BTreeMap<OutputRow, i128> {
        let before = self.output(config);
        for event in events {
            adjust(&mut self.right, event.row.clone(), event.difference);
        }
        relation_delta(&before, &self.output(config))
    }
}

fn equality_matches(left: &LeftRow, right: &RightRow, equality: Option<AsOfEqualityMode>) -> bool {
    match equality {
        None => true,
        Some(AsOfEqualityMode::Equal) => {
            left.group.is_some() && right.group.is_some() && left.group == right.group
        }
        Some(AsOfEqualityMode::NotDistinct) => left.group == right.group,
    }
}

fn order_tuple_left(row: &LeftRow, config: Config) -> Vec<i64> {
    let mut order = vec![row.at.expect("eligible left order is non-NULL")];
    if config.second_order {
        order.push(row.id);
    }
    order
}

fn order_tuple_right(row: &RightRow, config: Config) -> Vec<i64> {
    let mut order = vec![row.at.expect("eligible right order is non-NULL")];
    if config.second_order {
        order.push(row.id);
    }
    order
}

fn order_eligible(left: &LeftRow, right: &RightRow, config: Config) -> bool {
    let left_order = order_tuple_left(left, config);
    let right_order = order_tuple_right(right, config);
    let eligible = match config.direction {
        AsOfDirection::Backward { allow_exact } => {
            right_order < left_order || (allow_exact && right_order == left_order)
        }
        AsOfDirection::Forward { allow_exact } => {
            right_order > left_order || (allow_exact && right_order == left_order)
        }
        AsOfDirection::Nearest { allow_exact, .. } => allow_exact || right_order != left_order,
    };
    eligible
        && config
            .tolerance
            .is_none_or(|tolerance| u128::from(left_order[0].abs_diff(right_order[0])) <= tolerance)
}

fn adjust<K: Ord + Clone>(rows: &mut BTreeMap<K, u64>, row: K, difference: i64) {
    let before = rows.get(&row).copied().unwrap_or(0);
    let after = if difference >= 0 {
        before.checked_add(difference.unsigned_abs()).unwrap()
    } else {
        before.checked_sub(difference.unsigned_abs()).unwrap()
    };
    if after == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, after);
    }
}

fn relation_delta(
    before: &BTreeMap<OutputRow, u64>,
    after: &BTreeMap<OutputRow, u64>,
) -> BTreeMap<OutputRow, i128> {
    let mut delta = BTreeMap::new();
    for (row, weight) in before {
        *delta.entry(row.clone()).or_default() -= i128::from(*weight);
    }
    for (row, weight) in after {
        *delta.entry(row.clone()).or_default() += i128::from(*weight);
    }
    delta.retain(|_, difference| *difference != 0);
    delta
}

fn observed(outputs: &[Change], kind: AsOfJoinKind) -> BTreeMap<OutputRow, i128> {
    let mut result = BTreeMap::new();
    for (row, difference) in observed_sequence(outputs, kind) {
        *result.entry(row).or_default() += i128::from(difference);
    }
    result.retain(|_, difference| *difference != 0);
    result
}

fn observed_sequence(outputs: &[Change], kind: AsOfJoinKind) -> Vec<(OutputRow, i64)> {
    let mut sequence = Vec::new();
    for output in outputs {
        for index in 0..output.num_rows() {
            sequence.push((output_row(output, index, kind), output.diffs().value(index)));
        }
    }
    sequence
}

fn output_row(output: &Change, index: usize, kind: AsOfJoinKind) -> OutputRow {
    let left_groups = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let left_at = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_id = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left = LeftRow {
        group: (!left_groups.is_null(index)).then(|| left_groups.value(index).to_owned()),
        at: (!left_at.is_null(index)).then(|| left_at.value(index)),
        id: left_id.value(index),
    };
    if matches!(kind, AsOfJoinKind::LeftSemi | AsOfJoinKind::LeftAnti) {
        return OutputRow::Left(left);
    }

    let right_id = output
        .records()
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right = if right_id.is_null(index) {
        None
    } else {
        let groups = output
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let at = output
            .records()
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let priority = output
            .records()
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        Some(RightRow {
            group: (!groups.is_null(index)).then(|| groups.value(index).to_owned()),
            at: (!at.is_null(index)).then(|| at.value(index)),
            priority: (!priority.is_null(index)).then(|| priority.value(index)),
            id: right_id.value(index),
        })
    };
    OutputRow::Pair(left, right)
}

fn left(group: Option<&str>, at: Option<i64>, id: i64, difference: i64) -> LeftEvent {
    LeftEvent {
        row: LeftRow {
            group: group.map(str::to_owned),
            at,
            id,
        },
        difference,
    }
}

fn right(
    group: Option<&str>,
    at: Option<i64>,
    priority: Option<i64>,
    id: i64,
    difference: i64,
) -> RightEvent {
    RightEvent {
        row: RightRow {
            group: group.map(str::to_owned),
            at,
            priority,
            id,
        },
        difference,
    }
}

fn apply_left(fixture: &mut Fixture, oracle: &mut Oracle, events: &[LeftEvent]) {
    let mut expected = oracle.clone();
    let delta = expected.apply_left(events, fixture.config);
    let output = fixture.run(0, &left_change(events)).unwrap();
    assert_eq!(observed(&output, fixture.config.kind), delta);
    *oracle = expected;
}

fn apply_right(fixture: &mut Fixture, oracle: &mut Oracle, events: &[RightEvent]) {
    let mut expected = oracle.clone();
    let delta = expected.apply_right(events, fixture.config);
    let output = fixture.run(1, &right_change(events)).unwrap();
    assert_eq!(observed(&output, fixture.config.kind), delta);
    *oracle = expected;
}

#[test]
fn literal_definition_has_tag_resources_exact_schema_and_decoded_runtime() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let source = definition(config);
    let decoded = assert_literal_definition(
        &source,
        ASOF_JOIN_LITERAL,
        17,
        OperationKind::TurnTransform(std::num::NonZeroU32::new(2).unwrap()),
    );
    assert_eq!(
        ASOF_RESOURCE_NAMES,
        [
            "asof_join.left_rows",
            "asof_join.right_rows",
            "asof_join.continuation",
        ]
    );

    let schemas = [left_schema(), right_schema()];
    let binding = construct_checked(decoded.as_ref(), &schemas).unwrap();
    let output = binding.as_ref().unwrap();
    assert_eq!(
        output
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("left_group", DataType::Utf8, true),
            ("left_at", DataType::Int64, true),
            ("left_id", DataType::Int64, false),
            ("right_group", DataType::Utf8, true),
            ("right_at", DataType::Int64, true),
            ("right_priority", DataType::Int64, true),
            ("right_id", DataType::Int64, true),
        ]
    );

    let root = TestStore::new();
    let (mut operation, mut transactions) =
        construct_join_operation(&root, decoded.as_ref(), &schemas);
    assert!(matches!(operation.turn(None).unwrap(), Turn::Idle));
    let right = right_change(&[right(Some("A"), Some(10), None, 10, 1)]);
    assert!(matches!(
        commit_ready(
            &mut operation,
            Some(OperationInput {
                port: 1,
                change: &right,
            }),
            &mut transactions,
        )
        .unwrap(),
        Action::Complete(None)
    ));
    let left = left_change(&[left(Some("A"), Some(12), 1, 1)]);
    let action = commit_ready(
        &mut operation,
        Some(OperationInput {
            port: 0,
            change: &left,
        }),
        &mut transactions,
    )
    .unwrap();
    assert_eq!(
        action_sequence(&action, config.kind),
        [(
            OutputRow::Pair(
                LeftRow {
                    group: Some("A".to_owned()),
                    at: Some(12),
                    id: 1,
                },
                Some(RightRow {
                    group: Some("A".to_owned()),
                    at: Some(10),
                    priority: None,
                    id: 10,
                }),
            ),
            1,
        )]
    );
}

#[test]
fn all_left_relational_kinds_and_search_directions_match_an_independent_oracle() {
    let directions = [
        AsOfDirection::Backward { allow_exact: true },
        AsOfDirection::Backward { allow_exact: false },
        AsOfDirection::Forward { allow_exact: true },
        AsOfDirection::Forward { allow_exact: false },
        AsOfDirection::Nearest {
            allow_exact: true,
            equidistant: AsOfEquidistantPreference::Backward,
        },
        AsOfDirection::Nearest {
            allow_exact: false,
            equidistant: AsOfEquidistantPreference::Forward,
        },
    ];
    let kinds = [
        AsOfJoinKind::Inner,
        AsOfJoinKind::LeftOuter,
        AsOfJoinKind::LeftSemi,
        AsOfJoinKind::LeftAnti,
    ];
    for kind in kinds {
        for direction in directions {
            let config = Config {
                direction,
                ..Config::backward(kind)
            };
            let mut fixture = Fixture::new(config);
            let mut oracle = Oracle::default();
            apply_right(
                &mut fixture,
                &mut oracle,
                &[
                    right(Some("A"), Some(10), None, 100, 1),
                    right(Some("A"), Some(20), None, 200, 1),
                    right(Some("B"), Some(12), None, 300, 1),
                    right(Some("A"), None, None, 400, 1),
                ],
            );
            apply_left(
                &mut fixture,
                &mut oracle,
                &[
                    left(Some("A"), Some(5), 1, 1),
                    left(Some("A"), Some(10), 2, 2),
                    left(Some("A"), Some(15), 3, 1),
                    left(Some("A"), Some(20), 4, 1),
                    left(Some("A"), Some(25), 5, 1),
                    left(Some("B"), Some(15), 6, 1),
                    left(Some("A"), None, 7, 1),
                ],
            );
        }
    }
}

#[test]
fn build_updates_rematch_history_and_ignore_positive_to_positive_candidate_weight() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut fixture = Fixture::new(config);
    let mut oracle = Oracle::default();
    let original = right(Some("A"), Some(10), None, 10, 1);
    apply_right(&mut fixture, &mut oracle, std::slice::from_ref(&original));
    apply_left(
        &mut fixture,
        &mut oracle,
        &[
            left(Some("A"), Some(12), 1, 2),
            left(Some("A"), Some(18), 2, 1),
            left(Some("A"), Some(24), 3, 3),
        ],
    );

    let replacement = right(Some("A"), Some(15), None, 15, 1);
    apply_right(
        &mut fixture,
        &mut oracle,
        std::slice::from_ref(&replacement),
    );
    apply_right(
        &mut fixture,
        &mut oracle,
        &[right(Some("A"), Some(15), None, 15, 4)],
    );
    apply_right(
        &mut fixture,
        &mut oracle,
        &[right(Some("A"), Some(15), None, 15, -5)],
    );
}

#[test]
fn null_safe_and_global_partitions_multi_order_tolerance_and_ties_are_explicit() {
    let cases = [
        Config {
            equality: Some(AsOfEqualityMode::NotDistinct),
            ..Config::backward(AsOfJoinKind::Inner)
        },
        Config {
            equality: None,
            second_order: true,
            ..Config::backward(AsOfJoinKind::Inner)
        },
        Config {
            direction: AsOfDirection::Nearest {
                allow_exact: true,
                equidistant: AsOfEquidistantPreference::Forward,
            },
            tolerance: Some(3),
            ..Config::backward(AsOfJoinKind::LeftOuter)
        },
        Config {
            tie: true,
            tie_fallback: AsOfTieFallback::CanonicalDescending,
            ..Config::backward(AsOfJoinKind::Inner)
        },
    ];

    for config in cases {
        let mut fixture = Fixture::new(config);
        let mut oracle = Oracle::default();
        apply_right(
            &mut fixture,
            &mut oracle,
            &[
                right(None, Some(10), Some(1), 1, 1),
                right(Some("A"), Some(10), Some(1), 2, 1),
                right(Some("A"), Some(10), Some(9), 3, 1),
                right(Some("B"), Some(20), None, 4, 1),
            ],
        );
        apply_left(
            &mut fixture,
            &mut oracle,
            &[
                left(None, Some(12), 1, 1),
                left(Some("A"), Some(12), 2, 1),
                left(Some("B"), Some(16), 3, 1),
            ],
        );
    }
}

#[test]
fn a_paged_build_correction_replays_after_rollback_and_reopen() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut fixture = Fixture::new(config);
    let mut oracle = Oracle::default();
    let left_events = (0..700)
        .map(|id| left(Some("A"), Some(100 + id), id, 1))
        .collect::<Vec<_>>();
    apply_left(&mut fixture, &mut oracle, &left_events);

    let event = right(Some("A"), Some(50), None, 9, 1);
    let change = right_change(std::slice::from_ref(&event));
    let rolled_back = fixture.rollback_once(1, &change).unwrap();
    let replayed = fixture.commit_once(1, &change).unwrap();
    assert_eq!(
        action_observed(rolled_back, config.kind),
        action_observed(replayed, config.kind)
    );

    fixture = fixture.reopen();
    let mut outputs = Vec::new();
    for _ in 0..20_000 {
        match fixture.commit_once(1, &change).unwrap() {
            Action::Commit(output) => outputs.extend(output),
            Action::Complete(output) => {
                outputs.extend(output);
                break;
            }
            Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
        }
    }
    let mut expected = oracle.clone();
    let delta = expected.apply_right(&[event], config);
    assert_eq!(observed(&outputs, config.kind), delta);
}

#[test]
fn a_large_right_claim_without_left_rows_is_budgeted_and_reopens() {
    let config = Config::backward(AsOfJoinKind::Inner);
    let mut fixture = Fixture::new(config);
    let events = (0..600)
        .map(|at| right(Some("A"), Some(at), None, at, 1))
        .collect::<Vec<_>>();
    let change = right_change(&events);

    assert!(
        matches!(
            fixture.commit_once(1, &change).unwrap(),
            Action::Commit(None)
        ),
        "an empty left relation must not let a large right Claim bypass the turn budget"
    );
    fixture = fixture.reopen();

    let mut committed_turns = 1;
    loop {
        match fixture.commit_once(1, &change).unwrap() {
            Action::Commit(None) => {
                committed_turns += 1;
                fixture = fixture.reopen();
            }
            Action::Complete(None) => break,
            Action::Commit(Some(_)) | Action::Complete(Some(_)) => {
                panic!("right preload into an empty left relation emitted output")
            }
            Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
        }
    }
    assert!(
        committed_turns >= 4,
        "both Probe and Emit must page a 600-row right Claim"
    );

    let probe = left(Some("A"), Some(1_000), 1, 1);
    let winner = events.last().unwrap();
    assert_eq!(
        observed_sequence(
            &fixture
                .run(0, &left_change(std::slice::from_ref(&probe)))
                .unwrap(),
            config.kind,
        ),
        [(OutputRow::Pair(probe.row, Some(winner.row.clone())), 1,)]
    );
}

#[test]
fn right_rematch_skips_persisted_null_order_left_history() {
    let config = Config::backward(AsOfJoinKind::Inner);
    let mut fixture = Fixture::new(config);
    let null_left = (0..513)
        .map(|id| left(Some("A"), None, id, 1))
        .collect::<Vec<_>>();
    assert!(fixture.run(0, &left_change(&null_left)).unwrap().is_empty());

    let right_events = (0..129)
        .map(|at| right(Some("A"), Some(at), None, at, 1))
        .collect::<Vec<_>>();
    let change = right_change(&right_events);
    let mut committed_turns = 0;
    loop {
        committed_turns += 1;
        match fixture.commit_once(1, &change).unwrap() {
            Action::Commit(None) => fixture = fixture.reopen(),
            Action::Complete(None) => break,
            Action::Commit(Some(_)) | Action::Complete(Some(_)) => {
                panic!("NULL-order left history produced an ASOF correction")
            }
            Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
        }
    }
    assert_eq!(
        committed_turns, 2,
        "only the 129 right events in Probe and Emit should consume turn work"
    );
}

#[test]
fn left_selection_skips_persisted_null_order_right_history() {
    let config = Config::backward(AsOfJoinKind::Inner);
    let mut fixture = Fixture::new(config);
    let null_right = (0..513)
        .map(|id| right(Some("A"), None, None, id, 1))
        .collect::<Vec<_>>();
    assert!(
        fixture
            .run(1, &right_change(&null_right))
            .unwrap()
            .is_empty()
    );
    fixture = fixture.reopen();

    let probe = left(Some("A"), Some(100), 1, 1);
    assert!(matches!(
        fixture
            .commit_once(0, &left_change(std::slice::from_ref(&probe)))
            .unwrap(),
        Action::Complete(None)
    ));
    fixture = fixture.reopen();

    let retract = left(Some("A"), Some(100), 1, -1);
    assert!(matches!(
        fixture
            .commit_once(0, &left_change(std::slice::from_ref(&retract)))
            .unwrap(),
        Action::Complete(None)
    ));
}

#[test]
fn residual_skips_false_and_null_near_candidates_then_rematches_in_event_order() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let residual = col("right.priority")
        .gt(lit(0_i64))
        .and(col("left.left_id").gt(col("right.right_id")));
    let definition = definition_with_residual(config, Some(residual));
    let mut fixture = Fixture::with_definition(config, definition);

    let far = right(Some("A"), Some(10), Some(1), 10, 1);
    fixture
        .run(
            1,
            &right_change(&[
                far.clone(),
                right(Some("A"), Some(20), None, 20, 1),
                right(Some("A"), Some(30), Some(-1), 30, 1),
            ]),
        )
        .unwrap();
    let probe = left(Some("A"), Some(35), 100, 1);
    assert_eq!(
        observed_sequence(
            &fixture
                .run(0, &left_change(std::slice::from_ref(&probe)))
                .unwrap(),
            config.kind
        ),
        [(OutputRow::Pair(probe.row.clone(), Some(far.row.clone())), 1,)]
    );

    let nearer = right(Some("A"), Some(25), Some(2), 25, 1);
    assert_eq!(
        observed_sequence(
            &fixture
                .run(1, &right_change(std::slice::from_ref(&nearer)))
                .unwrap(),
            config.kind,
        ),
        [
            (
                OutputRow::Pair(probe.row.clone(), Some(far.row.clone())),
                -1,
            ),
            (
                OutputRow::Pair(probe.row.clone(), Some(nearer.row.clone())),
                1,
            ),
        ]
    );

    let mut retract = nearer.clone();
    retract.difference = -1;
    assert_eq!(
        observed_sequence(
            &fixture.run(1, &right_change(&[retract])).unwrap(),
            config.kind,
        ),
        [
            (OutputRow::Pair(probe.row.clone(), Some(nearer.row)), -1),
            (OutputRow::Pair(probe.row, Some(far.row)), 1),
        ]
    );
}

#[test]
fn reject_ties_are_scoped_to_each_residual_eligible_probe_set() {
    let config = Config {
        tie: true,
        tie_fallback: AsOfTieFallback::Reject,
        ..Config::backward(AsOfJoinKind::Inner)
    };
    let definition =
        definition_with_residual(config, Some(col("left.left_id").gt(col("right.right_id"))));
    let mut fixture = Fixture::with_definition(config, definition);
    let first = right(Some("A"), Some(10), Some(5), 1, 1);
    let second = right(Some("A"), Some(10), Some(5), 3, 1);

    // An unused RHS partition is allowed to contain an unresolved rank, and
    // the residual can still make a concrete probe's eligible winner unique.
    assert!(
        fixture
            .run(1, &right_change(&[first.clone(), second.clone()]))
            .unwrap()
            .is_empty()
    );
    let unique = left(Some("A"), Some(20), 2, 1);
    assert_eq!(
        observed_sequence(
            &fixture
                .run(0, &left_change(std::slice::from_ref(&unique)))
                .unwrap(),
            config.kind,
        ),
        [(OutputRow::Pair(unique.row, Some(first.row.clone())), 1)]
    );

    let ambiguous = left(Some("A"), Some(20), 4, 1);
    let error = fixture
        .run(0, &left_change(std::slice::from_ref(&ambiguous)))
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AsOfJoinError>(),
        Some(AsOfJoinError::AmbiguousTie)
    ));

    let mut retract = second;
    retract.difference = -1;
    assert!(
        fixture
            .run(1, &right_change(&[retract]))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        observed_sequence(
            &fixture
                .run(0, &left_change(std::slice::from_ref(&ambiguous)))
                .unwrap(),
            config.kind,
        ),
        [(OutputRow::Pair(ambiguous.row, Some(first.row)), 1)]
    );
}

#[test]
fn whole_claim_preflight_rolls_back_late_negative_prefixes() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut fixture = Fixture::new(config);
    let accepted = left(Some("A"), Some(20), 1, 1);
    let invalid = left(Some("A"), Some(30), 2, -1);
    let error = fixture
        .run(0, &left_change(&[accepted.clone(), invalid]))
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AsOfJoinError>(),
        Some(AsOfJoinError::NegativeWeight)
    ));

    let candidate = right(Some("A"), Some(10), None, 10, 1);
    assert!(
        fixture
            .run(1, &right_change(std::slice::from_ref(&candidate)))
            .unwrap()
            .is_empty(),
        "the valid prefix of a rejected Claim leaked into left state"
    );
    assert_eq!(
        observed_sequence(
            &fixture
                .run(0, &left_change(std::slice::from_ref(&accepted)))
                .unwrap(),
            config.kind,
        ),
        [(OutputRow::Pair(accepted.row, Some(candidate.row)), 1)]
    );
}

#[test]
fn build_rematch_overflow_is_preflighted_and_i64_min_retraction_remains_valid() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut fixture = Fixture::new(config);
    let original = right(Some("A"), Some(10), None, 10, 1);
    fixture
        .run(1, &right_change(std::slice::from_ref(&original)))
        .unwrap();

    let probe = left(Some("A"), Some(20), 1, i64::MAX);
    fixture
        .run(0, &left_change(std::slice::from_ref(&probe)))
        .unwrap();
    fixture
        .run(0, &left_change(&[left(Some("A"), Some(20), 1, 1)]))
        .unwrap();

    let replacement = right(Some("A"), Some(15), None, 15, 1);
    let error = fixture.run(1, &right_change(&[replacement])).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AsOfJoinError>(),
        Some(AsOfJoinError::OutputDifferenceOverflow)
    ));

    let removal = left(Some("A"), Some(20), 1, i64::MIN);
    assert_eq!(
        observed_sequence(
            &fixture.run(0, &left_change(&[removal])).unwrap(),
            config.kind,
        ),
        [(OutputRow::Pair(probe.row, Some(original.row)), i64::MIN,)],
        "the failed replacement must not persist and -2^63 is representable"
    );
}

#[test]
fn rebatching_one_port_preserves_the_flattened_correction_sequence() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut batched = Fixture::new(config);
    let mut split = Fixture::new(config);
    let seed_right = [right(Some("A"), Some(0), None, 0, 1)];
    let seed_left = [
        left(Some("A"), Some(5), 5, 1),
        left(Some("A"), Some(15), 15, 1),
        left(Some("A"), Some(25), 25, 1),
    ];
    for fixture in [&mut batched, &mut split] {
        fixture.run(1, &right_change(&seed_right)).unwrap();
        fixture.run(0, &left_change(&seed_left)).unwrap();
    }

    let events = [
        right(Some("A"), Some(10), None, 10, 1),
        right(Some("A"), Some(20), None, 20, 1),
        right(Some("A"), Some(10), None, 10, -1),
        right(Some("A"), Some(12), None, 12, 1),
    ];
    let batched_sequence = observed_sequence(
        &batched.run(1, &right_change(&events)).unwrap(),
        config.kind,
    );
    let mut split_sequence = Vec::new();
    for event in events {
        split_sequence.extend(observed_sequence(
            &split.run(1, &right_change(&[event])).unwrap(),
            config.kind,
        ));
    }
    assert_eq!(batched_sequence, split_sequence);
}

#[test]
fn every_paged_phase_replays_after_rollback_and_repeated_reopen() {
    let config = Config::backward(AsOfJoinKind::LeftOuter);
    let mut fixture = Fixture::new(config);
    let candidates = (0..130)
        .map(|at| right(Some("A"), Some(at), None, at, 1))
        .collect::<Vec<_>>();
    fixture.run(1, &right_change(&candidates)).unwrap();
    let probes = (0..5)
        .map(|id| left(Some("A"), Some(200 + id), id, 1))
        .collect::<Vec<_>>();
    fixture.run(0, &left_change(&probes)).unwrap();

    let event = right(Some("A"), Some(150), None, 150, 1);
    let change = right_change(std::slice::from_ref(&event));
    let mut outputs = Vec::new();
    let mut completed = false;
    for turn in 0..2_000 {
        let rolled_back = fixture.rollback_once(1, &change).unwrap();
        let committed = fixture.commit_once(1, &change).unwrap();
        assert_eq!(
            action_sequence(&rolled_back, config.kind),
            action_sequence(&committed, config.kind),
            "turn {turn} changed after rollback"
        );
        match committed {
            Action::Commit(output) => outputs.extend(output),
            Action::Complete(output) => {
                outputs.extend(output);
                completed = true;
                break;
            }
            Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
        }
        fixture = fixture.reopen();
    }
    assert!(completed, "paged ASOF Claim did not complete");

    let mut oracle = Oracle::default();
    oracle.apply_right(&candidates, config);
    oracle.apply_left(&probes, config);
    let expected = oracle.apply_right(&[event], config);
    assert_eq!(observed(&outputs, config.kind), expected);
}

fn action_observed(action: Action, kind: AsOfJoinKind) -> BTreeMap<OutputRow, i128> {
    match action {
        Action::Commit(output) | Action::Complete(output) => {
            observed(&output.into_iter().collect::<Vec<_>>(), kind)
        }
        Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
    }
}

fn action_sequence(action: &Action, kind: AsOfJoinKind) -> Vec<(OutputRow, i64)> {
    match action {
        Action::Commit(output) | Action::Complete(output) => {
            output.as_ref().map_or_else(Vec::new, |change| {
                observed_sequence(std::slice::from_ref(change), kind)
            })
        }
        Action::Idle => panic!("ASOF join returned Idle for a pinned input"),
    }
}

fn scalar_order_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("at", data_type, true),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn scalar_order_definition(
    direction: AsOfDirection,
    tolerance: Option<u128>,
) -> AsOfJoinDefinition {
    AsOfJoinDefinition::try_new(
        AsOfJoinKind::LeftOuter,
        direction,
        std::iter::empty::<AsOfEqualityKey>(),
        [AsOfOrderKey::new(col("at"), col("at"))],
        std::iter::empty::<AsOfTieBreak>(),
        AsOfTieFallback::CanonicalAscending,
        tolerance,
        ["left_at", "left_id", "right_at", "right_id"],
        None,
    )
    .unwrap()
}

fn scalar_order_change(schema: &SchemaRef, values: &[ScalarValue], ids: &[i64]) -> Change {
    assert_eq!(values.len(), ids.len());
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            ScalarValue::iter_to_array(values.iter().cloned()).unwrap(),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1_i64; ids.len()])).unwrap()
}

fn run_claim(
    operation: &mut Operation,
    transactions: &mut Transactions,
    port: usize,
    change: &Change,
) -> Vec<Change> {
    let mut outputs = Vec::new();
    for _ in 0..20_000 {
        match commit_ready(
            operation,
            Some(OperationInput { port, change }),
            transactions,
        )
        .unwrap()
        {
            Action::Commit(output) => outputs.extend(output),
            Action::Complete(output) => {
                outputs.extend(output);
                return outputs;
            }
            Action::Idle => panic!("ASOF join returned Idle for a pinned scalar Claim"),
        }
    }
    panic!("ASOF join did not complete a bounded scalar Claim")
}

fn scalar_selection(
    data_type: DataType,
    left: ScalarValue,
    right: &[(ScalarValue, i64)],
    direction: AsOfDirection,
    tolerance: Option<u128>,
) -> Option<i64> {
    let schema = scalar_order_schema(data_type);
    let definition = scalar_order_definition(direction, tolerance);
    let root = TestStore::new();
    let schemas = [Arc::clone(&schema), Arc::clone(&schema)];
    let (mut operation, mut transactions) = construct_join_operation(&root, &definition, &schemas);

    let right_values = right
        .iter()
        .map(|(value, _)| value.clone())
        .collect::<Vec<_>>();
    let right_ids = right.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    assert!(
        run_claim(
            &mut operation,
            &mut transactions,
            1,
            &scalar_order_change(&schema, &right_values, &right_ids),
        )
        .is_empty()
    );
    let outputs = run_claim(
        &mut operation,
        &mut transactions,
        0,
        &scalar_order_change(&schema, &[left], &[99]),
    );

    let mut selected = Vec::new();
    for output in outputs {
        let right_ids = output
            .records()
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..output.num_rows() {
            assert_eq!(output.diffs().value(index), 1);
            selected.push((!right_ids.is_null(index)).then(|| right_ids.value(index)));
        }
    }
    assert_eq!(selected.len(), 1);
    selected[0]
}

type DistanceScalarCase = (
    &'static str,
    DataType,
    ScalarValue,
    ScalarValue,
    ScalarValue,
);

fn assert_distance_scalar_cases(cases: impl IntoIterator<Item = DistanceScalarCase>) {
    for (name, data_type, left, before, after) in cases {
        assert_eq!(
            scalar_selection(
                data_type,
                left,
                &[(before, 8), (after, 12)],
                AsOfDirection::Nearest {
                    allow_exact: false,
                    equidistant: AsOfEquidistantPreference::Backward,
                },
                Some(2),
            ),
            Some(8),
            "distance type {name} did not select its equidistant predecessor"
        );
    }
}

#[test]
fn every_integer_distance_scalar_executes_nearest_search_at_the_inclusive_boundary() {
    assert_distance_scalar_cases([
        (
            "Int8",
            DataType::Int8,
            ScalarValue::Int8(Some(10)),
            ScalarValue::Int8(Some(8)),
            ScalarValue::Int8(Some(12)),
        ),
        (
            "Int16",
            DataType::Int16,
            ScalarValue::Int16(Some(10)),
            ScalarValue::Int16(Some(8)),
            ScalarValue::Int16(Some(12)),
        ),
        (
            "Int32",
            DataType::Int32,
            ScalarValue::Int32(Some(10)),
            ScalarValue::Int32(Some(8)),
            ScalarValue::Int32(Some(12)),
        ),
        (
            "Int64",
            DataType::Int64,
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(8)),
            ScalarValue::Int64(Some(12)),
        ),
        (
            "UInt8",
            DataType::UInt8,
            ScalarValue::UInt8(Some(10)),
            ScalarValue::UInt8(Some(8)),
            ScalarValue::UInt8(Some(12)),
        ),
        (
            "UInt16",
            DataType::UInt16,
            ScalarValue::UInt16(Some(10)),
            ScalarValue::UInt16(Some(8)),
            ScalarValue::UInt16(Some(12)),
        ),
        (
            "UInt32",
            DataType::UInt32,
            ScalarValue::UInt32(Some(10)),
            ScalarValue::UInt32(Some(8)),
            ScalarValue::UInt32(Some(12)),
        ),
        (
            "UInt64",
            DataType::UInt64,
            ScalarValue::UInt64(Some(10)),
            ScalarValue::UInt64(Some(8)),
            ScalarValue::UInt64(Some(12)),
        ),
    ]);
}

#[test]
fn every_temporal_and_decimal_distance_scalar_executes_nearest_search() {
    let utc: Arc<str> = Arc::from("UTC");
    assert_distance_scalar_cases([
        (
            "Date32",
            DataType::Date32,
            ScalarValue::Date32(Some(10)),
            ScalarValue::Date32(Some(8)),
            ScalarValue::Date32(Some(12)),
        ),
        (
            "TimestampSecond",
            DataType::Timestamp(TimeUnit::Second, None),
            ScalarValue::TimestampSecond(Some(10), None),
            ScalarValue::TimestampSecond(Some(8), None),
            ScalarValue::TimestampSecond(Some(12), None),
        ),
        (
            "TimestampMillisecondWithTimezone",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::clone(&utc))),
            ScalarValue::TimestampMillisecond(Some(10), Some(Arc::clone(&utc))),
            ScalarValue::TimestampMillisecond(Some(8), Some(Arc::clone(&utc))),
            ScalarValue::TimestampMillisecond(Some(12), Some(Arc::clone(&utc))),
        ),
        (
            "TimestampMicrosecond",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ScalarValue::TimestampMicrosecond(Some(10), None),
            ScalarValue::TimestampMicrosecond(Some(8), None),
            ScalarValue::TimestampMicrosecond(Some(12), None),
        ),
        (
            "TimestampNanosecond",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            ScalarValue::TimestampNanosecond(Some(10), None),
            ScalarValue::TimestampNanosecond(Some(8), None),
            ScalarValue::TimestampNanosecond(Some(12), None),
        ),
        (
            "Decimal128",
            DataType::Decimal128(20, 4),
            ScalarValue::Decimal128(Some(10), 20, 4),
            ScalarValue::Decimal128(Some(8), 20, 4),
            ScalarValue::Decimal128(Some(12), 20, 4),
        ),
    ]);
}

#[test]
fn tolerance_zero_boundary_outside_and_equidistant_direction_are_distinct() {
    let data_type = DataType::Int64;
    let scalar = |value| ScalarValue::Int64(Some(value));
    assert_eq!(
        scalar_selection(
            data_type.clone(),
            scalar(10),
            &[(scalar(8), 8), (scalar(10), 10)],
            AsOfDirection::Backward { allow_exact: true },
            Some(0),
        ),
        Some(10)
    );
    assert_eq!(
        scalar_selection(
            data_type.clone(),
            scalar(10),
            &[(scalar(8), 8)],
            AsOfDirection::Backward { allow_exact: false },
            Some(2),
        ),
        Some(8),
        "tolerance is inclusive"
    );
    assert_eq!(
        scalar_selection(
            data_type.clone(),
            scalar(10),
            &[(scalar(8), 8)],
            AsOfDirection::Backward { allow_exact: false },
            Some(1),
        ),
        None,
        "a candidate outside tolerance must not match"
    );
    assert_eq!(
        scalar_selection(
            data_type,
            scalar(10),
            &[(scalar(8), 8), (scalar(12), 12)],
            AsOfDirection::Nearest {
                allow_exact: false,
                equidistant: AsOfEquidistantPreference::Forward,
            },
            Some(2),
        ),
        Some(12)
    );
}

#[test]
fn distance_uses_the_full_unsigned_range_without_narrowing() {
    let decimal_limit = 10_i128.pow(38) - 1;
    let decimal_distance = decimal_limit.unsigned_abs() * 2;
    for (name, data_type, left, right, tolerance) in [
        (
            "Int64",
            DataType::Int64,
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
            u128::from(u64::MAX),
        ),
        (
            "UInt64",
            DataType::UInt64,
            ScalarValue::UInt64(Some(u64::MIN)),
            ScalarValue::UInt64(Some(u64::MAX)),
            u128::from(u64::MAX),
        ),
        (
            "Decimal128",
            DataType::Decimal128(38, 0),
            ScalarValue::Decimal128(Some(-decimal_limit), 38, 0),
            ScalarValue::Decimal128(Some(decimal_limit), 38, 0),
            decimal_distance,
        ),
    ] {
        assert_eq!(
            scalar_selection(
                data_type,
                left,
                &[(right, 1)],
                AsOfDirection::Forward { allow_exact: false },
                Some(tolerance),
            ),
            Some(1),
            "distance for {name} was narrowed before comparison"
        );
    }

    assert_eq!(
        scalar_selection(
            DataType::Decimal128(38, 0),
            ScalarValue::Decimal128(Some(-decimal_limit), 38, 0),
            &[(ScalarValue::Decimal128(Some(decimal_limit), 38, 0), 1)],
            AsOfDirection::Forward { allow_exact: false },
            Some(decimal_distance - 1),
        ),
        None,
        "the largest Arrow Decimal128 distance exceeds a narrowed integer domain"
    );
}

#[test]
fn every_non_distance_order_scalar_executes_lexicographic_search_and_null_is_unmatched() {
    assert_eq!(
        scalar_selection(
            DataType::Boolean,
            ScalarValue::Boolean(Some(true)),
            &[
                (ScalarValue::Boolean(Some(false)), 0),
                (ScalarValue::Boolean(Some(true)), 1),
            ],
            AsOfDirection::Backward { allow_exact: false },
            None,
        ),
        Some(0)
    );
    assert_eq!(
        scalar_selection(
            DataType::Utf8,
            ScalarValue::Utf8(Some("aa".to_owned())),
            &[
                (ScalarValue::Utf8(Some("a".to_owned())), 1),
                (ScalarValue::Utf8(Some("aa".to_owned())), 2),
                (ScalarValue::Utf8(Some("b".to_owned())), 3),
            ],
            AsOfDirection::Backward { allow_exact: false },
            None,
        ),
        Some(1),
        "framed Utf8 prefix order changed"
    );
    assert_eq!(
        scalar_selection(
            DataType::Binary,
            ScalarValue::Binary(Some(vec![1, 0])),
            &[
                (ScalarValue::Binary(Some(vec![1])), 1),
                (ScalarValue::Binary(Some(vec![1, 0])), 2),
                (ScalarValue::Binary(Some(vec![2])), 3),
            ],
            AsOfDirection::Backward { allow_exact: false },
            None,
        ),
        Some(1),
        "framed Binary prefix order changed"
    );
    assert_eq!(
        scalar_selection(
            DataType::Null,
            ScalarValue::Null,
            &[(ScalarValue::Null, 1)],
            AsOfDirection::Backward { allow_exact: true },
            None,
        ),
        None,
        "a NULL order is never matchable even for null-safe equality partitions"
    );
}

fn tie_definition(
    descending: bool,
    nulls_first: bool,
    fallback: AsOfTieFallback,
) -> AsOfJoinDefinition {
    AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        [AsOfEqualityKey::new(
            AsOfEqualityMode::Equal,
            col("group"),
            col("group"),
        )],
        [AsOfOrderKey::new(col("at"), col("at"))],
        [AsOfTieBreak::new(col("priority"), descending, nulls_first)],
        fallback,
        None,
        output_names(AsOfJoinKind::Inner).iter().copied(),
        None,
    )
    .unwrap()
}

fn selected_fixture_right_id(outputs: &[Change]) -> i64 {
    let sequence = observed_sequence(outputs, AsOfJoinKind::Inner);
    assert_eq!(sequence.len(), 1);
    let (OutputRow::Pair(_, Some(right)), difference) = &sequence[0] else {
        panic!("expected one matched ASOF pair")
    };
    assert_eq!(*difference, 1);
    right.id
}

#[test]
fn tie_break_direction_null_placement_and_canonical_fallback_are_independent() {
    for (descending, nulls_first, expected) in [
        (false, true, 0),
        (false, false, 1),
        (true, true, 0),
        (true, false, 2),
    ] {
        let config = Config {
            tie: true,
            ..Config::backward(AsOfJoinKind::Inner)
        };
        let mut fixture = Fixture::with_definition(
            config,
            tie_definition(descending, nulls_first, AsOfTieFallback::CanonicalAscending),
        );
        fixture
            .run(
                1,
                &right_change(&[
                    right(Some("A"), Some(10), Some(2), 2, 1),
                    right(Some("A"), Some(10), None, 0, 1),
                    right(Some("A"), Some(10), Some(1), 1, 1),
                ]),
            )
            .unwrap();
        let outputs = fixture
            .run(0, &left_change(&[left(Some("A"), Some(20), 99, 1)]))
            .unwrap();
        assert_eq!(
            selected_fixture_right_id(&outputs),
            expected,
            "descending={descending}, nulls_first={nulls_first}"
        );
    }

    for (fallback, expected) in [
        (AsOfTieFallback::CanonicalAscending, 1),
        (AsOfTieFallback::CanonicalDescending, 2),
    ] {
        let config = Config {
            tie: true,
            tie_fallback: fallback,
            ..Config::backward(AsOfJoinKind::Inner)
        };
        let mut fixture = Fixture::with_definition(config, tie_definition(false, false, fallback));
        fixture
            .run(
                1,
                &right_change(&[
                    right(Some("A"), Some(10), Some(7), 2, 1),
                    right(Some("A"), Some(10), Some(7), 1, 1),
                ]),
            )
            .unwrap();
        let outputs = fixture
            .run(0, &left_change(&[left(Some("A"), Some(20), 99, 1)]))
            .unwrap();
        assert_eq!(selected_fixture_right_id(&outputs), expected);
    }
}

fn bind_rejection(definition: &AsOfJoinDefinition, schemas: &[SchemaRef]) -> OperationBindError {
    match construct_checked(definition, schemas) {
        Ok(_) => panic!("invalid ASOF definition unexpectedly bound"),
        Err(error) => error,
    }
}

fn rejected_asof_error(error: &OperationBindError) -> &AsOfJoinSchemaError {
    let OperationBindError::Rejected { source } = error else {
        panic!("expected the ASOF definition itself to reject binding")
    };
    source.downcast_ref::<AsOfJoinSchemaError>().unwrap()
}

#[test]
fn binding_rejects_unsupported_index_types_in_every_expression_role() {
    let unsupported = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Float64, false),
        Field::new("at", DataType::Int64, false),
        Field::new("tie", DataType::Float64, false),
    ]));
    let names = [
        "left_key",
        "left_at",
        "left_tie",
        "right_key",
        "right_at",
        "right_tie",
    ];

    let unsupported_equality = AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        [AsOfEqualityKey::new(
            AsOfEqualityMode::Equal,
            col("key"),
            col("key"),
        )],
        [AsOfOrderKey::new(col("at"), col("at"))],
        std::iter::empty::<AsOfTieBreak>(),
        AsOfTieFallback::Reject,
        None,
        names,
        None,
    )
    .unwrap();
    let error = bind_rejection(
        &unsupported_equality,
        &[Arc::clone(&unsupported), Arc::clone(&unsupported)],
    );
    assert!(matches!(
        rejected_asof_error(&error),
        AsOfJoinSchemaError::UnsupportedType {
            role: "equality",
            index: 0,
            data_type: DataType::Float64,
        }
    ));

    let unsupported_tie = AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        std::iter::empty::<AsOfEqualityKey>(),
        [AsOfOrderKey::new(col("at"), col("at"))],
        [AsOfTieBreak::new(col("tie"), false, false)],
        AsOfTieFallback::Reject,
        None,
        names,
        None,
    )
    .unwrap();
    let error = bind_rejection(
        &unsupported_tie,
        &[Arc::clone(&unsupported), Arc::clone(&unsupported)],
    );
    assert!(matches!(
        rejected_asof_error(&error),
        AsOfJoinSchemaError::UnsupportedType {
            role: "tie break",
            index: 0,
            data_type: DataType::Float64,
        }
    ));

    let unsupported_order = AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        std::iter::empty::<AsOfEqualityKey>(),
        [AsOfOrderKey::new(col("key"), col("key"))],
        std::iter::empty::<AsOfTieBreak>(),
        AsOfTieFallback::Reject,
        None,
        names,
        None,
    )
    .unwrap();
    let error = bind_rejection(
        &unsupported_order,
        &[Arc::clone(&unsupported), Arc::clone(&unsupported)],
    );
    assert!(matches!(
        rejected_asof_error(&error),
        AsOfJoinSchemaError::UnsupportedType {
            role: "order",
            index: 0,
            data_type: DataType::Float64,
        }
    ));
}

#[test]
fn binding_rejects_multi_order_distance_and_wrong_output_cardinality() {
    let integer = scalar_order_schema(DataType::Int64);
    for (direction, tolerance, feature) in [
        (
            AsOfDirection::Nearest {
                allow_exact: true,
                equidistant: AsOfEquidistantPreference::Backward,
            },
            None,
            "nearest direction",
        ),
        (
            AsOfDirection::Backward { allow_exact: true },
            Some(1),
            "tolerance",
        ),
    ] {
        let definition = AsOfJoinDefinition::try_new(
            AsOfJoinKind::Inner,
            direction,
            std::iter::empty::<AsOfEqualityKey>(),
            [
                AsOfOrderKey::new(col("at"), col("at")),
                AsOfOrderKey::new(col("id"), col("id")),
            ],
            std::iter::empty::<AsOfTieBreak>(),
            AsOfTieFallback::Reject,
            tolerance,
            ["left_at", "left_id", "right_at", "right_id"],
            None,
        )
        .unwrap();
        let error = bind_rejection(&definition, &[Arc::clone(&integer), Arc::clone(&integer)]);
        assert!(matches!(
            rejected_asof_error(&error),
            AsOfJoinSchemaError::DistanceOrder { feature: actual } if *actual == feature
        ));
    }

    let wrong_names = AsOfJoinDefinition::try_new(
        AsOfJoinKind::Inner,
        AsOfDirection::Backward { allow_exact: true },
        std::iter::empty::<AsOfEqualityKey>(),
        [AsOfOrderKey::new(col("at"), col("at"))],
        std::iter::empty::<AsOfTieBreak>(),
        AsOfTieFallback::Reject,
        None,
        ["only_one"],
        None,
    )
    .unwrap();
    let error = bind_rejection(&wrong_names, &[Arc::clone(&integer), integer]);
    assert!(matches!(
        rejected_asof_error(&error),
        AsOfJoinSchemaError::OutputNameCount {
            expected: 4,
            actual: 1,
        }
    ));
}
