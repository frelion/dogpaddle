use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::{
    OperationKind,
    operation::{
        Action, Operation, OperationError, OperationInput, sink::DiscardDefinition,
        source::SequenceSourceDefinition, transform::CountDefinition,
    },
};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, Small, Store, StoreError, TransactionAccess,
    Transactions,
};

use crate::{
    build::FlowFactory,
    flow::{AdvanceOutcome, Flow},
};

use super::{
    ACTIVE_INPUT_KEY, CURSOR_ORIGIN, CompletionPlan, ConsumerCursor, Output, Station, StationParts,
    cursor_key, decode_active_input, decode_cursor, encode_active_input, encode_cursor,
    plan_complete, protocol::StationError,
};

type State = OrderedMap<Vec<u8>, Vec<u8>, Small>;
type Log = AppendLog<Vec<u8>>;

struct RuntimeFixture {
    _root: tempfile::TempDir,
    transactions: Transactions,
    reads: ReadTransactions,
    stations: Vec<Station>,
}

impl RuntimeFixture {
    fn try_step(&mut self, station: usize) -> Result<AdvanceOutcome, StationError> {
        self.stations[station].advance(&self.reads, &mut self.transactions)
    }

    fn step(&mut self, station: usize) -> AdvanceOutcome {
        self.try_step(station).unwrap()
    }

    fn cursor(&mut self, station: usize, input: usize) -> u64 {
        read_cursor(&self.stations[station], &mut self.transactions, input)
    }

    fn bounds(&mut self, station: usize) -> std::ops::Range<u64> {
        output_bounds(&self.stations[station], &mut self.transactions)
    }
}

struct MultiInputFixture {
    _root: tempfile::TempDir,
    transactions: Transactions,
    reads: ReadTransactions,
    station: Station,
}

impl MultiInputFixture {
    fn try_step(&mut self) -> Result<AdvanceOutcome, StationError> {
        self.station.advance(&self.reads, &mut self.transactions)
    }

    fn step(&mut self) -> AdvanceOutcome {
        self.try_step().unwrap()
    }

    fn active(&mut self) -> usize {
        read_active(&self.station, &mut self.transactions)
    }

    fn cursor(&mut self, input: usize) -> u64 {
        read_cursor(&self.station, &mut self.transactions, input)
    }

    fn bounds(&mut self, input: usize) -> std::ops::Range<u64> {
        output_bounds_log(
            self.station.inbox.ports()[input].output().log(),
            &mut self.transactions,
        )
    }
}

enum ScriptResult {
    Action(Action),
    Error,
}

struct ScriptedOperation {
    write: Option<(State, Vec<u8>)>,
    poison_with: Option<(State, tempfile::TempDir)>,
    result: ScriptResult,
}

impl ScriptedOperation {
    fn returning(action: Action) -> Self {
        Self {
            write: None,
            poison_with: None,
            result: ScriptResult::Action(action),
        }
    }

    fn writing(state: State, value: &[u8], result: ScriptResult) -> Self {
        Self {
            write: Some((state, value.to_vec())),
            poison_with: None,
            result,
        }
    }
}

impl Operation for ScriptedOperation {
    fn turn(
        &self,
        _input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        if let Some((state, value)) = &self.write {
            state.access(access)?.put(&b"attempt".to_vec(), value)?;
        }
        if let Some((foreign, _)) = &self.poison_with {
            assert!(matches!(
                foreign.access(access),
                Err(StoreError::WrongStore)
            ));
        }
        match &self.result {
            ScriptResult::Action(action) => Ok(action.clone()),
            ScriptResult::Error => Err(std::io::Error::other("planned turn failure").into()),
        }
    }
}

#[test]
fn input_state_keys_and_values_keep_the_v1_encoding() {
    assert_eq!(ACTIVE_INPUT_KEY, b"input/active");
    assert_eq!(CURSOR_ORIGIN, 0);
    assert_eq!(encode_active_input(0x0102_0304), [1, 2, 3, 4]);
    assert_eq!(decode_active_input(&[1, 2, 3, 4]), Some(0x0102_0304));
    assert_eq!(decode_active_input(&[1, 2, 3]), None);
    assert_eq!(cursor_key(0x0102_0304), b"input/01020304/cursor");
    assert_eq!(
        encode_cursor(0x0102_0304_0506_0708),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        decode_cursor(&[1, 2, 3, 4, 5, 6, 7, 8]),
        Some(0x0102_0304_0506_0708)
    );
    assert_eq!(decode_cursor(&[1, 2, 3, 4]), None);
}

#[test]
fn assembly_shares_each_unified_output_with_its_input_ports() {
    let fixture = source_count_sink(NonZeroU64::MAX, NonZeroU64::MAX);
    assert!(fixture.stations[0].inbox.ports().is_empty());
    assert_eq!(fixture.stations[1].inbox.ports().len(), 1);
    assert_eq!(fixture.stations[2].inbox.ports().len(), 1);
    assert!(fixture.stations[2].output.is_none());
    assert!(Arc::ptr_eq(
        fixture.stations[0].output.as_ref().unwrap(),
        fixture.stations[1].inbox.ports()[0].output(),
    ));
    assert!(Arc::ptr_eq(
        fixture.stations[1].output.as_ref().unwrap(),
        fixture.stations[2].inbox.ports()[0].output(),
    ));
}

#[test]
fn claim_trace_preserves_durable_identity_across_commit_pin_cache_loss_and_reopen() {
    let mut pinned = multi_input_station(Action::Idle);
    assert_eq!(pinned.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&pinned.station), Some((1, 0)));
    let memory_identity = claim_ptr(&pinned.station);
    let encoded = claim_bytes(&pinned.station);
    assert!(
        !pinned
            .station
            .inbox
            .intake(&pinned.reads, &mut pinned.transactions)
            .unwrap()
    );
    assert_eq!(claim_ptr(&pinned.station), memory_identity);

    let mut reopened = reopen_multi_input(pinned, Action::Idle);
    assert_eq!((reopened.active(), reopened.cursor(1)), (1, 0));
    assert_eq!(reopened.step(), AdvanceOutcome::Idle);
    assert_eq!(claim_id(&reopened.station), Some((1, 0)));
    assert_eq!(claim_bytes(&reopened.station), encoded);
    let replay_identity = claim_ptr(&reopened.station);
    reopened.station.operation = Box::new(ScriptedOperation::returning(Action::Commit(None)));
    assert_eq!(reopened.step(), AdvanceOutcome::Progressed);
    assert_eq!((reopened.active(), reopened.cursor(1)), (1, 0));
    assert_eq!(claim_ptr(&reopened.station), replay_identity);

    reopened.station.inbox.clear_cached_claim();
    assert!(
        !reopened
            .station
            .inbox
            .intake(&reopened.reads, &mut reopened.transactions)
            .unwrap()
    );
    assert_eq!(claim_id(&reopened.station), Some((1, 0)));
    assert_eq!(claim_bytes(&reopened.station), encoded);
}

#[test]
fn action_matrix_commits_exactly_the_allowed_effects() {
    let mut fixture = source_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.stations[1].inbox.state().clone();

    set_result(
        &mut fixture.stations[1],
        &state,
        b"idle",
        ScriptResult::Action(Action::Idle),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Idle);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);

    set_result(
        &mut fixture.stations[1],
        &state,
        b"error",
        ScriptResult::Error,
    );
    assert!(matches!(
        fixture.try_step(1),
        Err(StationError::Operation(_))
    ));
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);

    set_script(
        &mut fixture.stations[1],
        &state,
        b"commit",
        Action::Commit(Some(change(&[9]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    let claim_identity = claim_ptr(&fixture.stations[1]);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"commit".as_slice())
    );
    assert_eq!(fixture.cursor(1, 0), 0);
    assert_eq!(fixture.bounds(1), 0..1);

    set_script(
        &mut fixture.stations[1],
        &state,
        b"rejected-commit",
        Action::Commit(Some(change(&[10]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Backpressured);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"commit".as_slice())
    );
    assert_eq!(claim_ptr(&fixture.stations[1]), claim_identity);

    assert_eq!(fixture.step(2), AdvanceOutcome::Progressed);
    set_script(
        &mut fixture.stations[1],
        &state,
        b"complete",
        Action::Complete(Some(change(&[11]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"complete".as_slice())
    );
    assert_eq!((fixture.cursor(1, 0), fixture.bounds(0)), (1, 1..1));
    assert_eq!(claim_id(&fixture.stations[1]), None);

    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    set_script(
        &mut fixture.stations[1],
        &state,
        b"rejected-complete",
        Action::Complete(Some(change(&[12]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Backpressured);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"complete".as_slice())
    );
    assert_eq!(
        (fixture.cursor(1, 0), fixture.bounds(0), fixture.bounds(1)),
        (1, 1..2, 1..2)
    );
    assert_eq!(claim_id(&fixture.stations[1]), Some((0, 1)));
}

#[test]
fn completion_planner_matches_every_small_reachable_transition() {
    for input_count in 1..=3 {
        for claim_port in 0..input_count {
            for consumer_count in 1..=3 {
                for tail in 1..=3 {
                    for cursors in cursor_vectors(consumer_count, tail) {
                        let head = *cursors.iter().min().unwrap();
                        for consumer_slot in 0..consumer_count {
                            let claim_offset = cursors[consumer_slot];
                            if claim_offset == u64::try_from(tail).unwrap() {
                                continue;
                            }
                            let actual = plan_complete(
                                claim_port,
                                claim_offset,
                                claim_port,
                                NonZeroUsize::new(input_count).unwrap(),
                                consumer_slot,
                                head..u64::try_from(tail).unwrap(),
                                &cursors,
                            )
                            .unwrap();
                            let mut updated = cursors.clone();
                            updated[consumer_slot] += 1;
                            let next_head = *updated.iter().min().unwrap();
                            assert_eq!(
                                actual,
                                CompletionPlan {
                                    next_cursor: claim_offset + 1,
                                    next_active: (claim_port + 1) % input_count,
                                    reclaim_to: (next_head != head).then_some(next_head),
                                }
                            );
                            assert!(next_head == head || next_head == head + 1);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn completion_planner_rejects_each_invalid_durable_fact() {
    #[rustfmt::skip]
    let cases = [
        ("active", 0, 0, 1, 2, 0, 0..1, vec![0], "claimed input port 0 does not match durable active input port 1"),
        ("claim", 0, 1, 0, 1, 0, 0..2, vec![0], "claimed input offset 1 does not match durable consumer cursor 0"),
        ("tail", 0, 1, 0, 1, 0, 1..1, vec![1], "claimed input offset 1 is at output tail 1"),
        ("range", 0, 0, 0, 1, 0, 1..2, vec![0], "output consumer 0 cursor 0 is outside retained range [1, 2]"),
        ("head", 0, 1, 0, 1, 0, 0..2, vec![1], "output retention head 0 does not equal minimum consumer cursor 1"),
    ];
    for (name, port, offset, active, inputs, slot, bounds, cursors, expected) in cases {
        assert_eq!(
            plan_complete(
                port,
                offset,
                active,
                NonZeroUsize::new(inputs).unwrap(),
                slot,
                bounds,
                &cursors,
            )
            .unwrap_err()
            .to_string(),
            expected,
            "case {name}"
        );
    }
}

#[test]
fn complete_rejects_a_post_claim_active_mismatch_without_effects() {
    let mut fixture = multi_input_station(Action::Complete(None));
    assert!(
        fixture
            .station
            .inbox
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let claim = claim_ptr(&fixture.station);
    let transaction = fixture.transactions.begin().unwrap();
    let mut state = fixture
        .station
        .inbox
        .state()
        .access(transaction.access())
        .unwrap();
    state
        .put(&ACTIVE_INPUT_KEY.to_vec(), &encode_active_input(0).to_vec())
        .unwrap();
    assert!(state.remove(&cursor_key(1)).unwrap());
    transaction.commit().unwrap();

    assert!(matches!(
        fixture.station.process(&mut fixture.transactions),
        Err(StationError::ClaimActiveInputMismatch {
            claimed: 1,
            durable: 0
        })
    ));
    assert_eq!((fixture.active(), fixture.bounds(1)), (0, 0..1));
    assert_eq!(
        read_state(
            fixture.station.inbox.state(),
            &mut fixture.transactions,
            &cursor_key(1)
        ),
        None
    );
    assert_eq!(claim_ptr(&fixture.station), claim);
}

#[test]
fn duplicate_edges_share_one_output_but_acknowledge_independently() {
    let mut fixture = duplicate_input_station();
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!((fixture.cursor(0), fixture.cursor(1)), (1, 0));
    assert_eq!((fixture.active(), fixture.bounds(0)), (1, 0..1));
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!((fixture.cursor(1), fixture.active()), (1, 0));
    assert_eq!(fixture.bounds(0), 1..1);
}

#[test]
fn illegal_actions_roll_back_operation_state_and_claim_effects() {
    let mut sink = source_sink(1, NonZeroU64::MAX);
    assert_eq!(sink.step(0), AdvanceOutcome::Progressed);
    let state = sink.stations[1].inbox.state().clone();
    set_script(
        &mut sink.stations[1],
        &state,
        b"unexpected-output",
        Action::Complete(Some(change(&[9]))),
    );
    assert!(matches!(
        sink.try_step(1),
        Err(StationError::UnexpectedOutput)
    ));
    assert_eq!(read_attempt(&state, &mut sink.transactions), None);
    assert_eq!((sink.cursor(1, 0), sink.bounds(0)), (0, 0..1));
    assert_eq!(claim_id(&sink.stations[1]), Some((0, 0)));

    let mut source = source_sink(1, NonZeroU64::MAX);
    let state = source.stations[0].inbox.state().clone();
    set_script(
        &mut source.stations[0],
        &state,
        b"source-complete",
        Action::Complete(None),
    );
    assert!(matches!(
        source.try_step(0),
        Err(StationError::OperationCompletedWithoutInput)
    ));
    assert_eq!(read_attempt(&state, &mut source.transactions), None);
    assert_eq!(source.bounds(0), 0..0);
}

#[test]
fn failed_outer_commits_preserve_claim_and_roll_back_every_durable_effect() {
    let mut complete = multi_input_station(Action::Idle);
    assert!(
        complete
            .station
            .inbox
            .intake(&complete.reads, &mut complete.transactions)
            .unwrap()
    );
    let identity = claim_ptr(&complete.station);
    let state = complete.station.inbox.state().clone();
    complete.station.operation = Box::new(poisoned_script(
        &state,
        b"must-roll-back",
        Action::Complete(None),
    ));
    assert!(matches!(
        complete.station.process(&mut complete.transactions),
        Err(StationError::Store(StoreError::TransactionPoisoned))
    ));
    assert_eq!(read_attempt(&state, &mut complete.transactions), None);
    assert_eq!(
        (complete.active(), complete.cursor(1), complete.bounds(1)),
        (1, 0, 0..1)
    );
    assert_eq!(claim_ptr(&complete.station), identity);

    complete.station.operation = Box::new(ScriptedOperation::returning(Action::Complete(None)));
    assert_eq!(
        complete
            .station
            .process(&mut complete.transactions)
            .unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(claim_id(&complete.station), None);
    assert_eq!((complete.active(), complete.bounds(1)), (0, 1..1));
}

#[test]
fn completing_an_oversize_entry_restores_empty_log_admission() {
    let mut fixture = source_sink(1, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(fixture.step(0), AdvanceOutcome::Backpressured);
    assert_eq!(fixture.bounds(0), 0..1);
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    assert_eq!(fixture.bounds(0), 1..1);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(fixture.bounds(0), 1..2);
}

fn source_sink(consumer_count: usize, source_capacity: NonZeroU64) -> RuntimeFixture {
    let root = tempfile::tempdir().unwrap();
    let mut builder = FlowFactory::new(root.path().join("flow"));
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    builder.output_capacity_bytes(source, source_capacity);
    for index in 0..consumer_count {
        let sink = builder.station(format!("sink-{index}"), DiscardDefinition::new());
        builder.connect([source], sink);
    }
    fixture(root, builder.build().unwrap())
}

fn source_count_sink(source_capacity: NonZeroU64, count_capacity: NonZeroU64) -> RuntimeFixture {
    let root = tempfile::tempdir().unwrap();
    let mut builder = FlowFactory::new(root.path().join("flow"));
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(source, source_capacity);
    builder.output_capacity_bytes(count, count_capacity);
    builder.connect([source], count);
    builder.connect([count], sink);
    fixture(root, builder.build().unwrap())
}

fn multi_input_station(action: Action) -> MultiInputFixture {
    raw_station(&[0, 1], &[1], action)
}

fn duplicate_input_station() -> MultiInputFixture {
    raw_station(&[0, 0], &[0], Action::Complete(None))
}

fn raw_station(sources: &[usize], populated: &[usize], action: Action) -> MultiInputFixture {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let state = store.create_data::<State>("state").unwrap();
    let output_count = sources.iter().copied().max().unwrap() + 1;
    let outputs = (0..output_count)
        .map(|index| {
            store
                .create_data::<Log>(&format!("output-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let parts = station_parts(state.clone(), sources.len(), action);
    let (mut transactions, reads) = store.into_transactions().split();
    let transaction = transactions.begin().unwrap();
    parts.initialize_input_state(transaction.access()).unwrap();
    let encoded = encode_change(&change(&[7])).unwrap();
    for source in populated {
        outputs[*source]
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
    }
    transaction.commit().unwrap();
    let station = finish_station(parts, &state, &outputs, sources);
    MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
    }
}

fn reopen_multi_input(fixture: MultiInputFixture, action: Action) -> MultiInputFixture {
    let MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
    } = fixture;
    let path = root.path().join("flow");
    drop((transactions, reads, station));
    let store = Store::open(&path).unwrap();
    let state = store.open_data::<State>("state").unwrap();
    let outputs = (0..2)
        .map(|index| store.open_data::<Log>(&format!("output-{index}")).unwrap())
        .collect::<Vec<_>>();
    let station = finish_station(
        station_parts(state.clone(), 2, action),
        &state,
        &outputs,
        &[0, 1],
    );
    let (transactions, reads) = store.into_transactions().split();
    MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
    }
}

fn station_parts(state: State, input_count: usize, action: Action) -> StationParts {
    StationParts::new(
        state,
        Box::new(ScriptedOperation::returning(action)),
        OperationKind::Sink(NonZeroU32::new(u32::try_from(input_count).unwrap()).unwrap()),
        None,
    )
}

fn finish_station(parts: StationParts, state: &State, logs: &[Log], sources: &[usize]) -> Station {
    let outputs = logs
        .iter()
        .enumerate()
        .map(|(source, log)| {
            Arc::new(Output::new(
                log.clone(),
                NonZeroU64::MAX,
                sources
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| **candidate == source)
                    .map(|(input, _)| ConsumerCursor::new(ReadOnly::new(state.clone()), input))
                    .collect(),
            ))
        })
        .collect::<Vec<_>>();
    let mut slots = vec![0; outputs.len()];
    let inputs = sources
        .iter()
        .map(|source| {
            let port = outputs[*source].port(slots[*source]);
            slots[*source] += 1;
            port
        })
        .collect();
    parts.finish(inputs, None)
}

fn fixture(root: tempfile::TempDir, flow: Flow) -> RuntimeFixture {
    let (transactions, reads, stations) = flow.into_runtime_parts();
    RuntimeFixture {
        _root: root,
        transactions,
        reads,
        stations,
    }
}

fn set_script(station: &mut Station, state: &State, value: &[u8], action: Action) {
    set_result(station, state, value, ScriptResult::Action(action));
}

fn set_result(station: &mut Station, state: &State, value: &[u8], result: ScriptResult) {
    station.operation = Box::new(ScriptedOperation::writing(state.clone(), value, result));
}

fn poisoned_script(state: &State, value: &[u8], action: Action) -> ScriptedOperation {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("foreign")).unwrap();
    let foreign = store.create_data::<State>("state").unwrap();
    let mut operation =
        ScriptedOperation::writing(state.clone(), value, ScriptResult::Action(action));
    operation.poison_with = Some((foreign, root));
    operation
}

fn cursor_vectors(consumers: usize, entries: usize) -> Vec<Vec<u64>> {
    let radix = entries + 1;
    (0..radix.pow(u32::try_from(consumers).unwrap()))
        .map(|mut encoded| {
            (0..consumers)
                .map(|_| {
                    let cursor = u64::try_from(encoded % radix).unwrap();
                    encoded /= radix;
                    cursor
                })
                .collect()
        })
        .collect()
}

fn claim_id(station: &Station) -> Option<(usize, u64)> {
    station
        .inbox
        .cached_claim()
        .map(|claim| (claim.port(), claim.offset()))
}

fn claim_ptr(station: &Station) -> *const Change {
    std::ptr::from_ref(station.inbox.cached_claim().unwrap().change())
}

fn claim_bytes(station: &Station) -> Vec<u8> {
    encode_change(station.inbox.cached_claim().unwrap().change()).unwrap()
}

fn read_active(station: &Station, transactions: &mut Transactions) -> usize {
    let encoded = read_state(station.inbox.state(), transactions, ACTIVE_INPUT_KEY).unwrap();
    decode_active_input(&encoded).unwrap()
}

fn read_cursor(station: &Station, transactions: &mut Transactions, input: usize) -> u64 {
    let encoded = read_state(station.inbox.state(), transactions, &cursor_key(input)).unwrap();
    decode_cursor(&encoded).unwrap()
}

fn read_attempt(state: &State, transactions: &mut Transactions) -> Option<Vec<u8>> {
    read_state(state, transactions, b"attempt")
}

fn read_state(state: &State, transactions: &mut Transactions, key: &[u8]) -> Option<Vec<u8>> {
    let transaction = transactions.begin().unwrap();
    state
        .access(transaction.access())
        .unwrap()
        .get(&key.to_vec())
        .unwrap()
}

fn output_bounds(station: &Station, transactions: &mut Transactions) -> std::ops::Range<u64> {
    output_bounds_log(station.output.as_ref().unwrap().log(), transactions)
}

fn output_bounds_log(output: &Log, transactions: &mut Transactions) -> std::ops::Range<u64> {
    let transaction = transactions.begin().unwrap();
    output
        .access(transaction.access())
        .unwrap()
        .bounds()
        .unwrap()
}

fn change(values: &[u64]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values.to_vec()))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1; values.len()])).unwrap()
}
