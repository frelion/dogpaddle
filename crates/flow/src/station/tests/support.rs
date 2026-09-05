use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::{
    OperationKind,
    operation::{
        Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
        scan::SequenceScanDefinition, sink::DiscardDefinition,
        transform::RunningEventCountDefinition,
    },
};
use dogpaddle_store::{
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, Small, Store, StoreError, Transactions,
};

use crate::{
    build::FlowFactory,
    flow::{AdvanceOutcome, Flow},
};

use super::super::{
    ACTIVE_INPUT_KEY, ConsumerCursor, Output, Station, StationParts, cursor_key,
    decode_active_input, decode_cursor, protocol::StationError,
};

pub(super) type State = OrderedMap<Vec<u8>, Vec<u8>, Small>;
pub(super) type Log = AppendLog<Vec<u8>>;

pub(super) struct RuntimeFixture {
    pub(super) _root: tempfile::TempDir,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) stations: Vec<Station>,
}

impl RuntimeFixture {
    pub(super) fn try_step(&mut self, station: usize) -> Result<AdvanceOutcome, StationError> {
        self.stations[station].advance(&self.reads, &mut self.transactions)
    }

    pub(super) fn step(&mut self, station: usize) -> AdvanceOutcome {
        self.try_step(station).unwrap()
    }

    pub(super) fn cursor(&mut self, station: usize, input: usize) -> u64 {
        read_cursor(&self.stations[station], &mut self.transactions, input)
    }

    pub(super) fn bounds(&mut self, station: usize) -> std::ops::Range<u64> {
        output_bounds(&self.stations[station], &mut self.transactions)
    }
}

pub(super) struct MultiInputFixture {
    pub(super) _root: tempfile::TempDir,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) station: Station,
}

impl MultiInputFixture {
    pub(super) fn try_step(&mut self) -> Result<AdvanceOutcome, StationError> {
        self.station.advance(&self.reads, &mut self.transactions)
    }

    pub(super) fn step(&mut self) -> AdvanceOutcome {
        self.try_step().unwrap()
    }

    pub(super) fn active(&mut self) -> usize {
        read_active(&self.station, &mut self.transactions)
    }

    pub(super) fn cursor(&mut self, input: usize) -> u64 {
        read_cursor(&self.station, &mut self.transactions, input)
    }

    pub(super) fn bounds(&mut self, input: usize) -> std::ops::Range<u64> {
        output_bounds_log(
            self.station.inbox.ports()[input].output().log(),
            &mut self.transactions,
        )
    }
}

pub(super) enum ScriptResult {
    TurnIdle,
    Action(Action),
    Error,
}

#[derive(Clone)]
struct ScriptedAfterCommit {
    runs: Arc<AtomicUsize>,
    fails: bool,
    panics: bool,
}

pub(super) struct ScriptedOperation {
    write: Option<(State, Vec<u8>)>,
    poison_with: Option<(State, tempfile::TempDir)>,
    result: ScriptResult,
    after_commit: Option<ScriptedAfterCommit>,
}

impl ScriptedOperation {
    pub(super) fn returning(action: Action) -> Self {
        Self {
            write: None,
            poison_with: None,
            result: ScriptResult::Action(action),
            after_commit: None,
        }
    }

    pub(super) fn idle_before_transaction(state: State, value: &[u8]) -> Self {
        Self {
            write: Some((state, value.to_vec())),
            poison_with: None,
            result: ScriptResult::TurnIdle,
            after_commit: None,
        }
    }

    pub(super) fn writing(state: State, value: &[u8], result: ScriptResult) -> Self {
        Self {
            write: Some((state, value.to_vec())),
            poison_with: None,
            result,
            after_commit: None,
        }
    }

    pub(super) fn with_after_commit(mut self, runs: Arc<AtomicUsize>, fails: bool) -> Self {
        self.after_commit = Some(ScriptedAfterCommit {
            runs,
            fails,
            panics: false,
        });
        self
    }

    pub(super) fn with_panicking_after_commit(mut self, runs: Arc<AtomicUsize>) -> Self {
        self.after_commit = Some(ScriptedAfterCommit {
            runs,
            fails: false,
            panics: true,
        });
        self
    }
}

impl Operation for ScriptedOperation {
    fn turn<'turn>(
        &'turn mut self,
        _input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if matches!(self.result, ScriptResult::TurnIdle) {
            return Ok(Turn::Idle);
        }

        let write = self.write.clone();
        let poison_with = self
            .poison_with
            .as_ref()
            .map(|(foreign, _root)| foreign.clone());
        let result = match &self.result {
            ScriptResult::TurnIdle => unreachable!("handled before preparing a transaction"),
            ScriptResult::Action(action) => Ok(repeat_action(action)),
            ScriptResult::Error => Err(()),
        };
        let after_commit = self.after_commit.clone();
        Ok(Turn::ready(move |access| {
            if let Some((state, value)) = &write {
                state.access(access)?.put(&b"attempt".to_vec(), value)?;
            }
            if let Some(foreign) = &poison_with {
                assert!(matches!(
                    foreign.access(access),
                    Err(StoreError::WrongStore)
                ));
            }
            let action = result.map_err(|()| {
                OperationError::from(std::io::Error::other("planned turn failure"))
            })?;
            let after_commit = after_commit.map_or_else(AfterCommit::none, |script| {
                AfterCommit::new(move || {
                    script.runs.fetch_add(1, Ordering::Relaxed);
                    assert!(!script.panics, "planned after-commit panic");
                    if script.fails {
                        Err(PostCommitError::new(std::io::Error::other(
                            "planned after-commit failure",
                        )))
                    } else {
                        Ok(())
                    }
                })
            });
            Ok((action, after_commit))
        }))
    }
}

fn repeat_action(action: &Action) -> Action {
    match action {
        Action::Idle => Action::Idle,
        Action::Commit(output) => Action::Commit(output.clone()),
        Action::Complete(output) => Action::Complete(output.clone()),
    }
}

pub(super) fn scan_sink(consumer_count: usize, scan_capacity: NonZeroU64) -> RuntimeFixture {
    let root = tempfile::tempdir().unwrap();
    let mut builder = FlowFactory::new(root.path().join("flow"));
    let scan = builder.station("scan", SequenceScanDefinition::new(0));
    builder.output_capacity_bytes(scan, scan_capacity);
    for index in 0..consumer_count {
        let sink = builder.station(format!("sink-{index}"), DiscardDefinition::new());
        builder.connect([scan], sink);
    }
    fixture(root, builder.build().unwrap())
}

pub(super) fn scan_count_sink(
    scan_capacity: NonZeroU64,
    count_capacity: NonZeroU64,
) -> RuntimeFixture {
    let root = tempfile::tempdir().unwrap();
    let mut builder = FlowFactory::new(root.path().join("flow"));
    let scan = builder.station("scan", SequenceScanDefinition::new(0));
    let count = builder.station("count", RunningEventCountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(scan, scan_capacity);
    builder.output_capacity_bytes(count, count_capacity);
    builder.connect([scan], count);
    builder.connect([count], sink);
    fixture(root, builder.build().unwrap())
}

pub(super) fn multi_input_station(action: Action) -> MultiInputFixture {
    raw_station(&[0, 1], &[1], action)
}

pub(super) fn duplicate_input_station() -> MultiInputFixture {
    raw_station(&[0, 0], &[0], Action::Complete(None))
}

fn raw_station(inputs: &[usize], populated: &[usize], action: Action) -> MultiInputFixture {
    raw_station_with_change(inputs, populated, action, &change(&[7]))
}

fn raw_station_with_change(
    inputs: &[usize],
    populated: &[usize],
    action: Action,
    populated_change: &Change,
) -> MultiInputFixture {
    let output_count = inputs.iter().copied().max().unwrap() + 1;
    let schemas = std::iter::repeat_with(value_schema)
        .take(output_count)
        .collect::<Vec<_>>();
    raw_station_with_change_and_schemas(inputs, populated, action, populated_change, &schemas)
}

pub(super) fn raw_station_with_change_and_schemas(
    inputs: &[usize],
    populated: &[usize],
    action: Action,
    populated_change: &Change,
    output_schemas: &[Arc<Schema>],
) -> MultiInputFixture {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut store = Store::create(&path).unwrap();
    let state = store.create_data::<State>("state").unwrap();
    let output_count = inputs.iter().copied().max().unwrap() + 1;
    assert_eq!(output_schemas.len(), output_count);
    let outputs = (0..output_count)
        .map(|index| {
            store
                .create_data::<Log>(&format!("output-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let parts = station_parts(state.clone(), inputs.len(), action);
    let (mut transactions, reads) = store.into_transactions().split();
    let transaction = transactions.begin().unwrap();
    parts.initialize_input_state(transaction.access()).unwrap();
    let encoded = encode_change(populated_change).unwrap();
    for input in populated {
        outputs[*input]
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
    }
    transaction.commit().unwrap();
    let station = finish_station_with_schemas(parts, &state, &outputs, inputs, output_schemas);
    MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
    }
}

pub(super) fn reopen_multi_input(fixture: MultiInputFixture, action: Action) -> MultiInputFixture {
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

fn finish_station(parts: StationParts, state: &State, logs: &[Log], inputs: &[usize]) -> Station {
    let schemas = std::iter::repeat_with(value_schema)
        .take(logs.len())
        .collect::<Vec<_>>();
    finish_station_with_schemas(parts, state, logs, inputs, &schemas)
}

fn finish_station_with_schemas(
    parts: StationParts,
    state: &State,
    logs: &[Log],
    inputs: &[usize],
    output_schemas: &[Arc<Schema>],
) -> Station {
    assert_eq!(output_schemas.len(), logs.len());
    let outputs = logs
        .iter()
        .enumerate()
        .map(|(input, log)| {
            Arc::new(Output::new(
                log.clone(),
                NonZeroU64::MAX,
                Arc::clone(&output_schemas[input]),
                inputs
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| **candidate == input)
                    .map(|(input, _)| ConsumerCursor::new(ReadOnly::new(state.clone()), input))
                    .collect(),
            ))
        })
        .collect::<Vec<_>>();
    let mut slots = vec![0; outputs.len()];
    let input_ports = inputs
        .iter()
        .map(|input| {
            let port = outputs[*input].port(slots[*input]);
            slots[*input] += 1;
            port
        })
        .collect();
    parts.finish(input_ports, None)
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

pub(super) fn set_script(station: &mut Station, state: &State, value: &[u8], action: Action) {
    set_result(station, state, value, ScriptResult::Action(action));
}

pub(super) fn set_result(station: &mut Station, state: &State, value: &[u8], result: ScriptResult) {
    station.operation = Box::new(ScriptedOperation::writing(state.clone(), value, result));
}

pub(super) fn poisoned_script(state: &State, value: &[u8], action: Action) -> ScriptedOperation {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("foreign")).unwrap();
    let foreign = store.create_data::<State>("state").unwrap();
    let mut operation =
        ScriptedOperation::writing(state.clone(), value, ScriptResult::Action(action));
    operation.poison_with = Some((foreign, root));
    operation
}

pub(super) fn cursor_vectors(consumers: usize, entries: usize) -> Vec<Vec<u64>> {
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

pub(super) fn claim_id(station: &Station) -> Option<(usize, u64)> {
    station
        .inbox
        .cached_claim()
        .map(|claim| (claim.port(), claim.offset()))
}

pub(super) fn claim_ptr(station: &Station) -> *const Change {
    std::ptr::from_ref(station.inbox.cached_claim().unwrap().change())
}

pub(super) fn claim_bytes(station: &Station) -> Vec<u8> {
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

pub(super) fn read_attempt(state: &State, transactions: &mut Transactions) -> Option<Vec<u8>> {
    read_state(state, transactions, b"attempt")
}

pub(super) fn read_state(
    state: &State,
    transactions: &mut Transactions,
    key: &[u8],
) -> Option<Vec<u8>> {
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

pub(super) fn change(values: &[u64]) -> Change {
    uint64_change(value_schema(), values)
}

pub(super) fn count_change(values: &[u64]) -> Change {
    uint64_change(count_schema(), values)
}

fn uint64_change(schema: Arc<Schema>, values: &[u64]) -> Change {
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values.to_vec()))]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1; values.len()])).unwrap()
}

pub(super) fn value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

pub(super) fn count_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}
