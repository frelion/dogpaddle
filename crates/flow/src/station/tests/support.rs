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
    },
};
use dogpaddle_store::{Cell, ReadTransactions, Store, StoreError, SubscribedLog, Transactions};

use crate::flow::AdvanceOutcome;

use super::super::{Output, Station, StationParts, protocol::StationError};

pub(super) type State = Cell<Vec<u8>>;

pub(super) struct RuntimeFixture {
    pub(super) _root: tempfile::TempDir,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) stations: Vec<Station>,
    pub(super) states: Vec<State>,
}

impl RuntimeFixture {
    pub(super) fn try_step(&mut self, station: usize) -> Result<AdvanceOutcome, StationError> {
        self.stations[station].advance(&self.reads, &mut self.transactions)
    }

    pub(super) fn step(&mut self, station: usize) -> AdvanceOutcome {
        self.try_step(station).unwrap()
    }

    pub(super) fn position(&self, station: usize, input: usize) -> u64 {
        let transaction = self.reads.begin();
        self.stations[station]
            .status("test", transaction.access())
            .unwrap()
            .inputs[input]
            .position
    }

    pub(super) fn bounds(&self, station: usize) -> std::ops::Range<u64> {
        let transaction = self.reads.begin();
        let status = self.stations[station]
            .status("test", transaction.access())
            .unwrap()
            .output
            .unwrap();
        status.head..status.tail
    }
}

pub(super) struct MultiInputFixture {
    pub(super) _root: tempfile::TempDir,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
    pub(super) station: Station,
    pub(super) state: State,
    inputs: Vec<usize>,
    output_schemas: Vec<Arc<Schema>>,
}

impl MultiInputFixture {
    pub(super) fn try_step(&mut self) -> Result<AdvanceOutcome, StationError> {
        self.station.advance(&self.reads, &mut self.transactions)
    }

    pub(super) fn step(&mut self) -> AdvanceOutcome {
        self.try_step().unwrap()
    }

    pub(super) fn active(&self) -> usize {
        let transaction = self.reads.begin();
        self.station
            .status("test", transaction.access())
            .unwrap()
            .active_input
            .unwrap()
    }

    pub(super) fn position(&self, input: usize) -> u64 {
        let transaction = self.reads.begin();
        self.station
            .status("test", transaction.access())
            .unwrap()
            .inputs[input]
            .position
    }

    pub(super) fn bounds(&self, input: usize) -> std::ops::Range<u64> {
        let transaction = self.reads.begin();
        let status = self.station.inbox.ports()[input]
            .output()
            .status(transaction.access())
            .unwrap();
        status.head..status.tail
    }

    pub(super) fn append(&mut self, input: usize, change: &Change) {
        let transaction = self.transactions.begin();
        assert!(
            self.station.inbox.ports()[input]
                .output()
                .try_append(change, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();
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
                state.access(access)?.set(value)?;
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
    let mut inputs = vec![Vec::new()];
    let mut actions = vec![Action::Commit(Some(change(&[0])))];
    let mut outputs = vec![Some((scan_capacity, value_schema()))];
    for _ in 0..consumer_count {
        inputs.push(vec![0]);
        actions.push(Action::Complete(None));
        outputs.push(None);
    }
    runtime_fixture(&inputs, outputs, actions)
}

pub(super) fn scan_count_sink(
    scan_capacity: NonZeroU64,
    count_capacity: NonZeroU64,
) -> RuntimeFixture {
    runtime_fixture(
        &[Vec::new(), vec![0], vec![1]],
        vec![
            Some((scan_capacity, value_schema())),
            Some((count_capacity, count_schema())),
            None,
        ],
        vec![
            Action::Commit(Some(change(&[0]))),
            Action::Idle,
            Action::Complete(None),
        ],
    )
}

fn runtime_fixture(
    inputs_by_station: &[Vec<usize>],
    outputs: Vec<Option<(NonZeroU64, Arc<Schema>)>>,
    actions: Vec<Action>,
) -> RuntimeFixture {
    assert_eq!(inputs_by_station.len(), outputs.len());
    assert_eq!(inputs_by_station.len(), actions.len());
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();
    let states = (0..inputs_by_station.len())
        .map(|station| {
            store
                .create_data::<State>(&format!("script-state-{station}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut parts = inputs_by_station
        .iter()
        .zip(outputs)
        .zip(actions)
        .enumerate()
        .map(|(station, ((inputs, output), action))| {
            let active = (inputs.len() > 1).then(|| {
                store
                    .create_data::<Cell<u32>>(&format!("active-{station}"))
                    .unwrap()
            });
            let output = output.map(|(capacity, schema)| {
                let log = store
                    .create_data::<SubscribedLog<Vec<u8>>>(&format!("output-{station}"))
                    .unwrap();
                (log, capacity, schema)
            });
            StationParts::new(
                active,
                Box::new(ScriptedOperation::returning(action)),
                operation_kind(inputs.len(), output.is_some()),
                output,
            )
        })
        .collect::<Vec<_>>();
    let subscriber_counts = subscriber_counts(inputs_by_station);
    let (mut transactions, reads) = store.into_transactions().split();
    let transaction = transactions.begin();
    for (part, subscribers) in parts.iter().zip(&subscriber_counts) {
        part.initialize(*subscribers, transaction.access()).unwrap();
    }
    transaction.commit().unwrap();
    let stations = assemble(&mut parts, inputs_by_station);
    RuntimeFixture {
        _root: root,
        transactions,
        reads,
        stations,
        states,
    }
}

fn operation_kind(input_count: usize, has_output: bool) -> OperationKind {
    match (input_count, has_output) {
        (0, true) => OperationKind::Scan,
        (0, false) => panic!("an input-free test Station must have output"),
        (input_count, true) => {
            OperationKind::Transform(NonZeroU32::new(u32::try_from(input_count).unwrap()).unwrap())
        }
        (input_count, false) => {
            OperationKind::Sink(NonZeroU32::new(u32::try_from(input_count).unwrap()).unwrap())
        }
    }
}

fn subscriber_counts(inputs_by_station: &[Vec<usize>]) -> Vec<u64> {
    let mut counts = vec![0_u64; inputs_by_station.len()];
    for producer in inputs_by_station.iter().flatten() {
        counts[*producer] += 1;
    }
    counts
}

fn assemble(parts: &mut Vec<StationParts>, inputs_by_station: &[Vec<usize>]) -> Vec<Station> {
    let mut next_subscriber = vec![0_u64; parts.len()];
    let subscriptions = inputs_by_station
        .iter()
        .map(|inputs| {
            inputs
                .iter()
                .map(|producer| {
                    let subscriber = next_subscriber[*producer];
                    next_subscriber[*producer] += 1;
                    parts[*producer].subscription(subscriber)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let outputs = parts
        .iter_mut()
        .map(StationParts::prepare_output)
        .collect::<Vec<_>>();
    let inputs = inputs_by_station
        .iter()
        .zip(subscriptions)
        .map(|(producers, subscriptions)| {
            producers
                .iter()
                .zip(subscriptions)
                .map(|(producer, subscription)| {
                    outputs[*producer].as_ref().unwrap().port(subscription)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    std::mem::take(parts)
        .into_iter()
        .zip(inputs)
        .zip(outputs)
        .map(|((part, inputs), output)| part.finish(inputs, output))
        .collect()
}

pub(super) fn multi_input_station(action: Action) -> MultiInputFixture {
    raw_station(&[0, 1], &[1], action)
}

pub(super) fn duplicate_input_station() -> MultiInputFixture {
    raw_station(&[0, 0], &[0], Action::Complete(None))
}

fn raw_station(inputs: &[usize], populated: &[usize], action: Action) -> MultiInputFixture {
    raw_station_with_change_and_schemas(
        inputs,
        populated,
        action,
        &change(&[7]),
        &std::iter::repeat_with(value_schema)
            .take(inputs.iter().copied().max().unwrap() + 1)
            .collect::<Vec<_>>(),
    )
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
    let active = (inputs.len() > 1).then(|| store.create_data::<Cell<u32>>("active").unwrap());
    let output_count = inputs.iter().copied().max().unwrap() + 1;
    assert_eq!(output_schemas.len(), output_count);
    let logs = (0..output_count)
        .map(|index| {
            store
                .create_data::<SubscribedLog<Vec<u8>>>(&format!("output-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let parts = station_parts(active, inputs.len(), action);
    let subscribers = {
        let mut counts = vec![0_u64; output_count];
        for input in inputs {
            counts[*input] += 1;
        }
        counts
    };
    let (mut transactions, reads) = store.into_transactions().split();
    let transaction = transactions.begin();
    parts.initialize(0, transaction.access()).unwrap();
    for (log, subscribers) in logs.iter().zip(&subscribers) {
        log.initialize(NonZeroU64::new(*subscribers).unwrap(), transaction.access())
            .unwrap();
    }
    let encoded = encode_change(populated_change).unwrap();
    for output in populated {
        assert!(
            logs[*output]
                .writer()
                .try_append(&encoded, NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
    }
    transaction.commit().unwrap();
    let station = finish_station(parts, &logs, inputs, output_schemas);
    MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
        state,
        inputs: inputs.to_vec(),
        output_schemas: output_schemas.to_vec(),
    }
}

pub(super) fn reopen_multi_input(fixture: MultiInputFixture, action: Action) -> MultiInputFixture {
    let MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
        state: _,
        inputs,
        output_schemas,
    } = fixture;
    let path = root.path().join("flow");
    drop((transactions, reads, station));
    let store = Store::open(&path).unwrap();
    let state = store.open_data::<State>("state").unwrap();
    let active = (inputs.len() > 1).then(|| store.open_data::<Cell<u32>>("active").unwrap());
    let logs = (0..output_schemas.len())
        .map(|index| {
            store
                .open_data::<SubscribedLog<Vec<u8>>>(&format!("output-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let subscribers = {
        let mut counts = vec![0_u64; logs.len()];
        for input in &inputs {
            counts[*input] += 1;
        }
        counts
    };
    let parts = station_parts(active, inputs.len(), action);
    let transaction = store.read_transaction();
    parts.validate(0, transaction.access()).unwrap();
    for (log, subscribers) in logs.iter().zip(&subscribers) {
        log.validate(NonZeroU64::new(*subscribers).unwrap(), transaction.access())
            .unwrap();
    }
    drop(transaction);
    let station = finish_station(parts, &logs, &inputs, &output_schemas);
    let (transactions, reads) = store.into_transactions().split();
    MultiInputFixture {
        _root: root,
        transactions,
        reads,
        station,
        state,
        inputs,
        output_schemas,
    }
}

fn station_parts(active: Option<Cell<u32>>, input_count: usize, action: Action) -> StationParts {
    StationParts::new(
        active,
        Box::new(ScriptedOperation::returning(action)),
        OperationKind::Sink(NonZeroU32::new(u32::try_from(input_count).unwrap()).unwrap()),
        None,
    )
}

fn finish_station(
    parts: StationParts,
    logs: &[SubscribedLog<Vec<u8>>],
    inputs: &[usize],
    output_schemas: &[Arc<Schema>],
) -> Station {
    let outputs = logs
        .iter()
        .zip(output_schemas)
        .map(|(log, schema)| {
            Arc::new(Output::new(
                log.writer(),
                NonZeroU64::MAX,
                Arc::clone(schema),
            ))
        })
        .collect::<Vec<_>>();
    let mut next_subscriber = vec![0_u64; outputs.len()];
    let input_ports = inputs
        .iter()
        .map(|input| {
            let subscriber = next_subscriber[*input];
            next_subscriber[*input] += 1;
            outputs[*input].port(logs[*input].subscription(subscriber))
        })
        .collect();
    parts.finish(input_ports, None)
}

pub(super) fn set_script(station: &mut Station, state: &State, value: &[u8], action: Action) {
    set_result(station, state, value, ScriptResult::Action(action));
}

pub(super) fn set_result(station: &mut Station, state: &State, value: &[u8], result: ScriptResult) {
    station.replace_operation(Box::new(ScriptedOperation::writing(
        state.clone(),
        value,
        result,
    )));
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

pub(super) fn read_attempt(state: &State, transactions: &mut Transactions) -> Option<Vec<u8>> {
    let transaction = transactions.begin();
    state.access(transaction.access()).unwrap().get().unwrap()
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
