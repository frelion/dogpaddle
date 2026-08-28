use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, decode_change, encode_change};
use dogpaddle_operation::{
    OperationDefinition, encode_definition,
    operation::{
        InputProgress, Operation, OperationError, OperationInput, TurnCommit, TurnDecision,
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{
    AppendLog, Cell, OrderedMap, ReadOnly, ReadTransactions, ScanLimit, Small, Store, StoreError,
    TransactionAccess, Transactions,
};

use crate::{build::FlowFactory, flow::Flow};

use super::{
    ACTIVE_INPUT_KEY, CURSOR_ORIGIN, ConsumerCursor, Station, StationParts, cursor_key,
    decode_active_input, decode_cursor, encode_active_input, encode_cursor,
    gc::GC_MAX_ITEMS,
    protocol::{ProcessOutcome, StationError},
};

struct StationFixture {
    transactions: Transactions,
    source: Station,
    count: Station,
    source_definition: SequenceSourceDefinition,
    count_definition: CountDefinition,
    _root: tempfile::TempDir,
}

struct IntakeFixture {
    transactions: Transactions,
    reads: ReadTransactions,
    stations: Vec<Station>,
    changes: Vec<Change>,
    root: tempfile::TempDir,
}

struct ReplayFixture {
    transactions: Transactions,
    reads: ReadTransactions,
    station: Station,
    continuation: Cell<u64>,
    observed: Arc<Mutex<Vec<ObservedInput>>>,
    _root: tempfile::TempDir,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedInput {
    port: usize,
    values: Vec<u64>,
    diffs: Vec<i64>,
    column_address: usize,
}

struct KeepThenCompleteOperation {
    definition: CountDefinition,
    continuation: Cell<u64>,
    observed: Arc<Mutex<Vec<ObservedInput>>>,
}

struct MissingInputProgressOperation {
    definition: CountDefinition,
}

struct AlwaysIdleOperation {
    definition: CountDefinition,
    attempted_continuation: Option<Cell<u64>>,
    expected_values: Vec<u64>,
}

struct FailOnceThenCompleteOperation {
    definition: CountDefinition,
    continuation: Cell<u64>,
    failed: AtomicBool,
}

impl Operation for KeepThenCompleteOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError> {
        let input = input.expect("replay test operation requires an input");
        let values = input
            .change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("replay test input has a UInt64 column")
            .values()
            .to_vec();
        let column_address = Arc::as_ptr(input.change.records().column(0)).cast::<()>() as usize;
        self.observed.lock().unwrap().push(ObservedInput {
            port: input.port,
            values,
            diffs: input.change.diffs().values().to_vec(),
            column_address,
        });

        let mut continuation = self.continuation.access(access)?;
        let (progress, output) = match continuation.get()? {
            None => {
                continuation.set(&1)?;
                (InputProgress::Keep, change(&[10]))
            }
            Some(1) => {
                continuation.clear()?;
                (InputProgress::Complete, change(&[20]))
            }
            Some(value) => panic!("unexpected replay test continuation {value}"),
        };
        Ok(TurnDecision::Commit(TurnCommit {
            input: Some(progress),
            output: Some(output),
        }))
    }
}

impl Operation for MissingInputProgressOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        _input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError> {
        Ok(TurnDecision::Commit(TurnCommit {
            input: None,
            output: None,
        }))
    }
}

impl Operation for AlwaysIdleOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError> {
        assert_replay_input(input, &self.expected_values);
        if let Some(continuation) = &self.attempted_continuation {
            continuation.access(access)?.set(&1)?;
        }
        Ok(TurnDecision::Idle)
    }
}

impl Operation for FailOnceThenCompleteOperation {
    fn definition(&self) -> &dyn OperationDefinition {
        &self.definition
    }

    fn turn(
        &self,
        input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<TurnDecision, OperationError> {
        assert_replay_input(input, &[7, 8, 9]);
        let mut continuation = self.continuation.access(access)?;
        assert_eq!(
            continuation.get()?,
            None,
            "the failed turn's continuation write must have rolled back"
        );
        if !self.failed.swap(true, Ordering::SeqCst) {
            continuation.set(&1)?;
            return Err(std::io::Error::other("planned replay test failure").into());
        }
        continuation.set(&2)?;
        Ok(TurnDecision::Commit(TurnCommit {
            input: Some(InputProgress::Complete),
            output: None,
        }))
    }
}

fn assert_replay_input(input: Option<OperationInput<'_>>, expected_values: &[u64]) {
    let input = input.expect("replay test operation requires an input");
    assert_eq!(input.port, 0);
    let values = input
        .change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("replay test input has a UInt64 column")
        .values();
    assert_eq!(values.as_ref(), expected_values);
    assert_eq!(
        input.change.diffs().values().as_ref(),
        vec![1; expected_values.len()]
    );
}

impl IntakeFixture {
    fn write_active_input(&mut self, station: usize, input: usize) {
        let transaction = self.transactions.begin().unwrap();
        self.stations[station]
            .state
            .access(transaction.access())
            .unwrap()
            .put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(input).to_vec(),
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    fn write_cursor(&mut self, station: usize, input: usize, offset: u64) {
        let transaction = self.transactions.begin().unwrap();
        self.stations[station]
            .state
            .access(transaction.access())
            .unwrap()
            .put(&cursor_key(input), &encode_cursor(offset).to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
}

#[test]
fn construction_boxes_heterogeneous_operations_and_keeps_station_state_isolated() {
    let mut fixture = station_fixture();

    assert_eq!(
        encode_definition(fixture.source.operation.definition()),
        encode_definition(&fixture.source_definition)
    );
    assert_eq!(
        encode_definition(fixture.count.operation.definition()),
        encode_definition(&fixture.count_definition)
    );

    let transaction = fixture.transactions.begin().unwrap();
    put_state(&fixture.source, transaction.access(), b"source");
    put_state(&fixture.count, transaction.access(), b"count");
    assert_eq!(
        read_state(&fixture.source, transaction.access()),
        b"source".to_vec()
    );
    assert_eq!(
        read_state(&fixture.count, transaction.access()),
        b"count".to_vec()
    );
    transaction.commit().unwrap();
}

#[test]
fn flow_owned_transaction_reaches_station_output_and_read_only_input() {
    let mut fixture = station_fixture();

    assert!(fixture.source.inputs.logs.is_empty());
    assert_eq!(fixture.count.inputs.logs.len(), 1);
    assert!(fixture.source.output.is_some());
    assert!(fixture.count.output.is_some());

    let transaction = fixture.transactions.begin().unwrap();
    fixture
        .source
        .output
        .as_ref()
        .unwrap()
        .access(transaction.access())
        .unwrap()
        .append(&b"change".to_vec())
        .unwrap();
    assert_eq!(
        fixture.count.inputs.logs[0]
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..1
    );
    transaction.commit().unwrap();
}

#[test]
fn build_and_open_inject_the_later_declared_source_output_as_read_only_input() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let count = factory.station("count", CountDefinition::new());
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    factory.connect([source], count);

    assert_station_wiring(factory.build().unwrap());
    assert_station_wiring(FlowFactory::open(&path).unwrap());
}

#[test]
fn populated_intake_cache_is_a_store_free_idempotent_noop() {
    let mut fixture = flow_with_changes(&[&[10, 11, 12]]);

    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    let first_column = {
        let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
        assert_eq!(cached.input, 0);
        assert_eq!(cached.offset, 0);
        assert_eq!(cached.change.records(), fixture.changes[0].records());
        assert_eq!(cached.change.diffs(), fixture.changes[0].diffs());
        cached.change.records().column(0).clone()
    };

    fixture.write_cursor(1, 0, u64::MAX);
    fixture.write_active_input(1, 1);
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.change.num_rows(), 3);
    assert!(Arc::ptr_eq(
        &first_column,
        cached.change.records().column(0)
    ));
}

#[test]
fn intake_loads_the_next_entry_after_the_cursor_advances_and_cache_is_released() {
    let mut fixture = flow_with_changes(&[&[10, 11], &[20]]);

    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    let first_column = fixture.stations[1]
        .inputs
        .cache
        .as_ref()
        .unwrap()
        .change
        .records()
        .column(0)
        .clone();

    fixture.write_cursor(1, 0, 1);
    fixture.stations[1].inputs.cache = None;
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 0);
    assert_eq!(cached.offset, 1);
    assert_eq!(cached.change.records(), fixture.changes[1].records());
    assert!(!Arc::ptr_eq(
        &first_column,
        cached.change.records().column(0)
    ));

    fixture.write_cursor(1, 0, 2);
    fixture.stations[1].inputs.cache = None;
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    assert!(fixture.stations[1].inputs.cache.is_none());
    let transaction = fixture.transactions.begin().unwrap();
    assert_eq!(
        read_active_input(&fixture.stations[1], transaction.access()),
        0
    );
    transaction.commit().unwrap();
}

#[test]
fn intake_rejects_a_missing_durable_cursor() {
    let mut fixture = flow_with_changes(&[&[10]]);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .state
            .access(transaction.access())
            .unwrap()
            .remove(&cursor_key(0))
            .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(error, StationError::MissingCursor { input: 0 }));
    assert!(fixture.stations[1].inputs.cache.is_none());
}

#[test]
fn intake_rejects_a_missing_durable_active_input() {
    let mut fixture = flow_with_changes(&[&[10]]);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .state
            .access(transaction.access())
            .unwrap()
            .remove(&ACTIVE_INPUT_KEY.to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(error, StationError::MissingActiveInput));
    assert!(fixture.stations[1].inputs.cache.is_none());
}

#[test]
fn intake_rejects_a_malformed_durable_active_input() {
    let mut fixture = flow_with_changes(&[&[10]]);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .state
            .access(transaction.access())
            .unwrap()
            .put(&ACTIVE_INPUT_KEY.to_vec(), &vec![0; 3])
            .unwrap();
        transaction.commit().unwrap();
    }

    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(error, StationError::MalformedActiveInput));
    assert!(fixture.stations[1].inputs.cache.is_none());
}

#[test]
fn intake_rejects_an_active_input_outside_the_runtime_inputs() {
    let mut fixture = flow_with_changes(&[&[10]]);
    fixture.write_active_input(1, 1);

    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(
        error,
        StationError::ActiveInputOutOfRange {
            input: 1,
            input_count: 1
        }
    ));
    assert!(fixture.stations[1].inputs.cache.is_none());
}

#[test]
fn intake_rejects_a_malformed_durable_cursor() {
    let mut fixture = flow_with_changes(&[&[10]]);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .state
            .access(transaction.access())
            .unwrap()
            .put(&cursor_key(0), &vec![0; 7])
            .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(error, StationError::MalformedCursor { input: 0 }));
    assert!(fixture.stations[1].inputs.cache.is_none());
}

#[test]
fn intake_surfaces_invalid_change_bytes_without_populating_the_cache() {
    let mut fixture = flow_with_changes(&[]);
    let empty_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(empty_log));
    fixture.write_cursor(1, 1, CURSOR_ORIGIN);
    fixture.write_active_input(1, 1);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[0]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append(&b"not an Arrow Change stream".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap_err();

    assert!(matches!(
        error,
        StationError::InvalidInputChange { input: 0, .. }
    ));
    assert!(fixture.stations[1].inputs.cache.is_none());
    let transaction = fixture.transactions.begin().unwrap();
    assert_eq!(
        read_active_input(&fixture.stations[1], transaction.access()),
        0
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], transaction.access(), 0),
        CURSOR_ORIGIN
    );
}

#[test]
fn intake_stops_after_populating_the_single_cache() {
    let mut fixture = flow_with_changes(&[&[10, 11]]);
    let second_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(second_log));
    fixture.write_cursor(1, 1, CURSOR_ORIGIN);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append(&b"not an Arrow Change stream".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 0);
    assert_eq!(cached.offset, 0);
    assert_eq!(cached.change.records(), fixture.changes[0].records());
    assert_eq!(cached.change.diffs(), fixture.changes[0].diffs());
}

#[test]
fn intake_searches_cyclically_from_the_durable_active_input() {
    let mut fixture = flow_with_changes(&[&[10]]);
    let second_changes = [change(&[20]), change(&[30])];
    let second_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(second_log));
    fixture.write_cursor(1, 1, 1);
    fixture.write_active_input(1, 1);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append_batch(
                &second_changes
                    .iter()
                    .map(|change| encode_change(change).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 1);
    assert_eq!(cached.offset, 1);
    assert_eq!(cached.change.records(), second_changes[1].records());
    assert_eq!(cached.change.diffs(), second_changes[1].diffs());
}

#[test]
fn intake_pins_the_wrapped_input_before_populating_the_cache() {
    let mut fixture = flow_with_changes(&[&[10]]);
    let empty_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(empty_log));
    fixture.write_cursor(1, 1, CURSOR_ORIGIN);
    fixture.write_active_input(1, 1);

    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let cached = fixture.stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 0);
    assert_eq!(cached.offset, 0);
    assert_eq!(cached.change.records(), fixture.changes[0].records());
    assert_eq!(cached.change.diffs(), fixture.changes[0].diffs());
    let transaction = fixture.transactions.begin().unwrap();
    assert_eq!(
        read_active_input(&fixture.stations[1], transaction.access()),
        0
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], transaction.access(), 0),
        CURSOR_ORIGIN
    );
    transaction.commit().unwrap();
}

#[test]
fn gc_truncates_only_the_prefix_retired_by_every_consumer() {
    let root = tempfile::tempdir().unwrap();
    let mut factory = FlowFactory::new(root.path().join("flow"));
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let first = factory.station("first", CountDefinition::new());
    let second = factory.station("second", CountDefinition::new());
    factory.connect([source], first);
    factory.connect([source], second);
    let flow = factory.build().unwrap();
    let (mut transactions, _reads, stations) = flow.into_runtime_parts();
    {
        let transaction = transactions.begin().unwrap();
        stations[0]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append_batch(&[vec![0], vec![1], vec![2]])
            .unwrap();
        write_cursor(&stations[1], transaction.access(), 0, 3);
        write_cursor(&stations[2], transaction.access(), 0, 1);
        transaction.commit().unwrap();
    }

    stations[0].gc(&mut transactions).unwrap();

    assert_eq!(output_bounds(&stations[0], &mut transactions), 1..3);
}

#[test]
fn gc_deletes_at_most_one_bounded_batch() {
    let root = tempfile::tempdir().unwrap();
    let mut factory = FlowFactory::new(root.path().join("flow"));
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let count = factory.station("count", CountDefinition::new());
    factory.connect([source], count);
    let flow = factory.build().unwrap();
    let (mut transactions, _reads, stations) = flow.into_runtime_parts();
    let entry_count = GC_MAX_ITEMS.get() + 1;
    {
        let transaction = transactions.begin().unwrap();
        stations[0]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append_batch(&vec![Vec::new(); entry_count])
            .unwrap();
        write_cursor(
            &stations[1],
            transaction.access(),
            0,
            u64::try_from(entry_count).unwrap(),
        );
        transaction.commit().unwrap();
    }

    stations[0].gc(&mut transactions).unwrap();

    assert_eq!(
        output_bounds(&stations[0], &mut transactions),
        u64::try_from(GC_MAX_ITEMS.get()).unwrap()..u64::try_from(entry_count).unwrap()
    );
}

#[test]
fn reopen_rebuilds_cache_from_the_durable_active_input_and_offset() {
    let mut fixture = flow_with_changes(&[&[10]]);
    let second_changes = [change(&[20]), change(&[30])];
    fixture.write_cursor(1, 1, 1);
    fixture.write_active_input(1, 1);
    {
        let transaction = fixture.transactions.begin().unwrap();
        fixture.stations[1]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append_batch(
                &second_changes
                    .iter()
                    .map(|change| encode_change(change).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    let IntakeFixture {
        transactions,
        reads,
        stations,
        changes: _,
        root,
    } = fixture;
    drop(stations);
    drop(reads);
    drop(transactions);

    let flow = FlowFactory::open(root.path().join("flow")).unwrap();
    let (mut transactions, reads, mut stations) = flow.into_runtime_parts();
    let second_log = stations[1].output.as_ref().unwrap().clone();
    stations[1].inputs.logs.push(ReadOnly::new(second_log));
    assert!(stations[1].inputs.cache.is_none());

    stations[1].intake(&reads, &mut transactions).unwrap();
    let cached = stations[1].inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 1);
    assert_eq!(cached.offset, 1);
    assert_eq!(cached.change.records(), second_changes[1].records());
    assert_eq!(cached.change.diffs(), second_changes[1].diffs());
}

#[test]
fn keep_commits_a_prefix_and_reoffers_the_same_complete_change_until_complete() {
    let mut fixture = replay_fixture();

    assert!(
        !fixture
            .station
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let cached_column = fixture
        .station
        .inputs
        .cache
        .as_ref()
        .unwrap()
        .change
        .records()
        .column(0)
        .clone();

    assert_eq!(
        fixture.station.process(&mut fixture.transactions).unwrap(),
        ProcessOutcome::Progressed
    );
    let cached = fixture.station.inputs.cache.as_ref().unwrap();
    assert_eq!(cached.input, 0);
    assert_eq!(cached.offset, 0);
    assert_eq!(cached.change.num_rows(), 3);
    assert!(Arc::ptr_eq(
        &cached_column,
        cached.change.records().column(0)
    ));
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            fixture
                .continuation
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(1)
        );
        assert_eq!(read_cursor(&fixture.station, transaction.access(), 0), 0);
        assert_eq!(read_active_input(&fixture.station, transaction.access()), 0);
        transaction.commit().unwrap();
    }
    assert_eq!(
        count_output_values(&fixture.station, &mut fixture.transactions),
        [10]
    );

    fixture.station.inputs.cache = None;
    assert!(
        !fixture
            .station
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let rebuilt = fixture.station.inputs.cache.as_ref().unwrap();
    assert_eq!(rebuilt.input, 0);
    assert_eq!(rebuilt.offset, 0);
    assert_eq!(rebuilt.change.num_rows(), 3);
    assert!(!Arc::ptr_eq(
        &cached_column,
        rebuilt.change.records().column(0)
    ));

    assert_eq!(
        fixture.station.process(&mut fixture.transactions).unwrap(),
        ProcessOutcome::Progressed
    );
    assert!(fixture.station.inputs.cache.is_none());
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            fixture
                .continuation
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        assert_eq!(read_cursor(&fixture.station, transaction.access(), 0), 1);
        assert_eq!(read_active_input(&fixture.station, transaction.access()), 0);
        transaction.commit().unwrap();
    }
    assert_eq!(
        count_output_values(&fixture.station, &mut fixture.transactions),
        [10, 20]
    );

    let observed = fixture.observed.lock().unwrap();
    assert_eq!(observed.len(), 2);
    for input in observed.iter() {
        assert_eq!(input.port, 0);
        assert_eq!(input.values, [7, 8, 9]);
        assert_eq!(input.diffs, [1, 1, 1]);
    }
    assert_ne!(observed[0].column_address, observed[1].column_address);
}

#[test]
fn idle_rolls_back_operation_writes_and_keeps_the_same_input_claimed() {
    let mut fixture = replay_fixture();
    fixture.station.operation = Box::new(AlwaysIdleOperation {
        definition: CountDefinition::new(),
        attempted_continuation: Some(fixture.continuation.clone()),
        expected_values: vec![7, 8, 9],
    });
    fixture
        .station
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    for _ in 0..2 {
        assert_eq!(
            fixture.station.process(&mut fixture.transactions).unwrap(),
            ProcessOutcome::Idle
        );
        assert!(fixture.station.inputs.cache.is_some());
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            fixture
                .continuation
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        assert_eq!(read_cursor(&fixture.station, transaction.access(), 0), 0);
        assert_eq!(read_active_input(&fixture.station, transaction.access()), 0);
        transaction.commit().unwrap();
    }
}

#[test]
fn operation_error_rolls_back_writes_and_retries_the_same_input() {
    let mut fixture = replay_fixture();
    fixture.station.operation = Box::new(FailOnceThenCompleteOperation {
        definition: CountDefinition::new(),
        continuation: fixture.continuation.clone(),
        failed: AtomicBool::new(false),
    });
    fixture
        .station
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let error = fixture
        .station
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(error, StationError::Operation(_)));
    assert!(fixture.station.inputs.cache.is_some());
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            fixture
                .continuation
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        assert_eq!(read_cursor(&fixture.station, transaction.access(), 0), 0);
        assert_eq!(read_active_input(&fixture.station, transaction.access()), 0);
        transaction.commit().unwrap();
    }

    assert_eq!(
        fixture.station.process(&mut fixture.transactions).unwrap(),
        ProcessOutcome::Progressed
    );
    assert!(fixture.station.inputs.cache.is_none());
    let transaction = fixture.transactions.begin().unwrap();
    assert_eq!(
        fixture
            .continuation
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(2)
    );
    assert_eq!(read_cursor(&fixture.station, transaction.access(), 0), 1);
    transaction.commit().unwrap();
}

#[test]
fn a_durable_intake_pin_counts_as_progress_when_the_operation_is_idle() {
    let mut fixture = flow_with_changes(&[&[10]]);
    let empty_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(empty_log));
    fixture.write_cursor(1, 1, CURSOR_ORIGIN);
    fixture.write_active_input(1, 1);
    fixture.stations[1].operation = Box::new(AlwaysIdleOperation {
        definition: CountDefinition::new(),
        attempted_continuation: None,
        expected_values: vec![10],
    });

    assert_eq!(
        fixture.stations[1]
            .advance(&fixture.reads, &mut fixture.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );
    assert!(fixture.stations[1].inputs.cache.is_some());
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            read_active_input(&fixture.stations[1], transaction.access()),
            0
        );
        assert_eq!(
            read_cursor(&fixture.stations[1], transaction.access(), 0),
            0
        );
        transaction.commit().unwrap();
    }

    assert_eq!(
        fixture.stations[1]
            .advance(&fixture.reads, &mut fixture.transactions)
            .unwrap(),
        ProcessOutcome::Idle
    );
}

#[test]
fn process_consumes_the_complete_change_and_reopen_does_not_replay_it() {
    let values = [10, 20, 30];
    let mut fixture = flow_with_changes(&[&values]);

    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    assert_eq!(
        fixture.stations[1]
            .process(&mut fixture.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );
    assert!(fixture.stations[1].inputs.cache.is_none());
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            read_cursor(&fixture.stations[1], transaction.access(), 0),
            1
        );
        assert_eq!(
            read_active_input(&fixture.stations[1], transaction.access()),
            0
        );
    }
    assert_eq!(
        count_output_values(&fixture.stations[1], &mut fixture.transactions),
        [1, 2, 3]
    );

    let IntakeFixture {
        transactions,
        reads,
        stations,
        changes: _,
        root,
    } = fixture;
    drop(stations);
    drop(reads);
    drop(transactions);

    let flow = FlowFactory::open(root.path().join("flow")).unwrap();
    let (mut transactions, reads, mut stations) = flow.into_runtime_parts();
    stations[1].intake(&reads, &mut transactions).unwrap();
    assert!(stations[1].inputs.cache.is_none());
    assert_eq!(
        stations[1].process(&mut transactions).unwrap(),
        ProcessOutcome::Idle
    );
    assert_eq!(
        count_output_values(&stations[1], &mut transactions),
        [1, 2, 3]
    );
}

#[test]
fn process_rejects_a_cache_whose_port_is_not_the_durable_active_input() {
    let mut fixture = flow_with_changes(&[&[10]]);
    let second_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .logs
        .push(ReadOnly::new(second_log));
    fixture.write_cursor(1, 1, CURSOR_ORIGIN);
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    fixture.write_active_input(1, 1);

    let error = fixture.stations[1]
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(
        error,
        StationError::CachedActiveInputMismatch {
            cached: 0,
            durable: 1,
        }
    ));
    assert!(fixture.stations[1].inputs.cache.is_some());
    assert_eq!(
        count_output_values(&fixture.stations[1], &mut fixture.transactions),
        []
    );
}

#[test]
fn process_rejects_input_progress_that_does_not_match_the_invocation() {
    let mut fixture = flow_with_changes(&[&[10]]);
    fixture.stations[1].operation = Box::new(MissingInputProgressOperation {
        definition: CountDefinition::new(),
    });
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let error = fixture.stations[1]
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(
        error,
        StationError::OperationInputProgressMismatch {
            offered_input: true,
            returned_input: false,
        }
    ));
    assert!(fixture.stations[1].inputs.cache.is_some());
    let transaction = fixture.transactions.begin().unwrap();
    assert_eq!(
        read_cursor(&fixture.stations[1], transaction.access(), 0),
        0
    );
    transaction.commit().unwrap();
}

#[test]
fn process_rejects_a_stale_cache_before_calling_the_operation() {
    let mut fixture = flow_with_changes(&[&[10]]);
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();
    fixture.write_cursor(1, 0, 1);

    let error = fixture.stations[1]
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(
        error,
        StationError::CachedCursorMismatch {
            input: 0,
            cached: 0,
            durable: 1,
        }
    ));
    assert!(fixture.stations[1].inputs.cache.is_some());
    assert!(count_output_values(&fixture.stations[1], &mut fixture.transactions).is_empty());

    fixture.write_cursor(1, 0, 0);
    assert_eq!(
        fixture.stations[1]
            .process(&mut fixture.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );
    assert_eq!(
        count_output_values(&fixture.stations[1], &mut fixture.transactions),
        [1]
    );
}

#[test]
fn output_append_failure_rolls_back_operation_progress_and_keeps_the_cache() {
    let mut fixture = flow_with_changes(&[&[10]]);
    fixture.stations[1]
        .intake(&fixture.reads, &mut fixture.transactions)
        .unwrap();

    let valid_output = fixture.stations[1].output.take().unwrap();
    let foreign_root = tempfile::tempdir().unwrap();
    let mut foreign_store = Store::create(foreign_root.path().join("store")).unwrap();
    let foreign_output = foreign_store
        .create_data::<AppendLog<Vec<u8>>>("output")
        .unwrap();
    let _foreign_transactions = foreign_store.into_transactions();
    fixture.stations[1].output = Some(foreign_output);

    let error = fixture.stations[1]
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(error, StationError::Store(StoreError::WrongStore)));
    assert!(fixture.stations[1].inputs.cache.is_some());
    {
        let transaction = fixture.transactions.begin().unwrap();
        assert_eq!(
            read_cursor(&fixture.stations[1], transaction.access(), 0),
            0
        );
    }

    fixture.stations[1].output = Some(valid_output);
    assert_eq!(
        fixture.stations[1]
            .process(&mut fixture.transactions)
            .unwrap(),
        ProcessOutcome::Progressed
    );
    assert_eq!(
        count_output_values(&fixture.stations[1], &mut fixture.transactions),
        [1]
    );
}

#[test]
fn input_state_keys_and_values_have_stable_encodings() {
    assert_eq!(ACTIVE_INPUT_KEY, b"input/active");
    assert_eq!(encode_active_input(0x0102_0304), [0x01, 0x02, 0x03, 0x04]);
    assert_eq!(decode_active_input(&encode_active_input(15)), Some(15));
    assert_eq!(decode_active_input(&[0; 3]), None);
    assert_eq!(cursor_key(0), b"input/00000000/cursor");
    assert_eq!(cursor_key(15), b"input/0000000f/cursor");
    assert_eq!(
        encode_cursor(0x0102_0304_0506_0708),
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
    assert_eq!(decode_cursor(&encode_cursor(CURSOR_ORIGIN)), Some(0));
    assert_eq!(decode_cursor(&[0; 7]), None);
}

fn assert_station_wiring(flow: Flow) {
    let (mut transactions, _reads, stations) = flow.into_runtime_parts();
    assert_eq!(stations.len(), 2);
    assert_eq!(stations[0].inputs.logs.len(), 1);
    assert!(stations[0].consumers.is_empty());
    assert!(stations[0].output.is_some());
    assert!(stations[1].inputs.logs.is_empty());
    assert_eq!(stations[1].consumers.len(), 1);
    assert!(stations[1].output.is_some());

    let transaction = transactions.begin().unwrap();
    let mut output = stations[1]
        .output
        .as_ref()
        .expect("source produces output")
        .access(transaction.access())
        .unwrap();
    if output.bounds().unwrap().is_empty() {
        output.append(&b"change".to_vec()).unwrap();
    }
    assert_eq!(
        stations[0].inputs.logs[0]
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..1
    );
    transaction.commit().unwrap();
}

fn station_fixture() -> StationFixture {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();

    let source_definition = SequenceSourceDefinition::new(100);
    let source_state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("source-state")
        .unwrap();
    let source_operation = Box::new(SequenceSourceOperation::new(
        source_definition,
        store.create_data::<Cell<u64>>("source-position").unwrap(),
    ));
    let source_output = store
        .create_data::<AppendLog<Vec<u8>>>("source-output")
        .unwrap();
    let count_input = ReadOnly::new(source_output.clone());

    let count_definition = CountDefinition::new();
    let count_state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("count-state")
        .unwrap();
    let count_operation = Box::new(CountOperation::new(
        count_definition,
        store.create_data::<Cell<u64>>("count-value").unwrap(),
    ));
    let count_output = store
        .create_data::<AppendLog<Vec<u8>>>("count-output")
        .unwrap();

    let transactions = store.into_transactions();
    let source = StationParts::new(source_state, source_operation, Some(source_output)).finish(
        Vec::new(),
        vec![ConsumerCursor::new(ReadOnly::new(count_state.clone()), 0)],
    );
    let count = StationParts::new(count_state, count_operation, Some(count_output))
        .finish(vec![count_input], Vec::new());
    StationFixture {
        transactions,
        source,
        count,
        source_definition,
        count_definition,
        _root: root,
    }
}

fn flow_with_changes(values: &[&[u64]]) -> IntakeFixture {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let count = factory.station("count", CountDefinition::new());
    factory.connect([source], count);
    let flow = factory.build().unwrap();
    let (mut transactions, reads, stations) = flow.into_runtime_parts();

    let changes = values
        .iter()
        .map(|values| change(values))
        .collect::<Vec<_>>();
    let encoded = changes
        .iter()
        .map(|change| encode_change(change).unwrap())
        .collect::<Vec<_>>();
    {
        let transaction = transactions.begin().unwrap();
        stations[0]
            .output
            .as_ref()
            .unwrap()
            .access(transaction.access())
            .unwrap()
            .append_batch(&encoded)
            .unwrap();
        transaction.commit().unwrap();
    }

    IntakeFixture {
        transactions,
        reads,
        stations,
        changes,
        root,
    }
}

fn replay_fixture() -> ReplayFixture {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();
    let state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("state")
        .unwrap();
    let continuation = store.create_data::<Cell<u64>>("continuation").unwrap();
    let input_log = store.create_data::<AppendLog<Vec<u8>>>("input").unwrap();
    let output = store.create_data::<AppendLog<Vec<u8>>>("output").unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let operation = Box::new(KeepThenCompleteOperation {
        definition: CountDefinition::new(),
        continuation: continuation.clone(),
        observed: Arc::clone(&observed),
    });
    let parts = StationParts::new(state, operation, Some(output));
    let input = ReadOnly::new(input_log.clone());
    let (mut transactions, reads) = store.into_transactions().split();
    {
        let transaction = transactions.begin().unwrap();
        parts.initialize_input_state(transaction.access()).unwrap();
        input_log
            .access(transaction.access())
            .unwrap()
            .append(&encode_change(&change(&[7, 8, 9])).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    let station = parts.finish(vec![input], Vec::new());

    ReplayFixture {
        transactions,
        reads,
        station,
        continuation,
        observed,
        _root: root,
    }
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

fn put_state(station: &Station, access: dogpaddle_store::TransactionAccess<'_>, value: &[u8]) {
    station
        .state
        .access(access)
        .unwrap()
        .put(&b"key".to_vec(), &value.to_vec())
        .unwrap();
}

fn read_state(station: &Station, access: dogpaddle_store::TransactionAccess<'_>) -> Vec<u8> {
    station
        .state
        .access(access)
        .unwrap()
        .get(&b"key".to_vec())
        .unwrap()
        .unwrap()
}

fn write_cursor(
    station: &Station,
    access: dogpaddle_store::TransactionAccess<'_>,
    input: usize,
    offset: u64,
) {
    station
        .state
        .access(access)
        .unwrap()
        .put(&cursor_key(input), &encode_cursor(offset).to_vec())
        .unwrap();
}

fn read_active_input(station: &Station, access: dogpaddle_store::TransactionAccess<'_>) -> usize {
    let encoded = station
        .state
        .access(access)
        .unwrap()
        .get(&ACTIVE_INPUT_KEY.to_vec())
        .unwrap()
        .unwrap();
    decode_active_input(&encoded).unwrap()
}

fn read_cursor(
    station: &Station,
    access: dogpaddle_store::TransactionAccess<'_>,
    input: usize,
) -> u64 {
    let encoded = station
        .state
        .access(access)
        .unwrap()
        .get(&cursor_key(input))
        .unwrap()
        .unwrap();
    decode_cursor(&encoded).unwrap()
}

fn count_output_values(station: &Station, transactions: &mut Transactions) -> Vec<u64> {
    let transaction = transactions.begin().unwrap();
    let output = station
        .output
        .as_ref()
        .unwrap()
        .access(transaction.access())
        .unwrap();
    let bounds = output.bounds().unwrap();
    let mut encoded = Vec::new();
    output
        .scan(
            bounds.start,
            ScanLimit::new(usize::MAX, usize::MAX).unwrap(),
            |entry| -> Result<(), StoreError> {
                encoded.push(entry.decode_owned()?);
                Ok(())
            },
        )
        .unwrap();
    drop(transaction);

    encoded
        .iter()
        .flat_map(|encoded| {
            let change = decode_change(encoded).unwrap();
            change
                .records()
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn output_bounds(station: &Station, transactions: &mut Transactions) -> std::ops::Range<u64> {
    let transaction = transactions.begin().unwrap();
    station
        .output
        .as_ref()
        .unwrap()
        .access(transaction.access())
        .unwrap()
        .bounds()
        .unwrap()
}
