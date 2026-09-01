use std::{
    num::{NonZeroU32, NonZeroU64},
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
    AppendLog, OrderedMap, ReadOnly, ReadTransactions, Small, Store, TransactionAccess,
    Transactions,
};

use crate::{
    build::FlowFactory,
    flow::{AdvanceOutcome, Flow},
};

use super::{
    ACTIVE_INPUT_KEY, CURSOR_ORIGIN, ConsumerCursor, InputPort, OutputRetention, Station,
    StationParts, cursor_key, decode_active_input, decode_cursor, encode_active_input,
    encode_cursor, protocol::StationError,
};

struct RuntimeFixture {
    _root: tempfile::TempDir,
    transactions: Transactions,
    reads: ReadTransactions,
    stations: Vec<Station>,
}

struct FixedOperation {
    action: Action,
}

enum WriteResult {
    Idle,
    Commit,
    Fail,
}

struct WritingOperation {
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    result: WriteResult,
}

impl Operation for FixedOperation {
    fn turn(
        &self,
        _input: Option<OperationInput<'_>>,
        _access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        Ok(self.action.clone())
    }
}

impl Operation for WritingOperation {
    fn turn(
        &self,
        _input: Option<OperationInput<'_>>,
        access: TransactionAccess<'_>,
    ) -> Result<Action, OperationError> {
        self.state
            .access(access)?
            .put(&b"attempt".to_vec(), &b"written".to_vec())?;
        match self.result {
            WriteResult::Idle => Ok(Action::Idle),
            WriteResult::Commit => Ok(Action::Commit(None)),
            WriteResult::Fail => Err(std::io::Error::other("planned turn failure").into()),
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
fn assembly_injects_private_inboxes_and_preserves_output_properties() {
    let fixture = source_count_sink(NonZeroU64::MAX, NonZeroU64::MAX);

    assert!(fixture.stations[0].inbox.ports().is_empty());
    assert!(fixture.stations[0].output.is_some());
    assert_eq!(fixture.stations[1].inbox.ports().len(), 1);
    assert!(fixture.stations[1].output.is_some());
    assert_eq!(fixture.stations[2].inbox.ports().len(), 1);
    assert!(fixture.stations[2].output.is_none());
}

#[test]
fn intake_owns_one_claim_and_is_an_idempotent_memory_hit() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );

    assert!(
        !fixture.stations[1]
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let first = fixture.stations[1].inbox.cached_claim().unwrap();
    assert_eq!((first.port(), first.offset()), (0, 0));
    let identity = std::ptr::from_ref(first.change());
    let encoded = encode_change(first.change()).unwrap();

    assert!(
        !fixture.stations[1]
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    assert_eq!(
        std::ptr::from_ref(fixture.stations[1].inbox.cached_claim().unwrap().change()),
        identity
    );

    fixture.stations[1].inbox.clear_cached_claim();
    assert!(
        !fixture.stations[1]
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let rebuilt = fixture.stations[1].inbox.cached_claim().unwrap();
    assert_eq!((rebuilt.port(), rebuilt.offset()), (0, 0));
    assert_eq!(encode_change(rebuilt.change()).unwrap(), encoded);
}

#[test]
fn complete_advances_cursor_selector_and_at_most_one_physical_head_atomically() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..2
    );

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        1
    );
    assert_eq!(
        read_active(&fixture.stations[1], &mut fixture.transactions),
        0
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..2
    );
    assert!(fixture.stations[1].inbox.cached_claim().is_none());

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        2
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        2..2
    );
}

#[test]
fn fanout_reclaims_only_after_every_consumer_crosses_the_same_head() {
    let mut fixture = source_sink(2, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        1
    );
    assert_eq!(
        read_cursor(&fixture.stations[2], &mut fixture.transactions, 0),
        0
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..2
    );

    assert_eq!(
        advance(&mut fixture, 2).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..2
    );

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..2
    );
    assert_eq!(
        advance(&mut fixture, 2).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        2..2
    );
}

#[test]
fn duplicate_edges_share_one_retention_and_acknowledge_the_entry_independently() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("flow")).unwrap();
    let state = store
        .create_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>("state")
        .unwrap();
    let output = store
        .create_data::<AppendLog<Vec<u8>>>("producer-output")
        .unwrap();
    let parts = StationParts::new(
        state.clone(),
        Box::new(FixedOperation {
            action: Action::Complete(None),
        }),
        OperationKind::Sink(NonZeroU32::new(2).unwrap()),
        None,
    );
    let (mut transactions, reads) = store.into_transactions().split();
    {
        let transaction = transactions.begin().unwrap();
        parts.initialize_input_state(transaction.access()).unwrap();
        output
            .access(transaction.access())
            .unwrap()
            .append(&encode_change(&change(&[7])).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    let retention = Arc::new(OutputRetention::new(
        output.clone(),
        vec![
            ConsumerCursor::new(ReadOnly::new(state.clone()), 0),
            ConsumerCursor::new(ReadOnly::new(state.clone()), 1),
        ],
    ));
    let mut station = parts.finish(vec![
        InputPort::new(ReadOnly::new(output.clone()), Arc::clone(&retention), 0),
        InputPort::new(ReadOnly::new(output.clone()), retention, 1),
    ]);

    assert_eq!(
        station.advance(&reads, &mut transactions).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(read_cursor(&station, &mut transactions, 0), 1);
    assert_eq!(read_cursor(&station, &mut transactions, 1), 0);
    assert_eq!(read_active(&station, &mut transactions), 1);
    assert_eq!(output_bounds_log(&output, &mut transactions), 0..1);

    assert_eq!(
        station.advance(&reads, &mut transactions).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(read_cursor(&station, &mut transactions, 1), 1);
    assert_eq!(read_active(&station, &mut transactions), 0);
    assert_eq!(output_bounds_log(&output, &mut transactions), 1..1);
}

#[test]
fn commit_is_keep_and_reoffers_the_identical_owned_claim() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    fixture.stations[1].operation = Box::new(FixedOperation {
        action: Action::Commit(None),
    });

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    let identity = std::ptr::from_ref(fixture.stations[1].inbox.cached_claim().unwrap().change());
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        0
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..1
    );

    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        std::ptr::from_ref(fixture.stations[1].inbox.cached_claim().unwrap().change()),
        identity
    );
    fixture.stations[1].operation = Box::new(FixedOperation {
        action: Action::Complete(None),
    });
    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert!(fixture.stations[1].inbox.cached_claim().is_none());
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..1
    );
}

#[test]
fn idle_and_operation_error_roll_back_writes_without_releasing_the_claim() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    let state = fixture.stations[1].state.clone();
    fixture.stations[1].operation = Box::new(WritingOperation {
        state: state.clone(),
        result: WriteResult::Idle,
    });

    assert_eq!(advance(&mut fixture, 1).unwrap(), AdvanceOutcome::Idle);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert!(fixture.stations[1].inbox.cached_claim().is_some());

    fixture.stations[1].operation = Box::new(WritingOperation {
        state: state.clone(),
        result: WriteResult::Fail,
    });
    assert!(matches!(
        advance(&mut fixture, 1),
        Err(StationError::Operation(_))
    ));
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert!(fixture.stations[1].inbox.cached_claim().is_some());

    fixture.stations[1].operation = Box::new(WritingOperation {
        state: state.clone(),
        result: WriteResult::Commit,
    });
    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions),
        Some(b"written".to_vec())
    );
    assert!(fixture.stations[1].inbox.cached_claim().is_some());
}

#[test]
fn complete_backpressure_rolls_back_output_state_cursor_and_reclaim() {
    let mut fixture = source_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..1
    );
    assert_eq!(
        output_bounds(&fixture.stations[1], &mut fixture.transactions),
        0..1
    );

    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Backpressured
    );
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        1
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        1..2
    );
    assert_eq!(
        output_bounds(&fixture.stations[1], &mut fixture.transactions),
        0..1
    );
    assert_eq!(
        fixture.stations[1].inbox.cached_claim().unwrap().offset(),
        1
    );

    assert_eq!(
        advance(&mut fixture, 2).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        advance(&mut fixture, 1).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        2..2
    );
    assert_eq!(
        output_bounds(&fixture.stations[1], &mut fixture.transactions),
        1..2
    );
}

#[test]
fn unexpected_output_rolls_back_completion_and_preserves_the_claim() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    fixture.stations[1].operation = Box::new(FixedOperation {
        action: Action::Complete(Some(change(&[9]))),
    });

    assert!(matches!(
        advance(&mut fixture, 1),
        Err(StationError::UnexpectedOutput)
    ));
    assert!(fixture.stations[1].inbox.cached_claim().is_some());
    assert_eq!(
        read_cursor(&fixture.stations[1], &mut fixture.transactions, 0),
        0
    );
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..1
    );
}

#[test]
fn source_complete_is_rejected_without_committing_its_turn() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    fixture.stations[0].operation = Box::new(FixedOperation {
        action: Action::Complete(None),
    });

    assert!(matches!(
        advance(&mut fixture, 0),
        Err(StationError::OperationCompletedWithoutInput)
    ));
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..0
    );
}

#[test]
fn destructive_complete_validates_the_old_cursor_vector_before_writing() {
    let mut fixture = source_sink(1, NonZeroU64::MAX);
    assert_eq!(
        advance(&mut fixture, 0).unwrap(),
        AdvanceOutcome::Progressed
    );
    assert!(
        !fixture.stations[1]
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    write_cursor(&fixture.stations[1], &mut fixture.transactions, 0, 1);

    assert!(matches!(
        fixture.stations[1].process(&mut fixture.transactions),
        Err(StationError::RetentionHeadMismatch {
            head: 0,
            minimum: 1
        })
    ));
    assert!(fixture.stations[1].inbox.cached_claim().is_some());
    assert_eq!(
        output_bounds(&fixture.stations[0], &mut fixture.transactions),
        0..1
    );
}

fn source_sink(consumer_count: usize, source_capacity: NonZeroU64) -> RuntimeFixture {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
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
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let count = builder.station("count", CountDefinition::new());
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.output_capacity_bytes(source, source_capacity);
    builder.output_capacity_bytes(count, count_capacity);
    builder.connect([source], count);
    builder.connect([count], sink);
    fixture(root, builder.build().unwrap())
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

fn advance(fixture: &mut RuntimeFixture, station: usize) -> Result<AdvanceOutcome, StationError> {
    fixture.stations[station].advance(&fixture.reads, &mut fixture.transactions)
}

fn read_active(station: &Station, transactions: &mut Transactions) -> usize {
    let transaction = transactions.begin().unwrap();
    let encoded = station
        .state
        .access(transaction.access())
        .unwrap()
        .get(&ACTIVE_INPUT_KEY.to_vec())
        .unwrap()
        .unwrap();
    decode_active_input(&encoded).unwrap()
}

fn read_cursor(station: &Station, transactions: &mut Transactions, input: usize) -> u64 {
    let transaction = transactions.begin().unwrap();
    let encoded = station
        .state
        .access(transaction.access())
        .unwrap()
        .get(&cursor_key(input))
        .unwrap()
        .unwrap();
    decode_cursor(&encoded).unwrap()
}

fn write_cursor(station: &Station, transactions: &mut Transactions, input: usize, offset: u64) {
    let transaction = transactions.begin().unwrap();
    station
        .state
        .access(transaction.access())
        .unwrap()
        .put(&cursor_key(input), &encode_cursor(offset).to_vec())
        .unwrap();
    transaction.commit().unwrap();
}

fn read_attempt(
    state: &OrderedMap<Vec<u8>, Vec<u8>, Small>,
    transactions: &mut Transactions,
) -> Option<Vec<u8>> {
    let transaction = transactions.begin().unwrap();
    state
        .access(transaction.access())
        .unwrap()
        .get(&b"attempt".to_vec())
        .unwrap()
}

fn output_bounds(station: &Station, transactions: &mut Transactions) -> std::ops::Range<u64> {
    output_bounds_log(station.output.as_ref().unwrap().log(), transactions)
}

fn output_bounds_log(
    output: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
) -> std::ops::Range<u64> {
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
