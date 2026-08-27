use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_operation::{
    encode_definition,
    operation::{
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{
    AppendLog, Cell, OrderedMap, ReadOnly, ReadTransactions, Small, Store, Transactions,
};

use crate::{build::FlowFactory, flow::Flow};

use super::{
    Cursor, Input, Station, StationParts, cursor_key,
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

impl IntakeFixture {
    fn write_cursor(&mut self, station: usize, input: usize, cursor: Cursor) {
        let transaction = self.transactions.begin().unwrap();
        self.stations[station]
            .state
            .access(transaction.access())
            .unwrap()
            .put(&cursor_key(input), &cursor.encode().to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
}

#[test]
fn two_phase_protocol_separates_read_intake_from_transactional_processing() {
    let _: fn(&mut Station, &ReadTransactions) -> Result<(), StationError> = Station::intake;
    let _: fn(&mut Station, &mut Transactions) -> Result<ProcessOutcome, StationError> =
        Station::process;
    assert_ne!(ProcessOutcome::Idle, ProcessOutcome::Progressed);
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

    assert!(fixture.source.inputs.is_empty());
    assert_eq!(fixture.count.inputs.len(), 1);
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
        fixture.count.inputs[0]
            .log
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

    fixture.stations[1].intake(&fixture.reads).unwrap();
    let first_column = {
        let change = fixture.stations[1].inputs[0].cache.as_ref().unwrap();
        assert_eq!(change.records(), fixture.changes[0].records());
        assert_eq!(change.diffs(), fixture.changes[0].diffs());
        change.records().column(0).clone()
    };

    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 0,
            row_index: u64::MAX,
        },
    );
    fixture.stations[1].intake(&fixture.reads).unwrap();

    let change = fixture.stations[1].inputs[0].cache.as_ref().unwrap();
    assert_eq!(change.num_rows(), 3);
    assert!(Arc::ptr_eq(&first_column, change.records().column(0)));
}

#[test]
fn intake_loads_the_next_entry_after_the_cursor_advances_and_cache_is_released() {
    let mut fixture = flow_with_changes(&[&[10, 11], &[20]]);

    fixture.stations[1].intake(&fixture.reads).unwrap();
    let first_column = fixture.stations[1].inputs[0]
        .cache
        .as_ref()
        .unwrap()
        .records()
        .column(0)
        .clone();

    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 1,
            row_index: 0,
        },
    );
    fixture.stations[1].inputs[0].cache = None;
    fixture.stations[1].intake(&fixture.reads).unwrap();
    let change = fixture.stations[1].inputs[0].cache.as_ref().unwrap();
    assert_eq!(change.records(), fixture.changes[1].records());
    assert!(!Arc::ptr_eq(&first_column, change.records().column(0)));

    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 2,
            row_index: 0,
        },
    );
    fixture.stations[1].inputs[0].cache = None;
    fixture.stations[1].intake(&fixture.reads).unwrap();
    assert!(fixture.stations[1].inputs[0].cache.is_none());
}

#[test]
fn intake_rejects_noncanonical_cursor_rows_without_populating_the_cache() {
    let mut fixture = flow_with_changes(&[&[10, 11]]);

    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 0,
            row_index: 2,
        },
    );
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(
        error,
        StationError::CursorRowOutOfRange {
            input: 0,
            row_index: 2,
            rows: 2,
        }
    ));
    assert!(fixture.stations[1].inputs[0].cache.is_none());
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
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(error, StationError::MissingCursor { input: 0 }));
    assert!(fixture.stations[1].inputs[0].cache.is_none());
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
            .put(&cursor_key(0), &vec![0; 15])
            .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(error, StationError::MalformedCursor { input: 0 }));
    assert!(fixture.stations[1].inputs[0].cache.is_none());
}

#[test]
fn intake_rejects_a_nonzero_row_at_the_log_tail() {
    let mut fixture = flow_with_changes(&[]);
    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 0,
            row_index: 1,
        },
    );
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(
        error,
        StationError::NonzeroRowAtTail {
            input: 0,
            offset: 0,
            row_index: 1,
        }
    ));
    assert!(fixture.stations[1].inputs[0].cache.is_none());
}

#[test]
fn intake_surfaces_invalid_change_bytes_without_populating_the_cache() {
    let mut fixture = flow_with_changes(&[]);
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
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(
        error,
        StationError::InvalidInputChange { input: 0, .. }
    ));
    assert!(fixture.stations[1].inputs[0].cache.is_none());
}

#[test]
fn intake_does_not_install_earlier_misses_when_a_later_input_is_invalid() {
    let mut fixture = flow_with_changes(&[&[10, 11]]);
    let second_log = fixture.stations[1].output.as_ref().unwrap().clone();
    fixture.stations[1]
        .inputs
        .push(Input::new(ReadOnly::new(second_log)));
    fixture.write_cursor(1, 1, Cursor::ORIGIN);
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
    let error = fixture.stations[1].intake(&fixture.reads).unwrap_err();

    assert!(matches!(
        error,
        StationError::InvalidInputChange { input: 1, .. }
    ));
    assert!(
        fixture.stations[1]
            .inputs
            .iter()
            .all(|input| input.cache.is_none())
    );
}

#[test]
fn reopen_rebuilds_an_empty_cache_from_the_durable_partial_cursor() {
    let mut fixture = flow_with_changes(&[&[10, 11, 12]]);
    fixture.write_cursor(
        1,
        0,
        Cursor {
            offset: 0,
            row_index: 1,
        },
    );
    let IntakeFixture {
        transactions,
        reads,
        stations,
        changes,
        root,
    } = fixture;
    drop(stations);
    drop(reads);
    drop(transactions);

    let flow = FlowFactory::open(root.path().join("flow")).unwrap();
    let (_transactions, reads, mut stations) = flow.into_runtime_parts();
    assert!(stations[1].inputs[0].cache.is_none());

    stations[1].intake(&reads).unwrap();
    let change = stations[1].inputs[0].cache.as_ref().unwrap();
    assert_eq!(change.records(), changes[0].records());
    assert_eq!(change.diffs(), changes[0].diffs());
}

#[test]
fn cursor_keys_and_values_have_stable_port_ordered_encodings() {
    assert_eq!(cursor_key(0), b"input/00000000/cursor");
    assert_eq!(cursor_key(15), b"input/0000000f/cursor");
    assert_eq!(
        Cursor {
            offset: 0x0102_0304_0506_0708,
            row_index: 0x1112_1314_1516_1718,
        }
        .encode(),
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18,
        ]
    );
    assert_eq!(
        Cursor::decode(&Cursor::ORIGIN.encode()),
        Some(Cursor::ORIGIN)
    );
    assert_eq!(Cursor::decode(&[0; 15]), None);
}

fn assert_station_wiring(flow: Flow) {
    let (mut transactions, _reads, stations) = flow.into_runtime_parts();
    assert_eq!(stations.len(), 2);
    assert_eq!(stations[0].inputs.len(), 1);
    assert!(stations[0].output.is_some());
    assert!(stations[1].inputs.is_empty());
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
        stations[0].inputs[0]
            .log
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
    let source =
        StationParts::new(source_state, source_operation, Some(source_output)).finish(Vec::new());
    let count = StationParts::new(count_state, count_operation, Some(count_output))
        .finish(vec![count_input]);
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
