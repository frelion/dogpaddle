use super::DeliveryBatch;
use super::runtime::BufferedSink;
use super::state::{self, BufferState, Position, State};
use crate::operation::sink::relation::{RelationTarget, plan};
use crate::operation::{OperationError, SinkOperation};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_store::{Cell, OrderedMap, Store, StoreSetup};
use std::sync::Arc;

struct EndpointTarget;

impl RelationTarget for EndpointTarget {
    fn require_absent(&mut self) -> Result<(), OperationError> {
        unreachable!("tests seed initialized Ready state")
    }
    fn initialize(&mut self) -> Result<(), OperationError> {
        unreachable!("tests seed initialized Ready state")
    }
    fn deliver_prefix(
        &mut self,
        input: &DeliveryBatch,
        tail: u64,
        original_head: (u64, &Change),
    ) -> Result<(), OperationError> {
        let batch = plan(
            input,
            input.first_event_offset(),
            tail,
            original_head,
            |_| unreachable!("positive-only delivery must not look up target rows"),
        )?;
        assert_eq!(input.change().diffs().values().as_ref(), &[1]);
        assert_eq!(batch.inserts.len(), 1);
        assert_eq!(batch.inserts[0].technical_id, u64::MAX - 1);
        assert!(batch.deletes.is_empty());
        Ok(())
    }
}

fn integer_change(diff: i64) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap(),
        Int64Array::from(vec![diff]),
    )
    .unwrap()
}

#[test]
fn ready_control_has_fixed_event_interval_layout_including_exhausted_empty_tail() {
    let initial = BufferState {
        head: Position {
            entry_start: 7,
            event_offset: 9,
        },
        tail: 12,
        retained_bytes: 30,
    };
    let golden = [
        1, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0,
        0, 0, 0, 30,
    ];
    assert_eq!(State::Ready(initial).encode(), golden);
    assert_eq!(golden.len(), state::MAX_CONTROL_BYTES);
    let State::Ready(decoded) = state::decode(&golden).unwrap() else {
        panic!("expected Ready");
    };
    assert_eq!(decoded, initial);
    assert_eq!(decoded.pending_events(), 3);
    let exhausted = BufferState {
        head: Position::entry_start(u64::MAX),
        tail: u64::MAX,
        retained_bytes: 0,
    };
    exhausted.validate().unwrap();
    let State::Ready(decoded) = state::decode(&State::Ready(exhausted).encode()).unwrap() else {
        panic!("expected Ready");
    };
    assert_eq!(decoded, exhausted);
    assert!(decoded.is_empty());
}

#[test]
fn control_rejects_every_truncation_trailing_bytes_and_unknown_phase() {
    let ready = State::Ready(BufferState {
        head: Position::entry_start(7),
        tail: 12,
        retained_bytes: 30,
    })
    .encode();
    for encoded in [State::Initialize.encode(), ready] {
        for length in 0..encoded.len() {
            assert!(
                state::decode(&encoded[..length]).is_err(),
                "accepted truncation at {length}"
            );
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(state::decode(&trailing).is_err());
    }
    for encoded in [[1, 2], [1, 255], [2, 0]] {
        assert!(state::decode(&encoded).is_err());
    }
}

#[test]
fn control_rejects_reversed_offsets_and_empty_head_disagreement() {
    for buffer in [
        BufferState {
            head: Position::entry_start(0),
            tail: 1,
            retained_bytes: 8,
        },
        BufferState {
            head: Position {
                entry_start: 3,
                event_offset: 2,
            },
            tail: 4,
            retained_bytes: 8,
        },
        BufferState {
            head: Position {
                entry_start: 1,
                event_offset: 3,
            },
            tail: 2,
            retained_bytes: 8,
        },
        BufferState {
            head: Position {
                entry_start: 1,
                event_offset: 3,
            },
            tail: 3,
            retained_bytes: 0,
        },
        BufferState {
            head: Position::entry_start(3),
            tail: 3,
            retained_bytes: 8,
        },
        BufferState {
            head: Position::entry_start(3),
            tail: 4,
            retained_bytes: 0,
        },
    ] {
        assert!(state::decode(&State::Ready(buffer).encode()).is_err());
    }
}

#[test]
fn recovery_rejects_a_retained_original_entry_that_exceeded_admission_capacity() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let control = store.create_data::<Cell<Vec<u8>>>(super::CONTROL).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>(super::BUFFER)
        .unwrap();
    let change = integer_change(1_048_577);
    let codec = SchemaBoundChangeCodec::try_new(change.records().schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    // Only one event remains, but no accepted entry could contain this original span.
    let initial = BufferState {
        head: Position {
            entry_start: 1,
            event_offset: 1_048_577,
        },
        tail: 1_048_578,
        retained_bytes: u64::try_from(encoded.len()).unwrap() + 8,
    };
    let (mut writes, reads) = store
        .commit(root.path().join("store"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let transaction = writes.begin();
        buffer
            .access(transaction.access())
            .unwrap()
            .put(&1, &encoded)
            .unwrap();
        control
            .access(transaction.access())
            .unwrap()
            .set(&State::Ready(initial).encode())
            .unwrap();
        transaction.commit().unwrap();
    }
    let mut sink = BufferedSink::new(codec, EndpointTarget, control, buffer);
    let snapshot = reads.begin();
    let Err(error) = sink.load(snapshot.access()) else {
        panic!("accepted an entry impossible under admission");
    };
    assert!(error.to_string().contains("buffered-event capacity"));
}

#[test]
fn last_event_offset_settles_and_reopens_then_rejects_all_further_events_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = StoreSetup::new();
    let control = store.create_data::<Cell<Vec<u8>>>(super::CONTROL).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>(super::BUFFER)
        .unwrap();
    let input = integer_change(1);
    let codec = SchemaBoundChangeCodec::try_new(input.records().schema()).unwrap();
    let initial = BufferState {
        head: Position::entry_start(u64::MAX - 1),
        tail: u64::MAX - 1,
        retained_bytes: 0,
    };
    let (mut writes, reads) = store.commit(&path, |_| Ok(())).unwrap().split();
    {
        let transaction = writes.begin();
        control
            .access(transaction.access())
            .unwrap()
            .set(&State::Ready(initial).encode())
            .unwrap();
        transaction.commit().unwrap();
    }
    let mut sink = BufferedSink::new(codec, EndpointTarget, control.clone(), buffer.clone());
    {
        let snapshot = reads.begin();
        assert!(sink.load(snapshot.access()).unwrap().is_none());
    }
    {
        let transaction = writes.begin();
        assert!(sink.try_enqueue(transaction.access(), &input).unwrap());
        transaction.commit().unwrap();
    }
    let pending = {
        let snapshot = reads.begin();
        sink.load(snapshot.access()).unwrap().unwrap()
    };
    assert!(!sink.prepare_initialize(&pending).unwrap());
    sink.deliver(&pending).unwrap();
    {
        let transaction = writes.begin();
        sink.settle(transaction.access(), &pending).unwrap();
        transaction.commit().unwrap();
    }
    let expected = State::Ready(BufferState {
        head: Position::entry_start(u64::MAX),
        tail: u64::MAX,
        retained_bytes: 0,
    })
    .encode();
    for diff in [1, -1] {
        let transaction = writes.begin();
        let error = sink
            .try_enqueue(transaction.access(), &integer_change(diff))
            .unwrap_err();
        assert!(error.to_string().contains("event offset is exhausted"));
        // Committing the failed admission proves no partial Store write was made.
        transaction.commit().unwrap();
        let snapshot = reads.begin();
        let access = snapshot.access();
        assert_eq!(
            control.read(access).unwrap().get().unwrap(),
            Some(expected.clone())
        );
        for key in [u64::MAX - 1, u64::MAX] {
            assert!(buffer.read(access).unwrap().get(&key).unwrap().is_none());
        }
    }
    drop((sink, pending, writes, reads));
    let store = Store::open(&path).unwrap();
    let control = store.open_data::<Cell<Vec<u8>>>(super::CONTROL).unwrap();
    let buffer = store
        .open_data::<OrderedMap<u64, Vec<u8>>>(super::BUFFER)
        .unwrap();
    let mut sink = BufferedSink::new(
        SchemaBoundChangeCodec::try_new(input.records().schema()).unwrap(),
        EndpointTarget,
        control.clone(),
        buffer,
    );
    let (_, reads) = store.into_transactions().split();
    let snapshot = reads.begin();
    assert!(sink.load(snapshot.access()).unwrap().is_none());
    assert_eq!(
        control.read(snapshot.access()).unwrap().get().unwrap(),
        Some(expected)
    );
}
