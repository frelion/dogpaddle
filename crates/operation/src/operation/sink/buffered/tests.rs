use super::runtime::BufferedSink;
use super::state::{self, BufferState, Header, Position, Prepared, State};
use crate::operation::sink::relation::{Batch, Lookup, Matches, RelationTarget};
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
    fn lookup(&mut self, _: &Change, _: &[Lookup]) -> Result<Vec<Matches>, OperationError> {
        unreachable!("positive-only delivery must not look up target rows")
    }
    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        assert_eq!(input.diffs().values().as_ref(), &[1]);
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
    assert_eq!(golden.len(), state::MAX_READY_BYTES);
    let Header::Ready(decoded) = state::decode_header(&golden).unwrap() else {
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
    let Header::Ready(decoded) = state::decode_header(&State::Ready(exhausted).encode()).unwrap()
    else {
        panic!("expected Ready");
    };
    assert_eq!(decoded, exhausted);
    assert!(decoded.is_empty());
}

#[test]
fn prepared_control_stores_only_boundaries_and_ordered_negative_ids() {
    let before = BufferState {
        head: Position::entry_start(7),
        tail: 12,
        retained_bytes: 30,
    };
    let after = BufferState {
        head: Position {
            entry_start: 7,
            event_offset: 10,
        },
        tail: 12,
        retained_bytes: 30,
    };
    let state = State::Prepared(Prepared {
        before,
        after,
        negative_ids: vec![4, 2],
    });
    state.validate().unwrap();
    let golden = [
        1, 2, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0,
        0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 12, 0,
        0, 0, 0, 0, 0, 0, 30, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2,
    ];
    assert_eq!(state.encode(), golden);
    let Header::Prepared {
        before: decoded_before,
        after: decoded_after,
        encoded_negative_ids,
    } = state::decode_header(&golden).unwrap()
    else {
        panic!("expected Prepared");
    };
    assert_eq!((decoded_before, decoded_after), (before, after));
    assert_eq!(
        state::decode_negative_ids(encoded_negative_ids).unwrap(),
        [4, 2]
    );
    for length in 0..golden.len() {
        let result = state::decode_header(&golden[..length]).and_then(|header| match header {
            Header::Prepared {
                encoded_negative_ids,
                ..
            } => state::decode_negative_ids(encoded_negative_ids).map(|_| ()),
            _ => Ok(()),
        });
        assert!(result.is_err(), "accepted truncation at {length}");
    }
    let mut trailing = golden.to_vec();
    trailing.push(0);
    let Header::Prepared {
        encoded_negative_ids,
        ..
    } = state::decode_header(&trailing).unwrap()
    else {
        panic!("expected Prepared");
    };
    assert!(state::decode_negative_ids(encoded_negative_ids).is_err());
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
        assert!(state::decode_header(&State::Ready(buffer).encode()).is_err());
    }
    assert!(state::decode_negative_ids(&[4, 1]).is_err());
    assert!(state::decode_negative_ids(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(state::decode_negative_ids(&[0, 1, 255, 255, 255, 255, 255, 255, 255, 255]).is_err());
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
fn maximum_control_length_is_the_exact_bounded_negative_id_layout() {
    let prepared = State::Prepared(Prepared {
        before: BufferState {
            head: Position::entry_start(1025),
            tail: 2049,
            retained_bytes: 8,
        },
        after: BufferState {
            head: Position::entry_start(2049),
            tail: 2049,
            retained_bytes: 0,
        },
        negative_ids: (1..=1024).collect(),
    });
    prepared.validate().unwrap();
    assert_eq!(prepared.encode().len(), 8260);
    assert_eq!(state::MAX_CONTROL_BYTES, 8260);
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
    let prepared = sink.prepare(pending).unwrap();
    {
        let transaction = writes.begin();
        sink.persist_prepared(transaction.access(), &prepared)
            .unwrap();
        transaction.commit().unwrap();
    }
    sink.deliver(&prepared).unwrap();
    {
        let transaction = writes.begin();
        sink.settle(transaction.access(), &prepared).unwrap();
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
    drop((sink, prepared, writes, reads));
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
