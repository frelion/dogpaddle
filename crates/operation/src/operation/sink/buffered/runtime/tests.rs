use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, encode_change};
use dogpaddle_store::{Cell, OrderedMap, Store, Transactions};

use super::*;
use crate::operation::sink::buffered::{
    batch::encoded_item_bytes,
    state::{Header, Position},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Plan {
    batch_id: u64,
    events: u64,
    fingerprint: u64,
}

#[derive(Default)]
struct TargetState {
    present: bool,
    initialize_calls: usize,
    prepare_calls: usize,
    delivery_attempts: usize,
    delivered: BTreeMap<u64, Plan>,
    fail_before_delivery_once: bool,
    fail_after_delivery_once: bool,
    event_bytes: u64,
    recovery_positive_capacity: Option<u64>,
}

struct Target {
    state: Arc<Mutex<TargetState>>,
}

impl Target {
    fn new(state: Arc<Mutex<TargetState>>) -> Self {
        Self { state }
    }
}

impl SinkTarget for Target {
    type Checkpoint = u64;
    type Plan = Plan;

    const MAX_BATCH_EVENTS: NonZeroU32 = NonZeroU32::new(3).unwrap();

    fn require_absent(&mut self) -> Result<(), OperationError> {
        if self.state.lock().unwrap().present {
            Err(invalid("fake target already exists"))
        } else {
            Ok(())
        }
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        let mut state = self.state.lock().unwrap();
        state.initialize_calls += 1;
        state.present = true;
        Ok(())
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        0
    }

    fn event_bytes(&self, _input: &Change, _row_index: usize) -> Result<u64, OperationError> {
        Ok(self.state.lock().unwrap().event_bytes.max(1))
    }

    fn validate_admission(
        &self,
        _input: &Change,
        _checkpoint: &Self::Checkpoint,
        _buffered_events: u64,
    ) -> Result<(), OperationError> {
        Ok(())
    }

    fn validate_recovery(
        &self,
        _checkpoint: &Self::Checkpoint,
        remaining_positive_events: u64,
    ) -> Result<(), OperationError> {
        if self
            .state
            .lock()
            .unwrap()
            .recovery_positive_capacity
            .is_some_and(|capacity| remaining_positive_events > capacity)
        {
            Err(invalid("fake recovery capacity is exhausted"))
        } else {
            Ok(())
        }
    }

    fn prepare(
        &mut self,
        input: &DeliveryBatch,
        checkpoint: &Self::Checkpoint,
        batch_id: u64,
    ) -> Result<(Self::Checkpoint, Self::Plan), OperationError> {
        self.state.lock().unwrap().prepare_calls += 1;
        let events = batch::event_count(input.change())?;
        let checkpoint = checkpoint
            .checked_add(events)
            .ok_or_else(|| invalid("fake checkpoint overflow"))?;
        Ok((
            checkpoint,
            Plan {
                batch_id,
                events,
                fingerprint: fingerprint(input),
            },
        ))
    }

    fn deliver(
        &mut self,
        input: &DeliveryBatch,
        batch_id: u64,
        plan: &Self::Plan,
    ) -> Result<(), OperationError> {
        let expected = Plan {
            batch_id,
            events: batch::event_count(input.change())?,
            fingerprint: fingerprint(input),
        };
        if *plan != expected {
            return Err(invalid("fake plan differs from its delivery"));
        }

        let mut state = self.state.lock().unwrap();
        state.delivery_attempts += 1;
        if std::mem::take(&mut state.fail_before_delivery_once) {
            return Err(invalid("fake delivery failed before commit"));
        }
        match state.delivered.get(&batch_id) {
            Some(previous) if previous != plan => {
                return Err(invalid("fake batch ID was reused with a different plan"));
            }
            Some(_) => {}
            None => {
                state.delivered.insert(batch_id, plan.clone());
            }
        }
        if std::mem::take(&mut state.fail_after_delivery_once) {
            return Err(invalid("fake delivery result was uncertain"));
        }
        Ok(())
    }

    fn encode_checkpoint(checkpoint: &Self::Checkpoint, output: &mut Vec<u8>) {
        output.extend(checkpoint.to_be_bytes());
    }

    fn decode_checkpoint(input: &mut &[u8]) -> Result<Self::Checkpoint, OperationError> {
        Ok(u64::from_be_bytes(state::read(input)?))
    }

    fn encode_plan(plan: &Self::Plan, output: &mut Vec<u8>) {
        output.push(1);
        output.extend(plan.batch_id.to_be_bytes());
        output.extend(plan.events.to_be_bytes());
        output.extend(plan.fingerprint.to_be_bytes());
    }

    fn decode_plan(
        input: &mut &[u8],
        change: &DeliveryBatch,
        _checkpoint: &Self::Checkpoint,
    ) -> Result<Self::Plan, OperationError> {
        if state::read::<1>(input)? != [1] {
            return Err(invalid("unknown fake-plan version"));
        }
        let plan = Plan {
            batch_id: u64::from_be_bytes(state::read(input)?),
            events: u64::from_be_bytes(state::read(input)?),
            fingerprint: u64::from_be_bytes(state::read(input)?),
        };
        if plan.events != batch::event_count(change.change())?
            || plan.fingerprint != fingerprint(change)
        {
            return Err(invalid(
                "fake plan does not match its reconstructed delivery",
            ));
        }
        Ok(plan)
    }
}

fn fingerprint(batch: &DeliveryBatch) -> u64 {
    let values = batch.change().records().column(0);
    let values = values.as_any().downcast_ref::<UInt64Array>().unwrap();
    batch.change().diffs().values().iter().enumerate().fold(
        0xcbf2_9ce4_8422_2325_u64,
        |hash, (row, diff)| {
            hash.wrapping_mul(0x100_0000_01b3)
                ^ u64::from_be_bytes(diff.to_be_bytes())
                ^ batch.admission(row).rotate_left(17)
                ^ values.value(row).rotate_left(31)
        },
    )
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn change(values: &[u64], diffs: &[i64]) -> Change {
    assert_eq!(values.len(), diffs.len());
    Change::try_new(
        RecordBatch::try_new(schema(), vec![Arc::new(UInt64Array::from(values.to_vec()))]).unwrap(),
        Int64Array::from(diffs.to_vec()),
    )
    .unwrap()
}

fn input(change: &Change) -> OperationInput<'_> {
    OperationInput { port: 0, change }
}

struct Fixture {
    _root: tempfile::TempDir,
    path: PathBuf,
    schema: SchemaRef,
    target: Arc<Mutex<TargetState>>,
    control: Cell<Vec<u8>>,
    buffer: OrderedMap<u64, Vec<u8>>,
    operation: BufferedSink<Target>,
    transactions: Transactions,
}

impl Fixture {
    fn create() -> Self {
        Self::create_with_schema(schema())
    }

    fn create_with_schema(schema: SchemaRef) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let mut store = Store::create(&path).unwrap();
        let control = store.create_data::<Cell<Vec<u8>>>("sink.control").unwrap();
        let buffer = store
            .create_data::<OrderedMap<u64, Vec<u8>>>("sink.buffer")
            .unwrap();
        let target = Arc::new(Mutex::new(TargetState::default()));
        let operation = BufferedSink::new(
            Arc::clone(&schema),
            Target::new(Arc::clone(&target)),
            control.clone(),
            buffer.clone(),
        );
        Self {
            _root: root,
            path,
            schema,
            target,
            control,
            buffer,
            operation,
            transactions: store.into_transactions(),
        }
    }

    fn reopen(self) -> Self {
        let Self {
            _root,
            path,
            schema,
            target,
            control,
            buffer,
            operation,
            transactions,
        } = self;
        drop((control, buffer, operation, transactions));
        let store = Store::open(&path).unwrap();
        let control = store.open_data::<Cell<Vec<u8>>>("sink.control").unwrap();
        let buffer = store
            .open_data::<OrderedMap<u64, Vec<u8>>>("sink.buffer")
            .unwrap();
        let operation = BufferedSink::new(
            Arc::clone(&schema),
            Target::new(Arc::clone(&target)),
            control.clone(),
            buffer.clone(),
        );
        Self {
            _root,
            path,
            schema,
            target,
            control,
            buffer,
            operation,
            transactions: store.into_transactions(),
        }
    }

    fn bootstrap(&mut self) {
        assert_commit(self.commit(None).unwrap());
        assert_commit(self.commit(None).unwrap());
        assert_commit(self.commit(None).unwrap());
        assert!(matches!(self.commit(None).unwrap(), None));
    }

    fn commit(&mut self, offered: Option<&Change>) -> Result<Option<Action>, OperationError> {
        let turn = self.operation.turn(offered.map(input))?;
        let Turn::Ready(prepared) = turn else {
            return Ok(None);
        };
        let transaction = self.transactions.begin();
        let (action, after_commit) = prepared.apply(transaction.access())?;
        if matches!(&action, Action::Idle) {
            drop(transaction);
            drop(after_commit);
        } else {
            transaction.commit()?;
            after_commit
                .run()
                .map_err(|error| Box::new(error) as OperationError)?;
        }
        Ok(Some(action))
    }

    fn rollback(&mut self, offered: Option<&Change>) -> Result<Action, OperationError> {
        let Turn::Ready(prepared) = self.operation.turn(offered.map(input))? else {
            panic!("expected a prepared turn");
        };
        let transaction = self.transactions.begin();
        let (action, after_commit) = prepared.apply(transaction.access())?;
        drop(transaction);
        drop(after_commit);
        Ok(action)
    }

    fn commit_without_after_commit(
        &mut self,
        offered: Option<&Change>,
    ) -> Result<Action, OperationError> {
        let Turn::Ready(prepared) = self.operation.turn(offered.map(input))? else {
            panic!("expected a prepared turn");
        };
        let transaction = self.transactions.begin();
        let (action, after_commit) = prepared.apply(transaction.access())?;
        transaction.commit()?;
        drop(after_commit);
        Ok(action)
    }

    fn control(&mut self) -> Option<Vec<u8>> {
        let transaction = self.transactions.begin();
        let value = self
            .control
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap();
        transaction.commit().unwrap();
        value
    }

    fn set_control(&mut self, value: &Vec<u8>) {
        let transaction = self.transactions.begin();
        self.control
            .access(transaction.access())
            .unwrap()
            .set(value)
            .unwrap();
        transaction.commit().unwrap();
    }

    fn entry(&mut self, sequence: u64) -> Option<Vec<u8>> {
        let transaction = self.transactions.begin();
        let value = self
            .buffer
            .access(transaction.access())
            .unwrap()
            .get(&sequence)
            .unwrap();
        transaction.commit().unwrap();
        value
    }
}

fn assert_commit(action: Option<Action>) {
    assert!(matches!(action, Some(Action::Commit(None))));
}

fn assert_complete(action: Option<Action>) {
    assert!(matches!(action, Some(Action::Complete(None))));
}

fn ready(bytes: &[u8]) -> Ready<u64> {
    match state::decode_header::<Target>(bytes).unwrap() {
        Header::Ready(ready) => ready,
        Header::Initialize | Header::Prepared { .. } => panic!("expected Ready control state"),
    }
}

#[test]
fn control_codec_has_phase_goldens_and_rejects_corruption() {
    let delivery = DeliveryBatch::for_test(change(&[7], &[2]), vec![2]).unwrap();
    let initialize = State::<u64, Plan>::Initialize;
    assert_eq!(initialize.encode::<Target>(), [1, 0]);
    assert!(matches!(
        State::<u64, Plan>::decode::<Target>(&[1, 0], None).unwrap(),
        State::Initialize
    ));

    let ready_state = State::<u64, Plan>::Ready(Ready {
        buffer: BufferState::EMPTY,
        next_batch_id: u64::MAX,
        checkpoint: 9,
    });
    let ready_bytes = ready_state.encode::<Target>();
    assert_eq!(
        ready_bytes,
        [
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 9,
        ]
    );
    let State::Ready(decoded_ready) =
        State::<u64, Plan>::decode::<Target>(&ready_bytes, None).unwrap()
    else {
        panic!("expected Ready");
    };
    assert_eq!(decoded_ready.buffer, BufferState::EMPTY);
    assert_eq!(decoded_ready.next_batch_id, u64::MAX);
    assert_eq!(decoded_ready.checkpoint, 9);
    let zero_ready = State::<u64, Plan>::Ready(Ready {
        buffer: BufferState::EMPTY,
        next_batch_id: 0,
        checkpoint: 9,
    })
    .encode::<Target>();
    assert!(State::<u64, Plan>::decode::<Target>(&zero_ready, None).is_err());

    let prepared = State::Prepared(Prepared {
        before: BufferState {
            head: Some(Position {
                sequence: 3,
                row_index: 0,
                remaining: 2,
            }),
            tail: 4,
            pending_events: 2,
            retained_bytes: 100,
        },
        after: BufferState::EMPTY,
        batch_id: 5,
        checkpoint: 11,
        plan: Plan {
            batch_id: 5,
            events: 2,
            fingerprint: fingerprint(&delivery),
        },
    });
    let prepared_bytes = prepared.encode::<Target>();
    let State::Prepared(decoded) =
        State::<u64, Plan>::decode::<Target>(&prepared_bytes, Some(&delivery)).unwrap()
    else {
        panic!("expected Prepared");
    };
    assert_eq!(decoded.before.head.unwrap().sequence, 3);
    assert_eq!(decoded.after, BufferState::EMPTY);
    assert_eq!(decoded.plan.batch_id, 5);
    for end in 0..prepared_bytes.len() {
        assert!(
            State::<u64, Plan>::decode::<Target>(&prepared_bytes[..end], Some(&delivery)).is_err(),
            "accepted truncated control state at {end}"
        );
    }
    let mut trailing = prepared_bytes.clone();
    trailing.push(0);
    assert!(State::<u64, Plan>::decode::<Target>(&trailing, Some(&delivery)).is_err());
    let mut corrupt_plan = prepared_bytes;
    *corrupt_plan.last_mut().unwrap() ^= 1;
    assert!(State::<u64, Plan>::decode::<Target>(&corrupt_plan, Some(&delivery)).is_err());
    for invalid_id in [0, u64::MAX] {
        let invalid = State::Prepared(Prepared {
            before: BufferState {
                head: Some(Position {
                    sequence: 3,
                    row_index: 0,
                    remaining: 2,
                }),
                tail: 4,
                pending_events: 2,
                retained_bytes: 100,
            },
            after: BufferState::EMPTY,
            batch_id: invalid_id,
            checkpoint: 11,
            plan: Plan {
                batch_id: invalid_id,
                events: 2,
                fingerprint: fingerprint(&delivery),
            },
        })
        .encode::<Target>();
        assert!(State::<u64, Plan>::decode::<Target>(&invalid, Some(&delivery)).is_err());
    }
}

#[test]
fn loader_slices_mixed_diffs_and_preserves_whole_row_admission() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>("buffer")
        .unwrap();
    let first = change(&[10, 11, 12], &[2, -5, 1]);
    let second = change(&[20, 21], &[-3, 4]);
    let encoded = [
        encode_change(&first).unwrap(),
        encode_change(&second).unwrap(),
    ];
    let retained_bytes = encoded
        .iter()
        .map(|encoded| encoded_item_bytes(encoded).unwrap())
        .sum();
    let before = BufferState {
        head: Some(batch::first_position(0, &first)),
        tail: 2,
        pending_events: 15,
        retained_bytes,
    };
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut map = buffer.access(transaction.access()).unwrap();
    map.put(&0, &encoded[0]).unwrap();
    map.put(&1, &encoded[1]).unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    let first_batch = batch::load(
        &buffer,
        before,
        6,
        MAX_DELIVERY_BYTES,
        MAX_DELIVERY_BYTES,
        MAX_TARGET_BATCH_BYTES,
        &mut None,
        &schema(),
        transaction.access(),
        |_, _| Ok(1),
    )
    .unwrap();
    transaction.commit().unwrap();
    assert_eq!(first_batch.delivery.change().diffs().values(), &[2, -4]);
    assert_eq!(first_batch.delivery.admission(0), 2);
    assert_eq!(first_batch.delivery.admission(1), 5);
    assert_eq!(
        first_batch.after.head,
        Some(Position {
            sequence: 0,
            row_index: 1,
            remaining: 1,
        })
    );

    let transaction = transactions.begin();
    let second_batch = batch::load(
        &buffer,
        first_batch.after,
        4,
        MAX_DELIVERY_BYTES,
        MAX_DELIVERY_BYTES,
        MAX_TARGET_BATCH_BYTES,
        &mut None,
        &schema(),
        transaction.access(),
        |_, _| Ok(1),
    )
    .unwrap();
    transaction.commit().unwrap();
    assert_eq!(
        second_batch.delivery.change().diffs().values(),
        &[-1, 1, -2]
    );
    assert_eq!(second_batch.delivery.admission(0), 1);
    assert_eq!(second_batch.delivery.admission(1), 1);
    assert_eq!(second_batch.delivery.admission(2), 3);
    assert_eq!(
        second_batch.after.head,
        Some(Position {
            sequence: 1,
            row_index: 0,
            remaining: 1,
        })
    );
}

#[test]
fn loader_bounds_cross_entry_aggregation_by_encoded_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>("buffer")
        .unwrap();
    let first = change(&[1], &[1]);
    let second = change(&[2], &[1]);
    let first_encoded = encode_change(&first).unwrap();
    let second_encoded = encode_change(&second).unwrap();
    let first_bytes = encoded_item_bytes(&first_encoded).unwrap();
    let second_bytes = encoded_item_bytes(&second_encoded).unwrap();
    let before = BufferState {
        head: Some(batch::first_position(0, &first)),
        tail: 2,
        pending_events: 2,
        retained_bytes: first_bytes + second_bytes,
    };
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let mut map = buffer.access(transaction.access()).unwrap();
    map.put(&0, &first_encoded).unwrap();
    map.put(&1, &second_encoded).unwrap();
    transaction.commit().unwrap();

    let transaction = transactions.begin();
    let loaded = batch::load(
        &buffer,
        before,
        2,
        first_bytes + second_bytes - 1,
        MAX_DELIVERY_BYTES,
        MAX_TARGET_BATCH_BYTES,
        &mut None,
        &schema(),
        transaction.access(),
        |_, _| Ok(1),
    )
    .unwrap();
    transaction.commit().unwrap();
    assert_eq!(loaded.delivery.change().diffs().values(), &[1]);
    assert_eq!(loaded.after.head, Some(Position::entry_start(1)));
    assert_eq!(loaded.after.retained_bytes, second_bytes);
}

#[test]
fn loader_rejects_missing_corrupt_and_wrong_schema_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>("buffer")
        .unwrap();
    let mut transactions = store.into_transactions();
    let before = BufferState {
        head: Some(Position {
            sequence: 0,
            row_index: 0,
            remaining: 1,
        }),
        tail: 1,
        pending_events: 1,
        retained_bytes: 8,
    };

    let transaction = transactions.begin();
    assert!(
        batch::load(
            &buffer,
            before,
            1,
            MAX_DELIVERY_BYTES,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
            &mut None,
            &schema(),
            transaction.access(),
            |_, _| Ok(1),
        )
        .is_err()
    );
    drop(transaction);

    let transaction = transactions.begin();
    buffer
        .access(transaction.access())
        .unwrap()
        .put(&0, &vec![0, 1, 2])
        .unwrap();
    transaction.commit().unwrap();
    let corrupt = BufferState {
        retained_bytes: 11,
        ..before
    };
    let transaction = transactions.begin();
    assert!(
        batch::load(
            &buffer,
            corrupt,
            1,
            MAX_DELIVERY_BYTES,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
            &mut None,
            &schema(),
            transaction.access(),
            |_, _| Ok(1),
        )
        .is_err()
    );
    drop(transaction);

    let wrong = Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "different",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    let encoded = encode_change(&wrong).unwrap();
    let transaction = transactions.begin();
    buffer
        .access(transaction.access())
        .unwrap()
        .put(&0, &encoded)
        .unwrap();
    transaction.commit().unwrap();
    let wrong_schema = BufferState {
        retained_bytes: encoded_item_bytes(&encoded).unwrap(),
        ..before
    };
    let transaction = transactions.begin();
    assert!(
        batch::load(
            &buffer,
            wrong_schema,
            1,
            MAX_DELIVERY_BYTES,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
            &mut None,
            &schema(),
            transaction.access(),
            |_, _| Ok(1),
        )
        .is_err()
    );
}

#[test]
fn initialization_intent_rollback_has_no_target_effect() {
    let mut fixture = Fixture::create();
    assert_commit(fixture.commit(None).unwrap());
    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert!(fixture.control().is_none());
    let state = fixture.target.lock().unwrap();
    assert!(!state.present);
    assert_eq!(state.initialize_calls, 0);
}

#[test]
fn admission_rolls_back_atomically_and_small_claims_form_multiple_entries() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let first = change(&[1], &[1]);
    let second = change(&[2], &[1]);

    assert!(matches!(
        fixture.rollback(Some(&first)).unwrap(),
        Action::Complete(None)
    ));
    assert!(fixture.entry(0).is_none());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());

    assert_complete(fixture.commit(Some(&first)).unwrap());
    assert_complete(fixture.commit(Some(&second)).unwrap());
    assert!(fixture.entry(0).is_some());
    assert!(fixture.entry(1).is_some());
    let durable = ready(&fixture.control().unwrap());
    assert_eq!(durable.buffer.tail, 2);
    assert_eq!(durable.buffer.pending_events, 2);
}

#[test]
fn load_cache_and_prepared_intent_only_advance_after_commit() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let input = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&input)).unwrap());

    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert_eq!(fixture.target.lock().unwrap().prepare_calls, 0);
    assert!(matches!(
        state::decode_header::<Target>(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert_commit(fixture.commit(None).unwrap());
    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert_eq!(fixture.target.lock().unwrap().prepare_calls, 1);
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
    assert!(matches!(
        state::decode_header::<Target>(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert_commit(fixture.commit(None).unwrap());
    assert_eq!(fixture.target.lock().unwrap().prepare_calls, 1);
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 1);
}

#[test]
fn continuous_claim_is_held_while_threshold_drains_and_then_is_admitted() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let buffered = change(&[1], &[3]);
    let offered = change(&[9], &[1]);
    assert_complete(fixture.commit(Some(&buffered)).unwrap());

    assert_commit(fixture.commit(Some(&offered)).unwrap());
    assert_eq!(fixture.target.lock().unwrap().prepare_calls, 0);
    assert_commit(fixture.commit(Some(&offered)).unwrap());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
    assert_commit(fixture.commit(Some(&offered)).unwrap());
    assert!(fixture.entry(0).is_none());
    assert_complete(fixture.commit(Some(&offered)).unwrap());
    assert!(fixture.entry(0).is_some());
}

#[test]
fn one_large_diff_is_settled_across_bounded_batches() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let input = change(&[1], &[7]);
    assert_complete(fixture.commit(Some(&input)).unwrap());

    for expected_batch in 1..=3 {
        assert_commit(fixture.commit(None).unwrap());
        assert_commit(fixture.commit(None).unwrap());
        assert_commit(fixture.commit(None).unwrap());
        assert_eq!(
            fixture.target.lock().unwrap().delivered.len(),
            expected_batch
        );
        assert_eq!(fixture.entry(0).is_none(), expected_batch == 3);
    }
    assert!(matches!(fixture.commit(None).unwrap(), None));
    let delivered = fixture.target.lock().unwrap();
    assert_eq!(delivered.delivered[&1].events, 3);
    assert_eq!(delivered.delivered[&2].events, 3);
    assert_eq!(delivered.delivered[&3].events, 1);
}

#[test]
fn prepared_delivery_is_rebuilt_after_an_uncertain_result() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    fixture.target.lock().unwrap().fail_after_delivery_once = true;
    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
    assert!(fixture.entry(0).is_some());
    assert!(matches!(
        state::decode_header::<Target>(&fixture.control().unwrap()).unwrap(),
        Header::Prepared { .. }
    ));

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 2);
    assert_commit(fixture.commit(None).unwrap());
    assert!(fixture.entry(0).is_none());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());
}

#[test]
fn committed_prepared_without_its_callback_replays_on_open() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert!(matches!(
        fixture.commit_without_after_commit(None).unwrap(),
        Action::Commit(None)
    ));
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 1);
    assert_commit(fixture.commit(None).unwrap());
    assert!(fixture.entry(0).is_none());
}

#[test]
fn settlement_rollback_keeps_the_prepared_state_and_complete_entries() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());

    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert!(fixture.entry(0).is_some());
    assert!(matches!(
        state::decode_header::<Target>(&fixture.control().unwrap()).unwrap(),
        Header::Prepared { .. }
    ));
    assert_commit(fixture.commit(None).unwrap());
    assert!(fixture.entry(0).is_none());
}

#[test]
fn recovery_rejects_a_forged_settlement_before_decoding_its_plan() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let input_change = change(&[1], &[5]);
    assert_complete(fixture.commit(Some(&input_change)).unwrap());
    let before = ready(&fixture.control().unwrap());
    let delivery = DeliveryBatch::for_test(change(&[1], &[3]), vec![5]).unwrap();
    let forged = State::Prepared(Prepared {
        before: before.buffer,
        after: BufferState {
            head: Some(Position {
                sequence: 0,
                row_index: 0,
                remaining: 1,
            }),
            tail: 1,
            pending_events: 1,
            retained_bytes: before.buffer.retained_bytes,
        },
        batch_id: before.next_batch_id,
        checkpoint: 3,
        plan: Plan {
            batch_id: before.next_batch_id,
            events: 3,
            fingerprint: fingerprint(&delivery),
        },
    })
    .encode::<Target>();
    let transaction = fixture.transactions.begin();
    fixture
        .control
        .access(transaction.access())
        .unwrap()
        .set(&forged)
        .unwrap();
    transaction.commit().unwrap();

    let mut fixture = fixture.reopen();
    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
}

#[test]
fn oversized_event_and_item_are_rejected_without_partial_admission() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let too_many = change(&[1], &[i64::MIN]);
    assert!(fixture.commit(Some(&too_many)).is_err());
    assert!(fixture.entry(0).is_none());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());
    let target = Target::new(Arc::new(Mutex::new(TargetState::default())));
    assert!(prepare_admission(&target, &0, 0, &change(&[1], &[1_048_576])).is_ok());
    assert!(prepare_admission(&target, &0, 0, &change(&[1], &[1_048_577])).is_err());
    assert!(batch::event_count(&change(&[1, 2], &[i64::MIN, i64::MIN])).is_err());

    let binary_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let payload = vec![7_u8; usize::try_from(MAX_DELIVERY_BYTES).unwrap()];
    let oversized = Change::try_new(
        RecordBatch::try_new(
            binary_schema,
            vec![Arc::new(BinaryArray::from(vec![Some(payload.as_slice())]))],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    assert!(prepare_admission(&target, &0, 0, &oversized).is_err());
}

#[test]
fn target_byte_charge_slices_delivery_and_rejects_one_oversized_event_before_ack() {
    let mut fixture = Fixture::create();
    fixture.target.lock().unwrap().event_bytes = MAX_TARGET_BATCH_BYTES / 2 + 1;
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&change(&[7], &[3]))).unwrap());

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap());
    for _ in 0..32 {
        if fixture.commit(None).unwrap().is_none() {
            break;
        }
    }
    let target = fixture.target.lock().unwrap();
    assert_eq!(target.delivered.len(), 3);
    assert!(target.delivered.values().all(|plan| plan.events == 1));
    drop(target);
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());

    let mut oversized = Fixture::create();
    oversized.target.lock().unwrap().event_bytes = MAX_TARGET_BATCH_BYTES + 1;
    oversized.bootstrap();
    assert!(oversized.commit(Some(&change(&[9], &[1]))).is_err());
    assert!(oversized.entry(0).is_none());
    assert!(ready(&oversized.control().unwrap()).buffer.is_empty());
    assert!(oversized.target.lock().unwrap().delivered.is_empty());
}

#[test]
fn restore_checks_positive_capacity_before_any_target_io() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&change(&[1, 2], &[-2, 2]))).unwrap());
    fixture.target.lock().unwrap().recovery_positive_capacity = Some(1);

    let mut fixture = fixture.reopen();
    assert!(fixture.commit(None).is_err());
    assert!(fixture.entry(0).is_some());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
}

#[test]
fn a_wide_large_change_round_trips_through_owned_ipc_and_reopen() {
    let wide_schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("first", DataType::Binary, false),
        Field::new("second", DataType::Binary, false),
        Field::new("third", DataType::Binary, false),
    ]));
    let payload = vec![7_u8; 2 * 1024 * 1024];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![1])),
        Arc::new(BinaryArray::from(vec![Some(payload.as_slice())])),
        Arc::new(BinaryArray::from(vec![Some(payload.as_slice())])),
        Arc::new(BinaryArray::from(vec![Some(payload.as_slice())])),
    ];
    let input = Change::try_new(
        RecordBatch::try_new(Arc::clone(&wide_schema), columns).unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    assert!(encoded_item_bytes(&encode_change(&input).unwrap()).unwrap() < MAX_DELIVERY_BYTES);

    let mut fixture = Fixture::create_with_schema(wide_schema);
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&input)).unwrap());
    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
}

#[test]
fn admission_reserves_enough_batch_ids_for_every_buffered_event() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    fixture.set_control(
        &State::<u64, Plan>::Ready(Ready {
            buffer: BufferState::EMPTY,
            next_batch_id: u64::MAX - 1,
            checkpoint: 0,
        })
        .encode::<Target>(),
    );
    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap());

    assert!(fixture.commit(Some(&change(&[1], &[2]))).is_err());
    assert!(fixture.entry(0).is_none());
    assert_eq!(
        ready(&fixture.control().unwrap()).next_batch_id,
        u64::MAX - 1
    );

    assert_complete(fixture.commit(Some(&change(&[1], &[1]))).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    assert_commit(fixture.commit(None).unwrap());
    let exhausted = ready(&fixture.control().unwrap());
    assert!(exhausted.buffer.is_empty());
    assert_eq!(exhausted.next_batch_id, u64::MAX);
    assert!(fixture.commit(Some(&change(&[2], &[1]))).is_err());
}

#[test]
fn recovery_rejects_buffered_work_that_cannot_drain_before_id_exhaustion() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&change(&[1], &[2]))).unwrap());
    let admitted = ready(&fixture.control().unwrap());
    fixture.set_control(
        &State::<u64, Plan>::Ready(Ready {
            buffer: admitted.buffer,
            next_batch_id: u64::MAX - 1,
            checkpoint: admitted.checkpoint,
        })
        .encode::<Target>(),
    );

    let mut fixture = fixture.reopen();
    assert!(fixture.commit(None).is_err());
    assert!(fixture.entry(0).is_some());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
}

#[test]
fn restore_validates_every_buffer_entry_and_accounting_before_delivery() {
    let mut missing = Fixture::create();
    missing.bootstrap();
    assert_complete(missing.commit(Some(&change(&[1], &[1]))).unwrap());
    assert_complete(missing.commit(Some(&change(&[2], &[1]))).unwrap());
    let transaction = missing.transactions.begin();
    assert!(
        missing
            .buffer
            .access(transaction.access())
            .unwrap()
            .remove(&1)
            .unwrap()
    );
    transaction.commit().unwrap();
    let mut missing = missing.reopen();
    assert!(missing.commit(None).is_err());
    assert!(missing.target.lock().unwrap().delivered.is_empty());

    let mut corrupt = Fixture::create();
    corrupt.bootstrap();
    assert_complete(corrupt.commit(Some(&change(&[1], &[1]))).unwrap());
    assert_complete(corrupt.commit(Some(&change(&[2], &[1]))).unwrap());
    let transaction = corrupt.transactions.begin();
    let malformed = vec![0, 1, 2];
    corrupt
        .buffer
        .access(transaction.access())
        .unwrap()
        .put(&1, &malformed)
        .unwrap();
    transaction.commit().unwrap();
    let mut corrupt = corrupt.reopen();
    assert!(corrupt.commit(None).is_err());
    assert!(corrupt.target.lock().unwrap().delivered.is_empty());

    let mut miscounted = Fixture::create();
    miscounted.bootstrap();
    assert_complete(miscounted.commit(Some(&change(&[1, 2], &[1, 1]))).unwrap());
    let mut durable = ready(&miscounted.control().unwrap());
    durable.buffer.pending_events += 1;
    let encoded = State::<u64, Plan>::Ready(durable).encode::<Target>();
    let transaction = miscounted.transactions.begin();
    miscounted
        .control
        .access(transaction.access())
        .unwrap()
        .set(&encoded)
        .unwrap();
    transaction.commit().unwrap();
    let mut miscounted = miscounted.reopen();
    assert!(miscounted.commit(None).is_err());
    assert!(miscounted.target.lock().unwrap().delivered.is_empty());
}

#[test]
fn restore_validation_crosses_scan_pages_before_external_io() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let count = BUFFER_VALIDATION_ITEMS + 1;
    let valid = (0..count)
        .map(|value| encode_change(&change(&[u64::try_from(value).unwrap()], &[1])).unwrap())
        .collect::<Vec<_>>();
    let malformed = vec![0, 1, 2];
    let retained_bytes = valid[..count - 1]
        .iter()
        .map(|encoded| encoded_item_bytes(encoded).unwrap())
        .sum::<u64>()
        + encoded_item_bytes(&malformed).unwrap();
    let control = State::<u64, Plan>::Ready(Ready {
        buffer: BufferState {
            head: Some(Position {
                sequence: 0,
                row_index: 0,
                remaining: 1,
            }),
            tail: u64::try_from(count).unwrap(),
            pending_events: u64::try_from(count).unwrap(),
            retained_bytes,
        },
        next_batch_id: 1,
        checkpoint: 0,
    })
    .encode::<Target>();
    let transaction = fixture.transactions.begin();
    let access = transaction.access();
    let mut buffer = fixture.buffer.access(access).unwrap();
    for (sequence, encoded) in valid[..count - 1].iter().enumerate() {
        buffer
            .put(&u64::try_from(sequence).unwrap(), encoded)
            .unwrap();
    }
    buffer
        .put(&u64::try_from(count - 1).unwrap(), &malformed)
        .unwrap();
    fixture
        .control
        .access(access)
        .unwrap()
        .set(&control)
        .unwrap();
    transaction.commit().unwrap();

    let mut fixture = fixture.reopen();
    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
}

#[test]
fn restore_rejects_a_nonzero_orphan_without_control_state() {
    let mut fixture = Fixture::create();
    let transaction = fixture.transactions.begin();
    fixture
        .buffer
        .access(transaction.access())
        .unwrap()
        .put(&7, &encode_change(&change(&[7], &[1])).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().initialize_calls, 0);
}
