use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_store::{Cell, OrderedMap, Store, Transactions};

use super::*;
use crate::operation::relation::canonical_row;
use crate::operation::sink::buffered::{
    batch::encoded_item_bytes,
    state::{Header, Position},
};
use crate::operation::sink::relation::{self, Batch, Insert, Lookup, Matches, RelationTarget};

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // Independent one-shot faults keep each recovery test explicit.
struct TargetState {
    present: bool,
    initialize_calls: usize,
    lookup_calls: usize,
    delivery_attempts: usize,
    delivered: Vec<Batch>,
    rows: BTreeMap<u64, Vec<u8>>,
    fail_lookup_once: bool,
    fail_before_delivery_once: bool,
    fail_after_delivery_once: bool,
    event_bytes: u64,
}

struct Target {
    state: Arc<Mutex<TargetState>>,
}

impl Target {
    fn new(state: Arc<Mutex<TargetState>>) -> Self {
        Self { state }
    }
}

impl RelationTarget for Target {
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

    fn event_bytes(&self, _input: &Change, _row_index: usize) -> Result<u64, OperationError> {
        Ok(self.state.lock().unwrap().event_bytes.max(1))
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        let mut state = self.state.lock().unwrap();
        state.lookup_calls += 1;
        if std::mem::take(&mut state.fail_lookup_once) {
            return Err(invalid("fake lookup failed"));
        }
        requests
            .iter()
            .map(|request| {
                let row = canonical_row(input.records(), request.row_index)?;
                let matching = state
                    .rows
                    .iter()
                    .filter(|(_, stored)| **stored == row)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                Ok(Matches {
                    count: u64::try_from(matching.len()).unwrap().min(request.needed),
                    ids: matching.into_iter().take(request.take).collect(),
                })
            })
            .collect()
    }

    fn write_batch(&mut self, input: &Change, plan: &Batch) -> Result<(), OperationError> {
        let mut state = self.state.lock().unwrap();
        state.delivery_attempts += 1;
        if std::mem::take(&mut state.fail_before_delivery_once) {
            return Err(invalid("fake delivery failed before commit"));
        }
        let mut rows = state.rows.clone();
        for insert in &plan.inserts {
            let row = canonical_row(input.records(), usize::try_from(insert.row_index).unwrap())?;
            match rows.entry(insert.technical_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(row);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &row => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(invalid("fake mutation ID belongs to a different row"));
                }
            }
        }
        for delete in &plan.deletes {
            let row = canonical_row(input.records(), usize::try_from(delete.row_index).unwrap())?;
            if rows
                .get(&delete.technical_id)
                .is_some_and(|stored| stored != &row)
            {
                return Err(invalid("fake deletion ID belongs to a different row"));
            }
            rows.remove(&delete.technical_id);
        }
        state.rows = rows;
        if !state.delivered.contains(plan) {
            state.delivered.push(plan.clone());
        }
        if std::mem::take(&mut state.fail_after_delivery_once) {
            return Err(invalid("fake delivery result was uncertain"));
        }
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn codec() -> SchemaBoundChangeCodec {
    SchemaBoundChangeCodec::try_new(schema()).unwrap()
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
    root: tempfile::TempDir,
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
        let mut operation = BufferedSink::new(
            SchemaBoundChangeCodec::try_new(Arc::clone(&schema)).unwrap(),
            Target::new(Arc::clone(&target)),
            control.clone(),
            buffer.clone(),
        );
        operation.max_batch_events = 3;
        Self {
            root,
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
            root,
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
        let mut operation = BufferedSink::new(
            SchemaBoundChangeCodec::try_new(Arc::clone(&schema)).unwrap(),
            Target::new(Arc::clone(&target)),
            control.clone(),
            buffer.clone(),
        );
        operation.max_batch_events = 3;
        Self {
            root,
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
        assert_commit(self.commit(None).unwrap().as_ref());
        assert_commit(self.commit(None).unwrap().as_ref());
        assert_commit(self.commit(None).unwrap().as_ref());
        assert!(self.commit(None).unwrap().is_none());
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

fn assert_commit(action: Option<&Action>) {
    assert!(matches!(action, Some(Action::Commit(None))));
}

fn assert_complete(action: Option<&Action>) {
    assert!(matches!(action, Some(Action::Complete(None))));
}

fn ready(bytes: &[u8]) -> Ready {
    match state::decode_header(bytes).unwrap() {
        Header::Ready(ready) => ready,
        Header::Initialize | Header::Prepared { .. } => panic!("expected Ready control state"),
    }
}

#[test]
fn control_codec_has_phase_goldens_and_rejects_corruption() {
    let delivery = DeliveryBatch::for_test(change(&[7], &[2]), vec![2]).unwrap();
    let initialize = State::Initialize;
    assert_eq!(initialize.encode(), [1, 0]);
    assert!(matches!(
        State::decode(&[1, 0], None).unwrap(),
        State::Initialize
    ));

    let ready_state = State::Ready(Ready {
        buffer: BufferState::EMPTY,
        checkpoint: 9,
    });
    let ready_bytes = ready_state.encode();
    assert_eq!(
        ready_bytes,
        [
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 9,
        ]
    );
    let State::Ready(decoded_ready) = State::decode(&ready_bytes, None).unwrap() else {
        panic!("expected Ready");
    };
    assert_eq!(decoded_ready.buffer, BufferState::EMPTY);
    assert_eq!(decoded_ready.checkpoint, 9);

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
        checkpoint: 3,
        plan: Batch {
            inserts: vec![
                Insert {
                    row_index: 0,
                    technical_id: 1,
                },
                Insert {
                    row_index: 0,
                    technical_id: 2,
                },
            ],
            deletes: Vec::new(),
        },
    });
    let prepared_bytes = prepared.encode();
    let mut prepared_golden = vec![1, 2, 1];
    for value in [3_u64, 0, 2, 4, 2, 100] {
        prepared_golden.extend(value.to_be_bytes());
    }
    prepared_golden.push(0);
    prepared_golden.extend([0; size_of::<u64>() * 3]);
    prepared_golden.extend(3_u64.to_be_bytes());
    prepared_golden.extend([1, 0, 2, 0, 0]);
    for id in [1_u64, 2] {
        prepared_golden.extend(0_u64.to_be_bytes());
        prepared_golden.extend(id.to_be_bytes());
    }
    assert_eq!(prepared_bytes, prepared_golden);
    let State::Prepared(decoded) = State::decode(&prepared_bytes, Some(&delivery)).unwrap() else {
        panic!("expected Prepared");
    };
    assert_eq!(decoded.before.head.unwrap().sequence, 3);
    assert_eq!(decoded.after, BufferState::EMPTY);
    assert_eq!(decoded.plan.inserts.len(), 2);
    assert_eq!(decoded.plan.inserts[0].technical_id, 1);
    for end in 0..prepared_bytes.len() {
        assert!(
            State::decode(&prepared_bytes[..end], Some(&delivery)).is_err(),
            "accepted truncated control state at {end}"
        );
    }
    let mut trailing = prepared_bytes.clone();
    trailing.push(0);
    assert!(State::decode(&trailing, Some(&delivery)).is_err());
    let mut corrupt_plan = prepared_bytes;
    *corrupt_plan.last_mut().unwrap() ^= 1;
    assert!(State::decode(&corrupt_plan, Some(&delivery)).is_err());
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
    let codec = codec();
    let encoded = [
        codec.encode(&first).unwrap(),
        codec.encode(&second).unwrap(),
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
        batch::LoadLimits::new(
            6,
            MAX_DELIVERY_BYTES,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
        ),
        &mut None,
        &codec,
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
        batch::LoadLimits::new(
            4,
            MAX_DELIVERY_BYTES,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
        ),
        &mut None,
        &codec,
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
    let codec = codec();
    let first_encoded = codec.encode(&first).unwrap();
    let second_encoded = codec.encode(&second).unwrap();
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
        batch::LoadLimits::new(
            2,
            first_bytes + second_bytes - 1,
            MAX_DELIVERY_BYTES,
            MAX_TARGET_BATCH_BYTES,
        ),
        &mut None,
        &codec,
        transaction.access(),
        |_, _| Ok(1),
    )
    .unwrap();
    transaction.commit().unwrap();
    assert_eq!(loaded.delivery.change().diffs().values(), &[1]);
    assert_eq!(loaded.after.head, Some(Position::entry_start(1)));
    assert_eq!(loaded.after.retained_bytes, second_bytes);
}

fn assert_loader_error(
    buffer: &OrderedMap<u64, Vec<u8>>,
    transactions: &mut Transactions,
    before: BufferState,
    codec: &SchemaBoundChangeCodec,
) {
    let transaction = transactions.begin();
    assert!(
        batch::load(
            buffer,
            before,
            batch::LoadLimits::new(
                1,
                MAX_DELIVERY_BYTES,
                MAX_DELIVERY_BYTES,
                MAX_TARGET_BATCH_BYTES,
            ),
            &mut None,
            codec,
            transaction.access(),
            |_, _| Ok(1),
        )
        .is_err()
    );
}

#[test]
fn loader_rejects_missing_corrupt_and_wrong_schema_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let buffer = store
        .create_data::<OrderedMap<u64, Vec<u8>>>("buffer")
        .unwrap();
    let mut transactions = store.into_transactions();
    let codec = codec();
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

    assert_loader_error(&buffer, &mut transactions, before, &codec);

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
    assert_loader_error(&buffer, &mut transactions, corrupt, &codec);

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
    let wrong_codec = SchemaBoundChangeCodec::try_new(wrong.records().schema()).unwrap();
    let encoded = wrong_codec.encode(&wrong).unwrap();
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
    assert_loader_error(&buffer, &mut transactions, wrong_schema, &codec);
}

#[test]
fn initialization_intent_rollback_has_no_target_effect() {
    let mut fixture = Fixture::create();
    assert_commit(fixture.commit(None).unwrap().as_ref());
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

    assert_complete(fixture.commit(Some(&first)).unwrap().as_ref());
    assert_complete(fixture.commit(Some(&second)).unwrap().as_ref());
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
    assert_complete(fixture.commit(Some(&input)).unwrap().as_ref());

    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(&fixture.operation.phase, Phase::Ready(_)));
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert!(matches!(&fixture.operation.phase, Phase::Loaded(_)));
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(matches!(&fixture.operation.phase, Phase::Delivered(_)));
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 1);
}

#[test]
fn failed_lookup_and_rolled_back_prepared_write_replan_the_loaded_batch() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&change(&[1], &[1]))).unwrap().as_ref());
    for _ in 0..3 {
        assert_commit(fixture.commit(None).unwrap().as_ref());
    }
    assert_complete(fixture.commit(Some(&change(&[1], &[-1]))).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());

    fixture.target.lock().unwrap().fail_lookup_once = true;
    assert!(fixture.commit(None).is_err());
    assert!(matches!(&fixture.operation.phase, Phase::Loaded(_)));
    assert_eq!(fixture.target.lock().unwrap().lookup_calls, 1);
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert_eq!(fixture.target.lock().unwrap().lookup_calls, 2);
    assert!(matches!(&fixture.operation.phase, Phase::Loaded(_)));
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Ready(_)
    ));

    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_eq!(fixture.target.lock().unwrap().lookup_calls, 3);
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 2);
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(fixture.target.lock().unwrap().rows.is_empty());
}

#[test]
fn continuous_claim_is_held_while_threshold_drains_and_then_is_admitted() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let buffered = change(&[1], &[3]);
    let offered = change(&[9], &[1]);
    assert_complete(fixture.commit(Some(&buffered)).unwrap().as_ref());

    assert_commit(fixture.commit(Some(&offered)).unwrap().as_ref());
    assert!(matches!(&fixture.operation.phase, Phase::Loaded(_)));
    assert_commit(fixture.commit(Some(&offered)).unwrap().as_ref());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
    assert_commit(fixture.commit(Some(&offered)).unwrap().as_ref());
    assert!(fixture.entry(0).is_none());
    assert_complete(fixture.commit(Some(&offered)).unwrap().as_ref());
    assert!(fixture.entry(0).is_some());
}

#[test]
fn one_large_diff_is_settled_across_bounded_batches() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let input = change(&[1], &[7]);
    assert_complete(fixture.commit(Some(&input)).unwrap().as_ref());

    for expected_batch in 1..=3 {
        assert_commit(fixture.commit(None).unwrap().as_ref());
        assert_commit(fixture.commit(None).unwrap().as_ref());
        assert_commit(fixture.commit(None).unwrap().as_ref());
        assert_eq!(
            fixture.target.lock().unwrap().delivered.len(),
            expected_batch
        );
        assert_eq!(fixture.entry(0).is_none(), expected_batch == 3);
    }
    assert!(fixture.commit(None).unwrap().is_none());
    let delivered = fixture.target.lock().unwrap();
    assert_eq!(delivered.delivered[0].inserts.len(), 3);
    assert_eq!(delivered.delivered[1].inserts.len(), 3);
    assert_eq!(delivered.delivered[2].inserts.len(), 1);
    assert_eq!(delivered.delivered[0].inserts[0].technical_id, 1);
    assert_eq!(delivered.delivered[1].inserts[0].technical_id, 4);
    assert_eq!(delivered.delivered[2].inserts[0].technical_id, 7);
}

#[test]
fn prepared_delivery_is_rebuilt_after_an_uncertain_result() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    fixture.target.lock().unwrap().fail_after_delivery_once = true;
    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
    assert!(fixture.entry(0).is_some());
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Prepared { .. }
    ));

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 2);
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(fixture.entry(0).is_none());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());
}

#[test]
fn committed_prepared_without_its_callback_replays_on_open() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(matches!(
        fixture.commit_without_after_commit(None).unwrap(),
        Action::Commit(None)
    ));
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 1);
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(fixture.entry(0).is_none());
}

#[test]
fn settlement_rollback_keeps_the_prepared_state_and_complete_entries() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let change = change(&[1], &[3]);
    assert_complete(fixture.commit(Some(&change)).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());

    assert!(matches!(
        fixture.rollback(None).unwrap(),
        Action::Commit(None)
    ));
    assert!(fixture.entry(0).is_some());
    assert!(matches!(
        state::decode_header(&fixture.control().unwrap()).unwrap(),
        Header::Prepared { .. }
    ));
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(fixture.entry(0).is_none());
}

#[test]
fn recovery_rejects_a_forged_settlement_before_decoding_its_plan() {
    let mut fixture = Fixture::create();
    fixture.bootstrap();
    let input_change = change(&[1], &[5]);
    assert_complete(fixture.commit(Some(&input_change)).unwrap().as_ref());
    let before = ready(&fixture.control().unwrap());
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
        checkpoint: 4,
        plan: Batch {
            inserts: (1..=3)
                .map(|technical_id| Insert {
                    row_index: 0,
                    technical_id,
                })
                .collect(),
            deletes: Vec::new(),
        },
    })
    .encode();
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
    let codec = codec();
    assert!(prepare_admission(&codec, &target, 1, 0, &change(&[1], &[1_048_576])).is_ok());
    assert!(prepare_admission(&codec, &target, 1, 0, &change(&[1], &[1_048_577])).is_err());
    assert!(batch::event_count(&change(&[1, 2], &[i64::MIN, i64::MIN])).is_err());

    let binary_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let binary_codec = SchemaBoundChangeCodec::try_new(Arc::clone(&binary_schema)).unwrap();
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
    assert!(prepare_admission(&binary_codec, &target, 1, 0, &oversized).is_err());
}

#[test]
fn target_byte_charge_slices_delivery_and_rejects_one_oversized_event_before_ack() {
    let mut fixture = Fixture::create();
    fixture.target.lock().unwrap().event_bytes = MAX_TARGET_BATCH_BYTES / 2 + 1;
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&change(&[7], &[3]))).unwrap().as_ref());

    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap().as_ref());
    for _ in 0..32 {
        if fixture.commit(None).unwrap().is_none() {
            break;
        }
    }
    let target = fixture.target.lock().unwrap();
    assert_eq!(target.delivered.len(), 3);
    assert!(target.delivered.iter().all(|plan| plan.inserts.len() == 1));
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
    assert_complete(
        fixture
            .commit(Some(&change(&[1, 2], &[-2, 2])))
            .unwrap()
            .as_ref(),
    );
    let mut durable = ready(&fixture.control().unwrap());
    durable.checkpoint = relation::MAX_TECHNICAL_ID + 1;
    let transaction = fixture.transactions.begin();
    fixture
        .control
        .access(transaction.access())
        .unwrap()
        .set(&State::Ready(durable).encode())
        .unwrap();
    transaction.commit().unwrap();

    let mut fixture = fixture.reopen();
    assert!(fixture.commit(None).is_err());
    assert!(fixture.entry(0).is_some());
    assert_eq!(fixture.target.lock().unwrap().delivery_attempts, 0);
}

#[test]
fn a_wide_large_change_round_trips_through_owned_bound_entry_and_reopen() {
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
    let codec = SchemaBoundChangeCodec::try_new(Arc::clone(&wide_schema)).unwrap();
    assert!(encoded_item_bytes(&codec.encode(&input).unwrap()).unwrap() < MAX_DELIVERY_BYTES);

    let mut fixture = Fixture::create_with_schema(wide_schema);
    fixture.bootstrap();
    assert_complete(fixture.commit(Some(&input)).unwrap().as_ref());
    let mut fixture = fixture.reopen();
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert_commit(fixture.commit(None).unwrap().as_ref());
    assert!(ready(&fixture.control().unwrap()).buffer.is_empty());
    assert_eq!(fixture.target.lock().unwrap().delivered.len(), 1);
}

#[test]
fn restore_validates_every_buffer_entry_and_accounting_before_delivery() {
    let mut missing = Fixture::create();
    missing.bootstrap();
    assert_complete(missing.commit(Some(&change(&[1], &[1]))).unwrap().as_ref());
    assert_complete(missing.commit(Some(&change(&[2], &[1]))).unwrap().as_ref());
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
    assert_complete(corrupt.commit(Some(&change(&[1], &[1]))).unwrap().as_ref());
    assert_complete(corrupt.commit(Some(&change(&[2], &[1]))).unwrap().as_ref());
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
    assert_complete(
        miscounted
            .commit(Some(&change(&[1, 2], &[1, 1])))
            .unwrap()
            .as_ref(),
    );
    let mut durable = ready(&miscounted.control().unwrap());
    durable.buffer.pending_events += 1;
    let encoded = State::Ready(durable).encode();
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
    let codec = codec();
    let valid = (0..count)
        .map(|value| {
            codec
                .encode(&change(&[u64::try_from(value).unwrap()], &[1]))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let malformed = vec![0, 1, 2];
    let retained_bytes = valid[..count - 1]
        .iter()
        .map(|encoded| encoded_item_bytes(encoded).unwrap())
        .sum::<u64>()
        + encoded_item_bytes(&malformed).unwrap();
    let control = State::Ready(Ready {
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
        checkpoint: 1,
    })
    .encode();
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
    let codec = codec();
    let transaction = fixture.transactions.begin();
    fixture
        .buffer
        .access(transaction.access())
        .unwrap()
        .put(&7, &codec.encode(&change(&[7], &[1])).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    assert!(fixture.commit(None).is_err());
    assert_eq!(fixture.target.lock().unwrap().initialize_calls, 0);
}
