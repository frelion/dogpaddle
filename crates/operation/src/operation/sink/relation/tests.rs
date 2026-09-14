use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, BinaryArray, Int64Array, ListArray, NullArray, RecordBatch};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::encode_change;

use super::*;
use crate::operation::sink::buffered::{DeliveryBatch, SinkTarget};

#[derive(Default)]
struct Target {
    rows: BTreeMap<u64, Vec<u8>>,
    lookups: Vec<Vec<Lookup>>,
}

impl RelationTarget for Target {
    fn require_absent(&mut self) -> Result<(), OperationError> {
        Ok(())
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        Ok(())
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        self.lookups.push(requests.to_vec());
        requests
            .iter()
            .map(|request| {
                let key = canonical_row(input.records(), request.row_index)?;
                let ids = self
                    .rows
                    .iter()
                    .filter(|(_, value)| **value == key)
                    .map(|(id, _)| *id)
                    .take(usize::try_from(request.needed).unwrap())
                    .collect::<Vec<_>>();
                Ok(Matches {
                    count: u64::try_from(ids.len()).unwrap(),
                    ids: ids.into_iter().take(request.take).collect(),
                })
            })
            .collect()
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        for insert in &batch.inserts {
            let expected =
                canonical_row(input.records(), usize::try_from(insert.row_index).unwrap())?;
            match self.rows.entry(insert.technical_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(expected);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &expected => {
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(invalid("insert ID belongs to a different row"));
                }
            }
        }
        for delete in &batch.deletes {
            let expected =
                canonical_row(input.records(), usize::try_from(delete.row_index).unwrap())?;
            if self
                .rows
                .get(&delete.technical_id)
                .is_some_and(|actual| actual != &expected)
            {
                return Err(invalid("delete ID belongs to a different row"));
            }
            self.rows.remove(&delete.technical_id);
        }
        Ok(())
    }
}

fn change(rows: &[(i64, i64)]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    Change::try_new(
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.0),
            ))],
        )
        .unwrap(),
        Int64Array::from_iter_values(rows.iter().map(|row| row.1)),
    )
    .unwrap()
}

fn delivery(rows: &[(i64, i64)], admissions: &[u64]) -> DeliveryBatch {
    DeliveryBatch::for_test(change(rows), admissions.to_vec()).unwrap()
}

fn apply(target: &mut Target, input: &DeliveryBatch, next_id: &mut u64) {
    let (next, plan) = plan::prepare(target, input, *next_id).unwrap();
    plan::validate(&plan, next, input.change()).unwrap();
    target.write_batch(input.change(), &plan).unwrap();
    let once = target.rows.clone();
    target.write_batch(input.change(), &plan).unwrap();
    assert_eq!(target.rows, once, "fixed plan replay must be idempotent");
    *next_id = next;
}

#[test]
fn insert_delete_same_id_replays_empty_and_never_reuses_an_id() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(
        &mut target,
        &delivery(&[(7, 2), (7, -2)], &[2, 2]),
        &mut next_id,
    );
    assert!(target.rows.is_empty());
    assert_eq!(next_id, 3);
    apply(&mut target, &delivery(&[(7, 1)], &[1]), &mut next_id);
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [3]);
}

#[test]
fn retractions_use_oldest_existing_then_newly_inserted_ids() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(&mut target, &delivery(&[(7, 3)], &[3]), &mut next_id);
    let input = delivery(&[(7, 2), (7, -4)], &[2, 4]);
    let (_, batch) = plan::prepare(&mut target, &input, next_id).unwrap();
    assert_eq!(
        batch
            .deletes
            .iter()
            .map(|delete| delete.technical_id)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| insert.technical_id)
            .collect::<Vec<_>>(),
        [4, 5]
    );
}

#[test]
fn later_inserts_cannot_cover_an_invalid_negative_prefix() {
    let mut target = Target::default();
    let input = delivery(&[(7, -1), (7, 1)], &[1, 1]);
    assert!(plan::prepare(&mut target, &input, 1).is_err());
    assert!(target.rows.is_empty());
}

#[test]
fn first_slice_admits_a_large_event_before_any_partial_delivery() {
    let mut target = Target::default();
    let mut next_id = 1;
    for (diff, admission) in [(1024, 2050), (1024, 1024), (2, 2)] {
        apply(
            &mut target,
            &delivery(&[(7, diff)], &[admission]),
            &mut next_id,
        );
    }
    assert_eq!(target.rows.len(), 2050);

    let invalid = delivery(&[(7, -1024)], &[2051]);
    assert!(plan::prepare(&mut target, &invalid, next_id).is_err());
    assert_eq!(target.rows.len(), 2050);

    target.lookups.clear();
    for (diff, admission) in [(-1024, 2050), (-1024, 1024), (-2, 2)] {
        apply(
            &mut target,
            &delivery(&[(7, diff)], &[admission]),
            &mut next_id,
        );
    }
    assert!(target.rows.is_empty());
    assert_eq!(
        target
            .lookups
            .iter()
            .map(|requests| (requests[0].needed, requests[0].take))
            .collect::<Vec<_>>(),
        [(2050, 1024), (1024, 1024), (2, 2)]
    );
}

#[test]
fn last_technical_id_is_usable_but_an_event_cannot_partly_overflow() {
    let mut target = Target::default();
    assert!(plan::prepare(&mut target, &delivery(&[(7, 1)], &[2]), MAX_TECHNICAL_ID,).is_err());
    let (next, batch) =
        plan::prepare(&mut target, &delivery(&[(7, 1)], &[1]), MAX_TECHNICAL_ID).unwrap();
    assert_eq!(next, EXHAUSTED_ID);
    assert_eq!(batch.inserts[0].technical_id, MAX_TECHNICAL_ID);
    assert!(plan::prepare(&mut target, &delivery(&[(7, 1)], &[1]), next).is_err());
}

#[test]
fn relation_checkpoint_and_plan_codecs_are_stable_and_validate_the_batch() {
    type Adapter = RelationSinkTarget<Target>;

    let input = delivery(&[(7, 1), (7, -1)], &[1, 1]);
    let mut adapter = Adapter::new(Target::default());
    let (checkpoint, plan) = adapter.prepare(&input, &1, 9).unwrap();
    let mut encoded = Vec::new();
    Adapter::encode_checkpoint(&checkpoint, &mut encoded);
    Adapter::encode_plan(&plan, &mut encoded);
    assert_eq!(
        encoded,
        [
            0, 0, 0, 0, 0, 0, 0, 2, // checkpoint
            1, 0, 1, 0, 1, // plan header
            0, 0, 0, 0, 0, 0, 0, 0, // insert row
            0, 0, 0, 0, 0, 0, 0, 1, // insert ID
            0, 0, 0, 0, 0, 0, 0, 1, // delete row
            0, 0, 0, 0, 0, 0, 0, 1, // delete ID
        ]
    );

    let mut cursor = encoded.as_slice();
    let decoded_checkpoint = Adapter::decode_checkpoint(&mut cursor).unwrap();
    let decoded = Adapter::decode_plan(&mut cursor, &input, &decoded_checkpoint).unwrap();
    assert!(cursor.is_empty());
    assert_eq!(decoded, plan);

    for end in 0..encoded.len() {
        let mut cursor = &encoded[..end];
        let result = Adapter::decode_checkpoint(&mut cursor)
            .and_then(|checkpoint| Adapter::decode_plan(&mut cursor, &input, &checkpoint));
        assert!(
            result.is_err(),
            "accepted truncated relation state at {end}"
        );
    }
    let different = delivery(&[(7, 1), (8, -1)], &[1, 1]);
    let mut cursor = encoded[8..].as_ref();
    assert!(Adapter::decode_plan(&mut cursor, &different, &checkpoint).is_err());
}

#[test]
fn relation_recovery_rejects_positive_work_beyond_the_id_frontier() {
    let adapter = RelationSinkTarget::new(Target::default());
    assert!(adapter.validate_recovery(&EXHAUSTED_ID, 0).is_ok());
    assert!(adapter.validate_recovery(&EXHAUSTED_ID, 1).is_err());
    assert!(adapter.validate_recovery(&(EXHAUSTED_ID - 2), 2).is_ok());
    assert!(adapter.validate_recovery(&(EXHAUSTED_ID - 2), 3).is_err());
}

#[test]
fn recovered_plan_compares_each_large_row_pair_once() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        false,
    )]));
    let payload = vec![7_u8; 3 * 1024 * 1024];
    let input = Change::try_new(
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from(vec![
                Some(payload.as_slice()),
                Some(payload.as_slice()),
            ]))],
        )
        .unwrap(),
        Int64Array::from(vec![512, -512]),
    )
    .unwrap();
    let batch = Batch {
        inserts: (1..=512)
            .map(|technical_id| Insert {
                row_index: 0,
                technical_id,
            })
            .collect(),
        deletes: (1..=512)
            .map(|technical_id| Delete {
                row_index: 1,
                technical_id,
            })
            .collect(),
    };

    plan::validate(&batch, 513, &input).unwrap();
}

#[test]
fn zero_width_nested_values_cannot_expand_past_the_planning_budget() {
    let child = Arc::new(Field::new("item", DataType::Null, true));
    let length = plan::MAX_CANONICAL_BATCH_BYTES + 1;
    let lists = ListArray::new(
        Arc::clone(&child),
        OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            i32::try_from(length).unwrap(),
        ])),
        Arc::new(NullArray::new(length)) as ArrayRef,
        None,
    );
    let input = Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(child),
                false,
            )])),
            vec![Arc::new(lists)],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap();
    assert!(encode_change(&input).unwrap().len() < 1024);

    let mut target = Target::default();
    assert!(
        plan::prepare(
            &mut target,
            &DeliveryBatch::for_test(input, vec![1]).unwrap(),
            1,
        )
        .is_err()
    );
    assert!(target.lookups.is_empty());
}
