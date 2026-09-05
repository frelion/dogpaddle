use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use super::{state::State, *};

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
            self.rows
                .entry(insert.technical_id)
                .or_insert(canonical_row(
                    input.records(),
                    usize::try_from(insert.row_index).unwrap(),
                )?);
        }
        for id in &batch.deletes {
            self.rows.remove(id);
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

fn apply(target: &mut Target, input: &Change, next_id: &mut u64) {
    let mut position = first_position(input);
    loop {
        let (next, batch) = plan::prepare(target, input, *next_id, position).unwrap();
        let state = State::Prepared {
            next_id: next,
            batch: batch.clone(),
        };
        assert_eq!(State::decode(&state.encode(), input).unwrap(), state);
        target.write_batch(input, &batch).unwrap();
        let once = target.rows.clone();
        target.write_batch(input, &batch).unwrap();
        assert_eq!(target.rows, once, "fixed batch replay must be idempotent");
        *next_id = next;
        match batch.continuation {
            Continuation::Done => break,
            Continuation::Position(next) => position = next,
        }
    }
}

#[test]
fn insert_delete_same_id_replays_empty_and_never_reuses_an_id() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(&mut target, &change(&[(7, 2), (7, -2)]), &mut next_id);
    assert!(target.rows.is_empty());
    assert_eq!(next_id, 3);
    apply(&mut target, &change(&[(7, 1)]), &mut next_id);
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [3]);
}

#[test]
fn retractions_use_oldest_existing_then_newly_inserted_ids() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(&mut target, &change(&[(7, 3)]), &mut next_id);
    let input = change(&[(7, 2), (7, -4)]);
    let (_, batch) = plan::prepare(&mut target, &input, next_id, first_position(&input)).unwrap();
    assert_eq!(batch.deletes, [1, 2, 3, 4]);
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
    let input = change(&[(7, -1), (7, 1)]);
    assert!(plan::prepare(&mut target, &input, 1, first_position(&input)).is_err());
    assert!(target.rows.is_empty());
}

#[test]
fn large_negative_event_is_admitted_whole_once_then_looks_up_only_its_next_batch() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(&mut target, &change(&[(7, 2050)]), &mut next_id);
    let invalid = change(&[(7, -2051)]);
    assert!(plan::prepare(&mut target, &invalid, next_id, first_position(&invalid)).is_err());
    assert_eq!(target.rows.len(), 2050);
    target.lookups.clear();
    apply(&mut target, &change(&[(7, -2050)]), &mut next_id);
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
fn a_thousand_distinct_updates_are_matched_in_two_bounded_calls() {
    let mut target = Target::default();
    let mut next_id = 1;
    apply(
        &mut target,
        &change(&(0..1000).map(|value| (value, 1)).collect::<Vec<_>>()),
        &mut next_id,
    );
    let updates = (0..1000)
        .flat_map(|value| [(value, -1), (value + 1000, 1)])
        .collect::<Vec<_>>();
    apply(&mut target, &change(&updates), &mut next_id);
    assert_eq!(
        target.lookups.iter().map(Vec::len).collect::<Vec<_>>(),
        [512, 488]
    );
    assert_eq!(
        target.rows.keys().copied().collect::<Vec<_>>(),
        (1001..=2000).collect::<Vec<_>>()
    );
}

#[test]
fn stable_rebatching_preserves_exact_technical_ids_and_rows() {
    let events = [(7, 1025), (8, 2), (7, -1024), (9, 1), (8, -1), (7, 1)];
    let mut whole = Target::default();
    let mut whole_next = 1;
    apply(&mut whole, &change(&events), &mut whole_next);
    for split in 1..events.len() {
        let mut split_target = Target::default();
        let mut split_next = 1;
        apply(
            &mut split_target,
            &change(&events[..split]),
            &mut split_next,
        );
        apply(
            &mut split_target,
            &change(&events[split..]),
            &mut split_next,
        );
        assert_eq!(split_target.rows, whole.rows);
        assert_eq!(split_next, whole_next);
    }
}

#[test]
fn last_id_is_usable_but_an_overflowing_event_is_not_partly_applied() {
    let mut target = Target::default();
    let input = change(&[(7, 2)]);
    assert!(
        plan::prepare(
            &mut target,
            &input,
            MAX_TECHNICAL_ID,
            first_position(&input)
        )
        .is_err()
    );
    let input = change(&[(7, 1)]);
    let (next, batch) = plan::prepare(
        &mut target,
        &input,
        MAX_TECHNICAL_ID,
        first_position(&input),
    )
    .unwrap();
    assert_eq!(next, EXHAUSTED_ID);
    assert_eq!(batch.inserts[0].technical_id, MAX_TECHNICAL_ID);
    assert!(plan::prepare(&mut target, &input, next, first_position(&input)).is_err());
}

#[test]
fn state_codec_has_exact_phase_goldens_and_rejects_invalid_bytes() {
    let input = change(&[(7, 1), (7, -1)]);

    let initialize_bytes = [1, 0];
    assert_eq!(State::Initialize.encode(), initialize_bytes);
    assert_eq!(
        State::decode(&initialize_bytes, &input).unwrap(),
        State::Initialize
    );

    let ready_input = change(&[(7, 1025)]);
    let ready = State::Ready {
        next_id: 1025,
        position: Some(Position {
            row_index: 0,
            remaining: 1,
        }),
    };
    let ready_bytes = [
        1, 1, 0, 0, 0, 0, 0, 0, 4, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ];
    assert_eq!(ready.encode(), ready_bytes);
    assert_eq!(State::decode(&ready_bytes, &ready_input).unwrap(), ready);

    let (_, batch) =
        plan::prepare(&mut Target::default(), &input, 1, first_position(&input)).unwrap();
    let state = State::Prepared { next_id: 2, batch };
    let bytes = state.encode();
    assert_eq!(
        bytes,
        [
            1, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 1, 0, 0, 0, 0, 0, 0, 0, 1
        ]
    );
    assert_eq!(State::decode(&bytes, &input).unwrap(), state);
    for end in 0..bytes.len() {
        assert!(State::decode(&bytes[..end], &input).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(State::decode(&trailing, &input).is_err());
    assert!(State::decode(&bytes, &change(&[(7, -1), (7, 1)])).is_err());
}

#[test]
fn recovery_rejects_impossible_deletions_and_skipped_batch_prefixes() {
    let invalid = State::Prepared {
        next_id: 2,
        batch: Batch {
            inserts: vec![Insert {
                row_index: 1,
                technical_id: 1,
            }],
            deletes: vec![1],
            continuation: Continuation::Done,
        },
    };
    assert!(State::decode(&invalid.encode(), &change(&[(7, -1), (7, 1)])).is_err());
    let different = State::Prepared {
        next_id: 2,
        batch: Batch {
            inserts: vec![Insert {
                row_index: 0,
                technical_id: 1,
            }],
            deletes: vec![1],
            continuation: Continuation::Done,
        },
    };
    assert!(State::decode(&different.encode(), &change(&[(7, 1), (8, -1)])).is_err());
    let skipped = State::Prepared {
        next_id: 2,
        batch: Batch {
            inserts: vec![Insert {
                row_index: 1,
                technical_id: 1,
            }],
            deletes: vec![],
            continuation: Continuation::Done,
        },
    };
    assert!(State::decode(&skipped.encode(), &change(&[(7, 1), (8, 1)])).is_err());
    let ready = State::Ready {
        next_id: 2,
        position: Some(Position {
            row_index: 0,
            remaining: 1024,
        }),
    };
    assert!(State::decode(&ready.encode(), &change(&[(7, 1025)])).is_err());
}

#[test]
fn state_decoder_never_panics_on_single_byte_mutations() {
    let input = change(&[(7, 2), (7, -1), (8, 1), (8, -1)]);
    let (next_id, batch) =
        plan::prepare(&mut Target::default(), &input, 1, first_position(&input)).unwrap();
    let original = State::Prepared { next_id, batch }.encode();
    for index in 0..original.len() {
        for value in 0..=u8::MAX {
            let mut bytes = original.clone();
            bytes[index] = value;
            let _ = State::decode(&bytes, &input);
        }
    }
}
