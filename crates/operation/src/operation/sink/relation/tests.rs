use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, Float32Array, Float64Array, Int64Array, ListArray, NullArray,
    RecordBatch, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::SchemaBoundChangeCodec;

use super::*;
use crate::operation::sink::buffered::DeliveryBatch;

#[derive(Default)]
struct Target {
    rows: BTreeMap<u64, Vec<u8>>,
    lookups: Vec<Vec<Lookup>>,
    lookup_reply: Option<Vec<Matches>>,
}

impl Target {
    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
        through: u64,
    ) -> Result<Vec<Matches>, OperationError> {
        self.lookups.push(requests.to_vec());
        if let Some(reply) = self.lookup_reply.take() {
            return Ok(reply);
        }
        requests
            .iter()
            .map(|request| {
                let key = canonical_row_bounded(input.records(), request.row_index, usize::MAX)?;
                let ids = self
                    .rows
                    .iter()
                    .filter(|(_, value)| **value == key)
                    .map(|(id, _)| *id)
                    .take(request.take)
                    .collect();
                Ok(Matches { through, ids })
            })
            .collect()
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        for insert in &batch.inserts {
            let expected = canonical_row_bounded(
                input.records(),
                usize::try_from(insert.row_index).unwrap(),
                usize::MAX,
            )?;
            match self.rows.entry(insert.technical_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(expected);
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(invalid("new insert ID already exists"));
                }
            }
        }
        for delete in &batch.deletes {
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

fn delivery(rows: &[(i64, i64)], first_event_offset: u64) -> DeliveryBatch {
    DeliveryBatch::for_test(change(rows), first_event_offset).unwrap()
}

fn plan(
    target: &mut Target,
    input: &DeliveryBatch,
    original_head: Option<(u64, &Change)>,
) -> Result<Batch, OperationError> {
    plan_from(target, input, input.first_event_offset(), original_head)
}

fn plan_from(
    target: &mut Target,
    input: &DeliveryBatch,
    from: u64,
    original_head: Option<(u64, &Change)>,
) -> Result<Batch, OperationError> {
    super::plan(
        input,
        from,
        input.end_event_offset()?.max(from),
        original_head.unwrap_or((input.first_event_offset(), input.change())),
        |requests| target.lookup(input.change(), requests, from - 1),
    )
}

fn delete_ids(batch: &Batch) -> Vec<u64> {
    batch
        .deletes
        .iter()
        .map(|delete| delete.technical_id)
        .collect()
}

fn apply(target: &mut Target, input: &DeliveryBatch) -> Batch {
    let batch = plan(target, input, None).unwrap();
    target.write_batch(input.change(), &batch).unwrap();
    let once = target.rows.clone();
    let repeated = plan_from(target, input, input.end_event_offset().unwrap(), None).unwrap();
    assert!(repeated.inserts.is_empty() && repeated.deletes.is_empty());
    target.write_batch(input.change(), &repeated).unwrap();
    assert_eq!(
        target.rows, once,
        "covered prefix must produce no mutations"
    );
    batch
}

#[test]
fn negative_events_leave_gaps_between_stable_positive_ids() {
    let mut target = Target::default();
    apply(&mut target, &delivery(&[(7, 2), (7, -2)], 1));
    assert!(target.rows.is_empty());
    apply(&mut target, &delivery(&[(7, 1)], 5));
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [5]);
}

#[test]
fn insert_only_ids_follow_event_order_without_lookup() {
    let input = delivery(&[(7, 2), (8, 1), (7, 3)], 100);
    let mut target = Target::default();
    let batch = plan(&mut target, &input, None).unwrap();
    assert!(target.lookups.is_empty() && batch.deletes.is_empty());
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| (insert.row_index, insert.technical_id))
            .collect::<Vec<_>>(),
        [(0, 100), (0, 101), (1, 102), (2, 103), (2, 104), (2, 105)]
    );
    let clipped = plan_from(&mut target, &input, 104, None).unwrap();
    assert_eq!(
        clipped
            .inserts
            .iter()
            .map(|insert| insert.technical_id)
            .collect::<Vec<_>>(),
        [104, 105]
    );
    assert!(target.lookups.is_empty());
}

#[test]
fn retractions_use_oldest_existing_then_current_event_ids() {
    let mut target = Target::default();
    apply(&mut target, &delivery(&[(7, 3)], 1));
    let batch = plan(&mut target, &delivery(&[(7, 2), (7, -4)], 4), None).unwrap();
    assert_eq!(delete_ids(&batch), [1, 2, 3, 4]);
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| insert.technical_id)
            .collect::<Vec<_>>(),
        [4, 5]
    );
    assert_eq!(
        batch
            .deletes
            .iter()
            .map(|delete| delete.event_offset)
            .collect::<Vec<_>>(),
        [6, 7, 8, 9]
    );
}

#[test]
fn later_inserts_cannot_cover_an_invalid_negative_prefix() {
    let mut target = Target::default();
    assert!(plan(&mut target, &delivery(&[(7, -1), (7, 1)], 1), None).is_err());
    assert!(target.rows.is_empty());
}

#[test]
fn weighted_slices_keep_absolute_ids_and_only_lookup_current_negative_mutations() {
    let mut target = Target::default();
    for (offset, diff) in [(1, 1024), (1025, 1024), (2049, 2)] {
        apply(&mut target, &delivery(&[(7, diff)], offset));
    }
    assert_eq!(target.rows.len(), 2050);
    for (offset, diff) in [(2051, -1024), (3075, -1024), (4099, -2)] {
        apply(&mut target, &delivery(&[(7, diff)], offset));
    }
    assert!(target.rows.is_empty());
    assert_eq!(
        target
            .lookups
            .iter()
            .map(|requests| requests[0].take)
            .collect::<Vec<_>>(),
        [1024, 1024, 2]
    );
}

#[test]
fn last_event_id_is_usable_and_exclusive_tail_cannot_overflow() {
    let mut target = Target::default();
    let input = delivery(&[(7, 1)], MAX_TECHNICAL_ID);
    assert_eq!(
        plan(&mut target, &input, None).unwrap().inserts[0].technical_id,
        MAX_TECHNICAL_ID
    );
    assert_eq!(input.end_event_offset().unwrap(), u64::MAX);
    assert!(DeliveryBatch::for_test(change(&[(7, 2)]), MAX_TECHNICAL_ID).is_err());
    assert!(DeliveryBatch::for_test(change(&[(7, 1)]), u64::MAX).is_err());
}

#[test]
fn progress_clips_weighted_positive_and_negative_intervals_before_fifo() {
    let input = delivery(&[(7, 3), (7, -3)], 100);
    for (through, ids, inserted, deleted) in [
        (101, vec![100, 101], vec![102], vec![100, 101, 102]),
        (104, vec![102], vec![], vec![102]),
    ] {
        let mut target = Target {
            lookup_reply: Some(vec![Matches { through, ids }]),
            ..Target::default()
        };
        let batch = plan(&mut target, &input, None).unwrap();
        assert_eq!(
            batch
                .inserts
                .iter()
                .map(|insert| insert.technical_id)
                .collect::<Vec<_>>(),
            inserted
        );
        assert_eq!(delete_ids(&batch), deleted);
        assert_eq!(target.lookups[0][0].take, 3);
    }
    let covered_negative = delivery(&[(7, -3), (7, 3)], 100);
    let mut target = Target {
        lookup_reply: Some(vec![Matches {
            through: 102,
            ids: vec![],
        }]),
        ..Target::default()
    };
    let batch = plan(&mut target, &covered_negative, None).unwrap();
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| insert.technical_id)
            .collect::<Vec<_>>(),
        [103, 104, 105]
    );
    assert!(batch.deletes.is_empty());
}

#[test]
fn skipped_delivery_births_still_require_positive_events_and_complete_rows() {
    let input = delivery(&[(7, 2), (8, 1), (7, -2), (8, -1)], 100);
    let mut target = Target {
        lookup_reply: Some(vec![
            Matches {
                through: 103,
                ids: vec![101],
            },
            Matches {
                through: 102,
                ids: vec![102],
            },
        ]),
        ..Target::default()
    };
    let batch = plan(&mut target, &input, None).unwrap();
    assert!(batch.inserts.is_empty());
    assert_eq!(delete_ids(&batch), [101, 102]);
    assert_eq!(
        batch
            .deletes
            .iter()
            .map(|delete| delete.event_offset)
            .collect::<Vec<_>>(),
        [104, 105]
    );
    for bad_id in [102, 103] {
        let mut target = Target {
            lookup_reply: Some(vec![
                Matches {
                    through: 103,
                    ids: vec![bad_id],
                },
                Matches {
                    through: 105,
                    ids: vec![],
                },
            ]),
            ..Target::default()
        };
        assert!(
            plan(&mut target, &input, None).is_err(),
            "accepted skipped birth {bad_id}"
        );
    }
    let mut target = Target {
        lookup_reply: Some(vec![
            Matches {
                through: 102,
                ids: vec![100, 101],
            },
            Matches {
                through: 102,
                ids: vec![102],
            },
        ]),
        ..Target::default()
    };
    let batch = plan_from(&mut target, &input, 103, None).unwrap();
    assert!(batch.inserts.is_empty());
    assert_eq!(delete_ids(&batch), [100, 101, 102]);
    assert_eq!(
        target.lookups[0]
            .iter()
            .map(|request| request.take)
            .collect::<Vec<_>>(),
        [2, 1]
    );
}

#[test]
fn retained_head_evidence_rejects_negative_births_and_wrong_full_rows() {
    let head = change(&[(7, 3), (8, -2), (9, 4)]);
    let input = delivery(&[(7, -2), (9, -1)], 109);
    for (ids, valid) in [
        (vec![100, 102], true),
        (vec![100, 103], false),
        (vec![100, 106], false),
    ] {
        let mut target = Target {
            lookup_reply: Some(vec![
                Matches { through: 108, ids },
                Matches {
                    through: 108,
                    ids: vec![107],
                },
            ]),
            ..Target::default()
        };
        assert_eq!(plan(&mut target, &input, Some((100, &head))).is_ok(), valid);
    }
    let wrong = change(&[(7, 3), (8, -2), (8, 4)]);
    let mut target = Target {
        lookup_reply: Some(vec![
            Matches {
                through: 108,
                ids: vec![100, 102],
            },
            Matches {
                through: 108,
                ids: vec![108],
            },
        ]),
        ..Target::default()
    };
    assert!(plan(&mut target, &input, Some((100, &wrong))).is_err());
}

#[test]
fn reclaimed_historical_ids_have_no_retained_birth_evidence() {
    let input = delivery(&[(7, -1)], 4);
    let mut target = Target {
        lookup_reply: Some(vec![Matches {
            through: 3,
            ids: vec![2],
        }]),
        ..Target::default()
    };
    assert_eq!(delete_ids(&plan(&mut target, &input, None).unwrap()), [2]);
}

#[test]
fn signed_sql_ids_roundtrip_and_preserve_unsigned_order() {
    let ids = [
        1,
        2,
        i64::MAX.unsigned_abs(),
        1_u64 << 63,
        (1_u64 << 63) + 1,
        MAX_TECHNICAL_ID,
    ];
    let encoded = ids.map(encode_signed_id);
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    for (id, sql) in ids.into_iter().zip(encoded) {
        assert_eq!(decode_signed_id(sql).unwrap(), id);
    }
    assert!(decode_signed_id(i64::MIN).is_err());
    assert!(decode_signed_id(i64::MAX).is_err());
}

#[test]
fn terminal_mutations_keep_the_true_birth_or_negative_event_version() {
    assert_eq!(
        terminal_mutations(&Batch {
            inserts: vec![
                Insert {
                    row_index: 1,
                    technical_id: 11
                },
                Insert {
                    row_index: 0,
                    technical_id: 7
                }
            ],
            deletes: vec![
                Delete {
                    row_index: 2,
                    technical_id: 11,
                    event_offset: 13
                },
                Delete {
                    row_index: 0,
                    technical_id: 3,
                    event_offset: 10
                }
            ],
        }),
        vec![
            TerminalMutation {
                row_index: 0,
                technical_id: 3,
                version: 10
            },
            TerminalMutation {
                row_index: 0,
                technical_id: 7,
                version: 7
            },
            TerminalMutation {
                row_index: 2,
                technical_id: 11,
                version: 13
            },
        ]
    );
}

fn repeated_binary_change(payload_bytes: usize, diffs: [i64; 2]) -> Change {
    let payload = vec![7_u8; payload_bytes];
    Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(payload.as_slice()),
                Some(payload.as_slice()),
            ]))],
        )
        .unwrap(),
        Int64Array::from(diffs.to_vec()),
    )
    .unwrap()
}

#[test]
fn large_current_row_pairs_plan_with_a_single_canonical_budget() {
    let input =
        DeliveryBatch::for_test(repeated_binary_change(3 * 1024 * 1024, [512, -512]), 1).unwrap();
    let mut target = Target::default();
    assert_eq!(
        delete_ids(&plan(&mut target, &input, None).unwrap()),
        (1..=512).collect::<Vec<_>>()
    );
}

#[test]
fn new_fifo_births_need_no_retained_row_work_but_keep_input_admission() {
    let retained = vec![7_u8; 7 * 1024 * 1024];
    let head = Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(retained.as_slice()),
                Some([1_u8].as_slice()),
                Some([1_u8].as_slice()),
            ]))],
        )
        .unwrap(),
        Int64Array::from(vec![1, 512, -512]),
    )
    .unwrap();
    let codec = SchemaBoundChangeCodec::try_new(head.records().schema()).unwrap();
    assert!(codec.encode(&head).unwrap().len() < 8 * 1024 * 1024);
    let current = Change::try_new(
        head.records().slice(1, 2),
        Int64Array::from(vec![512, -512]),
    )
    .unwrap();
    let input = DeliveryBatch::for_test(current, 2).unwrap();
    let mut target = Target::default();
    let batch = plan(&mut target, &input, Some((1, &head))).unwrap();
    let ids = (2..=513).collect::<Vec<_>>();
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| insert.technical_id)
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(delete_ids(&batch), ids);
    assert_eq!(batch.deletes.first().unwrap().event_offset, 514);
    assert_eq!(batch.deletes.last().unwrap().event_offset, 1025);

    let oversized =
        DeliveryBatch::for_test(repeated_binary_change(4 * 1024 * 1024, [512, -512]), 2).unwrap();
    let mut target = Target::default();
    assert!(plan(&mut target, &oversized, Some((1, &head))).is_err());
    assert!(target.lookups.is_empty());
}

#[test]
fn old_deletes_and_insert_only_rows_share_the_canonical_budget() {
    for (diffs, offset) in [([-1, -1], 3), ([1, 1], 1)] {
        let input = DeliveryBatch::for_test(repeated_binary_change(4 * 1024 * 1024, diffs), offset)
            .unwrap();
        let mut target = Target::default();
        assert!(plan(&mut target, &input, None).is_err());
        assert!(target.lookups.is_empty());
    }
}

#[test]
fn retained_wide_birth_comparison_does_not_duplicate_the_canonical_budget() {
    let head = repeated_binary_change(3 * 1024 * 1024, [3, -1]);
    let codec = SchemaBoundChangeCodec::try_new(head.records().schema()).unwrap();
    assert!(codec.encode(&head).unwrap().len() < 8 * 1024 * 1024);
    let mut target = Target::default();
    let first = Change::try_new(head.records().slice(0, 1), Int64Array::from(vec![2])).unwrap();
    apply(&mut target, &DeliveryBatch::for_test(first, 1).unwrap());
    let input =
        DeliveryBatch::for_test(repeated_binary_change(3 * 1024 * 1024, [1, -1]), 3).unwrap();
    let batch = plan(&mut target, &input, Some((1, &head))).unwrap();
    assert_eq!(delete_ids(&batch), [1]);
    assert_eq!(batch.inserts[0].technical_id, 3);
    target.write_batch(input.change(), &batch).unwrap();
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [2, 3]);
}

#[test]
fn one_wide_retained_delete_needs_no_second_canonical_payload() {
    let head = repeated_binary_change(6 * 1024 * 1024, [1, -1]);
    let current = Change::try_new(head.records().slice(1, 1), Int64Array::from(vec![-1])).unwrap();
    let input = DeliveryBatch::for_test(current, 2).unwrap();
    let mut target = Target {
        lookup_reply: Some(vec![Matches {
            through: 1,
            ids: vec![1],
        }]),
        ..Target::default()
    };
    assert_eq!(
        delete_ids(&plan(&mut target, &input, Some((1, &head))).unwrap()),
        [1]
    );
}

#[test]
fn retained_scan_skips_unreferenced_canonical_rows() {
    let payload = vec![7_u8; 8 * 1024 * 1024];
    let head = Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(payload.as_slice()),
                Some([1_u8].as_slice()),
            ]))],
        )
        .unwrap(),
        Int64Array::from(vec![1, 1]),
    )
    .unwrap();
    let current = Change::try_new(
        RecordBatch::try_new(
            head.records().schema(),
            vec![Arc::new(BinaryArray::from(vec![Some([1_u8].as_slice())]))],
        )
        .unwrap(),
        Int64Array::from(vec![-1]),
    )
    .unwrap();
    let input = DeliveryBatch::for_test(current, 3).unwrap();
    let mut target = Target {
        lookup_reply: Some(vec![Matches {
            through: 2,
            ids: vec![2],
        }]),
        ..Target::default()
    };
    assert!(plan(&mut target, &input, Some((1, &head))).is_ok());
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
    let codec = SchemaBoundChangeCodec::try_new(input.records().schema()).unwrap();
    assert!(codec.encode(&input).unwrap().len() < 1024);
    let mut target = Target::default();
    assert!(
        plan(
            &mut target,
            &DeliveryBatch::for_test(input, 1).unwrap(),
            None
        )
        .is_err()
    );
    assert!(target.lookups.is_empty());
}

#[test]
fn lookup_requires_bounded_unique_sorted_ids_and_progress_below_tail() {
    let request = Lookup {
        row_index: 0,
        take: 2,
    };
    for found in [
        Matches {
            through: 9,
            ids: vec![0],
        },
        Matches {
            through: 9,
            ids: vec![10],
        },
        Matches {
            through: 9,
            ids: vec![2, 1],
        },
        Matches {
            through: 9,
            ids: vec![1, 1],
        },
        Matches {
            through: 9,
            ids: vec![1, 2, 3],
        },
        Matches {
            through: 10,
            ids: vec![],
        },
        Matches {
            through: 0,
            ids: vec![1],
        },
    ] {
        assert!(plan::validate_matches(&request, &found, 10).is_err());
    }
    assert!(
        plan::validate_matches(
            &request,
            &Matches {
                through: 9,
                ids: vec![1, 9]
            },
            10
        )
        .is_ok()
    );
    assert!(
        plan::validate_matches(
            &request,
            &Matches {
                through: 12,
                ids: vec![10, 11]
            },
            20
        )
        .is_ok()
    );
}

#[test]
fn lookup_rejects_missing_results_and_ids_shared_between_distinct_rows() {
    let input = delivery(&[(7, -1), (8, -1)], 10);
    for reply in [
        vec![],
        vec![Matches {
            through: 9,
            ids: vec![1],
        }],
        vec![
            Matches {
                through: 9,
                ids: vec![1],
            },
            Matches {
                through: 9,
                ids: vec![1],
            },
        ],
    ] {
        let mut target = Target {
            lookup_reply: Some(reply),
            ..Target::default()
        };
        assert!(plan(&mut target, &input, None).is_err());
    }
}

#[test]
fn delivery_bounds_reject_future_progress_and_reversed_floor_before_lookup() {
    let input = delivery(&[(7, -1)], 10);
    for (from, tail, head) in [
        (9, 11, 10),
        (12, 11, 10),
        (10, 10, 10),
        (10, 11, 0),
        (10, 11, 11),
    ] {
        assert!(
            super::plan(&input, from, tail, (head, input.change()), |_| panic!(
                "invalid bounds performed lookup"
            ))
            .is_err()
        );
    }
}

#[test]
fn borrowed_retained_row_equality_matches_canonical_float_bits_and_nested_nulls() {
    let child = Arc::new(Field::new("value", DataType::Int64, true));
    let fields = vec![
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new(
            "nested",
            DataType::Struct(vec![Arc::clone(&child)].into()),
            true,
        ),
    ];
    let nested = StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(vec![Some(7), Some(99), None]))],
        Some(NullBuffer::from(vec![false, false, true])),
    );
    let nan32 = f32::from_bits(0x7fc0_0001);
    let nan64 = f64::from_bits(0x7ff8_0000_0000_0001);
    let records = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Float32Array::from(vec![nan32, nan32, nan32])),
            Arc::new(Float64Array::from(vec![nan64, nan64, nan64])),
            Arc::new(nested),
        ],
    )
    .unwrap();
    for (left, right) in [(0, 1), (0, 2)] {
        assert_eq!(
            records.slice(left, 1) == records.slice(right, 1),
            canonical_row_bounded(&records, left, 1024).unwrap()
                == canonical_row_bounded(&records, right, 1024).unwrap()
        );
    }
    assert_eq!(records.slice(0, 1), records.slice(1, 1));
    assert_ne!(records.slice(0, 1), records.slice(2, 1));
    for values in [
        [-0.0_f64, 0.0],
        [nan64, f64::from_bits(nan64.to_bits() + 1)],
    ] {
        let records = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "float",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(values.to_vec()))],
        )
        .unwrap();
        assert_ne!(records.slice(0, 1), records.slice(1, 1));
        assert_ne!(
            canonical_row_bounded(&records, 0, 1024).unwrap(),
            canonical_row_bounded(&records, 1, 1024).unwrap()
        );
    }
}

#[test]
fn borrowed_row_equality_matches_canonical_identity_for_every_v1_type_family() {
    use datafusion_common::ScalarValue;

    let list = ScalarValue::new_list(
        &[ScalarValue::Int64(Some(7)), ScalarValue::Int64(None)],
        &DataType::Int64,
        true,
    );
    let values = vec![
        ScalarValue::Null,
        ScalarValue::Boolean(Some(true)),
        ScalarValue::Int8(Some(i8::MIN)),
        ScalarValue::Int16(Some(i16::MIN)),
        ScalarValue::Int32(Some(i32::MIN)),
        ScalarValue::Int64(Some(i64::MIN)),
        ScalarValue::UInt8(Some(u8::MAX)),
        ScalarValue::UInt16(Some(u16::MAX)),
        ScalarValue::UInt32(Some(u32::MAX)),
        ScalarValue::UInt64(Some(u64::MAX)),
        ScalarValue::Float32(Some(-0.0)),
        ScalarValue::Float64(Some(-0.0)),
        ScalarValue::Date32(Some(-123)),
        ScalarValue::TimestampSecond(Some(-7), None),
        ScalarValue::TimestampMillisecond(Some(-7), None),
        ScalarValue::TimestampMicrosecond(Some(-7), Some("UTC".into())),
        ScalarValue::TimestampNanosecond(Some(-7), None),
        ScalarValue::Decimal128(Some(-123), 12, 2),
        ScalarValue::Utf8(Some("embedded\0text".to_owned())),
        ScalarValue::Binary(Some(vec![0, 255, 7])),
        ScalarValue::List(list),
        ScalarValue::Struct(Arc::new(StructArray::new_empty_fields(1, None))),
    ];
    for value in values {
        let data_type = value.data_type();
        let null = ScalarValue::try_from(&data_type).unwrap();
        let values = ScalarValue::iter_to_array([value.clone(), value, null]).unwrap();
        let records = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                data_type.clone(),
                true,
            )])),
            vec![values],
        )
        .unwrap();
        for left in 0..3 {
            for right in 0..3 {
                assert_eq!(
                    records.slice(left, 1) == records.slice(right, 1),
                    canonical_row_bounded(&records, left, 1024).unwrap()
                        == canonical_row_bounded(&records, right, 1024).unwrap(),
                    "identity mismatch for {data_type} rows {left}/{right}"
                );
            }
        }
    }
}
