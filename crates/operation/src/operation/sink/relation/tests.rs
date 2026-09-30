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
    lookup_reply: Option<Vec<Vec<u64>>>,
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
        if let Some(reply) = self.lookup_reply.take() {
            return Ok(reply.into_iter().map(|ids| Matches { ids }).collect());
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
                    .collect::<Vec<_>>();
                Ok(Matches {
                    ids: ids.into_iter().take(request.take).collect(),
                })
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
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &expected => {
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(invalid("insert ID belongs to a different row"));
                }
            }
        }
        for delete in &batch.deletes {
            let expected = canonical_row_bounded(
                input.records(),
                usize::try_from(delete.row_index).unwrap(),
                usize::MAX,
            )?;
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

fn delivery(rows: &[(i64, i64)], first_event_offset: u64) -> DeliveryBatch {
    DeliveryBatch::for_test(change(rows), first_event_offset).unwrap()
}

fn prepare(
    target: &mut impl RelationTarget,
    input: &DeliveryBatch,
    original_head: Option<(u64, &Change)>,
) -> Result<Batch, OperationError> {
    super::prepare(
        target,
        input,
        original_head.unwrap_or((input.first_event_offset(), input.change())),
    )
}

fn recover(
    input: &DeliveryBatch,
    negative_ids: &[u64],
    original_head: Option<(u64, &Change)>,
) -> Result<Batch, OperationError> {
    super::recover(
        input,
        negative_ids,
        original_head.unwrap_or((input.first_event_offset(), input.change())),
    )
}

fn apply(target: &mut Target, input: &DeliveryBatch) -> Batch {
    let batch = prepare(target, input, None).unwrap();
    assert_eq!(recover(input, &batch.negative_ids(), None).unwrap(), batch);
    target.write_batch(input.change(), &batch).unwrap();
    let once = target.rows.clone();
    target.write_batch(input.change(), &batch).unwrap();
    assert_eq!(target.rows, once, "fixed plan replay must be idempotent");
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
fn insert_only_ids_follow_event_order_without_lookup_or_negative_plan() {
    let input = delivery(&[(7, 2), (8, 1), (7, 3)], 100);
    let mut target = Target::default();
    let batch = prepare(&mut target, &input, None).unwrap();
    assert!(target.lookups.is_empty());
    assert!(batch.negative_ids().is_empty());
    assert_eq!(
        batch
            .inserts
            .iter()
            .map(|insert| (insert.row_index, insert.technical_id))
            .collect::<Vec<_>>(),
        [(0, 100), (0, 101), (1, 102), (2, 103), (2, 104), (2, 105)]
    );
    assert_eq!(recover(&input, &[], None).unwrap(), batch);
}

#[test]
fn retractions_use_oldest_existing_then_current_event_ids() {
    let mut target = Target::default();
    apply(&mut target, &delivery(&[(7, 3)], 1));
    let batch = prepare(&mut target, &delivery(&[(7, 2), (7, -4)], 4), None).unwrap();
    assert_eq!(batch.negative_ids(), [1, 2, 3, 4]);
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
    assert!(prepare(&mut target, &delivery(&[(7, -1), (7, 1)], 1), None).is_err());
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
        prepare(&mut target, &input, None).unwrap().inserts[0].technical_id,
        MAX_TECHNICAL_ID
    );
    assert!(DeliveryBatch::for_test(change(&[(7, 2)]), MAX_TECHNICAL_ID).is_err());
    assert!(DeliveryBatch::for_test(change(&[(7, 1)]), u64::MAX).is_err());
}

#[test]
fn recovery_derives_indexes_and_rejects_wrong_counts_domains_duplicates_and_birth_rows() {
    let input = delivery(&[(7, 2), (8, 1), (7, -2), (8, -1)], 100);
    let mut target = Target::default();
    let batch = prepare(&mut target, &input, None).unwrap();
    assert_eq!(batch.negative_ids(), [100, 101, 102]);
    assert_eq!(recover(&input, &batch.negative_ids(), None).unwrap(), batch);
    for ids in [
        vec![],
        vec![100],
        vec![0, 101, 102],
        vec![u64::MAX, 101, 102],
        vec![100, 100, 102],
        vec![102, 101, 100],
        vec![103, 101, 102],
        vec![106, 101, 102],
    ] {
        assert!(
            recover(&input, &ids, None).is_err(),
            "accepted invalid IDs {ids:?}"
        );
    }
    let later = delivery(&[(7, -1), (7, 1)], 100);
    assert!(recover(&later, &[101], None).is_err());
}

#[test]
fn retained_head_evidence_rejects_negative_births_and_wrong_full_rows() {
    let head = change(&[(7, 3), (8, -2), (9, 4)]);
    let input = delivery(&[(7, -2), (9, -1)], 109);
    let batch = recover(&input, &[100, 102, 107], Some((100, &head))).unwrap();
    assert_eq!(batch.negative_ids(), [100, 102, 107]);
    assert!(recover(&input, &[100, 103, 107], Some((100, &head))).is_err());
    assert!(recover(&input, &[100, 106, 107], Some((100, &head))).is_err());
    assert!(
        recover(
            &input,
            &[100, 102, 108],
            Some((100, &change(&[(7, 3), (8, -2), (8, 4)])))
        )
        .is_err()
    );
}

#[test]
fn reclaimed_negative_gap_has_no_birth_evidence_and_keeps_missing_delete_replay() {
    let mut target = Target::default();
    apply(&mut target, &delivery(&[(7, 1), (7, -1), (7, 1)], 1));
    let input = delivery(&[(7, -1)], 4);
    let corrupted = recover(&input, &[2], None).unwrap();
    target.write_batch(input.change(), &corrupted).unwrap();
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [3]);
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
fn mutation_grouping_collects_each_rows_insert_validation_and_delete_ids() {
    let grouped = group_mutations(&Batch {
        inserts: vec![
            Insert {
                row_index: 1,
                technical_id: 11,
            },
            Insert {
                row_index: 0,
                technical_id: 7,
            },
        ],
        deletes: vec![
            Delete {
                row_index: 1,
                technical_id: 11,
            },
            Delete {
                row_index: 0,
                technical_id: 3,
            },
        ],
    });

    assert_eq!(
        grouped,
        MutationGroups {
            rows: vec![
                MutationGroup {
                    row_index: 0,
                    insert_ids: vec![7],
                    mutation_ids: vec![7, 3],
                },
                MutationGroup {
                    row_index: 1,
                    insert_ids: vec![11],
                    mutation_ids: vec![11, 11],
                },
            ],
            delete_ids: vec![11, 3],
        }
    );
    assert_eq!(
        terminal_mutations(&Batch {
            inserts: vec![
                Insert {
                    row_index: 1,
                    technical_id: 11,
                },
                Insert {
                    row_index: 0,
                    technical_id: 7,
                },
            ],
            deletes: vec![
                Delete {
                    row_index: 2,
                    technical_id: 11,
                },
                Delete {
                    row_index: 0,
                    technical_id: 3,
                },
            ],
        }),
        vec![
            TerminalMutation {
                row_index: 0,
                technical_id: 3,
                deleted: true,
            },
            TerminalMutation {
                row_index: 0,
                technical_id: 7,
                deleted: false,
            },
            TerminalMutation {
                row_index: 2,
                technical_id: 11,
                deleted: true,
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
fn recovered_plan_compares_each_large_row_pair_once() {
    let input =
        DeliveryBatch::for_test(repeated_binary_change(3 * 1024 * 1024, [512, -512]), 1).unwrap();
    let ids = (1..=512).collect::<Vec<_>>();
    assert_eq!(recover(&input, &ids, None).unwrap().negative_ids(), ids);
}

#[test]
fn recovered_old_deletes_and_insert_only_rows_share_the_canonical_budget() {
    let deletes =
        DeliveryBatch::for_test(repeated_binary_change(4 * 1024 * 1024, [-1, -1]), 3).unwrap();
    assert!(recover(&deletes, &[1, 2], None).is_err());
    let inserts =
        DeliveryBatch::for_test(repeated_binary_change(4 * 1024 * 1024, [1, 1]), 1).unwrap();
    let mut target = Target::default();
    assert!(prepare(&mut target, &inserts, None).is_err());
    assert!(target.lookups.is_empty());
}

#[test]
fn retained_wide_birth_comparison_does_not_duplicate_the_canonical_budget() {
    let head = repeated_binary_change(3 * 1024 * 1024, [3, -1]);
    let codec = SchemaBoundChangeCodec::try_new(head.records().schema()).unwrap();
    assert!(codec.encode(&head).unwrap().len() < 8 * 1024 * 1024);
    let mut target = Target::default();
    let first = Change::try_new(head.records().slice(0, 1), Int64Array::from(vec![2])).unwrap();
    apply(&mut target, &DeliveryBatch::for_test(first, 1).unwrap());
    let current =
        DeliveryBatch::for_test(repeated_binary_change(3 * 1024 * 1024, [1, -1]), 3).unwrap();
    let batch = prepare(&mut target, &current, Some((1, &head))).unwrap();
    assert_eq!(batch.negative_ids(), [1]);
    assert_eq!(batch.inserts[0].technical_id, 3);
    assert_eq!(recover(&current, &[1], Some((1, &head))).unwrap(), batch);
    target.write_batch(current.change(), &batch).unwrap();
    target.write_batch(current.change(), &batch).unwrap();
    assert_eq!(target.rows.keys().copied().collect::<Vec<_>>(), [2, 3]);
}

#[test]
fn one_wide_retained_delete_needs_no_second_canonical_payload() {
    let head = repeated_binary_change(6 * 1024 * 1024, [1, -1]);
    let current = Change::try_new(head.records().slice(1, 1), Int64Array::from(vec![-1])).unwrap();
    let input = DeliveryBatch::for_test(current, 2).unwrap();
    assert_eq!(
        recover(&input, &[1], Some((1, &head)))
            .unwrap()
            .negative_ids(),
        [1]
    );
}

#[test]
fn retained_scan_skips_unreferenced_canonical_rows() {
    let payload = vec![7_u8; 8 * 1024 * 1024];
    let input = Change::try_new(
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
            input.records().schema(),
            vec![Arc::new(BinaryArray::from(vec![Some([1_u8].as_slice())]))],
        )
        .unwrap(),
        Int64Array::from(vec![-1]),
    )
    .unwrap();
    let delivery = DeliveryBatch::for_test(current, 3).unwrap();
    assert!(recover(&delivery, &[2], Some((1, &input))).is_ok());
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
        prepare(
            &mut target,
            &DeliveryBatch::for_test(input, 1).unwrap(),
            None
        )
        .is_err()
    );
    assert!(target.lookups.is_empty());
}

#[test]
fn lookup_requires_bounded_unique_sorted_ids_below_the_delivery_horizon() {
    let request = Lookup {
        row_index: 0,
        take: 2,
    };
    for ids in [vec![0], vec![10], vec![2, 1], vec![1, 1], vec![1, 2, 3]] {
        assert!(plan::validate_matches(&request, &Matches { ids }, 10).is_err());
    }
    assert!(plan::validate_matches(&request, &Matches { ids: vec![1, 9] }, 10).is_ok());
}

#[test]
fn lookup_rejects_missing_results_and_ids_shared_between_distinct_rows() {
    let input = delivery(&[(7, -1), (8, -1)], 10);
    for reply in [vec![], vec![vec![1]], vec![vec![1], vec![1]]] {
        let mut target = Target {
            lookup_reply: Some(reply),
            ..Target::default()
        };
        assert!(prepare(&mut target, &input, None).is_err());
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
