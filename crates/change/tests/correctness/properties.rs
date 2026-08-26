use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{
    Change, ChangeProjection, decode_change, decode_change_projected, encode_change,
};

const PROPERTY_SEEDS: [u64; 12] = [
    0x0000_0000_0000_0001,
    0x0000_0000_0000_0040,
    0x0000_0000_0000_0041,
    0x0123_4567_89ab_cdef,
    0x0f0f_f0f0_55aa_aa55,
    0x243f_6a88_85a3_08d3,
    0x6a09_e667_f3bc_c909,
    0x7fff_ffff_ffff_ffff,
    0x8000_0000_0000_0001,
    0x9e37_79b9_7f4a_7c15,
    0xdead_beef_cafe_babe,
    0xffff_ffff_ffff_ffff,
];

#[derive(Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn below(&mut self, upper: usize) -> usize {
        assert!(upper > 0);
        usize::try_from(self.next_u64() % u64::try_from(upper).unwrap()).unwrap()
    }
}

fn generated_change(seed: u64) -> Change {
    let mut rng = DeterministicRng(seed);
    let rows = 1 + rng.below(65);
    let ids = (0..rows).map(|_| rng.next_u64() % 11).collect::<Vec<_>>();
    let flags = (0..rows)
        .map(|_| match rng.next_u64() % 4 {
            0 => None,
            value => Some(value.is_multiple_of(2)),
        })
        .collect::<Vec<_>>();
    let diffs = (0..rows)
        .map(|_| {
            let magnitude = i64::try_from(1 + rng.next_u64() % 4).unwrap();
            if rng.next_u64().is_multiple_of(2) {
                magnitude
            } else {
                -magnitude
            }
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

fn events(change: &Change) -> Vec<(u64, Option<bool>, i64)> {
    let ids = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let flags = change
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    ids.values()
        .iter()
        .copied()
        .zip(flags.iter())
        .zip(change.diffs().values().iter().copied())
        .map(|((id, flag), diff)| (id, flag, diff))
        .collect()
}

#[test]
fn seeded_change_properties_preserve_roundtrip_projection_slice_and_rebatch() {
    for seed in PROPERTY_SEEDS {
        let change = generated_change(seed);
        let encoded = encode_change(&change).unwrap();
        let full = decode_change(&encoded).unwrap();
        assert_eq!(full.records(), change.records(), "seed {seed:#018x}");
        assert_eq!(full.diffs(), change.diffs(), "seed {seed:#018x}");

        for selection in [vec![], vec![0], vec![1], vec![0, 1]] {
            let projection = ChangeProjection::try_new(change.schema(), selection.clone()).unwrap();
            let expected = full.try_project(&projection).unwrap();
            let actual = decode_change_projected(&encoded, &projection).unwrap();
            assert_eq!(
                actual.records(),
                expected.records(),
                "seed {seed:#018x}, projection {selection:?}"
            );
            assert_eq!(
                actual.diffs(),
                expected.diffs(),
                "seed {seed:#018x}, projection {selection:?}"
            );
        }

        let expected_events = events(&change);
        let mut partition_rng = DeterministicRng(seed ^ 0xa5a5_5a5a_d3c4_b2e1);
        let offset = partition_rng.below(change.num_rows());
        let length = 1 + partition_rng.below(change.num_rows() - offset);
        let slice = change.try_slice(offset, length).unwrap();
        let decoded_slice = decode_change(&encode_change(&slice).unwrap()).unwrap();
        assert_eq!(
            events(&decoded_slice),
            expected_events[offset..offset + length],
            "seed {seed:#018x}, slice {offset}..{}",
            offset + length
        );

        let mut actual_events = Vec::with_capacity(change.num_rows());
        let mut start = 0;
        while start < change.num_rows() {
            let remaining = change.num_rows() - start;
            let batch_rows = 1 + partition_rng.below(remaining.min(8));
            let batch = change.try_slice(start, batch_rows).unwrap();
            let decoded = decode_change(&encode_change(&batch).unwrap()).unwrap();
            actual_events.extend(events(&decoded));
            start += batch_rows;
        }
        assert_eq!(
            actual_events, expected_events,
            "seed {seed:#018x}, stable rebatching"
        );
    }
}
