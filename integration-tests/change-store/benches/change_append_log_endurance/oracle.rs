use std::collections::BTreeMap;

use dogpaddle_change::{Change, encode_change};
use dogpaddle_change_store_integration::{WorkloadPersona, assert_change_eq};

use super::workload::ExpectedPageEntry;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SchemaRelation {
    entries: usize,
    rows: usize,
    weight: i128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StreamOracle {
    next_offset: u64,
    entries: usize,
    rows: usize,
    relation: BTreeMap<&'static str, SchemaRelation>,
    order_checksum: u64,
}

impl StreamOracle {
    pub(super) fn new(start_offset: u64) -> Self {
        Self {
            next_offset: start_offset,
            entries: 0,
            rows: 0,
            relation: BTreeMap::new(),
            order_checksum: FNV_OFFSET,
        }
    }

    pub(super) fn observe(
        &mut self,
        offset: u64,
        change: &Change,
        concrete_persona: WorkloadPersona,
        encoded: &[u8],
    ) {
        assert_eq!(offset, self.next_offset, "Change offsets remain contiguous");
        assert!(
            change.diffs().values().iter().all(|diff| *diff == 1),
            "endurance personas use the declared insert-only difference model"
        );
        let schema = concrete_persona.descriptor().schemas[0];
        assert_eq!(change.records().num_columns(), schema.business_columns);
        let relation = self.relation.entry(schema.name).or_default();
        relation.entries = relation
            .entries
            .checked_add(1)
            .expect("relation entry count fits usize");
        relation.rows = relation
            .rows
            .checked_add(change.num_rows())
            .expect("relation row count fits usize");
        relation.weight = relation
            .weight
            .checked_add(i128::try_from(change.num_rows()).expect("row count fits i128"))
            .expect("relation weight fits i128");
        self.entries = self
            .entries
            .checked_add(1)
            .expect("stream entry count fits usize");
        self.rows = self
            .rows
            .checked_add(change.num_rows())
            .expect("stream row count fits usize");
        self.order_checksum = hash_u64(self.order_checksum, offset);
        self.order_checksum = hash_u64(
            self.order_checksum,
            u64::try_from(encoded.len()).expect("encoded entry length fits u64"),
        );
        self.order_checksum = hash_bytes(self.order_checksum, encoded);
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .expect("stream offset fits u64");
    }

    pub(super) const fn entries(&self) -> usize {
        self.entries
    }

    pub(super) const fn rows(&self) -> usize {
        self.rows
    }

    pub(super) const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub(super) const fn order_checksum(&self) -> u64 {
        self.order_checksum
    }
}

pub(super) fn verify_full_page(
    expected: &[ExpectedPageEntry],
    actual: &[(u64, Change)],
    oracle: &mut StreamOracle,
) {
    assert_eq!(actual.len(), expected.len());
    for ((offset, actual), expected) in actual.iter().zip(expected) {
        assert_eq!(*offset, expected.offset);
        assert_change_eq(actual, &expected.full);
        let actual_encoded = encode_change(actual).expect("re-encode full consumer Change");
        assert_eq!(actual_encoded.len(), expected.encoded_len);
        let expected_encoded = encode_change(&expected.full).expect("encode full consumer oracle");
        assert_eq!(actual_encoded, expected_encoded);
        oracle.observe(*offset, actual, expected.concrete_persona, &actual_encoded);
    }
}

pub(super) fn verify_projected_page(expected: &[ExpectedPageEntry], actual: &[(u64, Change)]) {
    assert_eq!(actual.len(), expected.len());
    for ((offset, actual), expected) in actual.iter().zip(expected) {
        assert_eq!(*offset, expected.offset);
        assert_change_eq(actual, &expected.projected);
    }
}

fn hash_u64(state: u64, value: u64) -> u64 {
    hash_bytes(state, &value.to_le_bytes())
}

fn hash_bytes(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state = (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
    }
    state
}
