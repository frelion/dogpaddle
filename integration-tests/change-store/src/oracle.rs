use arrow_array::{Array, UInt64Array};
use dogpaddle_change::Change;

use crate::fixture::narrow_schema;

/// One flattened event from a narrow Change sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    /// Logical record value.
    pub value: u64,
    /// Signed difference for the record occurrence.
    pub diff: i64,
}

/// Asserts complete logical equality between two Changes.
///
/// # Panics
///
/// Panics when schemas, arrays, row order, or differences differ.
pub fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
}

/// Flattens narrow Changes without treating physical Change boundaries as
/// semantic boundaries.
///
/// # Panics
///
/// Panics when a Change does not use the narrow fixture schema.
#[must_use]
pub fn flatten_narrow(changes: &[Change]) -> Vec<Event> {
    changes
        .iter()
        .flat_map(|change| {
            assert_eq!(
                change.schema().as_ref(),
                narrow_schema().as_ref(),
                "narrow fixture Schema"
            );
            let values = change
                .records()
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("narrow fixture UInt64 column");
            assert_eq!(values.null_count(), 0, "narrow fixture is non-nullable");
            values
                .values()
                .iter()
                .copied()
                .zip(change.diffs().values().iter().copied())
                .map(|(value, diff)| Event { value, diff })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Computes an order-sensitive checksum over a decoded Change.
///
/// The checksum includes differences, column count, null counts, complete
/// `UInt64` value buffers, and representative `Binary` length/edge bytes. It
/// deliberately stays linear in rows rather than payload bytes. It is only a
/// lightweight benchmark sink, not a complete equality oracle or persistent
/// identifier; untimed validation uses [`assert_change_eq`].
///
/// # Panics
///
/// Panics if the Change contains a fixture type not understood by this oracle.
#[must_use]
pub fn checksum_change(change: &Change) -> u64 {
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    checksum = mix(checksum, change.num_rows() as u64);
    checksum = mix(checksum, change.records().num_columns() as u64);
    for diff in change.diffs().values() {
        checksum = mix(checksum, u64::from_ne_bytes(diff.to_ne_bytes()));
    }
    for column in change.records().columns() {
        checksum = mix(checksum, column.null_count() as u64);
        if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
            for value in values.values() {
                checksum = mix(checksum, *value);
            }
        } else if let Some(values) = column.as_any().downcast_ref::<arrow_array::BinaryArray>() {
            for index in 0..values.len() {
                if values.is_null(index) {
                    checksum = mix(checksum, u64::MAX);
                } else {
                    let value = values.value(index);
                    checksum = mix(checksum, value.len() as u64);
                    if let Some(first) = value.first() {
                        checksum = mix(checksum, u64::from(*first));
                    }
                    if let Some(last) = value.last() {
                        checksum = mix(checksum, u64::from(*last));
                    }
                }
            }
        } else {
            panic!("checksum oracle does not support {:?}", column.data_type());
        }
    }
    checksum
}

fn mix(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(0x0000_0100_0000_01b3)
}
