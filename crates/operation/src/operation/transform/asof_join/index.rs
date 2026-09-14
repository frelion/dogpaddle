//! Collision-free framing for the ordered ASOF row indexes.

use thiserror::Error;

const ESCAPE: u8 = 0;
const ESCAPED_ZERO: u8 = u8::MAX;
const TERMINATOR: u8 = 0;

/// Fully decoded ordered-map key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParsedIndexKey {
    pub(super) partition: Vec<u8>,
    pub(super) order: Vec<u8>,
    pub(super) rank: Vec<u8>,
    pub(super) row: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum IndexCodecError {
    #[error("ASOF index component is truncated")]
    TruncatedComponent,
    #[error("ASOF index component escape is invalid")]
    InvalidEscape,
    #[error("ASOF index key has trailing bytes")]
    TrailingBytes,
}

/// Prefix containing every row in one equality partition.
pub(super) fn partition_prefix(partition: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    push_component(&mut key, partition);
    key
}

/// Prefix containing only matchable-order rows in one equality partition.
///
/// The first byte of every unframed order tuple is its matchability marker.
/// Marker `1` never needs component escaping, so appending it to the framed
/// partition prefix selects every order component beginning with that marker
/// without including marker-`0` NULL-order rows.
pub(super) fn matchable_partition_prefix(partition: &[u8]) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.push(1);
    key
}

/// Prefix containing every rank and exact row at one order value.
pub(super) fn order_prefix(partition: &[u8], order: &[u8]) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    push_component(&mut key, order);
    key
}

/// Prefix containing every exact row at one deterministic rank.
pub(super) fn rank_prefix(partition: &[u8], order: &[u8], rank: &[u8]) -> Vec<u8> {
    let mut key = order_prefix(partition, order);
    push_component(&mut key, rank);
    key
}

/// Builds one complete ordered-map key.
pub(super) fn row_key(partition: &[u8], order: &[u8], rank: &[u8], row: &[u8]) -> Vec<u8> {
    let mut key = rank_prefix(partition, order, rank);
    push_component(&mut key, row);
    key
}

/// Strictly parses a complete key produced by [`row_key`].
pub(super) fn parse_row_key(encoded: &[u8]) -> Result<ParsedIndexKey, IndexCodecError> {
    let mut remaining = encoded;
    let partition = take_component(&mut remaining)?;
    let order = take_component(&mut remaining)?;
    let rank = take_component(&mut remaining)?;
    let row = take_component(&mut remaining)?;
    if !remaining.is_empty() {
        return Err(IndexCodecError::TrailingBytes);
    }
    Ok(ParsedIndexKey {
        partition,
        order,
        rank,
        row,
    })
}

/// Returns the smallest byte key strictly above every key beginning with `prefix`.
pub(super) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.pop() {
        if last != u8::MAX {
            successor.push(last + 1);
            return Some(successor);
        }
    }
    None
}

pub(super) fn push_component(output: &mut Vec<u8>, component: &[u8]) {
    output.reserve(component.len().saturating_add(2));
    for byte in component {
        if *byte == ESCAPE {
            output.extend_from_slice(&[ESCAPE, ESCAPED_ZERO]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[ESCAPE, TERMINATOR]);
}

/// Appends one prefix-free ordered component, optionally reversing its byte order.
///
/// Reversal happens after framing, including the terminator. Reversing an
/// unframed variable-width value would leave prefix pairs such as `a` and `aa`
/// in ascending order.
pub(super) fn push_ordered_component(output: &mut Vec<u8>, component: &[u8], descending: bool) {
    let start = output.len();
    push_component(output, component);
    if descending {
        for byte in &mut output[start..] {
            *byte = !*byte;
        }
    }
}

/// Appends one nullable ordered rank component with independent NULL placement.
pub(super) fn push_nullable_ordered_component(
    output: &mut Vec<u8>,
    component: Option<&[u8]>,
    descending: bool,
    nulls_first: bool,
) {
    let null_marker = u8::from(!nulls_first);
    let value_marker = u8::from(nulls_first);
    match component {
        None => push_component(output, &[null_marker]),
        Some(component) => {
            push_component(output, &[value_marker]);
            push_ordered_component(output, component, descending);
        }
    }
}

pub(super) fn take_component(remaining: &mut &[u8]) -> Result<Vec<u8>, IndexCodecError> {
    let mut decoded = Vec::new();
    loop {
        let (&byte, rest) = remaining
            .split_first()
            .ok_or(IndexCodecError::TruncatedComponent)?;
        *remaining = rest;
        if byte != ESCAPE {
            decoded.push(byte);
            continue;
        }
        let (&marker, rest) = remaining
            .split_first()
            .ok_or(IndexCodecError::TruncatedComponent)?;
        *remaining = rest;
        match marker {
            TERMINATOR => return Ok(decoded),
            ESCAPED_ZERO => decoded.push(0),
            _ => return Err(IndexCodecError::InvalidEscape),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_escaped_components_are_injective_and_order_preserving() {
        let components = [
            b"".as_slice(),
            b"\0".as_slice(),
            b"\0\0".as_slice(),
            b"\0\x01".as_slice(),
            b"a".as_slice(),
            b"a\0".as_slice(),
            b"aa".as_slice(),
        ];
        let encoded = components
            .iter()
            .map(|component| partition_prefix(component))
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(encoded[1], [0, u8::MAX, 0, 0]);
    }

    #[test]
    fn descending_components_reverse_variable_width_prefix_order() {
        let components = [
            b"".as_slice(),
            b"a".as_slice(),
            b"a\0".as_slice(),
            b"aa".as_slice(),
        ];
        let ascending = components
            .iter()
            .map(|component| {
                let mut encoded = Vec::new();
                push_ordered_component(&mut encoded, component, false);
                encoded
            })
            .collect::<Vec<_>>();
        let descending = components
            .iter()
            .map(|component| {
                let mut encoded = Vec::new();
                push_ordered_component(&mut encoded, component, true);
                encoded
            })
            .collect::<Vec<_>>();
        assert!(ascending.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(descending.windows(2).all(|pair| pair[0] > pair[1]));
    }

    #[test]
    fn nullable_rank_has_independent_direction_and_null_placement() {
        let values = [
            None,
            Some(b"".as_slice()),
            Some(b"a".as_slice()),
            Some(b"aa".as_slice()),
        ];
        for (descending, nulls_first, expected) in [
            (false, true, vec![0, 1, 2, 3]),
            (false, false, vec![1, 2, 3, 0]),
            (true, true, vec![0, 3, 2, 1]),
            (true, false, vec![3, 2, 1, 0]),
        ] {
            let mut encoded = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let mut rank = Vec::new();
                    push_nullable_ordered_component(&mut rank, *value, descending, nulls_first);
                    (rank, index)
                })
                .collect::<Vec<_>>();
            encoded.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(
                encoded
                    .into_iter()
                    .map(|(_, index)| index)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn every_prefix_and_complete_key_has_the_expected_boundary() {
        let partition = [0, 1];
        let order = [2, 0];
        let rank = [3];
        let row = [4, 0, 5];
        let partition_prefix = partition_prefix(&partition);
        let order_prefix = order_prefix(&partition, &order);
        let rank_prefix = rank_prefix(&partition, &order, &rank);
        let key = row_key(&partition, &order, &rank, &row);

        assert_eq!(
            key,
            [
                0,
                u8::MAX,
                1,
                0,
                0, // partition
                2,
                0,
                u8::MAX,
                0,
                0, // order
                3,
                0,
                0, // rank
                4,
                0,
                u8::MAX,
                5,
                0,
                0, // canonical row
            ]
        );

        assert!(order_prefix.starts_with(&partition_prefix));
        assert!(rank_prefix.starts_with(&order_prefix));
        assert!(key.starts_with(&rank_prefix));
        assert_eq!(
            parse_row_key(&key).unwrap(),
            ParsedIndexKey {
                partition: partition.to_vec(),
                order: order.to_vec(),
                rank: rank.to_vec(),
                row: row.to_vec(),
            }
        );
        let upper = prefix_successor(&order_prefix).unwrap();
        assert!(key < upper);
        assert!(!upper.starts_with(&order_prefix));
    }

    #[test]
    fn matchable_partition_prefix_excludes_null_order_rows() {
        let partition = b"partition";
        let prefix = matchable_partition_prefix(partition);
        let null_order = row_key(partition, &[0, 0, 0], b"", b"null");
        let matchable_order = row_key(partition, &[1, 0, 0], b"", b"value");

        assert!(!null_order.starts_with(&prefix));
        assert!(matchable_order.starts_with(&prefix));
        assert!(matchable_order < prefix_successor(&prefix).unwrap());
    }

    #[test]
    fn parser_rejects_bad_escape_shape_and_trailing_components() {
        assert_eq!(
            parse_row_key(&[]).unwrap_err(),
            IndexCodecError::TruncatedComponent
        );
        assert_eq!(
            parse_row_key(&[0, 0]).unwrap_err(),
            IndexCodecError::TruncatedComponent
        );
        assert_eq!(
            parse_row_key(&[0, 1]).unwrap_err(),
            IndexCodecError::InvalidEscape
        );

        let mut trailing = row_key(b"p", b"o", b"r", b"row");
        push_component(&mut trailing, b"extra");
        assert_eq!(
            parse_row_key(&trailing).unwrap_err(),
            IndexCodecError::TrailingBytes
        );
    }

    #[test]
    fn prefix_successor_handles_carry_and_unbounded_prefixes() {
        assert_eq!(prefix_successor(&[1, 2, 3]), Some(vec![1, 2, 4]));
        assert_eq!(prefix_successor(&[1, 2, u8::MAX]), Some(vec![1, 3]));
        assert_eq!(prefix_successor(&[u8::MAX]), None);
        assert_eq!(prefix_successor(&[]), None);
    }
}
