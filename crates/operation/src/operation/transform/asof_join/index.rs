//! Collision-free framing for the ordered ASOF row indexes.

use thiserror::Error;

const ESCAPE: u8 = 0;
const ESCAPED_ZERO: u8 = u8::MAX;
const TERMINATOR: u8 = 0;

/// Decoded equality/order headers and the borrowed canonical row suffix.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct ParsedIndexKey<'a> {
    pub(super) partition: Vec<u8>,
    pub(super) order: Vec<u8>,
    pub(super) row: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum IndexCodecError {
    #[error("ASOF index component is truncated")]
    TruncatedComponent,
    #[error("ASOF index component escape is invalid")]
    InvalidEscape,
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

/// Prefix containing every exact row at one order value.
pub(super) fn order_prefix(partition: &[u8], order: &[u8]) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    push_component(&mut key, order);
    key
}

/// Builds one exact row key in partition/order/canonical-row order.
pub(super) fn row_key(partition: &[u8], order: &[u8], row: &[u8]) -> Vec<u8> {
    let mut key = order_prefix(partition, order);
    key.extend_from_slice(row);
    key
}

/// Strictly parses the two framed headers and borrows the remaining row bytes.
/// Canonical row validation belongs to the schema-bound row decoder.
pub(super) fn parse_row_key(encoded: &[u8]) -> Result<ParsedIndexKey<'_>, IndexCodecError> {
    let mut remaining = encoded;
    let partition = take_component(&mut remaining)?;
    let order = take_component(&mut remaining)?;
    Ok(ParsedIndexKey {
        partition,
        order,
        row: remaining,
    })
}

/// Validates both headers without materializing them and borrows the row suffix.
pub(super) fn row_suffix(encoded: &[u8]) -> Result<&[u8], IndexCodecError> {
    let mut remaining = encoded;
    consume_component(&mut remaining, |_| {})?;
    consume_component(&mut remaining, |_| {})?;
    Ok(remaining)
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

fn take_component(remaining: &mut &[u8]) -> Result<Vec<u8>, IndexCodecError> {
    let mut decoded = Vec::new();
    consume_component(remaining, |byte| decoded.push(byte))?;
    Ok(decoded)
}

fn consume_component(
    remaining: &mut &[u8],
    mut decoded: impl FnMut(u8),
) -> Result<(), IndexCodecError> {
    loop {
        let (&byte, rest) = remaining
            .split_first()
            .ok_or(IndexCodecError::TruncatedComponent)?;
        *remaining = rest;
        if byte != ESCAPE {
            decoded(byte);
            continue;
        }
        let (&marker, rest) = remaining
            .split_first()
            .ok_or(IndexCodecError::TruncatedComponent)?;
        *remaining = rest;
        match marker {
            TERMINATOR => return Ok(()),
            ESCAPED_ZERO => decoded(0),
            _ => return Err(IndexCodecError::InvalidEscape),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_layout_contains_exactly_partition_order_and_row() {
        let key = row_key(b"p\0", b"o", b"r\0");
        assert_eq!(key, b"p\0\xff\0\0o\0\0r\0");
        let decoded = parse_row_key(&key).unwrap();
        assert_eq!(decoded.partition, b"p\0");
        assert_eq!(decoded.order, b"o");
        assert_eq!(decoded.row, b"r\0");
        assert_eq!(row_suffix(&key).unwrap(), decoded.row);
        let header = order_prefix(b"p\0", b"o");
        for length in 0..header.len() {
            assert!(parse_row_key(&key[..length]).is_err());
            assert!(row_suffix(&key[..length]).is_err());
        }
        assert!(parse_row_key(&header).unwrap().row.is_empty());
        assert_eq!(parse_row_key(&[0, 1]), Err(IndexCodecError::InvalidEscape));
        assert_eq!(row_suffix(&[0, 1]), Err(IndexCodecError::InvalidEscape));
        assert_eq!(
            parse_row_key(&[0, 0, 0, 1]),
            Err(IndexCodecError::InvalidEscape)
        );
        assert_eq!(
            row_suffix(&[0, 0, 0, 1]),
            Err(IndexCodecError::InvalidEscape)
        );
    }
    #[test]
    fn framing_and_prefix_successors_preserve_index_order() {
        let components = [b"".as_slice(), b"\0", b"\0\0", b"a", b"aa", b"b", b"\xff"];
        let keys = components
            .iter()
            .map(|value| row_key(b"p", value, b"row"))
            .collect::<Vec<_>>();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for value in components {
            let prefix = order_prefix(b"p", value);
            let key = row_key(b"p", value, b"row");
            assert!(key >= prefix);
            assert!(key < prefix_successor(&prefix).unwrap());
        }
        let rows = components
            .iter()
            .map(|row| row_key(b"p", b"o", row))
            .collect::<Vec<_>>();
        assert_eq!(rows[0], order_prefix(b"p", b"o"));
        assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
