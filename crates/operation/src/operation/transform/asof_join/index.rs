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

/// Prefix containing every exact row at one order value.
pub(super) fn order_prefix(partition: &[u8], order: &[u8]) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    push_component(&mut key, order);
    key
}

/// Builds one exact row key in partition/order/canonical-row order.
pub(super) fn row_key(partition: &[u8], order: &[u8], row: &[u8]) -> Vec<u8> {
    let mut key = order_prefix(partition, order);
    push_component(&mut key, row);
    key
}

/// Strictly parses a complete key produced by [`row_key`].
pub(super) fn parse_row_key(encoded: &[u8]) -> Result<ParsedIndexKey, IndexCodecError> {
    let mut remaining = encoded;
    let partition = take_component(&mut remaining)?;
    let order = take_component(&mut remaining)?;
    let row = take_component(&mut remaining)?;
    if !remaining.is_empty() {
        return Err(IndexCodecError::TrailingBytes);
    }
    Ok(ParsedIndexKey {
        partition,
        order,
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
    fn current_layout_contains_exactly_partition_order_and_row() {
        let key = row_key(b"p\0", b"o", b"r\0");
        assert_eq!(key, b"p\0\xff\0\0o\0\0r\0\xff\0\0");
        let decoded = parse_row_key(&key).unwrap();
        assert_eq!(decoded.partition, b"p\0");
        assert_eq!(decoded.order, b"o");
        assert_eq!(decoded.row, b"r\0");
        for length in 0..key.len() {
            assert!(parse_row_key(&key[..length]).is_err());
        }
        let mut extra = key;
        extra.push(1);
        assert!(parse_row_key(&extra).is_err());
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
    }
}
