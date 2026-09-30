use std::borrow::Cow;

use dogpaddle_store::{CodecError, OrderedMap, PartitionKey, StoreValue};
use serde::{Deserialize, Deserializer, Serialize};

use super::EquiJoinError;

pub(super) type Rows = OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, std::num::NonZeroU64>;
pub(super) type Counts = OrderedMap<Vec<u8>, KeyCounts>;
pub(super) type MatchCounts = OrderedMap<Vec<u8>, u64>;

/// Returns the collision-free key for one row's committed qualifying-match count.
pub(super) fn actual_match_key(port: usize, row: &[u8]) -> Vec<u8> {
    let port = u8::try_from(port)
        .ok()
        .filter(|port| *port <= 1)
        .expect("a validated equi-join port is zero or one");
    let mut key = Vec::with_capacity(row.len().saturating_add(1));
    key.push(port);
    key.extend_from_slice(row);
    key
}

/// Positive distinct rows per side; a missing key represents two zero counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct KeyCounts(pub(super) [u64; 2]);

impl KeyCounts {
    pub(super) fn adjust(
        &mut self,
        port: usize,
        before: u64,
        after: u64,
    ) -> Result<(), EquiJoinError> {
        if before == 0 && after > 0 {
            self.0[port] = self.0[port]
                .checked_add(1)
                .ok_or(EquiJoinError::KeyCountOverflow)?;
        } else if before > 0 && after == 0 {
            self.0[port] = self.0[port]
                .checked_sub(1)
                .ok_or(EquiJoinError::KeyCountUnderflow)?;
        }
        Ok(())
    }

    pub(super) const fn is_empty(self) -> bool {
        self.0[0] == 0 && self.0[1] == 0
    }
}

impl StoreValue for KeyCounts {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.is_empty() {
            return Err(CodecError::new(
                "empty equi-join key counts must be removed",
            ));
        }
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.0[0].to_be_bytes());
        bytes[8..].copy_from_slice(&self.0[1].to_be_bytes());
        Ok(bytes)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let encoded: [u8; 16] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid equi-join key counts length"))?;
        let counts = Self([
            u64::from_be_bytes(encoded[..8].try_into().expect("the slice has eight bytes")),
            u64::from_be_bytes(encoded[8..].try_into().expect("the slice has eight bytes")),
        ]);
        if counts.is_empty() {
            return Err(CodecError::new("empty equi-join key counts must be absent"));
        }
        Ok(counts)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct JoinCursor {
    pub(super) found_match: bool,
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) resume_after: Option<Vec<u8>>,
}
fn decode_optional_bytes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<&[u8]>::deserialize(decoder).map(|value| value.map(ToOwned::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_counts_codec_is_fixed_width_and_empty_counts_are_absent() {
        let counts = KeyCounts([7, u64::MAX]);
        let encoded = counts.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [7_u64.to_be_bytes(), u64::MAX.to_be_bytes()].concat()
        );
        assert_eq!(
            KeyCounts::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            counts
        );
        for length in 0..16 {
            assert!(KeyCounts::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(KeyCounts::decode_value(Cow::Borrowed(&trailing)).is_err());
        assert!(KeyCounts::decode_value(Cow::Borrowed(&[0; 16])).is_err());
        assert!(KeyCounts::default().encode_value().is_err());
    }

    #[test]
    fn key_counts_track_distinct_membership_without_summing_weights() {
        let mut counts = KeyCounts::default();
        counts.adjust(0, 0, u64::MAX).unwrap();
        counts.adjust(0, 0, u64::MAX).unwrap();
        counts.adjust(1, 0, 1).unwrap();
        assert_eq!(counts, KeyCounts([2, 1]));
        counts.adjust(0, u64::MAX, 1).unwrap();
        assert_eq!(counts, KeyCounts([2, 1]));
        counts.adjust(0, 1, 0).unwrap();
        counts.adjust(0, u64::MAX, 0).unwrap();
        counts.adjust(1, 1, 0).unwrap();
        assert!(counts.is_empty());
        assert!(matches!(
            counts.adjust(0, 1, 0),
            Err(EquiJoinError::KeyCountUnderflow)
        ));
        assert!(matches!(
            KeyCounts([u64::MAX, 0]).adjust(0, 0, 1),
            Err(EquiJoinError::KeyCountOverflow)
        ));
    }

    #[test]
    fn match_count_keys_separate_ports_and_preserve_the_canonical_row() {
        assert_eq!(actual_match_key(0, &[]), [0]);
        assert_eq!(actual_match_key(1, &[0, 1, 2]), [1, 0, 1, 2]);
        assert_ne!(actual_match_key(0, &[1, 2]), actual_match_key(1, &[1, 2]));
    }
}
