use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, PartitionedMultiset, StoreValue};
use serde::{Deserialize, Deserializer, Serialize};

use super::EquiJoinError;

pub(super) type Rows = PartitionedMultiset<Vec<u8>, Vec<u8>>;
pub(super) type Continuation = Cell<JoinContinuation>;
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

const VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct JoinContinuation {
    pub(super) port: u8,
    pub(super) row: u64,
    pub(super) found_match: bool,
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) resume_after: Option<Vec<u8>>,
}

fn decode_optional_bytes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<&[u8]>::deserialize(decoder).map(|value| value.map(ToOwned::to_owned))
}

impl StoreValue for JoinContinuation {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.port > 1 {
            return Err(CodecError::new("equi-join continuation port is invalid"));
        }
        bincode::serde::encode_to_vec(
            (VERSION, self),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("equi-join continuation cannot be encoded"))
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.first() != Some(&VERSION) {
            return Err(CodecError::new(
                "unsupported equi-join continuation version",
            ));
        }
        if bytes.get(1).is_none_or(|port| *port > 1) {
            return Err(CodecError::new("equi-join continuation port is invalid"));
        }
        let ((_, state), consumed): ((u8, Self), usize) = bincode::serde::borrow_decode_from_slice(
            bytes.as_ref(),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("equi-join continuation is invalid"))?;
        if consumed != bytes.len() {
            return Err(CodecError::new("equi-join continuation has trailing bytes"));
        }
        if state.encode_value()?.as_ref() != bytes.as_ref() {
            return Err(CodecError::new("equi-join continuation is non-canonical"));
        }
        Ok(state)
    }
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
    fn continuation_codec_is_strict_and_round_trips_empty_resume_key() {
        let state = JoinContinuation {
            port: 1,
            row: 7,
            found_match: true,
            resume_after: Some(Vec::new()),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded, [1, 1, 7, 1, 1, 0]);
        assert_eq!(
            JoinContinuation::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            state
        );
        for length in 0..encoded.len() {
            assert!(JoinContinuation::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
        }
        for index in [0, 1, 3, 4] {
            let mut invalid = encoded.clone();
            invalid[index] = u8::MAX;
            assert!(JoinContinuation::decode_value(Cow::Borrowed(&invalid)).is_err());
        }
        let mut overlong_row = vec![1, 1, 251, 0, 7];
        overlong_row.extend_from_slice(&encoded[3..]);
        assert!(JoinContinuation::decode_value(Cow::Borrowed(&overlong_row)).is_err());
        let mut huge_resume_key = vec![1, 1, 7, 1, 1, 253];
        huge_resume_key.extend_from_slice(&[255; 8]);
        assert!(JoinContinuation::decode_value(Cow::Borrowed(&huge_resume_key)).is_err());
        let mut invalid_version = encoded.clone();
        invalid_version[0] = u8::MAX;
        assert!(JoinContinuation::decode_value(Cow::Borrowed(&invalid_version)).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(JoinContinuation::decode_value(Cow::Borrowed(&trailing)).is_err());
    }

    #[test]
    fn match_count_keys_separate_ports_and_preserve_the_canonical_row() {
        assert_eq!(actual_match_key(0, &[]), [0]);
        assert_eq!(actual_match_key(1, &[0, 1, 2]), [1, 0, 1, 2]);
        assert_ne!(actual_match_key(0, &[1, 2]), actual_match_key(1, &[1, 2]));
    }
}
