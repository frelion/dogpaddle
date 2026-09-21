use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, PartitionedMultiset, StoreValue};

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
        let mut cursor = Cursor::new(bytes.as_ref());
        let counts = Self([cursor.u64()?, cursor.u64()?]);
        cursor.finish()?;
        if counts.is_empty() {
            return Err(CodecError::new("empty equi-join key counts must be absent"));
        }
        Ok(counts)
    }
}

const VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JoinContinuation {
    pub(super) port: u8,
    pub(super) row: u64,
    pub(super) found_match: bool,
    pub(super) resume_after: Option<Vec<u8>>,
}

impl StoreValue for JoinContinuation {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = Vec::new();
        encoded.push(VERSION);
        encoded.push(self.port);
        encoded.extend_from_slice(&self.row.to_be_bytes());
        encoded.push(u8::from(self.found_match));
        match &self.resume_after {
            None => encoded.push(0),
            Some(resume) => {
                encoded.push(1);
                let length = u64::try_from(resume.len())
                    .map_err(|_| CodecError::new("equi-join continuation key is too long"))?;
                encoded.extend_from_slice(&length.to_be_bytes());
                encoded.extend_from_slice(resume);
            }
        }
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut cursor = Cursor::new(bytes.as_ref());
        if cursor.u8()? != VERSION {
            return Err(CodecError::new(
                "unsupported equi-join continuation version",
            ));
        }
        let port = cursor.u8()?;
        if port > 1 {
            return Err(CodecError::new("equi-join continuation port is invalid"));
        }
        let row = cursor.u64()?;
        let found_match = match cursor.u8()? {
            0 => false,
            1 => true,
            _ => {
                return Err(CodecError::new(
                    "equi-join continuation match marker is invalid",
                ));
            }
        };
        let resume_after = match cursor.u8()? {
            0 => None,
            1 => {
                let length = usize::try_from(cursor.u64()?).map_err(|_| {
                    CodecError::new("equi-join continuation key length exceeds usize")
                })?;
                Some(cursor.bytes(length)?.to_vec())
            }
            _ => {
                return Err(CodecError::new(
                    "equi-join continuation key marker is invalid",
                ));
            }
        };
        cursor.finish()?;
        Ok(Self {
            port,
            row,
            found_match,
            resume_after,
        })
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take::<1>()?[0])
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.take()?))
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let (bytes, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or_else(|| CodecError::new("equi-join continuation is truncated"))?;
        self.remaining = remaining;
        Ok(bytes)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let (bytes, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or_else(|| CodecError::new("equi-join continuation is truncated"))?;
        self.remaining = remaining;
        Ok(*bytes)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new("equi-join continuation has trailing bytes"))
        }
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
        assert_eq!(
            encoded,
            [
                &[1, 1][..],
                &7_u64.to_be_bytes(),
                &[1, 1],
                &0_u64.to_be_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            JoinContinuation::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            state
        );
        for length in 0..encoded.len() {
            assert!(JoinContinuation::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
        }
        for index in [0, 1, 10, 11] {
            let mut invalid = encoded.clone();
            invalid[index] = u8::MAX;
            assert!(JoinContinuation::decode_value(Cow::Borrowed(&invalid)).is_err());
        }
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
