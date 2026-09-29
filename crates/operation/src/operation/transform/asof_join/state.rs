//! Persistent ASOF row multiplicities and replay continuation.

use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, StoreValue};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

pub(super) type Rows = OrderedMap<Vec<u8>, RowWeight>;
pub(super) type Continuation = Cell<AsOfContinuation>;

/// Positive multiplicity of one exact indexed row; absence represents zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RowWeight(u64);

impl RowWeight {
    pub(super) const fn new(weight: u64) -> Option<Self> {
        if weight == 0 {
            None
        } else {
            Some(Self(weight))
        }
    }

    pub(super) const fn get(self) -> u64 {
        self.0
    }

    pub(super) fn adjusted(
        current: Option<Self>,
        difference: i64,
    ) -> Result<Option<Self>, RowWeightError> {
        let before = current.map_or(0, Self::get);
        let after = if difference >= 0 {
            before
                .checked_add(difference.unsigned_abs())
                .ok_or(RowWeightError::Overflow)?
        } else {
            before
                .checked_sub(difference.unsigned_abs())
                .ok_or(RowWeightError::Negative)?
        };
        Ok(Self::new(after))
    }
}

impl StoreValue for RowWeight {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.0 == 0 {
            return Err(CodecError::new("zero ASOF row weight must be absent"));
        }
        Ok(self.0.to_be_bytes())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let encoded: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid ASOF row weight length"))?;
        Self::new(u64::from_be_bytes(encoded))
            .ok_or_else(|| CodecError::new("zero ASOF row weight must be absent"))
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum RowWeightError {
    #[error("ASOF row weight would become negative")]
    Negative,
    #[error("ASOF row weight overflow")]
    Overflow,
}

/// Durable cursor for one input row's paged, replayable correction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct AsOfContinuation {
    pub(super) port: u8,
    pub(super) row: u64,
    /// On port 1, the current left key while `candidate_resume_after` is set,
    /// otherwise the last completely processed left key. Always absent on port 0.
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) left_resume_after: Option<Vec<u8>>,
    /// Last right candidate consumed while selecting the current left row's winner.
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) candidate_resume_after: Option<Vec<u8>>,
    /// Best committed-state right index key seen for the current left row.
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) best_before: Option<Vec<u8>>,
    /// Best event-overlay right index key seen for the current left row.
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) best_after: Option<Vec<u8>>,
    /// Whether another committed-state row tied `best_before` under Reject fallback.
    pub(super) ambiguous_before: bool,
    /// Whether another event-overlay row tied `best_after` under Reject fallback.
    pub(super) ambiguous_after: bool,
}

fn decode_optional_bytes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<&[u8]>::deserialize(decoder).map(|value| value.map(ToOwned::to_owned))
}

const VERSION: u8 = 1;

impl StoreValue for AsOfContinuation {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.port > 1 {
            return Err(CodecError::new("ASOF continuation port is invalid"));
        }
        bincode::serde::encode_to_vec(
            (VERSION, self),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("ASOF continuation cannot be encoded"))
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.first() != Some(&VERSION) {
            return Err(CodecError::new("unsupported ASOF continuation version"));
        }
        if bytes.get(1).is_none_or(|port| *port > 1) {
            return Err(CodecError::new("ASOF continuation port is invalid"));
        }
        let ((_, state), consumed): ((u8, Self), usize) = bincode::serde::borrow_decode_from_slice(
            bytes.as_ref(),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("ASOF continuation is invalid"))?;
        if consumed != bytes.len() {
            return Err(CodecError::new("ASOF continuation has trailing bytes"));
        }
        if state.encode_value()?.as_ref() != bytes.as_ref() {
            return Err(CodecError::new("ASOF continuation is non-canonical"));
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_weight_is_positive_fixed_width_and_checked() {
        let weight = RowWeight::new(u64::MAX).unwrap();
        let encoded = weight.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded, u64::MAX.to_be_bytes());
        assert_eq!(
            RowWeight::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            weight
        );
        assert!(RowWeight(0).encode_value().is_err());
        assert!(RowWeight::decode_value(Cow::Borrowed(&[0; 8])).is_err());
        assert!(RowWeight::decode_value(Cow::Borrowed(&encoded[..7])).is_err());
        assert_eq!(
            RowWeight::adjusted(None, -1).unwrap_err(),
            RowWeightError::Negative
        );
        assert_eq!(
            RowWeight::adjusted(Some(weight), 1).unwrap_err(),
            RowWeightError::Overflow
        );
        assert_eq!(RowWeight::adjusted(RowWeight::new(7), -7).unwrap(), None);
    }

    #[test]
    fn continuation_codec_is_strict_and_distinguishes_absent_from_empty() {
        let continuation = AsOfContinuation {
            port: 1,
            row: 7,
            left_resume_after: Some(Vec::new()),
            candidate_resume_after: None,
            best_before: Some(vec![0, 1]),
            best_after: Some(vec![2]),
            ambiguous_before: false,
            ambiguous_after: true,
        };
        let encoded = continuation.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [
                1, 1, // version, port
                7, // row
                1, 0, // present empty left cursor
                0, // absent candidate cursor
                1, 2, 0, 1, // before key
                1, 1, 2, // after key
                0, 1, // ambiguity markers
            ]
        );
        assert_eq!(
            AsOfContinuation::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            continuation
        );
        for length in 0..encoded.len() {
            assert!(
                AsOfContinuation::decode_value(Cow::Borrowed(&encoded[..length])).is_err(),
                "prefix length {length} was accepted"
            );
        }
        for index in [0, 1, 3, encoded.len() - 2, encoded.len() - 1] {
            let mut invalid = encoded.clone();
            invalid[index] = u8::MAX;
            assert!(AsOfContinuation::decode_value(Cow::Borrowed(&invalid)).is_err());
        }
        let mut overlong_row = vec![1, 1, 251, 0, 7];
        overlong_row.extend_from_slice(&encoded[3..]);
        assert!(AsOfContinuation::decode_value(Cow::Borrowed(&overlong_row)).is_err());
        let mut huge_left_key = vec![1, 1, 7, 1, 253];
        huge_left_key.extend_from_slice(&[255; 8]);
        assert!(AsOfContinuation::decode_value(Cow::Borrowed(&huge_left_key)).is_err());
        let mut invalid_version = encoded.clone();
        invalid_version[0] = u8::MAX;
        assert!(AsOfContinuation::decode_value(Cow::Borrowed(&invalid_version)).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(AsOfContinuation::decode_value(Cow::Borrowed(&trailing)).is_err());
    }

    #[test]
    fn continuation_encoder_rejects_invalid_port() {
        let continuation = AsOfContinuation {
            port: 2,
            row: 0,
            left_resume_after: None,
            candidate_resume_after: None,
            best_before: None,
            best_after: None,
            ambiguous_before: false,
            ambiguous_after: false,
        };
        assert!(continuation.encode_value().is_err());
    }
}
