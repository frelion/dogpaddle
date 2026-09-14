//! Persistent ASOF row multiplicities and replay continuation.

use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, StoreValue};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Probe,
    Emit,
}

/// Durable cursor for one input row's paged, replayable correction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AsOfContinuation {
    pub(super) port: u8,
    pub(super) phase: Phase,
    pub(super) row: u64,
    /// On port 1, the current left key while `candidate_resume_after` is set,
    /// otherwise the last completely processed left key. Always absent on port 0.
    pub(super) left_resume_after: Option<Vec<u8>>,
    /// Last right candidate consumed while selecting the current left row's winner.
    pub(super) candidate_resume_after: Option<Vec<u8>>,
    /// Best committed-state right index key seen for the current left row.
    pub(super) best_before: Option<Vec<u8>>,
    /// Best event-overlay right index key seen for the current left row.
    pub(super) best_after: Option<Vec<u8>>,
    /// Whether another committed-state row tied `best_before` under Reject fallback.
    pub(super) ambiguous_before: bool,
    /// Whether another event-overlay row tied `best_after` under Reject fallback.
    pub(super) ambiguous_after: bool,
}

const VERSION: u8 = 1;

impl StoreValue for AsOfContinuation {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.port > 1 {
            return Err(CodecError::new("ASOF continuation port is invalid"));
        }
        let mut encoded = Vec::new();
        encoded.push(VERSION);
        encoded.push(self.port);
        encoded.push(match self.phase {
            Phase::Probe => 0,
            Phase::Emit => 1,
        });
        encoded.extend_from_slice(&self.row.to_be_bytes());
        put_optional(&mut encoded, self.left_resume_after.as_deref())?;
        put_optional(&mut encoded, self.candidate_resume_after.as_deref())?;
        put_optional(&mut encoded, self.best_before.as_deref())?;
        put_optional(&mut encoded, self.best_after.as_deref())?;
        encoded.push(u8::from(self.ambiguous_before));
        encoded.push(u8::from(self.ambiguous_after));
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut cursor = Cursor::new(bytes.as_ref());
        if cursor.u8()? != VERSION {
            return Err(CodecError::new("unsupported ASOF continuation version"));
        }
        let port = cursor.u8()?;
        if port > 1 {
            return Err(CodecError::new("ASOF continuation port is invalid"));
        }
        let phase = match cursor.u8()? {
            0 => Phase::Probe,
            1 => Phase::Emit,
            _ => return Err(CodecError::new("ASOF continuation phase is invalid")),
        };
        let row = cursor.u64()?;
        let left_resume_after = cursor.optional()?;
        let candidate_resume_after = cursor.optional()?;
        let best_before = cursor.optional()?;
        let best_after = cursor.optional()?;
        let ambiguous_before = cursor.boolean("ASOF continuation before ambiguity is invalid")?;
        let ambiguous_after = cursor.boolean("ASOF continuation after ambiguity is invalid")?;
        cursor.finish()?;
        Ok(Self {
            port,
            phase,
            row,
            left_resume_after,
            candidate_resume_after,
            best_before,
            best_after,
            ambiguous_before,
            ambiguous_after,
        })
    }
}

fn put_optional(output: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), CodecError> {
    match value {
        None => output.push(0),
        Some(bytes) => {
            output.push(1);
            let length = u64::try_from(bytes.len())
                .map_err(|_| CodecError::new("ASOF continuation component is too long"))?;
            output.extend_from_slice(&length.to_be_bytes());
            output.extend_from_slice(bytes);
        }
    }
    Ok(())
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

    fn optional(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let length = usize::try_from(self.u64()?).map_err(|_| {
                    CodecError::new("ASOF continuation component length exceeds usize")
                })?;
                Ok(Some(self.bytes(length)?.to_vec()))
            }
            _ => Err(CodecError::new(
                "ASOF continuation optional marker is invalid",
            )),
        }
    }

    fn boolean(&mut self, message: &'static str) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError::new(message)),
        }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let (bytes, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or_else(|| CodecError::new("ASOF continuation is truncated"))?;
        self.remaining = remaining;
        Ok(bytes)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let (bytes, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or_else(|| CodecError::new("ASOF continuation is truncated"))?;
        self.remaining = remaining;
        Ok(*bytes)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new("ASOF continuation has trailing bytes"))
        }
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
            phase: Phase::Emit,
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
                1, 1, 1, // version, port, Emit
                0, 0, 0, 0, 0, 0, 0, 7, // row
                1, 0, 0, 0, 0, 0, 0, 0, 0, // present empty left cursor
                0, // absent candidate cursor
                1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, // before key
                1, 0, 0, 0, 0, 0, 0, 0, 1, 2, // after key
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
        for index in [0, 1, 2, 11, encoded.len() - 2, encoded.len() - 1] {
            let mut invalid = encoded.clone();
            invalid[index] = u8::MAX;
            assert!(AsOfContinuation::decode_value(Cow::Borrowed(&invalid)).is_err());
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(AsOfContinuation::decode_value(Cow::Borrowed(&trailing)).is_err());
    }

    #[test]
    fn continuation_encoder_rejects_invalid_port() {
        let continuation = AsOfContinuation {
            port: 2,
            phase: Phase::Probe,
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
