use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, PartitionedMultiset, StoreValue};

pub(super) type Rows = PartitionedMultiset<Vec<u8>, Vec<u8>>;
pub(super) type Continuation = Cell<JoinContinuation>;

const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Probe,
    Emit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JoinContinuation {
    pub(super) port: u8,
    pub(super) phase: Phase,
    pub(super) row: u64,
    pub(super) resume_after: Option<Vec<u8>>,
}

impl StoreValue for JoinContinuation {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = Vec::new();
        encoded.push(VERSION);
        encoded.push(self.port);
        encoded.push(match self.phase {
            Phase::Probe => 0,
            Phase::Emit => 1,
        });
        encoded.extend_from_slice(&self.row.to_be_bytes());
        match &self.resume_after {
            None => encoded.push(0),
            Some(resume) => {
                encoded.push(1);
                let length = u64::try_from(resume.len())
                    .map_err(|_| CodecError::new("inner join continuation key is too long"))?;
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
                "unsupported inner join continuation version",
            ));
        }
        let port = cursor.u8()?;
        if port > 1 {
            return Err(CodecError::new("inner join continuation port is invalid"));
        }
        let phase = match cursor.u8()? {
            0 => Phase::Probe,
            1 => Phase::Emit,
            _ => return Err(CodecError::new("inner join continuation phase is invalid")),
        };
        let row = cursor.u64()?;
        let resume_after = match cursor.u8()? {
            0 => None,
            1 => {
                let length = usize::try_from(cursor.u64()?).map_err(|_| {
                    CodecError::new("inner join continuation key length exceeds usize")
                })?;
                Some(cursor.bytes(length)?.to_vec())
            }
            _ => {
                return Err(CodecError::new(
                    "inner join continuation key marker is invalid",
                ));
            }
        };
        cursor.finish()?;
        Ok(Self {
            port,
            phase,
            row,
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
            .ok_or_else(|| CodecError::new("inner join continuation is truncated"))?;
        self.remaining = remaining;
        Ok(bytes)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let (bytes, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or_else(|| CodecError::new("inner join continuation is truncated"))?;
        self.remaining = remaining;
        Ok(*bytes)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new(
                "inner join continuation has trailing bytes",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_codec_is_strict_and_round_trips_empty_resume_key() {
        let state = JoinContinuation {
            port: 1,
            phase: Phase::Emit,
            row: 7,
            resume_after: Some(Vec::new()),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [
                &[1, 1, 1][..],
                &7_u64.to_be_bytes(),
                &[1],
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
        for index in [0, 1, 2, 11] {
            let mut invalid = encoded.clone();
            invalid[index] = 2;
            assert!(JoinContinuation::decode_value(Cow::Borrowed(&invalid)).is_err());
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert!(JoinContinuation::decode_value(Cow::Borrowed(&trailing)).is_err());
    }
}
