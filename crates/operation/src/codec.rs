use thiserror::Error;

use crate::{CountDefinition, OperationDefinition, SequenceSourceDefinition};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const FORMAT_VERSION: u16 = 1;
const SEQUENCE_TAG: u16 = 1;
const COUNT_TAG: u16 = 2;

/// Stable operation-definition encoding failure.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum DefinitionCodecError {
    /// The encoded definition ends before all required fields are present.
    #[error("operation definition is truncated")]
    Truncated,
    /// The encoded bytes do not begin with the `DogPaddle` operation marker.
    #[error("operation definition marker is invalid")]
    InvalidMagic,
    /// The outer operation-definition format version is unsupported.
    #[error("unsupported operation definition format version {0}")]
    UnsupportedVersion(u16),
    /// The operation variant tag is unknown to this binary.
    #[error("unknown operation definition tag {0}")]
    UnknownTag(u16),
    /// Bytes remain after decoding the selected operation variant.
    #[error("operation definition contains trailing bytes")]
    TrailingBytes,
}

impl OperationDefinition {
    /// Encodes this definition using `DogPaddle`'s stable explicit binary format.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(MAGIC.len() + 12);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        match self {
            Self::SequenceSource(definition) => {
                encoded.extend_from_slice(&SEQUENCE_TAG.to_be_bytes());
                encoded.extend_from_slice(&definition.start().to_be_bytes());
            }
            Self::Count(_) => encoded.extend_from_slice(&COUNT_TAG.to_be_bytes()),
        }
        encoded
    }

    /// Decodes one definition from `DogPaddle`'s stable explicit binary format.
    ///
    /// # Errors
    ///
    /// Returns a [`DefinitionCodecError`] for truncated, unsupported, unknown,
    /// or non-canonical input.
    pub fn decode(encoded: &[u8]) -> Result<Self, DefinitionCodecError> {
        if encoded.len() < MAGIC.len() {
            return Err(DefinitionCodecError::Truncated);
        }
        if &encoded[..MAGIC.len()] != MAGIC {
            return Err(DefinitionCodecError::InvalidMagic);
        }

        let mut cursor = Cursor::new(&encoded[MAGIC.len()..]);
        let version = cursor.read_u16()?;
        if version != FORMAT_VERSION {
            return Err(DefinitionCodecError::UnsupportedVersion(version));
        }
        let definition = match cursor.read_u16()? {
            SEQUENCE_TAG => Self::SequenceSource(SequenceSourceDefinition::new(cursor.read_u64()?)),
            COUNT_TAG => Self::Count(CountDefinition::new()),
            tag => return Err(DefinitionCodecError::UnknownTag(tag)),
        };
        if cursor.is_empty() {
            Ok(definition)
        } else {
            Err(DefinitionCodecError::TrailingBytes)
        }
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn read_u16(&mut self) -> Result<u16, DefinitionCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn read_u64(&mut self) -> Result<u64, DefinitionCodecError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], DefinitionCodecError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(DefinitionCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(*value)
    }
}
