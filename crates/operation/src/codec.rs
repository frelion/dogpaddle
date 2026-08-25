use thiserror::Error;

use crate::{
    OperationDefinition,
    operation::{source, transform},
};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const FORMAT_VERSION: u16 = 1;

pub(crate) type DecodeFn = fn(&[u8]) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError>;

pub(crate) const DECODERS: &[(u16, DecodeFn)] = &[
    (source::TAG, source::decode_definition),
    (transform::TAG, transform::decode_definition),
];

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

/// Encodes a definition using `DogPaddle`'s stable explicit binary format.
#[must_use]
pub fn encode_definition(definition: &dyn OperationDefinition) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(MAGIC.len() + 12);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&definition.persistence_tag().to_be_bytes());
    definition.encode_payload(&mut encoded);
    encoded
}

/// Decodes one definition from `DogPaddle`'s stable explicit binary format.
///
/// # Errors
///
/// Returns a [`DefinitionCodecError`] for truncated, unsupported, unknown, or
/// non-canonical input.
pub fn decode_definition(
    encoded: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
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
    let tag = cursor.read_u16()?;
    let decoder = DECODERS
        .iter()
        .find_map(|(registered, decoder)| (*registered == tag).then_some(*decoder))
        .ok_or(DefinitionCodecError::UnknownTag(tag))?;
    decoder(cursor.remaining())
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    const fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    fn read_u16(&mut self) -> Result<u16, DefinitionCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
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
