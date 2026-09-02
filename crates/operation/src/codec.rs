use thiserror::Error;

use crate::{
    OperationDefinition,
    operation::{sink, source, transform},
};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const FORMAT_VERSION: u16 = 1;

pub(crate) type DecodeFn = fn(&[u8]) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError>;

pub(crate) const DECODERS: &[(u16, DecodeFn)] = &[
    (source::sequence::TAG, source::sequence::decode_definition),
    (transform::count::TAG, transform::count::decode_definition),
    (
        transform::project::TAG,
        transform::project::decode_definition,
    ),
    (transform::filter::TAG, transform::filter::decode_definition),
    (transform::extend::TAG, transform::extend::decode_definition),
    (sink::discard::TAG, sink::discard::decode_definition),
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
    /// A known operation variant contains a non-canonical persistent payload.
    #[error("operation definition payload is invalid: {0}")]
    InvalidPayload(&'static str),
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

    let mut cursor = PayloadCursor::new(&encoded[MAGIC.len()..]);
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

pub(crate) struct PayloadCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> PayloadCursor<'a> {
    pub(crate) const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    pub(crate) const fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    pub(crate) const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, DefinitionCodecError> {
        Ok(self.take::<1>()?[0])
    }

    pub(crate) fn read_u16(&mut self) -> Result<u16, DefinitionCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32, DefinitionCodecError> {
        Ok(u32::from_be_bytes(self.take::<4>()?))
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64, DefinitionCodecError> {
        Ok(i64::from_be_bytes(self.take::<8>()?))
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, DefinitionCodecError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    pub(crate) fn read_bytes(&mut self, length: usize) -> Result<&'a [u8], DefinitionCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(DefinitionCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), DefinitionCodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(DefinitionCodecError::TrailingBytes)
        }
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
