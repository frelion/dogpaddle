use thiserror::Error;

use crate::{
    InlineDefinition, InlineEligibilityError, OperationDefinition,
    operation::{scan, sink, transform},
};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const FORMAT_VERSION: u16 = 1;

pub(crate) type DecodeFn = fn(&[u8]) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError>;
pub(crate) type InlineDecodeFn = fn(&[u8]) -> Result<InlineDefinition, DefinitionCodecError>;

pub(crate) const DECODERS: &[(u16, DecodeFn)] = &[
    (scan::mysql_cdc::TAG, scan::mysql_cdc::decode_definition),
    (
        scan::postgres_cdc::TAG,
        scan::postgres_cdc::decode_definition,
    ),
    (scan::sequence::TAG, scan::sequence::decode_definition),
    (
        transform::aggregate::TAG,
        transform::aggregate::decode_definition,
    ),
    (
        transform::distinct::TAG,
        transform::distinct::decode_definition,
    ),
    (
        transform::running_event_count::TAG,
        transform::running_event_count::decode_definition,
    ),
    (
        transform::project::TAG,
        transform::project::decode_definition,
    ),
    (transform::filter::TAG, transform::filter::decode_definition),
    (transform::extend::TAG, transform::extend::decode_definition),
    (transform::select::TAG, transform::select::decode_definition),
    (
        transform::union_all::TAG,
        transform::union_all::decode_definition,
    ),
    (
        transform::schema_align::TAG,
        transform::schema_align::decode_definition,
    ),
    (sink::discard::TAG, sink::discard::decode_definition),
    (sink::postgres::TAG, sink::postgres::decode_definition),
    (sink::sqlite::TAG, sink::sqlite::decode_definition),
];

pub(crate) const INLINE_DECODERS: &[(u16, InlineDecodeFn)] = &[
    (
        transform::project::TAG,
        transform::project::decode_inline_definition,
    ),
    (
        transform::filter::TAG,
        transform::filter::decode_inline_definition,
    ),
    (
        transform::extend::TAG,
        transform::extend::decode_inline_definition,
    ),
    (
        transform::select::TAG,
        transform::select::decode_inline_definition,
    ),
    (
        transform::schema_align::TAG,
        transform::schema_align::decode_inline_definition,
    ),
];

/// Versioned operation-definition encoding failure.
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
    /// The tag names a known Operation that does not implement inline execution.
    #[error("operation definition tag {0} is not inline-capable")]
    NotInlineCapable(u16),
    /// The payload is valid for a normal Operation but unsafe to replay inline.
    #[error("operation definition is not eligible for inline execution: {0}")]
    InlineIneligible(#[source] InlineEligibilityError),
    /// A known operation variant contains a non-canonical persistent payload.
    #[error("operation definition payload is invalid: {0}")]
    InvalidPayload(&'static str),
    /// Bytes remain after decoding the selected operation variant.
    #[error("operation definition contains trailing bytes")]
    TrailingBytes,
}

/// Encodes a definition using `DogPaddle`'s versioned binary format.
///
/// Some operation payloads contain bytes owned by an exactly pinned upstream
/// codec. Such bytes are part of this outer format version and may require a
/// version bump when that dependency changes.
#[must_use]
pub fn encode_definition(definition: &dyn OperationDefinition) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(MAGIC.len() + 12);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&definition.persistence_tag().to_be_bytes());
    definition.encode_payload(&mut encoded);
    encoded
}

/// Encodes an inline definition using the normal Operation tag and payload.
///
/// The result is byte-for-byte identical to encoding the originating concrete
/// definition with [`encode_definition`]. The separate entrypoint preserves the
/// inline capability after type erasure.
#[must_use]
pub fn encode_inline_definition(definition: &InlineDefinition) -> Vec<u8> {
    encode_definition(definition.as_operation_definition())
}

/// Decodes one definition from `DogPaddle`'s versioned binary format.
///
/// # Errors
///
/// Returns a [`DefinitionCodecError`] for truncated, unsupported, unknown, or
/// non-canonical input.
pub fn decode_definition(
    encoded: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let (tag, payload) = decode_header(encoded)?;
    let decoder = DECODERS
        .iter()
        .find_map(|(registered, decoder)| (*registered == tag).then_some(*decoder))
        .ok_or(DefinitionCodecError::UnknownTag(tag))?;
    decoder(payload)
}

/// Decodes and validates one inline-capable Operation definition.
///
/// # Errors
///
/// Returns [`DefinitionCodecError::NotInlineCapable`] for a known normal
/// Operation without the sealed inline capability, and
/// [`DefinitionCodecError::InlineIneligible`] when the concrete payload uses a
/// non-replay-safe expression.
pub fn decode_inline_definition(encoded: &[u8]) -> Result<InlineDefinition, DefinitionCodecError> {
    let (tag, payload) = decode_header(encoded)?;
    let decoder = INLINE_DECODERS
        .iter()
        .find_map(|(registered, decoder)| (*registered == tag).then_some(*decoder));
    match decoder {
        Some(decoder) => decoder(payload),
        None if DECODERS.iter().any(|(registered, _)| *registered == tag) => {
            Err(DefinitionCodecError::NotInlineCapable(tag))
        }
        None => Err(DefinitionCodecError::UnknownTag(tag)),
    }
}

fn decode_header(encoded: &[u8]) -> Result<(u16, &[u8]), DefinitionCodecError> {
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
    Ok((tag, cursor.remaining()))
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

    pub(crate) fn read_u16(&mut self) -> Result<u16, DefinitionCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32, DefinitionCodecError> {
        Ok(u32::from_be_bytes(self.take::<4>()?))
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
