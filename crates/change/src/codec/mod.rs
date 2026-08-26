//! Encoding and decoding for self-contained `DogPaddle` Change streams.

use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow_schema::ArrowError;
use thiserror::Error;

use crate::{
    change::{Change, ChangeError},
    projection::{ChangeProjection, ProjectionError},
    schema::SchemaError,
};

mod batch;
mod stream;

#[cfg(test)]
mod tests;

/// Encodes one self-contained change as a standard Arrow IPC stream.
///
/// The stream contains one physical Schema, exactly one `RecordBatch`, and the
/// canonical end-of-stream marker. The physical Schema prepends the non-null
/// `$dogpaddle.diff` `Int64` field and carries `DogPaddle`'s kind and format
/// version markers in Arrow Schema metadata. Record rows and their paired diffs
/// are written in their original event order without sorting or consolidation.
///
/// # Errors
///
/// Returns [`CodecError`] when the current target is not little-endian or Arrow
/// cannot encode the physical batch.
pub fn encode_change(change: &Change) -> Result<Vec<u8>, CodecError> {
    ensure_little_endian_target()?;
    stream::encode(change)
}

/// Decodes one self-contained `DogPaddle` change from an Arrow IPC stream.
///
/// The decoder requires the canonical v1 shape produced by [`encode_change`]:
/// one marked physical Schema, exactly one non-empty `RecordBatch`, and one
/// canonical end-of-stream marker with no trailing bytes. Decoding preserves
/// the encoded event order exactly.
///
/// # Errors
///
/// Returns [`CodecError`] for malformed Arrow IPC, a missing or unsupported
/// `DogPaddle` format marker, an invalid physical Schema or batch metadata,
/// non-canonical framing, invalid Arrow values, or invalid differences.
pub fn decode_change(encoded: &[u8]) -> Result<Change, CodecError> {
    decode_guarded(encoded, None)
}

/// Decodes selected top-level logical fields from one self-contained Change.
///
/// The embedded Schema, stream framing, complete `RecordBatch` metadata, and
/// every buffer descriptor are validated before body access. Only the physical
/// diff buffers and complete buffer subtrees selected by `projection` are
/// copied into owned Arrow memory and decoded. The returned value is an
/// ordinary [`Change`], not a lazy view over `encoded`.
///
/// Value-level invariants of unselected field bodies, such as UTF-8 contents or
/// nested offsets, cannot be validated without reading those bodies. Use
/// [`decode_change`] when every logical field must be fully validated.
///
/// # Errors
///
/// Returns [`CodecError`] for malformed or non-canonical Arrow IPC, an invalid
/// physical Schema, a Schema different from the one bound to `projection`,
/// malformed batch metadata, invalid selected values, or invalid differences.
pub fn decode_change_projected(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, CodecError> {
    decode_guarded(encoded, Some(projection))
}

/// A self-contained Change encoding or decoding failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The logical Schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// The decoded change violates its logical invariants.
    #[error(transparent)]
    Change(#[from] ChangeError),
    /// The requested logical projection is invalid for this Change.
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    /// The embedded `DogPaddle` Change format version is unsupported.
    #[error("unsupported DogPaddle Change format version {version:?}")]
    UnsupportedVersion {
        /// Unsupported metadata value.
        version: String,
    },
    /// The current target cannot safely interpret v1 Arrow IPC buffers.
    #[error("DogPaddle Change v1 only supports little-endian targets")]
    UnsupportedTargetEndianness,
    /// The encoding is not a canonical `DogPaddle` Change stream.
    #[error("invalid DogPaddle Change encoding: {message}")]
    InvalidEncoding {
        /// Diagnostic reason.
        message: String,
    },
    /// Arrow IPC rejected the Schema, stream, or record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

impl CodecError {
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidEncoding {
            message: message.into(),
        }
    }
}

fn decode_guarded(
    encoded: &[u8],
    projection: Option<&ChangeProjection>,
) -> Result<Change, CodecError> {
    ensure_little_endian_target()?;
    catch_unwind(AssertUnwindSafe(|| {
        let parsed = stream::parse(encoded)?;
        if let Some(projection) = projection {
            projection.require_schema(parsed.logical_schema.as_ref())?;
        }
        batch::decode(&parsed, projection)
    }))
    .map_err(|_| CodecError::invalid("Arrow IPC decoding panicked"))?
}

fn ensure_little_endian_target() -> Result<(), CodecError> {
    if cfg!(target_endian = "little") {
        Ok(())
    } else {
        Err(CodecError::UnsupportedTargetEndianness)
    }
}
