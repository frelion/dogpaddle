//! Encoding and decoding for `DogPaddle` Change entries.

use arrow_schema::ArrowError;
use thiserror::Error;

use crate::{change::ChangeError, schema::SchemaError};

mod batch;
mod bound;
mod size;
mod stream;

pub use bound::SchemaBoundChangeCodec;

#[cfg(test)]
mod tests;

/// A Change encoding or decoding failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The logical Schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// The decoded change violates its logical invariants.
    #[error(transparent)]
    Change(#[from] ChangeError),
    /// The current target cannot safely interpret v1 Arrow IPC buffers.
    #[error("DogPaddle Change v1 only supports little-endian targets")]
    UnsupportedTargetEndianness,
    /// The encoded Change exceeds a caller-provided byte limit.
    #[error("encoded DogPaddle Change exceeds the {max_bytes}-byte limit")]
    EncodedSizeLimitExceeded {
        /// Maximum accepted uncompressed body and complete entry length.
        max_bytes: usize,
    },
    /// A Change or encoded entry has a different Schema from its codec.
    #[error("the Change schema differs from the schema bound to this codec")]
    SchemaMismatch,
    /// The encoding is not a canonical `DogPaddle` Change entry.
    #[error("invalid DogPaddle Change encoding: {message}")]
    InvalidEncoding {
        /// Diagnostic reason.
        message: String,
    },
    /// Arrow IPC rejected the Schema or record batch.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

impl CodecError {
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidEncoding {
            message: message.into(),
        }
    }

    const fn size_limit(max_bytes: usize) -> Self {
        Self::EncodedSizeLimitExceeded { max_bytes }
    }
}

fn ensure_little_endian_target() -> Result<(), CodecError> {
    if cfg!(target_endian = "little") {
        Ok(())
    } else {
        Err(CodecError::UnsupportedTargetEndianness)
    }
}
