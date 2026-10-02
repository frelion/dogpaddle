use std::io::{self, Write};

use super::{
    definition::FlowDefinition,
    validate::{self, TopologyError},
};
use thiserror::Error;

const MAGIC: &[u8] = b"dogpaddle.flow\0";
const FORMAT_VERSION: u16 = 1;
pub(super) const CHECKSUM_LENGTH: usize = size_of::<u32>();
pub(crate) const DEFINITION_DATA_NAME: &str = "flow/definition";
pub(super) const MAX_DEFINITION_BYTES: usize = 8 * 1024 * 1024;

/// Failure while encoding or decoding the sole logical DAG.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowDefinitionError {
    /// A declared field is incomplete.
    #[error("flow definition is truncated")]
    Truncated,
    /// The format marker is invalid.
    #[error("flow definition marker is invalid")]
    InvalidMagic,
    /// The development format version is unsupported.
    #[error("unsupported flow definition version {0}")]
    UnsupportedVersion(u16),
    /// A field exceeds the bounded format.
    #[error("{0} exceeds the flow definition limit")]
    LengthOverflow(&'static str),
    /// JSON failed without retaining or echoing any input values.
    #[error("flow definition JSON has {reason} at line {line} column {column}")]
    InvalidJson {
        /// Static category without input text.
        reason: &'static str,
        /// JSON line number.
        line: usize,
        /// JSON column number.
        column: usize,
    },
    /// JSON is valid but not the unique encoding of this plan.
    #[error("flow definition JSON is not canonical")]
    NonCanonical,
    /// The integrity checksum does not match.
    #[error("flow definition checksum does not match")]
    IntegrityMismatch,
    /// The graph is invalid.
    #[error(transparent)]
    Topology(#[from] TopologyError),
}

pub(crate) fn operation_prefix(index: usize) -> String {
    format!("operation/{index:08x}")
}

pub(crate) fn encode(definition: &FlowDefinition) -> Result<Vec<u8>, FlowDefinitionError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    serde_json::to_writer(&mut encoded, definition).map_err(|error| json_error(&error))?;
    if encoded.len() > MAX_DEFINITION_BYTES - CHECKSUM_LENGTH {
        return Err(FlowDefinitionError::LengthOverflow("definition"));
    }
    encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode(encoded: &[u8]) -> Result<FlowDefinition, FlowDefinitionError> {
    if encoded.len() > MAX_DEFINITION_BYTES {
        return Err(FlowDefinitionError::LengthOverflow("definition"));
    }
    if encoded.len() < MAGIC.len() {
        return Err(FlowDefinitionError::Truncated);
    }
    if &encoded[..MAGIC.len()] != MAGIC {
        return Err(FlowDefinitionError::InvalidMagic);
    }
    let header_length = MAGIC.len() + size_of::<u16>();
    if encoded.len() < header_length + CHECKSUM_LENGTH {
        return Err(FlowDefinitionError::Truncated);
    }
    let (payload, checksum) = encoded.split_at(encoded.len() - CHECKSUM_LENGTH);
    if crc32fast::hash(payload) != u32::from_be_bytes(checksum.try_into().expect("fixed checksum"))
    {
        return Err(FlowDefinitionError::IntegrityMismatch);
    }
    let version = u16::from_be_bytes(
        payload[MAGIC.len()..header_length]
            .try_into()
            .expect("fixed version"),
    );
    if version != FORMAT_VERSION {
        return Err(FlowDefinitionError::UnsupportedVersion(version));
    }
    let payload = &payload[header_length..];
    let definition: FlowDefinition =
        serde_json::from_slice(payload).map_err(|error| json_error(&error))?;
    let mut comparison = PayloadComparisonWriter {
        expected: payload,
        position: 0,
        matches: true,
    };
    serde_json::to_writer(&mut comparison, &definition).map_err(|error| json_error(&error))?;
    if !comparison.matches || comparison.position != payload.len() {
        return Err(FlowDefinitionError::NonCanonical);
    }
    validate::validate_definition(&definition)?;
    Ok(definition)
}

fn json_error(error: &serde_json::Error) -> FlowDefinitionError {
    use serde_json::error::Category;
    FlowDefinitionError::InvalidJson {
        reason: match error.classify() {
            Category::Eof => "unexpected end",
            Category::Data => "invalid value",
            Category::Syntax => "invalid syntax",
            Category::Io => "I/O failure",
        },
        line: error.line(),
        column: error.column(),
    }
}

struct PayloadComparisonWriter<'a> {
    expected: &'a [u8],
    position: usize,
    matches: bool,
}

impl Write for PayloadComparisonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self.position.saturating_add(bytes.len());
        self.matches &= self.expected.get(self.position..end) == Some(bytes);
        self.position = end;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
