use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use serde_json::error::Category;
use thiserror::Error;

use crate::{
    OperationDefinition,
    operation::{scan, sink, transform},
};

const MAGIC: &[u8] = b"dogpaddle.operation\0";
const FORMAT_VERSION: u16 = 1;

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
    /// A known operation variant contains a non-canonical persistent payload.
    #[error("operation definition payload is invalid: {0}")]
    InvalidPayload(&'static str),
    /// A JSON payload could not be decoded. The category and position never echo input values.
    #[error("operation definition JSON payload has {reason} at line {line} column {column}")]
    InvalidJsonPayload {
        /// Static error category without input text.
        reason: &'static str,
        /// One-based JSON line number.
        line: usize,
        /// One-based JSON column number.
        column: usize,
    },
    /// Bytes remain after decoding the selected operation variant.
    #[error("operation definition contains trailing bytes")]
    TrailingBytes,
}

/// Encodes a definition using `DogPaddle`'s versioned binary format.
///
/// Operation payloads may embed canonical bytes from pinned upstream codecs.
/// Development v1 state is rebuilt when such bytes or the payload layout changes;
/// no older payload format is recognized or migrated.
///
/// # Panics
///
/// Panics if a trusted definition violates its serialization invariant.
#[must_use]
pub fn encode_definition(definition: &OperationDefinition) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(MAGIC.len() + 12);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&definition.persistence_tag().to_be_bytes());
    serde_json::to_writer(&mut encoded, definition)
        .expect("validated operation definition has a serializable payload");
    encoded
}

/// Decodes one definition from `DogPaddle`'s versioned binary format.
///
/// # Errors
///
/// Returns a [`DefinitionCodecError`] for truncated, unsupported, unknown, or
/// non-canonical input.
pub fn decode_definition(encoded: &[u8]) -> Result<OperationDefinition, DefinitionCodecError> {
    let (tag, payload) = decode_header(encoded)?;
    match tag {
        scan::mysql_cdc::TAG => {
            scan::mysql_cdc::decode_definition(payload).map(OperationDefinition::MySqlCdcScan)
        }
        scan::postgres_cdc::TAG => {
            scan::postgres_cdc::decode_definition(payload).map(OperationDefinition::PostgresCdcScan)
        }
        scan::sequence::TAG => {
            scan::sequence::decode_definition(payload).map(OperationDefinition::SequenceScan)
        }
        transform::aggregate::TAG => {
            transform::aggregate::decode_definition(payload).map(OperationDefinition::Aggregate)
        }
        transform::asof_join::TAG => {
            transform::asof_join::decode_definition(payload).map(OperationDefinition::AsOfJoin)
        }
        transform::distinct::TAG => {
            transform::distinct::decode_definition(payload).map(OperationDefinition::Distinct)
        }
        transform::running_event_count::TAG => {
            transform::running_event_count::decode_definition(payload)
                .map(OperationDefinition::RunningEventCount)
        }
        transform::filter::TAG => {
            transform::filter::decode_definition(payload).map(OperationDefinition::Filter)
        }
        transform::equi_join::TAG => {
            transform::equi_join::decode_definition(payload).map(OperationDefinition::EquiJoin)
        }
        transform::select::TAG => {
            transform::select::decode_definition(payload).map(OperationDefinition::Select)
        }
        transform::union_all::TAG => {
            transform::union_all::decode_definition(payload).map(OperationDefinition::UnionAll)
        }
        transform::schema_align::TAG => transform::schema_align::decode_definition(payload)
            .map(OperationDefinition::SchemaAlign),
        sink::clickhouse::TAG => {
            sink::clickhouse::decode_definition(payload).map(OperationDefinition::ClickHouseSink)
        }
        sink::discard::TAG => {
            sink::discard::decode_definition(payload).map(OperationDefinition::Discard)
        }
        sink::doris::TAG => {
            sink::doris::decode_definition(payload).map(OperationDefinition::DorisSink)
        }
        sink::postgres::TAG => {
            sink::postgres::decode_definition(payload).map(OperationDefinition::PostgresSink)
        }
        sink::sqlite::TAG => {
            sink::sqlite::decode_definition(payload).map(OperationDefinition::SqliteSink)
        }
        _ => Err(DefinitionCodecError::UnknownTag(tag)),
    }
}

pub(crate) fn decode_json_payload<T>(
    payload: &[u8],
    invalid: &'static str,
) -> Result<T, DefinitionCodecError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let definition: T = parse_json_payload(payload)?;
    require_canonical_json_payload(&definition, payload, invalid)?;
    Ok(definition)
}

pub(crate) fn require_canonical_json_payload<T: Serialize>(
    definition: &T,
    payload: &[u8],
    invalid: &'static str,
) -> Result<(), DefinitionCodecError> {
    let mut comparison = PayloadComparisonWriter {
        expected: payload,
        position: 0,
        matches: true,
    };
    serde_json::to_writer(&mut comparison, &definition)
        .expect("decoded operation definition has a serializable payload");
    if !comparison.matches || comparison.position != payload.len() {
        return Err(DefinitionCodecError::InvalidPayload(invalid));
    }
    Ok(())
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

pub(crate) fn parse_json_payload<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
) -> Result<T, DefinitionCodecError> {
    let mut decoder = serde_json::Deserializer::from_slice(payload);
    let definition = T::deserialize(&mut decoder).map_err(|error| {
        let reason = match error.classify() {
            Category::Eof => return DefinitionCodecError::Truncated,
            Category::Data => safe_json_data_reason(&error),
            Category::Syntax => "invalid syntax",
            Category::Io => "I/O failure",
        };
        DefinitionCodecError::InvalidJsonPayload {
            reason,
            line: error.line(),
            column: error.column(),
        }
    })?;
    decoder
        .end()
        .map_err(|_| DefinitionCodecError::TrailingBytes)?;
    Ok(definition)
}

fn safe_json_data_reason(error: &serde_json::Error) -> &'static str {
    const EXPRESSION_REASONS: [&str; 6] = [
        "DataFusion expression protobuf is too large",
        "DataFusion expression protobuf is invalid",
        "DataFusion expression protobuf contains non-canonical map metadata",
        "DataFusion expression cannot be re-encoded",
        "DataFusion expression protobuf is not canonical",
        "expression must be immutable and row-local",
    ];

    let mut prefix = ErrorPrefix::default();
    std::fmt::write(&mut prefix, format_args!("{error}"))
        .expect("bounded error prefix writer cannot fail");
    let message = prefix.as_bytes();
    if message.starts_with(b"expression protobuf base64 is invalid") {
        return "expression protobuf base64 is invalid";
    }
    if let Some(detail) = message.strip_prefix(b"operation definition payload is invalid: ") {
        for reason in EXPRESSION_REASONS {
            if detail.starts_with(reason.as_bytes()) {
                return reason;
            }
        }
    }
    "invalid value"
}

struct ErrorPrefix {
    bytes: [u8; 160],
    len: usize,
}

impl Default for ErrorPrefix {
    fn default() -> Self {
        Self {
            bytes: [0; 160],
            len: 0,
        }
    }
}

impl ErrorPrefix {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl std::fmt::Write for ErrorPrefix {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let count = text.len().min(self.bytes.len() - self.len);
        self.bytes[self.len..self.len + count].copy_from_slice(&text.as_bytes()[..count]);
        self.len += count;
        Ok(())
    }
}

fn decode_header(encoded: &[u8]) -> Result<(u16, &[u8]), DefinitionCodecError> {
    if encoded.len() < MAGIC.len() {
        return Err(DefinitionCodecError::Truncated);
    }
    if &encoded[..MAGIC.len()] != MAGIC {
        return Err(DefinitionCodecError::InvalidMagic);
    }

    let (version, remaining) = encoded[MAGIC.len()..]
        .split_first_chunk::<2>()
        .ok_or(DefinitionCodecError::Truncated)?;
    let version = u16::from_be_bytes(*version);
    if version != FORMAT_VERSION {
        return Err(DefinitionCodecError::UnsupportedVersion(version));
    }
    let (tag, payload) = remaining
        .split_first_chunk::<2>()
        .ok_or(DefinitionCodecError::Truncated)?;
    Ok((u16::from_be_bytes(*tag), payload))
}
