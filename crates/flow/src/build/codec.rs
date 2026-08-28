use std::{collections::HashMap, num::NonZeroU64};

use dogpaddle_operation::{decode_definition, encode_definition};
use thiserror::Error;

use super::{
    definition::{FlowDefinition, StationDefinition},
    validate::{TopologyError, validate_decoded_topology, validate_station_ids},
};

const MAGIC: &[u8] = b"dogpaddle.flow\0";
const FORMAT_VERSION: u16 = 1;
pub(super) const CHECKSUM_LENGTH: usize = size_of::<u32>();
const CRC32_POLYNOMIAL: u32 = 0xedb8_8320;
pub(crate) const DEFINITION_DATA_NAME: &str = "flow/definition";
pub(crate) const FLOW_STATE_DATA_NAME: &str = "flow/state";

/// Failure while encoding or decoding a durable Flow definition.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowDefinitionError {
    /// The encoded definition ends before all declared fields are present.
    #[error("flow definition is truncated")]
    Truncated,
    /// The encoded bytes do not begin with the `DogPaddle` Flow marker.
    #[error("flow definition marker is invalid")]
    InvalidMagic,
    /// The Flow definition format version is unsupported.
    #[error("unsupported flow definition format version {0}")]
    UnsupportedVersion(u16),
    /// A station or source ID is not valid UTF-8.
    #[error("flow definition contains an invalid UTF-8 station ID")]
    InvalidUtf8,
    /// A length cannot be represented by the durable format.
    #[error("{0} is too large for the flow definition format")]
    LengthOverflow(&'static str),
    /// A source ID does not identify a declared station.
    #[error("station {station:?} references unknown source {source_id:?}")]
    UnknownSource {
        /// Station containing the invalid source reference.
        station: String,
        /// Missing source ID.
        source_id: String,
    },
    /// One operation definition is invalid or unsupported.
    #[error(transparent)]
    Operation(#[from] dogpaddle_operation::DefinitionCodecError),
    /// The persisted checksum does not match the definition bytes.
    #[error("flow definition checksum does not match its contents")]
    IntegrityMismatch,
    /// The decoded graph violates topology rules.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// Bytes remain after the complete definition.
    #[error("flow definition contains trailing bytes")]
    TrailingBytes,
}

pub(crate) fn station_state_name(index: usize) -> String {
    format!("station/{index:08x}/state")
}

pub(crate) fn station_output_name(index: usize) -> String {
    format!("station/{index:08x}/output")
}

pub(crate) fn station_operation_data_name(index: usize, logical_name: &str) -> String {
    format!("station/{index:08x}/operation/{logical_name}")
}

pub(crate) fn encode(definition: &FlowDefinition) -> Result<Vec<u8>, FlowDefinitionError> {
    let station_count = u32::try_from(definition.stations().len())
        .map_err(|_| FlowDefinitionError::LengthOverflow("station count"))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&station_count.to_be_bytes());

    for station in definition.stations() {
        encode_string(&mut encoded, station.id(), "station ID")?;
        let operation = encode_definition(station.operation());
        encode_bytes(&mut encoded, &operation, "operation definition")?;
        encoded.extend_from_slice(
            &station
                .output_capacity_bytes()
                .map_or(0, NonZeroU64::get)
                .to_be_bytes(),
        );
        let source_count = u32::try_from(station.sources().len())
            .map_err(|_| FlowDefinitionError::LengthOverflow("source count"))?;
        encoded.extend_from_slice(&source_count.to_be_bytes());
        for source in station.sources() {
            encode_string(&mut encoded, source, "source ID")?;
        }
    }
    let checksum = crc32(&encoded);
    encoded.extend_from_slice(&checksum.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode(encoded: &[u8]) -> Result<FlowDefinition, FlowDefinitionError> {
    if encoded.len() < MAGIC.len() {
        return Err(FlowDefinitionError::Truncated);
    }
    if &encoded[..MAGIC.len()] != MAGIC {
        return Err(FlowDefinitionError::InvalidMagic);
    }
    if encoded.len() < MAGIC.len() + size_of::<u16>() + size_of::<u32>() + CHECKSUM_LENGTH {
        return Err(FlowDefinitionError::Truncated);
    }

    let checksum_offset = encoded.len() - CHECKSUM_LENGTH;
    let (definition, encoded_checksum) = encoded.split_at(checksum_offset);
    let expected_checksum = u32::from_be_bytes(
        encoded_checksum
            .try_into()
            .expect("checksum slice has a fixed length"),
    );
    if crc32(definition) != expected_checksum {
        return Err(FlowDefinitionError::IntegrityMismatch);
    }

    let mut cursor = Cursor::new(&definition[MAGIC.len()..]);
    let version = cursor.read_u16()?;
    if version != FORMAT_VERSION {
        return Err(FlowDefinitionError::UnsupportedVersion(version));
    }

    let station_count = cursor.read_u32()?;
    let mut stations = Vec::new();
    for _ in 0..station_count {
        let id = cursor.read_string()?;
        let operation = decode_definition(cursor.read_bytes()?)?;
        let output_capacity_bytes = NonZeroU64::new(cursor.read_u64()?);
        let source_count = cursor.read_u32()?;
        let mut sources = Vec::new();
        for _ in 0..source_count {
            sources.push(cursor.read_string()?);
        }
        stations.push(StationDefinition {
            id,
            operation,
            output_capacity_bytes,
            sources,
        });
    }
    if !cursor.is_empty() {
        return Err(FlowDefinitionError::TrailingBytes);
    }

    validate_definition(stations)
}

fn validate_definition(
    stations: Vec<StationDefinition>,
) -> Result<FlowDefinition, FlowDefinitionError> {
    validate_station_ids(&stations)?;
    let sources_by_target = {
        let ids = stations
            .iter()
            .enumerate()
            .map(|(index, station)| (station.id.as_str(), index))
            .collect::<HashMap<_, _>>();
        stations
            .iter()
            .map(|station| {
                if station.sources.is_empty() {
                    return Ok(None);
                }
                station
                    .sources
                    .iter()
                    .map(|source| {
                        ids.get(source.as_str()).copied().ok_or_else(|| {
                            FlowDefinitionError::UnknownSource {
                                station: station.id.clone(),
                                source_id: source.clone(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Some)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    validate_decoded_topology(&stations, &sources_by_target)?;
    Ok(FlowDefinition::new(stations))
}

fn encode_string(
    encoded: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), FlowDefinitionError> {
    encode_bytes(encoded, value.as_bytes(), field)
}

fn encode_bytes(
    encoded: &mut Vec<u8>,
    value: &[u8],
    field: &'static str,
) -> Result<(), FlowDefinitionError> {
    let length =
        u32::try_from(value.len()).map_err(|_| FlowDefinitionError::LengthOverflow(field))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

pub(super) fn crc32(bytes: &[u8]) -> u32 {
    let mut checksum = u32::MAX;
    for byte in bytes {
        checksum ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (checksum & 1).wrapping_neg();
            checksum = (checksum >> 1) ^ (CRC32_POLYNOMIAL & mask);
        }
    }
    !checksum
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

    fn read_u16(&mut self) -> Result<u16, FlowDefinitionError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, FlowDefinitionError> {
        Ok(u32::from_be_bytes(self.take::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, FlowDefinitionError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], FlowDefinitionError> {
        let length = usize::try_from(self.read_u32()?)
            .map_err(|_| FlowDefinitionError::LengthOverflow("encoded field"))?;
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(FlowDefinitionError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn read_string(&mut self) -> Result<String, FlowDefinitionError> {
        String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| FlowDefinitionError::InvalidUtf8)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], FlowDefinitionError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(FlowDefinitionError::Truncated)?;
        self.remaining = remaining;
        Ok(*value)
    }
}
