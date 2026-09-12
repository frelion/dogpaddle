use std::{collections::HashMap, num::NonZeroU64};

use dogpaddle_operation::{decode_definition, encode_definition};
use thiserror::Error;

use crate::assembly::{ResolvedTopology, resolve_topology};

use super::{
    definition::{FlowDefinition, InputDefinition, StationDefinition},
    validate::{TopologyError, validate_decoded_topology, validate_station_ids},
};

const MAGIC: &[u8] = b"dogpaddle.flow\0";
const FORMAT_VERSION: u16 = 1;
pub(super) const CHECKSUM_LENGTH: usize = size_of::<u32>();
const CRC32_POLYNOMIAL: u32 = 0xedb8_8320;
pub(crate) const DEFINITION_DATA_NAME: &str = "flow/definition";

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
    /// A station or input ID is not valid UTF-8.
    #[error("flow definition contains an invalid UTF-8 station ID")]
    InvalidUtf8,
    /// A length cannot be represented by the durable format.
    #[error("{0} is too large for the flow definition format")]
    LengthOverflow(&'static str),
    /// The output-presence discriminator is outside the canonical domain.
    #[error("flow definition contains invalid output presence {0}")]
    InvalidOutputPresence(u8),
    /// A present output must declare a nonzero retained-byte capacity.
    #[error("flow definition contains a zero output capacity")]
    ZeroOutputCapacity,
    /// An input ID does not identify a declared station.
    #[error("station {station:?} references unknown input {input_id:?}")]
    UnknownInput {
        /// Station containing the invalid input reference.
        station: String,
        /// Missing input ID.
        input_id: String,
    },
    /// One Operation definition is invalid or unsupported.
    #[error("station {station_id:?} operation {operation} definition is invalid: {source}")]
    Operation {
        /// Stable ID of the Station containing the Operation.
        station_id: String,
        /// Zero-based Operation ordinal.
        operation: usize,
        /// Operation codec failure.
        #[source]
        source: dogpaddle_operation::DefinitionCodecError,
    },
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

pub(crate) fn station_active_input_name(index: usize) -> String {
    format!("station/{index:08x}/active-input")
}

pub(crate) fn station_output_name(index: usize) -> String {
    format!("station/{index:08x}/output")
}

pub(crate) fn station_operation_data_name(
    station: usize,
    operation: usize,
    logical_name: &str,
) -> String {
    format!("station/{station:08x}/operation/{operation:08x}/{logical_name}")
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
        let operation_count = u32::try_from(station.operations().len())
            .map_err(|_| FlowDefinitionError::LengthOverflow("operation count"))?;
        encoded.extend_from_slice(&operation_count.to_be_bytes());
        for operation in station.operations() {
            let operation = encode_definition(operation.as_ref());
            encode_bytes(&mut encoded, &operation, "operation definition")?;
        }
        let input_count = u32::try_from(station.inputs().len())
            .map_err(|_| FlowDefinitionError::LengthOverflow("input count"))?;
        encoded.extend_from_slice(&input_count.to_be_bytes());
        for input in station.input_definitions() {
            encode_string(&mut encoded, input.station_id(), "input ID")?;
        }
        if let Some(capacity) = station.output_capacity_bytes() {
            encoded.push(1);
            encoded.extend_from_slice(&capacity.get().to_be_bytes());
        } else {
            encoded.push(0);
        }
    }
    let checksum = crc32(&encoded);
    encoded.extend_from_slice(&checksum.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode(
    encoded: &[u8],
) -> Result<(FlowDefinition, ResolvedTopology), FlowDefinitionError> {
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
        let operation_count = cursor.read_u32()?;
        let mut operations = Vec::new();
        for operation in 0..operation_count {
            let operation =
                usize::try_from(operation).expect("a u32 Operation ordinal fits supported targets");
            operations.push(decode_definition(cursor.read_bytes()?).map_err(|source| {
                FlowDefinitionError::Operation {
                    station_id: id.clone(),
                    operation,
                    source,
                }
            })?);
        }
        let input_count = cursor.read_u32()?;
        let mut inputs = Vec::new();
        for _ in 0..input_count {
            inputs.push(InputDefinition::new(cursor.read_string()?));
        }
        let output_capacity_bytes = match cursor.read_u8()? {
            0 => None,
            1 => {
                let capacity = NonZeroU64::new(cursor.read_u64()?)
                    .ok_or(FlowDefinitionError::ZeroOutputCapacity)?;
                Some(capacity)
            }
            presence => return Err(FlowDefinitionError::InvalidOutputPresence(presence)),
        };
        stations.push(StationDefinition {
            id,
            operations,
            output_capacity_bytes,
            inputs,
        });
    }
    if !cursor.is_empty() {
        return Err(FlowDefinitionError::TrailingBytes);
    }

    validate_definition(stations)
}

fn validate_definition(
    stations: Vec<StationDefinition>,
) -> Result<(FlowDefinition, ResolvedTopology), FlowDefinitionError> {
    validate_station_ids(&stations)?;
    let inputs_by_station = {
        let ids = stations
            .iter()
            .enumerate()
            .map(|(index, station)| (station.id.as_str(), index))
            .collect::<HashMap<_, _>>();
        stations
            .iter()
            .map(|station| {
                if station.inputs.is_empty() {
                    return Ok(None);
                }
                station
                    .inputs
                    .iter()
                    .map(|input| {
                        ids.get(input.station_id()).copied().ok_or_else(|| {
                            FlowDefinitionError::UnknownInput {
                                station: station.id.clone(),
                                input_id: input.station_id.clone(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Some)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let schedule = validate_decoded_topology(&stations, &inputs_by_station)?;
    let topology = resolve_topology(inputs_by_station, schedule);
    Ok((FlowDefinition::new(stations), topology))
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

    fn read_u8(&mut self) -> Result<u8, FlowDefinitionError> {
        Ok(self.take::<1>()?[0])
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
