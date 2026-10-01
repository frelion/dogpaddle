use super::{
    definition::{FlowDefinition, OperationNode},
    validate::{self, ResolvedTopology, TopologyError},
};
use dogpaddle_operation::{decode_definition, encode_definition};
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
    /// An ID is not valid UTF-8.
    #[error("flow definition contains invalid UTF-8")]
    InvalidUtf8,
    /// A field exceeds the bounded format.
    #[error("{0} exceeds the flow definition limit")]
    LengthOverflow(&'static str),
    /// The identity discriminator is noncanonical.
    #[error("invalid owner identity presence {0}")]
    InvalidOwnerIdentityPresence(u8),
    /// A concrete Operation definition is invalid.
    #[error("operation {operation_id:?} definition is invalid: {source}")]
    Operation {
        /// Stable Operation ID.
        operation_id: String,
        /// Concrete codec failure.
        #[source]
        source: dogpaddle_operation::DefinitionCodecError,
    },
    /// The integrity checksum does not match.
    #[error("flow definition checksum does not match")]
    IntegrityMismatch,
    /// The graph is invalid.
    #[error(transparent)]
    Topology(#[from] TopologyError),
    /// Bytes remain after the complete definition.
    #[error("flow definition contains trailing bytes")]
    TrailingBytes,
}

pub(crate) fn operation_prefix(index: usize) -> String {
    format!("operation/{index:08x}")
}

pub(crate) fn encode(definition: &FlowDefinition) -> Result<Vec<u8>, FlowDefinitionError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    if let Some(identity) = definition.owner_identity {
        encoded.push(1);
        encoded.extend_from_slice(&identity);
    } else {
        encoded.push(0);
    }
    let count = u32::try_from(definition.operations.len())
        .map_err(|_| FlowDefinitionError::LengthOverflow("operation count"))?;
    encoded.extend_from_slice(&count.to_be_bytes());
    for node in &definition.operations {
        encode_string(&mut encoded, &node.id, "operation ID")?;
        encode_bytes(
            &mut encoded,
            &encode_definition(&node.definition),
            "operation definition",
        )?;
        let count = u32::try_from(node.inputs.len())
            .map_err(|_| FlowDefinitionError::LengthOverflow("input count"))?;
        encoded.extend_from_slice(&count.to_be_bytes());
        for &input in &node.inputs {
            let input = u32::try_from(input)
                .map_err(|_| FlowDefinitionError::LengthOverflow("input ordinal"))?;
            encoded.extend_from_slice(&input.to_be_bytes());
        }
    }
    if encoded.len() > MAX_DEFINITION_BYTES - CHECKSUM_LENGTH {
        return Err(FlowDefinitionError::LengthOverflow("definition"));
    }
    encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode(
    encoded: &[u8],
) -> Result<(FlowDefinition, ResolvedTopology), FlowDefinitionError> {
    if encoded.len() > MAX_DEFINITION_BYTES {
        return Err(FlowDefinitionError::LengthOverflow("definition"));
    }
    if encoded.len() < MAGIC.len() {
        return Err(FlowDefinitionError::Truncated);
    }
    if &encoded[..MAGIC.len()] != MAGIC {
        return Err(FlowDefinitionError::InvalidMagic);
    }
    if encoded.len() < MAGIC.len() + 2 + 1 + 4 + CHECKSUM_LENGTH {
        return Err(FlowDefinitionError::Truncated);
    }
    let (payload, checksum) = encoded.split_at(encoded.len() - CHECKSUM_LENGTH);
    if crc32fast::hash(payload) != u32::from_be_bytes(checksum.try_into().expect("fixed checksum"))
    {
        return Err(FlowDefinitionError::IntegrityMismatch);
    }
    let mut cursor = Cursor::new(&payload[MAGIC.len()..]);
    let version = cursor.read_u16()?;
    if version != FORMAT_VERSION {
        return Err(FlowDefinitionError::UnsupportedVersion(version));
    }
    let owner_identity = match cursor.read_u8()? {
        0 => None,
        1 => Some(cursor.take::<32>()?),
        tag => return Err(FlowDefinitionError::InvalidOwnerIdentityPresence(tag)),
    };
    let count = cursor.read_u32()? as usize;
    if count > validate::MAX_OPERATIONS {
        return Err(FlowDefinitionError::LengthOverflow("operation count"));
    }
    let mut operations = Vec::with_capacity(count);
    for _ in 0..count {
        let id = cursor.read_string()?;
        let definition = decode_definition(cursor.read_bytes()?).map_err(|source| {
            FlowDefinitionError::Operation {
                operation_id: id.clone(),
                source,
            }
        })?;
        let count = cursor.read_u32()? as usize;
        if count > validate::MAX_OPERATIONS {
            return Err(FlowDefinitionError::LengthOverflow("input count"));
        }
        let inputs = (0..count)
            .map(|_| cursor.read_u32().map(|input| input as usize))
            .collect::<Result<Vec<_>, _>>()?;
        operations.push(OperationNode {
            id,
            definition,
            inputs,
        });
    }
    if !cursor.is_empty() {
        return Err(FlowDefinitionError::TrailingBytes);
    }
    let definition = FlowDefinition {
        owner_identity,
        operations,
    };
    let topology = validate::resolve(&definition)?;
    Ok((definition, topology))
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
