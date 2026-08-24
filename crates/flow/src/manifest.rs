use dogpaddle_operation::OperationDefinition;

use crate::{
    FlowDefinitionError,
    topology::{Topology, TopologyBuilder},
};

const MAGIC: &[u8] = b"dogpaddle.flow\0";
const FORMAT_VERSION: u16 = 1;
const CHECKSUM_LENGTH: usize = size_of::<u32>();
const CRC32_POLYNOMIAL: u32 = 0xedb8_8320;

pub(crate) fn encode(
    topology: &Topology<OperationDefinition>,
) -> Result<Vec<u8>, FlowDefinitionError> {
    let stage_count = u32::try_from(topology.stages().len())
        .map_err(|_| FlowDefinitionError::LengthOverflow("stage count"))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&stage_count.to_be_bytes());

    for stage in topology.stages() {
        encode_string(&mut encoded, stage.id(), "stage ID")?;
        let operation = stage.operation().encode();
        encode_bytes(&mut encoded, &operation, "operation definition")?;
        let source_count = u32::try_from(stage.sources().len())
            .map_err(|_| FlowDefinitionError::LengthOverflow("source count"))?;
        encoded.extend_from_slice(&source_count.to_be_bytes());
        for source in stage.sources() {
            encode_string(&mut encoded, source, "source ID")?;
        }
    }
    let checksum = crc32(&encoded);
    encoded.extend_from_slice(&checksum.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode(encoded: &[u8]) -> Result<Topology<OperationDefinition>, FlowDefinitionError> {
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

    let stage_count = cursor.read_u32()?;
    let mut records = Vec::new();
    for _ in 0..stage_count {
        let id = cursor.read_string()?;
        let operation = OperationDefinition::decode(cursor.read_bytes()?)?;
        let source_count = cursor.read_u32()?;
        let mut sources = Vec::new();
        for _ in 0..source_count {
            sources.push(cursor.read_string()?);
        }
        records.push(StageRecord {
            id,
            operation,
            sources,
        });
    }
    if !cursor.is_empty() {
        return Err(FlowDefinitionError::TrailingBytes);
    }

    topology_from_records(&records)
}

fn topology_from_records(
    records: &[StageRecord],
) -> Result<Topology<OperationDefinition>, FlowDefinitionError> {
    let ids = records
        .iter()
        .map(|record| record.id.clone())
        .collect::<Vec<_>>();
    let mut builder = TopologyBuilder::new();
    let references = records
        .iter()
        .map(|record| builder.stage(record.id.clone(), record.operation.clone()))
        .collect::<Vec<_>>();
    builder.validate_stage_ids()?;

    for (target, record) in records.iter().enumerate() {
        if record.sources.is_empty() {
            continue;
        }
        let sources = record
            .sources
            .iter()
            .map(|source| {
                ids.iter()
                    .position(|id| id == source)
                    .map(|index| references[index])
                    .ok_or_else(|| FlowDefinitionError::UnknownSource {
                        stage: record.id.clone(),
                        source_id: source.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        builder.connect(sources, references[target]);
    }
    builder.finish().map_err(FlowDefinitionError::from)
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

fn crc32(bytes: &[u8]) -> u32 {
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

struct StageRecord {
    id: String,
    operation: OperationDefinition,
    sources: Vec<String>,
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

#[cfg(test)]
mod tests {
    use dogpaddle_operation::{CountDefinition, SequenceSourceDefinition};

    use super::{CHECKSUM_LENGTH, crc32, decode, encode};
    use crate::{FlowDefinitionError, topology::TopologyBuilder};

    fn topology() -> crate::topology::Topology<dogpaddle_operation::OperationDefinition> {
        let mut builder = TopologyBuilder::new();
        let source = builder.stage("source", SequenceSourceDefinition::new(7).into());
        let count = builder.stage("count", CountDefinition::new().into());
        builder.connect([source], count);
        builder.finish().unwrap()
    }

    fn topology_with_ids(
        source_id: &str,
        count_id: &str,
    ) -> crate::topology::Topology<dogpaddle_operation::OperationDefinition> {
        let mut builder = TopologyBuilder::new();
        let source = builder.stage(source_id, SequenceSourceDefinition::new(7).into());
        let count = builder.stage(count_id, CountDefinition::new().into());
        builder.connect([source], count);
        builder.finish().unwrap()
    }

    #[test]
    fn codec_is_canonical_and_round_trips_ordered_sources() {
        let encoded = encode(&topology()).unwrap();
        let mut expected = [
            b"dogpaddle.flow\0".as_slice(),
            &[0, 1, 0, 0, 0, 2],
            &[0, 0, 0, 6],
            b"source",
            &[0, 0, 0, 32],
            b"dogpaddle.operation\0",
            &[0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7],
            &[0, 0, 0, 0],
            &[0, 0, 0, 5],
            b"count",
            &[0, 0, 0, 24],
            b"dogpaddle.operation\0",
            &[0, 1, 0, 2],
            &[0, 0, 0, 1, 0, 0, 0, 6],
            b"source",
        ]
        .concat();
        expected.extend_from_slice(&crc32(&expected).to_be_bytes());
        assert_eq!(encoded, expected);

        let decoded = decode(&encoded).unwrap();
        assert_eq!(encode(&decoded).unwrap(), encoded);
    }

    #[test]
    fn decoder_rejects_truncation_and_trailing_bytes() {
        let encoded = encode(&topology()).unwrap();
        let payload_end = encoded.len() - CHECKSUM_LENGTH;
        let mut truncated = encoded[..payload_end - 1].to_vec();
        truncated.extend_from_slice(&crc32(&truncated).to_be_bytes());
        assert_eq!(
            decode(&truncated).unwrap_err(),
            FlowDefinitionError::Truncated
        );

        let mut trailing = encoded;
        let checksum_offset = trailing.len() - CHECKSUM_LENGTH;
        trailing.insert(checksum_offset, 0);
        let checksum_offset = trailing.len() - CHECKSUM_LENGTH;
        let checksum = crc32(&trailing[..checksum_offset]);
        trailing[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            decode(&trailing).unwrap_err(),
            FlowDefinitionError::TrailingBytes
        );
    }

    #[test]
    fn decoder_validates_all_stage_ids_before_resolving_sources() {
        let mut encoded = encode(&topology_with_ids("first", "other")).unwrap();
        let duplicate = encoded
            .windows(b"other".len())
            .position(|window| window == b"other")
            .unwrap();
        encoded[duplicate..duplicate + b"first".len()].copy_from_slice(b"first");
        let source_reference = encoded
            .windows(b"first".len())
            .rposition(|window| window == b"first")
            .unwrap();
        encoded[source_reference..source_reference + b"ghost".len()].copy_from_slice(b"ghost");
        let checksum_offset = encoded.len() - CHECKSUM_LENGTH;
        let checksum = crc32(&encoded[..checksum_offset]);
        encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert_eq!(
            decode(&encoded).unwrap_err(),
            FlowDefinitionError::Topology(crate::TopologyError::DuplicateStageId(
                "first".to_owned()
            ))
        );
    }

    #[test]
    fn decoder_rejects_semantic_bit_flips_and_checksum_damage() {
        let original = encode(&topology()).unwrap();

        let mut changed_start = original.clone();
        let start = changed_start
            .windows(7_u64.to_be_bytes().len())
            .position(|window| window == 7_u64.to_be_bytes())
            .unwrap();
        changed_start[start + 7] ^= 1;
        assert_eq!(
            decode(&changed_start).unwrap_err(),
            FlowDefinitionError::IntegrityMismatch
        );

        let mut changed_id = original.clone();
        let id = changed_id
            .windows(b"source".len())
            .position(|window| window == b"source")
            .unwrap();
        changed_id[id + b"source".len() - 1] = b'f';
        assert_eq!(
            decode(&changed_id).unwrap_err(),
            FlowDefinitionError::IntegrityMismatch
        );

        let mut changed_source = encode(&topology_with_ids("first", "other")).unwrap();
        let source_reference = changed_source
            .windows(b"first".len())
            .rposition(|window| window == b"first")
            .unwrap();
        changed_source[source_reference..source_reference + b"other".len()]
            .copy_from_slice(b"other");
        assert_eq!(
            decode(&changed_source).unwrap_err(),
            FlowDefinitionError::IntegrityMismatch
        );

        let mut changed_checksum = original;
        let final_byte = changed_checksum.last_mut().unwrap();
        *final_byte ^= 1;
        assert_eq!(
            decode(&changed_checksum).unwrap_err(),
            FlowDefinitionError::IntegrityMismatch
        );
    }

    #[test]
    fn checksum_uses_the_stable_ieee_crc32_algorithm() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }
}
