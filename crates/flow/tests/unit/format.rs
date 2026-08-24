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
        FlowDefinitionError::Topology(crate::TopologyError::DuplicateStageId("first".to_owned()))
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
    changed_source[source_reference..source_reference + b"other".len()].copy_from_slice(b"other");
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
