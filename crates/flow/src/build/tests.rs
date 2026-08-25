use dogpaddle_operation::{
    OperationDefinition, encode_definition,
    operation::{source::SequenceSourceDefinition, transform::CountDefinition},
};

use super::{
    FlowBuilder, FlowDefinitionError, InvalidStageIdReason, StageRef, TopologyError,
    codec::{CHECKSUM_LENGTH, crc32, decode, encode},
    definition::{FlowDefinition, StageDefinition},
    validate::{validate_acyclic, validate_connections},
};

fn source(start: u64) -> SequenceSourceDefinition {
    SequenceSourceDefinition::new(start)
}

fn count() -> CountDefinition {
    CountDefinition::new()
}

fn builder() -> FlowBuilder {
    FlowBuilder::new("")
}

fn find_stage<'a>(definition: &'a FlowDefinition, id: &str) -> &'a StageDefinition {
    definition
        .stages
        .iter()
        .find(|stage| stage.id == id)
        .unwrap()
}

fn finish_target<D>(operation: D, actual: usize) -> Result<FlowDefinition, TopologyError>
where
    D: OperationDefinition,
{
    let mut builder = builder();
    let sources = (0..actual)
        .map(|index| builder.stage(format!("source-{index}"), source(index as u64)))
        .collect::<Vec<_>>();
    let target = builder.stage("target", operation);
    if !sources.is_empty() {
        builder.connect(sources, target);
    }
    builder.finish_definition()
}

#[test]
fn finish_preserves_stage_order_and_resolves_references() {
    let mut builder = builder();
    builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([second], target);

    let definition = builder.finish_definition().unwrap();

    assert_eq!(
        definition
            .stages
            .iter()
            .map(|stage| stage.id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second", "target"]
    );
    let target = find_stage(&definition, "target");
    assert_eq!(
        encode_definition(target.operation()),
        encode_definition(&count())
    );
    assert_eq!(
        target
            .sources
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["second"]
    );
}

#[test]
fn connection_validation_preserves_n_ary_order_and_repeated_sources() {
    let mut builder = builder();
    let first = builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([second, first, second], target);

    let sources =
        validate_connections(builder.token, &builder.stages, &builder.connections).unwrap();

    assert_eq!(sources[target.index].as_deref(), Some([1, 0, 1].as_slice()));
    assert_eq!(validate_acyclic(builder.stages.len(), &sources), Ok(()));
}

#[test]
fn finish_rejects_an_empty_definition() {
    assert_eq!(
        builder().finish_definition().unwrap_err(),
        TopologyError::EmptyTopology
    );
}

#[test]
fn finish_rejects_invalid_stage_ids_in_declaration_order() {
    let mut builder = builder();
    builder.stage("", source(0));
    builder.stage("contains\0nul", source(1));

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::InvalidStageId {
            id: String::new(),
            reason: InvalidStageIdReason::Empty,
        }
    );

    let mut nul_builder = FlowBuilder::new("");
    nul_builder.stage("contains\0nul", source(0));
    assert_eq!(
        nul_builder.finish_definition().unwrap_err(),
        TopologyError::InvalidStageId {
            id: "contains\0nul".to_owned(),
            reason: InvalidStageIdReason::ContainsNul,
        }
    );
}

#[test]
fn finish_rejects_duplicate_stage_ids_before_connections() {
    let mut builder = builder();
    let first = builder.stage("same", source(0));
    builder.stage("same", source(1));
    builder.connect(
        [first],
        StageRef {
            builder_token: 0,
            index: 0,
        },
    );

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::DuplicateStageId("same".to_owned())
    );
}

#[test]
fn finish_rejects_foreign_source_and_target_references() {
    let mut left = builder();
    let foreign = left.stage("foreign", source(0));

    let mut right = builder();
    let target = right.stage("target", count());
    right.connect([foreign], target);
    assert_eq!(
        right.finish_definition().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );

    let mut right = builder();
    let source = right.stage("source", source(0));
    right.connect([source], foreign);
    assert_eq!(
        right.finish_definition().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );
}

#[test]
fn finish_rejects_an_explicit_empty_source_list() {
    let mut builder = builder();
    let target = builder.stage("target", source(0));
    builder.connect([], target);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::EmptySources("target".to_owned())
    );
}

#[test]
fn finish_accepts_the_known_zero_and_unary_input_counts() {
    let source_definition = finish_target(source(0), 0).unwrap();
    assert!(find_stage(&source_definition, "target").sources.is_empty());

    let count_definition = finish_target(count(), 1).unwrap();
    assert_eq!(find_stage(&count_definition, "target").sources.len(), 1);
}

#[test]
fn finish_rejects_every_known_input_count_mismatch() {
    assert_eq!(
        finish_target(source(0), 1).unwrap_err(),
        TopologyError::InputCount {
            stage: "target".to_owned(),
            expected: 0,
            actual: 1,
        }
    );
    assert_eq!(
        finish_target(count(), 0).unwrap_err(),
        TopologyError::InputCount {
            stage: "target".to_owned(),
            expected: 1,
            actual: 0,
        }
    );
    assert_eq!(
        finish_target(count(), 2).unwrap_err(),
        TopologyError::InputCount {
            stage: "target".to_owned(),
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn finish_rejects_setting_sources_twice() {
    let mut builder = builder();
    let first = builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([first], target);
    builder.connect([second], target);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::SourcesAlreadySet("target".to_owned())
    );
}

#[test]
fn finish_rejects_a_direct_self_loop() {
    let mut builder = builder();
    let stage = builder.stage("stage", count());
    builder.connect([stage], stage);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::SelfLoop("stage".to_owned())
    );
}

#[test]
fn finish_rejects_a_multi_stage_cycle() {
    let mut builder = builder();
    let first = builder.stage("first", count());
    let second = builder.stage("second", count());
    let third = builder.stage("third", count());
    builder.connect([first], second);
    builder.connect([second], third);
    builder.connect([third], first);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::Cycle
    );
}

#[test]
fn finish_allows_fan_out() {
    let mut builder = builder();
    let source = builder.stage("source", source(0));
    let left = builder.stage("left", count());
    let right = builder.stage("right", count());
    builder.connect([source], left);
    builder.connect([source], right);

    let definition = builder.finish_definition().unwrap();

    assert_eq!(find_stage(&definition, "left").sources, ["source"]);
    assert_eq!(find_stage(&definition, "right").sources, ["source"]);
}

#[test]
fn finish_allows_zero_input_stages_and_disconnected_components() {
    let mut builder = builder();
    builder.stage("isolated", source(0));
    let source = builder.stage("source", source(1));
    let count = builder.stage("count", count());
    builder.connect([source], count);

    let definition = builder.finish_definition().unwrap();

    assert!(find_stage(&definition, "isolated").sources.is_empty());
    assert!(find_stage(&definition, "source").sources.is_empty());
    assert_eq!(find_stage(&definition, "count").sources, ["source"]);
}

#[test]
fn finish_rejects_a_cycle_in_one_of_multiple_components() {
    let mut builder = builder();
    builder.stage("isolated", source(0));
    let left = builder.stage("left", count());
    let right = builder.stage("right", count());
    builder.connect([left], right);
    builder.connect([right], left);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::Cycle
    );
}

fn codec_definition() -> FlowDefinition {
    let mut builder = builder();
    let source = builder.stage("source", source(7));
    let count = builder.stage("count", count());
    builder.connect([source], count);
    builder.finish_definition().unwrap()
}

fn codec_definition_with_ids(source_id: &str, count_id: &str) -> FlowDefinition {
    let mut builder = builder();
    let source = builder.stage(source_id, source(7));
    let count = builder.stage(count_id, count());
    builder.connect([source], count);
    builder.finish_definition().unwrap()
}

#[test]
fn codec_is_canonical_and_round_trips_ordered_sources() {
    let encoded = encode(&codec_definition()).unwrap();
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
fn decoder_round_trips_a_large_chain() {
    const STAGE_COUNT: usize = 4_096;

    let mut builder = builder();
    let mut previous = builder.stage("stage-0000", source(0));
    for index in 1..STAGE_COUNT {
        let current = builder.stage(format!("stage-{index:04}"), count());
        builder.connect([previous], current);
        previous = current;
    }
    let encoded = encode(&builder.finish_definition().unwrap()).unwrap();

    let decoded = decode(&encoded).unwrap();

    assert_eq!(decoded.stages().len(), STAGE_COUNT);
    assert_eq!(encode(&decoded).unwrap(), encoded);
}

#[test]
fn decoder_rejects_truncation_and_trailing_bytes() {
    let encoded = encode(&codec_definition()).unwrap();
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
    let mut encoded = encode(&codec_definition_with_ids("first", "other")).unwrap();
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
        FlowDefinitionError::Topology(TopologyError::DuplicateStageId("first".to_owned()))
    );
}

#[test]
fn decoder_rejects_semantic_bit_flips_and_checksum_damage() {
    let original = encode(&codec_definition()).unwrap();

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

    let mut changed_source = encode(&codec_definition_with_ids("first", "other")).unwrap();
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
