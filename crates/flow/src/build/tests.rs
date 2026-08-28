use dogpaddle_operation::{
    OperationDefinition, encode_definition,
    operation::{
        sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
    },
};

use super::{
    FlowDefinitionError, FlowFactory, InvalidStationIdReason, StationRef, TopologyError,
    codec::{CHECKSUM_LENGTH, crc32, decode, encode},
    definition::{FlowDefinition, StationDefinition},
    validate::{validate_acyclic, validate_connections},
};

fn source(start: u64) -> SequenceSourceDefinition {
    SequenceSourceDefinition::new(start)
}

fn count() -> CountDefinition {
    CountDefinition::new()
}

fn discard() -> DiscardDefinition {
    DiscardDefinition::new()
}

fn factory() -> FlowFactory {
    FlowFactory::new("")
}

fn find_station<'a>(definition: &'a FlowDefinition, id: &str) -> &'a StationDefinition {
    definition
        .stations
        .iter()
        .find(|station| station.id == id)
        .unwrap()
}

fn finish_with_target<D>(
    operation: D,
    upstream_count: usize,
) -> Result<FlowDefinition, TopologyError>
where
    D: OperationDefinition,
{
    let mut builder = factory();
    let has_output = operation.category().has_output();
    let upstreams = (0..upstream_count)
        .map(|index| builder.station(format!("source-{index}"), source(index as u64)))
        .collect::<Vec<_>>();
    let target = builder.station("target", operation);
    if !upstreams.is_empty() {
        builder.connect(upstreams, target);
    }
    if has_output {
        let sink = builder.station("sink", discard());
        builder.connect([target], sink);
    }
    builder.finish_definition()
}

#[test]
fn finish_preserves_station_order_and_resolves_references() {
    let mut builder = factory();
    let first = builder.station("first", source(1));
    let second = builder.station("second", source(2));
    let target = builder.station("target", count());
    let first_sink = builder.station("first-sink", discard());
    let target_sink = builder.station("target-sink", discard());
    builder.connect([first], first_sink);
    builder.connect([second], target);
    builder.connect([target], target_sink);

    let definition = builder.finish_definition().unwrap();

    assert_eq!(
        definition
            .stations
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second", "target", "first-sink", "target-sink"]
    );
    let target = find_station(&definition, "target");
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
    let mut builder = factory();
    let first = builder.station("first", source(1));
    let second = builder.station("second", source(2));
    let target = builder.station("target", count());
    builder.connect([second, first, second], target);

    let sources =
        validate_connections(builder.token, &builder.stations, &builder.connections).unwrap();

    assert_eq!(sources[target.index].as_deref(), Some([1, 0, 1].as_slice()));
    assert_eq!(validate_acyclic(builder.stations.len(), &sources), Ok(()));
}

#[test]
fn finish_rejects_an_empty_definition() {
    assert_eq!(
        factory().finish_definition().unwrap_err(),
        TopologyError::EmptyTopology
    );
}

#[test]
fn finish_rejects_invalid_station_ids_in_declaration_order() {
    let mut builder = factory();
    builder.station("", source(0));
    builder.station("contains\0nul", source(1));

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::InvalidStationId {
            id: String::new(),
            reason: InvalidStationIdReason::Empty,
        }
    );

    let mut nul_builder = FlowFactory::new("");
    nul_builder.station("contains\0nul", source(0));
    assert_eq!(
        nul_builder.finish_definition().unwrap_err(),
        TopologyError::InvalidStationId {
            id: "contains\0nul".to_owned(),
            reason: InvalidStationIdReason::ContainsNul,
        }
    );
}

#[test]
fn finish_rejects_duplicate_station_ids_before_connections() {
    let mut builder = factory();
    let first = builder.station("same", source(0));
    builder.station("same", source(1));
    builder.connect(
        [first],
        StationRef {
            factory_token: 0,
            index: 0,
        },
    );

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::DuplicateStationId("same".to_owned())
    );
}

#[test]
fn finish_rejects_foreign_source_and_target_references() {
    let mut left = factory();
    let foreign = left.station("foreign", source(0));

    let mut right = factory();
    let target = right.station("target", count());
    right.connect([foreign], target);
    assert_eq!(
        right.finish_definition().unwrap_err(),
        TopologyError::ForeignStationRef(foreign)
    );

    let mut right = factory();
    let source = right.station("source", source(0));
    right.connect([source], foreign);
    assert_eq!(
        right.finish_definition().unwrap_err(),
        TopologyError::ForeignStationRef(foreign)
    );
}

#[test]
fn finish_rejects_an_explicit_empty_source_list() {
    let mut builder = factory();
    let target = builder.station("target", source(0));
    builder.connect([], target);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::EmptySources("target".to_owned())
    );
}

#[test]
fn finish_accepts_each_builtin_input_count() {
    let source_definition = finish_with_target(source(0), 0).unwrap();
    assert!(
        find_station(&source_definition, "target")
            .sources
            .is_empty()
    );

    let count_definition = finish_with_target(count(), 1).unwrap();
    assert_eq!(find_station(&count_definition, "target").sources.len(), 1);

    let discard_definition = finish_with_target(discard(), 1).unwrap();
    assert_eq!(find_station(&discard_definition, "target").sources.len(), 1);
}

#[test]
fn finish_rejects_forbidden_or_excess_inputs() {
    assert_eq!(
        finish_with_target(source(0), 1).unwrap_err(),
        TopologyError::InputCount {
            station: "target".to_owned(),
            expected: 0,
            actual: 1,
        }
    );
    assert_eq!(
        finish_with_target(count(), 2).unwrap_err(),
        TopologyError::InputCount {
            station: "target".to_owned(),
            expected: 1,
            actual: 2,
        }
    );
    assert_eq!(
        finish_with_target(discard(), 2).unwrap_err(),
        TopologyError::InputCount {
            station: "target".to_owned(),
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn finish_reports_a_non_source_root_before_its_missing_input() {
    assert_eq!(
        finish_with_target(count(), 0).unwrap_err(),
        TopologyError::RootIsNotSource("target".to_owned())
    );
    assert_eq!(
        finish_with_target(discard(), 0).unwrap_err(),
        TopologyError::RootIsNotSource("target".to_owned())
    );
}

#[test]
fn finish_rejects_setting_sources_twice() {
    let mut builder = factory();
    let first = builder.station("first", source(1));
    let second = builder.station("second", source(2));
    let target = builder.station("target", count());
    builder.connect([first], target);
    builder.connect([second], target);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::SourcesAlreadySet("target".to_owned())
    );
}

#[test]
fn finish_rejects_a_direct_self_loop() {
    let mut builder = factory();
    let station = builder.station("station", count());
    builder.connect([station], station);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::SelfLoop("station".to_owned())
    );
}

#[test]
fn finish_rejects_a_multi_station_cycle() {
    let mut builder = factory();
    let first = builder.station("first", count());
    let second = builder.station("second", count());
    let third = builder.station("third", count());
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
    let mut builder = factory();
    let source = builder.station("source", source(0));
    let left = builder.station("left", count());
    let right = builder.station("right", count());
    let left_sink = builder.station("left-sink", discard());
    let right_sink = builder.station("right-sink", discard());
    builder.connect([source], left);
    builder.connect([source], right);
    builder.connect([left], left_sink);
    builder.connect([right], right_sink);

    let definition = builder.finish_definition().unwrap();

    assert_eq!(find_station(&definition, "left").sources, ["source"]);
    assert_eq!(find_station(&definition, "right").sources, ["source"]);
}

#[test]
fn finish_allows_multiple_source_sink_components() {
    let mut builder = factory();
    let direct_source = builder.station("direct-source", source(0));
    let source = builder.station("source", source(1));
    let count = builder.station("count", count());
    let direct_sink = builder.station("direct-sink", discard());
    let count_sink = builder.station("count-sink", discard());
    builder.connect([direct_source], direct_sink);
    builder.connect([source], count);
    builder.connect([count], count_sink);

    let definition = builder.finish_definition().unwrap();

    assert!(
        find_station(&definition, "direct-source")
            .sources
            .is_empty()
    );
    assert!(find_station(&definition, "source").sources.is_empty());
    assert_eq!(find_station(&definition, "count").sources, ["source"]);
    assert_eq!(
        find_station(&definition, "direct-sink").sources,
        ["direct-source"]
    );
    assert_eq!(find_station(&definition, "count-sink").sources, ["count"]);
}

#[test]
fn finish_rejects_a_cycle_in_one_of_multiple_components() {
    let mut builder = factory();
    builder.station("isolated", source(0));
    let left = builder.station("left", count());
    let right = builder.station("right", count());
    builder.connect([left], right);
    builder.connect([right], left);

    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::Cycle
    );
}

#[test]
fn finish_matches_an_exhaustive_small_unary_graph_oracle() {
    const MAX_STATION_COUNT: usize = 5;
    const EXPECTED_GRAPH_COUNT: usize = 8_476;

    let mut visited = 0;

    for station_count in 1..=MAX_STATION_COUNT {
        for count_mask in 0..(1_usize << station_count) {
            let count_targets = (0..station_count)
                .filter(|index| count_mask & (1_usize << index) != 0)
                .collect::<Vec<_>>();
            let assignment_count = station_count.pow(u32::try_from(count_targets.len()).unwrap());

            for assignment in 0..assignment_count {
                visited += 1;
                let parents = decode_parent_assignment(station_count, &count_targets, assignment);
                let expected = classify_unary_graph(&parents);
                let graph = format!(
                    "station_count={station_count}, count_mask={count_mask:#b}, parents={parents:?}"
                );

                let mut builder = factory();
                let references = (0..station_count)
                    .map(|index| {
                        let id = station_id(index);
                        if parents[index].is_some() {
                            builder.station(id, count())
                        } else {
                            builder.station(id, source(index as u64))
                        }
                    })
                    .collect::<Vec<_>>();
                for &target in &count_targets {
                    builder.connect([references[parents[target].unwrap()]], references[target]);
                }
                let leaves = (0..station_count)
                    .filter(|candidate| !parents.contains(&Some(*candidate)))
                    .collect::<Vec<_>>();
                for &leaf in &leaves {
                    let sink = builder.station(format!("sink-{leaf}"), discard());
                    builder.connect([references[leaf]], sink);
                }

                match expected {
                    UnaryGraphClass::Acyclic => {
                        let definition = builder
                            .finish_definition()
                            .unwrap_or_else(|error| panic!("{graph}: rejected with {error:?}"));
                        let expected_ids = (0..station_count)
                            .map(station_id)
                            .chain(leaves.iter().map(|leaf| format!("sink-{leaf}")))
                            .collect::<Vec<_>>();
                        assert_eq!(
                            definition
                                .stations
                                .iter()
                                .map(|station| station.id.as_str())
                                .collect::<Vec<_>>(),
                            expected_ids.iter().map(String::as_str).collect::<Vec<_>>(),
                            "{graph}: declaration order changed"
                        );
                        for (target, station) in
                            definition.stations.iter().take(station_count).enumerate()
                        {
                            let expected_sources = parents[target]
                                .map(|parent| vec![station_id(parent)])
                                .unwrap_or_default();
                            assert_eq!(
                                station.sources, expected_sources,
                                "{graph}: source order changed for target {target}"
                            );
                        }
                        for (&leaf, station) in
                            leaves.iter().zip(&definition.stations[station_count..])
                        {
                            assert_eq!(
                                station.sources,
                                [station_id(leaf)],
                                "{graph}: sink source changed for leaf {leaf}"
                            );
                        }
                    }
                    UnaryGraphClass::SelfLoop(target) => assert_eq!(
                        builder.finish_definition().unwrap_err(),
                        TopologyError::SelfLoop(station_id(target)),
                        "{graph}: direct cycle classification changed"
                    ),
                    UnaryGraphClass::Cycle => assert_eq!(
                        builder.finish_definition().unwrap_err(),
                        TopologyError::Cycle,
                        "{graph}: indirect cycle classification changed"
                    ),
                }
            }
        }
    }

    assert_eq!(visited, EXPECTED_GRAPH_COUNT);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnaryGraphClass {
    Acyclic,
    SelfLoop(usize),
    Cycle,
}

fn decode_parent_assignment(
    station_count: usize,
    count_targets: &[usize],
    mut assignment: usize,
) -> Vec<Option<usize>> {
    let mut parents = vec![None; station_count];
    for &target in count_targets {
        parents[target] = Some(assignment % station_count);
        assignment /= station_count;
    }
    assert_eq!(assignment, 0);
    parents
}

fn classify_unary_graph(parents: &[Option<usize>]) -> UnaryGraphClass {
    if let Some((target, _)) = parents
        .iter()
        .enumerate()
        .find(|(target, parent)| **parent == Some(*target))
    {
        return UnaryGraphClass::SelfLoop(target);
    }

    for start in 0..parents.len() {
        let mut visited = vec![false; parents.len()];
        let mut current = Some(start);
        while let Some(station) = current {
            if visited[station] {
                return UnaryGraphClass::Cycle;
            }
            visited[station] = true;
            current = parents[station];
        }
    }
    UnaryGraphClass::Acyclic
}

fn station_id(index: usize) -> String {
    format!("station-{index}")
}

fn codec_definition() -> FlowDefinition {
    let mut builder = factory();
    let source = builder.station("source", source(7));
    let count = builder.station("count", count());
    let sink = builder.station("sink", discard());
    builder.connect([source], count);
    builder.connect([count], sink);
    builder.finish_definition().unwrap()
}

fn codec_definition_with_ids(source_id: &str, count_id: &str) -> FlowDefinition {
    let mut builder = factory();
    let source = builder.station(source_id, source(7));
    let count = builder.station(count_id, count());
    let sink = builder.station("sink", discard());
    builder.connect([source], count);
    builder.connect([count], sink);
    builder.finish_definition().unwrap()
}

#[test]
fn codec_is_canonical_and_round_trips_ordered_sources() {
    let encoded = encode(&codec_definition()).unwrap();
    let mut expected = [
        b"dogpaddle.flow\0".as_slice(),
        &[0, 1, 0, 0, 0, 3],
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
        &[0, 0, 0, 4],
        b"sink",
        &[0, 0, 0, 24],
        b"dogpaddle.operation\0",
        &[0, 1, 0, 3],
        &[0, 0, 0, 1, 0, 0, 0, 5],
        b"count",
    ]
    .concat();
    expected.extend_from_slice(&crc32(&expected).to_be_bytes());
    assert_eq!(encoded, expected);

    let decoded = decode(&encoded).unwrap();
    assert_eq!(encode(&decoded).unwrap(), encoded);
}

#[test]
fn decoder_round_trips_a_large_chain() {
    const STATION_COUNT: usize = 4_096;

    let mut builder = factory();
    let mut previous = builder.station("station-0000", source(0));
    for index in 1..STATION_COUNT {
        let current = builder.station(format!("station-{index:04}"), count());
        builder.connect([previous], current);
        previous = current;
    }
    let sink = builder.station("sink", discard());
    builder.connect([previous], sink);
    let encoded = encode(&builder.finish_definition().unwrap()).unwrap();

    let decoded = decode(&encoded).unwrap();

    assert_eq!(decoded.stations().len(), STATION_COUNT + 1);
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
fn decoder_validates_all_station_ids_before_resolving_sources() {
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
        FlowDefinitionError::Topology(TopologyError::DuplicateStationId("first".to_owned()))
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
