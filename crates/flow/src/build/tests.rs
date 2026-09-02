use std::num::NonZeroU64;

use dogpaddle_operation::{
    OperationDefinition, encode_definition,
    operation::{
        sink::DiscardDefinition, source::SequenceSourceDefinition, transform::CountDefinition,
    },
};
use dogpaddle_store::StoreError;

use crate::{
    error::{FlowError, retention_open_error},
    station::StationError,
};

use super::{
    FlowDefinitionError, FlowFactory, StationRef, TopologyError,
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

fn declare_output_capacities(builder: &mut FlowFactory) {
    let output_stations = builder
        .stations
        .iter()
        .enumerate()
        .filter_map(|(index, station)| {
            station.has_output().then_some((
                StationRef {
                    factory_token: builder.token,
                    index,
                },
                NonZeroU64::new(u64::try_from(index + 1).unwrap() * 1_024).unwrap(),
            ))
        })
        .collect::<Vec<_>>();
    for (station, capacity) in output_stations {
        builder.output_capacity_bytes(station, capacity);
    }
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
    let has_output = operation.kind().has_output();
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
    declare_output_capacities(&mut builder);
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
    declare_output_capacities(&mut builder);

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
                declare_output_capacities(&mut builder);

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
    declare_output_capacities(&mut builder);
    builder.finish_definition().unwrap()
}

fn codec_definition_with_ids(source_id: &str, count_id: &str) -> FlowDefinition {
    let mut builder = factory();
    let source = builder.station(source_id, source(7));
    let count = builder.station(count_id, count());
    let sink = builder.station("sink", discard());
    builder.connect([source], count);
    builder.connect([count], sink);
    declare_output_capacities(&mut builder);
    builder.finish_definition().unwrap()
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
    declare_output_capacities(&mut builder);
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
fn decoder_validates_capacity_against_the_decoded_operation_category() {
    let mut missing = encode(&codec_definition()).unwrap();
    let source_start = missing
        .windows(7_u64.to_be_bytes().len())
        .position(|window| window == 7_u64.to_be_bytes())
        .unwrap();
    let source_capacity = source_start + size_of::<u64>();
    missing[source_capacity..source_capacity + size_of::<u64>()]
        .copy_from_slice(&0_u64.to_be_bytes());
    let checksum_offset = missing.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&missing[..checksum_offset]);
    missing[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        decode(&missing).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::MissingOutputCapacity("source".to_owned()))
    );

    let mut unexpected = encode(&codec_definition()).unwrap();
    let sink_operation = unexpected
        .windows(b"dogpaddle.operation\0".len())
        .rposition(|window| window == b"dogpaddle.operation\0")
        .unwrap();
    let sink_capacity = sink_operation + 24;
    unexpected[sink_capacity..sink_capacity + size_of::<u64>()]
        .copy_from_slice(&1_u64.to_be_bytes());
    let checksum_offset = unexpected.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&unexpected[..checksum_offset]);
    unexpected[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        decode(&unexpected).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::UnexpectedOutputCapacity("sink".to_owned()))
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

#[test]
fn retention_open_error_preserves_store_error_classification() {
    let error = retention_open_error(
        "producer",
        StationError::Store(StoreError::CorruptAppendLog {
            reason: "test corruption",
        }),
    );

    assert!(matches!(
        error,
        FlowError::Store(StoreError::CorruptAppendLog {
            reason: "test corruption"
        })
    ));
}

#[test]
fn retention_open_error_maps_an_invariant_to_runtime_state() {
    let error = retention_open_error(
        "producer",
        StationError::RetentionHeadMismatch {
            head: 3,
            minimum: 4,
        },
    );

    assert!(matches!(
        error,
        FlowError::InvalidRuntimeState { station_id, reason }
            if station_id == "producer"
                && reason == "output retention head 3 does not equal minimum consumer cursor 4"
    ));
}
