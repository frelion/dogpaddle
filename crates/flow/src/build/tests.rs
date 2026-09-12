use std::num::{NonZeroU32, NonZeroU64};

use dogpaddle_operation::{
    OperationDefinition, OperationKind,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{RunningEventCountDefinition, UnionAllDefinition},
    },
};
use dogpaddle_store::StoreError;

use crate::{
    error::{FlowError, runtime_state_error},
    station::StationError,
};

use super::{
    FlowDefinitionError, FlowFactory, StationRef, TopologyError,
    codec::{CHECKSUM_LENGTH, crc32, decode, encode},
    definition::FlowDefinition,
    validate::{topological_schedule, validate_connections},
};

fn scan(start: u64) -> SequenceScanDefinition {
    SequenceScanDefinition::new(start)
}

fn count() -> RunningEventCountDefinition {
    RunningEventCountDefinition::new()
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

fn finish_with_target<D>(operation: D, input_count: usize) -> Result<FlowDefinition, TopologyError>
where
    D: OperationDefinition,
{
    let mut builder = factory();
    let has_output = operation.kind().has_output();
    let inputs = (0..input_count)
        .map(|index| builder.station(format!("scan-{index}"), scan(index as u64)))
        .collect::<Vec<_>>();
    let target = builder.station("target", operation);
    if !inputs.is_empty() {
        builder.connect(inputs, target);
    }
    if has_output {
        let sink = builder.station("sink", discard());
        builder.connect([target], sink);
    }
    declare_output_capacities(&mut builder);
    builder.finish_definition()
}

#[test]
fn connection_validation_preserves_n_ary_order_and_repeated_inputs() {
    let mut builder = factory();
    let first = builder.station("first", scan(1));
    let second = builder.station("second", scan(2));
    let target = builder.station("target", count());
    builder.connect([second, first, second], target);

    let inputs =
        validate_connections(builder.token, &builder.stations, &builder.connections).unwrap();

    assert_eq!(inputs[target.index].as_deref(), Some([1, 0, 1].as_slice()));
    assert_eq!(topological_schedule(&inputs), Ok(vec![0, 1, 2]));
}

#[test]
fn finish_rejects_forbidden_or_excess_inputs() {
    assert_input_count(finish_with_target(scan(0), 1), 0, 1);
    assert_input_count(finish_with_target(count(), 2), 1, 2);
    let union = || UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
    assert_input_count(finish_with_target(union(), 1), 2, 1);
    assert_input_count(finish_with_target(union(), 3), 2, 3);
    assert_input_count(
        finish_with_target(UnionAllDefinition::new(NonZeroU32::MAX), 1),
        usize::try_from(u32::MAX).unwrap(),
        1,
    );
    assert_input_count(finish_with_target(discard(), 2), 1, 2);
}

fn assert_input_count(
    result: Result<FlowDefinition, TopologyError>,
    expected: usize,
    actual: usize,
) {
    assert_eq!(
        result.unwrap_err(),
        TopologyError::InputCount {
            station: "target".to_owned(),
            expected,
            actual,
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
                assert_unary_graph(station_count, &count_targets, &parents, expected, &graph);
            }
        }
    }

    assert_eq!(visited, EXPECTED_GRAPH_COUNT);
}

fn assert_unary_graph(
    station_count: usize,
    count_targets: &[usize],
    parents: &[Option<usize>],
    expected: UnaryGraphClass,
    graph: &str,
) {
    let (builder, leaves) = unary_graph_factory(station_count, count_targets, parents);
    match expected {
        UnaryGraphClass::Acyclic => {
            let definition = builder
                .finish_definition()
                .unwrap_or_else(|error| panic!("{graph}: rejected with {error:?}"));
            assert_acyclic_unary_graph(&definition, station_count, parents, &leaves, graph);
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

fn unary_graph_factory(
    station_count: usize,
    count_targets: &[usize],
    parents: &[Option<usize>],
) -> (FlowFactory, Vec<usize>) {
    let mut builder = factory();
    let references = (0..station_count)
        .map(|index| {
            let id = station_id(index);
            if parents[index].is_some() {
                builder.station(id, count())
            } else {
                builder.station(id, scan(index as u64))
            }
        })
        .collect::<Vec<_>>();
    for &target in count_targets {
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
    (builder, leaves)
}

fn assert_acyclic_unary_graph(
    definition: &FlowDefinition,
    station_count: usize,
    parents: &[Option<usize>],
    leaves: &[usize],
    graph: &str,
) {
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
    for (station_index, station) in definition.stations.iter().take(station_count).enumerate() {
        let expected_kind = parents[station_index].map_or(OperationKind::Scan, |_| {
            OperationKind::AtomicTransform(NonZeroU32::MIN)
        });
        assert_eq!(station.first_operation().kind(), expected_kind, "{graph}");
        let expected_inputs = parents[station_index]
            .map(|parent| vec![station_id(parent)])
            .unwrap_or_default();
        assert_eq!(
            station.inputs().collect::<Vec<_>>(),
            expected_inputs
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "{graph}: input order changed for station {station_index}"
        );
    }
    for (&leaf, station) in leaves.iter().zip(&definition.stations[station_count..]) {
        assert_eq!(
            station.inputs().collect::<Vec<_>>(),
            [station_id(leaf)]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "{graph}: sink input changed for leaf {leaf}"
        );
    }
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
    let scan = builder.station("scan", scan(7));
    let count = builder.station("count", count());
    let sink = builder.station("sink", discard());
    builder.connect([scan], count);
    builder.connect([count], sink);
    declare_output_capacities(&mut builder);
    builder.finish_definition().unwrap()
}

fn codec_definition_with_ids(scan_id: &str, count_id: &str) -> FlowDefinition {
    let mut builder = factory();
    let scan = builder.station(scan_id, scan(7));
    let count = builder.station(count_id, count());
    let sink = builder.station("sink", discard());
    builder.connect([scan], count);
    builder.connect([count], sink);
    declare_output_capacities(&mut builder);
    builder.finish_definition().unwrap()
}

#[test]
fn decoder_round_trips_a_large_chain() {
    const STATION_COUNT: usize = 4_096;

    let mut builder = factory();
    let mut previous = builder.station("station-0000", scan(0));
    for index in 1..STATION_COUNT {
        let current = builder.station(format!("station-{index:04}"), count());
        builder.connect([previous], current);
        previous = current;
    }
    let sink = builder.station("sink", discard());
    builder.connect([previous], sink);
    declare_output_capacities(&mut builder);
    let encoded = encode(&builder.finish_definition().unwrap()).unwrap();

    let (decoded, _) = decode(&encoded).unwrap();

    assert_eq!(decoded.stations().len(), STATION_COUNT + 1);
    assert_eq!(encode(&decoded).unwrap(), encoded);
}

#[test]
fn decoder_rejects_empty_and_non_atomic_station_tails() {
    let mut empty = codec_definition();
    empty.stations[0].operations.clear();
    assert_eq!(
        decode(&encode(&empty).unwrap()).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::EmptyOperationList("scan".to_owned()))
    );

    let mut invalid_tail = codec_definition();
    invalid_tail.stations[0]
        .operations
        .push(Box::new(discard()));
    assert_eq!(
        decode(&encode(&invalid_tail).unwrap()).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::InvalidAppendedOperation {
            station: "scan".to_owned(),
            operation: 1,
        })
    );
}

#[test]
fn decoder_validates_capacity_against_the_decoded_operation_category() {
    let encoded = encode(&codec_definition()).unwrap();
    let scan_start = encoded
        .windows(7_u64.to_be_bytes().len())
        .position(|window| window == 7_u64.to_be_bytes())
        .unwrap();
    let scan_output = scan_start + size_of::<u64>() + size_of::<u32>();

    let mut invalid_presence = encoded.clone();
    invalid_presence[scan_output] = 2;
    rewrite_internal_checksum(&mut invalid_presence);
    assert_eq!(
        decode(&invalid_presence).unwrap_err(),
        FlowDefinitionError::InvalidOutputPresence(2)
    );

    let mut zero_capacity = encoded.clone();
    zero_capacity[scan_output + 1..scan_output + 1 + size_of::<u64>()]
        .copy_from_slice(&0_u64.to_be_bytes());
    rewrite_internal_checksum(&mut zero_capacity);
    assert_eq!(
        decode(&zero_capacity).unwrap_err(),
        FlowDefinitionError::ZeroOutputCapacity
    );

    let mut missing = encoded.clone();
    missing.splice(scan_output..scan_output + 9, [0]);
    let checksum_offset = missing.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&missing[..checksum_offset]);
    missing[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        decode(&missing).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::MissingOutputCapacity("scan".to_owned()))
    );

    let mut unexpected = encode(&codec_definition()).unwrap();
    let sink_input = unexpected
        .windows(b"count".len())
        .rposition(|window| window == b"count")
        .unwrap();
    let sink_output = sink_input + b"count".len();
    let encoded_output = std::iter::once(1).chain(NonZeroU64::MIN.get().to_be_bytes());
    unexpected.splice(sink_output..=sink_output, encoded_output);
    let checksum_offset = unexpected.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&unexpected[..checksum_offset]);
    unexpected[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        decode(&unexpected).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::UnexpectedOutputCapacity("sink".to_owned()))
    );
}

fn rewrite_internal_checksum(encoded: &mut [u8]) {
    let checksum_offset = encoded.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}

#[test]
fn decoder_validates_all_station_ids_before_resolving_inputs() {
    let mut encoded = encode(&codec_definition_with_ids("first", "other")).unwrap();
    let duplicate = encoded
        .windows(b"other".len())
        .position(|window| window == b"other")
        .unwrap();
    encoded[duplicate..duplicate + b"first".len()].copy_from_slice(b"first");
    let input_reference = encoded
        .windows(b"first".len())
        .rposition(|window| window == b"first")
        .unwrap();
    encoded[input_reference..input_reference + b"ghost".len()].copy_from_slice(b"ghost");
    let checksum_offset = encoded.len() - CHECKSUM_LENGTH;
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

    assert_eq!(
        decode(&encoded).unwrap_err(),
        FlowDefinitionError::Topology(TopologyError::DuplicateStationId("first".to_owned()))
    );
}

#[test]
fn checksum_uses_the_stable_ieee_crc32_algorithm() {
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
}

#[test]
fn runtime_state_error_preserves_store_error_classification() {
    let error = runtime_state_error(
        "producer",
        StationError::Store(StoreError::CorruptSubscribedLog {
            reason: "test corruption",
        }),
    );

    assert!(matches!(
        error,
        FlowError::Store(StoreError::CorruptSubscribedLog {
            reason: "test corruption"
        })
    ));
}

#[test]
fn runtime_state_error_maps_an_invariant_to_runtime_state() {
    let error = runtime_state_error("union", StationError::MissingActiveInput);

    assert!(matches!(
        error,
        FlowError::InvalidRuntimeState { station_id, reason }
            if station_id == "union"
                && reason == "station has inputs but no durable active input"
    ));
}
