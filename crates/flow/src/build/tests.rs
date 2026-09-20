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
    FlowDefinitionError, FlowFactory, TopologyError,
    codec::{CHECKSUM_LENGTH, crc32, decode, encode},
    definition::{FlowDefinition, StationDefinition},
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

fn finish_with_target<D>(operation: D, input_count: usize) -> Result<FlowDefinition, TopologyError>
where
    D: OperationDefinition,
{
    let mut builder = factory();
    let has_output = operation.kind().has_output();
    let inputs = (0..input_count)
        .map(|index| builder.operation(format!("scan-{index}"), Box::new(scan(index as u64)), []))
        .collect::<Vec<_>>();
    let target = builder.operation("target", Box::new(operation), inputs);
    if has_output {
        builder.operation("sink", Box::new(discard()), [target]);
    }
    builder.finish_definition()
}

#[test]
fn declaration_preserves_n_ary_order_and_repeated_inputs() {
    let mut builder = factory();
    let first = builder.operation("first", Box::new(scan(1)), []);
    let second = builder.operation("second", Box::new(scan(2)), []);
    let target = builder.operation(
        "target",
        Box::new(UnionAllDefinition::new(NonZeroU32::new(3).unwrap())),
        [second, first, second],
    );
    builder.operation("sink", Box::new(discard()), [target]);
    let definition = builder.finish_definition().unwrap();
    assert_eq!(definition.stations[2].inputs, ["second", "first", "second"]);
    let (decoded, _) = decode(&encode(&definition).unwrap()).unwrap();
    assert_eq!(decoded.stations[2].inputs, ["second", "first", "second"]);
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
    let (definition, leaves) = unary_graph_definition(station_count, count_targets, parents);
    let result = decode(&encode(&definition).unwrap());
    match expected {
        UnaryGraphClass::Acyclic => {
            let (definition, _) =
                result.unwrap_or_else(|error| panic!("{graph}: rejected with {error:?}"));
            assert_acyclic_unary_graph(&definition, station_count, parents, &leaves, graph);
        }
        UnaryGraphClass::SelfLoop(target) => assert_eq!(
            result.unwrap_err(),
            FlowDefinitionError::Topology(TopologyError::SelfLoop(station_id(target))),
            "{graph}: direct cycle classification changed"
        ),
        UnaryGraphClass::Cycle => assert_eq!(
            result.unwrap_err(),
            FlowDefinitionError::Topology(TopologyError::Cycle),
            "{graph}: indirect cycle classification changed"
        ),
    }
}

fn unary_graph_definition(
    station_count: usize,
    count_targets: &[usize],
    parents: &[Option<usize>],
) -> (FlowDefinition, Vec<usize>) {
    let mut stations = (0..station_count)
        .map(|index| {
            let operation: Box<dyn OperationDefinition> = if parents[index].is_some() {
                Box::new(count())
            } else {
                Box::new(scan(index as u64))
            };
            let mut station = StationDefinition::new(station_id(index), operation);
            station.output_capacity_bytes = Some(NonZeroU64::MIN);
            station
        })
        .collect::<Vec<_>>();
    for &target in count_targets {
        stations[target].inputs = vec![station_id(parents[target].unwrap())];
    }
    let leaves = (0..station_count)
        .filter(|candidate| !parents.contains(&Some(*candidate)))
        .collect::<Vec<_>>();
    for &leaf in &leaves {
        let mut sink = StationDefinition::new(format!("sink-{leaf}"), Box::new(discard()));
        sink.inputs = vec![station_id(leaf)];
        stations.push(sink);
    }
    (FlowDefinition::new(None, stations), leaves)
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
    codec_definition_with_ids("scan", "count")
}

fn codec_definition_with_ids(scan_id: &str, count_id: &str) -> FlowDefinition {
    let mut builder = factory();
    let scan = builder.operation(scan_id, Box::new(scan(7)), []);
    builder.materialize(scan, NonZeroU64::new(1024).unwrap());
    let count = builder.operation(count_id, Box::new(count()), [scan]);
    builder.materialize(count, NonZeroU64::new(2048).unwrap());
    builder.operation("sink", Box::new(discard()), [count]);
    builder.finish_definition().unwrap()
}

#[test]
fn decoder_round_trips_a_large_chain() {
    const STATION_COUNT: usize = 4_096;
    let mut builder = factory();
    let mut previous = builder.operation("station-0000", Box::new(scan(0)), []);
    for index in 1..STATION_COUNT {
        builder.materialize(previous, NonZeroU64::MIN);
        previous = builder.operation(format!("station-{index:04}"), Box::new(count()), [previous]);
    }
    builder.operation("sink", Box::new(discard()), [previous]);
    let encoded = encode(&builder.finish_definition().unwrap()).unwrap();
    let (decoded, _) = decode(&encoded).unwrap();
    assert_eq!(decoded.stations().len(), STATION_COUNT + 1);
    assert_eq!(encode(&decoded).unwrap(), encoded);
}

#[test]
fn owner_identity_round_trips_exactly() {
    let identity = [0xa5; 32];
    let mut definition = codec_definition();
    definition.owner_identity = Some(identity);

    let encoded = encode(&definition).unwrap();
    let (decoded, _) = decode(&encoded).unwrap();

    assert_eq!(decoded.owner_identity(), Some(identity));
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

#[test]
fn planner_fuses_linear_operations_and_preserves_the_heads_identity() {
    let mut builder = factory();
    let source = builder.operation("source", Box::new(scan(0)), []);
    let first = builder.operation("first", Box::new(count()), [source]);
    let second = builder.operation("second", Box::new(count()), [first]);
    builder.operation("sink", Box::new(discard()), [second]);
    let definition = builder.finish_definition().unwrap();
    assert_eq!(definition.stations.len(), 2);
    assert_eq!(definition.stations[0].id, "source");
    assert_eq!(definition.stations[0].operations.len(), 3);
    assert_eq!(
        definition.stations[0].output_capacity_bytes.unwrap().get(),
        64 * 1024 * 1024
    );
    assert_eq!(definition.stations[1].inputs, ["source"]);
    decode(&encode(&definition).unwrap()).unwrap();
}

#[test]
fn materialization_ends_a_fused_program_and_overrides_its_capacity() {
    let mut builder = factory();
    builder.output_capacity_bytes(NonZeroU64::new(8192).unwrap());
    let source = builder.operation("source", Box::new(scan(0)), []);
    let first = builder.operation("first", Box::new(count()), [source]);
    builder.materialize(first, NonZeroU64::new(1024).unwrap());
    let second = builder.operation("second", Box::new(count()), [first]);
    builder.operation("sink", Box::new(discard()), [second]);
    let definition = builder.finish_definition().unwrap();
    assert_eq!(definition.stations.len(), 3);
    assert_eq!(definition.stations[0].operations.len(), 2);
    assert_eq!(
        definition.stations[0].output_capacity_bytes.unwrap().get(),
        1024
    );
    assert_eq!(definition.stations[1].id, "second");
    assert_eq!(
        definition.stations[1].output_capacity_bytes.unwrap().get(),
        8192
    );
    decode(&encode(&definition).unwrap()).unwrap();
}

#[test]
fn repeated_edges_and_fanout_prevent_absorbing_the_producer() {
    let mut builder = factory();
    let source = builder.operation("source", Box::new(scan(0)), []);
    let union = builder.operation(
        "union",
        Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        [source, source],
    );
    let count = builder.operation("count", Box::new(count()), [union]);
    builder.operation("sink", Box::new(discard()), [count]);
    builder.operation("other_sink", Box::new(discard()), [source]);
    let definition = builder.finish_definition().unwrap();
    assert_eq!(definition.stations.len(), 4);
    assert_eq!(definition.stations[0].operations.len(), 1);
    assert_eq!(definition.stations[1].inputs, ["source", "source"]);
    assert_eq!(definition.stations[1].operations.len(), 2);
    decode(&encode(&definition).unwrap()).unwrap();
}

#[test]
fn planner_checks_absorbed_ids_and_foreign_inputs() {
    let mut builder = factory();
    let source = builder.operation("duplicate", Box::new(scan(0)), []);
    let count = builder.operation("duplicate", Box::new(count()), [source]);
    builder.operation("sink", Box::new(discard()), [count]);
    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::DuplicateStationId("duplicate".to_owned())
    );

    let mut other = factory();
    let foreign = other.operation("source", Box::new(scan(0)), []);
    let mut builder = factory();
    builder.operation("sink", Box::new(discard()), [foreign]);
    assert_eq!(
        builder.finish_definition().unwrap_err(),
        TopologyError::ForeignOperationRef(foreign)
    );
}
