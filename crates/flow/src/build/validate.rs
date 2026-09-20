use std::{collections::HashSet, num::NonZeroU64};

use dogpaddle_operation::OperationKind;
use thiserror::Error;

use super::{
    DeclaredOperation, OperationRef,
    definition::{FlowDefinition, StationDefinition},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvalidStationIdReason {
    /// The station ID is empty.
    Empty,
    /// The station ID contains a NUL character.
    ContainsNul,
}

/// Failure while validating a Flow's static topology.
#[derive(Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TopologyError {
    /// A Flow must contain at least one station.
    #[error("a flow must contain at least one station")]
    EmptyTopology,
    /// A station ID violates the stable identity rules.
    #[error("invalid station ID {id:?}: {reason:?}")]
    InvalidStationId {
        /// The rejected ID.
        id: String,
        /// Why the ID was rejected.
        reason: InvalidStationIdReason,
    },
    /// Two stations declared the same stable ID.
    #[error("duplicate station ID {0:?}")]
    DuplicateStationId(String),
    /// An input or materialization does not reference an earlier declaration
    /// in the same factory.
    #[error("operation reference is not valid in this flow factory")]
    ForeignOperationRef(OperationRef),
    /// A station directly references itself.
    #[error("station {0:?} directly references itself")]
    SelfLoop(String),
    /// The topology contains an indirect cycle.
    #[error("flow topology contains a cycle")]
    Cycle,
    /// A root Station is not a Scan.
    #[error("root station {0:?} is not a scan")]
    RootIsNotScan(String),
    /// A terminal Station is not a sink.
    #[error("terminal station {0:?} is not a sink")]
    TerminalIsNotSink(String),
    /// The connected input count does not match the Station's input arity.
    #[error("station {station:?} requires {expected} inputs but received {actual}")]
    InputCount {
        /// Station whose input arity did not match.
        station: String,
        /// Required input count.
        expected: usize,
        /// Connected input count.
        actual: usize,
    },
    /// A connection uses an outputless Station as an input.
    #[error("station {station:?} cannot read from outputless input station {input_station:?}")]
    InputHasNoOutput {
        /// Input Station without an output stream.
        input_station: String,
        /// Station that attempted to consume it.
        station: String,
    },
    /// A Station with an output has no declared retained-byte capacity.
    #[error("output capacity for station {0:?} is missing")]
    MissingOutputCapacity(String),
    /// An outputless Station declared an output capacity.
    #[error("outputless station {0:?} cannot declare an output capacity")]
    UnexpectedOutputCapacity(String),
    /// A Station's output capacity was declared more than once.
    #[error("output capacity for station {0:?} was already set")]
    OutputCapacityAlreadySet(String),
    /// A decoded Station contains no Operation.
    #[error("station {0:?} contains no operation")]
    EmptyOperationList(String),
    /// The current final Operation requires a Station boundary.
    #[error("station {0:?} cannot append another operation")]
    StationCannotBeExtended(String),
    /// Only a single-input atomic transform can follow another Operation.
    #[error("station {station:?} operation {operation} must be a single-input atomic transform")]
    InvalidAppendedOperation {
        /// Station containing the invalid Operation.
        station: String,
        /// Zero-based Operation ordinal.
        operation: usize,
    },
}

pub(super) fn finish_definition(
    owner_identity: Option<[u8; 32]>,
    token: u64,
    operations: Vec<DeclaredOperation>,
    default_capacity: NonZeroU64,
    materializations: &[(OperationRef, NonZeroU64)],
) -> Result<FlowDefinition, TopologyError> {
    validate_ids(operations.iter().map(|operation| operation.id.as_str()))?;
    let mut consumers = vec![0_usize; operations.len()];
    for (index, operation) in operations.iter().enumerate() {
        let expected = usize::try_from(operation.definition.kind().input_count())
            .expect("Operation arity fits usize");
        if operation.inputs.len() != expected {
            return Err(TopologyError::InputCount {
                station: operation.id.clone(),
                expected,
                actual: operation.inputs.len(),
            });
        }
        for input in &operation.inputs {
            let input = resolve_ref(token, index, *input)?;
            if !operations[input].definition.kind().has_output() {
                return Err(TopologyError::InputHasNoOutput {
                    input_station: operations[input].id.clone(),
                    station: operation.id.clone(),
                });
            }
            consumers[input] += 1;
        }
    }
    let mut capacities = vec![None; operations.len()];
    for (reference, capacity) in materializations {
        let index = resolve_ref(token, operations.len(), *reference)?;
        if !operations[index].definition.kind().has_output() {
            return Err(TopologyError::UnexpectedOutputCapacity(
                operations[index].id.clone(),
            ));
        }
        if capacities[index].replace(*capacity).is_some() {
            return Err(TopologyError::OutputCapacityAlreadySet(
                operations[index].id.clone(),
            ));
        }
    }

    let mut stations: Vec<StationDefinition> = Vec::new();
    let mut station_by_operation: Vec<usize> = Vec::with_capacity(operations.len());
    for operation in operations {
        let is_atomic = matches!(
            operation.definition.kind(),
            OperationKind::AtomicTransform(count) if count.get() == 1
        );
        let fused_station = operation.inputs.first().and_then(|input| {
            let station = station_by_operation[input.index];
            let last = stations[station]
                .operations
                .last()
                .expect("nonempty program");
            // A sole consumer guarantees this input is still its Station's tail.
            (is_atomic
                && consumers[input.index] == 1
                && capacities[input.index].is_none()
                && last.kind().allows_atomic_tail())
            .then_some(station)
        });
        let capacity = capacities[station_by_operation.len()].unwrap_or(default_capacity);
        let station = if let Some(station) = fused_station {
            stations[station].operations.push(operation.definition);
            stations[station].output_capacity_bytes = Some(capacity);
            station
        } else {
            let mut station = StationDefinition::new(operation.id, operation.definition);
            station.inputs = operation
                .inputs
                .iter()
                .map(|input| stations[station_by_operation[input.index]].id.clone())
                .collect();
            station.output_capacity_bytes = station.has_output().then_some(capacity);
            stations.push(station);
            stations.len() - 1
        };
        station_by_operation.push(station);
    }
    // The canonical decoder owns durable graph validation and scheduling. The
    // declaration order already guarantees that the input graph is acyclic.
    Ok(FlowDefinition::new(owner_identity, stations))
}

fn validate_station_programs(stations: &[StationDefinition]) -> Result<(), TopologyError> {
    for station in stations {
        let Some((first, tail)) = station.operations.split_first() else {
            return Err(TopologyError::EmptyOperationList(station.id.clone()));
        };
        if !tail.is_empty() && !first.kind().allows_atomic_tail() {
            return Err(TopologyError::StationCannotBeExtended(station.id.clone()));
        }
        for (operation, definition) in tail.iter().enumerate() {
            if !matches!(
                definition.kind(),
                OperationKind::AtomicTransform(count) if count.get() == 1
            ) {
                return Err(TopologyError::InvalidAppendedOperation {
                    station: station.id.clone(),
                    operation: operation + 1,
                });
            }
        }
    }
    Ok(())
}

pub(super) fn validate_decoded_topology(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<Vec<usize>, TopologyError> {
    validate_station_programs(stations)?;
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        if inputs
            .as_ref()
            .is_some_and(|inputs| inputs.contains(&station))
        {
            return Err(TopologyError::SelfLoop(stations[station].id.clone()));
        }
    }
    let schedule = validate_topology(stations, inputs_by_station)?;
    validate_output_capacities(stations)?;
    Ok(schedule)
}

fn validate_output_capacities(stations: &[StationDefinition]) -> Result<(), TopologyError> {
    for station in stations {
        match (station.has_output(), station.output_capacity_bytes) {
            (true, None) => {
                return Err(TopologyError::MissingOutputCapacity(station.id.clone()));
            }
            (false, Some(_)) => {
                return Err(TopologyError::UnexpectedOutputCapacity(station.id.clone()));
            }
            (true, Some(_)) | (false, None) => {}
        }
    }
    Ok(())
}

fn validate_topology(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<Vec<usize>, TopologyError> {
    let schedule = topological_schedule(inputs_by_station)?;
    validate_endpoints(stations, inputs_by_station)?;
    validate_input_counts(stations, inputs_by_station)?;
    validate_inputs_have_output(stations, inputs_by_station)?;
    Ok(schedule)
}

fn validate_endpoints(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    let mut has_consumer = vec![false; stations.len()];
    for input in inputs_by_station.iter().flatten().flatten() {
        has_consumer[*input] = true;
    }

    for (index, station) in stations.iter().enumerate() {
        let is_root = inputs_by_station[index].as_ref().is_none_or(Vec::is_empty);
        if is_root && !station.is_scan() {
            return Err(TopologyError::RootIsNotScan(station.id.clone()));
        }
        if !has_consumer[index] && !station.is_sink() {
            return Err(TopologyError::TerminalIsNotSink(station.id.clone()));
        }
    }
    Ok(())
}

fn validate_inputs_have_output(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        for input in inputs.iter().flatten() {
            if !stations[*input].has_output() {
                return Err(TopologyError::InputHasNoOutput {
                    input_station: stations[*input].id.clone(),
                    station: stations[station].id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_input_counts(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (station, inputs) in stations.iter().zip(inputs_by_station) {
        let expected = station.input_count();
        let actual = inputs.as_ref().map_or(0, Vec::len);
        if actual != expected {
            return Err(TopologyError::InputCount {
                station: station.id.clone(),
                expected,
                actual,
            });
        }
    }
    Ok(())
}

pub(super) fn validate_station_ids(stations: &[StationDefinition]) -> Result<(), TopologyError> {
    validate_ids(stations.iter().map(|station| station.id.as_str()))
}

fn validate_ids<'a>(ids: impl IntoIterator<Item = &'a str>) -> Result<(), TopologyError> {
    let mut seen = HashSet::new();
    for id in ids {
        let reason = if id.is_empty() {
            Some(InvalidStationIdReason::Empty)
        } else if id.as_bytes().contains(&0) {
            Some(InvalidStationIdReason::ContainsNul)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(TopologyError::InvalidStationId {
                id: id.to_owned(),
                reason,
            });
        }
        if !seen.insert(id) {
            return Err(TopologyError::DuplicateStationId(id.to_owned()));
        }
    }
    if seen.is_empty() {
        return Err(TopologyError::EmptyTopology);
    }
    Ok(())
}

fn resolve_ref(
    token: u64,
    station_count: usize,
    reference: OperationRef,
) -> Result<usize, TopologyError> {
    if reference.factory_token != token || reference.index >= station_count {
        Err(TopologyError::ForeignOperationRef(reference))
    } else {
        Ok(reference.index)
    }
}

pub(super) fn topological_schedule(
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<Vec<usize>, TopologyError> {
    let station_count = inputs_by_station.len();
    let mut indegrees = vec![0_usize; station_count];
    let mut consumers_by_station = vec![Vec::new(); station_count];
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        for input in inputs.iter().flatten() {
            indegrees[station] += 1;
            consumers_by_station[*input].push(station);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(station, indegree)| (*indegree == 0).then_some(station))
        .collect::<Vec<_>>();
    let mut schedule = Vec::with_capacity(station_count);
    while !ready.is_empty() {
        let mut next = Vec::new();
        for station in ready {
            schedule.push(station);
            for consumer in &consumers_by_station[station] {
                indegrees[*consumer] -= 1;
                if indegrees[*consumer] == 0 {
                    next.push(*consumer);
                }
            }
        }
        next.sort_unstable();
        ready = next;
    }

    if schedule.len() == station_count {
        Ok(schedule)
    } else {
        Err(TopologyError::Cycle)
    }
}
