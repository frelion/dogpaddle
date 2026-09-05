use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroU64,
};

use thiserror::Error;

use super::{
    StationRef,
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
    /// A connection used a reference created by another factory.
    #[error("station reference belongs to another flow factory")]
    ForeignStationRef(StationRef),
    /// `connect` was called without any inputs.
    #[error("station {0:?} was connected with an empty input list")]
    EmptyInputs(String),
    /// A Station's complete input list was declared more than once.
    #[error("inputs for station {0:?} were already set")]
    InputsAlreadySet(String),
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
}

pub(super) fn finish_definition(
    token: u64,
    mut stations: Vec<StationDefinition>,
    connections: &[(Vec<StationRef>, StationRef)],
    output_capacities: &[(StationRef, NonZeroU64)],
) -> Result<FlowDefinition, TopologyError> {
    validate_station_ids(&stations)?;
    let mut inputs_by_station = validate_connections(token, &stations, connections)?;
    validate_topology(&stations, &inputs_by_station)?;
    apply_output_capacities(token, &mut stations, output_capacities)?;

    let station_ids = stations
        .iter()
        .map(|station| station.id.clone())
        .collect::<Vec<_>>();
    for (index, station) in stations.iter_mut().enumerate() {
        station.inputs = inputs_by_station[index]
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(|input| station_ids[input].clone())
            .collect();
    }

    Ok(FlowDefinition::new(stations))
}

pub(super) fn validate_decoded_topology(
    stations: &[StationDefinition],
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        if inputs
            .as_ref()
            .is_some_and(|inputs| inputs.contains(&station))
        {
            return Err(TopologyError::SelfLoop(stations[station].id.clone()));
        }
    }
    validate_topology(stations, inputs_by_station)?;
    validate_output_capacities(stations)
}

fn apply_output_capacities(
    token: u64,
    stations: &mut [StationDefinition],
    declarations: &[(StationRef, NonZeroU64)],
) -> Result<(), TopologyError> {
    let mut capacities = vec![None; stations.len()];
    for (reference, capacity) in declarations {
        let index = resolve_ref(token, stations.len(), *reference)?;
        if capacities[index].replace(*capacity).is_some() {
            return Err(TopologyError::OutputCapacityAlreadySet(
                stations[index].id.clone(),
            ));
        }
    }

    for (station, capacity) in stations.iter_mut().zip(capacities) {
        station.output_capacity_bytes = capacity;
    }
    validate_output_capacities(stations)
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
) -> Result<(), TopologyError> {
    validate_acyclic(stations.len(), inputs_by_station)?;
    validate_endpoints(stations, inputs_by_station)?;
    validate_input_counts(stations, inputs_by_station)?;
    validate_inputs_have_output(stations, inputs_by_station)
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
    if stations.is_empty() {
        return Err(TopologyError::EmptyTopology);
    }

    let mut seen = HashSet::with_capacity(stations.len());
    for station in stations {
        let reason = if station.id.is_empty() {
            Some(InvalidStationIdReason::Empty)
        } else if station.id.as_bytes().contains(&0) {
            Some(InvalidStationIdReason::ContainsNul)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(TopologyError::InvalidStationId {
                id: station.id.clone(),
                reason,
            });
        }
        if !seen.insert(station.id.as_str()) {
            return Err(TopologyError::DuplicateStationId(station.id.clone()));
        }
    }
    Ok(())
}

pub(super) fn validate_connections(
    token: u64,
    stations: &[StationDefinition],
    connections: &[(Vec<StationRef>, StationRef)],
) -> Result<Vec<Option<Vec<usize>>>, TopologyError> {
    let mut inputs_by_station = vec![None; stations.len()];
    for (inputs, station) in connections {
        let station = resolve_ref(token, stations.len(), *station)?;
        if inputs.is_empty() {
            return Err(TopologyError::EmptyInputs(stations[station].id.clone()));
        }
        if inputs_by_station[station].is_some() {
            return Err(TopologyError::InputsAlreadySet(
                stations[station].id.clone(),
            ));
        }

        let inputs = inputs
            .iter()
            .map(|input| resolve_ref(token, stations.len(), *input))
            .collect::<Result<Vec<_>, _>>()?;
        if inputs.contains(&station) {
            return Err(TopologyError::SelfLoop(stations[station].id.clone()));
        }
        inputs_by_station[station] = Some(inputs);
    }
    Ok(inputs_by_station)
}

fn resolve_ref(
    token: u64,
    station_count: usize,
    reference: StationRef,
) -> Result<usize, TopologyError> {
    if reference.factory_token != token || reference.index >= station_count {
        Err(TopologyError::ForeignStationRef(reference))
    } else {
        Ok(reference.index)
    }
}

pub(super) fn validate_acyclic(
    station_count: usize,
    inputs_by_station: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
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
        .collect::<VecDeque<_>>();
    let mut visited = 0_usize;
    while let Some(station) = ready.pop_front() {
        visited += 1;
        for consumer in &consumers_by_station[station] {
            indegrees[*consumer] -= 1;
            if indegrees[*consumer] == 0 {
                ready.push_back(*consumer);
            }
        }
    }

    if visited == station_count {
        Ok(())
    } else {
        Err(TopologyError::Cycle)
    }
}
