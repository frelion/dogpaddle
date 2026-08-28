use std::collections::{HashSet, VecDeque};

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
    /// `connect` was called without any sources.
    #[error("station {0:?} was connected with an empty source list")]
    EmptySources(String),
    /// A target's complete source list was declared more than once.
    #[error("sources for station {0:?} were already set")]
    SourcesAlreadySet(String),
    /// A station directly references itself.
    #[error("station {0:?} directly references itself")]
    SelfLoop(String),
    /// The topology contains an indirect cycle.
    #[error("flow topology contains a cycle")]
    Cycle,
    /// A root Station is not a source.
    #[error("root station {0:?} is not a source")]
    RootIsNotSource(String),
    /// A terminal Station is not a sink.
    #[error("terminal station {0:?} is not a sink")]
    TerminalIsNotSink(String),
    /// The connected upstream count does not match the Station's input arity.
    #[error("station {station:?} requires {expected} upstreams but received {actual}")]
    InputCount {
        /// Station whose input arity did not match.
        station: String,
        /// Required upstream count.
        expected: usize,
        /// Connected upstream count.
        actual: usize,
    },
    /// A connection uses a Station without an output stream as its upstream.
    #[error(
        "station {downstream_station:?} cannot read from outputless upstream {upstream_station:?}"
    )]
    UpstreamHasNoOutput {
        /// Upstream Station without an output stream.
        upstream_station: String,
        /// Downstream Station that attempted to consume it.
        downstream_station: String,
    },
}

pub(super) fn finish_definition(
    token: u64,
    mut stations: Vec<StationDefinition>,
    connections: &[(Vec<StationRef>, StationRef)],
) -> Result<FlowDefinition, TopologyError> {
    validate_station_ids(&stations)?;
    let mut sources_by_target = validate_connections(token, &stations, connections)?;
    validate_topology(&stations, &sources_by_target)?;

    let station_ids = stations
        .iter()
        .map(|station| station.id.clone())
        .collect::<Vec<_>>();
    for (index, station) in stations.iter_mut().enumerate() {
        station.sources = sources_by_target[index]
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(|source| station_ids[source].clone())
            .collect();
    }

    Ok(FlowDefinition::new(stations))
}

pub(super) fn validate_decoded_topology(
    stations: &[StationDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (target, sources) in sources_by_target.iter().enumerate() {
        if sources
            .as_ref()
            .is_some_and(|sources| sources.contains(&target))
        {
            return Err(TopologyError::SelfLoop(stations[target].id.clone()));
        }
    }
    validate_topology(stations, sources_by_target)
}

fn validate_topology(
    stations: &[StationDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    validate_acyclic(stations.len(), sources_by_target)?;
    validate_endpoints(stations, sources_by_target)?;
    validate_input_counts(stations, sources_by_target)?;
    validate_upstreams_have_output(stations, sources_by_target)
}

fn validate_endpoints(
    stations: &[StationDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    let mut has_downstream = vec![false; stations.len()];
    for upstream in sources_by_target.iter().flatten().flatten() {
        has_downstream[*upstream] = true;
    }

    for (index, station) in stations.iter().enumerate() {
        let is_root = sources_by_target[index].as_ref().is_none_or(Vec::is_empty);
        if is_root && !station.is_source() {
            return Err(TopologyError::RootIsNotSource(station.id.clone()));
        }
        if !has_downstream[index] && !station.is_sink() {
            return Err(TopologyError::TerminalIsNotSink(station.id.clone()));
        }
    }
    Ok(())
}

fn validate_upstreams_have_output(
    stations: &[StationDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (downstream, upstreams) in sources_by_target.iter().enumerate() {
        for upstream in upstreams.iter().flatten() {
            if !stations[*upstream].has_output() {
                return Err(TopologyError::UpstreamHasNoOutput {
                    upstream_station: stations[*upstream].id.clone(),
                    downstream_station: stations[downstream].id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_input_counts(
    stations: &[StationDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (station, sources) in stations.iter().zip(sources_by_target) {
        let expected = station.input_count();
        let actual = sources.as_ref().map_or(0, Vec::len);
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
    let mut sources_by_target = vec![None; stations.len()];
    for (sources, target) in connections {
        let target = resolve_ref(token, stations.len(), *target)?;
        if sources.is_empty() {
            return Err(TopologyError::EmptySources(stations[target].id.clone()));
        }
        if sources_by_target[target].is_some() {
            return Err(TopologyError::SourcesAlreadySet(
                stations[target].id.clone(),
            ));
        }

        let sources = sources
            .iter()
            .map(|source| resolve_ref(token, stations.len(), *source))
            .collect::<Result<Vec<_>, _>>()?;
        if sources.contains(&target) {
            return Err(TopologyError::SelfLoop(stations[target].id.clone()));
        }
        sources_by_target[target] = Some(sources);
    }
    Ok(sources_by_target)
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
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    let mut indegrees = vec![0_usize; station_count];
    let mut targets_by_source = vec![Vec::new(); station_count];
    for (target, sources) in sources_by_target.iter().enumerate() {
        for source in sources.iter().flatten() {
            indegrees[target] += 1;
            targets_by_source[*source].push(target);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(station, indegree)| (*indegree == 0).then_some(station))
        .collect::<VecDeque<_>>();
    let mut visited = 0_usize;
    while let Some(source) = ready.pop_front() {
        visited += 1;
        for target in &targets_by_source[source] {
            indegrees[*target] -= 1;
            if indegrees[*target] == 0 {
                ready.push_back(*target);
            }
        }
    }

    if visited == station_count {
        Ok(())
    } else {
        Err(TopologyError::Cycle)
    }
}
