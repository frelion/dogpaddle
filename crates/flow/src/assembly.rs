use std::collections::HashMap;

use crate::{
    build::FlowDefinition,
    flow::RuntimeTopology,
    station::{Station, StationParts},
};

pub(crate) struct AssembledFlow {
    pub(crate) stations: Vec<Station>,
    pub(crate) topology: RuntimeTopology,
}

pub(crate) struct ResolvedTopology {
    inputs_by_station: Vec<Vec<usize>>,
    subscriptions_by_station: Vec<Vec<u64>>,
    subscriber_counts: Vec<u64>,
    schedule: Vec<usize>,
}

pub(crate) fn resolve_topology(definition: &FlowDefinition) -> ResolvedTopology {
    let indices = definition
        .stations()
        .iter()
        .enumerate()
        .map(|(index, station)| (station.id(), index))
        .collect::<HashMap<_, _>>();
    let inputs_by_station = definition
        .stations()
        .iter()
        .map(|station| {
            station
                .inputs()
                .map(|input| {
                    indices
                        .get(input)
                        .copied()
                        .expect("validated input ID must identify one Station")
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut subscriber_counts = vec![0_u64; inputs_by_station.len()];
    let subscriptions_by_station = inputs_by_station
        .iter()
        .map(|inputs| {
            inputs
                .iter()
                .map(|producer| {
                    let subscriber = subscriber_counts[*producer];
                    subscriber_counts[*producer] = subscriber
                        .checked_add(1)
                        .expect("a materialized Flow cannot contain u64::MAX edges");
                    subscriber
                })
                .collect()
        })
        .collect();
    let schedule = topological_schedule(&inputs_by_station);
    ResolvedTopology {
        inputs_by_station,
        subscriptions_by_station,
        subscriber_counts,
        schedule,
    }
}

impl ResolvedTopology {
    pub(crate) fn inputs(&self, station: usize) -> &[usize] {
        &self.inputs_by_station[station]
    }

    pub(crate) fn schedule(&self) -> &[usize] {
        &self.schedule
    }

    pub(crate) fn subscriber_count(&self, station: usize) -> u64 {
        self.subscriber_counts[station]
    }
}

pub(crate) fn assemble_stations(
    topology: ResolvedTopology,
    mut parts: Vec<StationParts>,
) -> AssembledFlow {
    let ResolvedTopology {
        inputs_by_station,
        subscriptions_by_station,
        subscriber_counts: _,
        schedule,
    } = topology;
    let subscriptions = inputs_by_station
        .iter()
        .zip(&subscriptions_by_station)
        .map(|(inputs, subscribers)| {
            inputs
                .iter()
                .zip(subscribers)
                .map(|(producer, subscriber)| parts[*producer].subscription(*subscriber))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let outputs = parts
        .iter_mut()
        .map(StationParts::prepare_output)
        .collect::<Vec<_>>();
    let inputs = inputs_by_station
        .iter()
        .zip(subscriptions)
        .map(|(inputs, subscriptions)| {
            inputs
                .iter()
                .copied()
                .zip(subscriptions)
                .map(|(input, subscription)| {
                    outputs[input]
                        .as_ref()
                        .expect("validated input Station must produce output")
                        .port(subscription)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let stations = parts
        .into_iter()
        .zip(inputs)
        .zip(outputs)
        .map(|((part, inputs), output)| part.finish(inputs, output))
        .collect();
    AssembledFlow {
        stations,
        topology: RuntimeTopology { schedule },
    }
}

fn topological_schedule(inputs_by_station: &[Vec<usize>]) -> Vec<usize> {
    let mut indegrees = inputs_by_station.iter().map(Vec::len).collect::<Vec<_>>();
    let mut consumers_by_station = vec![Vec::new(); inputs_by_station.len()];
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        for input in inputs {
            consumers_by_station[*input].push(station);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(station, indegree)| (*indegree == 0).then_some(station))
        .collect::<Vec<_>>();
    let mut schedule = Vec::with_capacity(inputs_by_station.len());
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
    assert_eq!(
        schedule.len(),
        inputs_by_station.len(),
        "validated Flow definition must remain acyclic during assembly"
    );
    schedule
}
