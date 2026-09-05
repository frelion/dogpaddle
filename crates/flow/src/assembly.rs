use std::collections::HashMap;

use dogpaddle_store::ReadOnly;

use crate::{
    build::FlowDefinition,
    flow::RuntimeTopology,
    station::{ConsumerCursor, Station, StationParts},
};

pub(crate) struct AssembledFlow {
    pub(crate) stations: Vec<Station>,
    pub(crate) topology: RuntimeTopology,
}

pub(crate) struct ResolvedTopology {
    inputs_by_station: Vec<Vec<usize>>,
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
    let schedule = topological_schedule(&inputs_by_station);
    ResolvedTopology {
        inputs_by_station,
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
}

pub(crate) fn assemble_stations(
    topology: ResolvedTopology,
    mut parts: Vec<StationParts>,
) -> AssembledFlow {
    let ResolvedTopology {
        inputs_by_station,
        schedule,
    } = topology;
    let mut consumers = std::iter::repeat_with(Vec::new)
        .take(parts.len())
        .collect::<Vec<_>>();
    let mut consumer_slots = inputs_by_station
        .iter()
        .map(|inputs| Vec::with_capacity(inputs.len()))
        .collect::<Vec<_>>();
    for (station, inputs) in inputs_by_station.iter().enumerate() {
        for (port, input) in inputs.iter().copied().enumerate() {
            consumer_slots[station].push(consumers[input].len());
            consumers[input].push(ConsumerCursor::new(
                ReadOnly::new(parts[station].state().clone()),
                port,
            ));
        }
    }

    let outputs = consumers
        .into_iter()
        .enumerate()
        .map(|(station, consumers)| parts[station].prepare_output(consumers))
        .collect::<Vec<_>>();
    let inputs = inputs_by_station
        .iter()
        .zip(consumer_slots)
        .map(|(inputs, slots)| {
            inputs
                .iter()
                .copied()
                .zip(slots)
                .map(|(input, slot)| {
                    outputs[input]
                        .as_ref()
                        .expect("validated input Station must produce output")
                        .port(slot)
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
