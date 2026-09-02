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
    sources_by_target: Vec<Vec<usize>>,
    schedule: Vec<usize>,
}

pub(crate) fn resolve_topology(definition: &FlowDefinition) -> ResolvedTopology {
    let indices = definition
        .stations()
        .iter()
        .enumerate()
        .map(|(index, station)| (station.id(), index))
        .collect::<HashMap<_, _>>();
    let sources_by_target = definition
        .stations()
        .iter()
        .map(|station| {
            station
                .sources()
                .map(|source| {
                    indices
                        .get(source)
                        .copied()
                        .expect("validated source ID must identify one Station")
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let schedule = topological_schedule(&sources_by_target);
    ResolvedTopology {
        sources_by_target,
        schedule,
    }
}

impl ResolvedTopology {
    pub(crate) fn sources(&self, station: usize) -> &[usize] {
        &self.sources_by_target[station]
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
        sources_by_target,
        schedule,
    } = topology;
    let mut consumers = std::iter::repeat_with(Vec::new)
        .take(parts.len())
        .collect::<Vec<_>>();
    let mut consumer_slots = sources_by_target
        .iter()
        .map(|sources| Vec::with_capacity(sources.len()))
        .collect::<Vec<_>>();
    for (target, sources) in sources_by_target.iter().enumerate() {
        for (input, source) in sources.iter().copied().enumerate() {
            consumer_slots[target].push(consumers[source].len());
            consumers[source].push(ConsumerCursor::new(
                ReadOnly::new(parts[target].state().clone()),
                input,
            ));
        }
    }

    let outputs = consumers
        .into_iter()
        .enumerate()
        .map(|(source, consumers)| parts[source].prepare_output(consumers))
        .collect::<Vec<_>>();
    let inputs = sources_by_target
        .iter()
        .zip(consumer_slots)
        .map(|(sources, slots)| {
            sources
                .iter()
                .copied()
                .zip(slots)
                .map(|(source, slot)| {
                    outputs[source]
                        .as_ref()
                        .expect("validated source station must produce output")
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

fn topological_schedule(sources_by_target: &[Vec<usize>]) -> Vec<usize> {
    let mut indegrees = sources_by_target.iter().map(Vec::len).collect::<Vec<_>>();
    let mut targets_by_source = vec![Vec::new(); sources_by_target.len()];
    for (target, sources) in sources_by_target.iter().enumerate() {
        for source in sources {
            targets_by_source[*source].push(target);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(station, indegree)| (*indegree == 0).then_some(station))
        .collect::<Vec<_>>();
    let mut schedule = Vec::with_capacity(sources_by_target.len());
    while !ready.is_empty() {
        let mut next = Vec::new();
        for source in ready {
            schedule.push(source);
            for target in &targets_by_source[source] {
                indegrees[*target] -= 1;
                if indegrees[*target] == 0 {
                    next.push(*target);
                }
            }
        }
        next.sort_unstable();
        ready = next;
    }
    assert_eq!(
        schedule.len(),
        sources_by_target.len(),
        "validated Flow definition must remain acyclic during assembly"
    );
    schedule
}
