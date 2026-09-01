use std::{collections::HashMap, sync::Arc};

use dogpaddle_store::ReadOnly;

use crate::{
    build::FlowDefinition,
    flow::RuntimeTopology,
    station::{ConsumerCursor, InputPort, OutputRetention, Station, StationParts},
};

pub(crate) struct AssembledFlow {
    pub(crate) stations: Vec<Station>,
    pub(crate) topology: RuntimeTopology,
    pub(crate) retentions: Vec<Option<Arc<OutputRetention>>>,
}

pub(crate) fn assemble_stations(
    definition: &FlowDefinition,
    parts: Vec<StationParts>,
) -> AssembledFlow {
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
                .map(|source| indices[source])
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
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

    let retentions = consumers
        .into_iter()
        .enumerate()
        .map(|(source, consumers)| {
            parts[source]
                .output()
                .map(|output| Arc::new(OutputRetention::new(output.clone(), consumers)))
        })
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
                    let output = parts[source]
                        .output()
                        .expect("validated source station must produce output");
                    let retention = Arc::clone(
                        retentions[source]
                            .as_ref()
                            .expect("validated source output must have retention state"),
                    );
                    InputPort::new(ReadOnly::new(output.clone()), retention, slot)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let stations = parts
        .into_iter()
        .zip(inputs)
        .map(|(part, inputs)| part.finish(inputs))
        .collect();
    let schedule = topological_schedule(&sources_by_target);
    AssembledFlow {
        stations,
        topology: RuntimeTopology { schedule },
        retentions,
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
