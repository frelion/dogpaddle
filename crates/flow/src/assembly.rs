use std::collections::{HashMap, HashSet};

use dogpaddle_store::ReadOnly;

use crate::{
    build::FlowDefinition,
    flow::RuntimeTopology,
    station::{ConsumerCursor, Station, StationParts},
};

pub(crate) fn assemble_stations(
    definition: &FlowDefinition,
    parts: Vec<StationParts>,
) -> (Vec<Station>, RuntimeTopology) {
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
    let inputs = sources_by_target
        .iter()
        .map(|sources| {
            sources
                .iter()
                .map(|source| {
                    let output = parts[*source]
                        .output()
                        .expect("validated source station must produce output");
                    ReadOnly::new(output.clone())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut consumers = std::iter::repeat_with(Vec::new)
        .take(parts.len())
        .collect::<Vec<_>>();
    for (target, sources) in sources_by_target.iter().enumerate() {
        for (input, source) in sources.iter().copied().enumerate() {
            consumers[source].push(ConsumerCursor::new(
                ReadOnly::new(parts[target].state().clone()),
                input,
            ));
        }
    }

    let stations = parts
        .into_iter()
        .zip(inputs)
        .zip(consumers)
        .map(|((part, inputs), consumers)| part.finish(inputs, consumers))
        .collect();
    let schedule = topological_schedule(&sources_by_target);
    let gc_upstreams = sources_by_target
        .iter()
        .map(|sources| {
            let mut seen = HashSet::new();
            sources
                .iter()
                .copied()
                .filter(|source| seen.insert(*source))
                .collect()
        })
        .collect();
    (
        stations,
        RuntimeTopology {
            schedule,
            gc_upstreams,
        },
    )
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
