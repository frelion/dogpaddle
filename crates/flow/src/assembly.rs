use crate::{
    flow::RuntimeTopology,
    station::{Station, StationParts},
};

pub(crate) struct AssembledFlow {
    pub(crate) stations: Vec<Station>,
    pub(crate) topology: RuntimeTopology,
}

#[derive(Debug)]
pub(crate) struct ResolvedTopology {
    inputs_by_station: Vec<Vec<usize>>,
    subscriptions_by_station: Vec<Vec<u64>>,
    subscriber_counts: Vec<u64>,
    schedule: Vec<usize>,
}

pub(crate) fn resolve_topology(
    inputs_by_station: Vec<Option<Vec<usize>>>,
    schedule: Vec<usize>,
) -> ResolvedTopology {
    let inputs_by_station = inputs_by_station
        .into_iter()
        .map(Option::unwrap_or_default)
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
