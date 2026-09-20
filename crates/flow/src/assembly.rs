use crate::station::{Inbox, InputPort, Output, Station, StationError, StationProgram};
use arrow_schema::SchemaRef;
use dogpaddle_operation::operation::Operation;
use dogpaddle_store::{
    Cell, ReadTransactionAccess, StoreError, SubscribedLog, Subscription, TransactionAccess,
};
use std::{num::NonZeroU64, sync::Arc};

pub(crate) struct AssembledFlow {
    pub(crate) stations: Vec<Station>,
    pub(crate) schedule: Vec<usize>,
}

#[derive(Debug)]
pub(crate) struct ResolvedInput {
    pub(crate) producer: usize,
    subscriber: u64,
}

#[derive(Debug)]
pub(crate) struct ResolvedTopology {
    inputs_by_station: Vec<Vec<ResolvedInput>>,
    subscriber_counts: Vec<u64>,
    schedule: Vec<usize>,
}

pub(crate) fn resolve_topology(
    inputs_by_station: Vec<Option<Vec<usize>>>,
    schedule: Vec<usize>,
) -> ResolvedTopology {
    let mut subscriber_counts = vec![0_u64; inputs_by_station.len()];
    let inputs_by_station = inputs_by_station
        .into_iter()
        .map(|inputs| {
            inputs
                .unwrap_or_default()
                .into_iter()
                .map(|producer| {
                    let subscriber = subscriber_counts[producer];
                    subscriber_counts[producer] = subscriber
                        .checked_add(1)
                        .expect("a materialized Flow cannot contain u64::MAX edges");
                    ResolvedInput {
                        producer,
                        subscriber,
                    }
                })
                .collect()
        })
        .collect();
    ResolvedTopology {
        inputs_by_station,
        subscriber_counts,
        schedule,
    }
}

impl ResolvedTopology {
    pub(crate) fn inputs(&self, station: usize) -> &[ResolvedInput] {
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
    // Derive every Subscription before consuming the producer log handles.
    let subscriptions = topology
        .inputs_by_station
        .iter()
        .map(|inputs| {
            inputs
                .iter()
                .map(|input| parts[input.producer].subscription(input.subscriber))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let outputs = parts
        .iter_mut()
        .map(StationParts::prepare_output)
        .collect::<Vec<_>>();
    let stations = parts
        .into_iter()
        .zip(&topology.inputs_by_station)
        .zip(subscriptions)
        .enumerate()
        .map(|(index, ((part, inputs), subscriptions))| {
            let inputs = inputs
                .iter()
                .zip(subscriptions)
                .map(|(input, subscription)| {
                    outputs[input.producer]
                        .as_ref()
                        .expect("validated input Station must produce output")
                        .port(subscription)
                })
                .collect();
            part.finish(inputs, outputs[index].clone())
        })
        .collect();
    AssembledFlow {
        stations,
        schedule: topology.schedule,
    }
}

pub(crate) struct StationParts {
    active: Option<Cell<u32>>,
    input_count: usize,
    program: StationProgram,
    output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
}

impl StationParts {
    pub(crate) fn new(
        active: Option<Cell<u32>>,
        input_count: usize,
        operations: Vec<Operation>,
        output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaRef)>,
    ) -> Self {
        Self {
            active,
            input_count,
            program: StationProgram::new(operations),
            output,
        }
    }

    pub(crate) fn initialize(
        &self,
        subscriber_count: u64,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        if let Some(active) = &self.active {
            active.access(access)?.set(&0)?;
        }
        match (&self.output, NonZeroU64::new(subscriber_count)) {
            (Some((log, _, _)), Some(subscriber_count)) => {
                log.initialize(subscriber_count, access)?;
            }
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output ownership must match direct consumer count")
            }
        }
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        subscriber_count: u64,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StationError> {
        if let Some(active) = &self.active {
            let active = active
                .read(access)?
                .get()?
                .ok_or(StationError::MissingActiveInput)?;
            let input_count = self.input_count;
            let active = usize::try_from(active).expect("u32 fits usize on supported targets");
            if active >= input_count {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count,
                });
            }
        }
        match (&self.output, NonZeroU64::new(subscriber_count)) {
            (Some((log, _, _)), Some(subscriber_count)) => {
                log.validate(subscriber_count, access)?;
                Ok(())
            }
            (None, None) => Ok(()),
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output ownership must match direct consumer count")
            }
        }
    }

    pub(crate) fn subscription(&self, subscriber: u64) -> Subscription<Vec<u8>> {
        self.output
            .as_ref()
            .expect("validated input Station must produce output")
            .0
            .subscription(subscriber)
    }

    pub(crate) fn prepare_output(&mut self) -> Option<Arc<Output>> {
        self.output.take().map(|(log, capacity_bytes, schema)| {
            Arc::new(Output::new(log.writer(), capacity_bytes, schema))
        })
    }

    pub(crate) fn finish(self, inputs: Vec<InputPort>, output: Option<Arc<Output>>) -> Station {
        assert_eq!(
            inputs.len(),
            self.input_count,
            "station input capabilities must match its operation definition"
        );
        assert!(
            self.output.is_none(),
            "station output must be moved exactly once during assembly"
        );
        Station::new(self.program, Inbox::new(self.active, inputs), output)
    }
}
