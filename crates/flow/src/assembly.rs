use crate::{
    build::{FlowDefinition, ResolvedTopology, codec},
    error::{FlowError, operation_setup_error},
    flow::Flow,
    station::{Inbox, InputPort, Output, Station, StationError, StationProgram},
};
use arrow_schema::SchemaRef;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::{
    OperationBindError, OperationSetupError, RuntimeResource, operation::Operation,
};
use dogpaddle_store::{
    Cell, DataScope, ReadTransactionAccess, ReadTransactions, StoreError, SubscribedLog,
    Subscription, TransactionAccess, Transactions,
};
use std::{num::NonZeroU64, path::PathBuf, sync::Arc};

pub(crate) fn construct_stations(
    definition: &FlowDefinition,
    topology: &ResolvedTopology,
    data: &mut DataScope<'_>,
    resources: Vec<RuntimeResource>,
) -> Result<Vec<StationParts>, FlowError> {
    let mut resources = resources.into_iter().map(Some).collect::<Vec<_>>();
    let mut output_schemas = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<Option<SchemaRef>>>();
    let mut stations = std::iter::repeat_with(|| None)
        .take(definition.stations().len())
        .collect::<Vec<Option<StationParts>>>();

    for &station_index in topology.schedule() {
        let station = &definition.stations()[station_index];
        let input_schemas = topology
            .inputs(station_index)
            .iter()
            .map(|producer| {
                output_schemas[producer.producer]
                    .as_ref()
                    .expect("a scheduled, validated input must have a constructed output Schema")
                    .clone()
            })
            .collect::<Vec<_>>();
        let active = (station.inputs().len() > 1)
            .then(|| data.data::<Cell<u32>>(&codec::station_active_input_name(station_index)))
            .transpose()
            .map_err(map_store_error)?;
        let mut station_resource = resources[station_index]
            .take()
            .expect("each Station resource is consumed exactly once");
        let mut operations = Vec::with_capacity(station.operations().len());
        let mut current_schema = None;

        for (operation_index, definition) in station.operations().iter().enumerate() {
            let inputs = if operation_index == 0 {
                input_schemas.as_slice()
            } else {
                std::slice::from_ref(
                    current_schema
                        .as_ref()
                        .expect("a validated intermediate Operation has an output Schema"),
                )
            };
            let resource = if operation_index == 0 {
                std::mem::take(&mut station_resource)
            } else {
                RuntimeResource::default()
            };
            let prefix = codec::station_operation_prefix(station_index, operation_index);
            let constructed = definition
                .construct(inputs, &mut data.scoped(&prefix), resource)
                .map_err(|source| construction_error(station.id(), operation_index, source))?;
            let (operation, output_schema) = constructed.into_parts();
            operations.push(operation);
            current_schema = output_schema;
        }

        let output = match (station.output_capacity_bytes(), current_schema.as_ref()) {
            (Some(capacity), Some(schema)) => {
                let change_codec =
                    SchemaBoundChangeCodec::try_new(schema.clone()).map_err(|source| {
                        FlowError::OutputCodec {
                            station_id: station.id().to_owned(),
                            source,
                        }
                    })?;
                Some((
                    data.data::<SubscribedLog<Vec<u8>>>(&codec::station_output_name(station_index))
                        .map_err(map_store_error)?,
                    capacity,
                    change_codec,
                ))
            }
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                unreachable!("validated output capacity and constructed Schema must agree")
            }
        };
        output_schemas[station_index] = current_schema;
        stations[station_index] = Some(StationParts::new(active, operations, output));
    }

    Ok(stations
        .into_iter()
        .map(|station| station.expect("every validated Station must be scheduled and constructed"))
        .collect())
}

pub(crate) struct StationParts {
    active: Option<Cell<u32>>,
    program: StationProgram,
    output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaBoundChangeCodec)>,
}

impl StationParts {
    pub(crate) fn new(
        active: Option<Cell<u32>>,
        operations: Vec<Operation>,
        output: Option<(SubscribedLog<Vec<u8>>, NonZeroU64, SchemaBoundChangeCodec)>,
    ) -> Self {
        Self {
            active,
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
        input_count: usize,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StationError> {
        if let Some(active) = &self.active {
            let active = active
                .read(access)?
                .get()?
                .ok_or(StationError::MissingActiveInput)?;
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
        self.output.take().map(|(log, capacity_bytes, codec)| {
            Arc::new(Output::new(log.writer(), capacity_bytes, codec))
        })
    }

    pub(crate) fn finish(self, inputs: Vec<InputPort>, output: Option<Arc<Output>>) -> Station {
        assert!(
            self.output.is_none(),
            "station output must be moved exactly once during assembly"
        );
        Station::new(self.program, Inbox::new(self.active, inputs), output)
    }
}

pub(crate) fn assemble_flow(
    path: PathBuf,
    definition: &FlowDefinition,
    topology: ResolvedTopology,
    mut parts: Vec<StationParts>,
    transactions: Transactions,
    reads: ReadTransactions,
) -> Flow {
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
    let station_ids = definition
        .stations()
        .iter()
        .map(|station| station.id().to_owned())
        .collect();
    Flow::from_parts(
        path,
        station_ids,
        stations,
        topology.schedule,
        transactions,
        reads,
    )
}

fn construction_error(
    station_id: &str,
    operation: usize,
    source: OperationSetupError,
) -> FlowError {
    let source = match source {
        OperationSetupError::Bind(source) => {
            return FlowError::Schema {
                station_id: station_id.to_owned(),
                operation,
                source,
            };
        }
        OperationSetupError::Schema { source } => {
            return FlowError::Schema {
                station_id: station_id.to_owned(),
                operation,
                source: OperationBindError::Rejected { source },
            };
        }
        source => source,
    };
    operation_setup_error(station_id, operation, source)
}

fn map_store_error(source: StoreError) -> FlowError {
    match source {
        StoreError::DataNotFound(name) => FlowError::MissingResource { name },
        source => source.into(),
    }
}
