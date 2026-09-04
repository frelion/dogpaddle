use dogpaddle_operation::{DataInstances, OperationBinding, RuntimeResource};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store, StoreData, StoreError};

use crate::{
    assembly::{assemble_stations, resolve_topology},
    error::{FlowError, retention_open_error},
    flow::Flow,
    station::StationParts,
};

use super::{
    FlowFactory, StationDefinition, bind_resources, codec, schema, validate_data_declarations,
};

impl FlowFactory {
    /// Opens a completely built Flow and reassembles all runtime stations.
    ///
    /// One setup-phase read-only snapshot reads the definition while preserving
    /// the same Store for opening every declared data object. After setup is
    /// frozen into runtime transaction capabilities, another read-only snapshot
    /// checks the definition again and validates every output frontier before the
    /// Flow is returned.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError::IncompleteBuild`] when no complete definition was
    /// published, or another [`FlowError`] when the Store, definition, topology,
    /// or required station resources are invalid. Returns
    /// [`FlowError::OpenWithDefinition`] if this factory also declares topology
    /// or output capacities; open accepts only the path and runtime resources.
    pub fn open(self) -> Result<Flow, FlowError> {
        if !self.stations.is_empty()
            || !self.connections.is_empty()
            || !self.output_capacities.is_empty()
        {
            return Err(FlowError::OpenWithDefinition);
        }
        let path = self.path;
        let store = Store::open(&path)?;
        let published = open_definition_cell(&store)?;
        let definition_bytes = read_published_definition(&store, &published)?;
        let definition = codec::decode(&definition_bytes)?;
        let topology = resolve_topology(&definition);
        let bindings = schema::bind_operations(&definition, &topology)?;
        validate_data_declarations(&definition)?;
        let resources = bind_resources(&definition, &bindings, self.resources)?;
        let station_ids = definition
            .stations()
            .iter()
            .map(|station| station.id().to_owned())
            .collect();

        let station_parts = definition
            .stations()
            .iter()
            .enumerate()
            .zip(bindings)
            .zip(resources)
            .map(|(((index, station), binding), resource)| {
                open_station_part(&store, index, station, binding, resource)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (transactions, reads) = store.into_transactions().split();
        let assembled = assemble_stations(topology, station_parts);
        {
            let transaction = reads.begin()?;
            let published = published.read(transaction.access())?;
            let observed_definition = published.get()?.ok_or(FlowError::IncompleteBuild)?;
            if observed_definition != definition_bytes {
                return Err(FlowError::DefinitionChangedDuringOpen);
            }
            for (station_definition, station) in
                definition.stations().iter().zip(assembled.stations.iter())
            {
                station
                    .validate_output(transaction.access())
                    .map_err(|source| retention_open_error(station_definition.id(), source))?;
            }
        }

        Ok(Flow::from_parts(
            path,
            station_ids,
            assembled.stations,
            assembled.topology,
            transactions,
            reads,
        ))
    }
}

fn read_published_definition(
    store: &Store,
    definition: &Cell<Vec<u8>>,
) -> Result<Vec<u8>, FlowError> {
    let transaction = store.read_transaction()?;
    let definition = definition.read(transaction.access())?;
    definition.get()?.ok_or(FlowError::IncompleteBuild)
}

fn open_definition_cell(store: &Store) -> Result<Cell<Vec<u8>>, FlowError> {
    match store.open_data(codec::DEFINITION_DATA_NAME) {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(_)) => Err(FlowError::IncompleteBuild),
        Err(error) => Err(error.into()),
    }
}

fn open_station_part(
    store: &Store,
    index: usize,
    station: &StationDefinition,
    binding: OperationBinding,
    resource: RuntimeResource,
) -> Result<StationParts, FlowError> {
    let state = open_required_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>(
        store,
        &codec::station_state_name(index),
    )?;
    let definition = station.operation();
    let mut data = DataInstances::new();
    for declaration in definition.data() {
        let physical_name = codec::station_operation_data_name(index, declaration.name());
        let instance = require_resource(&physical_name, declaration.open(store, &physical_name))?;
        data.insert(instance)?;
    }
    let output_schema = binding.output_schema().cloned();
    let operation = binding.materialize(data, resource)?;
    let output = match (station.output_capacity_bytes(), output_schema) {
        (Some(capacity), Some(schema)) => {
            let name = codec::station_output_name(index);
            Some((
                open_required_data::<AppendLog<Vec<u8>>>(store, &name)?,
                capacity,
                schema,
            ))
        }
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            unreachable!("validated output capacity and bound Schema must agree")
        }
    };
    Ok(StationParts::new(
        state,
        operation,
        definition.kind(),
        output,
    ))
}

fn open_required_data<D: StoreData>(store: &Store, name: &str) -> Result<D, FlowError> {
    require_resource(name, store.open_data(name))
}

fn require_resource<T>(name: &str, result: Result<T, StoreError>) -> Result<T, FlowError> {
    match result {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(_)) => Err(FlowError::MissingResource {
            name: name.to_owned(),
        }),
        Err(error) => Err(error.into()),
    }
}
