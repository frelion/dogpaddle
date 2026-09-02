use std::path::Path;

use dogpaddle_operation::DataInstances;
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store, StoreData, StoreError};

use crate::{
    assembly::assemble_stations,
    error::{FlowError, retention_open_error},
    flow::Flow,
    station::StationParts,
};

use super::{FlowFactory, StationDefinition, codec};

impl FlowFactory {
    /// Opens a completely built Flow and reassembles all runtime stations.
    ///
    /// The definition is read first, then the Store is reopened so every
    /// declared data object can be opened before the Store is frozen into
    /// transaction capability. One read-only snapshot then checks the definition
    /// again and validates every output frontier before the Flow is returned.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError::IncompleteBuild`] when no complete definition was
    /// published, or another [`FlowError`] when the Store, definition, topology,
    /// or required station resources are invalid.
    pub fn open(path: impl AsRef<Path>) -> Result<Flow, FlowError> {
        let path = path.as_ref().to_path_buf();
        let definition_bytes = read_published_definition(&path)?;
        let definition = codec::decode(&definition_bytes)?;

        let store = Store::open(&path)?;
        let published = open_definition_cell(&store)?;
        let _flow_state = open_required_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>(
            &store,
            codec::FLOW_STATE_DATA_NAME,
        )?;
        let station_parts = definition
            .stations()
            .iter()
            .enumerate()
            .map(|(index, station)| open_station_part(&store, index, station))
            .collect::<Result<Vec<_>, _>>()?;
        let (transactions, reads) = store.into_transactions().split();
        let assembled = assemble_stations(&definition, station_parts);
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
            definition,
            assembled.stations,
            assembled.topology,
            transactions,
            reads,
        ))
    }
}

fn read_published_definition(path: &Path) -> Result<Vec<u8>, FlowError> {
    let store = Store::open(path)?;
    let definition = open_definition_cell(&store)?;
    let (_, reads) = store.into_transactions().split();
    let transaction = reads.begin()?;
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
    let operation = definition.materialize(&mut data)?;
    data.finish()?;
    let output = station
        .output_capacity_bytes()
        .map(|capacity| {
            let name = codec::station_output_name(index);
            open_required_data::<AppendLog<Vec<u8>>>(store, &name).map(|log| (log, capacity))
        })
        .transpose()?;
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
