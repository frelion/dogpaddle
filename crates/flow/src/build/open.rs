use dogpaddle_store::{Cell, Store, StoreError};

use crate::{
    assembly::assemble_stations,
    error::{FlowError, runtime_state_error},
    flow::Flow,
};

use super::{FlowFactory, codec, preflight_resources, schema};

impl FlowFactory {
    /// Opens a completely built Flow and reassembles all runtime stations.
    ///
    /// The owner identity is checked first. Runtime-resource metadata is then
    /// preflighted globally before Operations directly construct against the
    /// existing Store's data scope. The Store remains held while runtime state
    /// is validated and is consumed into transaction capabilities only last.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError::IncompleteBuild`] when no complete definition was
    /// published, or another [`FlowError`] when the Store, definition, topology,
    /// or required station resources are invalid. Returns
    /// [`FlowError::OpenWithDefinition`] if this factory also declares topology,
    /// or materialization boundaries; open accepts the path, owner identity, and
    /// runtime resources.
    pub fn open(self) -> Result<Flow, FlowError> {
        if !self.operations.is_empty() || !self.materializations.is_empty() {
            return Err(FlowError::OpenWithDefinition);
        }
        let path = self.path;
        let expected_owner_identity = self.owner_identity;
        let store = Store::open(&path)?;
        let published = open_definition_cell(&store)?;
        let definition_bytes = read_published_definition(&store, &published)?;
        let (definition, topology) = codec::decode(&definition_bytes)?;
        if definition.owner_identity() != expected_owner_identity {
            return Err(FlowError::OwnerIdentityMismatch);
        }
        let resources = preflight_resources(&definition, self.resources)?;
        let station_ids = definition
            .stations()
            .iter()
            .map(|station| station.id().to_owned())
            .collect();
        let station_parts =
            schema::construct_stations(&definition, &topology, &mut store.data_scope(), resources)?;
        {
            let transaction = store.read_transaction();
            for (index, (station_definition, station)) in
                definition.stations().iter().zip(&station_parts).enumerate()
            {
                station
                    .validate(topology.subscriber_count(index), transaction.access())
                    .map_err(|source| runtime_state_error(station_definition.id(), source))?;
            }
        }
        let (transactions, reads) = store.into_transactions().split();
        let assembled = assemble_stations(topology, station_parts);

        Ok(Flow::from_parts(
            path,
            station_ids,
            assembled.stations,
            assembled.schedule,
            transactions,
            reads,
        ))
    }
}

fn read_published_definition(
    store: &Store,
    definition: &Cell<Vec<u8>>,
) -> Result<Vec<u8>, FlowError> {
    let transaction = store.read_transaction();
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
