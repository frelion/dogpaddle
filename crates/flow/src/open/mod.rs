use std::path::Path;

use dogpaddle_operation::{DataInstances, OperationDefinition};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store, StoreData, StoreError};

use crate::{
    assembly::assemble_stages,
    build::{FlowDefinition, FlowFactory, codec},
    error::FlowError,
    flow::Flow,
    stage::StageParts,
};

impl FlowFactory {
    /// Opens a completely built Flow and reassembles all runtime stages.
    ///
    /// The definition is read first, then the Store is reopened so every
    /// declared data object can be opened before the Store is frozen into
    /// transaction capability. The definition is read again to guard the
    /// two-phase open.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError::IncompleteBuild`] when no complete definition was
    /// published, or another [`FlowError`] when the Store, definition, topology,
    /// or required stage resources are invalid.
    pub fn open(path: impl AsRef<Path>) -> Result<Flow, FlowError> {
        let path = path.as_ref().to_path_buf();
        let definition_bytes = read_published_definition(&path)?;
        let definition = codec::decode(&definition_bytes)?;

        let store = Store::open(&path)?;
        let published = open_definition_cell(&store)?;
        let flow_state = open_required_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>(
            &store,
            codec::FLOW_STATE_DATA_NAME,
        )?;
        let stage_parts = open_stage_parts(&store, &definition)?;
        let mut transactions = store.into_transactions();
        let observed_definition = {
            let transaction = transactions.begin()?;
            let published = published.access(transaction.access())?;
            published.get()?.ok_or(FlowError::IncompleteBuild)?
        };
        if observed_definition != definition_bytes {
            return Err(FlowError::DefinitionChangedDuringOpen);
        }
        let stages = assemble_stages(&definition, stage_parts, &transactions);

        Ok(Flow::from_parts(
            path,
            definition,
            flow_state,
            stages,
            transactions,
        ))
    }
}

fn read_published_definition(path: &Path) -> Result<Vec<u8>, FlowError> {
    let store = Store::open(path)?;
    let definition = open_definition_cell(&store)?;
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin()?;
    let definition = definition.access(transaction.access())?;
    definition.get()?.ok_or(FlowError::IncompleteBuild)
}

fn open_definition_cell(store: &Store) -> Result<Cell<Vec<u8>>, FlowError> {
    match store.open_data(codec::DEFINITION_DATA_NAME) {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(_)) => Err(FlowError::IncompleteBuild),
        Err(error) => Err(error.into()),
    }
}

fn open_stage_parts(
    store: &Store,
    definition: &FlowDefinition,
) -> Result<Vec<StageParts>, FlowError> {
    definition
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| open_stage_part(store, index, stage.operation()))
        .collect()
}

fn open_stage_part(
    store: &Store,
    index: usize,
    definition: &dyn OperationDefinition,
) -> Result<StageParts, FlowError> {
    let state = open_required_data::<OrderedMap<Vec<u8>, Vec<u8>, Small>>(
        store,
        &codec::stage_state_name(index),
    )?;
    let mut data = DataInstances::new();
    for declaration in definition.data() {
        let physical_name = codec::operation_data_name(index, declaration.name());
        let instance = require_resource(&physical_name, declaration.open(store, &physical_name))?;
        data.insert(instance)?;
    }
    let operation = definition.materialize(&mut data)?;
    data.finish()?;
    let output = definition
        .produces_output()
        .then(|| {
            let name = codec::stage_output_name(index);
            open_required_data::<AppendLog<Vec<u8>>>(store, &name)
        })
        .transpose()?;
    Ok(StageParts::new(state, operation, output))
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
