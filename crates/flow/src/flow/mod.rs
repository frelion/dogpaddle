use std::path::{Path, PathBuf};

use dogpaddle_operation::OperationDefinition;
use dogpaddle_store::{Cell, DataHandle, OrderedMap, Store, StoreError, Transactions};

use crate::{
    build::{FlowBuilder, FlowDefinition, StageDefinition, codec},
    error::FlowError,
    stage::Stage,
};

/// An opened persistent Flow.
///
/// A Flow owns the only active Store transaction capability for its path. Its
/// definition and data namespaces were frozen by a successful build.
pub struct Flow {
    path: PathBuf,
    definition: FlowDefinition,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "flow state is consumed by the next run phase")
    )]
    state: OrderedMap<Vec<u8>, Vec<u8>>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "stage instances are consumed by the next run phase"
        )
    )]
    stages: Vec<Stage>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "transactions are consumed by the next run phase")
    )]
    transactions: Transactions,
}

impl Flow {
    /// Starts a side-effect-free definition builder for `path`.
    #[must_use]
    pub fn builder(path: impl AsRef<Path>) -> FlowBuilder {
        FlowBuilder::new(path)
    }

    /// Opens a completely built Flow and rematerializes all stage resources.
    ///
    /// The definition is read first, then the Store is reopened so every data
    /// namespace can be resolved before the Store is frozen into transaction
    /// capability. The definition is read again to guard the two-phase open.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError::IncompleteBuild`] when no complete definition was
    /// published, or another [`FlowError`] when the Store, definition, topology,
    /// or required stage resources are invalid.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FlowError> {
        let path = path.as_ref().to_path_buf();
        let definition_bytes = read_published_definition(&path)?;
        let definition = codec::decode(&definition_bytes)?;

        let store = Store::open(&path)?;
        let published = open_definition_cell(&store)?;
        let flow_state = open_required_data(&store, codec::FLOW_STATE_DATA_NAME)?;
        let stages = open_stages(&store, &definition)?;
        let mut transactions = store.into_transactions();
        let observed_definition = {
            let transaction = transactions.begin()?;
            let published = published.access(&transaction)?;
            published.get()?.ok_or(FlowError::IncompleteBuild)?
        };
        if observed_definition != definition_bytes {
            return Err(FlowError::DefinitionChangedDuringOpen);
        }

        Ok(Self {
            path,
            definition,
            state: OrderedMap::new(flow_state),
            stages,
            transactions,
        })
    }

    pub(crate) fn from_build(
        path: PathBuf,
        definition: FlowDefinition,
        state: OrderedMap<Vec<u8>, Vec<u8>>,
        stages: Vec<Stage>,
        transactions: Transactions,
    ) -> Self {
        Self {
            path,
            definition,
            state,
            stages,
            transactions,
        }
    }

    /// Returns the Store path owned by this Flow.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of stages in declaration order.
    #[must_use]
    pub fn stage_count(&self) -> usize {
        self.definition.stages().len()
    }

    /// Iterates over stable stage IDs in declaration order.
    #[must_use]
    pub fn stage_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.definition.stages().iter().map(StageDefinition::id)
    }
}

fn read_published_definition(path: &Path) -> Result<Vec<u8>, FlowError> {
    let store = Store::open(path)?;
    let definition = open_definition_cell(&store)?;
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin()?;
    let definition = definition.access(&transaction)?;
    definition.get()?.ok_or(FlowError::IncompleteBuild)
}

fn open_definition_cell(store: &Store) -> Result<Cell<Vec<u8>>, FlowError> {
    match store.open_data(codec::DEFINITION_DATA_NAME) {
        Ok(data) => Ok(Cell::new(data)),
        Err(StoreError::DataNotFound(name)) if name == codec::DEFINITION_DATA_NAME => {
            Err(FlowError::IncompleteBuild)
        }
        Err(error) => Err(error.into()),
    }
}

fn open_stages(store: &Store, definition: &FlowDefinition) -> Result<Vec<Stage>, FlowError> {
    definition
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| open_stage(store, index, stage.operation()))
        .collect()
}

fn open_stage(
    store: &Store,
    index: usize,
    definition: &OperationDefinition,
) -> Result<Stage, FlowError> {
    let state = OrderedMap::new(open_required_data(store, &codec::stage_state_name(index))?);
    match definition {
        OperationDefinition::SequenceSource(definition) => {
            let position = Cell::new(open_required_data(
                store,
                &codec::sequence_position_name(index),
            )?);
            Ok(Stage::sequence_source(state, *definition, position))
        }
        OperationDefinition::Count(definition) => {
            let count = Cell::new(open_required_data(store, &codec::count_state_name(index))?);
            Ok(Stage::count(state, *definition, count))
        }
    }
}

fn open_required_data(store: &Store, name: &str) -> Result<DataHandle, FlowError> {
    match store.open_data(name) {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(name)) => Err(FlowError::MissingResource { name }),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
