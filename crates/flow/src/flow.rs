use std::path::{Path, PathBuf};

use dogpaddle_operation::OperationDefinition;
use dogpaddle_store::{
    Cell, DataHandle, DataPlacement, OrderedMap, Store, StoreError, Transactions,
};

use crate::{
    FlowError, StageRef, format,
    stage::Stage,
    topology::{InputCount, Topology, TopologyBuilder},
};

/// Builder for one persistent, immutable Flow definition.
///
/// Declaring stages and connections is side-effect free. [`FlowBuilder::build`]
/// validates the complete graph before creating the Store at the target path.
pub struct FlowBuilder {
    path: PathBuf,
    topology: TopologyBuilder<OperationDefinition>,
}

/// An opened persistent Flow.
///
/// A Flow owns the only active Store transaction capability for its path. Its
/// topology and data namespaces were frozen by a successful build.
pub struct Flow {
    path: PathBuf,
    topology: Topology<OperationDefinition>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "flow state is consumed by the next run phase")
    )]
    data: FlowData,
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

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "flow state is consumed by the next run phase")
)]
struct FlowData {
    state: OrderedMap<Vec<u8>, Vec<u8>>,
}

impl InputCount for OperationDefinition {
    fn input_count(&self) -> usize {
        OperationDefinition::input_count(self)
    }
}

impl Flow {
    /// Starts a side-effect-free definition builder for `path`.
    #[must_use]
    pub fn builder(path: impl AsRef<Path>) -> FlowBuilder {
        FlowBuilder {
            path: path.as_ref().to_path_buf(),
            topology: TopologyBuilder::new(),
        }
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
        let topology = format::decode(&definition_bytes)?;

        let store = Store::open(&path)?;
        let definition = open_definition_cell(&store)?;
        let flow_state = open_required_data(&store, format::FLOW_STATE_DATA_NAME)?;
        let stages = open_stages(&store, &topology)?;
        let mut transactions = store.into_transactions();
        let observed_definition = {
            let transaction = transactions.begin()?;
            let definition = definition.access(&transaction)?;
            definition.get()?.ok_or(FlowError::IncompleteBuild)?
        };
        if observed_definition != definition_bytes {
            return Err(FlowError::DefinitionChangedDuringOpen);
        }

        Ok(Self {
            path,
            topology,
            data: FlowData {
                state: OrderedMap::new(flow_state),
            },
            stages,
            transactions,
        })
    }

    /// Returns the Store path owned by this Flow.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of stages in declaration order.
    #[must_use]
    pub fn stage_count(&self) -> usize {
        self.topology.stages().len()
    }

    /// Iterates over stable stage IDs in declaration order.
    #[must_use]
    pub fn stage_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.topology
            .stages()
            .iter()
            .map(crate::topology::StageDefinition::id)
    }
}

impl FlowBuilder {
    /// Declares one stage containing exactly one concrete operation definition.
    ///
    /// The returned reference belongs to this builder and is used only by
    /// [`FlowBuilder::connect`]. The string ID is the stage's durable identity.
    pub fn stage<D>(&mut self, id: impl Into<String>, definition: D) -> StageRef
    where
        D: Into<OperationDefinition>,
    {
        self.topology.stage(id, definition.into())
    }

    /// Declares the target's complete, ordered source list.
    ///
    /// Call this exactly once for operations with inputs. Zero-input sources do
    /// not need a connection. Input order is preserved in the durable definition.
    pub fn connect<I>(&mut self, sources: I, target: StageRef) -> &mut Self
    where
        I: IntoIterator<Item = StageRef>,
    {
        self.topology.connect(sources, target);
        self
    }

    /// Validates, provisions, and atomically publishes this Flow definition.
    ///
    /// Pure topology validation and definition encoding finish before the Store
    /// path is created. Resource namespaces are then provisioned and the
    /// definition Cell is committed last as the build-complete marker.
    ///
    /// # Errors
    ///
    /// Returns a [`FlowError`] for an invalid topology, unencodable definition,
    /// occupied path, or Store failure. A Store failure after path creation can
    /// leave an incomplete build that [`Flow::open`] refuses to open.
    pub fn build(self) -> Result<Flow, FlowError> {
        let topology = self.topology.finish()?;
        let definition_bytes = format::encode(&topology)?;

        let mut store = Store::create(&self.path)?;
        let definition =
            Cell::new(store.create_data(format::DEFINITION_DATA_NAME, DataPlacement::Shared)?);
        let flow_state = store.create_data(format::FLOW_STATE_DATA_NAME, DataPlacement::Shared)?;
        let stages = create_stages(&mut store, &topology)?;
        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin()?;
            let mut definition = definition.access(&transaction)?;
            definition.set(&definition_bytes)?;
            transaction.commit()?;
        }

        Ok(Flow {
            path: self.path,
            topology,
            data: FlowData {
                state: OrderedMap::new(flow_state),
            },
            stages,
            transactions,
        })
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
    match store.open_data(format::DEFINITION_DATA_NAME) {
        Ok(data) => Ok(Cell::new(data)),
        Err(StoreError::DataNotFound(name)) if name == format::DEFINITION_DATA_NAME => {
            Err(FlowError::IncompleteBuild)
        }
        Err(error) => Err(error.into()),
    }
}

fn create_stages(
    store: &mut Store,
    topology: &Topology<OperationDefinition>,
) -> Result<Vec<Stage>, FlowError> {
    topology
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| Stage::create(store, index, stage.operation()))
        .collect()
}

fn open_stages(
    store: &Store,
    topology: &Topology<OperationDefinition>,
) -> Result<Vec<Stage>, FlowError> {
    topology
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| Stage::open(store, index, stage.operation()))
        .collect()
}

fn open_required_data(store: &Store, name: &str) -> Result<DataHandle, FlowError> {
    match store.open_data(name) {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(name)) => Err(FlowError::MissingResource { name }),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
#[path = "../tests/unit/flow.rs"]
mod tests;
