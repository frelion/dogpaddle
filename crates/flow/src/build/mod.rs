use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_operation::OperationDefinition;
use dogpaddle_store::{Cell, DataPlacement, OrderedMap, Store};

use crate::{error::FlowError, flow::Flow, stage::Stage};

pub(crate) mod codec;
mod definition;
mod validate;

pub use codec::FlowDefinitionError;
pub(crate) use definition::{FlowDefinition, StageDefinition};
pub use validate::{InvalidStageIdReason, TopologyError};

static NEXT_BUILDER_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Builder for one persistent, immutable Flow definition.
///
/// Declaring stages and connections is side-effect free. [`FlowBuilder::build`]
/// validates the complete graph before creating the Store at the target path.
pub struct FlowBuilder {
    path: PathBuf,
    token: u64,
    stages: Vec<StageDefinition>,
    connections: Vec<(Vec<StageRef>, StageRef)>,
}

/// Temporary reference to a stage declared in one [`FlowBuilder`].
///
/// A reference is valid only while assembling the builder that created it. The
/// durable Flow definition stores stable stage IDs instead.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StageRef {
    builder_token: u64,
    index: usize,
}

impl FlowBuilder {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        let token = NEXT_BUILDER_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "flow builder token space exhausted");
        Self {
            path: path.as_ref().to_path_buf(),
            token,
            stages: Vec::new(),
            connections: Vec::new(),
        }
    }

    /// Declares one stage containing exactly one concrete operation definition.
    ///
    /// The returned reference belongs to this builder and is used only by
    /// [`FlowBuilder::connect`]. The string ID is the stage's durable identity.
    pub fn stage<D>(&mut self, id: impl Into<String>, definition: D) -> StageRef
    where
        D: Into<OperationDefinition>,
    {
        let reference = StageRef {
            builder_token: self.token,
            index: self.stages.len(),
        };
        self.stages
            .push(StageDefinition::new(id.into(), definition.into()));
        reference
    }

    /// Declares the target's complete, ordered source list.
    ///
    /// Call this exactly once for operations with inputs. Zero-input sources do
    /// not need a connection. Input order is preserved in the durable definition.
    pub fn connect<I>(&mut self, sources: I, target: StageRef) -> &mut Self
    where
        I: IntoIterator<Item = StageRef>,
    {
        self.connections
            .push((sources.into_iter().collect(), target));
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
        let path = self.path.clone();
        let definition = self.finish_definition()?;
        let definition_bytes = codec::encode(&definition)?;

        let mut store = Store::create(&path)?;
        let published =
            Cell::new(store.create_data(codec::DEFINITION_DATA_NAME, DataPlacement::Shared)?);
        let flow_state = store.create_data(codec::FLOW_STATE_DATA_NAME, DataPlacement::Shared)?;
        let stages = create_stages(&mut store, &definition)?;
        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin()?;
            let mut published = published.access(&transaction)?;
            published.set(&definition_bytes)?;
            transaction.commit()?;
        }

        Ok(Flow::from_build(
            path,
            definition,
            OrderedMap::new(flow_state),
            stages,
            transactions,
        ))
    }

    fn finish_definition(self) -> Result<FlowDefinition, TopologyError> {
        validate::finish_definition(self.token, self.stages, &self.connections)
    }
}

fn create_stages(store: &mut Store, definition: &FlowDefinition) -> Result<Vec<Stage>, FlowError> {
    definition
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| Stage::create(store, index, stage.operation()))
        .collect()
}

#[cfg(test)]
mod tests;
