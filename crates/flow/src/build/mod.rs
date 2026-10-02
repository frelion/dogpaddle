use crate::{assembly::construct, error::FlowError, flow::Flow};
use dogpaddle_operation::{OperationDefinition, RuntimeResource};
use dogpaddle_store::{Cell, StoreSetup};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
pub(crate) mod codec;
mod definition;
mod open;
pub(crate) mod validate;
pub use codec::FlowDefinitionError;
pub(crate) use definition::FlowDefinition;
pub(crate) use validate::ResolvedTopology;
pub use validate::{InvalidOperationIdReason, TopologyError};
static NEXT_FACTORY_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Declares a logical DAG or reopens its immutable durable definition.
pub struct FlowFactory {
    path: PathBuf,
    token: u64,
    owner_identity: Option<[u8; 32]>,
    operations: Vec<DeclaredOperation>,
    resources: BTreeMap<String, RuntimeResource>,
}
/// Temporary reference to an earlier Operation in this factory.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationRef {
    factory_token: u64,
    index: usize,
}
struct DeclaredOperation {
    id: String,
    definition: OperationDefinition,
    inputs: Vec<OperationRef>,
}
impl FlowFactory {
    /// Creates a side-effect-free factory.
    /// # Panics
    /// Panics only when the process exhausts factory identities.
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        let token = NEXT_FACTORY_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "factory token space exhausted");
        Self {
            path: path.as_ref().to_owned(),
            token,
            owner_identity: None,
            operations: Vec::new(),
            resources: BTreeMap::new(),
        }
    }
    /// Sets the identity persisted at build and required exactly at open.
    pub fn owner_identity(&mut self, identity: [u8; 32]) -> &mut Self {
        self.owner_identity = Some(identity);
        self
    }
    /// Supplies one ephemeral resource by stable logical Operation ID.
    /// # Errors
    /// Returns an error when the ID already has a resource.
    pub fn resource<R: Send + 'static>(
        &mut self,
        id: impl Into<String>,
        resource: R,
    ) -> Result<&mut Self, FlowError> {
        let operation_id = id.into();
        if self.resources.contains_key(&operation_id) {
            return Err(FlowError::DuplicateRuntimeResource { operation_id });
        }
        self.resources
            .insert(operation_id, RuntimeResource::new(resource));
        Ok(self)
    }
    /// Declares an Operation with ordered references to previously declared inputs.
    /// Declaration order also determines construction and source/sink rotation.
    pub fn operation(
        &mut self,
        id: impl Into<String>,
        definition: impl Into<OperationDefinition>,
        inputs: impl IntoIterator<Item = OperationRef>,
    ) -> OperationRef {
        let reference = OperationRef {
            factory_token: self.token,
            index: self.operations.len(),
        };
        self.operations.push(DeclaredOperation {
            id: id.into(),
            definition: definition.into(),
            inputs: inputs.into_iter().collect(),
        });
        reference
    }
    /// Validates and binds the graph in declaration order before atomically publishing its catalog.
    /// The original plan is encoded for persistence without a decode round-trip.
    /// # Errors
    /// Returns topology, schema, resource or Store errors. Failed persistent creation
    /// can leave an incomplete path; open never repairs or deletes it.
    pub fn build(self) -> Result<Flow, FlowError> {
        let definition =
            validate::finish_definition(self.owner_identity, self.token, self.operations)?;
        let encoded = codec::encode(&definition)?;
        let resources = preflight_resources(&definition, self.resources)?;
        let mut setup = StoreSetup::new();
        let published: Cell<Vec<u8>> = setup.create_data(codec::DEFINITION_DATA_NAME)?;
        let mut runtime = construct(definition, &mut setup.data_scope(), resources)?;
        let transactions =
            setup.commit(&self.path, |access| published.access(access)?.set(&encoded))?;
        let (transactions, reads) = transactions.split();
        runtime.restore(reads.begin().access())?;
        Ok(Flow::from_parts(self.path, runtime, transactions, reads))
    }
}
fn preflight_resources(
    definition: &FlowDefinition,
    mut resources: BTreeMap<String, RuntimeResource>,
) -> Result<Vec<RuntimeResource>, FlowError> {
    let mut validated = Vec::with_capacity(definition.operations.len());
    for node in &definition.operations {
        let resource = resources.remove(&node.id).unwrap_or_default();
        node.definition
            .validate_resource(&resource)
            .map_err(|source| FlowError::RuntimeResource {
                operation_id: node.id.clone(),
                source,
            })?;
        validated.push(resource);
    }
    if let Some(operation_id) = resources.into_keys().next() {
        return Err(FlowError::UnknownRuntimeResource { operation_id });
    }
    Ok(validated)
}
