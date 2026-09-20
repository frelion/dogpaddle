use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_operation::{OperationDefinition, RuntimeResource};
use dogpaddle_store::{Cell, StoreSetup};

use crate::{assembly::assemble_stations, error::FlowError, flow::Flow};

pub(crate) mod codec;
mod definition;
mod open;
mod schema;
mod validate;

pub use codec::FlowDefinitionError;
pub(crate) use definition::FlowDefinition;
pub use schema::FlowSchemaError;
pub use validate::{InvalidStationIdReason, TopologyError};

static NEXT_FACTORY_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Factory for building or opening a persistent Flow.
///
/// Declaring Operations and their ordered inputs is side-effect free.
/// [`FlowFactory::build`] validates the complete graph before creating the Store
/// at the target path.
/// [`FlowFactory::open`] restores an already-built Flow using only the path and
/// any explicitly supplied runtime resources.
pub struct FlowFactory {
    path: PathBuf,
    token: u64,
    owner_identity: Option<[u8; 32]>,
    operations: Vec<DeclaredOperation>,
    output_capacity: NonZeroU64,
    materializations: Vec<(OperationRef, NonZeroU64)>,
    resources: BTreeMap<String, RuntimeResource>,
}

/// Temporary reference to an Operation declared in one [`FlowFactory`].
///
/// A reference is valid only while assembling the factory that created it. The
/// durable Flow definition stores the resulting Station programs instead.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationRef {
    factory_token: u64,
    index: usize,
}

struct DeclaredOperation {
    id: String,
    definition: Box<dyn OperationDefinition>,
    inputs: Vec<OperationRef>,
}

impl FlowFactory {
    /// Starts a side-effect-free factory for building or opening a persistent Flow.
    ///
    /// # Panics
    ///
    /// Panics if the process exhausts the nonzero factory-token space.
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        let token = NEXT_FACTORY_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "flow factory token space exhausted");
        Self {
            path: path.as_ref().to_path_buf(),
            token,
            owner_identity: None,
            operations: Vec::new(),
            output_capacity: NonZeroU64::new(64 * 1024 * 1024).expect("nonzero default capacity"),
            materializations: Vec::new(),
            resources: BTreeMap::new(),
        }
    }

    /// Sets the opaque identity of the owner that declares or expects this Flow.
    ///
    /// Build persists this value inside the immutable Flow definition. Open
    /// requires an exact match before binding Operations or runtime resources.
    /// A factory whose owner identity is unset only matches a definition built
    /// without one.
    pub fn owner_identity(&mut self, identity: [u8; 32]) -> &mut Self {
        self.owner_identity = Some(identity);
        self
    }

    /// Supplies an ephemeral resource by stable Operation ID, for build or open.
    ///
    /// Resources are moved into Operations during assembly and never persisted.
    /// Flow does not inspect their values or initialize external clients. Resources
    /// must belong to the first Operation in a resulting Station; build rejects
    /// resources addressed to fused tails.
    ///
    /// # Errors
    ///
    /// Returns an error if the same ID is assigned more than one resource.
    pub fn resource<R: Send + 'static>(
        &mut self,
        station_id: impl Into<String>,
        resource: R,
    ) -> Result<&mut Self, FlowError> {
        let station_id = station_id.into();
        if self.resources.contains_key(&station_id) {
            return Err(FlowError::DuplicateRuntimeResource { station_id });
        }
        self.resources
            .insert(station_id, RuntimeResource::new(resource));
        Ok(self)
    }

    /// Declares an Operation and its complete, ordered inputs.
    ///
    /// Inputs must refer to Operations previously declared in this factory.
    /// Build fuses eligible single-input atomic Operations automatically. Each
    /// resulting Station takes its first Operation's stable ID.
    pub fn operation(
        &mut self,
        id: impl Into<String>,
        definition: Box<dyn OperationDefinition>,
        inputs: impl IntoIterator<Item = OperationRef>,
    ) -> OperationRef {
        let reference = OperationRef {
            factory_token: self.token,
            index: self.operations.len(),
        };
        self.operations.push(DeclaredOperation {
            id: id.into(),
            definition,
            inputs: inputs.into_iter().collect(),
        });
        reference
    }

    /// Sets the retained-byte capacity for automatically created durable outputs.
    ///
    /// The default is 64 MiB. Only actual Station outputs persist a capacity.
    /// An empty output may admit one larger Change to avoid permanent stalls.
    /// This setting has no effect when opening an existing Flow.
    pub fn output_capacity_bytes(&mut self, capacity: NonZeroU64) -> &mut Self {
        self.output_capacity = capacity;
        self
    }

    /// Requires a durable output after this Operation, with the given capacity.
    ///
    /// This prevents fusion across that output and establishes an explicit
    /// transaction and backpressure boundary. Build rejects foreign references,
    /// duplicate declarations, and Operations without an output.
    pub fn materialize(&mut self, operation: OperationRef, capacity: NonZeroU64) -> &mut Self {
        self.materializations.push((operation, capacity));
        self
    }

    /// Validates the Flow, creates its data objects, and atomically publishes its definition.
    ///
    /// Pure topology validation and definition encoding finish before the Store
    /// path is created. The encoded bytes are decoded as the canonical durable
    /// Definition, all Operations are purely bound to exact Schemas, required
    /// data objects are staged, and the complete catalog, initialized data, and
    /// definition Cell are committed in one setup transaction.
    ///
    /// # Errors
    ///
    /// Returns a [`FlowError`] for an invalid topology, Schema or runtime resource,
    /// unencodable definition, occupied path, or Store failure. A Store failure
    /// after path creation can leave an incomplete build that [`FlowFactory::open`]
    /// refuses to open.
    pub fn build(mut self) -> Result<Flow, FlowError> {
        let resources = std::mem::take(&mut self.resources);
        let path = self.path.clone();
        let declared_definition = self.finish_definition()?;
        let definition_bytes = codec::encode(&declared_definition)?;
        let (definition, topology) =
            codec::decode(&definition_bytes).map_err(|error| match error {
                FlowDefinitionError::Topology(error) => FlowError::Topology(error),
                error => FlowError::Definition(error),
            })?;
        let resources = preflight_resources(&definition, resources)?;
        let station_ids = definition
            .stations()
            .iter()
            .map(|station| station.id().to_owned())
            .collect();

        let mut setup = StoreSetup::new();
        let published: Cell<Vec<u8>> = setup.create_data(codec::DEFINITION_DATA_NAME)?;
        let station_parts =
            schema::construct_stations(&definition, &topology, &mut setup.data_scope(), resources)?;
        let transactions = setup.commit(&path, |access| {
            for (index, station) in station_parts.iter().enumerate() {
                station.initialize(topology.subscriber_count(index), access)?;
            }
            let mut published = published.access(access)?;
            published.set(&definition_bytes)?;
            Ok(())
        })?;
        let (transactions, reads) = transactions.split();
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

    fn finish_definition(self) -> Result<FlowDefinition, TopologyError> {
        validate::finish_definition(
            self.owner_identity,
            self.token,
            self.operations,
            self.output_capacity,
            &self.materializations,
        )
    }
}
fn preflight_resources(
    definition: &FlowDefinition,
    mut resources: BTreeMap<String, RuntimeResource>,
) -> Result<Vec<RuntimeResource>, FlowError> {
    let mut validated = Vec::with_capacity(definition.stations().len());
    for station in definition.stations() {
        let resource = resources.remove(station.id()).unwrap_or_default();
        for (operation, operation_definition) in station.operations().iter().enumerate() {
            let empty = RuntimeResource::default();
            let operation_resource = if operation == 0 { &resource } else { &empty };
            operation_definition
                .validate_resource(operation_resource)
                .map_err(|source| FlowError::RuntimeResource {
                    station_id: station.id().to_owned(),
                    source,
                })?;
        }
        validated.push(resource);
    }
    if let Some(station_id) = resources.into_keys().next() {
        return Err(FlowError::UnknownRuntimeResource { station_id });
    }
    Ok(validated)
}

#[cfg(test)]
mod tests;
