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
pub(crate) use definition::{FlowDefinition, StationDefinition};
pub use schema::FlowSchemaError;
pub use validate::{InvalidStationIdReason, TopologyError};

static NEXT_FACTORY_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Factory for building or opening a persistent Flow.
///
/// Declaring stations, output capacities, and connections is side-effect free.
/// [`FlowFactory::build`] validates the complete graph before creating the Store
/// at the target path.
/// [`FlowFactory::open`] restores an already-built Flow using only the path and
/// any explicitly supplied runtime resources.
pub struct FlowFactory {
    path: PathBuf,
    token: u64,
    owner_identity: Option<[u8; 32]>,
    stations: Vec<StationDefinition>,
    connections: Vec<(Vec<StationRef>, StationRef)>,
    output_capacities: Vec<(StationRef, NonZeroU64)>,
    resources: BTreeMap<String, RuntimeResource>,
}

/// Temporary reference to a station declared in one [`FlowFactory`].
///
/// A reference is valid only while assembling the factory that created it. The
/// durable Flow definition stores stable station IDs instead.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StationRef {
    factory_token: u64,
    index: usize,
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
            stations: Vec::new(),
            connections: Vec::new(),
            output_capacities: Vec::new(),
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

    /// Supplies one ephemeral resource to a Station, for either build or open.
    ///
    /// Resources are moved into Operations during assembly and never persisted.
    /// Flow does not inspect their values or initialize external clients.
    ///
    /// # Errors
    ///
    /// Returns an error if the same Station is assigned more than one resource.
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

    /// Declares one Station with the first Operation in its linear program.
    ///
    /// The returned reference belongs to this factory and is used by
    /// [`FlowFactory::connect`] and [`FlowFactory::output_capacity_bytes`]. The
    /// string ID is the station's durable identity.
    pub fn station<D>(&mut self, id: impl Into<String>, definition: D) -> StationRef
    where
        D: OperationDefinition,
    {
        let reference = StationRef {
            factory_token: self.token,
            index: self.stations.len(),
        };
        self.stations
            .push(StationDefinition::new(id.into(), Box::new(definition)));
        reference
    }

    /// Appends one single-input atomic transform to a Station.
    ///
    /// Appended Operations execute in declaration order inside the Station's
    /// transaction and only the final result reaches its durable output.
    ///
    /// # Errors
    ///
    /// Returns an error if `station` belongs to another factory, the existing
    /// program requires a durable boundary, or `definition` is not a
    /// single-input atomic transform. Failure leaves the Station unchanged.
    pub fn append<D>(
        &mut self,
        station: StationRef,
        definition: D,
    ) -> Result<&mut Self, TopologyError>
    where
        D: OperationDefinition,
    {
        validate::append_operation(
            self.token,
            &mut self.stations,
            station,
            Box::new(definition),
        )?;
        Ok(self)
    }

    /// Declares a Station's complete, ordered input list.
    ///
    /// Call this exactly once for operations with inputs. Scans do
    /// not need a connection. Input order is preserved in the durable definition.
    pub fn connect<I>(&mut self, inputs: I, station: StationRef) -> &mut Self
    where
        I: IntoIterator<Item = StationRef>,
    {
        self.connections
            .push((inputs.into_iter().collect(), station));
        self
    }

    /// Declares the retained-output byte high-water mark for one Station.
    ///
    /// Call this exactly once for every Station whose Operation category has an
    /// output. Outputless Stations must not declare a capacity. The capacity is
    /// persisted as part of the immutable Flow definition. An empty output log
    /// may accept one entry larger than this mark so that one large change cannot
    /// permanently stall the Flow.
    pub fn output_capacity_bytes(
        &mut self,
        station: StationRef,
        capacity: NonZeroU64,
    ) -> &mut Self {
        self.output_capacities.push((station, capacity));
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
        let (definition, topology) = codec::decode(&definition_bytes)?;
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
            assembled.topology,
            transactions,
            reads,
        ))
    }

    fn finish_definition(self) -> Result<FlowDefinition, TopologyError> {
        validate::finish_definition(
            self.owner_identity,
            self.token,
            self.stations,
            &self.connections,
            &self.output_capacities,
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
