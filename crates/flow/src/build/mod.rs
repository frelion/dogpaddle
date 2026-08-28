use std::{
    collections::BTreeSet,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_operation::{DataInstances, MaterializeError, OperationDefinition};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

use crate::{assembly::assemble_stations, error::FlowError, flow::Flow, station::StationParts};

pub(crate) mod codec;
mod definition;
mod open;
mod validate;

pub use codec::FlowDefinitionError;
pub(crate) use definition::{FlowDefinition, StationDefinition};
pub use validate::{InvalidStationIdReason, TopologyError};

static NEXT_FACTORY_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Factory for building or opening a persistent Flow.
///
/// Declaring stations, output capacities, and connections is side-effect free.
/// [`FlowFactory::build`] validates the complete graph before creating the Store
/// at the target path.
/// [`FlowFactory::open`] directly restores an already-built Flow without
/// creating a factory instance.
pub struct FlowFactory {
    path: PathBuf,
    token: u64,
    stations: Vec<StationDefinition>,
    connections: Vec<(Vec<StationRef>, StationRef)>,
    output_capacities: Vec<(StationRef, NonZeroU64)>,
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
    /// Starts a side-effect-free definition for a new persistent Flow.
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
            stations: Vec::new(),
            connections: Vec::new(),
            output_capacities: Vec::new(),
        }
    }

    /// Declares one station containing exactly one concrete operation definition.
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

    /// Declares the target's complete, ordered source list.
    ///
    /// Call this exactly once for operations with inputs. Zero-input sources do
    /// not need a connection. Input order is preserved in the durable definition.
    pub fn connect<I>(&mut self, sources: I, target: StationRef) -> &mut Self
    where
        I: IntoIterator<Item = StationRef>,
    {
        self.connections
            .push((sources.into_iter().collect(), target));
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
    /// path is created. Required data objects are then created and the
    /// definition Cell is committed last as the build-complete marker.
    ///
    /// # Errors
    ///
    /// Returns a [`FlowError`] for an invalid topology, unencodable definition,
    /// occupied path, or Store failure. A Store failure after path creation can
    /// leave an incomplete build that [`FlowFactory::open`] refuses to open.
    pub fn build(self) -> Result<Flow, FlowError> {
        let path = self.path.clone();
        let definition = self.finish_definition()?;
        validate_data_declarations(&definition)?;
        let definition_bytes = codec::encode(&definition)?;

        let mut store = Store::create(&path)?;
        let published: Cell<Vec<u8>> = store.create_data(codec::DEFINITION_DATA_NAME)?;
        let flow_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
            store.create_data(codec::FLOW_STATE_DATA_NAME)?;
        let station_parts = definition
            .stations()
            .iter()
            .enumerate()
            .map(|(index, station)| create_station_part(&mut store, index, station))
            .collect::<Result<Vec<_>, _>>()?;
        let (mut transactions, reads) = store.into_transactions().split();
        {
            let transaction = transactions.begin()?;
            for station in &station_parts {
                station.initialize_input_state(transaction.access())?;
            }
            let mut published = published.access(transaction.access())?;
            published.set(&definition_bytes)?;
            transaction.commit()?;
        }
        let (stations, topology) = assemble_stations(&definition, station_parts);

        Ok(Flow::from_parts(
            path,
            definition,
            flow_state,
            stations,
            topology,
            transactions,
            reads,
        ))
    }

    fn finish_definition(self) -> Result<FlowDefinition, TopologyError> {
        validate::finish_definition(
            self.token,
            self.stations,
            &self.connections,
            &self.output_capacities,
        )
    }
}

fn validate_data_declarations(definition: &FlowDefinition) -> Result<(), MaterializeError> {
    for station in definition.stations() {
        let mut names = BTreeSet::new();
        for declaration in station.operation().data() {
            let name = declaration.name();
            if !names.insert(name) {
                return Err(MaterializeError::DuplicateData { name });
            }
        }
    }
    Ok(())
}

fn create_station_part(
    store: &mut Store,
    index: usize,
    station: &StationDefinition,
) -> Result<StationParts, FlowError> {
    let state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.create_data(&codec::station_state_name(index))?;
    let definition = station.operation();
    let mut data = DataInstances::new();
    for declaration in definition.data() {
        let physical_name = codec::station_operation_data_name(index, declaration.name());
        data.insert(declaration.create(store, &physical_name)?)?;
    }
    let operation = definition.materialize(&mut data)?;
    data.finish()?;
    let output = station
        .output_capacity_bytes()
        .map(|capacity| {
            store
                .create_data::<AppendLog<Vec<u8>>>(&codec::station_output_name(index))
                .map(|log| (log, capacity))
        })
        .transpose()?;
    Ok(StationParts::new(state, operation, output))
}

#[cfg(test)]
mod tests;
