use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::ErrorKind,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use rocksdb::{
    BlockBasedOptions, DBCompressionType, Direction, IteratorMode,
    OptimisticTransactionDB as Database, Options, ReadOptions, SnapshotWithThreadMode,
};

use super::{DataHandle, DataKind, DataScope, DataScopeMode, Store, StoreSetup, Transactions};
use crate::{StoreData, StoreError, data_class};

pub(super) const STORE_MARKER_KEY: &[u8] = &[0];
pub(super) const STORE_MARKER: &[u8] = b"dogpaddle.store.rocks.v1\0";
const CATALOG_DOMAIN: u8 = 1;
const MAX_NAME_BYTES: usize = 255;

/// Whole-key bloom filter bits per stored key.
///
/// Ten bits per key is the usual point-lookup tradeoff (roughly a one percent
/// false-positive rate) and is not correctness-relevant: a filter hit is always
/// confirmed against the stored block.
const BLOOM_BITS_PER_KEY: f64 = 10.0;

type Catalog = BTreeMap<String, (u32, DataKind)>;

static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

impl Store {
    /// Creates an empty store at a new path.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is occupied or `RocksDB` cannot be
    /// initialized. Initialization failure may leave a partial directory for
    /// the caller to inspect.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        fs::create_dir(path).map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                StoreError::PathExists(path.to_path_buf())
            } else {
                StoreError::storage("create store directory", error)
            }
        })?;

        let database = open_database(path, true)?;
        database
            .put_opt(
                STORE_MARKER_KEY,
                STORE_MARKER,
                &super::transaction::durable_write_options(),
            )
            .map_err(|error| StoreError::storage("write store marker", error))?;
        Ok(Self {
            database,
            token: fresh_token(),
            catalog: BTreeMap::new(),
            next_data_id: 0,
        })
    }

    /// Opens an existing store.
    ///
    /// # Errors
    ///
    /// Returns an error when the store is missing or corrupt.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        validate_store_path(path)?;
        let database = open_database(path, false)?;
        let snapshot = database.snapshot();
        let marker = snapshot
            .get(STORE_MARKER_KEY)
            .map_err(|error| StoreError::storage("read store marker", error))?
            .ok_or(StoreError::InvalidStore)?;
        if marker != STORE_MARKER {
            return Err(StoreError::InvalidStore);
        }
        let (catalog, next_data_id) = read_catalog(&snapshot)?;
        drop(snapshot);
        Ok(Self {
            database,
            token: fresh_token(),
            catalog,
            next_data_id,
        })
    }

    /// Creates one named typed data object.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate name, exhausted namespace
    /// identifiers, or a storage failure. After a storage failure, the caller
    /// must discard this setup owner because the catalog write outcome may be
    /// indeterminate.
    pub fn create_data<D: StoreData>(&mut self, name: &str) -> Result<D, StoreError> {
        let handle = self.create_handle(name, data_class::kind::<D>())?;
        Ok(data_class::from_handle(handle))
    }

    fn create_handle(&mut self, name: &str, kind: DataKind) -> Result<DataHandle, StoreError> {
        let data_id = create_binding(
            &mut self.catalog,
            &mut self.next_data_id,
            name,
            kind,
            |data_id| {
                self.database
                    .put_opt(
                        catalog_key(name),
                        encode_binding(data_id, kind),
                        &super::transaction::durable_write_options(),
                    )
                    .map_err(|error| StoreError::storage("write data catalog", error))
            },
        )?;
        Ok(self.handle(data_id))
    }

    /// Opens one named typed data object.
    ///
    /// # Errors
    ///
    /// Returns an error when the data object is missing or belongs to another
    /// collection kind.
    pub fn open_data<D: StoreData>(&self, name: &str) -> Result<D, StoreError> {
        validate_name(name)?;
        let &(data_id, actual) = self
            .catalog
            .get(name)
            .ok_or_else(|| StoreError::DataNotFound(name.to_owned()))?;
        let expected = data_class::kind::<D>();
        if actual != expected {
            return Err(StoreError::DataKindMismatch {
                name: name.to_owned(),
                expected: expected.name(),
                actual: actual.name(),
            });
        }
        Ok(data_class::from_handle(self.handle(data_id)))
    }

    /// Borrows a short-lived scope that strictly looks up existing data.
    #[must_use]
    pub const fn data_scope(&self) -> DataScope<'_> {
        DataScope {
            mode: DataScopeMode::Existing(self),
        }
    }

    /// Ends data object setup and yields the unique runtime write capability.
    #[must_use]
    pub fn into_transactions(self) -> Transactions {
        Transactions {
            database: Arc::new(self.database),
            store_token: self.token,
        }
    }

    const fn handle(&self, data_id: u32) -> DataHandle {
        DataHandle {
            store_token: self.token,
            data_id,
        }
    }
}

fn create_binding(
    catalog: &mut Catalog,
    next_data_id: &mut u64,
    name: &str,
    kind: DataKind,
    persist: impl FnOnce(u32) -> Result<(), StoreError>,
) -> Result<u32, StoreError> {
    validate_name(name)?;
    if catalog.contains_key(name) {
        return Err(StoreError::DataAlreadyExists(name.to_owned()));
    }
    let data_id = u32::try_from(*next_data_id).map_err(|_| StoreError::DataIdExhausted)?;
    // Namespace identifiers are monotonic within both a draft and an open Store.
    // Callers never reuse an identifier after this reservation succeeds.
    *next_data_id += 1;
    persist(data_id)?;
    catalog.insert(name.to_owned(), (data_id, kind));
    Ok(data_id)
}

impl StoreSetup {
    /// Creates an empty in-memory Store draft.
    ///
    /// This allocates the final Store identity but performs no filesystem I/O.
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: fresh_token(),
            catalog: BTreeMap::new(),
            next_data_id: 0,
        }
    }

    /// Creates one named typed data object in this in-memory draft.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or duplicate name or exhausted namespace
    /// identifiers.
    pub fn create_data<D: StoreData>(&mut self, name: &str) -> Result<D, StoreError> {
        let kind = data_class::kind::<D>();
        let data_id = create_binding(
            &mut self.catalog,
            &mut self.next_data_id,
            name,
            kind,
            |_| Ok(()),
        )?;
        Ok(data_class::from_handle(DataHandle {
            store_token: self.token,
            data_id,
        }))
    }

    /// Borrows a short-lived scope that strictly declares new data.
    #[must_use]
    pub fn data_scope(&mut self) -> DataScope<'_> {
        DataScope {
            mode: DataScopeMode::Declare(self),
        }
    }
}

impl Default for StoreSetup {
    fn default() -> Self {
        Self::new()
    }
}

impl DataScope<'_> {
    /// Declares or looks up one typed data object according to this scope's
    /// fixed mode.
    ///
    /// A scope from [`StoreSetup::data_scope`] declares a new name and rejects
    /// duplicates. A scope from [`Store::data_scope`] looks up an existing name
    /// and rejects missing names or collection-kind mismatches.
    ///
    /// # Errors
    ///
    /// Returns the corresponding declaration or lookup error.
    pub fn data<D: StoreData>(&mut self, name: &str) -> Result<D, StoreError> {
        match &mut self.mode {
            DataScopeMode::Declare(setup) => setup.create_data(name),
            DataScopeMode::Existing(store) => store.open_data(name),
        }
    }
}

impl DataKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Cell => 1,
            Self::OrderedMap => 2,
            Self::OrderedMultiset => 4,
            Self::PartitionedMultiset => 5,
            Self::Queue => 6,
            Self::SubscribedLog => 7,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Cell => "cell",
            Self::OrderedMap => "ordered map",
            Self::OrderedMultiset => "ordered multiset",
            Self::PartitionedMultiset => "partitioned multiset",
            Self::Queue => "queue",
            Self::SubscribedLog => "subscribed log",
        }
    }

    const fn decode(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Cell),
            2 => Some(Self::OrderedMap),
            4 => Some(Self::OrderedMultiset),
            5 => Some(Self::PartitionedMultiset),
            6 => Some(Self::Queue),
            7 => Some(Self::SubscribedLog),
            _ => None,
        }
    }
}

pub(super) fn open_database(path: &Path, create: bool) -> Result<Database, StoreError> {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_error_if_exists(create);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_block_based_table_factory(&table_options());
    Database::open(&options, path).map_err(|error| StoreError::storage("open RocksDB", error))
}

/// Table options shared by every collection in the default column family.
///
/// Point lookups dominate this Store: an adjustment always reads the current
/// multiplicity first, and for a key that is not stored yet that read would
/// otherwise fetch and decompress a data block only to learn the key is absent.
/// A whole-key filter answers such a lookup without touching a block, and a
/// false positive only costs the block read we would have paid anyway.
///
/// Nothing else is configured here. A prefix filter would need one fixed-length
/// partition header shared by all six collections, which is a layout decision
/// rather than an option; cache sizing is deliberately left at the `RocksDB`
/// default until a measurement justifies a number.
fn table_options() -> BlockBasedOptions {
    let mut table = BlockBasedOptions::default();
    table.set_bloom_filter(BLOOM_BITS_PER_KEY, true);
    table.set_whole_key_filtering(true);
    table
}

fn validate_store_path(path: &Path) -> Result<(), StoreError> {
    let metadata = fs::metadata(path).map_err(|error| {
        if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) {
            StoreError::StoreNotFound(path.to_path_buf())
        } else {
            StoreError::storage("inspect store directory", error)
        }
    })?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(StoreError::StoreNotFound(path.to_path_buf()))
    }
}

fn read_catalog(
    snapshot: &SnapshotWithThreadMode<'_, Database>,
) -> Result<(Catalog, u64), StoreError> {
    let mut catalog = BTreeMap::new();
    let mut data_ids = HashSet::new();
    let mut next_data_id = 0_u64;
    let mut options = ReadOptions::default();
    options.set_iterate_upper_bound([CATALOG_DOMAIN + 1]);
    for item in snapshot.iterator_opt(
        IteratorMode::From(&[CATALOG_DOMAIN], Direction::Forward),
        options,
    ) {
        let (key, value) = item.map_err(|error| StoreError::storage("read data catalog", error))?;
        let name = key
            .strip_prefix(&[CATALOG_DOMAIN])
            .expect("the catalog iterator is bounded to its key domain");
        let name = std::str::from_utf8(name).map_err(|_| StoreError::InvalidStore)?;
        validate_name(name).map_err(|_| StoreError::InvalidStore)?;
        let (data_id, kind) = decode_binding(&value)?;
        if !data_ids.insert(data_id) {
            return Err(StoreError::InvalidStore);
        }
        next_data_id = next_data_id.max(u64::from(data_id) + 1);
        catalog.insert(name.to_owned(), (data_id, kind));
    }
    Ok((catalog, next_data_id))
}

fn fresh_token() -> u64 {
    NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
}

fn validate_name(name: &str) -> Result<(), StoreError> {
    let reason = if name.is_empty() {
        Some("name must not be empty")
    } else if name.len() > MAX_NAME_BYTES {
        Some("name is too long")
    } else if name.as_bytes().contains(&0) {
        Some("name must not contain NUL")
    } else {
        None
    };
    reason.map_or(Ok(()), |reason| {
        Err(StoreError::InvalidName {
            name: name.to_owned(),
            reason,
        })
    })
}

pub(super) fn catalog_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.len());
    key.push(CATALOG_DOMAIN);
    key.extend_from_slice(name.as_bytes());
    key
}

pub(super) fn encode_binding(data_id: u32, kind: DataKind) -> [u8; 5] {
    let [a, b, c, d] = data_id.to_be_bytes();
    [kind.tag(), a, b, c, d]
}

fn decode_binding(bytes: &[u8]) -> Result<(u32, DataKind), StoreError> {
    let [tag, a, b, c, d] = bytes else {
        return Err(StoreError::InvalidStore);
    };
    let kind = DataKind::decode(*tag).ok_or(StoreError::InvalidStore)?;
    Ok((u32::from_be_bytes([*a, *b, *c, *d]), kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_catalog_write_reserves_the_attempted_namespace() {
        let mut catalog = Catalog::new();
        let mut next_data_id = 0;
        let failure = create_binding(
            &mut catalog,
            &mut next_data_id,
            "uncertain",
            DataKind::Cell,
            |_| Err(StoreError::storage("injected catalog write", "failure")),
        );
        assert!(matches!(failure, Err(StoreError::Storage { .. })));

        let next = create_binding(
            &mut catalog,
            &mut next_data_id,
            "next",
            DataKind::Cell,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(next, 1);
        assert_eq!(next_data_id, 2);
        assert_eq!(catalog.get("next"), Some(&(1, DataKind::Cell)));
    }
}
