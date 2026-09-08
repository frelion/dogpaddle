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
    DBCompressionType, Direction, IteratorMode, OptimisticTransactionDB as Database, Options,
    ReadOptions, SnapshotWithThreadMode,
};

use super::{DataHandle, DataKind, Store, Transactions, transaction::durable_write_options};
use crate::{StoreData, StoreError, data_class};

const STORE_MARKER_KEY: &[u8] = &[0];
const STORE_MARKER: &[u8] = b"dogpaddle.store.rocks.v1\0";
const CATALOG_DOMAIN: u8 = 1;
const MAX_NAME_BYTES: usize = 255;

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
            .put_opt(STORE_MARKER_KEY, STORE_MARKER, &durable_write_options())
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
    /// identifiers, or a storage failure.
    pub fn create_data<D: StoreData>(&mut self, name: &str) -> Result<D, StoreError> {
        let handle = self.create_handle(name, data_class::kind::<D>())?;
        Ok(data_class::from_handle(handle))
    }

    fn create_handle(&mut self, name: &str, kind: DataKind) -> Result<DataHandle, StoreError> {
        validate_name(name)?;
        if self.catalog.contains_key(name) {
            return Err(StoreError::DataAlreadyExists(name.to_owned()));
        }
        let data_id = u32::try_from(self.next_data_id).map_err(|_| StoreError::DataIdExhausted)?;
        self.database
            .put_opt(
                catalog_key(name),
                encode_binding(data_id, kind),
                &durable_write_options(),
            )
            .map_err(|error| StoreError::storage("write data catalog", error))?;
        self.catalog.insert(name.to_owned(), (data_id, kind));
        self.next_data_id += 1;
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

fn open_database(path: &Path, create: bool) -> Result<Database, StoreError> {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_error_if_exists(create);
    options.set_compression_type(DBCompressionType::Lz4);
    Database::open(&options, path).map_err(|error| StoreError::storage("open RocksDB", error))
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

fn catalog_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.len());
    key.push(CATALOG_DOMAIN);
    key.extend_from_slice(name.as_bytes());
    key
}

fn encode_binding(data_id: u32, kind: DataKind) -> [u8; 5] {
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
