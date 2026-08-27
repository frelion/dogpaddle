use std::{
    collections::HashSet,
    fs,
    io::ErrorKind,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, TableFlags, WriteFlags,
};

use super::{
    DataHandle, DataLocation, DataPlacement, Store, Transactions, dedicated_table_name,
    transaction::commit_mdbx,
};
use crate::{StoreData, StoreError, data_class};

const MDBX_DATA_FILE: &str = "mdbx.dat";
const STORE_MARKER_KEY: &[u8] = &[0];
const NEXT_ID_KEY: &[u8] = &[1];
const NEXT_DEDICATED_KEY: &[u8] = &[4];
const CATALOG_DOMAIN: u8 = 2;
const STORE_MARKER: &[u8] = b"dogpaddle.store\0";
const MAX_NAME_BYTES: usize = 255;
const MAX_DEDICATED_TABLES: u32 = 4_096;
const SHARED_PLACEMENT: u8 = 0;
const DEDICATED_PLACEMENT: u8 = 1;

static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

impl Store {
    /// Maximum number of data objects that may use dedicated physical tables.
    pub const LARGE_DATA_CAPACITY: u32 = MAX_DEDICATED_TABLES;

    /// Creates an empty store at a new path.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is occupied or MDBX cannot be initialized.
    /// Initialization failure may leave a partial directory for the caller to inspect.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        fs::create_dir(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StoreError::PathExists(path.to_path_buf())
            } else {
                StoreError::storage("create store directory", error)
            }
        })?;

        let database = open_database(path)?;
        let transaction = database
            .begin_rw_txn()
            .map_err(|error| StoreError::storage("begin store creation", error))?;
        let table = transaction
            .open_table(None)
            .map_err(|error| StoreError::storage("open store table", error))?;
        transaction
            .put(
                &table,
                STORE_MARKER_KEY,
                STORE_MARKER,
                WriteFlags::NO_OVERWRITE,
            )
            .map_err(|error| StoreError::storage("write store marker", error))?;
        transaction
            .put(
                &table,
                NEXT_ID_KEY,
                0_u32.to_be_bytes(),
                WriteFlags::NO_OVERWRITE,
            )
            .map_err(|error| StoreError::storage("write store counter", error))?;
        transaction
            .put(
                &table,
                NEXT_DEDICATED_KEY,
                0_u32.to_be_bytes(),
                WriteFlags::NO_OVERWRITE,
            )
            .map_err(|error| StoreError::storage("write dedicated table counter", error))?;
        commit_mdbx(transaction)?;
        Ok(Self {
            database,
            token: fresh_token(),
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
        let database = open_database(path)?;
        let transaction = database
            .begin_ro_txn()
            .map_err(|error| StoreError::storage("begin store validation", error))?;
        let table = transaction
            .open_table(None)
            .map_err(|error| StoreError::storage("open store table", error))?;
        let marker = transaction
            .get::<Vec<u8>>(&table, STORE_MARKER_KEY)
            .map_err(|error| StoreError::storage("read store marker", error))?
            .ok_or(StoreError::InvalidStore)?;
        if marker != STORE_MARKER {
            return Err(StoreError::InvalidStore);
        }
        let next_id = transaction
            .get::<Vec<u8>>(&table, NEXT_ID_KEY)
            .map_err(|error| StoreError::storage("read store counter", error))?
            .ok_or(StoreError::InvalidStore)?;
        let next_id = decode_u32(&next_id)?;
        let next_dedicated = transaction
            .get::<Vec<u8>>(&table, NEXT_DEDICATED_KEY)
            .map_err(|error| StoreError::storage("read dedicated table counter", error))?
            .ok_or(StoreError::InvalidStore)?;
        let next_dedicated = decode_u32(&next_dedicated)?;
        if next_dedicated > MAX_DEDICATED_TABLES {
            return Err(StoreError::InvalidStore);
        }
        validate_catalog(&transaction, &table, next_id, next_dedicated)?;
        drop(transaction);
        Ok(Self {
            database,
            token: fresh_token(),
        })
    }

    /// Creates one named typed data object.
    ///
    /// # Errors
    ///
    /// The data class fixes or selects its durable physical placement. A
    /// [`crate::Cell`] is always shared, an [`crate::AppendLog`] is always
    /// dedicated, and an [`crate::OrderedMap`] uses its [`crate::Small`] or
    /// [`crate::Large`] type parameter. At most [`Store::LARGE_DATA_CAPACITY`]
    /// objects may use dedicated placement.
    ///
    /// Returns an error for an invalid or duplicate name, exhausted dedicated
    /// capacity, or an MDBX failure.
    pub fn create_data<D: StoreData>(&mut self, name: &str) -> Result<D, StoreError> {
        let handle = self.create_handle(name, data_class::placement::<D>())?;
        Ok(data_class::from_handle(handle))
    }

    fn create_handle(
        &mut self,
        name: &str,
        placement: DataPlacement,
    ) -> Result<DataHandle, StoreError> {
        validate_name(name)?;
        let transaction = self
            .database
            .begin_rw_txn()
            .map_err(|error| StoreError::storage("begin data creation", error))?;
        let table = transaction
            .open_table(None)
            .map_err(|error| StoreError::storage("open store table", error))?;
        let catalog_key = catalog_key(name);
        if transaction
            .get::<Vec<u8>>(&table, &catalog_key)
            .map_err(|error| StoreError::storage("read data catalog", error))?
            .is_some()
        {
            return Err(StoreError::DataAlreadyExists(name.to_owned()));
        }
        let (location, counter_key, next) = match placement {
            DataPlacement::Shared => {
                let current = read_counter(&transaction, &table, NEXT_ID_KEY)?;
                let next = current.checked_add(1).ok_or(StoreError::DataIdExhausted)?;
                (DataLocation::Shared(current), NEXT_ID_KEY, next)
            }
            DataPlacement::Dedicated => {
                let current = read_counter(&transaction, &table, NEXT_DEDICATED_KEY)?;
                if current >= MAX_DEDICATED_TABLES {
                    return Err(StoreError::LargeDataCapacityExhausted);
                }
                let next = current + 1;
                transaction
                    .create_table(Some(&dedicated_table_name(current)), TableFlags::empty())
                    .map_err(|error| StoreError::storage("create dedicated data table", error))?;
                (DataLocation::Dedicated(current), NEXT_DEDICATED_KEY, next)
            }
        };
        transaction
            .put(
                &table,
                &catalog_key,
                encode_binding(location),
                WriteFlags::NO_OVERWRITE,
            )
            .map_err(|error| StoreError::storage("write data catalog", error))?;
        transaction
            .put(&table, counter_key, next.to_be_bytes(), WriteFlags::UPSERT)
            .map_err(|error| StoreError::storage("advance store counter", error))?;
        commit_mdbx(transaction)?;
        Ok(self.handle(location))
    }

    /// Opens one named typed data object.
    ///
    /// # Errors
    ///
    /// Returns an error when the data object is missing, its durable placement
    /// does not match `D`, or MDBX fails.
    pub fn open_data<D: StoreData>(&self, name: &str) -> Result<D, StoreError> {
        let handle = self.open_handle(name)?;
        let expected = data_class::placement::<D>();
        let actual = handle.placement();
        if actual != expected {
            return Err(StoreError::DataSizeMismatch {
                name: name.to_owned(),
                expected: expected.size_name(),
                actual: actual.size_name(),
            });
        }
        Ok(data_class::from_handle(handle))
    }

    fn open_handle(&self, name: &str) -> Result<DataHandle, StoreError> {
        validate_name(name)?;
        let transaction = self
            .database
            .begin_ro_txn()
            .map_err(|error| StoreError::storage("begin data lookup", error))?;
        let table = transaction
            .open_table(None)
            .map_err(|error| StoreError::storage("open store table", error))?;
        let binding = transaction
            .get::<Vec<u8>>(&table, &catalog_key(name))
            .map_err(|error| StoreError::storage("read data catalog", error))?
            .ok_or_else(|| StoreError::DataNotFound(name.to_owned()))?;
        let location = decode_binding(&binding)?;
        if let DataLocation::Dedicated(table_id) = location {
            transaction
                .open_table(Some(&dedicated_table_name(table_id)))
                .map_err(|error| StoreError::storage("open dedicated data table", error))?;
        }
        Ok(self.handle(location))
    }

    /// Ends data object setup and yields the unique runtime transaction capability.
    #[must_use]
    pub fn into_transactions(self) -> Transactions {
        Transactions {
            database: self.database,
            store_token: self.token,
        }
    }

    const fn handle(&self, location: DataLocation) -> DataHandle {
        DataHandle {
            store_token: self.token,
            location,
        }
    }
}

impl DataPlacement {
    const fn size_name(self) -> &'static str {
        match self {
            Self::Shared => "small",
            Self::Dedicated => "large",
        }
    }
}

fn open_database(path: &Path) -> Result<Database<NoWriteMap>, StoreError> {
    Database::<NoWriteMap>::open_with_options(
        path,
        DatabaseOptions {
            permissions: Some(0o600),
            max_tables: Some(u64::from(MAX_DEDICATED_TABLES)),
            exclusive: true,
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .map_err(|error| StoreError::storage("open MDBX environment", error))
}

fn validate_store_path(path: &Path) -> Result<(), StoreError> {
    let directory = metadata(path, path, "inspect store directory")?;
    if !directory.is_dir() {
        return Err(StoreError::StoreNotFound(path.to_path_buf()));
    }

    let data_file = metadata(&path.join(MDBX_DATA_FILE), path, "inspect store data file")?;
    if !data_file.is_file() {
        return Err(StoreError::StoreNotFound(path.to_path_buf()));
    }
    Ok(())
}

fn metadata(
    target: &Path,
    store_path: &Path,
    operation: &'static str,
) -> Result<fs::Metadata, StoreError> {
    fs::metadata(target).map_err(|error| {
        if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) {
            StoreError::StoreNotFound(store_path.to_path_buf())
        } else {
            StoreError::storage(operation, error)
        }
    })
}

fn validate_catalog(
    transaction: &libmdbx::Transaction<'_, libmdbx::RO, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    next_id: u32,
    next_dedicated: u32,
) -> Result<(), StoreError> {
    let mut cursor = transaction
        .cursor(table)
        .map_err(|error| StoreError::storage("open data catalog cursor", error))?;
    let mut shared_ids = HashSet::new();
    let mut dedicated_ids = HashSet::new();
    let mut entry = cursor
        .set_range::<Vec<u8>, Vec<u8>>(&[CATALOG_DOMAIN])
        .map_err(|error| StoreError::storage("seek data catalog", error))?;

    while let Some((key, binding)) = entry {
        let [domain, name @ ..] = key.as_slice() else {
            return Err(StoreError::InvalidStore);
        };
        if *domain != CATALOG_DOMAIN {
            break;
        }
        let name = std::str::from_utf8(name).map_err(|_| StoreError::InvalidStore)?;
        validate_name(name).map_err(|_| StoreError::InvalidStore)?;

        let unique_and_allocated = match decode_binding(&binding)? {
            DataLocation::Shared(id) => id < next_id && shared_ids.insert(id),
            DataLocation::Dedicated(id) => id < next_dedicated && dedicated_ids.insert(id),
        };
        if !unique_and_allocated {
            return Err(StoreError::InvalidStore);
        }

        entry = cursor
            .next::<Vec<u8>, Vec<u8>>()
            .map_err(|error| StoreError::storage("scan data catalog", error))?;
    }

    let shared_count = u32::try_from(shared_ids.len()).map_err(|_| StoreError::InvalidStore)?;
    let dedicated_count =
        u32::try_from(dedicated_ids.len()).map_err(|_| StoreError::InvalidStore)?;
    if shared_count != next_id || dedicated_count != next_dedicated {
        return Err(StoreError::InvalidStore);
    }
    drop(cursor);

    for id in dedicated_ids {
        transaction
            .open_table(Some(&dedicated_table_name(id)))
            .map_err(|_| StoreError::InvalidStore)?;
    }
    Ok(())
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
    let mut key = vec![CATALOG_DOMAIN];
    key.extend_from_slice(name.as_bytes());
    key
}

fn decode_u32(bytes: &[u8]) -> Result<u32, StoreError> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| StoreError::InvalidStore)
}

fn read_counter(
    transaction: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    key: &[u8],
) -> Result<u32, StoreError> {
    let bytes = transaction
        .get::<Vec<u8>>(table, key)
        .map_err(|error| StoreError::storage("read store counter", error))?
        .ok_or(StoreError::InvalidStore)?;
    decode_u32(&bytes)
}

fn encode_binding(location: DataLocation) -> [u8; 5] {
    let (placement, id) = match location {
        DataLocation::Shared(id) => (SHARED_PLACEMENT, id),
        DataLocation::Dedicated(id) => (DEDICATED_PLACEMENT, id),
    };
    let [a, b, c, d] = id.to_be_bytes();
    [placement, a, b, c, d]
}

fn decode_binding(bytes: &[u8]) -> Result<DataLocation, StoreError> {
    let [placement, a, b, c, d] = bytes else {
        return Err(StoreError::InvalidStore);
    };
    let id = u32::from_be_bytes([*a, *b, *c, *d]);
    match *placement {
        SHARED_PLACEMENT => Ok(DataLocation::Shared(id)),
        DEDICATED_PLACEMENT if id < MAX_DEDICATED_TABLES => Ok(DataLocation::Dedicated(id)),
        _ => Err(StoreError::InvalidStore),
    }
}
