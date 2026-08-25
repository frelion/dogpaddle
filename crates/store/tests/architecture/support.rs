use std::path::{Path, PathBuf};

use dogpaddle_store::{OrderedMap, Store, StoreData, StoreError};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};
use tempfile::TempDir;

pub type ByteMap<SIZE> = OrderedMap<Vec<u8>, Vec<u8>, SIZE>;

pub fn create_byte_map<SIZE>(store: &mut Store, name: &str) -> Result<ByteMap<SIZE>, StoreError>
where
    ByteMap<SIZE>: StoreData,
{
    store.create_data(name)
}

pub fn open_byte_map<SIZE>(store: &Store, name: &str) -> Result<ByteMap<SIZE>, StoreError>
where
    ByteMap<SIZE>: StoreData,
{
    store.open_data(name)
}

pub fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("store")
}

pub fn raw_database(path: &Path) -> Database<NoWriteMap> {
    Database::<NoWriteMap>::open_with_options(
        path,
        DatabaseOptions {
            permissions: Some(0o600),
            max_tables: Some(u64::from(Store::LARGE_DATA_CAPACITY)),
            exclusive: true,
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap()
}
