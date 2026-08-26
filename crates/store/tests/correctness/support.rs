use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use dogpaddle_store::{CodecError, OrderedMap, Store, StoreData, StoreError, StoreKey, StoreValue};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};
use tempfile::TempDir;

pub type ByteMap<SIZE> = OrderedMap<Vec<u8>, Vec<u8>, SIZE>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestValue(pub u64);

impl StoreValue for TestValue {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        self.0.encode_value()
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        u64::decode_value(bytes).map(Self)
    }
}

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

pub fn create_map<K: StoreKey, V: StoreValue, SIZE>(
    store: &mut Store,
    name: &str,
) -> Result<OrderedMap<K, V, SIZE>, StoreError>
where
    OrderedMap<K, V, SIZE>: StoreData,
{
    store.create_data(name)
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
