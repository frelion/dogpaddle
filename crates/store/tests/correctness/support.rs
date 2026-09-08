use std::{borrow::Cow, path::PathBuf};

use dogpaddle_store::{CodecError, OrderedMap, Store, StoreError, StoreKey, StoreValue};
use tempfile::TempDir;

pub type ByteMap = OrderedMap<Vec<u8>, Vec<u8>>;

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

pub fn create_byte_map(store: &mut Store, name: &str) -> Result<ByteMap, StoreError> {
    store.create_data(name)
}

pub fn open_byte_map(store: &Store, name: &str) -> Result<ByteMap, StoreError> {
    store.open_data(name)
}

pub fn create_map<K: StoreKey, V: StoreValue>(
    store: &mut Store,
    name: &str,
) -> Result<OrderedMap<K, V>, StoreError> {
    store.create_data(name)
}

pub fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("store")
}
