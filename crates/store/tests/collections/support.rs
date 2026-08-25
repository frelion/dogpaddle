use std::{borrow::Cow, path::PathBuf};

use dogpaddle_store::{CodecError, OrderedMap, Store, StoreData, StoreError, StoreKey, StoreValue};
use tempfile::TempDir;

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

pub fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("store")
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
