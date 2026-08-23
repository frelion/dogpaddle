use std::path::PathBuf;

use dogpaddle_store::{
    CodecError, DataPlacement, OrderedMap, Store, StoreError, StoreKey, StoreValue,
};
use tempfile::TempDir;

pub const PLACEMENTS: [DataPlacement; 2] = [DataPlacement::Shared, DataPlacement::Dedicated];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestValue(pub u64);

impl StoreValue for TestValue {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        self.0.encode_value()
    }

    fn decode_value(bytes: &[u8]) -> Result<Self, CodecError> {
        u64::decode_value(bytes).map(Self)
    }
}

pub fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("store")
}

pub fn create_map<K: StoreKey, V: StoreValue>(
    store: &mut Store,
    name: &str,
    placement: DataPlacement,
) -> Result<OrderedMap<K, V>, StoreError> {
    Ok(OrderedMap::new(store.create_data(name, placement)?))
}
