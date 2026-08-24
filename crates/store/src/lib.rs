#![doc = include_str!("../README.md")]

mod codec;
mod collections;
mod error;
mod store;

pub use codec::{CodecError, StoreKey, StoreValue};
pub use collections::{Cell, CellAccess, OrderedMap, OrderedMapAccess};
pub use error::StoreError;
pub use store::{
    DataAccess, DataHandle, DataPlacement, ScanBatch, ScanDirection, ScanLimit, Store, Transaction,
    Transactions,
};
