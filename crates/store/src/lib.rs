#![doc = include_str!("../README.md")]

mod codec;
mod collections;
mod data_class;
mod error;
mod store;

pub use codec::{CodecError, StoreKey, StoreValue};
pub use collections::{
    AppendLog, AppendLogAccess, AppendLogEntry, AppendLogScan, Cell, CellAccess, OrderedMap,
    OrderedMapAccess,
};
pub use data_class::StoreData;
pub use error::StoreError;
pub(crate) use store::{DataAccess, DataHandle, DataPlacement};
pub use store::{
    ScanBatch, ScanDirection, ScanLimit, Store, Transaction, TransactionAccess, Transactions,
};

/// Persistent size marker selecting shared physical storage for collections
/// that support a size choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Small;

/// Persistent size marker selecting dedicated physical storage for collections
/// that support a size choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Large;
