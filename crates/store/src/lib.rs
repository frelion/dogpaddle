#![doc = include_str!("../README.md")]

mod codec;
mod collections;
mod data_class;
mod error;
mod store;

pub use codec::{CodecError, StoreKey, StoreValue};
pub use collections::{
    AppendLog, AppendLogAccess, AppendLogEntry, AppendLogReadAccess, AppendLogScan, Cell,
    CellAccess, CellReadAccess, OrderedMap, OrderedMapAccess, OrderedMapEntry,
    OrderedMapReadAccess, ReadOnly,
};
pub use data_class::StoreData;
pub use error::StoreError;
pub(crate) use store::{DataAccess, DataHandle, DataPlacement, ReadDataAccess, TransactionRef};
pub use store::{ReadTransaction, ReadTransactionAccess, ReadTransactions};
pub use store::{ScanDirection, ScanLimit, Store, Transaction, TransactionAccess, Transactions};

/// Persistent size marker selecting shared physical storage for collections
/// that support a size choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Small;

/// Persistent size marker selecting dedicated physical storage for collections
/// that support a size choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Large;
