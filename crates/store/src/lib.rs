#![doc = include_str!("../README.md")]

mod codec;
mod collections;
mod data_class;
mod error;
mod store;

pub use codec::{CodecError, StoreKey, StoreValue};
pub use collections::{
    Cell, CellAccess, CellReadAccess, MapPartition, MapReadPartition, MultiplicityChange,
    OrderedMap, OrderedMapAccess, OrderedMapPage, OrderedMapReadAccess, PartitionKey, Queue,
    QueueAccess, QueueReadAccess, checked_weight,
};
pub use data_class::StoreData;
pub use error::StoreError;
pub use store::{
    BatchedTransaction, DataScope, DurabilityBatch, ScanDirection, ScanLimit, Store, StoreSetup,
    Transaction, TransactionAccess, Transactions,
};
pub(crate) use store::{DataAccess, DataHandle, DataKind, ReadDataAccess};
pub use store::{ReadTransaction, ReadTransactionAccess, ReadTransactions};
