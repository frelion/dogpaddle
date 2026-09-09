#![doc = include_str!("../README.md")]

mod codec;
mod collections;
mod data_class;
mod error;
mod store;

pub use codec::{CodecError, StoreKey, StoreValue};
pub use collections::{
    Cell, CellAccess, CellReadAccess, MultiplicityChange, MultisetEntry, MultisetPartition,
    OrderedMap, OrderedMapAccess, OrderedMapPage, OrderedMapReadAccess, OrderedMultiset,
    OrderedMultisetAccess, OrderedMultisetReadAccess, PartitionedMultiset,
    PartitionedMultisetAccess, PartitionedMultisetReadAccess, Queue, QueueAccess,
    ReadMultisetPartition, SubscribedLog, SubscribedLogStatus, SubscribedLogWriter, Subscription,
    SubscriptionStatus,
};
pub use data_class::StoreData;
pub use error::StoreError;
pub(crate) use store::{DataAccess, DataHandle, DataKind, ReadDataAccess};
pub use store::{ReadTransaction, ReadTransactionAccess, ReadTransactions};
pub use store::{
    ScanDirection, ScanLimit, Store, StoreSetup, Transaction, TransactionAccess, Transactions,
};
