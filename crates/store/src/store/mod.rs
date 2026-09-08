use std::{
    cell::Cell as PoisonFlag, collections::BTreeMap, marker::PhantomData, rc::Rc, sync::Arc,
};

use rocksdb::{
    OptimisticTransactionDB as Database, SnapshotWithThreadMode, Transaction as RocksTransaction,
};

mod data;
mod database;
mod transaction;

pub(crate) use data::{DataAccess, ReadDataAccess, TransactionRef};
pub use data::{ScanDirection, ScanLimit};

/// Persistent kind of one typed data namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataKind {
    Cell,
    OrderedMap,
    OrderedMultiset,
    PartitionedMultiset,
    Queue,
    SubscribedLog,
}

/// Locates one data object in a particular [`Store`].
#[derive(Clone)]
pub struct DataHandle {
    store_token: u64,
    data_id: u32,
}

/// Owns one durable store during named data object setup.
///
/// Setup code may create or open data objects and borrow a short-lived
/// read-only snapshot with [`Store::read_transaction`]. Entering runtime still
/// consumes this value with [`Store::into_transactions`].
pub struct Store {
    database: Database,
    token: u64,
    catalog: BTreeMap<String, (u32, DataKind)>,
    next_data_id: u64,
}

/// Uniquely owns the runtime capability to begin Store write transactions.
///
/// This value is obtained by consuming [`Store`]. It does not expose the
/// catalog or allow data objects to be created or opened. The capability is
/// intentionally not cloneable, so one runtime coordinator remains the sole
/// owner of transaction boundaries for this Store. It can be moved between
/// threads while idle. Its owner may consume it with [`Transactions::split`]
/// to derive read-only capabilities; a borrower cannot perform that split.
/// Those read-only capabilities may keep the same Store open after this value
/// is dropped.
///
/// ```compile_fail
/// fn require_clone<T: Clone>() {}
/// require_clone::<dogpaddle_store::Transactions>();
/// ```
///
/// ```no_run
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::Transactions>();
/// ```
pub struct Transactions {
    database: Arc<Database>,
    store_token: u64,
}

/// A shareable runtime capability for beginning read-only Store transactions.
///
/// This capability is created by consuming [`Transactions`] with
/// [`Transactions::split`] and shares the same database without carrying
/// write authority. Shared references may begin independent snapshots,
/// including while the unique write capability remains alive. The value is
/// intentionally not cloneable, so a borrower cannot retain transaction-start
/// authority. It does not expose the Store catalog or allow data objects to be
/// created or opened.
///
/// ```compile_fail
/// fn require_clone<T: Clone>() {}
/// require_clone::<dogpaddle_store::ReadTransactions>();
/// ```
///
/// ```no_run
/// fn require_send<T: Send>() {}
/// fn require_sync<T: Sync>() {}
/// require_send::<dogpaddle_store::ReadTransactions>();
/// require_sync::<dogpaddle_store::ReadTransactions>();
/// ```
pub struct ReadTransactions {
    database: Arc<Database>,
    store_token: u64,
}

/// Owns one atomic store transaction.
///
/// Dropping this value without calling [`Transaction::commit`] rolls back all
/// its changes. A transaction is intentionally neither `Send` nor `Sync`.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::Transaction<'static>>();
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<dogpaddle_store::Transaction<'static>>();
/// ```
#[must_use = "dropping a transaction rolls back its changes"]
pub struct Transaction<'database> {
    inner: RocksTransaction<'database, Database>,
    store_token: u64,
    poisoned: PoisonFlag<bool>,
    _thread_bound: PhantomData<Rc<()>>,
}

/// Owns one read-only Store snapshot.
///
/// The snapshot carries no commit authority and is released when dropped. It
/// is intentionally neither [`Send`] nor [`Sync`], so transaction-bound values
/// cannot cross threads even though [`ReadTransactions`] itself can be shared.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::ReadTransaction<'static>>();
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<dogpaddle_store::ReadTransaction<'static>>();
/// ```
///
/// A read transaction has no commit authority.
///
/// ```compile_fail
/// fn commit(transaction: dogpaddle_store::ReadTransaction<'_>) {
///     transaction.commit().unwrap();
/// }
/// ```
#[must_use = "dropping a read transaction releases its snapshot"]
pub struct ReadTransaction<'database> {
    snapshot: SnapshotWithThreadMode<'database, Database>,
    store_token: u64,
    poisoned: PoisonFlag<bool>,
    _thread_bound: PhantomData<Rc<()>>,
}

/// Borrows one active transaction only for typed data access.
///
/// This capability binds typed collection handles to the transaction, but
/// cannot begin or commit a transaction or access the Store catalog. Copying
/// it only copies a shared borrow; transaction ownership and commit authority
/// remain unique.
///
/// ```compile_fail
/// fn commit(access: dogpaddle_store::TransactionAccess<'_>) {
///     access.commit();
/// }
/// ```
///
/// Like its transaction owner, this capability is thread-bound.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::TransactionAccess<'static>>();
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<dogpaddle_store::TransactionAccess<'static>>();
/// ```
///
/// The capability cannot outlive its transaction or keep being used after the
/// transaction owner commits.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, Transaction};
///
/// fn commit_before_later_access(
///     transaction: Transaction<'_>,
///     cell: &Cell<u64>,
/// ) {
///     let access = transaction.access();
///     transaction.commit().unwrap();
///     cell.access(access).unwrap();
/// }
/// ```
#[derive(Clone, Copy)]
pub struct TransactionAccess<'transaction> {
    transaction: &'transaction Transaction<'transaction>,
}

/// Borrows one active read-only transaction for typed collection reads.
///
/// This capability can only be passed to collection `read` methods. It cannot
/// bind a writable collection access, begin or commit a transaction, or access
/// the Store catalog.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadTransactionAccess};
///
/// fn write(cell: &Cell<u64>, access: ReadTransactionAccess<'_>) {
///     cell.access(access).unwrap();
/// }
/// ```
///
/// Like its snapshot owner, this capability is thread-bound.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::ReadTransactionAccess<'static>>();
/// ```
#[derive(Clone, Copy)]
pub struct ReadTransactionAccess<'transaction> {
    transaction: &'transaction ReadTransaction<'transaction>,
}
