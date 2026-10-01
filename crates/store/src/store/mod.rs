use std::{cell::Cell, collections::BTreeMap, marker::PhantomData, rc::Rc, sync::Arc};

use rocksdb::{
    OptimisticTransactionDB as Database, SnapshotWithThreadMode, Transaction as RocksTransaction,
};

mod data;
mod database;
mod transaction;

pub(crate) use data::{DataAccess, ReadDataAccess};
pub use data::{ScanDirection, ScanLimit};

/// Persistent kind of one typed data namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataKind {
    Cell,
    OrderedMap,
    Queue,
}

/// Locates one data object in a particular [`Store`].
#[derive(Clone)]
pub struct DataHandle {
    store_token: u64,
    data_id: u32,
}

/// Owns an existing durable store while binding its declared resources.
///
/// Binding code may open data objects and borrow a short-lived
/// read-only snapshot with [`Store::read_transaction`]. Entering runtime still
/// consumes this value with [`Store::into_transactions`].
///
/// New resources must be declared with [`StoreSetup`] before the database is
/// published. Existing catalogs cannot be extended.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, Store};
/// fn add_resource(store: &mut Store) {
///     store.create_data::<Cell<u64>>("new").unwrap();
/// }
/// ```
pub struct Store {
    database: Database,
    token: u64,
    catalog: BTreeMap<String, (u32, DataKind)>,
}

/// Owns an in-memory draft of a new Store's complete typed resource set.
///
/// Creating or dropping a draft does not touch the filesystem. The draft
/// allocates the final Store identity, catalog, namespace identifiers, and
/// typed handles. [`StoreSetup::commit`] consumes it, creates the Store at the
/// supplied path, and atomically publishes the marker, complete catalog, and
/// caller-provided initialization.
///
/// A failed commit consumes the draft. Once creation has started, failure may
/// leave an incomplete path which [`Store::open`] rejects.
///
/// Draft setup cannot inspect data or enter runtime without committing.
///
/// ```compile_fail
/// let setup = dogpaddle_store::StoreSetup::new();
/// setup.read_transaction();
/// ```
///
/// ```compile_fail
/// let setup = dogpaddle_store::StoreSetup::new();
/// setup.into_transactions();
/// ```
pub struct StoreSetup {
    token: u64,
    catalog: BTreeMap<String, (u32, DataKind)>,
}

/// A short-lived capability for declaring or looking up typed Store data.
///
/// A scope borrowed from [`StoreSetup`] strictly declares new names, while a
/// scope borrowed from [`Store`] strictly looks up existing names. It exposes
/// no transaction capability, raw namespace identifier, or way to switch
/// modes. Typed handles returned by [`DataScope::data`] do not borrow the scope.
pub struct DataScope<'owner> {
    mode: DataScopeMode<'owner>,
    prefix: Option<String>,
}

enum DataScopeMode<'owner> {
    Declare(&'owner mut StoreSetup),
    Existing(&'owner Store),
}

/// Uniquely owns the runtime capability to begin Store write transactions.
///
/// This value is obtained from [`StoreSetup::commit`] or by consuming an
/// existing [`Store`]. It does not expose the catalog or allow data objects
/// to be created or opened. The capability is
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

/// Groups independent transactions behind explicit durability barriers.
///
/// Each transaction remains atomic and becomes visible when it commits, but
/// several successful commits can share one durable flush. Callers must invoke
/// [`DurabilityBatch::sync`] before performing an external effect that relies
/// on those commits and [`DurabilityBatch::finish`] before returning control
/// to their caller.
#[must_use = "a durability batch must be finished before returning control"]
pub struct DurabilityBatch<'transactions> {
    database: &'transactions Database,
    store_token: u64,
    pending: bool,
}

/// Owns one atomic transaction inside a [`DurabilityBatch`].
///
/// A successful commit is visible immediately, while durability is established
/// by the enclosing batch's next barrier.
#[must_use = "dropping a batched transaction rolls back its changes"]
pub struct BatchedTransaction<'database> {
    transaction: Transaction<'database>,
    pending: &'database mut bool,
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
    poisoned: Cell<bool>,
    has_writes: Cell<bool>,
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
    poisoned: Cell<bool>,
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
