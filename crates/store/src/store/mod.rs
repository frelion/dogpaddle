use std::{cell::Cell as PoisonFlag, marker::PhantomData, rc::Rc, sync::Arc};

use libmdbx::{Database, NoWriteMap, RO, RW, Transaction as MdbxTransaction};

mod data;
mod database;
mod transaction;

pub(crate) use data::{DataAccess, ReadDataAccess, TransactionRef};
pub use data::{ScanDirection, ScanLimit};

// These two types are nominally `pub` so the sealed StoreData supertrait can
// mention them. This module is private and the crate root reexports them only
// as `pub(crate)`, so neither type is part of the external API.
/// Physical placement of one logical data namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataPlacement {
    /// Share the main B+Tree with other small data namespaces.
    Shared,
    /// Own a dedicated MDBX named table for large data.
    Dedicated,
}

#[derive(Clone, Copy)]
enum DataLocation {
    Shared(u32),
    Dedicated(u32),
}

/// Locates one data object in a particular [`Store`].
#[derive(Clone)]
pub struct DataHandle {
    store_token: u64,
    location: DataLocation,
}

/// Owns one durable store during named data object setup.
///
/// Setup code may create or open data objects and borrow a short-lived
/// read-only snapshot with [`Store::read_transaction`]. Entering runtime still
/// consumes this value with [`Store::into_transactions`].
pub struct Store {
    database: Database<NoWriteMap>,
    token: u64,
}

/// Uniquely owns the runtime capability to begin Store write transactions.
///
/// This value is obtained by consuming [`Store`]. It does not expose the
/// catalog or allow data objects to be created or opened. The capability is
/// intentionally not cloneable, so one runtime coordinator remains the sole
/// owner of transaction boundaries for this Store. It can be moved between
/// threads while idle. Its owner may consume it with [`Transactions::split`]
/// to derive read-only capabilities; a borrower cannot perform that split.
/// Those read-only capabilities may keep the same Store environment open after
/// this value is dropped.
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
    database: Arc<Database<NoWriteMap>>,
    store_token: u64,
}

/// A shareable runtime capability for beginning read-only Store transactions.
///
/// This capability is created by consuming [`Transactions`] with
/// [`Transactions::split`] and shares the same MDBX environment without
/// carrying write authority. Shared references may begin independent
/// snapshots, including while the unique write capability remains alive. The
/// value is intentionally not cloneable, so a borrower cannot retain
/// transaction-start authority. It does not expose the Store catalog or allow
/// data objects to be created or opened.
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
    database: Arc<Database<NoWriteMap>>,
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
    mdbx: MdbxTransaction<'database, RW, NoWriteMap>,
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
    mdbx: MdbxTransaction<'database, RO, NoWriteMap>,
    store_token: u64,
    poisoned: PoisonFlag<bool>,
    _thread_bound: PhantomData<Rc<()>>,
}

/// Borrows one active transaction only for typed data access.
///
/// This capability can bind existing full or [`crate::ReadOnly`] collection
/// handles to the transaction, but cannot begin or commit a transaction or
/// access the Store catalog. The collection handle determines which methods
/// the resulting access exposes. Copying this capability only copies a shared
/// borrow; transaction ownership and commit authority remain unique.
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

fn dedicated_table_name(table_id: u32) -> String {
    format!("d/{table_id:08x}")
}
