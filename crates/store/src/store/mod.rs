use std::{cell::Cell as PoisonFlag, marker::PhantomData, rc::Rc};

use libmdbx::{Database, NoWriteMap, RW, Transaction as MdbxTransaction};

mod data;
mod database;
mod transaction;

pub(crate) use data::DataAccess;
pub use data::{ScanBatch, ScanDirection, ScanLimit};

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
pub struct Store {
    database: Database<NoWriteMap>,
    token: u64,
}

/// Grants the sole runtime capability to begin store transactions.
///
/// This value is obtained by consuming [`Store`]. It does not expose the
/// catalog or allow data objects to be created or opened.
pub struct Transactions {
    database: Database<NoWriteMap>,
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
#[must_use = "dropping a transaction rolls back its changes"]
pub struct Transaction<'database> {
    mdbx: MdbxTransaction<'database, RW, NoWriteMap>,
    store_token: u64,
    poisoned: PoisonFlag<bool>,
    _thread_bound: PhantomData<Rc<()>>,
}

fn dedicated_table_name(table_id: u32) -> String {
    format!("d/{table_id:08x}")
}
