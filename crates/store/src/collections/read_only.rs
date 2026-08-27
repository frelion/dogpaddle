use std::ops::{Range, RangeBounds};

use super::{
    AppendLog, AppendLogAccess, AppendLogEntry, AppendLogScan, Cell, CellAccess, OrderedMap,
    OrderedMapAccess, OrderedMapEntry,
};
use crate::{ScanDirection, ScanLimit, StoreError, StoreKey, StoreValue, TransactionAccess};

/// An opaque, owned attenuation of a typed collection capability.
///
/// `ReadOnly<C>` consumes a collection handle or transaction access capability
/// and never exposes `C` again. Its collection-specific APIs bind the same
/// persistent object and return a read-only transaction access that forwards
/// only read operations. Cloning a `ReadOnly<C>` clones only the attenuated
/// capability.
///
/// This is a process-local capability, not a persistent data class. It does
/// not implement [`crate::StoreData`] and cannot be created or opened by
/// [`crate::Store`]. An assembler that needs to retain full authority must
/// clone the full collection handle explicitly before attenuating one clone.
///
/// Full aliases retained elsewhere are not revoked. Code that receives only a
/// `ReadOnly<C>` cannot recover the full handle or call write operations in
/// safe Rust.
///
/// A read-only cell access cannot set or clear its value.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly, TransactionAccess};
///
/// fn set(cell: &ReadOnly<Cell<u64>>, transaction: TransactionAccess<'_>) {
///     let mut access = cell.access(transaction).unwrap();
///     access.set(&1).unwrap();
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly, TransactionAccess};
///
/// fn clear(cell: &ReadOnly<Cell<u64>>, transaction: TransactionAccess<'_>) {
///     let mut access = cell.access(transaction).unwrap();
///     access.clear().unwrap();
/// }
/// ```
///
/// A read-only ordered-map access cannot insert or remove entries.
///
/// ```compile_fail
/// use dogpaddle_store::{OrderedMap, ReadOnly, Small, TransactionAccess};
///
/// fn put(
///     map: &ReadOnly<OrderedMap<u64, u64, Small>>,
///     transaction: TransactionAccess<'_>,
/// ) {
///     let mut access = map.access(transaction).unwrap();
///     access.put(&1, &2).unwrap();
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{OrderedMap, ReadOnly, Small, TransactionAccess};
///
/// fn remove(
///     map: &ReadOnly<OrderedMap<u64, u64, Small>>,
///     transaction: TransactionAccess<'_>,
/// ) {
///     let mut access = map.access(transaction).unwrap();
///     access.remove(&1).unwrap();
/// }
/// ```
///
/// A read-only append-log access cannot append or truncate entries.
///
/// ```compile_fail
/// use dogpaddle_store::{AppendLog, ReadOnly, TransactionAccess};
///
/// fn append(log: &ReadOnly<AppendLog<Vec<u8>>>, transaction: TransactionAccess<'_>) {
///     let mut access = log.access(transaction).unwrap();
///     access.append(&Vec::new()).unwrap();
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{AppendLog, ReadOnly, TransactionAccess};
///
/// fn append_batch(
///     log: &ReadOnly<AppendLog<Vec<u8>>>,
///     transaction: TransactionAccess<'_>,
/// ) {
///     let mut access = log.access(transaction).unwrap();
///     access.append_batch(&[Vec::new()]).unwrap();
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{AppendLog, AppendLogEntry, ReadOnly, TransactionAccess};
///
/// fn append_entry<'entry>(
///     log: &ReadOnly<AppendLog<Vec<u8>>>,
///     transaction: TransactionAccess<'_>,
///     entry: &AppendLogEntry<'entry, Vec<u8>>,
/// ) {
///     let mut access = log.access(transaction).unwrap();
///     access.append_entry(entry).unwrap();
/// }
/// ```
///
/// ```compile_fail
/// use std::num::NonZeroUsize;
/// use dogpaddle_store::{AppendLog, ReadOnly, TransactionAccess};
///
/// fn truncate(
///     log: &ReadOnly<AppendLog<Vec<u8>>>,
///     transaction: TransactionAccess<'_>,
/// ) {
///     let mut access = log.access(transaction).unwrap();
///     access
///         .truncate_before(1, NonZeroUsize::new(1).unwrap())
///         .unwrap();
/// }
/// ```
///
/// The wrapper neither dereferences to nor returns its inner collection.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly};
///
/// fn recover(cell: &ReadOnly<Cell<u64>>) -> &Cell<u64> {
///     cell
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly};
///
/// fn recover(cell: ReadOnly<Cell<u64>>) -> Cell<u64> {
///     cell.into_inner()
/// }
/// ```
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly};
///
/// fn recover(cell: &ReadOnly<Cell<u64>>) -> &Cell<u64> {
///     cell.as_ref()
/// }
/// ```
///
/// ```compile_fail
/// use std::borrow::Borrow;
/// use dogpaddle_store::{Cell, ReadOnly};
///
/// fn recover(cell: &ReadOnly<Cell<u64>>) -> &Cell<u64> {
///     cell.borrow()
/// }
/// ```
///
/// It is also not a Store data class.
///
/// ```compile_fail
/// use dogpaddle_store::{Cell, ReadOnly, StoreData};
///
/// fn require_store_data<D: StoreData>() {}
/// require_store_data::<ReadOnly<Cell<u64>>>();
/// ```
pub struct ReadOnly<C> {
    inner: C,
}

impl<C> ReadOnly<C> {
    /// Consumes `inner` and exposes it only through the read-only API.
    ///
    /// This attenuation does not revoke other full aliases. Clone the full
    /// collection explicitly before this call when an assembler must retain
    /// owner authority.
    #[must_use]
    pub const fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Clone> Clone for ReadOnly<C> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

impl<T: StoreValue> ReadOnly<Cell<T>> {
    /// Binds this cell as read-only through an active transaction capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<ReadOnly<CellAccess<'transaction, T>>, StoreError> {
        self.inner.access(access).map(ReadOnly::new)
    }
}

impl<T: StoreValue> ReadOnly<CellAccess<'_, T>> {
    /// Reads the current cell value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        self.inner.get()
    }
}

impl<K: StoreKey, V: StoreValue, SIZE> ReadOnly<OrderedMap<K, V, SIZE>> {
    /// Binds this map as read-only through an active transaction capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<ReadOnly<OrderedMapAccess<'transaction, K, V>>, StoreError> {
        self.inner.access(access).map(ReadOnly::new)
    }
}

impl<K: StoreKey, V: StoreValue> ReadOnly<OrderedMapAccess<'_, K, V>> {
    /// Reads one map value.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding, storage access, or value decoding fails.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        self.inner.get(key)
    }

    /// Visits one bounded page using [`OrderedMapAccess::scan`] semantics.
    ///
    /// # Errors
    ///
    /// Returns an error when bound encoding, storage access, continuation or
    /// entry decoding fails, the first matching entry exceeds the byte limit,
    /// or the visitor fails.
    pub fn scan<E>(
        &self,
        range: impl RangeBounds<K>,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
        visit: impl for<'entry> FnMut(OrderedMapEntry<'entry, K, V>) -> Result<(), E>,
    ) -> Result<Option<K>, E>
    where
        E: From<StoreError>,
    {
        self.inner
            .scan(range, direction, resume_after, limit, visit)
    }
}

impl<T: StoreValue> ReadOnly<AppendLog<T>> {
    /// Binds this append log as read-only through an active transaction capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<ReadOnly<AppendLogAccess<'transaction, T>>, StoreError> {
        self.inner.access(access).map(ReadOnly::new)
    }
}

impl<T: StoreValue> ReadOnly<AppendLogAccess<'_, T>> {
    /// Returns the retained offset range `[head, tail)`.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails or the log metadata is corrupt.
    pub fn bounds(&self) -> Result<Range<u64>, StoreError> {
        self.inner.bounds()
    }

    /// Visits one bounded batch using [`AppendLogAccess::scan`] semantics.
    ///
    /// # Errors
    ///
    /// Returns an error when `offset` is outside the retained range, the first
    /// entry exceeds the byte limit, storage access or the callback fails, or
    /// the persisted log is corrupt.
    pub fn scan<E>(
        &self,
        offset: u64,
        limit: ScanLimit,
        visit: impl for<'entry> FnMut(AppendLogEntry<'entry, T>) -> Result<(), E>,
    ) -> Result<AppendLogScan, E>
    where
        E: From<StoreError>,
    {
        self.inner.scan(offset, limit, visit)
    }
}
