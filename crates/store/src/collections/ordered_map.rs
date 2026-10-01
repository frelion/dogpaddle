use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanDirection, ScanLimit,
    StoreError, StoreKey, StoreValue, TransactionAccess,
};

/// A named persistent ordered map with typed keys and values.
pub struct OrderedMap<K, V> {
    data: DataHandle,
    _types: PhantomData<fn() -> (K, V)>,
}

/// Transaction-bound access to an [`OrderedMap`].
pub struct OrderedMapAccess<'transaction, K, V> {
    pub(super) data: DataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

/// A read-only transaction-bound view of an [`OrderedMap`].
///
/// This view borrows an active [`crate::ReadTransaction`]. It exposes point reads and scans, but no
/// insertion or removal API, and cannot outlive the originating transaction.
///
/// ```compile_fail
/// use dogpaddle_store::OrderedMapReadAccess;
///
/// fn put(access: &mut OrderedMapReadAccess<'_, u64, u64>) {
///     access.put(&1, &2).unwrap();
/// }
/// ```
pub struct OrderedMapReadAccess<'transaction, K, V> {
    pub(super) data: ReadDataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

/// One fully decoded, owned page from an ordered-map scan.
///
/// The page does not borrow the map or transaction. Both entries and the
/// continuation are decoded before the scan returns successfully. Admission precedes
/// decoding borrowed iterator bytes directly into these owned entries. A continuation
/// is reconstructed through the key codec only when another matching entry exists;
/// keys need not implement `Clone`.
#[derive(Debug, Eq, PartialEq)]
pub struct OrderedMapPage<K, V> {
    /// Entries in the requested key order.
    pub entries: Vec<(K, V)>,
    /// The last returned key, present only when another matching entry exists.
    /// Pass it as `resume_after` for the next page with the same range and direction.
    pub continuation: Option<K>,
}

impl<K: StoreKey, V: StoreValue> OrderedMap<K, V> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _types: PhantomData,
        }
    }

    /// Binds this map through an active transaction's access capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<OrderedMapAccess<'transaction, K, V>, StoreError> {
        Ok(OrderedMapAccess {
            data: self.data.access(access)?,
            _types: PhantomData,
        })
    }

    /// Binds this map through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// underlying read transaction is already poisoned.
    pub fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<OrderedMapReadAccess<'transaction, K, V>, StoreError> {
        Ok(OrderedMapReadAccess {
            data: self.data.read(access)?,
            _types: PhantomData,
        })
    }
}

impl<K: StoreKey, V: StoreValue> OrderedMapAccess<'_, K, V> {
    /// Reads one value.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding, storage access, or value decoding fails.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        self.get_bounded(key, usize::MAX)
    }

    /// Reads one value after admitting its encoded value length.
    ///
    /// The pinned value's length is checked before copying or decoding it.
    /// The limit excludes the caller-owned lookup key.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ItemTooLarge`] without poisoning when the value
    /// exceeds `max_bytes`. Encoding, storage and decoding failures poison
    /// the transaction.
    pub fn get_bounded(&self, key: &K, max_bytes: usize) -> Result<Option<V>, StoreError> {
        read_map_value_bounded(self.data.as_read(), key, max_bytes)
    }

    /// Inserts or replaces one value.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails.
    pub fn put(&mut self, key: &K, value: &V) -> Result<(), StoreError> {
        let encoded_key = self
            .data
            .poison_on_error(key.encode_key())
            .map_err(StoreError::from)?;
        let encoded_value = self
            .data
            .poison_on_error(value.encode_value())
            .map_err(StoreError::from)?;
        self.data.put(encoded_key.as_ref(), encoded_value.as_ref())
    }

    /// Removes one key and reports whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding or storage access fails.
    pub fn remove(&mut self, key: &K) -> Result<bool, StoreError> {
        let encoded_key = self
            .data
            .poison_on_error(key.encode_key())
            .map_err(StoreError::from)?;
        self.data.delete(encoded_key.as_ref())
    }

    /// Unconditionally stages one key's deletion without first reading it.
    ///
    /// Use [`Self::remove`] when the caller needs to know whether the key
    /// existed. `erase` is intended for callers that already established the
    /// key's presence and would otherwise repeat the same point lookup.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding or storage access fails.
    pub fn erase(&mut self, key: &K) -> Result<(), StoreError> {
        let encoded_key = self
            .data
            .poison_on_error(key.encode_key())
            .map_err(StoreError::from)?;
        self.data.erase(encoded_key.as_ref())
    }

    /// Returns one fully decoded page in an ordered key range.
    ///
    /// `resume_after` excludes the last key of the preceding page. The limit
    /// bounds the entry count and logical encoded key-plus-value bytes.
    /// The owned page remains unchanged by later writes; subsequent scans see
    /// those writes when using the same write transaction.
    ///
    /// # Errors
    ///
    /// Encoding, decoding and storage failures poison the transaction. No
    /// partial page is returned. If the first matching entry exceeds the byte
    /// limit, returns [`StoreError::ItemTooLarge`] without poisoning, allowing
    /// another scan with a larger limit.
    pub fn scan(
        &self,
        range: impl RangeBounds<K>,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<OrderedMapPage<K, V>, StoreError> {
        scan_map(self.data.as_read(), range, direction, resume_after, limit)
    }
}

impl<K: StoreKey, V: StoreValue> OrderedMapReadAccess<'_, K, V> {
    /// Returns one value visible to this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding, storage access or decoding fails.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        self.get_bounded(key, usize::MAX)
    }

    /// Reads one value after admitting its encoded value length.
    ///
    /// The pinned value's length is checked before copying or decoding it.
    /// The limit excludes the caller-owned lookup key.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ItemTooLarge`] without poisoning when the value
    /// exceeds `max_bytes`. Encoding, storage and decoding failures poison
    /// the transaction.
    pub fn get_bounded(&self, key: &K, max_bytes: usize) -> Result<Option<V>, StoreError> {
        read_map_value_bounded(&self.data, key, max_bytes)
    }

    /// Returns one fully decoded, owned page visible to this snapshot.
    ///
    /// Range, continuation and admission semantics match [`OrderedMapAccess::scan`].
    ///
    /// # Errors
    ///
    /// Encoding, decoding and storage failures poison the snapshot. A first
    /// entry exceeding the byte limit returns [`StoreError::ItemTooLarge`]
    /// without poisoning it. No partial page is returned.
    pub fn scan(
        &self,
        range: impl RangeBounds<K>,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<OrderedMapPage<K, V>, StoreError> {
        scan_map(&self.data, range, direction, resume_after, limit)
    }
}

fn read_map_value_bounded<K: StoreKey, V: StoreValue>(
    data: &ReadDataAccess<'_>,
    key: &K,
    max_bytes: usize,
) -> Result<Option<V>, StoreError> {
    let encoded_key = data.poison_on_error(key.encode_key())?;
    data.get_bounded(encoded_key.as_ref(), max_bytes)
}

fn scan_map<K: StoreKey, V: StoreValue>(
    data: &ReadDataAccess<'_>,
    range: impl RangeBounds<K>,
    direction: ScanDirection,
    resume_after: Option<&K>,
    limit: ScanLimit,
) -> Result<OrderedMapPage<K, V>, StoreError> {
    let lower = data.poison_on_error(match range.start_bound() {
        Bound::Included(key) => key.encode_key().map(Bound::Included),
        Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
        Bound::Unbounded => Ok(Bound::Unbounded),
    })?;
    let upper = data.poison_on_error(match range.end_bound() {
        Bound::Included(key) => key.encode_key().map(Bound::Included),
        Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
        Bound::Unbounded => Ok(Bound::Unbounded),
    })?;
    let resume = data.poison_on_error(resume_after.map(StoreKey::encode_key).transpose())?;
    data.scan(
        (borrow_bound(&lower), borrow_bound(&upper)),
        direction,
        resume.as_ref().map(AsRef::as_ref),
        limit,
    )
}

impl<K, V> Clone for OrderedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _types: PhantomData,
        }
    }
}

fn borrow_bound<T: AsRef<[u8]>>(bound: &Bound<T>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(key) => Bound::Included(key.as_ref()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
