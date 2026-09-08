use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    CodecError, DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanDirection,
    ScanLimit, StoreError, StoreKey, StoreValue, TransactionAccess, TransactionRef,
};

/// A named persistent ordered map with typed keys and values.
pub struct OrderedMap<K, V> {
    data: DataHandle,
    _types: PhantomData<fn() -> (K, V)>,
}

/// Transaction-bound access to an [`OrderedMap`].
pub struct OrderedMapAccess<'transaction, K, V> {
    data: DataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

/// A read-only transaction-bound view of an [`OrderedMap`].
///
/// This view can originate from either an active [`crate::Transaction`] or
/// [`crate::ReadTransaction`]. It exposes point reads and scans, but no
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
    data: ReadDataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

/// One transaction-bound encoded entry in an ordered-map scan.
///
/// The entry can project only the encoded fields a caller needs or decode the
/// complete owned `(K, V)` pair. The transaction binding cannot escape the scan
/// callback.
///
/// ```compile_fail
/// use dogpaddle_store::{CodecError, OrderedMapEntry};
///
/// fn escape<'entry>(entry: OrderedMapEntry<'entry, u64, Vec<u8>>) -> &'entry [u8] {
///     entry
///         .project(|_key, value| Ok::<_, CodecError>(value))
///         .unwrap()
/// }
/// ```
///
/// The entry remains bound to its transaction and thread.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::OrderedMapEntry<'static, u64, u64>>();
/// ```
pub struct OrderedMapEntry<'entry, K, V> {
    encoded_key: Vec<u8>,
    encoded_value: Vec<u8>,
    transaction: TransactionRef<'entry>,
    _types: PhantomData<fn() -> (K, V)>,
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
        read_map_value(self.data.as_read(), key)
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

    /// Visits one bounded page in an ordered key range.
    ///
    /// `resume_after` is the last key visited by a previous page and is always
    /// excluded. The returned key is the current page's last visited key, and
    /// is present only when another matching entry exists; pass it back as the
    /// next page's `resume_after`.
    ///
    /// The complete page and its continuation are admitted before the first
    /// callback. Callbacks may therefore update other Store data in the same
    /// transaction without interleaving business code with a storage cursor.
    /// Updates to this map do not change entries already admitted for the
    /// current page, but may affect later pages.
    ///
    /// Callbacks should keep non-store side effects out of the transaction: a
    /// later callback failure poisons and rolls back Store writes, but cannot
    /// undo external effects.
    ///
    /// # Errors
    ///
    /// Returns an error when bound encoding, storage access, continuation or
    /// entry decoding fails, the first matching entry exceeds the byte limit,
    /// or the visitor fails. A visitor error poisons the transaction. If the
    /// visitor swallows an entry decoding error, the scan stops with
    /// [`StoreError::TransactionPoisoned`].
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
        scan_map(
            self.data.as_read(),
            range,
            direction,
            resume_after,
            limit,
            visit,
        )
    }
}

impl<K: StoreKey, V: StoreValue> OrderedMapReadAccess<'_, K, V> {
    /// Reads one value visible to the originating transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding, storage access, or value decoding fails.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        read_map_value(&self.data, key)
    }

    /// Visits one bounded page through this read-only view.
    ///
    /// Range, continuation, admission, and callback semantics match
    /// [`OrderedMapAccess::scan`].
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
        scan_map(&self.data, range, direction, resume_after, limit, visit)
    }
}

fn read_map_value<K: StoreKey, V: StoreValue>(
    data: &ReadDataAccess<'_>,
    key: &K,
) -> Result<Option<V>, StoreError> {
    let encoded_key = data
        .poison_on_error(key.encode_key())
        .map_err(StoreError::from)?;
    let encoded = data.get(encoded_key.as_ref())?;
    data.poison_on_error(
        encoded
            .map(|encoded| V::decode_value(Cow::Owned(encoded)))
            .transpose(),
    )
    .map_err(StoreError::from)
}

fn scan_map<K: StoreKey, V: StoreValue, E>(
    data: &ReadDataAccess<'_>,
    range: impl RangeBounds<K>,
    direction: ScanDirection,
    resume_after: Option<&K>,
    limit: ScanLimit,
    mut visit: impl for<'entry> FnMut(OrderedMapEntry<'entry, K, V>) -> Result<(), E>,
) -> Result<Option<K>, E>
where
    E: From<StoreError>,
{
    let lower = data
        .poison_on_error(match range.start_bound() {
            Bound::Included(key) => key.encode_key().map(Bound::Included),
            Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
            Bound::Unbounded => Ok(Bound::Unbounded),
        })
        .map_err(StoreError::from)
        .map_err(E::from)?;
    let upper = data
        .poison_on_error(match range.end_bound() {
            Bound::Included(key) => key.encode_key().map(Bound::Included),
            Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
            Bound::Unbounded => Ok(Bound::Unbounded),
        })
        .map_err(StoreError::from)
        .map_err(E::from)?;
    let continuation = data
        .poison_on_error(resume_after.map(StoreKey::encode_key).transpose())
        .map_err(StoreError::from)
        .map_err(E::from)?;
    let raw = data
        .scan(
            (borrow_bound(&lower), borrow_bound(&upper)),
            direction,
            continuation.as_ref().map(AsRef::as_ref),
            limit,
        )
        .map_err(E::from)?;
    debug_assert!(!raw.limited || !raw.items.is_empty());
    let continuation = data
        .poison_on_error(
            raw.items
                .last()
                .filter(|_| raw.limited)
                .map(|(key, _)| K::decode_key(Cow::Owned(key.clone())))
                .transpose(),
        )
        .map_err(StoreError::from)
        .map_err(E::from)?;

    for (encoded_key, encoded_value) in raw.items {
        let entry = OrderedMapEntry {
            encoded_key,
            encoded_value,
            transaction: data.transaction_ref(),
            _types: PhantomData,
        };
        data.poison_on_error(visit(entry))?;
        data.ensure_healthy().map_err(E::from)?;
    }
    Ok(continuation)
}

impl<K, V> OrderedMapEntry<'_, K, V> {
    /// Decodes a caller-selected projection from the encoded logical key and value.
    ///
    /// `project` receives temporary slices, and its returned value cannot refer
    /// to either encoding.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Codec`] when the projection rejects the encoding
    /// and poisons the entry's transaction.
    pub fn project<R>(
        &self,
        project: impl for<'encoded> FnOnce(&'encoded [u8], &'encoded [u8]) -> Result<R, CodecError>,
    ) -> Result<R, StoreError> {
        self.transaction
            .poison_on_error(project(&self.encoded_key, &self.encoded_value))
            .map_err(StoreError::from)
    }
}

impl<K: StoreKey, V: StoreValue> OrderedMapEntry<'_, K, V> {
    /// Fully decodes this entry into an owned key/value pair.
    ///
    /// Consuming the entry lets owning codecs reuse its owned encoded buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when either encoding is invalid and poisons the entry's
    /// transaction.
    pub fn decode_owned(self) -> Result<(K, V), StoreError> {
        let Self {
            encoded_key,
            encoded_value,
            transaction,
            _types: _,
        } = self;
        let decoded: Result<(K, V), CodecError> = (|| {
            Ok((
                K::decode_key(Cow::Owned(encoded_key))?,
                V::decode_value(Cow::Owned(encoded_value))?,
            ))
        })();
        transaction
            .poison_on_error(decoded)
            .map_err(StoreError::from)
    }
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
