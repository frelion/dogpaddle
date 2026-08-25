use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    CodecError, DataAccess, DataHandle, ScanBatch, ScanDirection, ScanLimit, StoreError, StoreKey,
    StoreValue, TransactionAccess,
};

type MapTypes<K, V, SIZE> = fn() -> (K, V, SIZE);

/// A named persistent ordered map with typed keys and values.
///
/// `SIZE` explicitly selects shared [`crate::Small`] or dedicated
/// [`crate::Large`] physical storage.
pub struct OrderedMap<K, V, SIZE> {
    data: DataHandle,
    _types: PhantomData<MapTypes<K, V, SIZE>>,
}

/// Transaction-bound access to an [`OrderedMap`].
pub struct OrderedMapAccess<'transaction, K, V> {
    data: DataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

impl<K: StoreKey, V: StoreValue, SIZE> OrderedMap<K, V, SIZE> {
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
}

impl<K: StoreKey, V: StoreValue> OrderedMapAccess<'_, K, V> {
    /// Reads one value.
    ///
    /// # Errors
    ///
    /// Returns an error when key encoding, storage access, or value decoding fails.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let encoded_key = self
            .data
            .poison_on_error(key.encode_key())
            .map_err(StoreError::from)?;
        let encoded = self.data.get(encoded_key.as_ref())?;
        self.data
            .poison_on_error(encoded.map(V::decode_value).transpose())
            .map_err(StoreError::from)
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

    /// Scans an ordered key range after an optional exclusive continuation.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding, storage access, or decoding fails.
    pub fn scan<R: RangeBounds<K>>(
        &self,
        range: R,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<ScanBatch<K, V>, StoreError> {
        let lower = self
            .data
            .poison_on_error(match range.start_bound() {
                Bound::Included(key) => key.encode_key().map(Bound::Included),
                Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
                Bound::Unbounded => Ok(Bound::Unbounded),
            })
            .map_err(StoreError::from)?;
        let upper = self
            .data
            .poison_on_error(match range.end_bound() {
                Bound::Included(key) => key.encode_key().map(Bound::Included),
                Bound::Excluded(key) => key.encode_key().map(Bound::Excluded),
                Bound::Unbounded => Ok(Bound::Unbounded),
            })
            .map_err(StoreError::from)?;
        let continuation = self
            .data
            .poison_on_error(resume_after.map(StoreKey::encode_key).transpose())
            .map_err(StoreError::from)?;
        let raw = self.data.scan(
            (borrow_bound(&lower), borrow_bound(&upper)),
            direction,
            continuation.as_ref().map(AsRef::as_ref),
            limit,
        )?;
        self.data
            .poison_on_error(decode_batch(raw))
            .map_err(StoreError::from)
    }
}

impl<K, V, SIZE> Clone for OrderedMap<K, V, SIZE> {
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

fn decode_batch<K: StoreKey, V: StoreValue>(
    raw: ScanBatch<Vec<u8>, Vec<u8>>,
) -> Result<ScanBatch<K, V>, CodecError> {
    let items = raw
        .items
        .into_iter()
        .map(|(key, value)| Ok((K::decode_key(key)?, V::decode_value(value.into())?)))
        .collect::<Result<Vec<_>, CodecError>>()?;
    let continuation = raw.continuation.map(K::decode_key).transpose()?;
    Ok(ScanBatch {
        items,
        continuation,
    })
}
