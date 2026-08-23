use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    CodecError, DataAccess, DataHandle, ScanBatch, ScanDirection, ScanLimit, StoreError, StoreKey,
    StoreValue, Transaction,
};

/// A typed ordered key/value map over one generic data namespace.
pub struct OrderedMap<K, V> {
    data: DataHandle,
    _types: PhantomData<fn() -> (K, V)>,
}

/// Transaction-bound access to an [`OrderedMap`].
pub struct OrderedMapAccess<'transaction, K, V> {
    data: DataAccess<'transaction>,
    _types: PhantomData<fn() -> (K, V)>,
}

impl<K: StoreKey, V: StoreValue> OrderedMap<K, V> {
    /// Wraps an existing generic data handle with ordered map behavior.
    #[must_use]
    pub fn new(data: DataHandle) -> Self {
        Self {
            data,
            _types: PhantomData,
        }
    }

    /// Binds this map to an active transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle belongs to another store or the
    /// transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        transaction: &'transaction Transaction<'transaction>,
    ) -> Result<OrderedMapAccess<'transaction, K, V>, StoreError> {
        Ok(OrderedMapAccess {
            data: self.data.access(transaction)?,
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

fn decode_batch<K: StoreKey, V: StoreValue>(
    raw: ScanBatch<Vec<u8>, Vec<u8>>,
) -> Result<ScanBatch<K, V>, CodecError> {
    let items = raw
        .items
        .into_iter()
        .map(|(key, value)| Ok((K::decode_key(key)?, V::decode_value(value)?)))
        .collect::<Result<Vec<_>, CodecError>>()?;
    let continuation = raw.continuation.map(K::decode_key).transpose()?;
    Ok(ScanBatch {
        items,
        continuation,
    })
}
