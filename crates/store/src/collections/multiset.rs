use std::{borrow::Cow, marker::PhantomData, num::NonZeroUsize, ops::Bound};

use crate::{
    DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanDirection, ScanLimit,
    StoreError, StoreKey, TransactionAccess,
};

/// A named persistent ordered set of keys with positive multiplicities.
///
/// An absent key has multiplicity zero. Stored multiplicities are always
/// positive; adjusting a key to zero removes it.
pub struct OrderedMultiset<K> {
    data: DataHandle,
    _key: PhantomData<fn() -> K>,
}

/// Transaction-bound access to an [`OrderedMultiset`].
pub struct OrderedMultisetAccess<'transaction, K> {
    data: DataAccess<'transaction>,
    _key: PhantomData<fn() -> K>,
}

/// Read-only transaction-bound access to an [`OrderedMultiset`].
pub struct OrderedMultisetReadAccess<'transaction, K> {
    data: ReadDataAccess<'transaction>,
    _key: PhantomData<fn() -> K>,
}

/// Multiplicity immediately before and after one successful adjustment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultiplicityChange {
    before: u64,
    after: u64,
}

/// One owned key and its positive multiplicity from an ordered scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultisetEntry<K> {
    /// Decoded logical key.
    pub key: K,
    /// Positive multiplicity stored for the key.
    pub multiplicity: u64,
}

impl MultiplicityChange {
    /// Returns the multiplicity visible before the adjustment.
    #[must_use]
    pub const fn before(self) -> u64 {
        self.before
    }

    /// Returns the multiplicity visible after the adjustment.
    #[must_use]
    pub const fn after(self) -> u64 {
        self.after
    }
}

impl<K: StoreKey> OrderedMultiset<K> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _key: PhantomData,
        }
    }

    /// Binds this multiset through an active write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<OrderedMultisetAccess<'transaction, K>, StoreError> {
        Ok(OrderedMultisetAccess {
            data: self.data.access(access)?,
            _key: PhantomData,
        })
    }

    /// Binds this multiset through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// transaction is already poisoned.
    pub fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<OrderedMultisetReadAccess<'transaction, K>, StoreError> {
        Ok(OrderedMultisetReadAccess {
            data: self.data.read(access)?,
            _key: PhantomData,
        })
    }
}

impl<K: StoreKey> OrderedMultisetAccess<'_, K> {
    /// Returns a key's current multiplicity, or zero when it is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage access fails, or persisted
    /// multiplicity bytes are invalid.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        read_multiplicity(self.data.as_read(), key)
    }

    /// Applies a signed difference and returns the before/after multiplicities.
    ///
    /// A zero difference is a read-only no-op. Underflow or overflow poisons
    /// the transaction, and no adjustment in a poisoned transaction can be
    /// committed.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage access fails, persisted bytes
    /// are invalid, or the resulting multiplicity is outside `0..=u64::MAX`.
    pub fn adjust(&mut self, key: &K, difference: i64) -> Result<MultiplicityChange, StoreError> {
        let encoded_key = self
            .data
            .poison_on_error(key.encode_key())
            .map_err(StoreError::from)?;
        adjust_encoded(&mut self.data, encoded_key.as_ref(), difference)
    }
}

impl<K: StoreKey> OrderedMultisetReadAccess<'_, K> {
    /// Returns a key's current multiplicity, or zero when it is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage access fails, or persisted
    /// multiplicity bytes are invalid.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        read_multiplicity(&self.data, key)
    }
}

fn read_multiplicity<K: StoreKey>(data: &ReadDataAccess<'_>, key: &K) -> Result<u64, StoreError> {
    let encoded_key = data
        .poison_on_error(key.encode_key())
        .map_err(StoreError::from)?;
    read_encoded_multiplicity(data, encoded_key.as_ref())
}

pub(super) fn read_encoded_multiplicity(
    data: &ReadDataAccess<'_>,
    encoded_key: &[u8],
) -> Result<u64, StoreError> {
    let Some(encoded) = data.get(encoded_key)? else {
        return Ok(0);
    };
    data.record_result(decode_multiplicity(encoded.as_ref()))
}

pub(super) fn adjust_encoded(
    data: &mut DataAccess<'_>,
    encoded_key: &[u8],
    difference: i64,
) -> Result<MultiplicityChange, StoreError> {
    let before = read_encoded_multiplicity(data.as_read(), encoded_key)?;
    if difference == 0 {
        return Ok(MultiplicityChange {
            before,
            after: before,
        });
    }
    let after = if difference > 0 {
        before
            .checked_add(difference.unsigned_abs())
            .ok_or(StoreError::MultiplicityOverflow)
    } else {
        before
            .checked_sub(difference.unsigned_abs())
            .ok_or(StoreError::MultiplicityUnderflow)
    };
    let after = data.poison_on_error(after)?;
    if after == 0 {
        data.delete(encoded_key)?;
    } else {
        data.put(encoded_key, &after.to_be_bytes())?;
    }
    Ok(MultiplicityChange { before, after })
}

pub(super) fn first_entry<K: StoreKey>(
    data: &ReadDataAccess<'_>,
    key_prefix: &[u8],
) -> Result<Option<MultisetEntry<K>>, StoreError> {
    Ok(scan_entries(
        data,
        key_prefix,
        ScanDirection::Ascending,
        NonZeroUsize::MIN,
    )?
    .pop())
}

pub(super) fn last_entry<K: StoreKey>(
    data: &ReadDataAccess<'_>,
    key_prefix: &[u8],
) -> Result<Option<MultisetEntry<K>>, StoreError> {
    Ok(scan_entries(
        data,
        key_prefix,
        ScanDirection::Descending,
        NonZeroUsize::MIN,
    )?
    .pop())
}

pub(super) fn scan_entries<K: StoreKey>(
    data: &ReadDataAccess<'_>,
    key_prefix: &[u8],
    direction: ScanDirection,
    max_items: NonZeroUsize,
) -> Result<Vec<MultisetEntry<K>>, StoreError> {
    let upper = prefix_successor(key_prefix);
    let upper_bound = upper.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    let raw = data.scan(
        (Bound::Included(key_prefix), upper_bound),
        direction,
        None,
        ScanLimit::new(max_items.get(), usize::MAX)
            .expect("a non-zero item count and usize::MAX are valid scan limits"),
    )?;
    let decoded = raw
        .items
        .into_iter()
        .map(|(key, value)| {
            let key = key
                .strip_prefix(key_prefix)
                .expect("the encoded scan range admits only this key prefix");
            let key = K::decode_key(Cow::Owned(key.to_vec())).map_err(StoreError::from)?;
            let multiplicity = decode_multiplicity(&value)?;
            Ok(MultisetEntry { key, multiplicity })
        })
        .collect::<Result<Vec<_>, StoreError>>();
    data.poison_on_error(decoded)
}

fn decode_multiplicity(encoded: &[u8]) -> Result<u64, StoreError> {
    let bytes: [u8; 8] = encoded
        .try_into()
        .map_err(|_| StoreError::CorruptMultiset {
            reason: "multiplicity is not an eight-byte unsigned integer",
        })?;
    let multiplicity = u64::from_be_bytes(bytes);
    if multiplicity == 0 {
        Err(StoreError::CorruptMultiset {
            reason: "a stored multiplicity is zero",
        })
    } else {
        Ok(multiplicity)
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

impl<K> Clone for OrderedMultiset<K> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _key: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, Store};

    #[test]
    fn malformed_persisted_multiplicity_poisons_and_rolls_back_other_writes() {
        assert_corrupt_multiplicity(&[]);
        assert_corrupt_multiplicity(&0_u64.to_be_bytes());
    }

    fn assert_corrupt_multiplicity(encoded: &[u8]) {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let multiset = store
            .create_data::<OrderedMultiset<Vec<u8>>>("multiset")
            .unwrap();
        let marker = store.create_data::<Cell<u64>>("marker").unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        multiset
            .data
            .access(transaction.access())
            .unwrap()
            .put(b"key", encoded)
            .unwrap();
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        marker
            .access(transaction.access())
            .unwrap()
            .set(&1)
            .unwrap();
        assert!(matches!(
            multiset
                .access(transaction.access())
                .unwrap()
                .multiplicity(&b"key".to_vec()),
            Err(StoreError::CorruptMultiset { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));

        let transaction = transactions.begin();
        assert_eq!(
            marker.access(transaction.access()).unwrap().get().unwrap(),
            None
        );
    }
}
