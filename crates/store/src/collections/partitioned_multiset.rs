use std::marker::PhantomData;

use crate::{
    CodecError, DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanDirection,
    ScanLimit, StoreError, StoreKey, TransactionAccess,
};

use super::multiset::{
    MultiplicityChange, MultisetEntry, MultisetPage, adjust_encoded, first_entry, last_entry,
    read_encoded_multiplicity, scan_entries,
};

/// A named persistent collection of independently ordered multisets.
pub struct PartitionedMultiset<P, K> {
    data: DataHandle,
    _types: PhantomData<fn() -> (P, K)>,
}

/// Transaction-bound access to a [`PartitionedMultiset`].
pub struct PartitionedMultisetAccess<'transaction, P, K> {
    data: DataAccess<'transaction>,
    _types: PhantomData<fn() -> (P, K)>,
}

/// Read-only transaction-bound access to a [`PartitionedMultiset`].
pub struct PartitionedMultisetReadAccess<'transaction, P, K> {
    data: ReadDataAccess<'transaction>,
    _types: PhantomData<fn() -> (P, K)>,
}

/// Mutable access to one typed partition.
pub struct MultisetPartition<'access, 'transaction, K> {
    data: &'access mut DataAccess<'transaction>,
    prefix: Vec<u8>,
    _key: PhantomData<fn() -> K>,
}

/// Read-only access to one typed partition.
pub struct ReadMultisetPartition<'access, 'transaction, K> {
    data: &'access ReadDataAccess<'transaction>,
    prefix: Vec<u8>,
    _key: PhantomData<fn() -> K>,
}

impl<P: StoreKey, K: StoreKey> PartitionedMultiset<P, K> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _types: PhantomData,
        }
    }

    /// Binds this collection through an active write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<PartitionedMultisetAccess<'transaction, P, K>, StoreError> {
        Ok(PartitionedMultisetAccess {
            data: self.data.access(access)?,
            _types: PhantomData,
        })
    }

    /// Binds this collection through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// transaction is already poisoned.
    pub fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<PartitionedMultisetReadAccess<'transaction, P, K>, StoreError> {
        Ok(PartitionedMultisetReadAccess {
            data: self.data.read(access)?,
            _types: PhantomData,
        })
    }
}

impl<'transaction, P: StoreKey, K: StoreKey> PartitionedMultisetAccess<'transaction, P, K> {
    /// Selects one partition for reads and checked adjustments.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition cannot be encoded.
    pub fn partition<'access>(
        &'access mut self,
        partition: &P,
    ) -> Result<MultisetPartition<'access, 'transaction, K>, StoreError> {
        let prefix = encode_partition(self.data.as_read(), partition)?;
        Ok(MultisetPartition {
            data: &mut self.data,
            prefix,
            _key: PhantomData,
        })
    }
}

impl<'transaction, P: StoreKey, K: StoreKey> PartitionedMultisetReadAccess<'transaction, P, K> {
    /// Selects one partition for reads.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition cannot be encoded.
    pub fn partition<'access>(
        &'access self,
        partition: &P,
    ) -> Result<ReadMultisetPartition<'access, 'transaction, K>, StoreError> {
        let prefix = encode_partition(&self.data, partition)?;
        Ok(ReadMultisetPartition {
            data: &self.data,
            prefix,
            _key: PhantomData,
        })
    }
}

impl<K: StoreKey> MultisetPartition<'_, '_, K> {
    /// Returns a key's current multiplicity, or zero when absent.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding, storage access, or decoding fails.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = encode_key(self.data.as_read(), &self.prefix, key)?;
        read_encoded_multiplicity(self.data.as_read(), &key)
    }

    /// Applies a signed difference and returns the before/after multiplicities.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage access fails, persisted bytes
    /// are invalid, or the resulting multiplicity is outside `0..=u64::MAX`.
    pub fn adjust(&mut self, key: &K, difference: i64) -> Result<MultiplicityChange, StoreError> {
        let key = encode_key(self.data.as_read(), &self.prefix, key)?;
        adjust_encoded(self.data, &key, difference)
    }

    /// Returns the smallest key and multiplicity in this partition.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or decoding fails.
    pub fn first(&self) -> Result<Option<MultisetEntry<K>>, StoreError> {
        first_entry(self.data.as_read(), &self.prefix)
    }

    /// Returns the largest key and multiplicity in this partition.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or decoding fails.
    pub fn last(&self) -> Result<Option<MultisetEntry<K>>, StoreError> {
        last_entry(self.data.as_read(), &self.prefix)
    }

    /// Returns one fully decoded, owned page from this partition.
    ///
    /// `resume_after` excludes the last key of the preceding page. The limit
    /// bounds the entry count and encoded partition framing, key, and
    /// multiplicity bytes.
    ///
    /// # Errors
    ///
    /// Encoding, decoding and storage failures poison the transaction. No
    /// partial page is returned. If the first matching entry exceeds the byte
    /// limit, returns [`StoreError::ItemTooLarge`] without poisoning, allowing
    /// another scan with a larger limit.
    pub fn scan(
        &self,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<MultisetPage<K>, StoreError> {
        scan_entries(
            self.data.as_read(),
            &self.prefix,
            direction,
            resume_after,
            limit,
        )
    }
}

impl<K: StoreKey> ReadMultisetPartition<'_, '_, K> {
    /// Returns a key's current multiplicity, or zero when absent.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding, storage access, or decoding fails.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = encode_key(self.data, &self.prefix, key)?;
        read_encoded_multiplicity(self.data, &key)
    }

    /// Returns the smallest key and multiplicity in this partition.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or decoding fails.
    pub fn first(&self) -> Result<Option<MultisetEntry<K>>, StoreError> {
        first_entry(self.data, &self.prefix)
    }

    /// Returns the largest key and multiplicity in this partition.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or decoding fails.
    pub fn last(&self) -> Result<Option<MultisetEntry<K>>, StoreError> {
        last_entry(self.data, &self.prefix)
    }

    /// Returns one fully decoded, owned page from this partition.
    ///
    /// Range, continuation and admission semantics match [`MultisetPartition::scan`].
    ///
    /// # Errors
    ///
    /// Encoding, decoding and storage failures poison the snapshot. A first
    /// entry exceeding the byte limit returns [`StoreError::ItemTooLarge`]
    /// without poisoning it. No partial page is returned.
    pub fn scan(
        &self,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<MultisetPage<K>, StoreError> {
        scan_entries(self.data, &self.prefix, direction, resume_after, limit)
    }
}

fn encode_partition<P: StoreKey>(
    data: &ReadDataAccess<'_>,
    partition: &P,
) -> Result<Vec<u8>, StoreError> {
    let encoded = data
        .poison_on_error(partition.encode_key())
        .map_err(StoreError::from)?;
    let length = data
        .poison_on_error(
            u64::try_from(encoded.as_ref().len())
                .map_err(|_| CodecError::new("partition key is too long")),
        )
        .map_err(StoreError::from)?;
    let mut prefix = Vec::with_capacity(8 + encoded.as_ref().len());
    prefix.extend_from_slice(&length.to_be_bytes());
    prefix.extend_from_slice(encoded.as_ref());
    Ok(prefix)
}

fn encode_key<K: StoreKey>(
    data: &ReadDataAccess<'_>,
    prefix: &[u8],
    key: &K,
) -> Result<Vec<u8>, StoreError> {
    let encoded = data
        .poison_on_error(key.encode_key())
        .map_err(StoreError::from)?;
    let mut framed = Vec::with_capacity(prefix.len() + encoded.as_ref().len());
    framed.extend_from_slice(prefix);
    framed.extend_from_slice(encoded.as_ref());
    Ok(framed)
}

impl<P, K> Clone for PartitionedMultiset<P, K> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _types: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, Store};

    #[test]
    fn corrupt_partition_multiplicity_poisons_and_rolls_back_other_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let multiset = store
            .create_data::<PartitionedMultiset<u64, u64>>("multiset")
            .unwrap();
        let marker = store.create_data::<Cell<u64>>("marker").unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        {
            let mut data = multiset.data.access(transaction.access()).unwrap();
            let prefix = encode_partition(data.as_read(), &7_u64).unwrap();
            let key = encode_key(data.as_read(), &prefix, &9_u64).unwrap();
            data.put(&key, &0_u64.to_be_bytes()).unwrap();
        }
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
                .partition(&7_u64)
                .unwrap()
                .multiplicity(&9_u64),
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

    #[test]
    fn corrupt_scan_returns_no_page_and_poisons_prior_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let multiset = store
            .create_data::<PartitionedMultiset<u64, u64>>("multiset")
            .unwrap();
        let marker = store.create_data::<Cell<u64>>("marker").unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        {
            let mut data = multiset.data.access(transaction.access()).unwrap();
            let prefix = encode_partition(data.as_read(), &7_u64).unwrap();
            let valid = encode_key(data.as_read(), &prefix, &1_u64).unwrap();
            data.put(&valid, &1_u64.to_be_bytes()).unwrap();
            let corrupt = encode_key(data.as_read(), &prefix, &2_u64).unwrap();
            data.put(&corrupt, &[1]).unwrap();
        }
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
                .partition(&7_u64)
                .unwrap()
                .scan(
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(2, 1024).unwrap(),
                ),
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
