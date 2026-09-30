use std::{borrow::Cow, marker::PhantomData, ops::Bound};

use crate::{
    CodecError, DataAccess, OrderedMapAccess, OrderedMapPage, OrderedMapReadAccess, ReadDataAccess,
    ScanDirection, ScanLimit, StoreError, StoreKey, StoreValue,
};

/// An ordered map key split into an independently ordered partition and local key.
///
/// Partition bytes escape zero as `00 ff` and terminate with `00 00`; the
/// local key follows unchanged. This preserves tuple ordering, including empty
/// and prefix-valued partitions, without a separate collection lifecycle.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionKey<P, K>(pub P, pub K);

impl<P: StoreKey, K: StoreKey> StoreKey for PartitionKey<P, K> {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = partition_prefix(&self.0)?;
        encoded.extend_from_slice(self.1.encode_key()?.as_ref());
        Ok(encoded)
    }
    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let bytes = bytes.as_ref();
        let mut partition = Vec::new();
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            index += 1;
            if byte != 0 {
                partition.push(byte);
                continue;
            }
            match bytes.get(index) {
                Some(255) => {
                    partition.push(0);
                    index += 1;
                }
                Some(0) => {
                    return Ok(Self(
                        P::decode_key(Cow::Owned(partition))?,
                        K::decode_key(Cow::Borrowed(&bytes[index + 1..]))?,
                    ));
                }
                _ => return Err(CodecError::new("invalid partition key framing")),
            }
        }
        Err(CodecError::new("partition key has no terminator"))
    }
}

/// Mutable transaction-bound view of one ordered-map partition.
pub struct MapPartition<'access, 'transaction, K, V> {
    pub(super) data: &'access mut DataAccess<'transaction>,
    pub(super) prefix: Vec<u8>,
    types: PhantomData<fn() -> (K, V)>,
}
/// Read-only transaction-bound view of one ordered-map partition.
pub struct MapReadPartition<'access, 'transaction, K, V> {
    pub(super) data: &'access ReadDataAccess<'transaction>,
    pub(super) prefix: Vec<u8>,
    types: PhantomData<fn() -> (K, V)>,
}

impl<'transaction, P: StoreKey, K: StoreKey, V: StoreValue>
    OrderedMapAccess<'transaction, PartitionKey<P, K>, V>
{
    /// Selects one partition without copying any stored entries.
    /// # Errors
    /// Returns a partition encoding error, poisoning the transaction.
    pub fn partition<'access>(
        &'access mut self,
        partition: &P,
    ) -> Result<MapPartition<'access, 'transaction, K, V>, StoreError> {
        let prefix = self
            .data
            .as_read()
            .poison_on_error(partition_prefix(partition))?;
        Ok(MapPartition {
            data: &mut self.data,
            prefix,
            types: PhantomData,
        })
    }
}
impl<'transaction, P: StoreKey, K: StoreKey, V: StoreValue>
    OrderedMapReadAccess<'transaction, PartitionKey<P, K>, V>
{
    /// Selects one partition visible to this snapshot.
    /// # Errors
    /// Returns a partition encoding error, poisoning the snapshot.
    pub fn partition<'access>(
        &'access self,
        partition: &P,
    ) -> Result<MapReadPartition<'access, 'transaction, K, V>, StoreError> {
        let prefix = self.data.poison_on_error(partition_prefix(partition))?;
        Ok(MapReadPartition {
            data: &self.data,
            prefix,
            types: PhantomData,
        })
    }
}

impl<K: StoreKey, V: StoreValue> MapPartition<'_, '_, K, V> {
    /// Reads one local key.
    /// # Errors
    /// Encoding, decoding and storage failures poison the transaction.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        read_value(self.data.as_read(), &self.prefix, key, usize::MAX)
    }
    /// Reads one value after checking its encoded length before copying or decoding.
    /// # Errors
    /// Returns retryable `ItemTooLarge` on byte admission failure; other errors poison.
    pub fn get_bounded(&self, key: &K, max_bytes: usize) -> Result<Option<V>, StoreError> {
        read_value(self.data.as_read(), &self.prefix, key, max_bytes)
    }
    /// Inserts or replaces one local key.
    /// # Errors
    /// Encoding and storage failures poison the transaction.
    pub fn put(&mut self, key: &K, value: &V) -> Result<(), StoreError> {
        let key = encode_key(self.data.as_read(), &self.prefix, key)?;
        let value = self.data.poison_on_error(value.encode_value())?;
        self.data.put(&key, value.as_ref())
    }
    /// Erases one local key without reading it.
    /// # Errors
    /// Encoding and storage failures poison the transaction.
    pub fn erase(&mut self, key: &K) -> Result<(), StoreError> {
        let key = encode_key(self.data.as_read(), &self.prefix, key)?;
        self.data.erase(&key)
    }
    /// Returns a decoded page; the limit includes partition framing, key and value bytes.
    /// # Errors
    /// Returns retryable `ItemTooLarge` if the first entry cannot fit; other failures poison.
    pub fn scan(
        &self,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<OrderedMapPage<K, V>, StoreError> {
        scan_partition(
            self.data.as_read(),
            &self.prefix,
            direction,
            resume_after,
            limit,
        )
    }
    /// Returns the first decoded entry after bounded byte admission.
    /// # Errors
    /// Same errors as `scan`; zero limits return `InvalidScanLimit`.
    pub fn first_bounded(&self, max_bytes: usize) -> Result<Option<(K, V)>, StoreError> {
        Ok(self
            .scan(
                ScanDirection::Ascending,
                None,
                ScanLimit::new(1, max_bytes)?,
            )?
            .entries
            .pop())
    }
    /// Returns the last decoded entry after bounded byte admission.
    /// # Errors
    /// Same errors as `scan`; zero limits return `InvalidScanLimit`.
    pub fn last_bounded(&self, max_bytes: usize) -> Result<Option<(K, V)>, StoreError> {
        Ok(self
            .scan(
                ScanDirection::Descending,
                None,
                ScanLimit::new(1, max_bytes)?,
            )?
            .entries
            .pop())
    }
}
impl<K: StoreKey, V: StoreValue> MapReadPartition<'_, '_, K, V> {
    /// Reads one local key visible to the snapshot.
    /// # Errors
    /// Encoding, decoding and storage failures poison the snapshot.
    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        read_value(self.data, &self.prefix, key, usize::MAX)
    }
    /// Reads one value after checking encoded length before copying or decoding.
    /// # Errors
    /// Returns retryable `ItemTooLarge` on admission failure; other failures poison.
    pub fn get_bounded(&self, key: &K, max_bytes: usize) -> Result<Option<V>, StoreError> {
        read_value(self.data, &self.prefix, key, max_bytes)
    }
    /// Returns a decoded page; the bound includes partition framing, key and value bytes.
    /// # Errors
    /// Returns retryable `ItemTooLarge` if the first entry cannot fit; other failures poison.
    pub fn scan(
        &self,
        direction: ScanDirection,
        resume_after: Option<&K>,
        limit: ScanLimit,
    ) -> Result<OrderedMapPage<K, V>, StoreError> {
        scan_partition(self.data, &self.prefix, direction, resume_after, limit)
    }
    /// Returns the first entry after bounded byte admission.
    /// # Errors
    /// Same errors as `scan`; zero limits return `InvalidScanLimit`.
    pub fn first_bounded(&self, max_bytes: usize) -> Result<Option<(K, V)>, StoreError> {
        Ok(self
            .scan(
                ScanDirection::Ascending,
                None,
                ScanLimit::new(1, max_bytes)?,
            )?
            .entries
            .pop())
    }
    /// Returns the last entry after bounded byte admission.
    /// # Errors
    /// Same errors as `scan`; zero limits return `InvalidScanLimit`.
    pub fn last_bounded(&self, max_bytes: usize) -> Result<Option<(K, V)>, StoreError> {
        Ok(self
            .scan(
                ScanDirection::Descending,
                None,
                ScanLimit::new(1, max_bytes)?,
            )?
            .entries
            .pop())
    }
}

fn partition_prefix<P: StoreKey>(partition: &P) -> Result<Vec<u8>, CodecError> {
    let bytes = partition.encode_key()?;
    let mut prefix = Vec::new();
    for byte in bytes.as_ref() {
        prefix.push(*byte);
        if *byte == 0 {
            prefix.push(255);
        }
    }
    prefix.extend_from_slice(&[0, 0]);
    Ok(prefix)
}
pub(super) fn encode_key<K: StoreKey>(
    data: &ReadDataAccess<'_>,
    prefix: &[u8],
    key: &K,
) -> Result<Vec<u8>, StoreError> {
    let key = data.poison_on_error(key.encode_key())?;
    let mut encoded = prefix.to_vec();
    encoded.extend_from_slice(key.as_ref());
    Ok(encoded)
}
fn read_value<K: StoreKey, V: StoreValue>(
    data: &ReadDataAccess<'_>,
    prefix: &[u8],
    key: &K,
    max_bytes: usize,
) -> Result<Option<V>, StoreError> {
    let key = encode_key(data, prefix, key)?;
    let value = data.get_bounded(&key, max_bytes)?;
    data.poison_on_error(
        value
            .map(|bytes| V::decode_value(Cow::Owned(bytes)))
            .transpose(),
    )
    .map_err(StoreError::from)
}
fn scan_partition<K: StoreKey, V: StoreValue>(
    data: &ReadDataAccess<'_>,
    prefix: &[u8],
    direction: ScanDirection,
    resume_after: Option<&K>,
    limit: ScanLimit,
) -> Result<OrderedMapPage<K, V>, StoreError> {
    let mut upper = prefix.to_vec();
    let last = upper.last_mut().expect("partition terminator is present");
    *last += 1;
    let resume = resume_after
        .map(|key| encode_key(data, prefix, key))
        .transpose()?;
    let raw = data.scan_key_suffix(
        (Bound::Included(prefix), Bound::Excluded(upper.as_slice())),
        direction,
        resume.as_deref(),
        limit,
        prefix,
    )?;
    let continuation = data.poison_on_error(
        raw.items
            .last()
            .filter(|_| raw.limited)
            .map(|(key, _)| K::decode_key(Cow::Borrowed(key)))
            .transpose(),
    )?;
    let entries = data.poison_on_error(
        raw.items
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    K::decode_key(Cow::Owned(key))?,
                    V::decode_value(Cow::Owned(value))?,
                ))
            })
            .collect::<Result<Vec<_>, CodecError>>(),
    )?;
    Ok(OrderedMapPage {
        entries,
        continuation,
    })
}
