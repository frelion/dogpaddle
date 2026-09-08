use std::ops::{Bound, RangeBounds};

use rocksdb::{DBAccess, Direction, IteratorMode, ReadOptions, SnapshotWithThreadMode};

use super::{DataHandle, ReadTransaction, ReadTransactionAccess, Transaction, TransactionAccess};
use crate::StoreError;

const DATA_DOMAIN: u8 = 2;

type EncodedBound<'key> = (&'key [u8], bool);
type EncodedEntry = (Vec<u8>, Vec<u8>);

pub(crate) struct ScanBatch {
    pub(crate) items: Vec<EncodedEntry>,
    pub(crate) limited: bool,
}

/// Direction of an ordered scan over encoded keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanDirection {
    /// Visit keys from smallest to largest.
    Ascending,
    /// Visit keys from largest to smallest.
    Descending,
}

/// Hard item and logical encoded-byte bounds for one scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanLimit {
    max_items: usize,
    max_bytes: usize,
}

/// Transaction-bound access to one encoded key/value namespace.
pub(crate) struct DataAccess<'transaction> {
    read: ReadDataAccess<'transaction>,
    transaction: &'transaction Transaction<'transaction>,
}

/// Read-only transaction-bound access to one encoded key/value namespace.
pub(crate) struct ReadDataAccess<'transaction> {
    transaction: TransactionRef<'transaction>,
    prefix: [u8; 5],
}

/// Identifies the transaction that backs a read without exposing mutation.
#[derive(Clone, Copy)]
pub(crate) enum TransactionRef<'transaction> {
    Read(&'transaction ReadTransaction<'transaction>),
    Write(&'transaction Transaction<'transaction>),
}

impl ScanLimit {
    /// Creates non-zero item and logical encoded key-plus-value byte bounds.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidScanLimit`] when either bound is zero.
    pub fn new(max_items: usize, max_bytes: usize) -> Result<Self, StoreError> {
        if max_items == 0 || max_bytes == 0 {
            Err(StoreError::InvalidScanLimit)
        } else {
            Ok(Self {
                max_items,
                max_bytes,
            })
        }
    }

    /// Returns the maximum number of entries in one batch.
    #[must_use]
    pub const fn max_items(self) -> usize {
        self.max_items
    }

    /// Returns the maximum logical encoded key-plus-value bytes in one batch.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

impl DataHandle {
    /// Binds this namespace through an active transaction's access capability.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong-store handle or a poisoned transaction.
    pub(crate) fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<DataAccess<'transaction>, StoreError> {
        let transaction = access.transaction();
        let read = self.bind_read(TransactionRef::Write(transaction))?;
        Ok(DataAccess { read, transaction })
    }

    /// Binds this namespace through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong-store handle or a poisoned transaction.
    pub(crate) fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<ReadDataAccess<'transaction>, StoreError> {
        self.bind_read(TransactionRef::Read(access.transaction()))
    }

    fn bind_read<'transaction>(
        &self,
        transaction: TransactionRef<'transaction>,
    ) -> Result<ReadDataAccess<'transaction>, StoreError> {
        transaction.ensure_access(self)?;
        Ok(ReadDataAccess {
            transaction,
            prefix: data_prefix(self.data_id),
        })
    }
}

impl<'transaction> DataAccess<'transaction> {
    /// Borrows the shared read core without transferring write authority.
    pub(crate) const fn as_read(&self) -> &ReadDataAccess<'transaction> {
        &self.read
    }

    /// Marks the transaction unusable when a collection-level operation fails.
    pub(crate) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        self.read.poison_on_error(result)
    }

    /// Reports whether an encoded key exists.
    pub(crate) fn contains_key(&self, key: &[u8]) -> Result<bool, StoreError> {
        self.read.contains_key(key)
    }

    /// Inserts or replaces an encoded value.
    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.transaction.ensure_healthy()?;
        let key = physical_key(self.read.prefix, key);
        self.transaction.record_result(
            self.transaction
                .inner
                .put(key, value)
                .map_err(|error| StoreError::storage("write data", error)),
        )
    }

    /// Deletes an encoded key and reports whether it existed.
    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.transaction.ensure_healthy()?;
        if !self.read.contains_key(key)? {
            return Ok(false);
        }
        let key = physical_key(self.read.prefix, key);
        self.transaction.record_result(
            self.transaction
                .inner
                .delete(key)
                .map(|()| true)
                .map_err(|error| StoreError::storage("delete data", error)),
        )
    }
}

impl TransactionRef<'_> {
    fn ensure_access(self, handle: &DataHandle) -> Result<(), StoreError> {
        match self {
            Self::Read(transaction) => transaction.ensure_access(handle),
            Self::Write(transaction) => transaction.ensure_access(handle),
        }
    }

    fn ensure_healthy(self) -> Result<(), StoreError> {
        match self {
            Self::Read(transaction) => transaction.ensure_healthy(),
            Self::Write(transaction) => transaction.ensure_healthy(),
        }
    }

    pub(crate) fn poison_on_error<T, E>(self, result: Result<T, E>) -> Result<T, E> {
        match self {
            Self::Read(transaction) => transaction.poison_on_error(result),
            Self::Write(transaction) => transaction.poison_on_error(result),
        }
    }

    fn record_result<T>(self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        match self {
            Self::Read(transaction) => transaction.record_result(result),
            Self::Write(transaction) => transaction.record_result(result),
        }
    }

    fn get(self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.ensure_healthy()?;
        let result = match self {
            Self::Read(transaction) => transaction.snapshot.get(key),
            Self::Write(transaction) => transaction.inner.snapshot().get(key),
        }
        .map_err(|error| StoreError::storage("read data", error));
        self.record_result(result)
    }

    fn scan<'key>(
        self,
        prefix: [u8; 5],
        direction: ScanDirection,
        lower: Option<&EncodedBound<'key>>,
        upper: Option<&EncodedBound<'key>>,
        limit: ScanLimit,
    ) -> Result<ScanBatch, StoreError> {
        self.ensure_healthy()?;
        let result = match self {
            Self::Read(transaction) => scan_data(
                &transaction.snapshot,
                prefix,
                direction,
                lower,
                upper,
                limit,
            ),
            Self::Write(transaction) => {
                let snapshot = transaction.inner.snapshot();
                scan_data(&snapshot, prefix, direction, lower, upper, limit)
            }
        };
        self.record_result(result)
    }
}

impl<'transaction> ReadDataAccess<'transaction> {
    /// Returns the transaction source used to poison a failed scan entry.
    pub(crate) const fn transaction_ref(&self) -> TransactionRef<'transaction> {
        self.transaction
    }

    /// Verifies that no earlier hard operation has poisoned the transaction.
    pub(crate) fn ensure_healthy(&self) -> Result<(), StoreError> {
        self.transaction.ensure_healthy()
    }

    /// Marks the transaction unusable when a collection-level operation fails.
    pub(crate) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        self.transaction.poison_on_error(result)
    }

    /// Applies the Store's normal hard-versus-retryable error policy.
    pub(crate) fn record_result<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        self.transaction.record_result(result)
    }

    /// Reads an encoded value.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.transaction.get(&physical_key(self.prefix, key))
    }

    /// Reports whether an encoded key exists.
    pub(crate) fn contains_key(&self, key: &[u8]) -> Result<bool, StoreError> {
        self.get(key).map(|value| value.is_some())
    }

    /// Reports whether this namespace contains no entries.
    pub(crate) fn is_physically_empty(&self) -> Result<bool, StoreError> {
        let limit = ScanLimit {
            max_items: 1,
            max_bytes: usize::MAX,
        };
        self.transaction
            .scan(self.prefix, ScanDirection::Ascending, None, None, limit)
            .map(|batch| batch.items.is_empty())
    }

    /// Owns one bounded page of encoded entries in byte order.
    pub(crate) fn scan<'range, R>(
        &self,
        range: R,
        direction: ScanDirection,
        resume_after: Option<&[u8]>,
        limit: ScanLimit,
    ) -> Result<ScanBatch, StoreError>
    where
        R: RangeBounds<&'range [u8]>,
    {
        let declared_lower = match range.start_bound() {
            Bound::Included(key) => Some((*key, true)),
            Bound::Excluded(key) => Some((*key, false)),
            Bound::Unbounded => None,
        };
        let declared_upper = match range.end_bound() {
            Bound::Included(key) => Some((*key, true)),
            Bound::Excluded(key) => Some((*key, false)),
            Bound::Unbounded => None,
        };
        let resume = resume_after.map(|key| (key, false));
        let (lower, upper) = match direction {
            ScanDirection::Ascending => (later_bound(declared_lower, resume), declared_upper),
            ScanDirection::Descending => (declared_lower, earlier_bound(declared_upper, resume)),
        };
        self.transaction.scan(
            self.prefix,
            direction,
            lower.as_ref(),
            upper.as_ref(),
            limit,
        )
    }
}

fn scan_data<D: DBAccess>(
    snapshot: &SnapshotWithThreadMode<'_, D>,
    prefix: [u8; 5],
    direction: ScanDirection,
    lower: Option<&EncodedBound<'_>>,
    upper: Option<&EncodedBound<'_>>,
    limit: ScanLimit,
) -> Result<ScanBatch, StoreError> {
    let mut read_options = ReadOptions::default();
    read_options.set_iterate_lower_bound(prefix.to_vec());
    if direction == ScanDirection::Ascending {
        read_options.set_iterate_upper_bound(prefix_successor(prefix));
    }

    let seek = match direction {
        ScanDirection::Ascending => {
            lower.map_or_else(|| prefix.to_vec(), |(key, _)| physical_key(prefix, key))
        }
        ScanDirection::Descending => upper.map_or_else(
            || prefix_successor(prefix),
            |(key, _)| physical_key(prefix, key),
        ),
    };
    let rocks_direction = match direction {
        ScanDirection::Ascending => Direction::Forward,
        ScanDirection::Descending => Direction::Reverse,
    };
    let mut items = Vec::new();
    let mut bytes = 0_usize;

    for item in snapshot.iterator_opt(IteratorMode::From(&seek, rocks_direction), read_options) {
        let (physical_key, value) =
            item.map_err(|error| StoreError::storage("scan data", error))?;
        let Some(key) = physical_key.strip_prefix(&prefix) else {
            if direction == ScanDirection::Descending {
                continue;
            }
            break;
        };
        if !within_lower(key, lower) {
            if direction == ScanDirection::Ascending {
                continue;
            }
            break;
        }
        if !within_upper(key, upper) {
            if direction == ScanDirection::Descending {
                continue;
            }
            break;
        }
        if items.len() == limit.max_items() {
            return Ok(ScanBatch {
                items,
                limited: true,
            });
        }

        let item_bytes = key
            .len()
            .checked_add(value.len())
            .ok_or(StoreError::ItemTooLarge {
                size: usize::MAX,
                limit: limit.max_bytes(),
            })?;
        let next_bytes = bytes.checked_add(item_bytes);
        if next_bytes.is_none_or(|size| size > limit.max_bytes()) {
            if items.is_empty() {
                return Err(StoreError::ItemTooLarge {
                    size: item_bytes,
                    limit: limit.max_bytes(),
                });
            }
            return Ok(ScanBatch {
                items,
                limited: true,
            });
        }
        bytes = next_bytes.expect("bounded sum was checked above");
        items.push((key.to_vec(), value.into_vec()));
    }

    Ok(ScanBatch {
        items,
        limited: false,
    })
}

fn data_prefix(data_id: u32) -> [u8; 5] {
    let [a, b, c, d] = data_id.to_be_bytes();
    [DATA_DOMAIN, a, b, c, d]
}

fn physical_key(prefix: [u8; 5], logical_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + logical_key.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(logical_key);
    key
}

fn prefix_successor(prefix: [u8; 5]) -> Vec<u8> {
    let mut successor = prefix.to_vec();
    for byte in successor.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return successor;
        }
        *byte = 0;
    }
    unreachable!("the data domain has a successor")
}

fn later_bound<'key>(
    left: Option<EncodedBound<'key>>,
    right: Option<EncodedBound<'key>>,
) -> Option<EncodedBound<'key>> {
    choose_bound(left, right, std::cmp::Ordering::Greater)
}

fn earlier_bound<'key>(
    left: Option<EncodedBound<'key>>,
    right: Option<EncodedBound<'key>>,
) -> Option<EncodedBound<'key>> {
    choose_bound(left, right, std::cmp::Ordering::Less)
}

fn choose_bound<'key>(
    left: Option<EncodedBound<'key>>,
    right: Option<EncodedBound<'key>>,
    preferred: std::cmp::Ordering,
) -> Option<EncodedBound<'key>> {
    match (left, right) {
        (Some(left), Some(right)) => match left.0.cmp(right.0) {
            std::cmp::Ordering::Less => Some(if preferred == std::cmp::Ordering::Less {
                left
            } else {
                right
            }),
            std::cmp::Ordering::Equal => Some((left.0, left.1 && right.1)),
            std::cmp::Ordering::Greater => Some(if preferred == std::cmp::Ordering::Greater {
                left
            } else {
                right
            }),
        },
        (bound, None) | (None, bound) => bound,
    }
}

fn within_lower(key: &[u8], lower: Option<&EncodedBound<'_>>) -> bool {
    lower.is_none_or(|(lower_key, inclusive)| match key.cmp(lower_key) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => *inclusive,
        std::cmp::Ordering::Less => false,
    })
}

fn within_upper(key: &[u8], upper: Option<&EncodedBound<'_>>) -> bool {
    upper.is_none_or(|(upper_key, inclusive)| match key.cmp(upper_key) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => *inclusive,
        std::cmp::Ordering::Greater => false,
    })
}
