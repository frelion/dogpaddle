use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
};

use libmdbx::{Cursor, ObjectLength, RW, Table, WriteFlags};

use super::{DataHandle, DataLocation, DataPlacement, Transaction, dedicated_table_name};
use crate::StoreError;

const DATA_DOMAIN: u8 = 3;
const INLINE_PHYSICAL_KEY_BYTES: usize = 64;

type EncodedBound<'key> = (&'key [u8], bool);
type ScanProbe<'txn> = (Cow<'txn, [u8]>, ObjectLength);

enum PhysicalKey<'key> {
    Borrowed(&'key [u8]),
    Inline {
        bytes: [u8; INLINE_PHYSICAL_KEY_BYTES],
        len: usize,
    },
    Heap(Vec<u8>),
}

impl AsRef<[u8]> for PhysicalKey<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(key) => key,
            Self::Inline { bytes, len } => &bytes[..*len],
            Self::Heap(key) => key,
        }
    }
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

/// One owned result batch from an ordered scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanBatch<K, V> {
    /// Entries in the requested scan direction.
    pub items: Vec<(K, V)>,
    /// Last returned key when another matching entry exists.
    pub continuation: Option<K>,
}

/// Transaction-bound access to one encoded key/value namespace.
///
/// This value cannot outlive its transaction. Collection implementations use
/// it as their only raw storage capability.
pub(crate) struct DataAccess<'transaction> {
    transaction: &'transaction Transaction<'transaction>,
    table: Table<'transaction>,
    prefix: Option<[u8; 5]>,
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
    pub(crate) const fn placement(&self) -> DataPlacement {
        match self.location {
            DataLocation::Shared(_) => DataPlacement::Shared,
            DataLocation::Dedicated(_) => DataPlacement::Dedicated,
        }
    }

    /// Binds this namespace to an active transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong-store handle, a poisoned transaction, or
    /// when MDBX cannot open the underlying table.
    pub(crate) fn access<'transaction>(
        &self,
        transaction: &'transaction Transaction<'transaction>,
    ) -> Result<DataAccess<'transaction>, StoreError> {
        transaction.ensure_access(self)?;
        let (table_name, prefix) = match self.location {
            DataLocation::Shared(data_id) => (None, Some(data_prefix(data_id))),
            DataLocation::Dedicated(table_id) => (Some(dedicated_table_name(table_id)), None),
        };
        let table = transaction.record_result(
            transaction
                .mdbx
                .open_table(table_name.as_deref())
                .map_err(|error| StoreError::storage("open store table", error)),
        )?;
        Ok(DataAccess {
            transaction,
            table,
            prefix,
        })
    }
}

impl DataAccess<'_> {
    /// Marks the transaction unusable when a collection-level hard operation fails.
    ///
    /// Custom collections use this for codec and invariant checks that happen
    /// outside the raw data methods. The original result and error type are
    /// preserved.
    ///
    /// # Errors
    ///
    /// Returns the original error after poisoning the transaction.
    pub(crate) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        self.transaction.poison_on_error(result)
    }

    /// Reads an encoded value.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or MDBX cannot read.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.transaction.ensure_healthy()?;
        let key = physical_key(self.prefix.as_ref(), key);
        self.transaction.record_result(
            self.transaction
                .mdbx
                .get::<Vec<u8>>(&self.table, key.as_ref())
                .map_err(|error| StoreError::storage("read data", error)),
        )
    }

    /// Inserts or replaces an encoded value.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or MDBX cannot write.
    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.transaction.ensure_healthy()?;
        let key = physical_key(self.prefix.as_ref(), key);
        self.transaction.record_result(
            self.transaction
                .mdbx
                .put(&self.table, key.as_ref(), value, WriteFlags::UPSERT)
                .map_err(|error| StoreError::storage("write data", error)),
        )
    }

    /// Deletes an encoded key from this namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or MDBX cannot delete.
    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.transaction.ensure_healthy()?;
        let key = physical_key(self.prefix.as_ref(), key);
        self.transaction.record_result(
            self.transaction
                .mdbx
                .del(&self.table, key.as_ref(), None)
                .map_err(|error| StoreError::storage("delete data", error)),
        )
    }

    /// Scans encoded keys in byte order within this namespace.
    ///
    /// `resume_after` is the last key returned by a previous scan with the
    /// same range and direction. It is always excluded from the result.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned, MDBX cannot scan, or
    /// the first matching item exceeds the byte limit.
    pub fn scan<'range, R>(
        &self,
        range: R,
        direction: ScanDirection,
        resume_after: Option<&[u8]>,
        limit: ScanLimit,
    ) -> Result<ScanBatch<Vec<u8>, Vec<u8>>, StoreError>
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

        self.transaction.ensure_healthy()?;
        self.transaction.record_result((|| {
            let mut cursor = self
                .transaction
                .mdbx
                .cursor(&self.table)
                .map_err(|error| StoreError::storage("open data cursor", error))?;
            let mut current = seek_scan(
                &mut cursor,
                self.prefix.as_ref(),
                direction,
                lower.as_ref(),
                upper.as_ref(),
            )?;
            let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut bytes = 0_usize;

            while let Some((physical_key, value_length)) = current {
                let key = match self.prefix.as_ref() {
                    Some(prefix) => {
                        let Some(key) = physical_key.as_ref().strip_prefix(prefix) else {
                            break;
                        };
                        key
                    }
                    None => physical_key.as_ref(),
                };
                if !within_bounds(key, lower.as_ref(), upper.as_ref()) {
                    break;
                }

                let item_bytes =
                    key.len()
                        .checked_add(*value_length)
                        .ok_or(StoreError::ItemTooLarge {
                            size: usize::MAX,
                            limit: limit.max_bytes(),
                        })?;
                let next_bytes = bytes.checked_add(item_bytes);
                if items.len() == limit.max_items()
                    || next_bytes.is_none_or(|size| size > limit.max_bytes())
                {
                    if items.is_empty() {
                        return Err(StoreError::ItemTooLarge {
                            size: item_bytes,
                            limit: limit.max_bytes(),
                        });
                    }
                    let continuation = items.last().map(|(key, _)| key.clone());
                    return Ok(ScanBatch {
                        items,
                        continuation,
                    });
                }

                // The probe reads only the value length. Materialize the value
                // after admission so rejected oversized entries are never copied.
                let key = key.to_vec();
                drop(physical_key);
                let ((), value) = cursor
                    .get_current::<(), Vec<u8>>()
                    .map_err(|error| StoreError::storage("read scanned data", error))?
                    .ok_or_else(|| {
                        StoreError::storage("read scanned data", "MDBX cursor lost its position")
                    })?;
                debug_assert_eq!(value.len(), *value_length);
                let Some(next_bytes) = next_bytes else {
                    return Err(StoreError::ItemTooLarge {
                        size: usize::MAX,
                        limit: limit.max_bytes(),
                    });
                };
                bytes = next_bytes;
                items.push((key, value));
                current = move_scan(&mut cursor, direction)?;
            }

            Ok(ScanBatch {
                items,
                continuation: None,
            })
        })())
    }
}

fn data_prefix(data_id: u32) -> [u8; 5] {
    let [a, b, c, d] = data_id.to_be_bytes();
    [DATA_DOMAIN, a, b, c, d]
}

fn physical_key<'key>(prefix: Option<&[u8; 5]>, logical_key: &'key [u8]) -> PhysicalKey<'key> {
    match prefix {
        Some(prefix) => {
            if logical_key.len() <= INLINE_PHYSICAL_KEY_BYTES - prefix.len() {
                let len = prefix.len() + logical_key.len();
                let mut bytes = [0; INLINE_PHYSICAL_KEY_BYTES];
                bytes[..prefix.len()].copy_from_slice(prefix);
                bytes[prefix.len()..len].copy_from_slice(logical_key);
                PhysicalKey::Inline { bytes, len }
            } else {
                let mut key = Vec::with_capacity(prefix.len() + logical_key.len());
                key.extend_from_slice(prefix);
                key.extend_from_slice(logical_key);
                PhysicalKey::Heap(key)
            }
        }
        None => PhysicalKey::Borrowed(logical_key),
    }
}

fn seek_scan<'txn>(
    cursor: &mut Cursor<'txn, RW>,
    prefix: Option<&[u8; 5]>,
    direction: ScanDirection,
    lower: Option<&EncodedBound<'_>>,
    upper: Option<&EncodedBound<'_>>,
) -> Result<Option<ScanProbe<'txn>>, StoreError> {
    match direction {
        ScanDirection::Ascending => match lower {
            Some((key, inclusive)) => {
                let seek = physical_key(prefix, key);
                let mut item = cursor
                    .set_range::<Cow<'txn, [u8]>, ObjectLength>(seek.as_ref())
                    .map_err(|error| StoreError::storage("seek data scan", error))?;
                if !inclusive
                    && item
                        .as_ref()
                        .is_some_and(|(physical_key, _)| physical_key.as_ref() == seek.as_ref())
                {
                    item = move_scan(cursor, direction)?;
                }
                Ok(item)
            }
            None => match prefix {
                Some(prefix) => cursor
                    .set_range::<Cow<'txn, [u8]>, ObjectLength>(prefix)
                    .map_err(|error| StoreError::storage("seek data scan", error)),
                None => cursor
                    .first::<Cow<'txn, [u8]>, ObjectLength>()
                    .map_err(|error| StoreError::storage("seek data scan", error)),
            },
        },
        ScanDirection::Descending => match upper {
            Some((key, inclusive)) => {
                let seek = physical_key(prefix, key);
                seek_at_or_below(cursor, seek.as_ref(), *inclusive, direction)
            }
            None => match prefix {
                Some(prefix) => {
                    let successor = prefix_successor(prefix);
                    seek_at_or_below(cursor, &successor, false, direction)
                }
                None => cursor
                    .last::<Cow<'txn, [u8]>, ObjectLength>()
                    .map_err(|error| StoreError::storage("seek data scan", error)),
            },
        },
    }
}

fn seek_at_or_below<'txn>(
    cursor: &mut Cursor<'txn, RW>,
    seek: &[u8],
    inclusive: bool,
    direction: ScanDirection,
) -> Result<Option<ScanProbe<'txn>>, StoreError> {
    let item = cursor
        .set_range::<Cow<'txn, [u8]>, ObjectLength>(seek)
        .map_err(|error| StoreError::storage("seek data scan", error))?;
    if inclusive
        && item
            .as_ref()
            .is_some_and(|(physical_key, _)| physical_key.as_ref() == seek)
    {
        Ok(item)
    } else if item.is_some() {
        move_scan(cursor, direction)
    } else {
        cursor
            .last::<Cow<'txn, [u8]>, ObjectLength>()
            .map_err(|error| StoreError::storage("seek data scan", error))
    }
}

fn move_scan<'txn>(
    cursor: &mut Cursor<'txn, RW>,
    direction: ScanDirection,
) -> Result<Option<ScanProbe<'txn>>, StoreError> {
    match direction {
        ScanDirection::Ascending => cursor.next::<Cow<'txn, [u8]>, ObjectLength>(),
        ScanDirection::Descending => cursor.prev::<Cow<'txn, [u8]>, ObjectLength>(),
    }
    .map_err(|error| StoreError::storage("advance data scan", error))
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

fn within_bounds(
    key: &[u8],
    lower: Option<&EncodedBound<'_>>,
    upper: Option<&EncodedBound<'_>>,
) -> bool {
    within_lower(key, lower) && within_upper(key, upper)
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

fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return successor;
        }
    }
    Vec::new()
}
