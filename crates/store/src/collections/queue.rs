use std::{borrow::Cow, marker::PhantomData, num::NonZeroU64};

use crate::{DataAccess, DataHandle, ReadDataAccess, StoreError, StoreValue, TransactionAccess};

const METADATA_KEY: &[u8] = &[];
const METADATA_BYTES: usize = 3 * size_of::<u64>();
const SEQUENCE_BYTES: u64 = size_of::<u64>() as u64;

#[derive(Clone, Copy)]
struct Metadata {
    head: u64,
    tail: u64,
    queued_bytes: u64,
}

impl Metadata {
    const EMPTY: Self = Self {
        head: 0,
        tail: 0,
        queued_bytes: 0,
    };

    const fn is_empty(self) -> bool {
        self.head == self.tail
    }

    fn is_valid(self) -> bool {
        let Some(len) = self.tail.checked_sub(self.head) else {
            return false;
        };
        if len == 0 {
            return false;
        }
        len.checked_mul(SEQUENCE_BYTES)
            .is_some_and(|minimum| self.queued_bytes >= minimum)
    }
}

/// A named persistent single-owner FIFO queue.
///
/// Capacity is expressed in logical bytes. Each queued value contributes its
/// complete encoded length plus the queue's private eight-byte sequence key.
/// Sequence numbers are never exposed and reset whenever the queue becomes
/// empty.
pub struct Queue<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Transaction-bound access to a [`Queue`].
pub struct QueueAccess<'transaction, T> {
    data: DataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

impl<T: StoreValue> Queue<T> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _value: PhantomData,
        }
    }

    /// Binds this queue through an active transaction's access capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<QueueAccess<'transaction, T>, StoreError> {
        Ok(QueueAccess {
            data: self.data.access(access)?,
            _value: PhantomData,
        })
    }
}

impl<T: StoreValue> QueueAccess<'_, T> {
    /// Reports whether the queue contains no values.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails or persisted queue metadata
    /// is corrupt.
    pub fn is_empty(&self) -> Result<bool, StoreError> {
        Ok(self.read_metadata()?.is_empty())
    }

    /// Returns the logical encoded bytes currently retained by the queue.
    ///
    /// Each value contributes its private eight-byte sequence key and complete
    /// encoded value. Metadata and storage-engine overhead are not included.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails or persisted queue metadata
    /// is corrupt.
    pub fn queued_bytes(&self) -> Result<u64, StoreError> {
        Ok(self.read_metadata()?.queued_bytes)
    }

    /// Pushes one value at the back when it fits within `capacity`.
    ///
    /// Returns `false` without writing or poisoning the transaction when the
    /// value would exceed the hard capacity. The rule also applies while the
    /// queue is empty.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails, sequence or byte
    /// accounting overflows, or persisted queue state is corrupt.
    pub fn try_push(&mut self, value: &T, capacity: NonZeroU64) -> Result<bool, StoreError> {
        let metadata = self.read_metadata()?;
        let metadata = if metadata.is_empty() {
            Metadata::EMPTY
        } else {
            if !self
                .data
                .contains_key(&encode_sequence(metadata.tail - 1))?
            {
                return self.fail(StoreError::CorruptQueue {
                    reason: "the entry before the tail is missing",
                });
            }
            metadata
        };
        let encoded = self
            .data
            .poison_on_error(value.encode_value().map_err(StoreError::from))?;
        let item_bytes = match encoded_item_bytes(encoded.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        if metadata.queued_bytes >= capacity.get()
            || item_bytes > capacity.get() - metadata.queued_bytes
        {
            return Ok(false);
        }

        let Some(tail) = metadata.tail.checked_add(1) else {
            return self.fail(StoreError::QueueSequenceExhausted);
        };
        let key = encode_sequence(metadata.tail);
        if self.data.contains_key(&key)? {
            return self.fail(StoreError::CorruptQueue {
                reason: "an entry already exists at the next sequence number",
            });
        }
        let Some(queued_bytes) = metadata.queued_bytes.checked_add(item_bytes) else {
            return self.fail(StoreError::QueueByteCountExhausted);
        };
        self.data.put(&key, encoded.as_ref())?;
        self.write_metadata(Metadata {
            head: metadata.head,
            tail,
            queued_bytes,
        })?;
        Ok(true)
    }

    /// Removes and returns the value at the front of the queue.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding or storage fails, byte accounting
    /// underflows, or persisted queue state is corrupt. Any such error poisons
    /// the transaction.
    pub fn pop_front(&mut self) -> Result<Option<T>, StoreError> {
        let metadata = self.read_metadata()?;
        if metadata.is_empty() {
            return Ok(None);
        }

        let key = encode_sequence(metadata.head);
        let Some(encoded) = self.data.as_read().get(&key)? else {
            return self.fail(StoreError::CorruptQueue {
                reason: "the front entry is missing",
            });
        };
        let item_bytes = match encoded_item_bytes(encoded.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        let value = self
            .data
            .poison_on_error(T::decode_value(Cow::Owned(encoded)).map_err(StoreError::from))?;
        let Some(queued_bytes) = metadata.queued_bytes.checked_sub(item_bytes) else {
            return self.fail(StoreError::CorruptQueue {
                reason: "queued-byte metadata is smaller than the front entry",
            });
        };
        self.data.erase(&key)?;

        let head = metadata.head + 1;
        if head == metadata.tail {
            if queued_bytes != 0 {
                return self.fail(StoreError::CorruptQueue {
                    reason: "an empty queue has a non-zero byte count",
                });
            }
            self.data.erase(METADATA_KEY)?;
            if !self.data.as_read().is_physically_empty()? {
                return self.fail(StoreError::CorruptQueue {
                    reason: "entries exist outside the queued range",
                });
            }
        } else {
            if !self.data.contains_key(&encode_sequence(head))? {
                return self.fail(StoreError::CorruptQueue {
                    reason: "the next front entry is missing",
                });
            }
            self.write_metadata(Metadata {
                head,
                tail: metadata.tail,
                queued_bytes,
            })?;
        }
        Ok(Some(value))
    }

    fn read_metadata(&self) -> Result<Metadata, StoreError> {
        read_metadata(self.data.as_read())
    }

    fn write_metadata(&mut self, metadata: Metadata) -> Result<(), StoreError> {
        if !metadata.is_valid() {
            return self.fail(StoreError::CorruptQueue {
                reason: "an update would write invalid queue metadata",
            });
        }
        self.data.put(METADATA_KEY, &encode_metadata(metadata))
    }

    fn fail<R>(&self, error: StoreError) -> Result<R, StoreError> {
        self.data.as_read().record_result(Err(error))
    }
}

fn read_metadata(data: &ReadDataAccess<'_>) -> Result<Metadata, StoreError> {
    let Some(encoded) = data.get(METADATA_KEY)? else {
        return if data.is_physically_empty()? {
            Ok(Metadata::EMPTY)
        } else {
            data.record_result(Err(StoreError::CorruptQueue {
                reason: "entries exist without queue metadata",
            }))
        };
    };
    let metadata = decode_metadata(encoded.as_ref()).ok_or(StoreError::CorruptQueue {
        reason: "invalid queue metadata",
    });
    data.record_result(metadata)
}

fn encoded_item_bytes(encoded: &[u8]) -> Result<u64, StoreError> {
    u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(SEQUENCE_BYTES))
        .ok_or(StoreError::QueueByteCountExhausted)
}

fn encode_sequence(sequence: u64) -> [u8; size_of::<u64>()] {
    sequence.to_be_bytes()
}

fn encode_metadata(metadata: Metadata) -> [u8; METADATA_BYTES] {
    let mut encoded = [0; METADATA_BYTES];
    encoded[..8].copy_from_slice(&metadata.head.to_be_bytes());
    encoded[8..16].copy_from_slice(&metadata.tail.to_be_bytes());
    encoded[16..].copy_from_slice(&metadata.queued_bytes.to_be_bytes());
    encoded
}

fn decode_metadata(encoded: &[u8]) -> Option<Metadata> {
    let encoded: &[u8; METADATA_BYTES] = encoded.try_into().ok()?;
    let metadata = Metadata {
        head: u64::from_be_bytes(encoded[..8].try_into().ok()?),
        tail: u64::from_be_bytes(encoded[8..16].try_into().ok()?),
        queued_bytes: u64::from_be_bytes(encoded[16..].try_into().ok()?),
    };
    metadata.is_valid().then_some(metadata)
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, Store};

    #[test]
    fn persisted_empty_metadata_poisons_and_rolls_back_other_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let queue = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
        let safe = store.create_data::<Cell<u64>>("safe").unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        queue
            .data
            .access(transaction.access())
            .unwrap()
            .put(METADATA_KEY, &encode_metadata(Metadata::EMPTY))
            .unwrap();
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        safe.access(transaction.access()).unwrap().set(&1).unwrap();
        assert!(matches!(
            queue.access(transaction.access()).unwrap().is_empty(),
            Err(StoreError::CorruptQueue { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));

        let transaction = transactions.begin();
        assert_eq!(
            safe.access(transaction.access()).unwrap().get().unwrap(),
            None
        );
    }

    #[test]
    fn missing_persisted_entry_poisons_and_rolls_back_other_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let queue = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
        let safe = store.create_data::<Cell<u64>>("safe").unwrap();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        assert!(
            queue
                .access(transaction.access())
                .unwrap()
                .try_push(&vec![1], NonZeroU64::new(100).unwrap())
                .unwrap()
        );
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        assert!(
            queue
                .data
                .access(transaction.access())
                .unwrap()
                .delete(&encode_sequence(0))
                .unwrap()
        );
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        safe.access(transaction.access()).unwrap().set(&2).unwrap();
        assert!(matches!(
            queue.access(transaction.access()).unwrap().pop_front(),
            Err(StoreError::CorruptQueue { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));

        let transaction = transactions.begin();
        assert_eq!(
            safe.access(transaction.access()).unwrap().get().unwrap(),
            None
        );
    }
}
