use std::{borrow::Cow, marker::PhantomData, num::NonZeroU64, ops::Bound};

use crate::{
    DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanDirection, ScanLimit,
    StoreError, StoreValue, TransactionAccess,
};

const METADATA_KEY: &[u8] = &[0];
const ENTRY_DOMAIN: u8 = 1;
const POSITION_DOMAIN: u8 = 2;
const METADATA_BYTES: usize = 3 * size_of::<u64>();
const OFFSET_BYTES: u64 = size_of::<u64>() as u64;

#[derive(Clone, Copy)]
struct Metadata {
    subscriber_count: u64,
    tail: u64,
    retained_bytes: u64,
}

impl Metadata {
    fn is_valid(self) -> bool {
        self.subscriber_count != 0 && (self.tail != 0 || self.retained_bytes == 0)
    }
}

/// Setup capability for a named persistent log with a fixed subscriber set.
///
/// Initialize or validate the log during setup, then derive its narrower
/// writer and subscription capabilities. Subscriber identities are the dense
/// integers `0..subscriber_count` and never change for this log.
pub struct SubscribedLog<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Append and retention-status capability for a [`SubscribedLog`].
///
/// This capability cannot create subscribers or acknowledge their entries.
pub struct SubscribedLogWriter<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Read-and-acknowledge capability for one fixed subscriber.
///
/// A subscription can only inspect its next entry and acknowledge that exact
/// offset. It cannot rewind, skip, truncate, or append.
pub struct Subscription<T> {
    data: DataHandle,
    subscriber: u64,
    _value: PhantomData<fn() -> T>,
}

/// Retention state of a [`SubscribedLog`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscribedLogStatus {
    /// First offset still needed by at least one subscriber.
    pub head: u64,
    /// Exclusive end offset and the offset assigned to the next entry.
    pub tail: u64,
    /// Logical bytes retained by entries in `[head, tail)`.
    pub retained_bytes: u64,
}

/// Durable progress of one [`Subscription`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionStatus {
    /// Offset of this subscriber's next entry.
    pub position: u64,
    /// Current exclusive end offset of the log.
    pub tail: u64,
}

impl<T: StoreValue> SubscribedLog<T> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _value: PhantomData,
        }
    }

    /// Initializes an empty log and all of its subscriber positions.
    ///
    /// Call this exactly once after creating the data object, in the same
    /// transaction that publishes the owning durable definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the namespace is not empty, belongs to another
    /// Store, or storage access fails. An error poisons the transaction.
    pub fn initialize(
        &self,
        subscriber_count: NonZeroU64,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let mut data = self.data.access(access)?;
        if !data.as_read().is_physically_empty()? {
            return fail(
                data.as_read(),
                StoreError::CorruptSubscribedLog {
                    reason: "initialization requires an empty namespace",
                },
            );
        }
        for subscriber in 0..subscriber_count.get() {
            data.put(&position_key(subscriber), &0_u64.to_be_bytes())?;
        }
        data.put(
            METADATA_KEY,
            &encode_metadata(Metadata {
                subscriber_count: subscriber_count.get(),
                tail: 0,
                retained_bytes: 0,
            }),
        )
    }

    /// Validates the fixed subscriber configuration and retained state.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured subscriber count differs, a
    /// position is missing or out of range, retained metadata is inconsistent,
    /// the data object belongs to another Store, or storage access fails.
    pub fn validate(
        &self,
        expected_subscribers: NonZeroU64,
        access: ReadTransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let data = self.data.read(access)?;
        let metadata = read_metadata(&data)?;
        if metadata.subscriber_count != expected_subscribers.get() {
            return fail(
                &data,
                StoreError::SubscriberCountMismatch {
                    expected: expected_subscribers.get(),
                    actual: metadata.subscriber_count,
                },
            );
        }
        read_frontier(&data, metadata).map(|_| ())
    }

    /// Derives the log's append-only runtime capability.
    #[must_use]
    pub fn writer(&self) -> SubscribedLogWriter<T> {
        SubscribedLogWriter {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }

    /// Derives one subscriber's runtime capability.
    ///
    /// The returned capability checks `subscriber` against the durable fixed
    /// count when it is used.
    #[must_use]
    pub fn subscription(&self, subscriber: u64) -> Subscription<T> {
        Subscription {
            data: self.data.clone(),
            subscriber,
            _value: PhantomData,
        }
    }
}

impl<T: StoreValue> SubscribedLogWriter<T> {
    /// Appends one value unless a non-empty backlog would exceed `capacity`.
    ///
    /// An empty backlog admits one oversized value so that a single large item
    /// cannot permanently stall its producer. Each retained item contributes
    /// its complete encoded value plus its private eight-byte offset.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails, offset or byte
    /// accounting overflows, or persisted log state is corrupt. Capacity
    /// rejection returns `false` without writing or poisoning the transaction.
    pub fn try_append(
        &self,
        value: &T,
        capacity: NonZeroU64,
        access: TransactionAccess<'_>,
    ) -> Result<bool, StoreError> {
        let mut data = self.data.access(access)?;
        let metadata = read_metadata(data.as_read())?;
        let encoded = data.poison_on_error(value.encode_value().map_err(StoreError::from))?;
        let item_bytes = match encoded_item_bytes(encoded.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) => return fail(data.as_read(), error),
        };
        if metadata.retained_bytes != 0
            && (metadata.retained_bytes >= capacity.get()
                || item_bytes > capacity.get() - metadata.retained_bytes)
        {
            return Ok(false);
        }
        if metadata.retained_bytes != 0 && !data.contains_key(&entry_key(metadata.tail - 1))? {
            return fail(
                data.as_read(),
                StoreError::CorruptSubscribedLog {
                    reason: "the entry before the tail is missing",
                },
            );
        }
        let Some(tail) = metadata.tail.checked_add(1) else {
            return fail(data.as_read(), StoreError::SubscribedLogOffsetExhausted);
        };
        let key = entry_key(metadata.tail);
        if data.contains_key(&key)? {
            return fail(
                data.as_read(),
                StoreError::CorruptSubscribedLog {
                    reason: "an entry already exists at the next offset",
                },
            );
        }
        let Some(retained_bytes) = metadata.retained_bytes.checked_add(item_bytes) else {
            return fail(
                data.as_read(),
                StoreError::SubscribedLogRetainedBytesExhausted,
            );
        };
        data.put(&key, encoded.as_ref())?;
        write_metadata(
            &mut data,
            Metadata {
                tail,
                retained_bytes,
                ..metadata
            },
        )?;
        Ok(true)
    }

    /// Returns the current retained range and logical byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when the data object belongs to another Store, storage
    /// access fails, or persisted log state is corrupt.
    pub fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<SubscribedLogStatus, StoreError> {
        let data = self.data.read(access)?;
        let metadata = read_metadata(&data)?;
        let head = read_frontier(&data, metadata)?;
        Ok(SubscribedLogStatus {
            head,
            tail: metadata.tail,
            retained_bytes: metadata.retained_bytes,
        })
    }
}

impl<T: StoreValue> Subscription<T> {
    /// Returns this subscriber's next offset and owned decoded value.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscriber is outside the fixed set, storage
    /// or decoding fails, or persisted log state is corrupt.
    pub fn peek(&self, access: ReadTransactionAccess<'_>) -> Result<Option<(u64, T)>, StoreError> {
        let data = self.data.read(access)?;
        let metadata = read_metadata(&data)?;
        let position = read_position(&data, metadata, self.subscriber)?;
        if position == metadata.tail {
            return Ok(None);
        }
        let encoded = data
            .get(&entry_key(position))?
            .ok_or(StoreError::CorruptSubscribedLog {
                reason: "a subscriber's next entry is missing",
            });
        let encoded = data.record_result(encoded)?;
        let value =
            data.poison_on_error(T::decode_value(Cow::Owned(encoded)).map_err(StoreError::from))?;
        Ok(Some((position, value)))
    }

    /// Acknowledges exactly the offset currently due to this subscriber.
    ///
    /// The subscriber advances by one and retained-state accounting is updated
    /// atomically.
    ///
    /// # Errors
    ///
    /// Returns an error when `expected_offset` is not the subscriber's current
    /// position, it is already caught up, storage fails, or persisted log state
    /// is corrupt. Any error poisons the transaction.
    pub fn acknowledge(
        &self,
        expected_offset: u64,
        access: TransactionAccess<'_>,
    ) -> Result<(), StoreError> {
        let mut data = self.data.access(access)?;
        let metadata = read_metadata(data.as_read())?;
        let positions = read_positions(data.as_read(), metadata)?;
        let index = subscriber_index(metadata, self.subscriber, data.as_read())?;
        let Some(&actual) = positions.get(index) else {
            return fail(
                data.as_read(),
                StoreError::CorruptSubscribedLog {
                    reason: "a subscriber position is missing from the validated set",
                },
            );
        };
        if actual != expected_offset {
            return fail(
                data.as_read(),
                StoreError::SubscriptionPositionMismatch {
                    subscriber: self.subscriber,
                    expected: expected_offset,
                    actual,
                },
            );
        }
        if actual == metadata.tail {
            return fail(
                data.as_read(),
                StoreError::SubscriptionAtTail {
                    subscriber: self.subscriber,
                    tail: metadata.tail,
                },
            );
        }
        let Some(next) = actual.checked_add(1) else {
            return fail(
                data.as_read(),
                StoreError::CorruptSubscribedLog {
                    reason: "a subscription position before the tail has no successor",
                },
            );
        };
        let encoded =
            data.as_read()
                .get(&entry_key(actual))?
                .ok_or(StoreError::CorruptSubscribedLog {
                    reason: "the acknowledged entry is missing",
                });
        let encoded = data.as_read().record_result(encoded)?;
        let frontiers = acknowledgement_frontiers(&positions, index, next);
        let (old_head, new_head) = data.as_read().record_result(frontiers)?;
        validate_retention(data.as_read(), metadata, old_head)?;
        let retained_bytes = if new_head == old_head {
            None
        } else {
            let item_bytes = match encoded_item_bytes(encoded.as_ref()) {
                Ok(bytes) => bytes,
                Err(error) => return fail(data.as_read(), error),
            };
            let Some(retained_bytes) = metadata.retained_bytes.checked_sub(item_bytes) else {
                return fail(
                    data.as_read(),
                    StoreError::CorruptSubscribedLog {
                        reason: "retained-byte metadata is smaller than the reclaimed entry",
                    },
                );
            };
            if new_head == metadata.tail && retained_bytes != 0 {
                return fail(
                    data.as_read(),
                    StoreError::CorruptSubscribedLog {
                        reason: "reclaiming the final entry would leave retained bytes",
                    },
                );
            }
            Some(retained_bytes)
        };

        data.put(&position_key(self.subscriber), &next.to_be_bytes())?;
        if let Some(retained_bytes) = retained_bytes {
            if !data.delete(&entry_key(old_head))? {
                return fail(
                    data.as_read(),
                    StoreError::CorruptSubscribedLog {
                        reason: "the retention-front entry disappeared during acknowledgement",
                    },
                );
            }
            write_metadata(
                &mut data,
                Metadata {
                    retained_bytes,
                    ..metadata
                },
            )?;
        }
        Ok(())
    }

    /// Returns this subscriber's durable position and the current log tail.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscriber is outside the fixed set, storage
    /// access fails, or persisted log state is corrupt.
    pub fn status(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<SubscriptionStatus, StoreError> {
        let data = self.data.read(access)?;
        let metadata = read_metadata(&data)?;
        Ok(SubscriptionStatus {
            position: read_position(&data, metadata, self.subscriber)?,
            tail: metadata.tail,
        })
    }
}

fn read_metadata(data: &ReadDataAccess<'_>) -> Result<Metadata, StoreError> {
    let encoded = data
        .get(METADATA_KEY)?
        .ok_or(StoreError::CorruptSubscribedLog {
            reason: "log metadata is missing",
        });
    let encoded = data.record_result(encoded)?;
    let metadata = decode_metadata(encoded.as_ref()).ok_or(StoreError::CorruptSubscribedLog {
        reason: "log metadata is malformed",
    });
    data.record_result(metadata)
}

fn write_metadata(data: &mut DataAccess<'_>, metadata: Metadata) -> Result<(), StoreError> {
    if !metadata.is_valid() {
        return fail(
            data.as_read(),
            StoreError::CorruptSubscribedLog {
                reason: "an update would write invalid log metadata",
            },
        );
    }
    data.put(METADATA_KEY, &encode_metadata(metadata))
}

fn read_frontier(data: &ReadDataAccess<'_>, metadata: Metadata) -> Result<u64, StoreError> {
    let positions = read_positions(data, metadata)?;
    let head = *positions
        .iter()
        .min()
        .expect("a subscribed log has a non-empty subscriber set");
    validate_retention(data, metadata, head)?;
    Ok(head)
}

fn read_positions(data: &ReadDataAccess<'_>, metadata: Metadata) -> Result<Vec<u64>, StoreError> {
    let Ok(expected) = usize::try_from(metadata.subscriber_count) else {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "subscriber count cannot be represented by this process",
            },
        );
    };
    let Some(max_items) = expected.checked_add(1) else {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "subscriber count cannot be bounded for validation",
            },
        );
    };
    let lower = [POSITION_DOMAIN];
    let upper = [POSITION_DOMAIN + 1];
    let raw = data.scan(
        (
            Bound::Included(lower.as_slice()),
            Bound::Excluded(upper.as_slice()),
        ),
        ScanDirection::Ascending,
        None,
        ScanLimit::new(max_items, usize::MAX)
            .expect("a positive subscriber count yields a valid scan limit"),
    )?;
    if raw.limited || raw.items.len() != expected {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "subscriber position keys do not match the fixed subscriber count",
            },
        );
    }
    let positions = raw
        .items
        .into_iter()
        .enumerate()
        .map(|(subscriber, (key, value))| {
            let subscriber = u64::try_from(subscriber)
                .expect("the persisted subscriber count was represented as usize");
            if key.as_slice() != position_key(subscriber).as_slice() {
                return Err(StoreError::CorruptSubscribedLog {
                    reason: "subscriber position keys are not dense",
                });
            }
            decode_position(value.as_ref(), metadata.tail)
        })
        .collect::<Result<Vec<_>, _>>();
    data.record_result(positions)
}

fn acknowledgement_frontiers(
    positions: &[u64],
    subscriber: usize,
    next: u64,
) -> Result<(u64, u64), StoreError> {
    let old_head = positions.iter().copied().min();
    let new_head = positions
        .iter()
        .enumerate()
        .map(
            |(index, position)| {
                if index == subscriber { next } else { *position }
            },
        )
        .min();
    old_head
        .zip(new_head)
        .ok_or(StoreError::CorruptSubscribedLog {
            reason: "a subscribed log has no subscriber positions",
        })
}

fn read_position(
    data: &ReadDataAccess<'_>,
    metadata: Metadata,
    subscriber: u64,
) -> Result<u64, StoreError> {
    subscriber_index(metadata, subscriber, data)?;
    let encoded = data
        .get(&position_key(subscriber))?
        .ok_or(StoreError::CorruptSubscribedLog {
            reason: "a subscriber position is missing",
        });
    let encoded = data.record_result(encoded)?;
    data.record_result(decode_position(encoded.as_ref(), metadata.tail))
}

fn subscriber_index(
    metadata: Metadata,
    subscriber: u64,
    data: &ReadDataAccess<'_>,
) -> Result<usize, StoreError> {
    if subscriber >= metadata.subscriber_count {
        return fail(
            data,
            StoreError::SubscriberOutOfRange {
                subscriber,
                subscriber_count: metadata.subscriber_count,
            },
        );
    }
    data.record_result(
        usize::try_from(subscriber).map_err(|_| StoreError::SubscriberOutOfRange {
            subscriber,
            subscriber_count: metadata.subscriber_count,
        }),
    )
}

fn decode_position(encoded: &[u8], tail: u64) -> Result<u64, StoreError> {
    let encoded: [u8; size_of::<u64>()] =
        encoded
            .try_into()
            .map_err(|_| StoreError::CorruptSubscribedLog {
                reason: "a subscriber position is malformed",
            })?;
    let position = u64::from_be_bytes(encoded);
    if position > tail {
        Err(StoreError::CorruptSubscribedLog {
            reason: "a subscriber position is beyond the log tail",
        })
    } else {
        Ok(position)
    }
}

fn validate_retention(
    data: &ReadDataAccess<'_>,
    metadata: Metadata,
    head: u64,
) -> Result<(), StoreError> {
    let entries = metadata.tail - head;
    if entries == 0 {
        if metadata.retained_bytes != 0 {
            return fail(
                data,
                StoreError::CorruptSubscribedLog {
                    reason: "an empty retained range has a non-zero byte count",
                },
            );
        }
        return Ok(());
    }
    let Some(minimum) = entries.checked_mul(OFFSET_BYTES) else {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "retained entry count cannot fit its offset bytes",
            },
        );
    };
    if metadata.retained_bytes < minimum {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "retained bytes cannot contain the retained offset keys",
            },
        );
    }
    if !data.contains_key(&entry_key(head))? {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "the retention-front entry is missing",
            },
        );
    }
    if !data.contains_key(&entry_key(metadata.tail - 1))? {
        return fail(
            data,
            StoreError::CorruptSubscribedLog {
                reason: "the entry before the tail is missing",
            },
        );
    }
    Ok(())
}

fn encoded_item_bytes(encoded: &[u8]) -> Result<u64, StoreError> {
    u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(OFFSET_BYTES))
        .ok_or(StoreError::SubscribedLogRetainedBytesExhausted)
}

fn entry_key(offset: u64) -> [u8; 1 + size_of::<u64>()] {
    let mut key = [0; 1 + size_of::<u64>()];
    key[0] = ENTRY_DOMAIN;
    key[1..].copy_from_slice(&offset.to_be_bytes());
    key
}

fn position_key(subscriber: u64) -> [u8; 1 + size_of::<u64>()] {
    let mut key = [0; 1 + size_of::<u64>()];
    key[0] = POSITION_DOMAIN;
    key[1..].copy_from_slice(&subscriber.to_be_bytes());
    key
}

fn encode_metadata(metadata: Metadata) -> [u8; METADATA_BYTES] {
    let mut encoded = [0; METADATA_BYTES];
    encoded[..8].copy_from_slice(&metadata.subscriber_count.to_be_bytes());
    encoded[8..16].copy_from_slice(&metadata.tail.to_be_bytes());
    encoded[16..].copy_from_slice(&metadata.retained_bytes.to_be_bytes());
    encoded
}

fn decode_metadata(encoded: &[u8]) -> Option<Metadata> {
    let encoded: &[u8; METADATA_BYTES] = encoded.try_into().ok()?;
    let metadata = Metadata {
        subscriber_count: u64::from_be_bytes(encoded[..8].try_into().ok()?),
        tail: u64::from_be_bytes(encoded[8..16].try_into().ok()?),
        retained_bytes: u64::from_be_bytes(encoded[16..].try_into().ok()?),
    };
    metadata.is_valid().then_some(metadata)
}

fn fail<T>(data: &ReadDataAccess<'_>, error: StoreError) -> Result<T, StoreError> {
    data.record_result(Err(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, OrderedMap, OrderedMultiset, PartitionedMultiset, Queue, Store};
    use rocksdb::{IteratorMode, OptimisticTransactionDB, SingleThreaded};

    #[test]
    fn rocksdb_and_collection_layouts_are_literal() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let mut store = Store::create(&path).unwrap();
        let cell = store.create_data::<Cell<u64>>("cell").unwrap();
        let map = store.create_data::<OrderedMap<u64, u64>>("map").unwrap();
        let multiset = store
            .create_data::<OrderedMultiset<u64>>("multiset")
            .unwrap();
        let partitioned = store
            .create_data::<PartitionedMultiset<u64, u64>>("partitioned")
            .unwrap();
        let queue = store.create_data::<Queue<Vec<u8>>>("queue").unwrap();
        let log = store.create_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
        let writer = log.writer();
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        cell.access(transaction.access()).unwrap().set(&42).unwrap();
        map.access(transaction.access())
            .unwrap()
            .put(&7, &9)
            .unwrap();
        multiset
            .access(transaction.access())
            .unwrap()
            .adjust(&3, 4)
            .unwrap();
        partitioned
            .access(transaction.access())
            .unwrap()
            .partition(&11)
            .unwrap()
            .adjust(&12, 5)
            .unwrap();
        assert!(
            queue
                .access(transaction.access())
                .unwrap()
                .try_push(&vec![0xcc], NonZeroU64::MAX)
                .unwrap()
        );
        log.initialize(NonZeroU64::new(2).unwrap(), transaction.access())
            .unwrap();
        assert!(
            writer
                .try_append(&vec![0xaa, 0xbb], NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();
        drop(transactions);

        let database: OptimisticTransactionDB<SingleThreaded> =
            OptimisticTransactionDB::open_default(path).unwrap();
        let actual = database
            .iterator(IteratorMode::Start)
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.into_vec(), value.into_vec())
            })
            .collect::<Vec<_>>();
        let expected = vec![
            (vec![0], b"dogpaddle.store.rocks.v1\0".to_vec()),
            (b"\x01cell".to_vec(), vec![1, 0, 0, 0, 0]),
            (b"\x01log".to_vec(), vec![7, 0, 0, 0, 5]),
            (b"\x01map".to_vec(), vec![2, 0, 0, 0, 1]),
            (b"\x01multiset".to_vec(), vec![4, 0, 0, 0, 2]),
            (b"\x01partitioned".to_vec(), vec![5, 0, 0, 0, 3]),
            (b"\x01queue".to_vec(), vec![6, 0, 0, 0, 4]),
            (b"\x02\0\0\0\0".to_vec(), 42_u64.to_be_bytes().to_vec()),
            (
                b"\x02\0\0\0\x01\0\0\0\0\0\0\0\x07".to_vec(),
                9_u64.to_be_bytes().to_vec(),
            ),
            (
                b"\x02\0\0\0\x02\0\0\0\0\0\0\0\x03".to_vec(),
                4_u64.to_be_bytes().to_vec(),
            ),
            (
                b"\x02\0\0\0\x03\0\0\0\0\0\0\0\x08\0\0\0\0\0\0\0\x0b\0\0\0\0\0\0\0\x0c".to_vec(),
                5_u64.to_be_bytes().to_vec(),
            ),
            (
                b"\x02\0\0\0\x04".to_vec(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9,
                ],
            ),
            (b"\x02\0\0\0\x04\0\0\0\0\0\0\0\0".to_vec(), vec![0xcc]),
            (
                b"\x02\0\0\0\x05\0".to_vec(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 10,
                ],
            ),
            (
                b"\x02\0\0\0\x05\x01\0\0\0\0\0\0\0\0".to_vec(),
                vec![0xaa, 0xbb],
            ),
            (b"\x02\0\0\0\x05\x02\0\0\0\0\0\0\0\0".to_vec(), vec![0; 8]),
            (b"\x02\0\0\0\x05\x02\0\0\0\0\0\0\0\x01".to_vec(), vec![0; 8]),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn missing_entry_poisons_acknowledgement_and_rolls_back_other_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let log = store.create_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
        let marker = store.create_data::<Cell<u64>>("marker").unwrap();
        let writer = log.writer();
        let subscription = log.subscription(0);
        let mut transactions = store.into_transactions();

        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        assert!(
            writer
                .try_append(&vec![1], NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        assert!(
            log.data
                .access(transaction.access())
                .unwrap()
                .delete(&entry_key(0))
                .unwrap()
        );
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        marker
            .access(transaction.access())
            .unwrap()
            .set(&1)
            .unwrap();
        assert!(matches!(
            subscription.acknowledge(0, transaction.access()),
            Err(StoreError::CorruptSubscribedLog { .. })
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
    fn final_acknowledgement_rejects_a_nonzero_byte_remainder_and_rolls_back() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let log = store.create_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
        let marker = store.create_data::<Cell<u64>>("marker").unwrap();
        let writer = log.writer();
        let subscription = log.subscription(0);
        let (mut transactions, reads) = store.into_transactions().split();

        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        assert!(
            writer
                .try_append(&vec![1], NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        let mut data = log.data.access(transaction.access()).unwrap();
        let metadata = read_metadata(data.as_read()).unwrap();
        write_metadata(
            &mut data,
            Metadata {
                retained_bytes: metadata.retained_bytes + 1,
                ..metadata
            },
        )
        .unwrap();
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        marker
            .access(transaction.access())
            .unwrap()
            .set(&1)
            .unwrap();
        assert!(matches!(
            subscription.acknowledge(0, transaction.access()),
            Err(StoreError::CorruptSubscribedLog { .. })
        ));
        assert!(matches!(
            transaction.commit(),
            Err(StoreError::TransactionPoisoned)
        ));

        let transaction = reads.begin();
        let access = transaction.access();
        assert_eq!(marker.read(access).unwrap().get().unwrap(), None);
        assert_eq!(subscription.peek(access).unwrap(), Some((0, vec![1])));
        assert_eq!(
            subscription.status(access).unwrap(),
            SubscriptionStatus {
                position: 0,
                tail: 1,
            }
        );
    }

    #[test]
    fn validation_rejects_extra_subscriber_positions() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let log = store.create_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
        let (mut transactions, reads) = store.into_transactions().split();

        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        transaction.commit().unwrap();

        let transaction = transactions.begin();
        log.data
            .access(transaction.access())
            .unwrap()
            .put(&position_key(1), &0_u64.to_be_bytes())
            .unwrap();
        transaction.commit().unwrap();

        let transaction = reads.begin();
        assert!(matches!(
            log.validate(NonZeroU64::MIN, transaction.access()),
            Err(StoreError::CorruptSubscribedLog { .. })
        ));
    }
}
