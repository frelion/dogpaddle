use std::{borrow::Cow, marker::PhantomData, num::NonZeroUsize, ops::Range};

use crate::{
    CodecError, DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, ScanLimit,
    StoreError, StoreValue, TransactionAccess, TransactionRef,
};

const METADATA_KEY: &[u8] = &[];
const METADATA_BYTES: usize = 16;
const OFFSET_BYTES: usize = size_of::<u64>();

/// A named append-only sequence of typed values.
///
/// An append log always owns a dedicated physical table. Its stable offsets
/// are never renumbered: retained entries occupy the half-open range returned
/// by [`AppendLogAccess::bounds`]. Removing a prefix advances the range start
/// without changing later offsets.
pub struct AppendLog<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Transaction-bound access to an [`AppendLog`].
pub struct AppendLogAccess<'transaction, T> {
    data: DataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

/// A read-only transaction-bound view of an [`AppendLog`].
///
/// This view can originate from either an active [`crate::Transaction`] or
/// [`crate::ReadTransaction`]. It exposes bounds and scans, but no append or
/// truncation API, and cannot outlive the originating transaction.
///
/// ```compile_fail
/// use dogpaddle_store::AppendLogReadAccess;
///
/// fn append(access: &mut AppendLogReadAccess<'_, Vec<u8>>) {
///     access.append(&b"change".to_vec()).unwrap();
/// }
/// ```
pub struct AppendLogReadAccess<'transaction, T> {
    data: ReadDataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

/// One encoded append-log entry borrowed for a scan callback.
///
/// The entry cannot outlive its callback. Callers may project only the fields
/// they need or decode the complete value. An entry borrowed from a writable
/// transaction may also be forwarded unchanged to another [`AppendLog`] in
/// that same write transaction; a read-snapshot entry carries no such
/// authority.
///
/// The entry is transaction-bound and cannot cross threads.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<dogpaddle_store::AppendLogEntry<'static, Vec<u8>>>();
/// ```
///
/// A projection cannot return a borrow of the encoded record.
///
/// ```compile_fail
/// use dogpaddle_store::AppendLogEntry;
///
/// fn escape<'entry>(entry: AppendLogEntry<'entry, Vec<u8>>) -> &'entry [u8] {
///     entry.project(|encoded| Ok(encoded)).unwrap()
/// }
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<dogpaddle_store::AppendLogEntry<'static, Vec<u8>>>();
/// ```
pub struct AppendLogEntry<'entry, T> {
    offset: u64,
    encoded: Cow<'entry, [u8]>,
    transaction: TransactionRef<'entry>,
    _value: PhantomData<fn() -> T>,
}

/// Progress produced by one bounded append-log scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendLogScan {
    /// Offset of the next entry not visited by this scan.
    pub next_offset: u64,
    /// Whether this scan reached the tail captured when the scan began.
    pub caught_up: bool,
}

impl<T: StoreValue> AppendLog<T> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _value: PhantomData,
        }
    }

    /// Binds this log through an active transaction's access capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<AppendLogAccess<'transaction, T>, StoreError> {
        Ok(AppendLogAccess {
            data: self.data.access(access)?,
            _value: PhantomData,
        })
    }

    /// Binds this log through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// underlying read transaction is already poisoned.
    pub fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<AppendLogReadAccess<'transaction, T>, StoreError> {
        Ok(AppendLogReadAccess {
            data: self.data.read(access)?,
            _value: PhantomData,
        })
    }
}

impl<'transaction, T: StoreValue> AppendLogAccess<'transaction, T> {
    pub(crate) fn into_read(self) -> AppendLogReadAccess<'transaction, T> {
        AppendLogReadAccess {
            data: self.data.into_read(),
            _value: PhantomData,
        }
    }

    /// Returns the retained offset range `[head, tail)`.
    ///
    /// `tail` is also the offset that the next append will receive.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails or the log metadata is corrupt.
    pub fn bounds(&self) -> Result<Range<u64>, StoreError> {
        read_log_bounds(self.data.as_read())
    }

    /// Appends one value and returns its stable offset.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails, the offset space is
    /// exhausted, or the persisted log is corrupt.
    pub fn append(&mut self, value: &T) -> Result<u64, StoreError> {
        let offsets = self.append_encoded(std::iter::once(
            value.encode_value().map_err(StoreError::from),
        ))?;
        Ok(offsets.start)
    }

    /// Appends one batch and returns the stable offset range assigned to it.
    ///
    /// An empty batch is a no-op and returns `tail..tail`. The values are
    /// encoded and written in slice order.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails, the offset range would
    /// overflow, or the persisted log is corrupt. Any failure poisons the
    /// transaction and rolls back entries already written from this batch.
    pub fn append_batch(&mut self, values: &[T]) -> Result<Range<u64>, StoreError> {
        self.append_encoded(
            values
                .iter()
                .map(|value| value.encode_value().map_err(StoreError::from)),
        )
    }

    /// Appends the unchanged encoding of an entry from another log of the
    /// same value type and returns its new offset.
    ///
    /// This avoids complete value decoding and re-encoding. MDBX still copies
    /// the encoded bytes into the destination table.
    ///
    /// # Errors
    ///
    /// Returns an error when storage fails, the offset space is exhausted, or
    /// the persisted destination log is corrupt.
    pub fn append_entry(&mut self, entry: &AppendLogEntry<'_, T>) -> Result<u64, StoreError> {
        if !self.data.is_same_write_transaction(entry.transaction) {
            entry.transaction.poison();
            return self.fail(StoreError::WrongTransaction);
        }
        let offsets =
            self.append_encoded(std::iter::once(Ok::<_, StoreError>(entry.encoded.as_ref())))?;
        Ok(offsets.start)
    }

    /// Visits one bounded batch beginning at the next unread `offset`.
    ///
    /// Values are not decoded eagerly. Each callback receives a temporary
    /// [`AppendLogEntry`] and can project only the fields it needs. The scan
    /// validates the selected offsets before invoking the first callback, and
    /// the returned [`AppendLogScan::next_offset`] is the first unvisited
    /// offset. Byte limits count each eight-byte offset plus its full encoded
    /// value, not the size of a projection.
    ///
    /// Callbacks should keep non-store side effects out of the transaction: a
    /// later callback failure poisons the transaction and rolls back store
    /// writes, but cannot undo external effects.
    ///
    /// # Errors
    ///
    /// Returns an error when `offset` is outside the retained range, the first
    /// entry exceeds the byte limit, storage access or the callback fails, or
    /// the persisted log is corrupt.
    pub fn scan<E>(
        &self,
        offset: u64,
        limit: ScanLimit,
        visit: impl for<'entry> FnMut(AppendLogEntry<'entry, T>) -> Result<(), E>,
    ) -> Result<AppendLogScan, E>
    where
        E: From<StoreError>,
    {
        scan_log(self.data.as_read(), offset, limit, visit)
    }

    /// Deletes at most `max_items` retained entries below `target` and
    /// returns the resulting head offset.
    ///
    /// A target at or below the current head is idempotent. Callers can repeat
    /// this operation until the returned head reaches the desired target.
    ///
    /// # Errors
    ///
    /// Returns an error when `target` is beyond the current tail, storage
    /// access fails, or the persisted log is corrupt.
    pub fn truncate_before(
        &mut self,
        target: u64,
        max_items: NonZeroUsize,
    ) -> Result<u64, StoreError> {
        let bounds = self.read_bounds()?;
        if target > bounds.end {
            return self.fail(StoreError::LogOffsetOutOfRange {
                offset: target,
                head: bounds.start,
                tail: bounds.end,
            });
        }
        if target <= bounds.start {
            return Ok(bounds.start);
        }

        let available = target - bounds.start;
        let requested = u64::try_from(max_items.get()).unwrap_or(u64::MAX);
        let next_head = bounds.start + available.min(requested);
        if !self
            .data
            .delete_exact_keys((bounds.start..next_head).map(encode_offset))?
        {
            return self.fail(StoreError::CorruptAppendLog {
                reason: "retained entries are missing or out of order during truncation",
            });
        }
        self.write_bounds(next_head..bounds.end)?;
        Ok(next_head)
    }

    fn append_encoded<B>(
        &mut self,
        encoded: impl ExactSizeIterator<Item = Result<B, StoreError>>,
    ) -> Result<Range<u64>, StoreError>
    where
        B: AsRef<[u8]>,
    {
        let bounds = self.read_bounds()?;
        let Ok(count) = u64::try_from(encoded.len()) else {
            return self.fail(StoreError::LogOffsetExhausted);
        };
        if count == 0 {
            return Ok(bounds.end..bounds.end);
        }
        let Some(next_tail) = bounds.end.checked_add(count) else {
            return self.fail(StoreError::LogOffsetExhausted);
        };
        if bounds.start < bounds.end && !self.data.contains_key(&encode_offset(bounds.end - 1))? {
            return self.fail(StoreError::CorruptAppendLog {
                reason: "the entry before the recorded tail is missing",
            });
        }
        let tail = bounds.end;
        let entries = encoded.enumerate().map(|(index, encoded)| {
            let offset = tail
                + u64::try_from(index)
                    .expect("exact-size iterator length was converted to u64 above");
            encoded.map(|encoded| (encode_offset(offset), encoded))
        });
        if !self.data.append_ordered(entries)? {
            return self.fail(StoreError::CorruptAppendLog {
                reason: "the next append offset is occupied or out of physical order",
            });
        }
        self.write_bounds(bounds.start..next_tail)?;
        Ok(tail..next_tail)
    }

    fn read_bounds(&self) -> Result<Range<u64>, StoreError> {
        read_log_bounds(self.data.as_read())
    }

    fn write_bounds(&mut self, bounds: Range<u64>) -> Result<(), StoreError> {
        self.data.put(METADATA_KEY, &encode_bounds(bounds))
    }

    fn fail<R>(&self, error: StoreError) -> Result<R, StoreError> {
        fail_read(self.data.as_read(), error)
    }
}

impl<T: StoreValue> AppendLogReadAccess<'_, T> {
    /// Returns the retained offset range `[head, tail)` visible to the
    /// originating transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails or the log metadata is corrupt.
    pub fn bounds(&self) -> Result<Range<u64>, StoreError> {
        read_log_bounds(&self.data)
    }

    /// Visits one bounded batch beginning at `offset` through this read-only
    /// view.
    ///
    /// Admission, callback, and progress semantics match
    /// [`AppendLogAccess::scan`]. Entries retain their transaction origin: an
    /// entry read through a write transaction can be forwarded within that
    /// transaction, while a read-snapshot entry cannot.
    ///
    /// # Errors
    ///
    /// Returns an error when `offset` is outside the retained range, the first
    /// entry exceeds the byte limit, storage access or the callback fails, or
    /// the persisted log is corrupt.
    pub fn scan<E>(
        &self,
        offset: u64,
        limit: ScanLimit,
        visit: impl for<'entry> FnMut(AppendLogEntry<'entry, T>) -> Result<(), E>,
    ) -> Result<AppendLogScan, E>
    where
        E: From<StoreError>,
    {
        scan_log(&self.data, offset, limit, visit)
    }
}

fn read_log_bounds(data: &ReadDataAccess<'_>) -> Result<Range<u64>, StoreError> {
    let Some(encoded) = data.get(METADATA_KEY)? else {
        return if data.is_physically_empty()? {
            Ok(0..0)
        } else {
            fail_read(
                data,
                StoreError::CorruptAppendLog {
                    reason: "entries exist without log metadata",
                },
            )
        };
    };
    let bounds = decode_bounds(&encoded).ok_or(StoreError::CorruptAppendLog {
        reason: "invalid log metadata",
    });
    data.record_result(bounds)
}

fn scan_log<T: StoreValue, E>(
    data: &ReadDataAccess<'_>,
    offset: u64,
    limit: ScanLimit,
    mut visit: impl for<'entry> FnMut(AppendLogEntry<'entry, T>) -> Result<(), E>,
) -> Result<AppendLogScan, E>
where
    E: From<StoreError>,
{
    let bounds = read_log_bounds(data).map_err(E::from)?;
    if offset < bounds.start || offset > bounds.end {
        return fail_read(
            data,
            StoreError::LogOffsetOutOfRange {
                offset,
                head: bounds.start,
                tail: bounds.end,
            },
        )
        .map_err(E::from);
    }
    if offset == bounds.end {
        return Ok(AppendLogScan {
            next_offset: offset,
            caught_up: true,
        });
    }

    let remaining = bounds.end - offset;
    let max_items = usize::try_from(remaining)
        .unwrap_or(limit.max_items())
        .min(limit.max_items());
    let raw_limit = ScanLimit::new(max_items, limit.max_bytes()).map_err(E::from)?;
    let raw = data
        .scan_borrowed_from(&encode_offset(offset), raw_limit)
        .map_err(E::from)?;

    let mut expected = offset;
    for (key, _) in &raw.items {
        if key.as_ref() != encode_offset(expected) {
            return fail_read(
                data,
                StoreError::CorruptAppendLog {
                    reason: "entry offsets are not contiguous",
                },
            )
            .map_err(E::from);
        }
        expected += 1;
    }
    if !raw.limited && expected < bounds.end {
        return fail_read(
            data,
            StoreError::CorruptAppendLog {
                reason: "retained entries end before the recorded tail",
            },
        )
        .map_err(E::from);
    }
    if raw.limited && expected == bounds.end {
        return fail_read(
            data,
            StoreError::CorruptAppendLog {
                reason: "an entry exists at or beyond the recorded tail",
            },
        )
        .map_err(E::from);
    }

    let mut next_offset = offset;
    for (_, encoded) in raw.items {
        let entry = AppendLogEntry {
            offset: next_offset,
            encoded,
            transaction: data.transaction_ref(),
            _value: PhantomData,
        };
        data.poison_on_error(visit(entry))?;
        data.ensure_healthy().map_err(E::from)?;
        next_offset += 1;
    }

    Ok(AppendLogScan {
        next_offset,
        caught_up: next_offset == bounds.end,
    })
}

fn fail_read<R>(data: &ReadDataAccess<'_>, error: StoreError) -> Result<R, StoreError> {
    data.record_result(Err(error))
}

impl<T> AppendLogEntry<'_, T> {
    /// Returns this entry's stable offset in its source log.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Decodes a caller-selected projection from the encoded value.
    ///
    /// The projection may create temporary borrowed views internally, but its
    /// returned value cannot borrow the encoded record. Store does not know
    /// the value's fields or schema.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Codec`] when `project` rejects the encoding and
    /// poisons the entry's transaction.
    pub fn project<R>(
        &self,
        project: impl for<'encoded> FnOnce(&'encoded [u8]) -> Result<R, CodecError>,
    ) -> Result<R, StoreError> {
        self.transaction
            .poison_on_error(project(self.encoded.as_ref()))
            .map_err(StoreError::from)
    }
}

impl<T: StoreValue> AppendLogEntry<'_, T> {
    /// Fully decodes this entry into an owned value.
    ///
    /// Full decoding borrows the encoding so the entry remains available for
    /// [`AppendLogAccess::append_entry`]. The value codec decides what the owned
    /// `T` must copy. Use [`Self::project`] when only a small part of a wide
    /// record is needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the encoded entry is not a valid `T`.
    pub fn decode_owned(&self) -> Result<T, StoreError> {
        self.transaction
            .poison_on_error(T::decode_value(Cow::Borrowed(self.encoded.as_ref())))
            .map_err(StoreError::from)
    }
}

impl<T> Clone for AppendLog<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }
}

fn encode_offset(offset: u64) -> [u8; OFFSET_BYTES] {
    offset.to_be_bytes()
}

fn encode_bounds(bounds: Range<u64>) -> [u8; METADATA_BYTES] {
    let mut encoded = [0; METADATA_BYTES];
    encoded[..OFFSET_BYTES].copy_from_slice(&bounds.start.to_be_bytes());
    encoded[OFFSET_BYTES..].copy_from_slice(&bounds.end.to_be_bytes());
    encoded
}

fn decode_bounds(encoded: &[u8]) -> Option<Range<u64>> {
    let encoded: &[u8; METADATA_BYTES] = encoded.try_into().ok()?;
    let head = u64::from_be_bytes(encoded[..OFFSET_BYTES].try_into().ok()?);
    let tail = u64::from_be_bytes(encoded[OFFSET_BYTES..].try_into().ok()?);
    (head <= tail).then_some(head..tail)
}
