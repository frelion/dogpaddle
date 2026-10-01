use std::marker::PhantomData;

use crate::{
    DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, StoreError, StoreValue,
    TransactionAccess,
};

const CELL_KEY: &[u8] = &[];

/// A named persistent cell holding one optional typed value.
///
/// A cell's cardinality is intrinsically bounded; callers choose no physical
/// placement or size class.
pub struct Cell<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Transaction-bound access to a [`Cell`].
pub struct CellAccess<'transaction, T> {
    data: DataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

/// A read-only transaction-bound view of a [`Cell`].
///
/// This view borrows an active [`crate::ReadTransaction`]. It has no `set` or `clear` method and cannot
/// outlive that transaction.
///
/// ```compile_fail
/// use dogpaddle_store::CellReadAccess;
///
/// fn set(access: &mut CellReadAccess<'_, u64>) {
///     access.set(&1).unwrap();
/// }
/// ```
pub struct CellReadAccess<'transaction, T> {
    data: ReadDataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

impl<T: StoreValue> Cell<T> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _value: PhantomData,
        }
    }

    /// Binds this cell through an active transaction's access capability.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another store or the
    /// underlying transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        access: TransactionAccess<'transaction>,
    ) -> Result<CellAccess<'transaction, T>, StoreError> {
        Ok(CellAccess {
            data: self.data.access(access)?,
            _value: PhantomData,
        })
    }

    /// Binds this cell through an active read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when this data object belongs to another Store or the
    /// underlying read transaction is already poisoned.
    pub fn read<'transaction>(
        &self,
        access: ReadTransactionAccess<'transaction>,
    ) -> Result<CellReadAccess<'transaction, T>, StoreError> {
        Ok(CellReadAccess {
            data: self.data.read(access)?,
            _value: PhantomData,
        })
    }
}

impl<T: StoreValue> CellAccess<'_, T> {
    /// Reads the current value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        self.get_bounded(usize::MAX)
    }

    /// Replaces the current value.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding or storage fails.
    pub fn set(&mut self, value: &T) -> Result<(), StoreError> {
        let encoded = self
            .data
            .poison_on_error(value.encode_value())
            .map_err(StoreError::from)?;
        self.data.put(CELL_KEY, encoded.as_ref())
    }

    /// Removes the current value and reports whether one existed.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails.
    pub fn clear(&mut self) -> Result<bool, StoreError> {
        self.data.delete(CELL_KEY)
    }
}

impl<T: StoreValue> CellAccess<'_, T> {
    /// Reads the current typed value only when its encoded length is within `max_bytes`.
    ///
    /// The length is checked through the transaction's pinned value before an
    /// owned payload is constructed. [`StoreError::ItemTooLarge`] is retryable,
    /// so the same transaction may retry with a larger limit.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails, or when
    /// the encoded value exceeds `max_bytes`.
    pub fn get_bounded(&self, max_bytes: usize) -> Result<Option<T>, StoreError> {
        self.data.as_read().get_bounded(CELL_KEY, max_bytes)
    }
}

impl<T: StoreValue> CellReadAccess<'_, T> {
    /// Reads the current value visible to the originating transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        self.get_bounded(usize::MAX)
    }
}

impl<T: StoreValue> CellReadAccess<'_, T> {
    /// Reads the current typed value only when its encoded length is within `max_bytes`.
    ///
    /// The length is checked through the read transaction's pinned value before
    /// decoding borrows its bytes to construct the final owned value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails, or when
    /// the encoded value exceeds `max_bytes`.
    pub fn get_bounded(&self, max_bytes: usize) -> Result<Option<T>, StoreError> {
        self.data.get_bounded(CELL_KEY, max_bytes)
    }
}

impl<T> Clone for Cell<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }
}
