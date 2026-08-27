use std::marker::PhantomData;

use crate::{
    DataAccess, DataHandle, ReadDataAccess, ReadTransactionAccess, StoreError, StoreValue,
    TransactionAccess,
};

const CELL_KEY: &[u8] = &[];

/// A named persistent cell holding one optional typed value.
///
/// Cells always use shared physical storage because their cardinality is
/// intrinsically bounded; callers do not choose a size class.
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
/// This view can originate from either an active [`crate::Transaction`] or
/// [`crate::ReadTransaction`]. It has no `set` or `clear` method and cannot
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

impl<'transaction, T: StoreValue> CellAccess<'transaction, T> {
    pub(crate) fn into_read(self) -> CellReadAccess<'transaction, T> {
        CellReadAccess {
            data: self.data.into_read(),
            _value: PhantomData,
        }
    }

    /// Reads the current value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        read_cell(self.data.as_read())
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

impl<T: StoreValue> CellReadAccess<'_, T> {
    /// Reads the current value visible to the originating transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        read_cell(&self.data)
    }
}

fn read_cell<T: StoreValue>(data: &ReadDataAccess<'_>) -> Result<Option<T>, StoreError> {
    let encoded = data.get(CELL_KEY)?;
    data.poison_on_error(encoded.map(T::decode_value).transpose())
        .map_err(StoreError::from)
}

impl<T> Clone for Cell<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }
}
