use std::marker::PhantomData;

use crate::{DataAccess, DataHandle, StoreError, StoreValue, TransactionAccess};

const CELL_KEY: &[u8] = &[];

/// A named persistent cell holding one optional typed value.
pub struct Cell<T, SIZE> {
    data: DataHandle,
    _types: PhantomData<fn() -> (T, SIZE)>,
}

/// Transaction-bound access to a [`Cell`].
pub struct CellAccess<'transaction, T> {
    data: DataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

impl<T: StoreValue, SIZE> Cell<T, SIZE> {
    pub(crate) fn from_handle(data: DataHandle) -> Self {
        Self {
            data,
            _types: PhantomData,
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
}

impl<T: StoreValue> CellAccess<'_, T> {
    /// Reads the current value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or value decoding fails.
    pub fn get(&self) -> Result<Option<T>, StoreError> {
        let encoded = self.data.get(CELL_KEY)?;
        self.data
            .poison_on_error(encoded.map(T::decode_value).transpose())
            .map_err(StoreError::from)
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

impl<T, SIZE> Clone for Cell<T, SIZE> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _types: PhantomData,
        }
    }
}
