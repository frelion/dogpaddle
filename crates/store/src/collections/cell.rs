use std::marker::PhantomData;

use crate::{DataAccess, DataHandle, StoreError, StoreValue, Transaction};

const CELL_KEY: &[u8] = &[];

/// A typed optional value stored in one generic data namespace.
pub struct Cell<T> {
    data: DataHandle,
    _value: PhantomData<fn() -> T>,
}

/// Transaction-bound access to a [`Cell`].
pub struct CellAccess<'transaction, T> {
    data: DataAccess<'transaction>,
    _value: PhantomData<fn() -> T>,
}

impl<T: StoreValue> Cell<T> {
    /// Wraps an existing generic data handle with cell behavior.
    #[must_use]
    pub fn new(data: DataHandle) -> Self {
        Self {
            data,
            _value: PhantomData,
        }
    }

    /// Binds this cell to an active transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle belongs to another store or the
    /// transaction is already poisoned.
    pub fn access<'transaction>(
        &self,
        transaction: &'transaction Transaction<'transaction>,
    ) -> Result<CellAccess<'transaction, T>, StoreError> {
        Ok(CellAccess {
            data: self.data.access(transaction)?,
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
            .poison_on_error(encoded.map(|bytes| T::decode_value(&bytes)).transpose())
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
        self.data.put(CELL_KEY, &encoded)
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

impl<T> Clone for Cell<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _value: PhantomData,
        }
    }
}
