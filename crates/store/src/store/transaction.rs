use libmdbx::{NoWriteMap, RW, Transaction as MdbxTransaction};

use super::{DataHandle, Transaction, TransactionAccess, Transactions};
use crate::StoreError;

impl Transactions {
    /// Begins one atomic transaction.
    ///
    /// The mutable borrow prevents this capability from starting another
    /// transaction until the first one is committed or dropped.
    ///
    /// # Errors
    ///
    /// Returns an error when MDBX cannot begin the transaction.
    pub fn begin(&mut self) -> Result<Transaction<'_>, StoreError> {
        Ok(Transaction {
            mdbx: self
                .database
                .begin_rw_txn()
                .map_err(|error| StoreError::storage("begin transaction", error))?,
            store_token: self.store_token,
            poisoned: std::cell::Cell::new(false),
            _thread_bound: std::marker::PhantomData,
        })
    }
}

impl Transaction<'_> {
    /// Borrows this transaction as a typed data-access capability.
    ///
    /// The returned value may be passed to existing [`crate::Cell`] and
    /// [`crate::OrderedMap`] objects. It cannot commit the transaction or
    /// create, open, or enumerate data objects.
    #[must_use]
    pub fn access(&self) -> TransactionAccess<'_> {
        TransactionAccess { transaction: self }
    }

    /// Atomically commits all changes and consumes the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or MDBX cannot commit it.
    pub fn commit(self) -> Result<(), StoreError> {
        let Self { mdbx, poisoned, .. } = self;
        if poisoned.get() {
            return Err(StoreError::TransactionPoisoned);
        }
        commit_mdbx(mdbx)
    }

    pub(super) fn record_result<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(error) = &result
            && error.poisons_transaction()
        {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn ensure_access(&self, handle: &DataHandle) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        if handle.store_token != self.store_token {
            self.poisoned.set(true);
            return Err(StoreError::WrongStore);
        }
        Ok(())
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), StoreError> {
        if self.poisoned.get() {
            Err(StoreError::TransactionPoisoned)
        } else {
            Ok(())
        }
    }
}

impl<'transaction> TransactionAccess<'transaction> {
    pub(super) const fn transaction(self) -> &'transaction Transaction<'transaction> {
        self.transaction
    }
}

pub(super) fn commit_mdbx(
    transaction: MdbxTransaction<'_, RW, NoWriteMap>,
) -> Result<(), StoreError> {
    match transaction.commit() {
        Ok(false) => Ok(()),
        Ok(true) => Err(StoreError::storage(
            "commit transaction",
            "MDBX aborted a transaction marked with a prior error",
        )),
        Err(error) => Err(StoreError::storage("commit transaction", error)),
    }
}
